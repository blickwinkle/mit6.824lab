package shardkv

import (
	"bytes"
	"log"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
	"github.com/sasha-s/go-deadlock"
)

const (
	cliOpTimeOut              = 150 * time.Millisecond
	leaderCheckDuration       = 80 * time.Millisecond
	configChangeCheckDuration = 35 * time.Millisecond
	acquirShardDuration       = 50 * time.Millisecond
	msgLoopTimeOut            = 100 * time.Millisecond
)

const Debug = 1

func DPrintf(format string, a ...interface{}) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
}

const (
	Expired = iota
	Acquiring
	Working
)

const (
	GET = "Get"
	PUT = "Put"
	APP = "Append"
)

type RecordReply struct {
	OpID     int64
	ClientID int64
	Reply    *GetReply
}

type CommitInfo struct {
	OpID int64
}

type ShardUpdateInfo struct {
	OpID      int64
	Shard     int
	ShardInfo GetShardReply
}

type ConfigChangeInfo struct {
	OpID   int64
	Config shardctrler.Config
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

}

type ShardKV struct {
	mu           deadlock.RWMutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	peresister   *raft.Persister
	mck          *shardctrler.Clerk
	CurrConfig   shardctrler.Config
	BeforeConfig shardctrler.Config
	// ClientID       int64
	WaitCh         map[int64]chan *CommitInfo
	ShardState     map[int]map[int]int // ConfigNum -> Shard -> State
	ShardStore     map[int]map[string]string
	ShardLastReply map[int]map[int64]*RecordReply // Shard -> ClientID -> LastReply
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if _, isleader := kv.rf.GetState(); !isleader {
		reply.Err = ErrWrongLeader
		return
	}
	shard := key2shard(args.Key)
	kv.mu.RLock()
	if kv.ShardState[kv.CurrConfig.Num][shard] != Working {
		kv.mu.RUnlock()
		reply.Err = ErrWrongGroup
		return
	}
	if _, ok := kv.ShardLastReply[shard][args.ClientID]; ok && kv.ShardLastReply[shard][args.ClientID].OpID == args.OpID {
		reply.Value = kv.ShardLastReply[shard][args.ClientID].Reply.Value
		reply.Err = kv.ShardLastReply[shard][args.ClientID].Reply.Err
		kv.mu.RUnlock()
		return
	} else if ok && kv.ShardLastReply[shard][args.ClientID].OpID > args.OpID {
		panic("OpID error")
	}
	kv.mu.RUnlock()
	if _, _, isleader := kv.rf.Start(*args); !isleader {
		reply.Err = ErrWrongLeader
		return
	}
	ch := make(chan *CommitInfo, 1)
	kv.mu.Lock()
	kv.WaitCh[args.ClientID] = ch
	kv.mu.Unlock()
	var cmminfo *CommitInfo
	select {
	case cmminfo = <-ch:
	case <-time.After(cliOpTimeOut):
		kv.mu.Lock()
		kv.WaitCh[args.ClientID] = nil
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	// kv.WaitCh[args.ClientID] = nil
	if cmminfo != nil && cmminfo.OpID == args.OpID {
		if _, ok := kv.ShardLastReply[shard][args.ClientID]; !ok {
			reply.Err = ErrWrongGroup
			return
		}
		if kv.ShardLastReply[shard][args.ClientID].OpID == args.OpID {
			reply.Value = kv.ShardLastReply[shard][args.ClientID].Reply.Value
			reply.Err = kv.ShardLastReply[shard][args.ClientID].Reply.Err
			return
		}
	}
	reply.Err = ErrWrongLeader
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if _, isleader := kv.rf.GetState(); !isleader {
		reply.Err = ErrWrongLeader
		return
	}
	shard := key2shard(args.Key)
	kv.mu.RLock()
	if kv.ShardState[kv.CurrConfig.Num][shard] != Working {
		kv.mu.RUnlock()
		reply.Err = ErrWrongGroup
		return
	}
	if _, ok := kv.ShardLastReply[shard][args.ClientID]; ok && kv.ShardLastReply[shard][args.ClientID].OpID == args.OpID {
		reply.Err = kv.ShardLastReply[shard][args.ClientID].Reply.Err
		kv.mu.RUnlock()
		return
	} else if ok && kv.ShardLastReply[shard][args.ClientID].OpID > args.OpID {
		panic("OpID error")
	}
	kv.mu.RUnlock()
	if _, _, isleader := kv.rf.Start(*args); !isleader {
		reply.Err = ErrWrongLeader
		return
	}
	// DPrintf("S%d PutAppend key %v value %v op %v", kv.me, args.Key, args.Value, args.Op)
	ch := make(chan *CommitInfo, 1)
	kv.mu.Lock()
	kv.WaitCh[args.ClientID] = ch
	kv.mu.Unlock()
	var cmminfo *CommitInfo
	select {
	case cmminfo = <-ch:
	case <-time.After(cliOpTimeOut):
		// DPrintf("S%d PutAppend key %v value %v op %v timeout", kv.me, args.Key, args.Value, args.Op)
		kv.mu.Lock()
		kv.WaitCh[args.ClientID] = nil
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	// kv.WaitCh[args.ClientID] = nil
	if cmminfo != nil && cmminfo.OpID == args.OpID {
		if _, ok := kv.ShardLastReply[shard][args.ClientID]; !ok {
			reply.Err = ErrWrongGroup
			return
		}
		if kv.ShardLastReply[shard][args.ClientID].OpID == args.OpID {
			reply.Err = kv.ShardLastReply[shard][args.ClientID].Reply.Err
			return
		}

	}
	reply.Err = ErrWrongLeader
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// Disable SnapShot
	// kv.maxraftstate = -1

	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	// kv.ClientID = nrand()
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.peresister = persister
	kv.ShardLastReply = make(map[int]map[int64]*RecordReply)
	kv.ShardStore = make(map[int]map[string]string)
	kv.ShardState = make(map[int]map[int]int)
	kv.WaitCh = make(map[int64]chan *CommitInfo)
	kv.CurrConfig = kv.mck.Query(0)
	kv.restore(kv.peresister.ReadSnapshot())
	go kv.msgloop()
	go kv.checkConfigChangeLoop()
	go kv.acquirShardLoop()
	return kv
}

func init() {
	labgob.Register(Op{})
	labgob.Register(GetArgs{})
	labgob.Register(PutAppendArgs{})
	labgob.Register(GetShardArgs{})
	labgob.Register(GetShardReply{})
	labgob.Register(ConfigChangeInfo{})
	labgob.Register(ShardUpdateInfo{})
	labgob.Register(RecordReply{})
}

func (kv *ShardKV) checkConfigChangeLoop() {
	for ; ; time.Sleep(configChangeCheckDuration) {
		if _, isleader := kv.rf.GetState(); !isleader {
			continue
		}
		kv.mu.RLock()
		// 如果还有未完成的配置变更，有shard在acquire，则不进行
		isContinue := true
		for _, state := range kv.ShardState[kv.CurrConfig.Num] {
			if state == Acquiring {

				isContinue = false
				break
			}
		}
		if !isContinue {
			kv.mu.RUnlock()
			continue
		}
		currConfig := kv.CurrConfig
		kv.mu.RUnlock()
		newConfig := kv.mck.Query(currConfig.Num + 1)
		kv.mu.RLock()
		currConfig = kv.CurrConfig
		kv.mu.RUnlock()
		if newConfig.Num != currConfig.Num+1 {
			continue
		}
		DPrintf("S%d Commit ConfigChange %v", kv.me, newConfig)
		kv.rf.Start(ConfigChangeInfo{OpID: nrand(), Config: newConfig})
		// kv.CurrConfig = kv.mck.Query(kv.CurrConfig.Num + 1)
	}
}

func (kv *ShardKV) acquirShardLoop() {
	for ; ; time.Sleep(acquirShardDuration) {
		if _, isleader := kv.rf.GetState(); !isleader {
			continue
		}
		kv.mu.RLock()

		for shard, gid := range kv.CurrConfig.Shards {
			if gid == kv.gid && kv.ShardState[kv.CurrConfig.Num][shard] == Acquiring {
				go func(shard int) {
					kv.mu.RLock()
					defer kv.mu.RUnlock()
					args := &GetShardArgs{Shard: shard, ConfigNum: kv.CurrConfig.Num, Gid: kv.gid, OpID: nrand()}
					if _, isleader := kv.rf.GetState(); !isleader {
						return
					}

					if kv.ShardState[kv.CurrConfig.Num][shard] != Acquiring {
						return
					}
					var reply GetShardReply
					for _, sname := range kv.BeforeConfig.Groups[kv.BeforeConfig.Shards[shard]] {
						if _, isleader := kv.rf.GetState(); !isleader {
							return
						}
						kv.mu.RUnlock()
						ok := kv.make_end(sname).Call("ShardKV.AcquirShard", args, &reply)
						kv.mu.RLock()
						if ok && reply.Err == OK {

							if kv.ShardState[kv.CurrConfig.Num][shard] != Acquiring {
								return
							}
							// DPrintf("S%d AcquirShard shard %v", kv.me, shard)
							kv.rf.Start(ShardUpdateInfo{OpID: args.OpID, Shard: shard, ShardInfo: reply})
							return
						} else if ok && reply.Err == ErrWrongGroup {
							return
						}
					}

				}(shard)
			}
		}
		kv.mu.RUnlock()
	}
}

func (kv *ShardKV) AcquirShard(args *GetShardArgs, reply *GetShardReply) {
	if _, isleader := kv.rf.GetState(); !isleader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.RLock()
	if kv.ShardState[args.ConfigNum] == nil || kv.ShardState[args.ConfigNum][args.Shard] != Expired {
		reply.Err = ErrWrongGroup
		kv.mu.RUnlock()
		return
	}
	kv.mu.RUnlock()
	DPrintf("S%d BeAcquirShard shard %v", kv.me, args.Shard)
	if _, _, isleader := kv.rf.Start(*args); !isleader {
		reply.Err = ErrWrongLeader
		return
	}
	ch := make(chan *CommitInfo, 1)
	kv.mu.Lock()
	kv.WaitCh[args.OpID] = ch
	kv.mu.Unlock()
	var cmminfo *CommitInfo
	select {
	case cmminfo = <-ch:
	case <-time.After(cliOpTimeOut):
		kv.mu.Lock()
		kv.WaitCh[args.OpID] = nil
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
		return
	}

	if cmminfo == nil || cmminfo.OpID != args.OpID {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	reply.ShardStore = kv.ShardStore[args.Shard]
	reply.ShardRecordReply = kv.ShardLastReply[args.Shard]
	reply.Err = OK
}

func (kv *ShardKV) applyOP(op interface{}) (int64, int64) {
	// DPrintf("S%d applyOP %v", kv.me, op)
	switch op := op.(type) {
	case GetArgs:
		shard := key2shard(op.Key)
		if kv.ShardState[kv.CurrConfig.Num][shard] != Working {
			return op.ClientID, op.OpID
		}
		var reply GetReply
		if val, ok := kv.ShardStore[shard][op.Key]; ok {
			reply.Err = OK
			reply.Value = val
		} else {
			reply.Err = ErrNoKey
		}
		if _, ok := kv.ShardLastReply[shard]; !ok {
			kv.ShardLastReply[shard] = make(map[int64]*RecordReply)
		}
		kv.ShardLastReply[shard][op.ClientID] = &RecordReply{OpID: op.OpID, ClientID: op.ClientID, Reply: &reply}
		DPrintf("S%d Get key %v value %v", kv.me, op.Key, reply.Value)
		if reply.Value == "" {
			DPrintf("S%d Get key %v value is empty", kv.me, op.Key)
		}
		return op.ClientID, op.OpID
	case PutAppendArgs:

		shard := key2shard(op.Key)
		if kv.ShardState[kv.CurrConfig.Num][shard] != Working {
			return op.ClientID, op.OpID
		}
		if _, ok := kv.ShardLastReply[shard][op.ClientID]; ok && kv.ShardLastReply[shard][op.ClientID].OpID >= op.OpID {
			return op.ClientID, op.OpID
		}
		var reply GetReply
		if _, ok := kv.ShardStore[shard]; !ok {
			kv.ShardStore[shard] = make(map[string]string)
		}
		if op.Op == PUT {
			kv.ShardStore[shard][op.Key] = op.Value
		}
		if op.Op == APP {
			kv.ShardStore[shard][op.Key] += op.Value
		}
		reply.Err = OK
		if _, ok := kv.ShardLastReply[shard]; !ok {
			kv.ShardLastReply[shard] = make(map[int64]*RecordReply)
		}
		kv.ShardLastReply[shard][op.ClientID] = &RecordReply{OpID: op.OpID, ClientID: op.ClientID, Reply: &reply}
		DPrintf("S%d PutAppend key %v value %v", kv.me, op.Key, op.Value)
		return op.ClientID, op.OpID
	case GetShardArgs:
		DPrintf("S%d GetShard shard %v", kv.me, op.Shard)
		return op.OpID, op.OpID
	case ConfigChangeInfo:
		if op.Config.Num != kv.CurrConfig.Num+1 {
			return op.OpID, op.OpID
		}
		copyConfig := op.Config
		copyConfig.Groups = make(map[int][]string)
		for k, v := range op.Config.Groups {
			copyConfig.Groups[k] = make([]string, len(v))
			copy(copyConfig.Groups[k], v)
		}

		// kv.CurrConfig = copyConfig
		kv.ShardState[copyConfig.Num] = make(map[int]int)

		for shard, gid := range op.Config.Shards {
			if kv.CurrConfig.Num == 0 {
				if gid == kv.gid {
					kv.ShardState[copyConfig.Num][shard] = Working
				} else {
					kv.ShardState[copyConfig.Num][shard] = Expired
				}
				continue
			}
			if gid == kv.gid {
				if kv.ShardState[kv.CurrConfig.Num][shard] != Working {
					kv.ShardState[copyConfig.Num][shard] = Acquiring
				} else {
					kv.ShardState[copyConfig.Num][shard] = Working
				}
			} else {
				kv.ShardState[copyConfig.Num][shard] = Expired
			}

		}
		kv.BeforeConfig = kv.CurrConfig
		kv.CurrConfig = copyConfig
		DPrintf("S%d ConfigChange %v", kv.me, op.Config)
		return op.OpID, op.OpID
	case ShardUpdateInfo:
		if kv.ShardState[kv.CurrConfig.Num][op.Shard] != Acquiring {
			return op.OpID, op.OpID
		}
		kv.ShardState[kv.CurrConfig.Num][op.Shard] = Working
		// kv.ShardStore[op.Shard] = make(map[string]string)
		if _, ok := kv.ShardStore[op.Shard]; !ok {
			kv.ShardStore[op.Shard] = make(map[string]string)
		}
		for k, v := range op.ShardInfo.ShardStore {
			kv.ShardStore[op.Shard][k] = v
		}
		if _, ok := kv.ShardLastReply[op.Shard]; !ok {
			kv.ShardLastReply[op.Shard] = make(map[int64]*RecordReply)
		}
		for k, v := range op.ShardInfo.ShardRecordReply {
			kv.ShardLastReply[op.Shard][k] = v
		}
		DPrintf("S%d ShardUpdate shard %v", kv.me, op.Shard)
		return op.OpID, op.OpID
	default:
		panic("unknown op type")
	}
}
func (kv *ShardKV) handleMsg(ch <-chan *raft.ApplyMsg) {
	for msg := range ch {
		if msg.CommandValid {

			kv.mu.Lock()
			clid, opid := kv.applyOP(msg.Command)

			if waitChannel, ok := kv.WaitCh[clid]; ok && waitChannel != nil {
				waitChannel <- &CommitInfo{opid}
				kv.WaitCh[clid] = nil
				close(waitChannel)
			}
			if kv.maxraftstate != -1 && kv.peresister.RaftStateSize() >= kv.maxraftstate {
				kv.rf.Snapshot(msg.CommandIndex, kv.store())
			}
			kv.mu.Unlock()
		} else if msg.SnapshotValid {
			kv.mu.Lock()
			kv.restore(msg.Snapshot)
			kv.mu.Unlock()
		}
	}
}

func (kv *ShardKV) msgloop() {
	msgCh := make(chan *raft.ApplyMsg, 1000)
	go kv.handleMsg(msgCh)
	for {
		select {
		case msg := <-kv.applyCh:
			// if msg.CommandIndex <= kv.commitIdx {
			// 	continue
			// }
			// sc.commitIdx = msg.CommandIndex
			msgCh <- &msg

			// DPrintf("S%d msgCh len %v", kv.me, len(msgCh))
		case <-time.After(msgLoopTimeOut):
			// if sc.killed() {
			// 	return
			// }

			go func() {
				_, isLeader := kv.rf.GetState()
				if !isLeader {
					kv.mu.Lock()
					defer kv.mu.Unlock()
					for _, waitChannel := range kv.WaitCh {
						if waitChannel != nil {
							waitChannel <- nil
							close(waitChannel)
						}
					}
					for k := range kv.WaitCh {
						delete(kv.WaitCh, k)
					}
				}
			}()
		}
	}
}

func (kv *ShardKV) store() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.ShardStore)
	e.Encode(kv.ShardLastReply)
	e.Encode(kv.ShardState)
	e.Encode(kv.CurrConfig)
	e.Encode(kv.BeforeConfig)

	return w.Bytes()
}

func (kv *ShardKV) restore(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var ShardStore map[int]map[string]string
	var ShardLastReply map[int]map[int64]*RecordReply
	var ShardState map[int]map[int]int
	var CurrConfig shardctrler.Config
	var BeforeConfig shardctrler.Config
	if d.Decode(&ShardStore) != nil ||
		d.Decode(&ShardLastReply) != nil ||
		d.Decode(&ShardState) != nil ||
		d.Decode(&CurrConfig) != nil ||
		d.Decode(&BeforeConfig) != nil {
		panic("failed to restore")
	} else {
		kv.ShardStore = ShardStore
		kv.ShardLastReply = ShardLastReply
		kv.ShardState = ShardState
		kv.CurrConfig = CurrConfig
		kv.BeforeConfig = BeforeConfig
	}
}
