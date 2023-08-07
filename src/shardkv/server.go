package shardkv

import (
	"sync"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)

const (
	cliOpTimeOut              = 150 * time.Millisecond
	leaderCheckDuration       = 100 * time.Millisecond
	configChangeCheckDuration = 100 * time.Millisecond
	acquirShardDuration       = 100 * time.Millisecond
	msgLoopTimeOut            = 100 * time.Millisecond
)

const (
	Working = iota
	Acquiring
	Expired
)

const (
	GET = "Get"
	PUT = "Put"
	APP = "Append"
)

type RecordReply struct {
	OpID     int64
	ClientID int64
	Reply    GetReply
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
	OpID      int64
	config   shardctrler.Config
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

}

type ShardKV struct {
	mu           sync.RWMutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	mck        *shardctrler.Clerk
	CurrConfig *shardctrler.Config
	// ClientID       int64
	WaitCh         map[int64]chan *CommitInfo
	ShardState     map[int]map[int]int // ConfigNum -> Shard -> State
	ShardStore     map[int]map[string]string
	ShardLastReply map[int]map[int64]RecordReply // Shard -> ClientID -> LastReply
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
	if cmminfo != nil && cmminfo.OpID == args.OpID && kv.ShardLastReply[shard][args.ClientID].OpID == args.OpID {
		reply.Value = kv.ShardLastReply[shard][args.ClientID].Reply.Value
		reply.Err = kv.ShardLastReply[shard][args.ClientID].Reply.Err
		return
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
	if cmminfo != nil && cmminfo.OpID == args.OpID && kv.ShardLastReply[shard][args.ClientID].OpID == args.OpID {
		reply.Err = kv.ShardLastReply[shard][args.ClientID].Reply.Err
		return
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

	return kv
}

func (kv *ShardKV) checkConfigChangeLoop() {
	for ; ; time.Sleep(configChangeCheckDuration) {
		if _, isleader := kv.rf.GetState(); !isleader {
			continue
		}
		kv.mu.RLock()
		currConfig := kv.CurrConfig
		kv.mu.RUnlock()
		newConfig := kv.mck.Query(currConfig.Num + 1)
		kv.mu.RLock()
		currConfig = kv.CurrConfig
		kv.mu.RUnlock()
		if newConfig.Num != currConfig.Num+1 {
			continue
		}
		kv.rf.Start(newConfig)
		// kv.CurrConfig = kv.mck.Query(kv.CurrConfig.Num + 1)
	}
}

func (kv *ShardKV) acquirShardLoop() {
	for ; ; time.Sleep(acquirShardDuration) {
		if _, isleader := kv.rf.GetState(); !isleader {
			continue
		}
		kv.mu.RLock()
		currConfig := kv.CurrConfig

		for shard, gid := range currConfig.Shards {
			if gid == kv.gid && kv.ShardState[currConfig.Num][shard] == Acquiring {
				go func(shard int) {
					args := &GetShardArgs{Shard: shard, ConfigNum: currConfig.Num, Gid: kv.gid, OpID: nrand()}
					if _, isleader := kv.rf.GetState(); !isleader {
						return
					}
					kv.mu.RLock()
					if kv.ShardState[currConfig.Num][shard] != Acquiring {
						kv.mu.RUnlock()
						return
					}
					kv.mu.RUnlock()
					var reply GetShardReply
					for _, sname := range currConfig.Groups[currConfig.Shards[shard]] {
						if _, isleader := kv.rf.GetState(); !isleader {
							return
						}
						if ok := kv.make_end(sname).Call("ShardKV.AcquirShard", args, &reply); ok && reply.Err == OK {
							kv.mu.RLock()
							if kv.ShardState[kv.CurrConfig.Num][shard] != Acquiring {
								kv.mu.RUnlock()
								return
							}
							kv.mu.RUnlock()
							kv.rf.Start(ConfigChangeInfo{OpID: args.OpID, Shard: shard, ShardInfo: reply})
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
		kv.ShardLastReply[shard][op.ClientID] = RecordReply{OpID: op.OpID, ClientID: op.ClientID, Reply: reply}
		return op.ClientID, op.OpID
	case PutAppendArgs:
		shard := key2shard(op.Key)
		if kv.ShardState[kv.CurrConfig.Num][shard] != Working {
			return op.ClientID, op.OpID
		}
		var reply GetReply
		if op.Op == PUT {
			kv.ShardStore[shard][op.Key] = op.Value
		}
		if op.Op == APP {
			kv.ShardStore[shard][op.Key] += op.Value
		}
		reply.Err = OK
		kv.ShardLastReply[shard][op.ClientID] = RecordReply{OpID: op.OpID, ClientID: op.ClientID, Reply: reply}
		return op.ClientID, op.OpID
	case GetShardArgs:
		return op.OpID, op.OpID
	case ConfigChangeInfo:
		if op.config.Num != kv.CurrConfig.Num+1 {
			return op.OpID, op.OpID
		}
		for shard, gid := range op.config.Shards {
			if gid == kv.gid {
				if kv.ShardState[kv.CurrConfig.Num][shard] != Working {
				kv.ShardState[kv.CurrConfig.Num][shard] = Acquiring
			} else {
				kv.ShardState[kv.CurrConfig.Num][shard] = Expired
			}
		}

	}
}
func (kv *ShardKV) handleMsg(ch <-chan *raft.ApplyMsg) {
	for msg := range ch {
		if msg.CommandValid {

			kv.mu.Lock()
			kv.applyOP(&op)

			if waitChannel, ok := kv.WaitCh[op.ClientID]; ok && waitChannel != nil {
				waitChannel <- &CommitInfo{OpID: op.OpID}
				kv.WaitCh[op.ClientID] = nil
				close(waitChannel)
			}
			if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate {
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
					for _, waitChannel := range sc.waitChannel {
						if waitChannel != nil {
							waitChannel <- nil
							close(waitChannel)
						}
					}
					for k := range kv.waitChannel {
						delete(kv.waitChannel, k)
					}
				}
			}()
		}
	}
}
