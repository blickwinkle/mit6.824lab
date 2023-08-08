package shardctrler

import (
	"encoding/gob"
	"log"
	"sort"
	"sync"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const timeout = 100 * time.Millisecond

type CommitInfo struct {
	OpID int64
}

type RecordReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
	OpID        int64
}

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	LastClientOp map[int64]*RecordReply       // client id -> last reply
	waitChannel  map[int64]chan<- *CommitInfo // client id -> wait channel
	configs      []Config                     // indexed by config num
}

type Op struct {
	// Your data here.
}

func init() {
	labgob.Register(Config{})
}

// 计算平衡每个组的shard数量后的config
func (sc *ShardCtrler) rebalance(config *Config) {
	// newConfig := *config
	// newConfig.Num++
	// for k, v := range config.Groups {
	// 	newConfig.Groups[k] = make([]string, len(v))
	// 	copy(newConfig.Groups[k], v)
	// }

	// 如果没有组了，那么所有的shard都属于0组
	if len(config.Groups) == 0 {
		for i := 0; i < NShards; i++ {
			config.Shards[i] = 0
		}
		return
	}
	ordered_group := make([]int, 0)
	for gid := range config.Groups {
		ordered_group = append(ordered_group, gid)
	}
	// 按照组的gid排序
	sort.Ints(ordered_group)

	// 计算每个组的shard数量
	groupShardNum := make(map[int]int)
	for _, gid := range config.Shards {
		groupShardNum[gid]++
	}
	// 没有shard的组初始化为0
	for gid := range config.Groups {
		if _, ok := groupShardNum[gid]; !ok {
			groupShardNum[gid] = 0
		}
	}
	// 计算每个组的shard数量的平均值， 不存在的组的shard要变成0
	avg := NShards / len(config.Groups)
	// 计算每个组需要移动的shard数量
	moveNum := make(map[int]int)
	for gid, num := range groupShardNum {
		if _, ok := config.Groups[gid]; !ok {
			moveNum[gid] = num
			continue
		}
		moveNum[gid] = num - avg
	}
	// 计算需要移动的shard
	for shard := 0; shard < NShards; shard++ {
		gid := config.Shards[shard]
		if moveNum[gid] > 0 {
			for _, gid2 := range ordered_group {
				num := moveNum[gid2]
				if num < 0 {
					config.Shards[shard] = gid2
					moveNum[gid]--
					moveNum[gid2]++
					break
				}
			}
		}
	}
	// 如果0组还有shard，那么就把它们分配给其他组
	for shard := 0; shard < NShards; shard++ {
		gid := config.Shards[shard]
		if _, ok := config.Groups[gid]; !ok {
			for _, gid2 := range ordered_group {

				config.Shards[shard] = gid2
				break

			}
		}
	}

}

func (sc *ShardCtrler) apply(op interface{}) {
	switch op := op.(type) {
	case JoinArgs:
		newConfig := sc.configs[len(sc.configs)-1]
		newConfig.Num++
		newConfig.Groups = make(map[int][]string)
		for k, v := range sc.configs[len(sc.configs)-1].Groups {
			newConfig.Groups[k] = make([]string, len(v))
			copy(newConfig.Groups[k], v)
		}
		for gid, servers := range op.Servers {

			newConfig.Groups[gid] = make([]string, len(servers))
			copy(newConfig.Groups[gid], servers)
		}
		sc.rebalance(&newConfig)
		sc.configs = append(sc.configs, newConfig)
		sc.LastClientOp[op.ClientID] = &RecordReply{OpID: op.OpID, Err: OK}
		DPrintf("S%d Join %v", sc.me, op.Servers)
	case LeaveArgs:
		newConfig := sc.configs[len(sc.configs)-1]
		newConfig.Num++
		newConfig.Groups = make(map[int][]string)
		for k, v := range sc.configs[len(sc.configs)-1].Groups {
			newConfig.Groups[k] = make([]string, len(v))
			copy(newConfig.Groups[k], v)
		}
		for _, gid := range op.GIDs {
			delete(newConfig.Groups, gid)
		}
		sc.rebalance(&newConfig)
		sc.configs = append(sc.configs, newConfig)
		sc.LastClientOp[op.ClientID] = &RecordReply{OpID: op.OpID, Err: OK}
		DPrintf("S%d Leave %v", sc.me, op.GIDs)
	case MoveArgs:
		newConfig := sc.configs[len(sc.configs)-1]
		newConfig.Num++
		newConfig.Groups = make(map[int][]string)
		for k, v := range sc.configs[len(sc.configs)-1].Groups {
			newConfig.Groups[k] = make([]string, len(v))
			copy(newConfig.Groups[k], v)
		}
		newConfig.Shards[op.Shard] = op.GID
		sc.configs = append(sc.configs, newConfig)
		sc.LastClientOp[op.ClientID] = &RecordReply{OpID: op.OpID, Err: OK}
		DPrintf("S%d Move %v", sc.me, op)
	case QueryArgs:
		if op.Num == -1 || op.Num >= len(sc.configs) {
			op.Num = len(sc.configs) - 1
		}

		sc.LastClientOp[op.ClientID] = &RecordReply{OpID: op.OpID, Err: OK, Config: sc.configs[op.Num]}
		DPrintf("S%d Query %v", sc.me, op.Num)

	default:
		panic("unknown op type")
	}
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	if _, isLeader := sc.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		return
	}
	sc.mu.Lock()
	if lastReply, ok := sc.LastClientOp[args.ClientID]; ok && lastReply.OpID == args.OpID {
		reply.Err = lastReply.Err
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()
	_, _, isLeader := sc.rf.Start(*args)
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	ch := make(chan *CommitInfo, 1)
	sc.mu.Lock()
	sc.waitChannel[args.ClientID] = ch
	sc.mu.Unlock()
	var waitInfo *CommitInfo
	select {
	case waitInfo = <-ch:
	case <-time.After(timeout):
		DPrintf("S%d Join timeout", sc.me)
		reply.WrongLeader = true
		return
	}
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if waitInfo != nil && waitInfo.OpID == args.OpID {

		reply.Err = OK
		reply.WrongLeader = false

	} else {
		reply.WrongLeader = true
	}

}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	if _, isLeader := sc.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		return
	}
	sc.mu.Lock()
	if lastReply, ok := sc.LastClientOp[args.ClientID]; ok && lastReply.OpID == args.OpID {
		reply.Err = lastReply.Err
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()
	_, _, isLeader := sc.rf.Start(*args)
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	ch := make(chan *CommitInfo, 1)
	sc.mu.Lock()
	sc.waitChannel[args.ClientID] = ch
	sc.mu.Unlock()
	var waitInfo *CommitInfo
	select {
	case waitInfo = <-ch:
	case <-time.After(2 * timeout):
		DPrintf("S%d Leave timeout", sc.me)
		reply.WrongLeader = true
		return
	}
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if waitInfo != nil && waitInfo.OpID == args.OpID {

		reply.Err = OK
		reply.WrongLeader = false

	} else {
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	if _, isLeader := sc.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		return
	}
	sc.mu.Lock()
	if lastReply, ok := sc.LastClientOp[args.ClientID]; ok && lastReply.OpID == args.OpID {
		reply.Err = lastReply.Err
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()
	_, _, isLeader := sc.rf.Start(*args)
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	ch := make(chan *CommitInfo, 1)
	sc.mu.Lock()
	sc.waitChannel[args.ClientID] = ch
	sc.mu.Unlock()
	var waitInfo *CommitInfo
	select {
	case waitInfo = <-ch:
	case <-time.After(timeout):
		DPrintf("S%d Move timeout", sc.me)
		reply.WrongLeader = true
		return
	}
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if waitInfo != nil && waitInfo.OpID == args.OpID {

		reply.Err = OK
		reply.WrongLeader = false

	} else {
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	if _, isLeader := sc.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		return
	}
	sc.mu.Lock()
	if lastReply, ok := sc.LastClientOp[args.ClientID]; ok && lastReply.OpID == args.OpID {
		reply.Config = lastReply.Config
		reply.Err = lastReply.Err
		sc.mu.Unlock()
		return
	}
	if args.Num == -1 || args.Num >= len(sc.configs) {
		args.Num = len(sc.configs) - 1
	}
	sc.mu.Unlock()
	_, _, isLeader := sc.rf.Start(*args)
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	ch := make(chan *CommitInfo, 1)
	sc.mu.Lock()
	sc.waitChannel[args.ClientID] = ch
	sc.mu.Unlock()
	var waitInfo *CommitInfo
	select {
	case waitInfo = <-ch:
	case <-time.After(timeout):
		DPrintf("S%d Query timeout", sc.me)
		reply.WrongLeader = true
		return
	}
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if waitInfo != nil && waitInfo.OpID == args.OpID {

		reply.Err = OK
		reply.Config = sc.configs[args.Num]

	} else {
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) handleMsg(ch <-chan *raft.ApplyMsg) {
	for msg := range ch {
		if msg.CommandValid {
			ClientId := int64(-1)
			OpID := int64(-1)
			switch cmd := msg.Command.(type) {
			case JoinArgs:
				ClientId = cmd.ClientID
				OpID = cmd.OpID
			case LeaveArgs:
				ClientId = cmd.ClientID
				OpID = cmd.OpID
			case MoveArgs:
				ClientId = cmd.ClientID
				OpID = cmd.OpID
			case QueryArgs:
				ClientId = cmd.ClientID
				OpID = cmd.OpID
			default:
				panic("unknown op type")
			}

			sc.mu.Lock()
			sc.apply(msg.Command)
			if waitChannel, ok := sc.waitChannel[ClientId]; ok && waitChannel != nil {
				waitChannel <- &CommitInfo{OpID: OpID}
				sc.waitChannel[ClientId] = nil
				close(waitChannel)
			}
			// if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate {
			// 	kv.rf.Snapshot(msg.CommandIndex, kv.store())
			// }
			sc.mu.Unlock()
		} else if msg.SnapshotValid {
			panic("snapshot not supported")
		}
	}
}

func (sc *ShardCtrler) loop() {
	msgCh := make(chan *raft.ApplyMsg, 1000)
	go sc.handleMsg(msgCh)
	for {
		select {
		case msg := <-sc.applyCh:
			// if msg.CommandIndex <= kv.commitIdx {
			// 	continue
			// }
			// sc.commitIdx = msg.CommandIndex
			msgCh <- &msg

			// DPrintf("S%d msgCh len %v", kv.me, len(msgCh))
		case <-time.After(timeout):
			// if sc.killed() {
			// 	return
			// }

			go func() {
				_, isLeader := sc.rf.GetState()
				if !isLeader {
					sc.mu.Lock()
					defer sc.mu.Unlock()
					for _, waitChannel := range sc.waitChannel {
						if waitChannel != nil {
							waitChannel <- nil
							close(waitChannel)
						}
					}
					for k := range sc.waitChannel {
						delete(sc.waitChannel, k)
					}
				}
			}()
		}
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.LastClientOp = make(map[int64]*RecordReply)
	sc.waitChannel = make(map[int64]chan<- *CommitInfo)
	gob.Register(JoinArgs{})
	gob.Register(LeaveArgs{})
	gob.Register(MoveArgs{})
	gob.Register(QueryArgs{})
	go sc.loop()
	return sc
}
