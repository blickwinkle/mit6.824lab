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
	Reply    *GetReply
}

type CommitInfo struct {
	OpID int64
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
	mck            *shardctrler.Clerk
	CurrConfig     *shardctrler.Config
	WaitCh         map[int64]chan *CommitInfo
	ShardState     map[int]int
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
	if kv.ShardState[shard] != Working {
		kv.mu.RUnlock()
		reply.Err = ErrWrongGroup
		return
	}
	if kv.ShardLastReply[shard][args.ClientID] != nil && kv.ShardLastReply[shard][args.ClientID].OpID == args.OpID {
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
	kv.mu.Lock()

}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
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
		newConfig := kv.mck.Query(kv.CurrConfig.Num + 1)
		if newConfig.Num != kv.CurrConfig.Num+1 {
			continue
		}
		kv.rf.Start(newConfig)
		// kv.CurrConfig = kv.mck.Query(kv.CurrConfig.Num + 1)
	}
}
