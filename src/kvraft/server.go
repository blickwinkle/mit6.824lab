package kvraft

import (
	"bytes"
	"log"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"github.com/sasha-s/go-deadlock"
)

const Debug = false

const d = 300 * time.Millisecond

const (
	GET    = "Get"
	PUT    = "Put"
	APPEND = "Append"
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key      string
	Value    string
	OpType   string
	ClientID int64
	OpID     int64
}

type OpRes struct {
	Err   Err
	Value string
	OpID  int64
}

type CommitInfo struct {
	op       *Op
	isLeader bool
}

type KVServer struct {
	mu      deadlock.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	persister    *raft.Persister
	LastClientOP map[int64]*OpRes
	Storage      map[string]string
	waitChannel  map[int64]chan<- *CommitInfo
	commitIdx    int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	if res, ok := kv.LastClientOP[args.ClientID]; ok && res.OpID == args.OpID {
		reply.Err = res.Err
		reply.Value = res.Value
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	op := Op{Key: args.Key, OpType: GET, ClientID: args.ClientID, OpID: args.OpID}
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	ch := make(chan *CommitInfo, 1)

	kv.mu.Lock()
	kv.waitChannel[args.ClientID] = ch
	kv.mu.Unlock()
	var waitInfo *CommitInfo
	for {
		select {
		case waitInfo = <-ch:
			goto OUTER_LOOP
		case <-time.After(10 * d):
			if kv.killed() {
				return
			}
			DPrintf("S%d Get key %v timeout", kv.me, args.Key)
			reply.Err = ErrWrongLeader
			return
		}
	}
OUTER_LOOP:
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.waitChannel[args.ClientID] = nil
	_, isLeader = kv.rf.GetState()
	if waitInfo != nil && waitInfo.op != nil && waitInfo.op.OpID == args.OpID && isLeader {
		if kv.Storage[args.Key] == "" {
			reply.Err = ErrNoKey
			reply.Value = ""
		} else {
			reply.Err = OK
			reply.Value = kv.Storage[args.Key]
		}
		// kv.LastClientOP[args.ClientID] = &OpRes{Err: reply.Err, Value: reply.Value, OpID: args.OpID}
	} else {
		reply.Err = ErrWrongLeader
	}
	DPrintf("S%d Get key %v value %v reply %v", kv.me, args.Key, reply.Value, reply.Err)

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	if res, ok := kv.LastClientOP[args.ClientID]; ok && res.OpID == args.OpID {
		reply.Err = res.Err
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	op := Op{Key: args.Key, Value: args.Value, OpType: args.Op, ClientID: args.ClientID, OpID: args.OpID}
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	ch := make(chan *CommitInfo, 2)
	kv.waitChannel[args.ClientID] = ch
	kv.mu.Unlock()

	var waitInfo *CommitInfo
	for {
		select {
		case waitInfo = <-ch:
			goto OUTER_LOOP
		case <-time.After(10 * d):
			// if kv.killed() {
			// 	return
			// }
			DPrintf("S%d PutAppend key %v timeout", kv.me, args.Key)

			reply.Err = ErrWrongLeader
			return

		}
	}
OUTER_LOOP:
	kv.mu.Lock()
	kv.waitChannel[args.ClientID] = nil
	kv.mu.Unlock()
	_, isLeader = kv.rf.GetState()
	if waitInfo != nil && waitInfo.op != nil && waitInfo.op.OpID == args.OpID && isLeader {
		reply.Err = OK
	} else {
		reply.Err = ErrWrongLeader
	}
	DPrintf("S%d PutAppend key %v value %v op %v reply %v", kv.me, args.Key, args.Value, args.Op, reply.Err)

}
func (kv *KVServer) handleMsg(ch <-chan *raft.ApplyMsg) {
	for msg := range ch {
		if msg.CommandValid {
			op := msg.Command.(Op)

			kv.mu.Lock()
			kv.applyOP(&op)

			if waitChannel, ok := kv.waitChannel[op.ClientID]; ok && waitChannel != nil {
				waitChannel <- &CommitInfo{op: &op, isLeader: true}
				kv.waitChannel[op.ClientID] = nil
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

func (kv *KVServer) loop() {
	msgCh := make(chan *raft.ApplyMsg, 1000)
	go kv.handleMsg(msgCh)
	for {
		select {
		case msg := <-kv.applyCh:
			// if msg.CommandIndex <= kv.commitIdx {
			// 	continue
			// }
			kv.commitIdx = msg.CommandIndex
			msgCh <- &msg

			// DPrintf("S%d msgCh len %v", kv.me, len(msgCh))
		case <-time.After(3 * d):
			if kv.killed() {
				return
			}

			go func() {
				_, isLeader := kv.rf.GetState()
				kv.mu.Lock()
				if !isLeader {
					for _, waitChannel := range kv.waitChannel {
						if waitChannel != nil {
							waitChannel <- &CommitInfo{op: nil, isLeader: isLeader}
							close(waitChannel)
						}
					}
					for k := range kv.waitChannel {
						delete(kv.waitChannel, k)
					}
				}
				kv.mu.Unlock()
			}()
		}
	}
}

func (kv *KVServer) applyOP(op *Op) {
	if res, ok := kv.LastClientOP[op.ClientID]; ok && res.OpID == op.OpID {
		return
	}

	switch op.OpType {
	case GET:
		DPrintf("S%d apply get key %v", kv.me, op.Key)
		if kv.Storage[op.Key] == "" {
			kv.LastClientOP[op.ClientID] = &OpRes{Err: ErrNoKey, OpID: op.OpID}
		} else {
			kv.LastClientOP[op.ClientID] = &OpRes{Err: OK, Value: kv.Storage[op.Key], OpID: op.OpID}
		}
	case PUT:
		DPrintf("S%d apply put key %v value %v", kv.me, op.Key, op.Value)
		kv.Storage[op.Key] = op.Value
		kv.LastClientOP[op.ClientID] = &OpRes{Err: OK, OpID: op.OpID}

	case APPEND:
		DPrintf("S%d apply append key %v value %v", kv.me, op.Key, op.Value)
		kv.Storage[op.Key] += op.Value
		kv.LastClientOP[op.ClientID] = &OpRes{Err: OK, OpID: op.OpID}

	}

}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) restore(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var lastClientOP map[int64]*OpRes
	var storage map[string]string
	if d.Decode(&lastClientOP) != nil ||
		d.Decode(&storage) != nil {
		panic("kvserver err in read snap")
	}
	kv.LastClientOP = lastClientOP
	kv.Storage = storage

}

func (kv *KVServer) store() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.LastClientOP)
	e.Encode(kv.Storage)
	return w.Bytes()
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.waitChannel = make(map[int64]chan<- *CommitInfo)
	kv.LastClientOP = make(map[int64]*OpRes)
	kv.Storage = make(map[string]string)
	kv.commitIdx = -1
	kv.persister = persister
	// You may need initialization code here.
	kv.restore(persister.ReadSnapshot())
	go kv.loop()
	return kv
}
