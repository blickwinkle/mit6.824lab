package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync/atomic"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leader   uint32
	clientID int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.leader = 0
	ck.clientID = nrand()
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	args := GetArgs{Key: key, ClientID: ck.clientID, OpID: nrand()}
	reply := GetReply{}
	leader := atomic.LoadUint32(&ck.leader)
	ok := ck.servers[leader].Call("KVServer.Get", &args, &reply)
	for !ok || reply.Err == ErrWrongLeader {
		leader = (leader + 1) % uint32(len(ck.servers))
		ok = ck.servers[leader].Call("KVServer.Get", &args, &reply)
	}
	atomic.StoreUint32(&ck.leader, leader)
	if reply.Err == OK {
		return reply.Value
	}
	return ""
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{Key: key, Value: value, Op: op, ClientID: ck.clientID, OpID: nrand()}
	reply := PutAppendReply{}
	leader := atomic.LoadUint32(&ck.leader)
	ok := ck.servers[leader].Call("KVServer.PutAppend", &args, &reply)
	for !ok || reply.Err == ErrWrongLeader {
		leader = (leader + 1) % uint32(len(ck.servers))
		ok = ck.servers[leader].Call("KVServer.PutAppend", &args, &reply)
	}
	atomic.StoreUint32(&ck.leader, leader)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
