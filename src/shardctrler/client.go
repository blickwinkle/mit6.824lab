package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	leader   uint32
	clientID int64
	mu       sync.Mutex
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
	// Your code here.
	ck.leader = 0
	ck.clientID = nrand()
	return ck
}

func (ck *Clerk) Query(num int) Config {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := &QueryArgs{ClientID: ck.clientID, OpID: nrand()}
	// Your code here.
	args.Num = num
	leader := atomic.LoadUint32(&ck.leader)

	// try each known server.
	first := true
	for i := leader; ; i = (i + 1) % uint32(len(ck.servers)) {
		srv := ck.servers[i]
		var reply QueryReply
		ok := srv.Call("ShardCtrler.Query", args, &reply)
		if ok && !reply.WrongLeader {
			atomic.StoreUint32(&ck.leader, i)
			return reply.Config
		}
		if i == leader {
			if first {
				first = false
			} else {
				time.Sleep(100 * time.Millisecond)
			}
		}
	}

}

func (ck *Clerk) Join(servers map[int][]string) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := &JoinArgs{ClientID: ck.clientID, OpID: nrand()}
	// Your code here.
	args.Servers = servers
	leader := atomic.LoadUint32(&ck.leader)
	first := true
	for i := leader; ; i = (i + 1) % uint32(len(ck.servers)) {
		srv := ck.servers[i]
		var reply JoinReply
		ok := srv.Call("ShardCtrler.Join", args, &reply)

		if ok && !reply.WrongLeader {
			atomic.StoreUint32(&ck.leader, i)
			return
		}
		if i == leader {
			if first {
				first = false
			} else {
				time.Sleep(100 * time.Millisecond)
			}
		}
	}
}

func (ck *Clerk) Leave(gids []int) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := &LeaveArgs{ClientID: ck.clientID, OpID: nrand()}
	// Your code here.
	args.GIDs = gids

	leader := atomic.LoadUint32(&ck.leader)
	first := true
	for i := leader; ; i = (i + 1) % uint32(len(ck.servers)) {
		srv := ck.servers[i]
		var reply LeaveReply
		ok := srv.Call("ShardCtrler.Leave", args, &reply)

		if ok && !reply.WrongLeader {
			atomic.StoreUint32(&ck.leader, i)
			return
		}
		if i == leader {
			if first {
				first = false
			} else {
				time.Sleep(100 * time.Millisecond)
			}
		}
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := &MoveArgs{ClientID: ck.clientID, OpID: nrand()}
	// Your code here.
	args.Shard = shard
	args.GID = gid

	leader := atomic.LoadUint32(&ck.leader)
	first := true
	for i := leader; ; i = (i + 1) % uint32(len(ck.servers)) {
		srv := ck.servers[i]

		var reply MoveReply
		ok := srv.Call("ShardCtrler.Move", args, &reply)

		if ok && !reply.WrongLeader {
			atomic.StoreUint32(&ck.leader, i)
			return
		}
		if i == leader {
			if first {
				first = false
			} else {
				time.Sleep(100 * time.Millisecond)
			}
		}
	}
}
