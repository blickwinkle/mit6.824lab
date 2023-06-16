package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
)

const (
	Follower  = 0
	Candidate = 1
	Leader    = 2
)

const heartbeatTimeout = time.Duration(300) * time.Millisecond

const electionTimeout = time.Duration(800) * time.Millisecond

const heartbeatInterval = time.Duration(50) * time.Millisecond

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	applyCh chan ApplyMsg
	// Persistent state on all servers
	currentTerm int        // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int        // candidateId that received vote in current term (or null if none)
	log         []LogEntry // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)

	// Volatile state on all servers
	commitIndex   int // index of highest log entry known to be committed
	lastApplied   int // index of highest log entry applied to state machine
	leaderId      int // leaderId of the leader
	role          int // role of the server
	lastHeartbeat time.Time

	// Volatile state on leaders
	nextIndex  []int // initialized to leader last log index + 1, for each server, index of the next log entry to send to that server
	matchIndex []int // initialized to 0, for each server, index of highest log entry known to be replicated on server
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = (rf.role == Leader)
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currTerm int
	var votedFor int
	var log []LogEntry
	if d.Decode(&currTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		// fmt.Printf("Error in readPersist")
		panic("Error in readPersist")
	} else {
		rf.currentTerm = currTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // leader's commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) convertToFollwer() {
	rf.role = Follower
	rf.votedFor = -1
	rf.persist()
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.lastHeartbeat = time.Now()

	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		PrettyDebug(dClient, fmt.Sprintf("Server %d appendEntries: term %v is less than current term %v", rf.me, args.Term, rf.currentTerm))
		return
	}
	if len(rf.log) <= args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		PrettyDebug(dClient, fmt.Sprintf("Server %d appendEntries: prevLogIndex %v or prevLogTerm %v does not match", rf.me, args.PrevLogIndex, args.PrevLogTerm))
		return
	}

	for i, entry := range args.Entries {
		if args.PrevLogIndex+i+1 >= len(rf.log) {
			PrettyDebug(dClient, fmt.Sprintf("Server %d appendEntries: append entry, ind %d", rf.me, args.PrevLogIndex+i+1))
			rf.log = append(rf.log, entry)
		} else {
			PrettyDebug(dClient, fmt.Sprintf("Server %d appendEntries: replace entry %d", rf.me, args.PrevLogIndex+i+1))
			rf.log[args.PrevLogIndex+i+1] = entry
		}
	}
	rf.currentTerm = args.Term
	rf.leaderId = args.LeaderId
	rf.convertToFollwer()
	rf.persist()
	if args.LeaderCommit > rf.commitIndex {
		newCommitInd := min(args.LeaderCommit, len(rf.log)-1)
		for i := rf.commitIndex + 1; i <= newCommitInd; i++ {
			rf.applyCh <- ApplyMsg{CommandValid: true, Command: rf.log[i].Command, CommandIndex: i}
			PrettyDebug(dCommit, fmt.Sprintf("Server %d appendEntries: commitIndex updated to %d", rf.me, i))
		}
	}

	reply.Term = rf.currentTerm
	reply.Success = true
}

func min(i1, i2 int) int {
	if i1 < i2 {
		return i1
	}
	return i2
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.convertToFollwer()
	}
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if args.LastLogTerm > rf.log[len(rf.log)-1].Term || (args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= len(rf.log)-1) {
			rf.votedFor = args.CandidateId
			rf.role = Follower
			reply.VoteGranted = true
			reply.Term = rf.currentTerm
			PrettyDebug(dClient, fmt.Sprintf("Server %d voted for %d, local last term %d canidata last term %d", rf.me, args.CandidateId, rf.log[len(rf.log)-1].Term, args.LastLogTerm))
			rf.persist()
			return
		}
	}
	reply.VoteGranted = false
	reply.Term = rf.currentTerm
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.convertToFollwer()
		}
		rf.mu.Unlock()
	}
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != Leader {
		return index, term, false
	}
	index = len(rf.log)
	term = rf.currentTerm
	isLeader = true
	rf.log = append(rf.log, LogEntry{Term: term, Command: command})
	rf.persist()
	PrettyDebug(dLeader, fmt.Sprintf("Leader %d start command, log number %d", rf.me, index))
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) leaderCommit() {
	for j := rf.commitIndex + 1; j < len(rf.log); j++ {

		count := 1
		for k := 0; k < len(rf.peers); k++ {
			if k != rf.me && rf.matchIndex[k] >= j {
				count++
			}
		}
		if count > len(rf.peers)/2 {
			rf.commitIndex = j
			PrettyDebug(dLeader, fmt.Sprintf("leader %d commit log %d", rf.me, j))
			rf.applyCh <- ApplyMsg{CommandValid: true, Command: rf.log[rf.commitIndex].Command, CommandIndex: rf.commitIndex}
		}

	}
}

func (rf *Raft) broadcastHeartbeatOrLog() {
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(i int) {
				for {
					rf.mu.Lock()
					if rf.role != Leader {
						rf.mu.Unlock()
						return
					}
					rf.leaderCommit()
					args := &AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: rf.nextIndex[i] - 1,
						PrevLogTerm:  rf.log[rf.nextIndex[i]-1].Term,
						Entries:      rf.log[rf.nextIndex[i]:],
						LeaderCommit: rf.commitIndex,
					}
					rf.mu.Unlock()
					reply := &AppendEntriesReply{}
					if !rf.sendAppendEntries(i, args, reply) {
						return
					}
					// rf.mu.Lock()
					// if rf.role != Leader {
					// 	rf.mu.Unlock()
					// 	return
					// }
					// rf.mu.Unlock()
					if reply.Success {
						rf.mu.Lock()
						rf.nextIndex[i] = len(rf.log)
						rf.matchIndex[i] = len(rf.log) - 1
						rf.leaderCommit()
						rf.mu.Unlock()
						return
					} else {
						PrettyDebug(dLeader, fmt.Sprintf("leader %d send log to %d failed, nextind %d replyTerm %d", rf.me, i, rf.nextIndex[i], reply.Term))
						rf.mu.Lock()
						if reply.Term > rf.currentTerm {
							rf.currentTerm = reply.Term
							rf.convertToFollwer()

							rf.mu.Unlock()
							PrettyDebug(dLeader, fmt.Sprintf("leader %d become follower, curr term %d", rf.me, rf.currentTerm))
							return
						}
						rf.nextIndex[i] = rf.nextIndex[i] - 1
						rf.mu.Unlock()
					}
				}
			}(i)
		}
	}
}

func (rf *Raft) broadcastRequestVote() {
	rf.mu.Lock()
	PrettyDebug(dCandidate, fmt.Sprintf("Server %d broadcasting request vote, currTerm %d", rf.me, rf.currentTerm))
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	rf.mu.Unlock()

	voteCh := make(chan bool, len(rf.peers)-1)
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(server int) {
				var reply RequestVoteReply
				if rf.sendRequestVote(server, args, &reply) {
					voteCh <- reply.VoteGranted
				}
			}(i)
		}
	}

	voteCount := 1
	for i := 0; i < len(rf.peers)-1; i++ {
		rf.mu.Lock()
		if rf.role != Candidate {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		select {
		case voteGranted := <-voteCh:
			if voteGranted {
				voteCount++
				if voteCount > len(rf.peers)/2 {
					rf.mu.Lock()
					if rf.role == Candidate {
						rf.role = Leader
						rf.startLeader()
					}
					rf.mu.Unlock()
					return
				}
			}
		case <-time.After(electionTimeout):
			// 超时，重新开始选举
			return
		}
	}

}

func (rf *Raft) startLeader() {
	PrettyDebug(dLeader, fmt.Sprintf("Server %d become leader currTerm %d", rf.me, rf.currentTerm))
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}

	go func() {
		for !rf.killed() {
			rf.mu.Lock()
			if rf.role != Leader {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
			rf.broadcastHeartbeatOrLog()
			time.Sleep(heartbeatInterval)
		}
	}()
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		time.Sleep(time.Duration(rand.Intn(20)+30) * time.Millisecond)
		rf.mu.Lock()
		if rf.role == Leader {
			rf.mu.Unlock()
			continue
		}
		rf.mu.Unlock()

		rf.mu.Lock()
		if rf.lastHeartbeat.Add(heartbeatTimeout).Before(time.Now()) {
			rf.currentTerm++
			rf.votedFor = rf.me
			rf.persist()
			rf.role = Candidate
			rf.lastHeartbeat = time.Now()
			rf.mu.Unlock()
			rf.broadcastRequestVote()
		} else {
			rf.mu.Unlock()
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.role = Follower
	rf.votedFor = -1
	rf.currentTerm = 0
	rf.log = make([]LogEntry, 1)
	rf.log[0] = LogEntry{Term: 0, Command: nil}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.lastHeartbeat = time.Now()

	rf.applyCh = applyCh
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
