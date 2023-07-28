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
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
	"github.com/sasha-s/go-deadlock"
)

const (
	Follower  = 0
	Candidate = 1
	Leader    = 2
)

var (
	ErrCompact  = fmt.Errorf("compact")
	ErrOutRange = fmt.Errorf("out of range")
)

const heartbeatTimeout = time.Duration(50) * time.Millisecond

const electionTimeout = time.Duration(100) * time.Millisecond

const heartbeatInterval = time.Duration(15) * time.Millisecond

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

type Log struct {
	Entries             []LogEntry
	LastSnapShotLogIdx  int
	LastSnapShotLogTerm int
	LastSnapshot        []byte
}

func (l *Log) init() {
	l.Entries = make([]LogEntry, 0)
	l.LastSnapShotLogIdx = -1
	l.LastSnapShotLogTerm = -1
}

func (l *Log) updateSnapShot(idx int, term int, snapshot []byte) {
	l.deleteFrom(idx + 1)

	l.LastSnapShotLogIdx = idx
	l.LastSnapShotLogTerm = term
	l.LastSnapshot = snapshot

}

func (l *Log) getLastestTerm() int {
	if len(l.Entries) == 0 {
		return l.LastSnapShotLogTerm
	}
	return l.Entries[len(l.Entries)-1].Term
}

func (l *Log) Term(idx int) (int, error) {
	if idx == l.LastSnapShotLogIdx {
		return l.LastSnapShotLogTerm, nil
	}
	if idx < l.LastSnapShotLogIdx {
		return -1, fmt.Errorf("idx %d is less than last snapshot idx %d", idx, l.LastSnapShotLogIdx)
	}
	if idx > l.LastSnapShotLogIdx+len(l.Entries) {
		return -1, fmt.Errorf("idx %d is greater than last log idx %d", idx, l.LastSnapShotLogIdx+len(l.Entries))
	}
	return l.Entries[idx-l.LastSnapShotLogIdx-1].Term, nil
}

func (l *Log) getLogEntry(idx int) LogEntry {
	return l.Entries[idx-l.LastSnapShotLogIdx-1]
}

func (l *Log) getLogEntries(startIdx int) []LogEntry {
	return l.Entries[startIdx-l.LastSnapShotLogIdx-1:]
}

func (l *Log) getLogEntriesFrom(startIdx int) []LogEntry {
	return l.Entries[startIdx-l.LastSnapShotLogIdx-1:]
}

func (l *Log) getLogEntriesTo(endIdx int) []LogEntry {
	return l.Entries[:endIdx-l.LastSnapShotLogIdx-1]
}

func (l *Log) getLogEntriesFromTo(startIdx int, endIdx int) []LogEntry {
	return l.Entries[startIdx-l.LastSnapShotLogIdx-1 : endIdx-l.LastSnapShotLogIdx-1]
}

func (l *Log) updateLogEntries(startIdx int, entries []LogEntry) {
	l.Entries = append(l.getLogEntriesTo(startIdx), entries...)
}

func (l *Log) updateLogEntriesFrom(startIdx int, entries []LogEntry) {
	l.Entries = append(l.getLogEntriesFrom(startIdx), entries...)
}

func (l *Log) updateLogEntriesTo(endIdx int, entries []LogEntry) {
	l.Entries = append(entries, l.getLogEntriesFrom(endIdx)...)
}

func (l *Log) updateLogEntriesFromTo(startIdx int, endIdx int, entries []LogEntry) {
	l.Entries = append(l.getLogEntriesFrom(startIdx), entries...)
	l.Entries = append(l.Entries, l.getLogEntriesFrom(endIdx)...)
}

func (l *Log) deleteFrom(startIdx int) {
	if startIdx-l.LastSnapShotLogIdx-1 < 0 || startIdx-l.LastSnapShotLogIdx-1 >= len(l.Entries) {
		l.Entries = make([]LogEntry, 0)
		return
	}
	l.Entries = l.getLogEntriesFrom(startIdx)
}

func (l *Log) deleteLogEntriesTo(endIdx int) {
	if endIdx-l.LastSnapShotLogIdx-1 < 0 {
		l.Entries = make([]LogEntry, 0)
		return
	}
	l.Entries = l.getLogEntriesTo(endIdx)
}

func (l *Log) deleteLogEntriesFromTo(startIdx int, endIdx int) {
	l.Entries = append(l.getLogEntriesTo(startIdx), l.getLogEntriesFrom(endIdx)...)
}

func (l *Log) deleteLogEntries(startIdx int, entries []LogEntry) {
	l.Entries = append(l.getLogEntriesTo(startIdx), l.getLogEntriesFrom(startIdx+len(entries))...)
}

func (l *Log) deleteLogEntriesFrom(startIdx int, entries []LogEntry) {
	l.Entries = append(l.getLogEntriesTo(startIdx), l.getLogEntriesFrom(startIdx+len(entries))...)
}

func (l *Log) replaceOneLogEntry(idx int, entry LogEntry) {
	l.Entries[idx-l.LastSnapShotLogIdx-1] = entry
}

func (l *Log) getLogLength() int {
	return len(l.Entries)
}

func (l *Log) getLastLogEntry() LogEntry {
	return l.Entries[len(l.Entries)-1]
}

func (l *Log) getLastFesibleLogEntryIdx() int {
	return l.LastSnapShotLogIdx + len(l.Entries)
}

func (l *Log) getOriginLen() int {
	return len(l.Entries) + l.LastSnapShotLogIdx + 1
}

func (l *Log) appendBack(entry LogEntry) {
	l.Entries = append(l.Entries, entry)
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        deadlock.Mutex      // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	applyCh chan ApplyMsg
	// Persistent state on all servers
	currentTerm int  // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int  // candidateId that received vote in current term (or null if none)
	log         *Log // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)

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
	tmp := rf.log.LastSnapshot
	rf.log.LastSnapshot = nil
	e.Encode(rf.log)
	rf.log.LastSnapshot = tmp
	data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	rf.persister.SaveStateAndSnapshot(data, rf.log.LastSnapshot)
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
	var log Log
	if d.Decode(&currTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		// fmt.Printf("Error in readPersist")
		panic("Error in readPersist")
	} else {
		rf.log = &log
		rf.log.LastSnapshot = rf.persister.ReadSnapshot()
		if rf.log.LastSnapshot == nil && rf.log.LastSnapShotLogIdx != -1 {
			panic("Error in readPersist snapshot is nil but LastSnapShotLogIdx is not -1")
		}
		rf.currentTerm = currTerm
		rf.votedFor = votedFor

	}
}

type InstallSnapshotArgs struct {
	Term              int    // leader's term
	LeaderId          int    // so follower can redirect clients
	LastIncludedIndex int    // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int    // term of lastIncludedIndex
	Data              []byte // raw bytes of the snapshot chunk, starting at offset
	Done              bool   // true if this is the last chunk
}

type InstallSnapshotReply struct {
	Term int // currentTerm, for leader to update itself
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.convertToFollwer()
	}
	rf.lastHeartbeat = time.Now()
	rf.role = Follower
	rf.leaderId = args.LeaderId

	rf.log.updateSnapShot(args.LastIncludedIndex, args.LastIncludedTerm, args.Data)
	rf.log.Entries = make([]LogEntry, 0)
	// rf.log.init()
	// rf.log.appendBack(LogEntry{Term: 0})

	rf.persist()
	reply.Term = rf.currentTerm
	rf.commitIndex = rf.log.LastSnapShotLogIdx
	rf.applyCh <- ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		Snapshot:      rf.log.LastSnapshot,
		SnapshotTerm:  rf.log.LastSnapShotLogTerm,
		SnapshotIndex: rf.log.LastSnapShotLogIdx,
	}
	PrettyDebug(dSnap, "S%d InstallSnapshot success. LastSanpeShot %d LastSnapTerm %d SnapShot len %d", rf.me, rf.log.LastSnapShotLogIdx, rf.log.LastSnapShotLogTerm, len(rf.log.LastSnapshot))

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

	go rf.snapShotInternal(index, snapshot)
}

func (rf *Raft) snapShotInternal(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.log.LastSnapShotLogIdx || index > rf.log.getLastFesibleLogEntryIdx() {
		PrettyDebug(dSnap, fmt.Sprintf("S%d Snapshot %d failed", rf.me, index))
		return
	}

	rf.log.updateSnapShot(index, rf.log.getLogEntry(index).Term, snapshot)
	rf.persist()
	PrettyDebug(dSnap, fmt.Sprintf("S%d Snapshot %d success snapshot len %d", rf.me, index, len(snapshot)))
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

	// Fast Revocer
	// XTerm：这个是Follower中与Leader冲突的Log对应的任期号。在之前（7.1）有介绍Leader会在prevLogTerm中带上本地Log记录中，前一条Log的任期号。如果Follower在对应位置的任期号不匹配，它会拒绝Leader的AppendEntries消息，并将自己的任期号放在XTerm中。如果Follower在对应位置没有Log，那么这里会返回 -1。
	// XIndex：这个是Follower中，对应任期号为XTerm的第一条Log条目的槽位号。
	// XLen：如果Follower在对应位置没有Log，那么XTerm会返回-1，XLen表示空白的Log槽位数。
	XTerm int // term of conflicting entry
	XIdx  int // first index of the conflicting term's log entry idx
	XLen  int // if Xtrem == -1, XLen is the length of the follower's blank log
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
		PrettyDebug(dClient, fmt.Sprintf("S%d appendEntries: term %v is less than current term %v", rf.me, args.Term, rf.currentTerm))
		return
	}
	term, err := rf.log.Term(args.PrevLogIndex)
	if rf.log.getLastFesibleLogEntryIdx() < args.PrevLogIndex || err != nil || term != args.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		PrettyDebug(dClient, fmt.Sprintf("S%d appendEntries: prevLogIndex %v or prevLogTerm %v does not match", rf.me, args.PrevLogIndex, args.PrevLogTerm))
		if rf.log.getLastFesibleLogEntryIdx() < args.PrevLogIndex {
			reply.XTerm = -1
			reply.XIdx = -1
			reply.XLen = args.PrevLogIndex - rf.log.getLastFesibleLogEntryIdx()
		} else {
			// if err != nil {
			// 	panic(err)
			// }
			reply.XTerm = term
			reply.XIdx = 0
			for i := args.PrevLogIndex - 1; i >= 0; i-- {
				term, err := rf.log.Term(i)
				if err != nil {
					reply.XIdx = rf.log.LastSnapShotLogIdx + 1
					break
				}
				if term != reply.XTerm {
					reply.XIdx = i + 1
					break
				}
			}
			reply.XLen = -1
		}
		return
	}

	for i, entry := range args.Entries {
		if args.PrevLogIndex+i+1 >= rf.log.getOriginLen() {
			PrettyDebug(dClient, fmt.Sprintf("S%d appendEntries: append entry, ind %d, log term %d", rf.me, args.PrevLogIndex+i+1, entry.Term))
			rf.log.appendBack(entry)
		} else {
			if rf.log.getLogEntry(args.PrevLogIndex+i+1).Term != entry.Term {
				rf.log.deleteLogEntriesTo(args.PrevLogIndex + i + 1)
				PrettyDebug(dClient, fmt.Sprintf("S%d appendEntries: append entry, ind %d, log term %d", rf.me, args.PrevLogIndex+i+1, entry.Term))
				rf.log.appendBack(entry)
				continue
			}
			PrettyDebug(dClient, fmt.Sprintf("S%d appendEntries: replace entry %d, old log term %d new log term %d", rf.me, args.PrevLogIndex+i+1, rf.log.getLogEntry(args.PrevLogIndex+i+1).Term, entry.Term))
			// rf.log[args.PrevLogIndex+i+1] = entry
			rf.log.replaceOneLogEntry(args.PrevLogIndex+i+1, entry)
		}
	}
	if rf.currentTerm < args.Term {
		rf.convertToFollwer()
	}
	rf.role = Follower
	rf.currentTerm = args.Term
	rf.leaderId = args.LeaderId

	rf.persist()
	if args.LeaderCommit > rf.commitIndex {
		newCommitInd := min(args.LeaderCommit, rf.log.getOriginLen()-1)
		for i := rf.commitIndex + 1; i <= newCommitInd; i++ {
			if i <= rf.log.LastSnapShotLogIdx {
				PrettyDebug(dCommit, fmt.Sprintf("S%d commit snapshot lastTerm %d lastIdx %d snapshot len %d", rf.me, rf.log.LastSnapShotLogTerm, rf.log.LastSnapShotLogIdx, len(rf.log.LastSnapshot)))
				rf.applyCh <- ApplyMsg{CommandValid: false,

					SnapshotValid: true,
					Snapshot:      rf.log.LastSnapshot,
					SnapshotTerm:  rf.log.LastSnapShotLogTerm,
					SnapshotIndex: rf.log.LastSnapShotLogIdx,
				}
				i = rf.log.LastSnapShotLogIdx
				continue
			}

			PrettyDebug(dCommit, fmt.Sprintf("S%d appendEntries: commitIndex updated to %d", rf.me, i))
			isSnapShot := false
			rf.applyCh <- ApplyMsg{CommandValid: true,
				Command:       rf.log.getLogEntry(i).Command,
				CommandIndex:  i,
				SnapshotValid: isSnapShot,
			}
		}
		// Test SnapShot
		// rf.snapShotInternal(newCommitInd, nil)

		rf.commitIndex = newCommitInd
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
		term := rf.log.getLastestTerm()

		if args.LastLogTerm > term || (args.LastLogTerm == term && args.LastLogIndex >= rf.log.getLastFesibleLogEntryIdx()) {
			rf.votedFor = args.CandidateId
			rf.role = Follower
			reply.VoteGranted = true
			reply.Term = rf.currentTerm
			PrettyDebug(dClient, fmt.Sprintf("S%d voted for %d, local last term %d canidata last term %d", rf.me, args.CandidateId, term, args.LastLogTerm))
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
	index = rf.log.getOriginLen()
	term = rf.currentTerm
	isLeader = true
	rf.log.appendBack(LogEntry{Term: term, Command: command})
	rf.persist()
	PrettyDebug(dLeader, fmt.Sprintf("L%d start command, log number %d", rf.me, index))
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

// If there exists an N such that N > commitIndex, a majority
// of matchIndex[i] ≥ N, and log[N].term == currentTerm:

// set commitIndex = N (§5.3, §5.4).
func (rf *Raft) leaderCommit() {
	for j := rf.commitIndex + 1; j < rf.log.getOriginLen(); j++ {
		if j <= rf.log.LastSnapShotLogIdx {
			continue
		}
		if rf.log.getLogEntry(j).Term != rf.currentTerm {
			continue
		}
		count := 1
		for k := 0; k < len(rf.peers); k++ {
			if k != rf.me && rf.matchIndex[k] >= j {
				count++
			}
		}
		if count > len(rf.peers)/2 {
			for i := rf.commitIndex + 1; i <= j; i++ {
				if i < rf.log.LastSnapShotLogIdx {
					continue
				}
				if i == rf.log.LastSnapShotLogIdx {
					PrettyDebug(dLeader, fmt.Sprintf("L%d commit snapshot lastTerm %d lastIdx %d snapshot len %d", rf.me, rf.log.LastSnapShotLogTerm, rf.log.LastSnapShotLogIdx, len(rf.log.LastSnapshot)))
					rf.applyCh <- ApplyMsg{
						CommandValid:  false,
						SnapshotValid: true,
						Snapshot:      rf.log.LastSnapshot,
						SnapshotTerm:  rf.log.LastSnapShotLogTerm,
						SnapshotIndex: rf.log.LastSnapShotLogIdx,
					}
					continue
				}
				PrettyDebug(dLeader, fmt.Sprintf("L%d commit log %d", rf.me, i))
				isSnapShot := false
				rf.applyCh <- ApplyMsg{
					CommandValid:  true,
					Command:       rf.log.getLogEntry(i).Command,
					CommandIndex:  i,
					SnapshotValid: isSnapShot,
				}
			}
			rf.commitIndex = j
		}
	}
}

func (rf *Raft) handleSendSnap(server int) bool {
	args := &InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.log.LastSnapShotLogIdx,
		LastIncludedTerm:  rf.log.LastSnapShotLogTerm,
		Data:              rf.log.LastSnapshot,
		Done:              true,
	}

	reply := &InstallSnapshotReply{}
	isSend := false
	rf.mu.Unlock()
	isSend = rf.sendInstallSnapshot(server, args, reply)
	rf.mu.Lock()
	if isSend {
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.convertToFollwer()
		} else {
			rf.nextIndex[server] = rf.log.LastSnapShotLogIdx + 1
			rf.matchIndex[server] = rf.log.LastSnapShotLogIdx
		}
	}
	return isSend
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
					// rf.leaderCommit()
					preTerm, err := rf.log.Term(rf.nextIndex[i] - 1)
					if err != nil {
						if !rf.handleSendSnap(i) || rf.role != Leader {
							rf.mu.Unlock()
							return
						}
						preTerm, err = rf.log.Term(rf.nextIndex[i] - 1)
						if err != nil {
							panic("Error in broadcastHeartbeatOrLog")
						}
					}
					args := &AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: rf.nextIndex[i] - 1,
						PrevLogTerm:  preTerm,
						//	Entries:      rf.log[rf.nextIndex[i]:],
						Entries:      rf.log.getLogEntriesFrom(rf.nextIndex[i]),
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
						rf.nextIndex[i] = len(args.Entries) + args.PrevLogIndex + 1
						rf.matchIndex[i] = len(args.Entries) + args.PrevLogIndex
						rf.leaderCommit()
						rf.mu.Unlock()
						return
					} else {
						PrettyDebug(dLeader, fmt.Sprintf("L%d send log to %d failed, nextind %d replyTerm %d", rf.me, i, rf.nextIndex[i], reply.Term))
						rf.mu.Lock()
						if reply.Term > rf.currentTerm {
							rf.currentTerm = reply.Term
							rf.convertToFollwer()

							rf.mu.Unlock()
							PrettyDebug(dLeader, fmt.Sprintf("L%d become follower, curr term %d", rf.me, rf.currentTerm))
							return
						}
						// rf.nextIndex[i] = max(rf.nextIndex[i]-1, 1)
						if reply.XTerm != -1 {
							rf.nextIndex[i] = reply.XIdx
							for j := args.PrevLogIndex; j >= 0; j-- {
								term, err := rf.log.Term(j)
								if err != nil {
									rf.nextIndex[i] = 0
									break
								}
								if term == reply.XTerm {
									rf.nextIndex[i] = j + 1
									break
								}
							}
						} else {
							rf.nextIndex[i] = reply.XLen
						}

						rf.mu.Unlock()
					}
				}
			}(i)
		}
	}
}

func max(i1, i2 int) int {
	if i1 > i2 {
		return i1
	}
	return i2
}

func (rf *Raft) broadcastRequestVote() {
	rf.mu.Lock()
	PrettyDebug(dCandidate, fmt.Sprintf("S%d broadcasting request vote, currTerm %d", rf.me, rf.currentTerm))
	term := rf.log.getLastestTerm()
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.log.getLastFesibleLogEntryIdx(),
		LastLogTerm:  term,
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
	PrettyDebug(dLeader, fmt.Sprintf("S%d become leader currTerm %d", rf.me, rf.currentTerm))
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.log.getOriginLen()
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
		time.Sleep(time.Duration(rand.Intn(10)+20) * time.Millisecond)
		rf.mu.Lock()
		if rf.role == Leader {
			rf.mu.Unlock()
			continue
		}

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
	rf.log = &Log{}
	rf.log.init()
	rf.log.appendBack(LogEntry{Term: 0})
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
