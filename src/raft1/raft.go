package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"fmt"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

const hbperiod = 100 * time.Millisecond
const electionTimeout = 1000 * time.Millisecond // if a follower hasn't heard from a leader in this time, it becomes candidate

type serverState int

// In any given time a server must be in one of three states: follower, candidate, or leader.
const (
	follower  serverState = iota // followers are passeive, they issue no requests
	candidate                    // is used to elect a new leader
	leader                       // handles client requests
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	sendinghb atomic.Bool

	hbtime atomic.Pointer[time.Time] // last heartbeat time
	fstate serverState               // state of this peer

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	ps  *PersistentState
	vs  *VolatileState
	lvs *LeaderVolatileState
}

type PersistentState struct {
	currentTerm int // each server stores a current term number
	votedFor    *int
	log         []LogEntry
}

type LogEntry struct {
	term    int
	command interface{} // TODO: choose type
}

type VolatileState struct {
	commitIndex int
	lastApplied int
}

type LeaderVolatileState struct {
	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.ps.currentTerm, rf.fstate == leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
}
type AppendEntriesReply struct{}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) { // initiated by candidates during elections
	now := time.Now()

	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("Raft peer %d received RequestVote RPC from peer %d for term %d\n", rf.me, args.CandidateId, args.Term)

	if args.Term < rf.ps.currentTerm { // if a server receives a request with a stale term number, it rejects the request
		reply.Term = rf.ps.currentTerm
		reply.VoteGranted = false
	} else if args.Term == rf.ps.currentTerm {
		if rf.ps.votedFor == nil || (rf.ps.votedFor == &args.CandidateId && rf.vs.commitIndex >= args.LastLogIndex) {
			rf.makeMeFollower(&args.CandidateId, args.Term)

			reply.VoteGranted = true
			rf.hbtime.Store(&now)
		} else {
			reply.VoteGranted = false
		}
	} else {
		rf.makeMeFollower(&args.CandidateId, args.Term) // if a candidate or a leader discovers that its terms is out of date, it immediately reverts to follower state

		reply.VoteGranted = true
		rf.hbtime.Store(&now)
	}

	// Your code here (3A, 3B).
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) { // initiated by leaders to replicate log entries; also used as heartbeat by leader or candidate
	now := time.Now()

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.ps.currentTerm { // if a server receives a request with a stale term number, it rejects the request
		return
	}

	if args.Term > rf.ps.currentTerm {
		rf.makeMeFollower(nil, args.Term) // if a candidate or a leader discovers that its terms is out of date, it immediately reverts to follower state
		rf.hbtime.Store(&now)
	}

	//persistent, and global states update
	switch rf.fstate {
	case candidate:
		// TODO: handle log addition
	case leader:
		//TODO: handle AppendEntries if i am leader
	case follower:
		// TODO: handle log addition
		rf.hbtime.Store(&now)
	default:
		panic("unknown follower state")
	}

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) buildAppendEntriesHBArgs() *AppendEntriesArgs {
	return &AppendEntriesArgs{
		Term:     rf.ps.currentTerm,
		LeaderId: rf.me,
	}
}

func (rf *Raft) buildAppendEntriesHBReply() *AppendEntriesReply {
	return &AppendEntriesReply{}
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

	// Your code here (3B).

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

func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		shouldStart := rf.shouldStartElection()
		currentTerm := rf.ps.currentTerm

		if shouldStart {
			DPrintf("Raft peer %d starting election for term %d\n", rf.me, currentTerm+1)

			rf.makeMeCandidate()

			done := make(chan struct{})

			go func() {
				rf.runElection()
				close(done)
			}()

			<-done
			DPrintf("Raft peer %d finished election for term %d\n", rf.me, currentTerm+1)

		}
		rf.mu.Unlock()

		if shouldStart {
			// pause for a random amount of time between 50 and 350
			// milliseconds.
			ms := 50 + (rand.Int63() % 300)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}
	}
}

func (rf *Raft) runElection() {
	votes := rf.requestForVotes()

	if rf.ps.votedFor != nil && *rf.ps.votedFor == rf.me {
		votes += 1
	}

	if votes >= (len(rf.peers)/2)+1 {
		rf.makeMeLeader()
	}
}

func (rf *Raft) requestForVotes() int {
	args := rf.buildRequestVoeteArgs()
	granted := make(chan bool, len(rf.peers)-1)
	DPrintf("Raft peer %d is requesting votes from all peers n", rf.me)
	wg := sync.WaitGroup{}
	wg.Add(len(rf.peers) - 1)

	// send RequestVote RPCs to all other servers
	for sid := range rf.peers {
		if sid == rf.me {
			continue
		}
		go func(sid int) {
			defer wg.Done()
			reply := rf.buildRequestVoteReply()
			if ok := rf.sendRequestVote(sid, args, reply); ok {
				if reply.VoteGranted {
					granted <- true
					DPrintf("Raft peer %d received \"vote\" from peer %d for term %d\n", rf.me, sid, args.Term)
				} else {
					DPrintf("Raft peer %d received \"no vote\" from peer %d for term %d\n", rf.me, sid, args.Term)
				}
			} else {
				DPrintf("Raft peer %d failed to send RequestVote RPC to peer %d for term %d\n", rf.me, sid, args.Term)
			}
		}(sid)
	}

	go func() {
		wg.Wait()
		close(granted)
	}()

	timeout := time.After(time.Duration(float64(electionTimeout) * 0.6)) // or your desired timeout
	votes := 0
	for {
		select {
		case _, ok := <-granted:
			if !ok {
				// Channel closed, all votes received
				return votes
			}
			votes++
		case <-timeout:
			DPrintf("Raft peer %d vote collection timed out\n", rf.me)
			return votes
		}
	}
}

func (rf *Raft) shouldStartElection() bool {
	return rf.fstate == candidate || (rf.fstate == follower && (rf.hbtime.Load() == nil || time.Since(*rf.hbtime.Load()) > electionTimeout))
}

func (rf *Raft) buildRequestVoeteArgs() *RequestVoteArgs {
	llIndex := 0
	llTerm := 0

	if len(rf.ps.log) > 0 {
		llIndex = len(rf.ps.log) - 1
		llTerm = rf.ps.log[llIndex].term
	}

	return &RequestVoteArgs{
		Term:         rf.ps.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: llIndex,
		LastLogTerm:  llTerm,
	}
}

func (rf *Raft) buildRequestVoteReply() *RequestVoteReply {
	return &RequestVoteReply{
		Term:        -1,
		VoteGranted: false,
	}
}

func (rf *Raft) makeMeLeader() {
	args := rf.buildAppendEntriesHBArgs()
	reply := rf.buildAppendEntriesHBReply()
	go rf.sendHb(args, reply)
	rf.fstate = leader
	DPrintf("Raft peer %d became leader for term %d\n", rf.me, rf.ps.currentTerm)
}

func (rf *Raft) makeMeCandidate() {
	rf.ps.votedFor = &rf.me
	rf.ps.currentTerm += 1
	rf.fstate = candidate
	rf.sendinghb.Store(true)

	DPrintf("Raft peer %d became candidate for term %d\n", rf.me, rf.ps.currentTerm)
}

func (rf *Raft) makeMeFollower(votedFor *int, term int) {
	rf.ps.currentTerm = term // if one server's term is smaller than another's, it updates its term to the larger value
	rf.ps.votedFor = votedFor
	rf.fstate = follower
	rf.sendinghb.Store(false)
	DPrintf("Raft peer %d became follower for term %d\n", rf.me, rf.ps.currentTerm)
}

func (rf *Raft) sendHeartbeats() {
	for !rf.killed() {
		if rf.sendinghb.Load() {

			rf.mu.Lock()
			args := rf.buildAppendEntriesHBArgs()
			reply := rf.buildAppendEntriesHBReply()
			rf.mu.Unlock()

			rf.sendHb(args, reply)
			time.Sleep(hbperiod)
		}
	}
}

func (rf *Raft) sendHb(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	for sid := range rf.peers {
		if sid == rf.me {
			continue
		}
		go func() {
			if ok := rf.sendAppendEntries(sid, args, reply); !ok {
				DPrintf("Raft peer %d failed to send AppendEntries RPC to peer %d\n", rf.me, sid)
			}
		}()
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
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.fstate = follower //when server starts, it begins as a follower
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.ps = initializePersistentState()
	rf.vs = initializeVolatileState()
	rf.lvs = initializeLeaderVolatileState(peers)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.sendHeartbeats()

	go func() {
		http.ListenAndServe("localhost:6060", nil)
	}()
	fmt.Println("Raft peer", rf.me, "started")
	return rf
}

func initializePersistentState() *PersistentState {
	// the structure is initialized by hand before the 3C is done
	return &PersistentState{
		currentTerm: 0,
		votedFor:    nil,
		log:         make([]LogEntry, 0),
	}
}

func initializeVolatileState() *VolatileState {
	return &VolatileState{
		commitIndex: 0,
		lastApplied: 0,
	}
}

func initializeLeaderVolatileState(peers []*labrpc.ClientEnd) *LeaderVolatileState {
	nextIndex := make([]int, len(peers))
	matchIndex := make([]int, len(peers))
	for i := range peers {
		nextIndex[i] = 0 //TODO: what value should be initial?
		matchIndex[i] = 0
	}
	return &LeaderVolatileState{
		nextIndex:  nextIndex,
		matchIndex: matchIndex,
	}
}
