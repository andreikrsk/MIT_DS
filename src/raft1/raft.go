package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	"bytes"

	"net/http"
	_ "net/http/pprof"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu sync.Mutex // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	applyCh   chan raftapi.ApplyMsg
	me        int   // this peer's index into peers[]
	dead      int32 // set by Kill()

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
	// each server stores a current term number
	currentTerm int
	votedFor    *int
	log         []*LogEntry
}

type LogEntry struct {
	Term    int
	Command interface{}
}

type VolatileState struct {
	// The index of the highest log entry known to be committed
	commitIndex int
	// The index of the highest log entry known to be applied to the state machine
	lastApplied int
}

type LeaderVolatileState struct {
	// The leader maintains a nextIndex for each follower, which is the index of the next log entry the leader will send to that follower
	nextIndex []int
	// The leader maintains a matchIndex for each follower, which is the index of the highest log entry known to be replicated on that follower
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[%d, state=%v] GetState called, returning term [%d] and isLeader [%v]", rf.me, rf.fstate, rf.ps.currentTerm, rf.fstate == leader)
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.ps.currentTerm)
	if rf.ps.votedFor == nil {
		e.Encode(-1)
	} else {
		e.Encode(*rf.ps.votedFor)
	}
	e.Encode(rf.ps.log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var ct int
	var vf int
	var l []*LogEntry
	if d.Decode(&ct) != nil ||
		d.Decode(&vf) != nil ||
		d.Decode(&l) != nil {
		DPrintf("Error reading persistent state")
	} else {
		rf.ps = &PersistentState{
			currentTerm: ct,
			log:         l,
		}
		if vf != -1 {
			rf.ps.votedFor = &vf
		}
	}
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
	// rf.commitCond = sync.NewCond(&rf.mu)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.peers = peers
	rf.persister = persister
	rf.applyCh = applyCh
	rf.fstate = follower //when server starts, it begins as a follower
	rf.me = me

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// Your initialization code here (3A, 3B, 3C).
	rf.initializePersistentState()
	rf.initializeVolatileState()
	rf.initializeLeaderVolatileState(peers)

	go func() {
		http.ListenAndServe("localhost:6060", nil)
	}()

	// start ticker goroutine to start elections
	go rf.ticker()
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.replicateLogJob(i)
	}
	go rf.commitJob()
	go rf.applyLogToStateMachineJob()

	// rf.mu.Lock()
	DPrintf("[%d, state=%v] started", rf.me, rf.fstate)
	// rf.mu.Unlock()
	return rf
}

func (rf *Raft) initializePersistentState() {
	// the structure is initialized by hand if not state was persisted
	if rf.ps == nil {
		rf.ps = &PersistentState{
			currentTerm: 0,
			votedFor:    nil,
			log:         make([]*LogEntry, 0),
		}
		rf.ps.log = append(rf.ps.log, &LogEntry{0, nil}) //dummy log entry at index 0
	}
}

func (rf *Raft) initializeVolatileState() {
	rf.vs = &VolatileState{
		commitIndex: 0,
		lastApplied: 0,
	}
}

func (rf *Raft) initializeLeaderVolatileState(peers []*labrpc.ClientEnd) {
	nextIndex := make([]int, len(peers))
	matchIndex := make([]int, len(peers))
	for i := range peers {
		nextIndex[i] = len(rf.ps.log) //initialized to leader last log index + 1
		matchIndex[i] = 0             //initialized to 0, increases monotonically
	}
	rf.lvs = &LeaderVolatileState{
		nextIndex:  nextIndex,
		matchIndex: matchIndex,
	}
}
