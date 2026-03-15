package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	applyCh   chan raftapi.ApplyMsg
	me        int   // this peer's index into peers[]
	dead      int32 // set by Kill()

	rand   *rand.Rand
	fstate serverState // state of this peer

	// election related state
	electionTimer           *time.Timer
	notifyKilledElectionJob chan struct{}
	// replecated the log related state
	replicateNotifyChMap           map[int]chan struct{} // a channer per server
	notifyKilledReplicateLogJobMap map[int]chan struct{}
	// commit related state
	commitNotifyCh        chan struct{}
	notifyKilledCommitJob chan struct{}
	// apply log to state machine related state
	applyNotifyCh                         chan struct{}
	notifyKilledApplyLogToStateMachineJob chan struct{}

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	ps  *PersistentState
	vs  *VolatileState
	lvs *LeaderVolatileState
}

type PersistentState struct {
	// each server stores a current term number
	currentTerm       int
	votedFor          *int
	log               []*LogEntry
	lastSnapshotIndex int
	lastSnapshotTerm  int
	snapshot          []byte
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
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	// DPrintf("[server=%d, state=%v, term=%d] GetState called, returning term [%d] and isLeader [%v]",
	// rf.me, rf.fstate, rf.ps.currentTerm, rf.ps.currentTerm, rf.fstate == leader)
	return rf.ps.currentTerm, rf.fstate == leader
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
	rf.mu.RLock()
	DPrintf("[server=%d, state=%v, term=%d] Kill called", rf.me, rf.fstate, rf.ps.currentTerm)
	rf.mu.RUnlock()
	atomic.StoreInt32(&rf.dead, 1)
	rf.notifyListenersAboutKilled()
	// Your code here, if desired.
}

func (rf *Raft) notifyListenersAboutKilled() {
	select {
	case rf.notifyKilledElectionJob <- struct{}{}:
	default:
	}

	select {
	case rf.notifyKilledCommitJob <- struct{}{}:
	default:
	}

	select {
	case rf.notifyKilledApplyLogToStateMachineJob <- struct{}{}:
	default:
	}

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}

		rf.mu.RLock()
		notifyKilledCh := rf.notifyKilledReplicateLogJobMap[peer]
		rf.mu.RUnlock()

		select {
		case notifyKilledCh <- struct{}{}:
		default:
		}
	}
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

	rf.rand = rand.New(rand.NewSource(time.Now().UnixNano()))

	rf.electionTimer = time.NewTimer(time.Duration(0))
	rf.notifyKilledElectionJob = make(chan struct{}, 1)

	rf.replicateNotifyChMap = make(map[int]chan struct{})
	rf.notifyKilledReplicateLogJobMap = make(map[int]chan struct{})

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		rf.replicateNotifyChMap[peer] = make(chan struct{}, 1)
		rf.notifyKilledReplicateLogJobMap[peer] = make(chan struct{}, 1)
	}

	rf.commitNotifyCh = make(chan struct{}, 1)
	rf.commitNotifyCh <- struct{}{}
	rf.notifyKilledCommitJob = make(chan struct{}, 1)

	rf.applyNotifyCh = make(chan struct{}, 1)
	rf.applyNotifyCh <- struct{}{}
	rf.notifyKilledApplyLogToStateMachineJob = make(chan struct{}, 1)

	// initialize from state persisted before a crash
	rf.readRfState()
	rf.readSnapshot()

	// Your initialization code here (3A, 3B, 3C).
	rf.initializePersistentState()
	rf.initializeVolatileState()
	rf.initializeLeaderVolatileState(peers)

	if rf.ps.lastSnapshotIndex != -1 && len(rf.ps.snapshot) == 0 {
		panic("lastSnapshotIndex is not -1, but missing snapshot")
	}

	if rf.ps.lastSnapshotIndex == -1 && len(rf.ps.snapshot) != 0 {
		panic("lastSnapshotIndex is -1, but snapshot exists")
	}

	if rf.ps.snapshot != nil {
		DPrintf("[server=%d, state=%v, term=%d] on service start applying snapshot with lastSnapshotIndex %d, lastSnapshotTerm %d, snapshot size %d",
			rf.me, rf.fstate, rf.ps.currentTerm, rf.ps.lastSnapshotIndex, rf.ps.lastSnapshotTerm, len(rf.ps.snapshot))
		rf.vs.lastApplied = rf.ps.lastSnapshotIndex - 1
		rf.advanceCommitIndex(rf.ps.lastSnapshotIndex)
	}

	go func() {
		http.ListenAndServe("localhost:6060", nil)
	}()

	// start ticker goroutine to start elections
	go rf.electionsJob()
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.replicateLogJob(i)
	}
	go rf.commitJob()
	go rf.applyLogToStateMachineJob()

	// rf.mu.Lock()
	DPrintf("[server=%d, state=%v, term=%d] started", rf.me, rf.fstate, rf.ps.currentTerm)
	// rf.mu.Unlock()
	return rf
}

func (rf *Raft) initializePersistentState() {
	// the structure is initialized by hand if not state was persisted
	if rf.ps == nil {
		rf.ps = &PersistentState{
			currentTerm:       0,
			votedFor:          nil,
			log:               make([]*LogEntry, 0),
			lastSnapshotTerm:  -1,
			lastSnapshotIndex: -1,
			snapshot:          nil,
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
		nextIndex[i] = rf.lastEntryIndex() + 1 //initialized to leader last log index + 1
		matchIndex[i] = 0                      //initialized to 0, increases monotonically
	}
	rf.lvs = &LeaderVolatileState{
		nextIndex:  nextIndex,
		matchIndex: matchIndex,
	}
}

// Index calculation methods

func (rf *Raft) lastEntryTerm() int {
	llIndex := 0
	llTerm := 0

	// If log is not empty, it has the most up to date entry
	if len(rf.ps.log) > 0 {
		llIndex = len(rf.ps.log) - 1
		llTerm = rf.ps.log[llIndex].Term
	} else if rf.ps.lastSnapshotIndex >= 0 {
		llTerm = rf.ps.lastSnapshotTerm
	}

	return llTerm
}

func (rf *Raft) lastEntryIndex() int {
	return rf.totalEntreisCount() - 1
}

func (rf *Raft) firstAfterTheLastLogEntryIndex() int {
	return rf.totalEntreisCount()
}

func (rf *Raft) trimmedLastApplied() int {
	return rf.idxShiftedLeftByLastSnapshotIdx(rf.vs.lastApplied)
}

func (rf *Raft) trimmedNextIndexFor(peer int) int {
	return rf.idxShiftedLeftByLastSnapshotIdx(rf.lvs.nextIndex[peer])
}

func (rf *Raft) totalEntreisCount() int {
	return len(rf.ps.log) + (rf.ps.lastSnapshotIndex + 1) // len of the log + len of the log in the snapshot
}

func (rf *Raft) idxShiftedLeftByLastSnapshotIdx(index int) int {
	return index - (rf.ps.lastSnapshotIndex + 1)
}
