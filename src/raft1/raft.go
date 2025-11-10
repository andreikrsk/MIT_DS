package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"

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

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
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

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// initiated by leaders to replicate log entries; also used as heartbeat by leader or candidate
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	now := time.Now()

	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.ps.currentTerm
	reply.Success = false

	// if a server receives a request with a stale term number, it rejects the request
	if args.Term < rf.ps.currentTerm {
		return
	}

	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
	if args.Term > rf.ps.currentTerm {
		rf.makeMeFollower(args.Term)
		DPrintf("[%d, state=%v] became follower for term [%d] due to higher term in AppendEntries reqest from [%d] with term [%d]", rf.me, rf.fstate, rf.ps.currentTerm, args.LeaderId, args.Term)
	}

	//persistent, and global states update
	switch rf.fstate {
	case leader:
		//TODO: what should be done here?
	case candidate:
		//TODO: what should be done here?
	case follower:
		rf.hbtime.Store(&now)

		if args.PrevLogIndex >= 0 {
			if args.PrevLogIndex >= len(rf.ps.log) {
				DPrintf("[%d, state=%v] Returning false on AppendEntries call. Follower missing prior entries. args={PrevLogIdx=[%d], PrevLogTerm=[%d], EntriesSize=[%d]}, len(log)=[%d]",
					rf.me, rf.fstate, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries), len(rf.ps.log))

				// follower missing prior entries -> reject
				return
			}
			if rf.ps.log[args.PrevLogIndex] == nil || rf.ps.log[args.PrevLogIndex].Term != args.PrevLogTerm {
				// mismatch -> reject
				DPrintf("[%d, state=%v] Returning false on AppendEntries call. Mismatch. args={PrevLogIdx=[%d], PrevLogTerm=[%d], EntriesSize=[%d]}, len(log)=[%d]",
					rf.me, rf.fstate, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries), len(rf.ps.log))
				return
			}
		}

		rf.resolveEntriesAppend(args)

		//If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
		if args.LeaderCommit > rf.vs.commitIndex {
			rf.vs.commitIndex = min(args.LeaderCommit, len(rf.ps.log)-1)
			DPrintf("[%d, state=%v] updating commit index to [%d], last applied index is [%d]", rf.me, rf.fstate, rf.vs.commitIndex, rf.vs.lastApplied)
		}

		reply.Success = true
	default:
		panic("unknown state")
	}

}

func (rf *Raft) resolveEntriesAppend(args *AppendEntriesArgs) {
	if len(args.Entries) == 0 {
		return
	}

	startIdx := args.PrevLogIndex + 1

	for i := 0; i < len(args.Entries); i++ {
		idx := startIdx + i
		incoming := args.Entries[i]

		if idx < len(rf.ps.log) {
			if rf.ps.log[idx].Term != incoming.Term {
				// conflict -> truncate and append remainder
				rf.ps.log = rf.ps.log[:idx]
				for j := i; j < len(args.Entries); j++ {
					e := args.Entries[j] // copy value
					rf.ps.log = append(rf.ps.log, &e)
				}
				break
			}
			// terms match -> entry already present, continue
		} else {
			// past end -> append remaining incoming entries
			for j := i; j < len(args.Entries); j++ {
				e := args.Entries[j]
				rf.ps.log = append(rf.ps.log, &e)
			}
			break
		}
	}
	DPrintf("[%d, state=%v] done applying log. Servers' log size [%v]", rf.me, rf.fstate, len(rf.ps.log))
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
	commandIdx, term, isLeader := -1, -1, false
	rf.mu.Lock()
	DPrintf("[%d, state=%v] starting a command=[%v]", rf.me, rf.fstate, command)
	if rf.fstate != leader {
		DPrintf("[%d, state=%v] is not a leader, returning", rf.me, rf.fstate)
		rf.mu.Unlock()
		return commandIdx, term, isLeader
	}

	// the leader appends the command to its log
	rf.ps.log = append(rf.ps.log, &LogEntry{
		Term:    rf.ps.currentTerm,
		Command: command,
	})

	commandIdx, term, isLeader = len(rf.ps.log)-1, rf.ps.currentTerm, true
	rf.mu.Unlock()

	// then issues AppendEntries RPCs in parallel to each of the other servers
	// if success, leader applies the command to its state machine and returns (made in applyLogToStateMachineJob)
	rf.mu.Lock()
	DPrintf("[%d, state=%v] returned", rf.me, rf.fstate)
	rf.mu.Unlock()
	return commandIdx, term, isLeader
}

func (rf *Raft) replicateLogJob(server int) {
	backoffBase := 100 * time.Millisecond

	for !rf.killed() {
		rf.sendLogEntries(server)

		time.Sleep(backoffBase)
	}
}

func (rf *Raft) sendLogEntries(server int) {
	for {
		rf.mu.Lock()
		if rf.fstate != leader {
			rf.mu.Unlock()
			return
		}
		// build args and reply
		args := rf.buildAppendEntriesArgs(server)
		reply := rf.buildAppendEntriesReply()
		if len(args.Entries) != 0 {
			DPrintf("[%d, state=%v] calling the sendLogEntries for server [%d]", rf.me, rf.fstate, server)
		}
		rf.mu.Unlock()

		ok := rf.sendAppendEntries(server, args, reply)
		// if RPC didn't return (network lost), retry until deadline
		if !ok {
			continue
		}

		// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
		rf.mu.Lock()
		if reply.Term > rf.ps.currentTerm {
			rf.makeMeFollower(reply.Term)
			DPrintf("[%d, state=%v] became follower for term [%d] due to higher term in AppendEntries reply from [%d] with term [%d]", rf.me, rf.fstate, rf.ps.currentTerm, server, reply.Term)
			rf.mu.Unlock()
			return
		}

		// If successful: update nextIndex and matchIndex for follower
		if reply.Success {
			// update matchIndex and nextIndex
			newMatch := args.PrevLogIndex + len(args.Entries)
			if len(args.Entries) != 0 {
				DPrintf("[%d, state=%v] success sending log entries to server [%d]. Updated nextIndex from [%d] to [%d], matchIndex from [%d] to [%d]",
					rf.me, rf.fstate, server, rf.lvs.nextIndex[server], newMatch+1, rf.lvs.matchIndex[server], newMatch)
			}
			rf.lvs.matchIndex[server] = newMatch
			rf.lvs.nextIndex[server] = newMatch + 1
			rf.mu.Unlock()
			return
		}

		// If AppendEntries fails because of log inconsistency: decrement nextIndex and retry
		if rf.lvs.nextIndex[server] > 1 {
			rf.lvs.nextIndex[server]--
		} else {
			rf.lvs.nextIndex[server] = 1
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) buildAppendEntriesArgs(server int) *AppendEntriesArgs {
	// recompute PrevLogIndex/PrevLogTerm and entries according to nextIndex
	next := rf.lvs.nextIndex[server]
	prevIdx := next - 1
	var prevTerm int
	if prevIdx >= 0 && prevIdx < len(rf.ps.log) {
		prevTerm = rf.ps.log[prevIdx].Term
	} else {
		prevTerm = -1
	}
	entries := make([]LogEntry, 0, max(0, len(rf.ps.log)-next))
	if next < len(rf.ps.log) {
		for i := next; i < len(rf.ps.log); i++ {
			entries = append(entries, *rf.ps.log[i])
		}
	}

	// build args and reply
	return &AppendEntriesArgs{
		Term:         rf.ps.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevIdx,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: rf.vs.commitIndex,
	}
}

func (rf *Raft) buildAppendEntriesReply() *AppendEntriesReply {
	return &AppendEntriesReply{
		Term:    -1,
		Success: false,
	}
}

func (rf *Raft) commitJob() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.fstate != leader {
			rf.mu.Unlock()
			time.Sleep(20 * time.Millisecond)
			continue
		}

		nextCommitIdx := rf.vs.commitIndex + 1
		expectedMatches := (len(rf.peers) / 2) + 1
		matches := 1 // leader itself
		for sid := range rf.peers {
			if sid == rf.me {
				continue
			}
			if rf.lvs.matchIndex[sid] >= nextCommitIdx {
				matches++
			}
		}
		if matches >= expectedMatches {
			rf.vs.commitIndex = nextCommitIdx
			DPrintf("[%d, state=%v] commitIndex updated to [%d], last applied index [%d]", rf.me, rf.fstate, rf.vs.commitIndex, rf.vs.lastApplied)
		}

		rf.mu.Unlock()
	}
}

func (rf *Raft) applyLogToStateMachineJob() {
	for !rf.killed() {
		rf.mu.Lock()
		// If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine
		for rf.vs.commitIndex > rf.vs.lastApplied {
			m := raftapi.ApplyMsg{
				CommandValid: true,
				Command:      rf.ps.log[rf.vs.lastApplied+1].Command,
				CommandIndex: rf.vs.lastApplied + 1,
			}
			DPrintf("[%d, state=%v] sending a msg to the applyChannel. msg={CommandValid [%v], Command [%v], CommandIndex [%v]}",
				rf.me, rf.fstate, m.CommandValid, m.Command, m.CommandIndex)
			rf.applyCh <- m
			rf.vs.lastApplied++
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(20) * time.Millisecond)
	}
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
	rf.peers = peers
	rf.persister = persister
	rf.applyCh = applyCh
	rf.fstate = follower //when server starts, it begins as a follower
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.ps = initializePersistentState()
	rf.ps.log = append(rf.ps.log, &LogEntry{0, nil}) //dummy log entry at index 0
	rf.vs = initializeVolatileState()
	rf.lvs = initializeLeaderVolatileState(peers)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

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

	rf.mu.Lock()
	DPrintf("[%d, state=%v] started", rf.me, rf.fstate)
	rf.mu.Unlock()
	return rf
}

func initializePersistentState() *PersistentState {
	// the structure is initialized by hand before the 3C is done
	return &PersistentState{
		currentTerm: 0,
		votedFor:    nil,
		log:         make([]*LogEntry, 0),
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
		nextIndex[i] = 1  //initialized to leader last log index + 1
		matchIndex[i] = 0 //initialized to 0, increases monotonically
	}
	return &LeaderVolatileState{
		nextIndex:  nextIndex,
		matchIndex: matchIndex,
	}
}
