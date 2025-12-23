package raft

import (
	"time"

	"6.5840/raftapi"
)

const rpcTimeout = 50 * time.Millisecond

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

	// optimization
	// term in the conflicting entry (if any)
	XTerm int
	// index of first entry with that term (if any)
	XIndex int
	// log length
	XLen int
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

	//  If the leader’s term (included in its RPC) is at least
	// as large as the candidate’s current term, then the candidate
	// recognizes the leader as legitimate and returns to follower state.
	if rf.fstate == candidate && args.Term == rf.ps.currentTerm {
		rf.makeMeFollower(args.Term)
		rf.persist()
		DPrintf("[%d, state=%v] became follower from a candidate for term [%d] due to AppendEntries request from [%d] with term [%d]", rf.me, rf.fstate, rf.ps.currentTerm, args.LeaderId, args.Term)
	}

	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
	if args.Term > rf.ps.currentTerm {
		rf.makeMeFollower(args.Term)
		rf.persist()
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
				DPrintf("[%d, state=%v] returning false on AppendEntries call. Follower missing prior entries. args={PrevLogIdx=[%d], PrevLogTerm=[%d], EntriesSize=[%d]}, len(log)=[%d]",
					rf.me, rf.fstate, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries), len(rf.ps.log))
				reply.XTerm = -1
				reply.XIndex = -1
				reply.XLen = len(rf.ps.log)
				// follower missing prior entries -> reject
				return
			}
			if rf.ps.log[args.PrevLogIndex].Term != args.PrevLogTerm {
				// mismatch -> reject and provide info for optimization
				reply.XTerm = rf.ps.log[args.PrevLogIndex].Term
				idx := args.PrevLogIndex
				//[0,0,1,1]
				//[0,1,2,3]
				for idx > 1 && rf.ps.log[idx-1].Term == reply.XTerm {
					idx--
				}
				reply.XIndex = idx
				reply.XLen = len(rf.ps.log)

				DPrintf("[%d, state=%v] returning false on AppendEntries call. Mismatch. args={PrevLogIdx=[%d], PrevLogTerm=[%d], EntriesSize=[%d]}, len(log)=[%d]",
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
	// rewrite to make the change atomic. create a copy, and replace the log ?
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
	rf.persist()
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
	defer rf.mu.Unlock()

	DPrintf("[%d, state=%v] starting a command=[%v]", rf.me, rf.fstate, command)
	if rf.fstate != leader {
		DPrintf("[%d, state=%v] is not a leader, returning", rf.me, rf.fstate)
		return commandIdx, term, isLeader
	}

	// the leader appends the command to its log
	rf.ps.log = append(rf.ps.log, &LogEntry{
		Term:    rf.ps.currentTerm,
		Command: command,
	})
	rf.persist()

	commandIdx, term, isLeader = len(rf.ps.log)-1, rf.ps.currentTerm, true

	// then issues AppendEntries RPCs in parallel to each of the other servers
	// if success, leader applies the command to its state machine and returns (made in applyLogToStateMachineJob)
	DPrintf("[%d, state=%v] returned", rf.me, rf.fstate)
	return commandIdx, term, isLeader
}

func (rf *Raft) replicateLogJob(server int) {
	backoffBase := 100 * time.Millisecond

	for !rf.killed() {
		sentEntries := rf.sendLogEntries(server)
		if !sentEntries {
			time.Sleep(backoffBase)
		}
	}
}

func (rf *Raft) sendLogEntries(server int) (sentEntries bool) {
	for {
		rf.mu.Lock()
		if rf.fstate != leader {
			rf.mu.Unlock()
			return false
		}
		// build args and reply
		args := rf.buildAppendEntriesArgs(server)
		reply := rf.buildAppendEntriesReply()
		if len(args.Entries) != 0 {
			DPrintf("[%d, state=%v] calling the sendLogEntries for server [%d]", rf.me, rf.fstate, server)
		}
		sentEntries = sentEntries || len(args.Entries) > 0

		rf.mu.Unlock()

		ok := rf.sendAppendEntriesWithTimeout(server, args, reply)
		// if RPC didn't return (network lost), retry until deadline
		if !ok {
			// rf.mu.Lock()
			// DPrintf("[%d, state=%v] failed to send AppendEntries RPC to peer [%d] for term [%d]", rf.me, rf.fstate, server, args.Term)
			// rf.mu.Unlock()
			continue
		}

		// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
		rf.mu.Lock()
		if reply.Term > rf.ps.currentTerm {
			rf.makeMeFollower(reply.Term)
			rf.persist()
			DPrintf("[%d, state=%v] became follower for term [%d] due to higher term in AppendEntries reply from [%d] with term [%d]", rf.me, rf.fstate, rf.ps.currentTerm, server, reply.Term)
			rf.mu.Unlock()
			return sentEntries
		}

		if reply.Term < rf.ps.currentTerm || rf.fstate != leader {
			// stale reply, ignore
			rf.mu.Unlock()
			return sentEntries
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
			return sentEntries
		}

		// If AppendEntries fails because of log inconsistency: decrement nextIndex and retry
		// if rf.lvs.nextIndex[server] > 1 {
		// rf.lvs.nextIndex[server]--
		// } else {
		// rf.lvs.nextIndex[server] = 1
		// }

		//optimized version
		// followers log is too short
		if reply.XTerm == -1 {
			rf.lvs.nextIndex[server] = reply.XLen
		} else {
			last := rf.lastIndexOfTerm(reply.XTerm)
			if last > 0 {
				rf.lvs.nextIndex[server] = last + 1
			} else {
				rf.lvs.nextIndex[server] = reply.XIndex
			}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) lastIndexOfTerm(term int) int {
	for i := len(rf.ps.log) - 1; i > 0; i-- {
		if rf.ps.log[i].Term == term {
			return i
		}
	}
	return 0
}

// can be further optimized by first sending a single entry. find agreement point, only after that send the rest of the log
// as the current approach works in O(len(log)^2) time
func (rf *Raft) sendAppendEntriesWithTimeout(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	done := make(chan bool, 1)
	go func() {
		done <- rf.sendAppendEntries(server, args, reply)
		// non-blocking send to avoid goroutine leak if timeout already fired
	}()

	timer := time.NewTimer(rpcTimeout)
	defer timer.Stop()

	select {
	case ok := <-done:
		return ok
	case <-timer.C:
		rf.mu.Lock()
		DPrintf("[%d, state=%v] append entries to [%d] timed out", rf.me, rf.fstate, server)
		rf.mu.Unlock()
		return false
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

		XTerm:  -1,
		XIndex: -1,
		XLen:   -1,
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

		// Raft never commits log entries from previous terms by counting replicas. Only log entries from the leader’s current
		// term are committed by counting replicas;
		nextCommitIdx := rf.vs.commitIndex + 1
		for len(rf.ps.log) > nextCommitIdx && rf.ps.log[nextCommitIdx].Term < rf.ps.currentTerm {
			DPrintf("[%d, state=%v] updating commit on the entry with idx [%d] without logs inspection. as it has out of date term [%d], current term [%d]",
				rf.me, rf.fstate, nextCommitIdx, rf.ps.log[nextCommitIdx].Term, rf.ps.currentTerm)
			nextCommitIdx++
		}

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
		// If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine
		for {
			rf.mu.Lock()
			if rf.vs.commitIndex <= rf.vs.lastApplied {
				rf.mu.Unlock()
				break
			}
			m := raftapi.ApplyMsg{
				CommandValid: true,
				Command:      rf.ps.log[rf.vs.lastApplied+1].Command,
				CommandIndex: rf.vs.lastApplied + 1,
			}
			DPrintf("[%d, state=%v] sending a msg to the applyChannel. msg={CommandValid [%v], Command [%v], CommandIndex [%v]}",
				rf.me, rf.fstate, m.CommandValid, m.Command, m.CommandIndex)
			rf.mu.Unlock()

			rf.applyCh <- m

			rf.mu.Lock()
			rf.vs.lastApplied++
			rf.mu.Unlock()
		}
		// rf.mu.Unlock()
		time.Sleep(time.Duration(20) * time.Millisecond)
	}
}
