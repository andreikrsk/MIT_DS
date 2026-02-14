package raft

import (
	"slices"
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

// +++++++++++++++++++++++++++++++++++++++++++++++++folowers logic+++++++++++++++++++++++++++++++++++++++++++++++++

// initiated by leaders to replicate log entries; also used as heartbeat by leader or candidate
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	now := time.Now()
	// DPrintf("[server=%d, state=%v, term=%d] called the AppendEntries. args={PrevLogIdx=[%d], PrevLogTerm=[%d], EntriesSize=[%d]}, len(log)=[%d]",
	// rf.me, rf.fstate, rf.ps.currentTerm, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries), len(rf.ps.log))

	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.ps.currentTerm
	reply.Success = false

	// if a server receives a request with a stale term number, it rejects the request
	if args.Term < rf.ps.currentTerm {
		return
	}

	// If the leader’s term (included in its RPC) is at least
	// as large as the candidate’s current term, then the candidate
	// recognizes the leader as legitimate and returns to follower state.
	if rf.fstate == candidate && args.Term == rf.ps.currentTerm {
		rf.makeMeFollower(args.Term)
		rf.persist(false)
		DPrintf("[server=%d, state=%v, term=%d] became follower from a candidate for term [%d] due to AppendEntries request from [%d] with term [%d]",
			rf.me, rf.fstate, rf.ps.currentTerm, rf.ps.currentTerm, args.LeaderId, args.Term)
	}

	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
	if args.Term > rf.ps.currentTerm {
		rf.makeMeFollower(args.Term)
		rf.persist(false)
		DPrintf("[server=%d, state=%v, term=%d] became follower for term [%d] due to higher term in AppendEntries reqest from [%d] with term [%d]",
			rf.me, rf.fstate, rf.ps.currentTerm, rf.ps.currentTerm, args.LeaderId, args.Term)
	}

	//persistent, and global states update
	switch rf.fstate {
	case leader:
		//TODO: what should be done here?
	case candidate:
		//TODO: what should be done here?
	case follower:
		rf.hbtime.Store(&now)

		// the entries are coming should be based on the log entries
		if args.PrevLogIndex > rf.ps.lastSnapshotIndex {
			adjPrevIdx := args.PrevLogIndex
			if rf.ps.lastSnapshotIndex >= 0 {
				adjPrevIdx = args.PrevLogIndex - rf.ps.lastSnapshotIndex - 1
			}

			if adjPrevIdx >= len(rf.ps.log) || adjPrevIdx < 0 {
				DPrintf("[server=%d, state=%v, term=%d] returning false on AppendEntries call. Follower missing prior entries. args={PrevLogIdx=[%d], PrevLogTerm=[%d], EntriesSize=[%d]}, len(log)=[%d]",
					rf.me, rf.fstate, rf.ps.currentTerm, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries), len(rf.ps.log))
				reply.XTerm = -1
				reply.XIndex = -1
				reply.XLen = rf.totalEntreisCount()
				// follower missing prior entries -> reject
				return
			}
			if rf.ps.log[adjPrevIdx].Term != args.PrevLogTerm {
				// mismatch -> reject and provide info for optimization
				reply.XTerm = rf.ps.log[adjPrevIdx].Term
				idx := adjPrevIdx
				//[0,0,1,1]
				//[0,1,2,3]
				for idx > 1 && rf.ps.log[idx-1].Term == reply.XTerm {
					idx--
				}
				reply.XIndex = idx
				reply.XLen = rf.totalEntreisCount()

				DPrintf("[server=%d, state=%v, term=%d] returning false on AppendEntries call. Mismatch. args={PrevLogIdx=[%d], PrevLogTerm=[%d], EntriesSize=[%d]}, len(log)=[%d]",
					rf.me, rf.fstate, rf.ps.currentTerm, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries), len(rf.ps.log))

				return
			}
		} else if args.PrevLogIndex != rf.ps.lastSnapshotIndex {
			// the entries are coming should be base on the snapshot
			DPrintf("[server=%d, state=%v, term=%d] UNREACHABLE STATE 113", rf.me, rf.fstate, rf.ps.currentTerm)
			reply.XTerm = rf.ps.lastSnapshotTerm
			reply.XIndex = rf.ps.lastSnapshotIndex
			reply.XLen = rf.totalEntreisCount()
			DPrintf("[server=%d, state=%v, term=%d] returning false on AppendEntries call. Follower missing prior entries. Returning snaphost info. args={PrevLogIdx=[%d], PrevLogTerm=[%d], EntriesSize=[%d]}, len(log)=[%d]",
				rf.me, rf.fstate, rf.ps.currentTerm, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries), len(rf.ps.log))
			return
		}

		rf.resolveEntriesAppend(args)

		//If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
		if args.LeaderCommit > rf.vs.commitIndex {
			rf.vs.commitIndex = min(args.LeaderCommit, rf.lastEntryIndex())
			DPrintf("[server=%d, state=%v, term=%d] updating commit index to [%d], last applied index is [%d]", rf.me, rf.fstate, rf.ps.currentTerm, rf.vs.commitIndex, rf.vs.lastApplied)
		}

		reply.Success = true
	default:
		panic("unknown state")
	}
	// DPrintf("[server=%d, state=%v, term=%d] returning from the AppendEntries after success logs addition. args={PrevLogIdx=[%d], PrevLogTerm=[%d], EntriesSize=[%d]}, len(log)=[%d]",
	// rf.me, rf.fstate, rf.ps.currentTerm, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries), len(rf.ps.log))
}

func (rf *Raft) resolveEntriesAppend(args *AppendEntriesArgs) {
	if len(args.Entries) == 0 {
		return
	}

	startIdx := args.PrevLogIndex + 1
	if rf.ps.lastSnapshotIndex >= 0 {
		startIdx = args.PrevLogIndex - rf.ps.lastSnapshotIndex // the index right after PrevLogIndex in the log slice
	}
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
	rf.persist(false)
	DPrintf("[server=%d, state=%v, term=%d] done appending log entries. Servers' log size [%v]", rf.me, rf.fstate, rf.ps.currentTerm, len(rf.ps.log))
}

// ---------------------------------------------------------------------------------------------------------------

// +++++++++++++++++++++++++++++++++++++++++++++++++leader's logic+++++++++++++++++++++++++++++++++++++++++++++++++

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

	// DPrintf("[%d, state=%v] starting a command=[%v]", rf.me, rf.fstate, command)
	if rf.fstate != leader {
		// DPrintf("[%d, state=%v] is not a leader, returning", rf.me, rf.fstate)
		return commandIdx, term, isLeader
	}

	// the leader appends the command to its log
	rf.ps.log = append(rf.ps.log, &LogEntry{
		Term:    rf.ps.currentTerm,
		Command: command,
	})
	rf.persist(false)
	commandIdx, term, isLeader = rf.lastEntryIndex(), rf.ps.currentTerm, true
	DPrintf("[server=%d, state=%v, term=%d] started a new command with command=[%v], commandIdx=[%d], term=[%d], isLeader=[%v]",
		rf.me, rf.fstate, rf.ps.currentTerm, command, commandIdx, term, isLeader)

	// then issues AppendEntries RPCs in parallel to each of the other servers
	// if success, leader applies the command to its state machine and returns (made in applyLogToStateMachineJob)
	// DPrintf("[%d, state=%v] returned", rf.me, rf.fstate)
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

func (rf *Raft) sendLogEntries(server int) (sentData bool) {
	for {
		rf.mu.Lock()
		if rf.fstate != leader {
			rf.mu.Unlock()
			return false
		}
		if rf.shouldSendLog(server) {
			args := rf.buildAppendEntriesArgs(server)
			reply := rf.buildAppendEntriesReply()
			// if len(args.Entries) != 0 {
			// DPrintf("[%d, state=%v] calling the sendLogEntries for server [%d]", rf.me, rf.fstate, server)
			// }
			sentData = sentData || len(args.Entries) > 0

			rf.mu.Unlock()

			ok := rf.sendAppendEntriesWithTimeout(server, args, reply)
			// if RPC didn't return (network lost), retry until deadline
			if !ok {
				// rf.mu.Lock()
				// DPrintf("[%d, state=%v] failed to send AppendEntries RPC to peer [%d] for term [%d]", rf.me, rf.fstate, server, args.Term)
				// rf.mu.Unlock()
				continue
			}

			// build args and reply

			// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
			rf.mu.Lock()
			if reply.Term > rf.ps.currentTerm {
				rf.makeMeFollower(reply.Term)
				rf.persist(false)
				DPrintf("[server=%d, state=%v, term=%d] became follower for term [%d] due to higher term in AppendEntries reply from [%d] with term [%d]",
					rf.me, rf.fstate, rf.ps.currentTerm, rf.ps.currentTerm, server, reply.Term)
				rf.mu.Unlock()
				return sentData
			}

			if reply.Term < rf.ps.currentTerm || rf.fstate != leader {
				// stale reply, ignore
				rf.mu.Unlock()
				return sentData
			}

			// If successful: update nextIndex and matchIndex for follower
			if reply.Success {
				// update matchIndex and nextIndex
				newMatch := args.PrevLogIndex + len(args.Entries)
				if len(args.Entries) != 0 {
					DPrintf("[server=%d, state=%v, term=%d] success sending log entries to server [%d]. Updated nextIndex from [%d] to [%d], matchIndex from [%d] to [%d]",
						rf.me, rf.fstate, rf.ps.currentTerm, server, rf.lvs.nextIndex[server], newMatch+1, rf.lvs.matchIndex[server], newMatch)
				}
				rf.lvs.matchIndex[server] = newMatch
				rf.lvs.nextIndex[server] = newMatch + 1
				rf.mu.Unlock()
				return sentData
			}

			rf.updateFollowersNextIndex(server, reply)
		} else {
			args := rf.buildInstallSnapshotArgs()
			reply := rf.buildInstallSnapshotReply()
			DPrintf("[server=%d, state=%v, term=%d] calling the sendInstallSnapshot for server [%d]", rf.me, rf.fstate, rf.ps.currentTerm, server)

			rf.mu.Unlock()

			ok := rf.sendInstallSnapshotWithTimeout(server, args, reply)
			// if RPC didn't return (network lost), retry until deadline
			if !ok {
				// rf.mu.Lock()
				// DPrintf("[%d, state=%v] failed to send AppendEntries RPC to peer [%d] for term [%d]", rf.me, rf.fstate, server, args.Term)
				// rf.mu.Unlock()
				continue
			}

			rf.mu.Lock()
			if reply.Term > rf.ps.currentTerm {
				rf.makeMeFollower(reply.Term)
				rf.persist(false)
				DPrintf("[server=%d, state=%v, term=%d] became follower for term [%d] due to higher term in AppendEntries reply from [%d] with term [%d]",
					rf.me, rf.fstate, rf.ps.currentTerm, rf.ps.currentTerm, server, reply.Term)
				rf.mu.Unlock()
				return true
			}

			if reply.Term < rf.ps.currentTerm || rf.fstate != leader {
				// stale reply, ignore
				rf.mu.Unlock()
				return true
			}

			rf.lvs.matchIndex[server] = args.LastIncludedIndex
			rf.lvs.nextIndex[server] = rf.lvs.matchIndex[server] + 1
			rf.mu.Unlock()
			return true

		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) shouldSendLog(server int) bool {
	return rf.lvs.nextIndex[server] > rf.ps.lastSnapshotIndex
}

func (rf *Raft) updateFollowersNextIndex(server int, reply *AppendEntriesReply) {
	// If AppendEntries fails because of log inconsistency: decrement nextIndex and retry
	// if rf.lvs.nextIndex[server] > 1 {
	// rf.lvs.nextIndex[server]--
	// } else {
	// rf.lvs.nextIndex[server] = 1
	// }

	// optimized version
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
		// DPrintf("[server=%d, state=%v, term=%d] append entries to [%d] timed out", rf.me, rf.fstate, rf.ps.currentTerm, server)
		rf.mu.Unlock()
		return false
	}
}

func (rf *Raft) sendInstallSnapshotWithTimeout(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	done := make(chan bool, 1)
	go func() {
		done <- rf.sendInstallSnapshot(server, args, reply)
		// non-blocking send to avoid goroutine leak if timeout already fired
	}()

	timer := time.NewTimer(rpcTimeout)
	defer timer.Stop()

	select {
	case ok := <-done:
		return ok
	case <-timer.C:
		rf.mu.Lock()
		// DPrintf("[server=%d, state=%v, term=%d] append entries to [%d] timed out", rf.me, rf.fstate, rf.ps.currentTerm, server)
		rf.mu.Unlock()
		return false
	}
}

func (rf *Raft) buildAppendEntriesArgs(server int) *AppendEntriesArgs {
	entries := rf.buildLogEntriesSlice(server)
	prevTerm := rf.findPrevTerm(server)

	next := rf.lvs.nextIndex[server]
	prevIdx := next - 1
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

func (rf *Raft) findPrevTerm(server int) int {
	prevTerm := -1
	adjPrevIdx := rf.trimmedNextIndexFor(server) - 1

	if adjPrevIdx >= 0 && adjPrevIdx < len(rf.ps.log) {
		prevTerm = rf.ps.log[adjPrevIdx].Term
	}

	if rf.ps.lastSnapshotIndex >= 0 && rf.lvs.nextIndex[server]-1 == rf.ps.lastSnapshotIndex {
		prevTerm = rf.ps.lastSnapshotTerm
	}

	return prevTerm
}

func (rf *Raft) buildLogEntriesSlice(server int) []LogEntry {
	adjNext := rf.trimmedNextIndexFor(server)

	// DPrintf("[%d, state=%v] lastSnapshotIndex=[%d], adjNext=[%d], log size=[%d]",
	// rf.me, rf.fstate, rf.ps.lastSnapshotIndex, adjNext, len(rf.ps.log))

	entries := make([]LogEntry, 0, max(0, len(rf.ps.log)-adjNext))
	if adjNext < len(rf.ps.log) {
		for i := adjNext; i < len(rf.ps.log); i++ {
			entries = append(entries, *rf.ps.log[i])
		}
	}

	return entries
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

func (rf *Raft) buildInstallSnapshotArgs() *InstallSnapshotArgs {
	return &InstallSnapshotArgs{
		Term:              rf.ps.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.ps.lastSnapshotIndex,
		LastIncludedTerm:  rf.ps.lastSnapshotTerm,
		Data:              rf.ps.snapshot,
	}
}

func (rf *Raft) buildInstallSnapshotReply() *InstallSnapshotReply {
	return &InstallSnapshotReply{
		Term: -1,
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
		// adjCommitIdx := rf.trimmedCommitIndex()
		expectedMatches := (len(rf.peers) / 2) + 1 // number of servers needed for majority

		topMatchIdx := make([]int, 0, len(rf.peers))                // holds all the match indexes, sorted in desc order
		topMatchIdx = append(topMatchIdx, rf.totalEntreisCount()-1) // leader itself
		for sid := range rf.peers {
			if sid == rf.me {
				continue
			}
			topMatchIdx = append(topMatchIdx, rf.lvs.matchIndex[sid])
		}
		slices.Sort(topMatchIdx)
		slices.Reverse(topMatchIdx)

		lowestMatchIdx := topMatchIdx[expectedMatches-1] // the lowest match index among the top majority
		adjLowestMathcIdx := rf.idxShiftedLeftByLastSnapshotIdx(lowestMatchIdx)

		if adjLowestMathcIdx == len(rf.ps.log) || adjLowestMathcIdx < 0 {
			DPrintf("[server=%d, state=%v, term=%d] topMatchIdx=[%v], lowestMatchIdx=[%d], adjustedLowestMatchIdx=[%d], log size=[%d], lastSnapshotIndex=[%d]",
				rf.me, rf.fstate, rf.ps.currentTerm, topMatchIdx, lowestMatchIdx, adjLowestMathcIdx, len(rf.ps.log), rf.ps.lastSnapshotIndex)
		}

		isTheLowesMatchInTheCurrentTerm := (lowestMatchIdx == rf.ps.lastSnapshotIndex && rf.ps.lastSnapshotTerm == rf.ps.currentTerm) ||
			(adjLowestMathcIdx >= 0 && rf.ps.log[adjLowestMathcIdx].Term == rf.ps.currentTerm)

		if lowestMatchIdx > rf.vs.commitIndex && isTheLowesMatchInTheCurrentTerm {
			rf.vs.commitIndex = lowestMatchIdx
			DPrintf("[server=%d, state=%v, term=%d] commitIndex updated to [%d], last applied index [%d]",
				rf.me, rf.fstate, rf.ps.currentTerm, rf.vs.commitIndex, rf.vs.lastApplied)
		}

		rf.mu.Unlock()
	}
}

// ------------------------------------------------leader's logic------------------------------------------------

// ++++++++++++++++++++++++++++++++++++++++++++++++common logic++++++++++++++++++++++++++++++++++++++++++++++++
func (rf *Raft) applyLogToStateMachineJob() {
	for !rf.killed() {
		// If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine
		for {
			rf.mu.Lock()
			if rf.vs.commitIndex <= rf.vs.lastApplied {
				rf.mu.Unlock()
				break
			}
			realLastApplied := rf.vs.lastApplied
			adjLastApplied := rf.trimmedLastApplied()

			DPrintf("[server=%d, state=%v, term=%d] trying to send a command from apply job. commitIndex=[%d], realLastApplied=[%d], adjLastApplied=[%d], log size=[%d]",
				rf.me, rf.fstate, rf.ps.currentTerm, rf.vs.commitIndex, rf.vs.lastApplied, adjLastApplied, len(rf.ps.log))

			m := raftapi.ApplyMsg{
				CommandValid: true,
				Command:      rf.ps.log[adjLastApplied+1].Command, // the index of the entry to be send is lastApplied + 1
				CommandIndex: realLastApplied + 1,                 // its real index if no snapshotting happened
			}
			DPrintf("[server=%d, state=%v, term=%d] sending a msg to the applyChannel. msg={CommandValid [%v], Command [%v], CommandIndex [%v]}",
				rf.me, rf.fstate, rf.ps.currentTerm, m.CommandValid, m.Command, m.CommandIndex)
			rf.mu.Unlock()

			rf.applyCh <- m

			rf.mu.Lock()
			rf.vs.lastApplied++
			DPrintf("[server=%d, state=%v, term=%d] sent a msg to the applyChannel. msg={CommandValid [%v], Command [%v], CommandIndex [%v]}",
				rf.me, rf.fstate, rf.ps.currentTerm, m.CommandValid, m.Command, m.CommandIndex)
			rf.mu.Unlock()
		}
		// rf.mu.Unlock()
		time.Sleep(time.Duration(20) * time.Millisecond)
	}
}

// -------------------------------------------------common logic-------------------------------------------------
