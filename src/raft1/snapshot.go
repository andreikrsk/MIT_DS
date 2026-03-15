package raft

type InstallSnapshotArgs struct {
	Term              int    // leader's term
	LeaderId          int    // so follower can redirect clients
	LastIncludedIndex int    // the snapshot replaces all entries up throughand including this index
	LastIncludedTerm  int    // term of lastIncludedIndex
	Offset            int64  // byte offset where chunk is positioned in thesnapshot file
	Data              []byte // raw bytes of the snapshot chunk, starting atoffset
	Done              bool   // true if this is the last chunk
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// for a follower to install a snapshot sent by the leader
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("[server=%d, state=%v, term=%d] called InstallSnapshot with LastIncludedIndex %d",
		rf.me, rf.fstate, rf.ps.currentTerm, args.LastIncludedIndex)
	// Your code here (3D).

	reply.Term = rf.ps.currentTerm

	// if a server receives a request with a stale term number, it rejects the request
	if args.Term < rf.ps.currentTerm {
		// rf.mu.Unlock()
		return
	}

	// If the leader’s term (included in its RPC) is at least
	// as large as the candidate’s current term, then the candidate
	// recognizes the leader as legitimate and returns to follower state.
	if rf.fstate == candidate && args.Term == rf.ps.currentTerm {
		rf.makeMeFollower(args.Term)
		rf.persist()
		DPrintf("[server=%d, state=%v, term=%d] became follower from a candidate for the current term due to AppendEntries request from [%d] with term [%d]",
			rf.me, rf.fstate, rf.ps.currentTerm, args.LeaderId, args.Term)
	}

	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
	if args.Term > rf.ps.currentTerm {
		rf.makeMeFollower(args.Term)
		rf.persist()
		DPrintf("[server=%d, state=%v, term=%d] became follower for the current term due to higher term in AppendEntries reqest from [%d] with term [%d]",
			rf.me, rf.fstate, rf.ps.currentTerm, args.LeaderId, args.Term)
	}

	if args.LastIncludedIndex <= rf.ps.lastSnapshotIndex {
		// snapshot is older than existing snapshot. we can move only forward
		DPrintf("[server=%d, state=%v, term=%d] ignoring InstallSnapshot call with LastIncludedIndex %d because it's <= logOffset %d",
			rf.me, rf.fstate, rf.ps.currentTerm, args.LastIncludedIndex, rf.ps.lastSnapshotIndex)
		// rf.mu.Unlock()
		return
	}

	// The index argument indicates the highest log entry that's reflected in the snapshot.
	// Raft should discard its log entries before that point.
	// You'll need to revise your Raft code to operate while storing only the tail of the log.
	rf.updateSnapshot(args.LastIncludedIndex, args.LastIncludedTerm, args.Data)
	// send the snapshot to the service (e.g., a key/value server) if the index > lastApplied

	// var msg *raftapi.ApplyMsg
	if args.LastIncludedIndex > rf.vs.lastApplied {
		rf.vs.lastApplied = args.LastIncludedIndex - 1
		rf.advanceCommitIndex(args.LastIncludedIndex)
		// msg = rf.buildApplySnapshotMsg(args.LastIncludedIndex, args.LastIncludedTerm, args.Data)
		DPrintf("[server=%d, state=%v, term=%d] applying snapshot to state machine with lastIncludedIndex %d",
			rf.me, rf.fstate, rf.ps.currentTerm, args.LastIncludedIndex)
		// rf.mu.Unlock()
	}

}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[server=%d, state=%v, term=%d] called Snapshot with index %d. log size is %d, last snapshot index is %d",
		rf.me, rf.fstate, rf.ps.currentTerm, index, len(rf.ps.log), rf.ps.lastSnapshotIndex)

	if index <= rf.ps.lastSnapshotIndex {
		// snapshot is older than existing snapshot. we can move only forward
		DPrintf("[server=%d, state=%v, term=%d] ignoring Snapshot call with index %d because it's <= logOffset %d",
			rf.me, rf.fstate, rf.ps.currentTerm, index, rf.ps.lastSnapshotIndex)
		return
	}

	rf.updateSnapshot(index, rf.ps.log[index-rf.ps.lastSnapshotIndex-1].Term, snapshot)
}

func (rf *Raft) updateSnapshot(lastIncludedIndex int, lastIncludedTerm int, snapshot []byte) {
	rf.ps.snapshot = snapshot
	rf.ps.lastSnapshotTerm = lastIncludedTerm
	rf.storeLogFrom(lastIncludedIndex - rf.ps.lastSnapshotIndex)
	rf.ps.lastSnapshotIndex = lastIncludedIndex
	rf.persist()
}

func (rf *Raft) storeLogFrom(index int) {
	// discard log entries up to and including index
	newLog := make([]*LogEntry, 0)
	for idx := index; idx < len(rf.ps.log); idx++ {
		newLog = append(newLog, rf.ps.log[idx])
	}
	rf.ps.log = newLog
}
