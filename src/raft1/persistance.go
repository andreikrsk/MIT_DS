package raft

import (
	"bytes"

	"6.5840/labgob"
)

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
	e.Encode(rf.ps.lastSnapshotIndex)
	e.Encode(rf.ps.lastSnapshotTerm)
	raftstate := w.Bytes()

	// If a server crashes, it must restart from persisted data.
	// Your Raft should persist both Raft state and the corresponding snapshot.
	// Use the second argument to persister.Save() to save the snapshot.
	// If there's no snapshot, pass nil as the second argument.

	rf.persister.Save(raftstate, rf.ps.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readRfState() {
	data := rf.persister.ReadRaftState()

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
	var lsi int
	var lst int
	if d.Decode(&ct) != nil ||
		d.Decode(&vf) != nil ||
		d.Decode(&l) != nil ||
		d.Decode(&lsi) != nil ||
		d.Decode(&lst) != nil {
		DPrintf("Error reading persistent state")
	} else {
		rf.ps = &PersistentState{
			currentTerm:       ct,
			log:               l,
			lastSnapshotTerm:  lst,
			lastSnapshotIndex: lsi,
		}
		if vf != -1 {
			rf.ps.votedFor = &vf
		}
	}
}

func (rf *Raft) readSnapshot() {
	if rf.persister.SnapshotSize() > 0 {
		rf.ps.snapshot = rf.persister.ReadSnapshot()
	}
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.persister.RaftStateSize()
}
