package raft

import (
	"math/rand"
	"sync"
	"time"
)

// const hbperiod = 100 * time.Millisecond

// if a follower hasn't heard from a leader in this time, it becomes candidate
const electionTimeout = 800

type serverState int

// In any given time a server must be in one of three states: follower, candidate, or leader.
const (
	follower  serverState = iota // followers are passeive, they issue no requests
	candidate                    // is used to elect a new leader
	leader                       // handles client requests
)

// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// initiated by candidates during elections
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	now := time.Now()

	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[%d, state=%v] received RequestVote RPC from [%d] for term [%d]", rf.me, rf.fstate, args.CandidateId, args.Term)
	// reply.Term must always be set to currentTerm
	reply.Term = rf.ps.currentTerm
	reply.VoteGranted = false

	// if a server receives a request with a stale term number, it rejects the request
	// 1) stale term -> reject
	if args.Term < rf.ps.currentTerm {
		return
	}

	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
	if args.Term > rf.ps.currentTerm {
		rf.makeMeFollower(args.Term)
		rf.persist()
		DPrintf("[%d, state=%v] became follower for term [%d] due to higher term in RequestVote request from [%d] with term [%d]", rf.me, rf.fstate, rf.ps.currentTerm, args.CandidateId, args.Term)
	}

	// 3) check whether we already voted for someone else this term
	if rf.ps.votedFor != nil && *rf.ps.votedFor != args.CandidateId {
		return
	}

	// Raft determines which of two logs is more up-to-date
	// by comparing the index and term of the last entries in the
	// logs. If the logs have last entries with different terms, then
	// the log with the later term is more up-to-date. If the logs
	// end with the same term, then whichever log is longer is
	// more up-to-date.
	// 4) check candidate's log up-to-date-ness
	lastIndex := 0
	lastTerm := 0
	if len(rf.ps.log) > 0 {
		lastIndex = len(rf.ps.log) - 1
		lastTerm = rf.ps.log[lastIndex].Term
	}
	candidateUpToDate := (args.LastLogTerm > lastTerm) ||
		(args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIndex)

	if !candidateUpToDate {
		// do NOT grant vote if candidate's log is older
		return
	}

	// 5) grant vote
	v := args.CandidateId
	rf.ps.votedFor = &v
	reply.VoteGranted = true
	rf.persist()
	rf.hbtime.Store(&now)
	// Your code here (3A, 3B).
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

func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		etms := electionTimeout + (rand.Int63() % 200) // election timeout between 800 and 1000
		shouldStart := rf.fstate == candidate || (rf.fstate == follower && (rf.hbtime.Load() == nil || time.Since(*rf.hbtime.Load()).Milliseconds() > etms))
		currentTerm := rf.ps.currentTerm
		rf.mu.Unlock()

		if shouldStart {
			rf.mu.Lock()
			DPrintf("[%d, state=%v] starting election for term [%d]", rf.me, rf.fstate, currentTerm+1)
			rf.makeMeCandidate()
			rf.persist()
			rf.mu.Unlock()

			done := make(chan struct{})

			go func() {
				rf.runElection()
				close(done)
			}()

			<-done
			// rf.mu.Lock()
			// DPrintf("[%d, state=%v] finished election for term [%d]", rf.me, rf.fstate, currentTerm+1)
			// rf.mu.Unlock()
		}

		if shouldStart {
			// pause for a random amount of time between 50 and 250
			// milliseconds.
			ms := 50 + (rand.Int63() % 100)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}
	}
}

func (rf *Raft) runElection() {
	votes := rf.requestForVotes()

	rf.mu.Lock()
	if rf.ps.votedFor != nil && *rf.ps.votedFor == rf.me {
		votes += 1
	}
	rf.mu.Unlock()

	if votes >= (len(rf.peers)/2)+1 {
		rf.makeMeLeader()
	}
}

func (rf *Raft) requestForVotes() int {
	granted := make(chan bool, len(rf.peers)-1)
	wg := sync.WaitGroup{}
	wg.Add(len(rf.peers) - 1)

	// send RequestVote RPCs to all other servers
	for sid := range rf.peers {
		if sid == rf.me {
			continue
		}
		go func(sid int) {
			defer wg.Done()
			rf.mu.Lock()
			args := rf.buildRequestVoeteArgs()
			reply := rf.buildRequestVoteReply()
			DPrintf("[%d, state=%v] is requesting votes from [%d]", rf.me, rf.fstate, sid)
			rf.mu.Unlock()
			if ok := rf.sendRequestVote(sid, args, reply); ok {
				if reply.VoteGranted {
					granted <- true
					// rf.mu.Lock()
					// DPrintf("[%d, state=%v] received \"vote\" from peer %d for term [%d]", rf.me, rf.fstate, sid, args.Term)
					// rf.mu.Unlock()
				} else {
					// rf.mu.Lock()
					// DPrintf("[%d, state=%v] received \"no vote\" from peer %d for term %d\n", rf.me, rf.fstate, sid, args.Term)
					// rf.mu.Unlock()
				}
				// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
				rf.mu.Lock()
				if reply.Term > rf.ps.currentTerm {
					rf.makeMeFollower(reply.Term)
					rf.persist()
					DPrintf("[%d, state=%v] became follower for term [%d] due to higher term in RequestVote reply from [%d] with term [%d]", rf.me, rf.fstate, rf.ps.currentTerm, sid, reply.Term)
				}
				rf.mu.Unlock()
			} else {
				// rf.mu.Lock()
				// DPrintf("[%d, state=%v] failed to send RequestVote RPC to peer [%d] for term [%d]", rf.me, rf.fstate, sid, args.Term)
				// rf.mu.Unlock()
			}
		}(sid)
	}

	go func() {
		wg.Wait()
		close(granted)
	}()

	timeout := time.After(rpcTimeout) // or your desired timeout
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
			// rf.mu.Lock()
			// DPrintf("[%d, state=%v] vote collection timed out", rf.me, rf.fstate)
			// rf.mu.Unlock()
			return votes
		}
	}
}

func (rf *Raft) buildRequestVoeteArgs() *RequestVoteArgs {
	llIndex := 0
	llTerm := 0

	if len(rf.ps.log) > 0 {
		llIndex = len(rf.ps.log) - 1
		llTerm = rf.ps.log[llIndex].Term
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
	rf.mu.Lock()
	DPrintf("[%d, state=%v] became leader for term [%d]", rf.me, rf.fstate, rf.ps.currentTerm)
	rf.fstate = leader
	//When a leader first comes to power, it initializes all nextIndex values to the index just after the last one in its log
	// and all matchIndex values to zero
	for i := range len(rf.lvs.nextIndex) {
		rf.lvs.nextIndex[i] = len(rf.ps.log)
		rf.lvs.matchIndex[i] = 0
	}
	rf.mu.Unlock()
}

func (rf *Raft) makeMeCandidate() {
	rf.ps.votedFor = &rf.me
	rf.ps.currentTerm += 1
	rf.fstate = candidate

	DPrintf("[%d, state=%v] became candidate for term [%d]", rf.me, rf.fstate, rf.ps.currentTerm)
}

func (rf *Raft) makeMeFollower(term int) {
	rf.ps.currentTerm = term
	rf.ps.votedFor = nil
	rf.fstate = follower
}
