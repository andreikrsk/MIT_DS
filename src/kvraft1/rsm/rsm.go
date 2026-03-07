package rsm

import (
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
	"github.com/google/uuid"
)

var useRaftStateMachine bool // to plug in another raft besided raft1
type MessageKey struct {
	Term  int
	Index int
}

type Op struct {
	Req any
	Key uuid.UUID
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type LeaderStateNotification struct {
	term     int
	isLeader bool
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	// Your definitions here.
	waitingOpsChs       map[uuid.UUID]chan any // maps opId and the channel to send the applied command to the waiting thread in Submit() method, for correlation
	termChangeListeners map[uuid.UUID]chan int
	lastAppliedIndex    int
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:                  me,
		maxraftstate:        maxraftstate,
		applyCh:             make(chan raftapi.ApplyMsg),
		sm:                  sm,
		waitingOpsChs:       make(map[uuid.UUID]chan any),
		lastAppliedIndex:    -1,
		termChangeListeners: make(map[uuid.UUID]chan int),
	}
	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}

	rsm.applyChJob()

	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {
	// The method should wait for the command to be commited (appear on the applyCh), and only then return

	// 0. Warp each req into an Op structure for correlation
	// 1. Submit the comamnd to raft, and verify if it was the leader
	// 1.1 If it was not the leader, return ErrWrongLeader.
	// 1.2 If it was the leader, wait for the command to be commited
	// 1.3 When the command is commited, it comes throught the applyCh
	// 1.4 Exectue the command on the state machine, and return the result
	raft.DPrintf("[server=%d] Received a submit request %s, wrapping it into an Op and submitting to Raft", rsm.me, req)

	op := Op{Key: uuid.New(), Req: req}

	resChan := make(chan any, 1)
	termChan := make(chan int, 1)

	rsm.mu.Lock()
	rsm.waitingOpsChs[op.Key] = resChan
	rsm.termChangeListeners[op.Key] = termChan
	rsm.mu.Unlock()

	defer func() {
		rsm.mu.Lock()
		delete(rsm.waitingOpsChs, op.Key)
		delete(rsm.termChangeListeners, op.Key)
		rsm.mu.Unlock()
	}()
	// If I first put the listeners in a map, and only after learn that I am not a leader, it is ok
	// if I am not a leader, the command will never be submitted to Raft
	// and because of the uuid it can be uniquely identified, so it won't cause any problem
	_, term, isLeader := rsm.Raft().Start(op)
	if !isLeader {
		return rpc.ErrWrongLeader, nil
	}


	for {
		select {
		case res := <-resChan:
			return rpc.OK, res
		case newTerm := <-termChan:
			if newTerm > term {
				return rpc.ErrWrongLeader, nil
			}
		}
	}

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.
}

func (rsm *RSM) applyChJob() {
	lastSeenTerm := -1
	go func() {
		for msg := range rsm.applyCh {
			if msg.CommandValid {
				if msg.CommandTerm != lastSeenTerm {
					rsm.notifyTermListeners(msg.CommandTerm)
					lastSeenTerm = msg.CommandTerm
				}

				// execute the command on the state machine
				raft.DPrintf("[server=%d] Executing the commited command with id %d on the state machine from doOpsJob()", rsm.me, msg.Command.(Op).Key)
				if msg.CommandIndex <= rsm.lastAppliedIndex {
					raft.DPrintf("[server=%d] Command with id %d has already been applied, skipping", rsm.me, msg.Command.(Op).Key)
					continue
				} else {
					rsm.lastAppliedIndex = msg.CommandIndex
				}

				// i have to always do the Op if it was commited by the Raft
				rep := rsm.sm.DoOp(msg.Command.(Op).Req)
				key := msg.Command.(Op).Key

				rsm.mu.Lock()
				ch, ok := rsm.waitingOpsChs[key]
				if ok {
					delete(rsm.waitingOpsChs, key)
				}
				rsm.mu.Unlock()

				if ok {
					// non-blocking send is even safer:
					select {
					case ch <- rep:
					default:
					}
				}
			} else {
				raft.DPrintf("[server=%d] Received an invalid command, ignoring", rsm.me)
			}
		}
		rsm.notifyTermListeners(lastSeenTerm + 1_000_000) // notify term listeners to unblock all waiting Submit()s
	}()
}

func (rsm *RSM) notifyTermListeners(term int) {
	go func() {
		rsm.mu.Lock()
		for _, ch := range rsm.termChangeListeners {
			select {
			case ch <- term:
			default:
			}
		}
		rsm.mu.Unlock()
	}()
}
