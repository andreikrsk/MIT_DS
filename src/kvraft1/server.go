package kvraft

import (
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	tester "6.5840/tester1"
)

type DbValue struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM

	// Your definitions here.

	dbLock sync.RWMutex
	db     map[string]DbValue // key-value database
}

type GetOpResult struct {
	Err     rpc.Err
	Value   string
	Version rpc.Tversion
}

type PutOpResult struct {
	Err rpc.Err
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	// Your code here

	switch castedReq := req.(type) {
	case rpc.GetArgs:
		kv.dbLock.RLock()
		defer kv.dbLock.RUnlock()
		key := castedReq.Key

		if val, ok := kv.db[key]; ok {
			return GetOpResult{Err: rpc.OK, Value: val.Value, Version: val.Version}
		} else {
			return GetOpResult{Err: rpc.ErrNoKey}
		}
	case rpc.PutArgs:
		kv.dbLock.Lock()
		defer kv.dbLock.Unlock()

		key := castedReq.Key
		version := castedReq.Version
		val := castedReq.Value

		dbVal, ok := kv.db[key]

		if !ok || version == dbVal.Version {
			kv.db[key] = DbValue{Value: val, Version: version + 1}
			return PutOpResult{Err: rpc.OK}
		} else {
			return PutOpResult{Err: rpc.ErrVersion}
		}
	default:
		raft.DPrintf("[server=%d] unknown request type: %T", kv.me, castedReq)
	}

	// unknown type of request, should not happen within the lap
	return nil
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	return nil
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	err, res := kv.rsm.Submit(*args)
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)
	if err == rpc.OK {
		opResult := res.(GetOpResult)

		if opResult.Err == rpc.OK {
			reply.Err = opResult.Err
			reply.Value = opResult.Value
			reply.Version = opResult.Version
		} else {
			reply.Err = opResult.Err
		}
	} else {
		reply.Err = err
	}
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	err, res := kv.rsm.Submit(*args)
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)
	if err == rpc.OK {
		reply.Err = res.(PutOpResult).Err
	} else {
		reply.Err = err
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})

	kv := &KVServer{me: me}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	kv.dbLock = sync.RWMutex{}
	kv.db = make(map[string]DbValue)

	return []tester.IService{kv, kv.rsm.Raft()}
}
