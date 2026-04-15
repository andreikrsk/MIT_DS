package shardgrp

import (
	"bytes"
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/shardkv1/shardgrp/shardrpc"
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
	gid  tester.Tgid

	// Your code here
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
	kv.dbLock.RLock()
	defer kv.dbLock.RUnlock()

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(kv.db)

	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
	kv.dbLock.Lock()
	defer kv.dbLock.Unlock()

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var db map[string]DbValue

	if d.Decode(&db) != nil {
		panic("Error reading persistent state")
	} else {
		kv.db = db
	}
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here
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
	// Your code here
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

// Freeze the specified shard (i.e., reject future Get/Puts for this
// shard) and return the key/values stored in that shard.
func (kv *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	// Your code here
}

// Install the supplied state for the specified shard.
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	// Your code here
}

// Delete the specified shard.
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	// Your code here
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

// StartShardServerGrp starts a server for shardgrp `gid`.
//
// StartShardServerGrp() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartServerShardGrp(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(shardrpc.FreezeShardArgs{})
	labgob.Register(shardrpc.InstallShardArgs{})
	labgob.Register(shardrpc.DeleteShardArgs{})
	labgob.Register(rsm.Op{})

	kv := &KVServer{gid: gid, me: me}
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	// Your code here
	kv.dbLock = sync.RWMutex{}
	kv.db = make(map[string]DbValue)

	return []tester.IService{kv, kv.rsm.Raft()}
}
