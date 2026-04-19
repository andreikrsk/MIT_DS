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
	"6.5840/shardkv1/shardcfg"
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
	db     map[shardcfg.Tshid]map[string]DbValue // sharded key-value database

	frozenShards      map[shardcfg.Tshid]any
	lastSeenConfigNum map[shardcfg.Tshid]shardcfg.Tnum // shardId -> last seen config num for that shard
}

type GetOpResult struct {
	Err     rpc.Err
	Value   string
	Version rpc.Tversion
}

type PutOpResult struct {
	Err rpc.Err
}

type FreezeShardOpResult struct {
	State []byte
	Num   shardcfg.Tnum
	Err   rpc.Err
}

type InstallShardOpResult struct {
	Err rpc.Err
}

type DeleteShardOpResult struct {
	Err rpc.Err
}

func (kv *KVServer) DoOp(req any) any {
	// Your code here

	switch castedReq := req.(type) {
	case rpc.GetArgs:
		return kv.handleGetOp(&castedReq)
	case rpc.PutArgs:
		return kv.handlePutOp(&castedReq)
	case shardrpc.FreezeShardArgs:
		return kv.handleFreezeShardOp(&castedReq)
	case shardrpc.InstallShardArgs:
		return kv.handleInstallShardOp(&castedReq)
	case shardrpc.DeleteShardArgs:
		return kv.handleDeleteShardOp(&castedReq)
	default:
		raft.DPrintf("[server=%d] unknown request type: %T", kv.me, castedReq)
	}

	// unknown type of request, should not happen within the lab
	return nil
}

func (kv *KVServer) handleGetOp(args *rpc.GetArgs) GetOpResult {
	kv.dbLock.RLock()
	defer kv.dbLock.RUnlock()

	key := args.Key
	shId := shardcfg.Key2Shard(key)

	db, err := kv.getShardDb(shId)
	if err != rpc.OK {
		return GetOpResult{Err: err}
	}

	if _, ok := kv.frozenShards[shId]; ok {
		return GetOpResult{Err: rpc.ErrWrongGroup}
	}

	if val, ok := db[key]; ok {
		return GetOpResult{Err: rpc.OK, Value: val.Value, Version: val.Version}
	} else {
		return GetOpResult{Err: rpc.ErrNoKey}
	}
}

func (kv *KVServer) handlePutOp(args *rpc.PutArgs) PutOpResult {
	kv.dbLock.Lock()
	defer kv.dbLock.Unlock()

	key := args.Key
	version := args.Version
	val := args.Value
	shId := shardcfg.Key2Shard(key)

	db, err := kv.getShardDb(shId)
	if err != rpc.OK {
		return PutOpResult{Err: err}
	}

	if _, ok := kv.frozenShards[shId]; ok {
		return PutOpResult{Err: rpc.ErrWrongGroup}
	}

	dbVal, ok := db[key]

	if !ok || version == dbVal.Version {
		db[key] = DbValue{Value: val, Version: version + 1}
		return PutOpResult{Err: rpc.OK}
	} else {
		return PutOpResult{Err: rpc.ErrVersion}
	}
}

func (kv *KVServer) handleFreezeShardOp(args *shardrpc.FreezeShardArgs) FreezeShardOpResult {
	kv.dbLock.RLock()
	defer kv.dbLock.RUnlock()

	raft.DPrintf("handleFreezeShardOp: shard=%d, num=%d, lastSeenConfigNum=%d", args.Shard, args.Num, kv.lastSeenConfigNum)

	if args.Num < kv.lastSeenConfigNum[args.Shard] {
		panic("Received FreezeShard request with old config num, idk how to handle it yet")
	}

	db, err := kv.getShardDb(args.Shard)
	if err != rpc.OK {
		return FreezeShardOpResult{Err: err}
	}

	// probably no op
	// _, ok := kv.frozenShards[args.Shard]
	// if ok {
	// panic("Received FreezeShard request for a shard that is already frozen, idk how to handle it yet")
	// }

	kv.frozenShards[args.Shard] = struct{}{}

	kv.lastSeenConfigNum[args.Shard] = args.Num
	return FreezeShardOpResult{
		State: kv.encodeData(db),
		Num:   args.Num,
		Err:   rpc.OK,
	}
}

func (kv *KVServer) handleInstallShardOp(args *shardrpc.InstallShardArgs) InstallShardOpResult {
	kv.dbLock.RLock()
	defer kv.dbLock.RUnlock()

	raft.DPrintf("handleInstallShardOp: shard=%d, num=%d, lastSeenConfigNum=%d", args.Shard, args.Num, kv.lastSeenConfigNum)

	if args.Num < kv.lastSeenConfigNum[args.Shard] {
		panic("Received FreezeShard request with old config num, idk how to handle it yet")
	}

	_, err := kv.getShardDb(args.Shard)
	if err == rpc.OK {
		panic("Recieved InstallShard request for a shard that already exists in the db, idk how to handle it yet")
	}

	r := bytes.NewBuffer(args.State)
	d := labgob.NewDecoder(r)

	var db map[string]DbValue

	if d.Decode(&db) != nil {
		panic("Error reading persistent state")
	} else {
		kv.db[args.Shard] = db
	}

	kv.lastSeenConfigNum[args.Shard] = args.Num
	return InstallShardOpResult{Err: rpc.OK}
}

func (kv *KVServer) handleDeleteShardOp(args *shardrpc.DeleteShardArgs) DeleteShardOpResult {
	kv.dbLock.RLock()
	defer kv.dbLock.RUnlock()

	raft.DPrintf("handleDeleteShardOp: shard=%d, num=%d, lastSeenConfigNum=%d", args.Shard, args.Num, kv.lastSeenConfigNum)

	if args.Num < kv.lastSeenConfigNum[args.Shard] {
		panic("Received DeleteShard request with old config num, idk how to handle it yet")
	}

	_, err := kv.getShardDb(args.Shard)
	if err != rpc.OK {
		return DeleteShardOpResult{Err: err}
	}

	_, ok := kv.frozenShards[args.Shard]
	if !ok {
		panic("Received DeleteShard request for a shard that is not frozen, idk how to handle it yet")
	}

	delete(kv.db, args.Shard)
	delete(kv.frozenShards, args.Shard)

	return DeleteShardOpResult{
		Err: rpc.OK,
	}
}

func (kv *KVServer) getShardDb(shId shardcfg.Tshid) (map[string]DbValue, rpc.Err) {
	// the group is not responsible for the shard, return an error
	dbShard, ok := kv.db[shId]
	if !ok {
		return nil, rpc.ErrWrongGroup
	}

	return dbShard, rpc.OK
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	kv.dbLock.RLock()
	defer kv.dbLock.RUnlock()

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(kv.db)

	return kv.encodeData(kv.db)
}

func (kv *KVServer) encodeData(data any) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(data)

	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
	kv.dbLock.Lock()
	defer kv.dbLock.Unlock()

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var db map[shardcfg.Tshid]map[string]DbValue

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
	err, res := kv.rsm.Submit(*args)

	if err == rpc.OK {
		opResult := res.(FreezeShardOpResult)

		reply.Num = opResult.Num
		reply.State = opResult.State
		reply.Err = opResult.Err
	} else {
		reply.Err = err
	}
}

// Install the supplied state for the specified shard.
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	// Your code here
	err, res := kv.rsm.Submit(*args)

	if err == rpc.OK {
		opResult := res.(InstallShardOpResult)

		reply.Err = opResult.Err
	} else {
		reply.Err = err
	}
}

// Delete the specified shard.
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	// Your code here
	err, res := kv.rsm.Submit(*args)

	if err == rpc.OK {
		opResult := res.(DeleteShardOpResult)

		reply.Err = opResult.Err
	} else {
		reply.Err = err
	}
}

func (kv *KVServer) makeShardDbCopy(shId shardcfg.Tshid) map[string]DbValue {
	kv.dbLock.RLock()
	defer kv.dbLock.RUnlock()

	dbCopy := make(map[string]DbValue)

	for k, v := range kv.db[shId] {
		dbCopy[k] = v
	}

	return dbCopy
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
	kv.db = make(map[shardcfg.Tshid]map[string]DbValue)
	kv.frozenShards = make(map[shardcfg.Tshid]any)
	kv.lastSeenConfigNum = make(map[shardcfg.Tshid]shardcfg.Tnum)

	if gid == shardcfg.Gid1 {
		// initialize the first group with all shards assigned to it
		for shId := 0; shId < shardcfg.NShards; shId++ {
			kv.db[shardcfg.Tshid(shId)] = make(map[string]DbValue)
			kv.lastSeenConfigNum[shardcfg.Tshid(shId)] = shardcfg.NumFirst
		}
	}

	return []tester.IService{kv, kv.rsm.Raft()}
}
