package shardgrp


import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	"6.5840/shardkv1/utils"
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
		// unknown type of request, should not happen within the lab
		panic(fmt.Sprintf("[server=%d] unknown request type: %T", kv.me, castedReq))
	}
}

func (kv *KVServer) handleGetOp(args *rpc.GetArgs) GetOpResult {
	utils.DPrintf("[server=%d, gid=%d, handleGetOp: shard=%d] key=%v",
		kv.me, kv.gid, kv.lastSeenConfigNum, args.Key)

	key := args.Key
	shId := shardcfg.Key2Shard(key)

	utils.DPrintf("[server=%d, gid=%d, handleGetOp: shard=%d] key=%v is on shard=%d",
		kv.me, kv.gid, kv.lastSeenConfigNum, args.Key, shId)

	db, err := kv.getShardDb(shId)
	if err != rpc.OK {
		utils.DPrintf("[server=%d, gid=%d, handleGetOp: shard=%d] key=%v is on shard=%d. The group is not the owner of the shard.",
			kv.me, kv.gid, kv.lastSeenConfigNum, args.Key, shId)
		return GetOpResult{Err: err}
	}

	if val, ok := db[key]; ok {
		utils.DPrintf("[server=%d, gid=%d, handleGetOp: shard=%d] key=%v is on shard=%d. Found the key, returning the value. The shard state=%v",
			kv.me, kv.gid, kv.lastSeenConfigNum, args.Key, shId, db)
		return GetOpResult{Err: rpc.OK, Value: val.Value, Version: val.Version}
	} else {
		utils.DPrintf("[server=%d, gid=%d, handleGetOp: shard=%d] key=%v is on shard=%d. ErrNoKey. The shard state=%v",
			kv.me, kv.gid, kv.lastSeenConfigNum, args.Key, shId, db)

		return GetOpResult{Err: rpc.ErrNoKey}
	}
}

func (kv *KVServer) handlePutOp(args *rpc.PutArgs) PutOpResult {
	utils.DPrintf("[server=%d, gid=%d, handlePutOp: shard=%d] key=%v, value=%v",
		kv.me, kv.gid, kv.lastSeenConfigNum, args.Key, args.Value)

	key := args.Key
	version := args.Version
	val := args.Value
	shId := shardcfg.Key2Shard(key)

	utils.DPrintf("[server=%d, gid=%d, handlePutOp: shard=%d] key=%v, value=%v. Should be placed to the shard=%d",
		kv.me, kv.gid, kv.lastSeenConfigNum, args.Key, args.Value, shId)

	if _, ok := kv.frozenShards[shId]; ok {
		utils.DPrintf("[server=%d, gid=%d, handlePutOp: shard=%d] key=%v is on shard=%d. The shard is frozen.",
			kv.me, kv.gid, kv.lastSeenConfigNum, args.Key, shId)

		return PutOpResult{Err: rpc.ErrWrongGroup}
	}

	db, err := kv.getShardDb(shId)
	if err != rpc.OK {
		utils.DPrintf("[server=%d, gid=%d, handlePutOp: shard=%d] key=%v is on shard=%d. The group is not the owner of the shard.",
			kv.me, kv.gid, kv.lastSeenConfigNum, args.Key, shId)

		return PutOpResult{Err: err}
	}

	dbVal, ok := db[key]

	// 1 true true -> (in db, and the version == db.version) -> update the value
	// 2 true false ->(in db, and the version != db.version) -> ErrVersion

	// 3 false true -> (not in the db, and the version == 0) -> update the value, it is not in the db
	// 4 false false -> (not in the db, and the version != 0) -> update the value, it is not in the db

	if ok {
		if version == dbVal.Version { // 1
			db[key] = DbValue{Value: val, Version: version + 1}
			utils.DPrintf("[server=%d, gid=%d, handlePutOp: shard=%d] key=%v is on shard=%d. Stored the key. The shard state=%v",
				kv.me, kv.gid, kv.lastSeenConfigNum, args.Key, shId, db)
			return PutOpResult{Err: rpc.OK}
		} else { // 2
			return PutOpResult{Err: rpc.ErrVersion}
		}
	} else {
		if version == 0 { // 3
			db[key] = DbValue{Value: val, Version: version + 1}
			utils.DPrintf("[server=%d, gid=%d, handlePutOp: shard=%d] key=%v is on shard=%d. Stored the key. The shard state=%v",
				kv.me, kv.gid, kv.lastSeenConfigNum, args.Key, shId, db)
			return PutOpResult{Err: rpc.OK}
		} else { // 4
			return PutOpResult{Err: rpc.ErrNoKey}
		}
	}
}

func (kv *KVServer) handleFreezeShardOp(args *shardrpc.FreezeShardArgs) FreezeShardOpResult {
	utils.DPrintf("[server=%d, gid=%d, handleFreezeShardOp: shard=%d, num=%d, lastSeenConfigNum=%d]",
		kv.me, kv.gid, args.Shard, args.Num, kv.lastSeenConfigNum)

	// the operation is idempotent. can be called > 1 times for the same Num
	if args.Num < kv.lastSeenConfigNum[args.Shard] {
		return FreezeShardOpResult{Err: rpc.OK}
	}

	// if no db, probably no op, just continue
	db, err := kv.getShardDb(args.Shard)
	if err != rpc.OK {
		utils.DPrintf("[server=%d, gid=%d, handleFreezeShardOp: shard=%d, num=%d, lastSeenConfigNum=%d] failed with err %v",
			kv.me, kv.gid, args.Shard, args.Num, kv.lastSeenConfigNum, err)
	}

	kv.frozenShards[args.Shard] = struct{}{}

	kv.lastSeenConfigNum[args.Shard] = args.Num

	utils.DPrintf("[server=%d, gid=%d, handleFreezeShardOp: shard=%d, num=%d, lastSeenConfigNum=%d]. Returning state %v",
		kv.me, kv.gid, args.Shard, args.Num, kv.lastSeenConfigNum, db)

	return FreezeShardOpResult{
		State: kv.encodeData(kv.makeShardDbCopy(args.Shard)),
		Num:   args.Num,
		Err:   rpc.OK,
	}
}

func (kv *KVServer) handleInstallShardOp(args *shardrpc.InstallShardArgs) InstallShardOpResult {
	db := make(map[string]DbValue, 0)

	// deserialize outside of the lock
	if len(args.State) > 0 {
		r := bytes.NewBuffer(args.State)
		d := labgob.NewDecoder(r)

		if err := d.Decode(&db); err != nil {
			panic(fmt.Sprintf("Error reading persistent state, args = %v, err = %v", args, err.Error()))
		}
	}

	utils.DPrintf("[server=%d, gid=%d, handleInstallShardOp: shard=%d, num=%d, lastSeenConfigNum=%d] starting. Recieved state = %v",
		kv.me, kv.gid, args.Shard, args.Num, kv.lastSeenConfigNum, db)

	// verify leniariz for the shard. if outdated -> OK
	if args.Num <= kv.lastSeenConfigNum[args.Shard] {
		// panic("Received FreezeShard request with old config num, idk how to handle it yet")
		utils.DPrintf("[server=%d, gid=%d, handleInstallShardOp: shard=%d, num=%d, lastSeenConfigNum=%d] returning no op",
			kv.me, kv.gid, args.Shard, args.Num, kv.lastSeenConfigNum)
		return InstallShardOpResult{Err: rpc.OK}
	}
	kv.db[args.Shard] = db

	// unfroze if some stale operation have not complete the process and the shard is frozen
	delete(kv.frozenShards, args.Shard)

	kv.lastSeenConfigNum[args.Shard] = args.Num

	utils.DPrintf("[server=%d, gid=%d, handleInstallShardOp: shard=%d, num=%d, lastSeenConfigNum=%d] returning",
		kv.me, kv.gid, args.Shard, args.Num, kv.lastSeenConfigNum)

	return InstallShardOpResult{Err: rpc.OK}
}

func (kv *KVServer) handleDeleteShardOp(args *shardrpc.DeleteShardArgs) DeleteShardOpResult {
	utils.DPrintf("[server=%d, gid=%d, handleDeleteShardOp: shard=%d, num=%d, lastSeenConfigNum=%d]",
		kv.me, kv.gid, args.Shard, args.Num, kv.lastSeenConfigNum)

	// verify leniariz for the shard. if outdated -> OK
	// as first interaction for the Num should be the Freeze operation, we should allow to call the Delete with the same Num
	// the Install operation makes sure in case of retries the deleted shard will not be installed again
	if args.Num < kv.lastSeenConfigNum[args.Shard] {
		// panic("Received DeleteShard request with old config num, idk how to handle it yet")
		return DeleteShardOpResult{
			Err: rpc.OK,
		}
	}

	delete(kv.db, args.Shard)
	delete(kv.frozenShards, args.Shard)

	// update version
	kv.lastSeenConfigNum[args.Shard] = args.Num

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
	utils.DPrintf("[server=%d, gid=%d, server=%d] Called Get with args = [%v]",
		kv.me, kv.gid, kv.me, args)

	if kv.killed() {
		utils.DPrintf("[server=%d, gid=%d, server=%d] Returning from Get as the KVServer is killed",
			kv.me, kv.gid, kv.me)
		reply.Err = rpc.ErrWrongGroup
		return
	}

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
	utils.DPrintf("[server=%d, gid=%d, server=%d] Called Put with args = [%v]",
		kv.me, kv.gid, kv.me, args)

	if kv.killed() {
		utils.DPrintf("[server=%d, gid=%d, server=%d] Returning from Put as the KVServer is killed",
			kv.me, kv.gid, kv.me)
		reply.Err = rpc.ErrWrongGroup
		return
	}

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
	utils.DPrintf("[server=%d, gid=%d, server=%d] Calling the FreezeShard in the server with [%v]",
		kv.me, kv.gid, kv.me, args)

	if kv.killed() {
		utils.DPrintf("[server=%d, gid=%d, server=%d] Returning from FreezeShard as the KVServer is killed",
			kv.me, kv.gid, kv.me)
		reply.Err = rpc.ErrWrongGroup
		return
	}

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
	if kv.killed() {
		utils.DPrintf("[server=%d, gid=%d, server=%d] Returning from InstallShard as the KVServer is killed",
			kv.me, kv.gid, kv.me)
		reply.Err = rpc.ErrWrongGroup
		return
	}

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
	if kv.killed() {
		utils.DPrintf("[server=%d, gid=%d, server=%d] Returning from DeleteShard as the KVServer is killed",
			kv.me, kv.gid, kv.me)
		reply.Err = rpc.ErrWrongGroup
		return
	}

	err, res := kv.rsm.Submit(*args)

	if err == rpc.OK {
		opResult := res.(DeleteShardOpResult)

		reply.Err = opResult.Err
	} else {
		reply.Err = err
	}
}

func (kv *KVServer) makeShardDbCopy(shId shardcfg.Tshid) map[string]DbValue {
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
	utils.DPrintf("[gid=%d, server=%d] Called killed within the KVServer", kv.gid, kv.me)
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
			kv.lastSeenConfigNum[shardcfg.Tshid(shId)] = 0
		}
	}

	return []tester.IService{kv, kv.rsm.Raft()}
}
