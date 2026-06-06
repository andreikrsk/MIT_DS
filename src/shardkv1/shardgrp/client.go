package shardgrp

import (
	"sync/atomic"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	"6.5840/shardkv1/utils"
	tester "6.5840/tester1"
)

type NetworkReply int

const (
	rpcTimeout = 1500 // in milliseconds

	TIMEOUT NetworkReply = iota
	DISCONNECT
	OK
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	// You will have to modify this struct.

	leader atomic.Int32 // the id of the current leader server
}

func MakeClerk(clnt *tester.Clnt, servers []string) *Clerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	ck.leader.Store(0)

	return ck
}

func (ck *Clerk) callGet(s int32, args *rpc.GetArgs, reply *rpc.GetReply) (NetworkReply, interface{}, rpc.Err) {
	deadline := time.After(rpcTimeout * time.Millisecond)
	resChan := make(chan bool, 1)

	utils.DPrintf("[shardgrp/clerk] Calling callGet for %v", args)

	go func() {
		ok := ck.clnt.Call(ck.servers[s], "KVServer.Get", args, reply)
		resChan <- ok
	}()

	select {
	case ok := <-resChan:
		if ok {
			return OK, *reply, reply.Err
		}
		return DISCONNECT, *reply, reply.Err
	case <-deadline:
		return TIMEOUT, *reply, rpc.OK
	}
}

func (ck *Clerk) callPut(s int32, args *rpc.PutArgs, reply *rpc.PutReply) (NetworkReply, interface{}, rpc.Err) {
	deadline := time.After(rpcTimeout * time.Millisecond)
	resChan := make(chan bool, 1)

	utils.DPrintf("[shardgrp/clerk] Calling callPut for %v", args)

	go func() {
		ok := ck.clnt.Call(ck.servers[s], "KVServer.Put", args, reply)
		resChan <- ok
	}()

	select {
	case ok := <-resChan:
		if ok {
			return OK, *reply, reply.Err
		}
		return DISCONNECT, *reply, reply.Err
	case <-deadline:
		return TIMEOUT, *reply, rpc.OK
	}
}

func (ck *Clerk) callFreezeShard(s int32, args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) (NetworkReply, interface{}, rpc.Err) {
	deadline := time.After(rpcTimeout * time.Millisecond)
	resChan := make(chan bool, 1)

	utils.DPrintf("Calling callFreezeShard for %v", args)

	go func() {
		ok := ck.clnt.Call(ck.servers[s], "KVServer.FreezeShard", args, reply)
		resChan <- ok
	}()

	select {
	case ok := <-resChan:
		if ok {
			utils.DPrintf("[shardgrp/clerk] callFreezeShard successful for shardId=%d, num=%d, reply=%v", args.Shard, args.Num, *reply)
			return OK, *reply, reply.Err
		}
		utils.DPrintf("[shardgrp/clerk] callFreezeShard failed for shardId=%d, num=%d", args.Shard, args.Num)
		return DISCONNECT, *reply, reply.Err
	case <-deadline:
		utils.DPrintf("[shardgrp/clerk] callFreezeShard timeout for shardId=%d, num=%d", args.Shard, args.Num)
		return TIMEOUT, *reply, rpc.OK
	}
}

func (ck *Clerk) callInstallShard(s int32, args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) (NetworkReply, interface{}, rpc.Err) {
	deadline := time.After(rpcTimeout * time.Millisecond)
	resChan := make(chan bool, 1)

	utils.DPrintf("[shardgrp/clerk] Calling callInstallShard for shardId=%d, num=%d", args.Shard, args.Num)

	go func() {
		ok := ck.clnt.Call(ck.servers[s], "KVServer.InstallShard", args, reply)
		resChan <- ok
	}()

	select {
	case ok := <-resChan:
		if ok {
			utils.DPrintf("[shardgrp/clerk] callInstallShard successful for shardId=%d, num=%d, reply=%v", args.Shard, args.Num, *reply)
			return OK, *reply, reply.Err
		}
		utils.DPrintf("[shardgrp/clerk] callInstallShard failed for shardId=%d, num=%d", args.Shard, args.Num)
		return DISCONNECT, *reply, reply.Err
	case <-deadline:
		utils.DPrintf("[shardgrp/clerk] callInstallShard timeout for shardId=%d, num=%d", args.Shard, args.Num)
		return TIMEOUT, *reply, rpc.OK
	}
}

func (ck *Clerk) callDeleteShard(s int32, args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) (NetworkReply, interface{}, rpc.Err) {
	deadline := time.After(rpcTimeout * time.Millisecond)
	resChan := make(chan bool, 1)

	utils.DPrintf("[shardgrp/clerk] Calling callDeleteShard for shardId=%d, num=%d", args.Shard, args.Num)

	go func() {
		ok := ck.clnt.Call(ck.servers[s], "KVServer.DeleteShard", args, reply)
		resChan <- ok
	}()

	select {
	case ok := <-resChan:
		if ok {
			utils.DPrintf("[shardgrp/clerk] callDeleteShard successful for shardId=%d, num=%d, reply=%v", args.Shard, args.Num, *reply)
			return OK, *reply, reply.Err
		}
		utils.DPrintf("[shardgrp/clerk] callDeleteShard failed for shardId=%d, num=%d", args.Shard, args.Num)
		return DISCONNECT, *reply, reply.Err
	case <-deadline:
		utils.DPrintf("[shardgrp/clerk] callDeleteShard timeout for shardId=%d, num=%d", args.Shard, args.Num)
		return TIMEOUT, *reply, rpc.OK
	}
}

// retry logic
func (ck *Clerk) sendReqToMaster(rpcCaller func(s int32) (NetworkReply, interface{}, rpc.Err)) (interface{}, rpc.Err) {
	currentLeader := ck.leader.Load()
	ntReply, resp, rpcErr := rpcCaller(currentLeader)
	utils.DPrintf("[shardgrp/clerk] sendReqToMaster response: ntReply=%v, rpcErr=%v", ntReply, rpcErr)
	hadLostCalls := ntReply == TIMEOUT || ntReply == DISCONNECT
	// partition := ntReply == DISCONNECT
	// if partition {
	// fmt.Println("Partitioned")
	// }

	if hadLostCalls || rpcErr == rpc.ErrWrongLeader {
		for {
			partitions := 0
			for idx := range ck.servers {
				ntReply, resp, rpcErr := rpcCaller(int32(idx))
				utils.DPrintf("[shardgrp/clerk] sendReqToMaster response: ntReply=%v, rpcErr=%v", ntReply, rpcErr)

				if ntReply == OK {
					switch rpcErr {
					case rpc.OK:
						ck.leader.CompareAndSwap(currentLeader, int32(idx))
						return resp, rpc.OK
					case rpc.ErrVersion:
						// the first call failed, we don't know if the Put was performed or not, return ErrMaybe
						ck.leader.CompareAndSwap(currentLeader, int32(idx))
						if hadLostCalls {
							return resp, rpc.ErrMaybe
						}
						return resp, rpc.ErrVersion
					case rpc.ErrWrongLeader:
						// try the next server
						continue
					case rpc.ErrWrongGroup:
						if hadLostCalls {
							return resp, rpc.ErrMaybe
						}
						return resp, rpc.ErrWrongGroup
					default:
						// some other error, return it
						return resp, rpcErr
					}
				} else if ntReply == TIMEOUT {
					utils.DPrintf("[shardgrp/clerk] Timeout to server = %v", ck.servers[idx])
					hadLostCalls = true
				} else {
					partitions++
					hadLostCalls = true
				}
			}
			if partitions == len(ck.servers) {
				utils.DPrintf("[shardgrp/clerk] All servers are partitioned, returning ErrMaybe")
				return resp, rpc.ErrMaybe
			}
			time.Sleep(20 * time.Millisecond)
		}
	}

	return resp, rpcErr
}

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	args := rpc.GetArgs{Key: key}

	rpcCaller := func(s int32) (NetworkReply, interface{}, rpc.Err) {
		reply := rpc.GetReply{Value: "", Version: 0, Err: rpc.OK}
		return ck.callGet(s, &args, &reply)
	}

	resp, rpcErr := ck.sendReqToMaster(rpcCaller)

	if rpcErr != rpc.OK {
		return "", 0, rpcErr
	}

	castedResp := resp.(rpc.GetReply)
	utils.DPrintf("[shardgrp/clerk] Get: done calling callGet with rpcErr=%v and reply.Err=%v", rpcErr, castedResp.Err)
	return castedResp.Value, castedResp.Version, castedResp.Err
}

func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	args := rpc.PutArgs{Key: key, Value: value, Version: version}

	utils.DPrintf("[shardgrp/clerk] Put: key=%s, value=%s, version=%d", key, value, version)

	rpcCaller := func(s int32) (NetworkReply, interface{}, rpc.Err) {
		reply := rpc.PutReply{Err: rpc.OK}
		return ck.callPut(s, &args, &reply)
	}

	resp, rpcErr := ck.sendReqToMaster(rpcCaller)

	if rpcErr != rpc.OK {
		return rpcErr
	}

	castedResp := resp.(rpc.PutReply)
	utils.DPrintf("[shardgrp/clerk] Put: done calling callPut with rpcErr=%v and reply.Err=%v", rpcErr, castedResp.Err)
	return castedResp.Err
}

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	args := shardrpc.FreezeShardArgs{Shard: s, Num: num}

	rpcCaller := func(s int32) (NetworkReply, interface{}, rpc.Err) {
		reply := shardrpc.FreezeShardReply{Num: 0, State: nil, Err: rpc.OK}
		return ck.callFreezeShard(s, &args, &reply)
	}

	resp, rpcErr := ck.sendReqToMaster(rpcCaller)

	if rpcErr != rpc.OK {
		return nil, rpcErr
	}

	castedResp := resp.(shardrpc.FreezeShardReply)
	utils.DPrintf("[shardgrp/clerk] FreezeShard: done calling callFreezeShard with rpcErr=%v and reply.Err=%v", rpcErr, castedResp.Err)
	return castedResp.State, castedResp.Err
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	args := shardrpc.InstallShardArgs{Shard: s, State: state, Num: num}

	rpcCaller := func(s int32) (NetworkReply, interface{}, rpc.Err) {
		reply := shardrpc.InstallShardReply{Err: rpc.OK}
		return ck.callInstallShard(s, &args, &reply)
	}

	resp, rpcErr := ck.sendReqToMaster(rpcCaller)

	if rpcErr != rpc.OK {
		return rpcErr
	}

	castedResp := resp.(shardrpc.InstallShardReply)
	utils.DPrintf("[shardgrp/clerk] InstallShard: done calling callInstallShard with rpcErr=%v and reply.Err=%v", rpcErr, castedResp.Err)
	return castedResp.Err
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	args := shardrpc.DeleteShardArgs{Shard: s, Num: num}

	rpcCaller := func(s int32) (NetworkReply, interface{}, rpc.Err) {
		reply := shardrpc.DeleteShardReply{Err: rpc.OK}
		return ck.callDeleteShard(s, &args, &reply)
	}

	resp, rpcErr := ck.sendReqToMaster(rpcCaller)

	// infra error
	if rpcErr != rpc.OK {
		return rpcErr
	}

	// operation result
	castedResp := resp.(shardrpc.DeleteShardReply)
	utils.DPrintf("[shardgrp/clerk] DeleteShard: done calling callDeleteShard with rpcErr=%v and reply.Err=%v", rpcErr, castedResp.Err)
	return castedResp.Err
}
