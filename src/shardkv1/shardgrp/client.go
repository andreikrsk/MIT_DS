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
	rpcTimeout = 3

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

func (ck *Clerk) callGet(s int32, args *rpc.GetArgs, reply *rpc.GetReply) (NetworkReply, rpc.Err) {
	deadline := time.After(rpcTimeout * time.Second)
	resChan := make(chan bool, 1)

	utils.DPrintf("[shardgrp/clerk] Calling callGet for %v", args)

	go func() {
		ok := ck.clnt.Call(ck.servers[s], "KVServer.Get", args, reply)
		resChan <- ok
	}()

	select {
	case ok := <-resChan:
		if ok {
			return OK, reply.Err
		}
		return DISCONNECT, reply.Err
	case <-deadline:
		return TIMEOUT, rpc.OK
	}
}

func (ck *Clerk) callPut(s int32, args *rpc.PutArgs, reply *rpc.PutReply) (NetworkReply, rpc.Err) {
	deadline := time.After(rpcTimeout * time.Second)
	resChan := make(chan bool, 1)

	utils.DPrintf("[shardgrp/clerk] Calling callPut for %v", args)

	go func() {
		ok := ck.clnt.Call(ck.servers[s], "KVServer.Put", args, reply)
		resChan <- ok
	}()

	select {
	case ok := <-resChan:
		if ok {
			return OK, reply.Err
		}
		return DISCONNECT, reply.Err
	case <-deadline:
		return TIMEOUT, rpc.OK
	}
}

func (ck *Clerk) callFreezeShard(s int32, args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) (NetworkReply, rpc.Err) {
	deadline := time.After(rpcTimeout * time.Second)
	resChan := make(chan bool, 1)

	// utils.DPrintf("Calling callFreezeShard for %v", args)

	go func() {
		ok := ck.clnt.Call(ck.servers[s], "KVServer.FreezeShard", args, reply)
		resChan <- ok
	}()

	select {
	case ok := <-resChan:
		if ok {
			return OK, reply.Err
		}
		return DISCONNECT, reply.Err
	case <-deadline:
		return TIMEOUT, rpc.OK
	}
}

func (ck *Clerk) callInstallShard(s int32, args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) (NetworkReply, rpc.Err) {
	deadline := time.After(rpcTimeout * time.Second)
	resChan := make(chan bool, 1)

	utils.DPrintf("[shardgrp/clerk] Calling callInstallShard for shardId=%d, num=%d", args.Shard, args.Num)

	go func() {
		ok := ck.clnt.Call(ck.servers[s], "KVServer.InstallShard", args, reply)
		resChan <- ok
	}()

	select {
	case ok := <-resChan:
		if ok {
			return OK, reply.Err
		}
		return DISCONNECT, reply.Err
	case <-deadline:
		return TIMEOUT, rpc.OK
	}
}

func (ck *Clerk) callDeleteShard(s int32, args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) (NetworkReply, rpc.Err) {
	deadline := time.After(rpcTimeout * time.Second)
	resChan := make(chan bool, 1)

	utils.DPrintf("[shardgrp/clerk] Calling callDeleteShard for shardId=%d, num=%d", args.Shard, args.Num)

	go func() {
		ok := ck.clnt.Call(ck.servers[s], "KVServer.DeleteShard", args, reply)
		resChan <- ok
	}()

	select {
	case ok := <-resChan:
		if ok {
			return OK, reply.Err
		}
		return DISCONNECT, reply.Err
	case <-deadline:
		return TIMEOUT, rpc.OK
	}
}

// retry logic
func (ck *Clerk) sendReqToMaster(rpcCaller func(s int32) (NetworkReply, rpc.Err)) rpc.Err {
	currentLeader := ck.leader.Load()
	ntReply, rpcErr := rpcCaller(currentLeader)
	utils.DPrintf("[shardgrp/clerk] sendReqToMaster response: ntReply=%v, rpcErr=%v", ntReply, rpcErr)
	hadLostCalls := ntReply != OK

	if hadLostCalls || rpcErr == rpc.ErrWrongLeader {
		for {
			partitions := 0
			for idx := range ck.servers {
				ntReply, rpcErr := rpcCaller(int32(idx))
				utils.DPrintf("[shardgrp/clerk] sendReqToMaster response: ntReply=%v, rpcErr=%v", ntReply, rpcErr)

				switch ntReply {
				case OK:
					err := ck.inferRPCCodeOnNetworkOK(currentLeader, int32(idx), hadLostCalls, rpcErr)
					if err != nil {
						return *err
					}
				case TIMEOUT:
					utils.DPrintf("[shardgrp/clerk] Timeout to server = %v", ck.servers[idx])
					hadLostCalls = true
				case DISCONNECT:
					partitions++
					hadLostCalls = true
				default:
					panic("Unknown NetworkReply")
				}
			}
			
			if partitions == len(ck.servers) {
				utils.DPrintf("[shardgrp/clerk] All servers are partitioned, retrying")
				return rpc.ErrMaybe
			}
			time.Sleep(20 * time.Millisecond)
		}
	}

	return rpcErr
}

func (ck *Clerk) inferRPCCodeOnNetworkOK(currentLeader, newLeader int32, hadLostCalls bool, rpcErr rpc.Err) *rpc.Err {
	var rpcErrToReturn rpc.Err

	switch rpcErr {
	case rpc.OK:
		ck.leader.CompareAndSwap(currentLeader, newLeader)
		rpcErrToReturn = rpc.OK
	case rpc.ErrVersion:
		// the first call failed, we don't know if the Put was performed or not, return ErrMaybe
		ck.leader.CompareAndSwap(currentLeader, newLeader)
		if hadLostCalls {
			rpcErrToReturn = rpc.ErrMaybe
		}
		rpcErrToReturn = rpc.ErrVersion
	case rpc.ErrWrongLeader:
		// try the next server
		// has to return nil and continue looking for the leader
	case rpc.ErrWrongGroup:
		if hadLostCalls {
			rpcErrToReturn = rpc.ErrMaybe
		}
		rpcErrToReturn = rpc.ErrWrongGroup
	default:
		// some other error, return it
		rpcErrToReturn = rpcErr
	}

	return &rpcErrToReturn
}

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	args := rpc.GetArgs{Key: key}
	reply := rpc.GetReply{Value: "", Version: 0, Err: rpc.OK}

	rpcCaller := func(s int32) (NetworkReply, rpc.Err) {
		return ck.callGet(s, &args, &reply)
	}

	rpcErr := ck.sendReqToMaster(rpcCaller)

	if rpcErr != rpc.OK {
		return "", 0, rpcErr
	}

	return reply.Value, reply.Version, reply.Err
}

func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	args := rpc.PutArgs{Key: key, Value: value, Version: version}
	reply := rpc.PutReply{Err: rpc.OK}

	utils.DPrintf("[shardgrp/clerk] Put: key=%s, value=%s, version=%d", key, value, version)

	rpcCaller := func(s int32) (NetworkReply, rpc.Err) {
		return ck.callPut(s, &args, &reply)
	}

	rpcErr := ck.sendReqToMaster(rpcCaller)

	utils.DPrintf("[shardgrp/clerk] Put: done calling callPut with rpcErr=%v and reply.Err=%v", rpcErr, reply.Err)

	if rpcErr != rpc.OK {
		return rpcErr
	}

	return reply.Err
}

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	args := shardrpc.FreezeShardArgs{Shard: s, Num: num}
	reply := shardrpc.FreezeShardReply{Num: 0, State: nil, Err: rpc.OK}

	rpcCaller := func(s int32) (NetworkReply, rpc.Err) {
		return ck.callFreezeShard(s, &args, &reply)
	}

	rpcErr := ck.sendReqToMaster(rpcCaller)

	if rpcErr != rpc.OK {
		return nil, rpcErr
	}

	return reply.State, reply.Err
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	args := shardrpc.InstallShardArgs{Shard: s, State: state, Num: num}
	reply := shardrpc.InstallShardReply{Err: rpc.OK}

	rpcCaller := func(s int32) (NetworkReply, rpc.Err) {
		return ck.callInstallShard(s, &args, &reply)
	}

	rpcErr := ck.sendReqToMaster(rpcCaller)

	if rpcErr != rpc.OK {
		return rpcErr
	}

	return reply.Err
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	args := shardrpc.DeleteShardArgs{Shard: s, Num: num}
	reply := shardrpc.DeleteShardReply{Err: rpc.OK}

	rpcCaller := func(s int32) (NetworkReply, rpc.Err) {
		return ck.callDeleteShard(s, &args, &reply)
	}

	rpcErr := ck.sendReqToMaster(rpcCaller)

	if rpcErr != rpc.OK {
		return rpcErr
	}

	return reply.Err
}
