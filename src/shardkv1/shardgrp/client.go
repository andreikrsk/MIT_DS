package shardgrp

import (
	"sync/atomic"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	tester "6.5840/tester1"
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

func (ck *Clerk) callGet(s int32, args *rpc.GetArgs, reply *rpc.GetReply) (bool, rpc.Err) {
	deadline := time.After(3 * time.Second)
	resChan := make(chan bool, 1)

	go func() {
		ok := ck.clnt.Call(ck.servers[s], "KVServer.Get", args, reply)
		resChan <- ok
	}()

	select {
	case ok := <-resChan:
		return ok, reply.Err
	case <-deadline:
		return false, rpc.Err("RPC timeout")
	}
}

func (ck *Clerk) callPut(s int32, args *rpc.PutArgs, reply *rpc.PutReply) (bool, rpc.Err) {
	deadline := time.After(3 * time.Second)
	resChan := make(chan bool, 1)

	go func() {
		ok := ck.clnt.Call(ck.servers[s], "KVServer.Put", args, reply)
		resChan <- ok
	}()

	select {
	case ok := <-resChan:
		return ok, reply.Err
	case <-deadline:
		return false, rpc.Err("RPC timeout")
	}
}

// retry logic
func (ck *Clerk) sendReqToMaster(rpcCaller func(s int32) (bool, rpc.Err)) rpc.Err {
	currentLeader := ck.leader.Load()
	ok, rpcErr := rpcCaller(currentLeader)

	hadLostCalls := !ok

	if !ok || rpcErr == rpc.ErrWrongLeader {
		for {
			for idx := range ck.servers {
				ok, rpcErr := rpcCaller(int32(idx))

				if !ok {
					hadLostCalls = true
				} else {
					switch rpcErr {
					case rpc.OK:
						ck.leader.CompareAndSwap(currentLeader, int32(idx))
						return rpc.OK
					case rpc.ErrVersion:
						// the first call failed, we don't know if the Put was performed or not, return ErrMaybe
						ck.leader.CompareAndSwap(currentLeader, int32(idx))
						if hadLostCalls {
							return rpc.ErrMaybe
						}
						return rpc.ErrVersion
					case rpc.ErrWrongLeader:
						// try the next server
						continue
					default:
						// some other error, return it
						return rpcErr
					}
				}
			}
			time.Sleep(20 * time.Millisecond)
		}
	}

	return rpcErr
}

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	args := rpc.GetArgs{Key: key}
	reply := rpc.GetReply{Value: "", Version: 0, Err: rpc.ErrNoKey}

	rpcCaller := func(s int32) (bool, rpc.Err) {
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

	rpcCaller := func(s int32) (bool, rpc.Err) {
		return ck.callPut(s, &args, &reply)
	}

	rpcErr := ck.sendReqToMaster(rpcCaller)

	if rpcErr != rpc.OK {
		return rpcErr
	}

	return reply.Err
}

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	// Your code here
	return nil, ""
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	// Your code here
	return ""
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	// Your code here
	return ""
}
