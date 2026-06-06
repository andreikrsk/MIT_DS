package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client uses the shardctrler to query for the current
// configuration and find the assignment of shards (keys) to groups,
// and then talks to the group that holds the key's shard.
//

import (
	"sync"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardctrler"
	"6.5840/shardkv1/shardgrp"
	"6.5840/shardkv1/utils"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt *tester.Clnt
	sck  *shardctrler.ShardCtrler
	// You will have to modify this struct.

	clerksLock              sync.Mutex
	grpClerks               map[tester.Tgid]*shardgrp.Clerk // gid -> clerk
	wrongGroupChanNotifyGet chan struct{}                   // channel to signal wrong group error
	wrongRoupChanNotifyPut  chan struct{}                   // channel to signal wrong group error
}

// The tester calls MakeClerk and passes in a shardctrler so that
// client can call it's Query method
func MakeClerk(clnt *tester.Clnt, sck *shardctrler.ShardCtrler) kvtest.IKVClerk {
	ck := &Clerk{
		clnt: clnt,
		sck:  sck,
	}
	// You'll have to add code here.

	ck.clerksLock = sync.Mutex{}
	ck.grpClerks = make(map[tester.Tgid]*shardgrp.Clerk)
	ck.wrongGroupChanNotifyGet = make(chan struct{}, 1)
	ck.wrongRoupChanNotifyPut = make(chan struct{}, 1)

	return ck
}

// Get a key from a shardgrp.  You can use shardcfg.Key2Shard(key) to
// find the shard responsible for the key and ck.sck.Query() to read
// the current configuration and lookup the servers in the group
// responsible for key.  You can make a clerk for that group by
// calling shardgrp.MakeClerk(ck.clnt, servers).
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// You will have to modify this function.
	shard := shardcfg.Key2Shard(key)

	var val string
	var version rpc.Tversion
	var err rpc.Err

	for {
		val, version, err = ck.tryGetValue(shard, key)
		if err == rpc.ErrWrongGroup || err == rpc.ErrMaybe {
			utils.DPrintf("Get: wrong group, retrying")
			continue
		}
		break
	}

	return val, version, err
}

func (ck *Clerk) tryGetValue(shard shardcfg.Tshid, key string) (string, rpc.Tversion, rpc.Err) {
	grpClerk := ck.getClerkForShard(shard)

	if grpClerk == nil {
		return "", 0, rpc.ErrWrongGroup
	}

	return grpClerk.Get(key)
}

// Put a key to a shard group.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	shard := shardcfg.Key2Shard(key)

	var err rpc.Err

	hadLostCalls := false

	for {
		err = ck.tryPutValue(shard, key, value, version)
		if err == rpc.ErrWrongGroup {
			utils.DPrintf("Put: wrong group, retrying")
			continue
		} else if err == rpc.ErrMaybe {
			hadLostCalls = true
			utils.DPrintf("Put: maybe error, retrying")
			continue
		}
		break
	}

	// could be two cases:
	// 1) the first call timeouted but was actually processed by the server, so we got an ErrVersion back on the retry
	// 2) the first call was processed by the server but the response was lost,
	// after the shard with the value is moved to another group, we have to check if the value is still there,
	// if retry returns ok -> we are sure the value is there on retry, else -> we return ErrMaybe since we don't know why we got ErrVersion back on retry

	// here we can land with errors: rpc.OK, rpc.ErrVersion
	if hadLostCalls {
		if err == rpc.ErrVersion {
			return rpc.ErrMaybe
		}
	}

	return err
}

func (ck *Clerk) tryPutValue(shard shardcfg.Tshid, key string, value string, version rpc.Tversion) rpc.Err {
	grpClerk := ck.getClerkForShard(shard)

	if grpClerk == nil {
		return rpc.ErrWrongGroup
	}

	return grpClerk.Put(key, value, version)
}

func (ck *Clerk) getClerkForShard(shard shardcfg.Tshid) *shardgrp.Clerk {
	cfg := ck.sck.Query()
	utils.DPrintf("[getClerkForShard] Queried config: %v", cfg)
	gid := cfg.Shards[shard]

	ck.clerksLock.Lock()
	defer ck.clerksLock.Unlock()

	grpClerk, ok := ck.grpClerks[gid]
	if !ok {
		servers, ok := cfg.Groups[gid]
		if !ok {
			utils.DPrintf("[getClerkForShard] No group found for gid %v, shard %v", gid, shard)

			return nil
		}

		grpClerk = shardgrp.MakeClerk(ck.clnt, servers)
		ck.grpClerks[gid] = grpClerk
	}

	return grpClerk
}
