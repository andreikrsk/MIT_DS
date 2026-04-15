package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	kvsrv "6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	tester "6.5840/tester1"
)

const (
	configKey = "config"
)

// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk

	killed int32 // set by Kill()

	// Your data here.
}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	// Your code here.
	return sck
}

// The tester calls InitController() before starting a new
// controller. In part A, this method doesn't need to do anything. In
// B and C, this method implements recovery.
func (sck *ShardCtrler) InitController() {
}

// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.  The initial configuration
// lists shardgrp shardcfg.Gid1 for all shards.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	//OK, ErrVersion, ErrMaybe
	err := sck.tryPutValueWithRetires(configKey, cfg.String(), 0)

	if err != rpc.OK {
		panic("InitConfig: put initial config failed")
	}
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	// Your code here.
	// for the first try let's just put the config
	// will handle wrong version case later
	value, version, err := sck.Get(configKey)
	kvsrv.DPrintf("Query: get value %v version %v err %v", value, version, err)
	if err != rpc.OK {
		panic("ChangeConfigTo: get current config failed")
	}

	var currVersion rpc.Tversion
	if version == 0 {
		currVersion = 0
	} else {
		currVersion = version
	}
	
	err = sck.tryPutValueWithRetires(configKey, new.String(), currVersion)
	if err != rpc.OK {
		panic("ChangeConfigTo: put new config failed")
	}
}

func (sck *ShardCtrler) tryPutValueWithRetires(key, value string, version rpc.Tversion) rpc.Err {
	err := sck.Put(key, value, version)
	retries := 0
	for err == rpc.ErrMaybe {
		err = sck.Put(key, value, version)
		retries++
	}

	kvsrv.DPrintf("InitConfig: done trying to put value %v with err %v after %d retries", value, err, retries)

	return err
}

// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	// Your code here.
	value, version, err := sck.Get(configKey)
	kvsrv.DPrintf("Query: get value %v version %v err %v", value, version, err)

	if err == rpc.OK {
		return shardcfg.FromString(value)
	}

	return nil
}
