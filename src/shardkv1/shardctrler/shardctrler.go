package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	"fmt"

	kvsrv "6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
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

type Transfer struct {
	fromG tester.Tgid
	toG   tester.Tgid
	shId  shardcfg.Tshid
	state []byte
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

	kvsrv.DPrintf("InitConfig: putting initial config %v\n", cfg)

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
	old := sck.Query()

	transfers := sck.freezeMovingShards(old, new)
	sck.installShards(new, transfers)
	sck.deleteShards(old, transfers)

	value, curVersion, err := sck.Get(configKey)
	kvsrv.DPrintf("Query: get value %v version %v err %v", value, curVersion, err)
	if err != rpc.OK {
		panic("ChangeConfigTo: get current config failed")
	}

	// for now doesnt matter the current config in the kv
	// just take its version and try to put the new config
	err = sck.tryPutValueWithRetires(configKey, new.String(), curVersion)
	for err == rpc.ErrVersion {
		value, curVersion, err := sck.Get(configKey)
		kvsrv.DPrintf("Query: get value %v version %v err %v", value, curVersion, err)
		if err != rpc.OK {
			panic("ChangeConfigTo: get current config failed")
		}

		sck.tryPutValueWithRetires(configKey, new.String(), curVersion)
	}
}

func (sck *ShardCtrler) freezeMovingShards(oldConfig, newConfig *shardcfg.ShardConfig) []Transfer {
	transfers := make([]Transfer, 0)

	for shId, gid := range oldConfig.Shards {
		newGid := newConfig.Shards[shId]
		if gid != newGid {
			gid := oldConfig.Shards[shId]

			servers, ok := oldConfig.Groups[gid]
			if !ok {
				panic("freezeMovingShards: old config has no group for shard that needs to be frozen, idk how to handle it yet")
			}

			grpClerk := shardgrp.MakeClerk(sck.clnt, servers)
			state, err := grpClerk.FreezeShard(shardcfg.Tshid(shId), oldConfig.Num)
			if err != rpc.OK {
				panic(fmt.Sprintf("freezeMovingShards: FreezeShard RPC failed for shard %d, gid %d, err %v", shId, gid, err))
			}

			transfers = append(transfers, Transfer{fromG: gid, toG: newGid, shId: shardcfg.Tshid(shId), state: state})
		}
	}

	return transfers
}

func (sck *ShardCtrler) installShards(newConfig *shardcfg.ShardConfig, transfers []Transfer) {
	for _, transfer := range transfers {
		serversTo, ok := newConfig.Groups[transfer.toG]
		if !ok {
			panic("installShards: new config has no group for shard that needs to be installed, idk how to handle it yet")
		}

		grpcClerk := shardgrp.MakeClerk(sck.clnt, serversTo)
		err := grpcClerk.InstallShard(transfer.shId, transfer.state, newConfig.Num)
		if err != rpc.OK {
			panic(fmt.Sprintf("installShards: InstallShard RPC failed for shard %d, from gid %d to gid %d, err %v", transfer.shId, transfer.fromG, transfer.toG, err))
		}
	}
}

func (sck *ShardCtrler) deleteShards(oldConfig *shardcfg.ShardConfig, transfers []Transfer) {
	for _, transfer := range transfers {
		serversFrom, ok := oldConfig.Groups[transfer.fromG]
		if !ok {
			panic("deleteShards: old config has no group for shard that needs to be deleted, idk how to handle it yet")
		}

		grpcClerk := shardgrp.MakeClerk(sck.clnt, serversFrom)
		err := grpcClerk.DeleteShard(transfer.shId, oldConfig.Num)
		if err != rpc.OK {
			panic(fmt.Sprintf("deleteShards: DeleteShard RPC failed for shard %d, from gid %d to gid %d, err %v", transfer.shId, transfer.fromG, transfer.toG, err))
		}
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
