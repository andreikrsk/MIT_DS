package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	kvsrv "6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	"6.5840/shardkv1/utils"
	tester "6.5840/tester1"
)

const (
	initialConfigVersion = 0
	configKey            = "config"
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

	utils.DPrintf("[schrdctrler] InitConfig: putting initial config %v\n", cfg.String())

	sck.ChangeConfigTo(cfg)
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	// Your code here.
	// for the first try let's just put the config
	// will handle wrong version case later
	err := sck.trySetNewConfig(new)
	for err != rpc.OK {
		utils.DPrintf("[schrdctrler] Got error = %v, retrying set new config", err)
		err = sck.trySetNewConfig(new)
		// time.Sleep(time.Duration(20) * time.Millisecond)
	}
}

func (sck *ShardCtrler) trySetNewConfig(new *shardcfg.ShardConfig) rpc.Err {
	value, curVersion, err := sck.Get(configKey)
	utils.DPrintf("[schrdctrler] Query: get value %v version %v err %v", value, curVersion, err)

	switch err {
	// if old config exists, migrating shards
	case rpc.OK:
		// the version of the config can only grow
		old := shardcfg.FromString(value)
		// NOT RELEVANT FOR THE 5A
		// fmt.Printf("Config chahge: old=%d, new=%d\n", old.Num, new.Num)
		// if old.Num+1 != new.Num {
		// fmt.Printf("VERSION SKIP: Config chahge: old=%d, new=%d\n", old.Num, new.Num)
		// }
		// if old.Num >= new.Num {
		// return rpc.OK
		// }

		err = sck.tryMigrateConfigs(old, new)
		if err != rpc.OK {
			return err
		}
		// if the config is initial one, set the version to defail value
	case rpc.ErrNoKey:
		curVersion = initialConfigVersion
	}

	return sck.tryPutValueWithRetires(configKey, new.String(), curVersion)
}

func (sck *ShardCtrler) tryMigrateConfigs(old, new *shardcfg.ShardConfig) rpc.Err {
	transfers := sck.prepareTransfers(old, new)

	// wg := sync.WaitGroup{}
	// wg.Add(len(transfers))
	var err rpc.Err
	for _, transfer := range transfers {
		// go func() {
		// defer wg.Done()
		err = sck.freezeShard(old, new, &transfer)
		if err != rpc.OK {
			utils.DPrintf("[schrdctrler] Failed to freeze shards")

			return err
		}

		err = sck.installShard(new, transfer)
		if err != rpc.OK {
			utils.DPrintf("[schrdctrler] Failed to install shards")

			return err
		}

		err = sck.deleteShards(old, new, transfer)
		if err != rpc.OK {
			utils.DPrintf("[schrdctrler] Failed to delete shards")

			return err
		}

		// return rpc.OK
		// }()
	}

	// wg.Wait()

	return rpc.OK
}

func (sck *ShardCtrler) prepareTransfers(oldConfig, newConfig *shardcfg.ShardConfig) []Transfer {
	transfers := make([]Transfer, 0)

	for shId, gid := range oldConfig.Shards {
		newGid := newConfig.Shards[shId]
		if gid != newGid {
			gid := oldConfig.Shards[shId]

			transfers = append(transfers, Transfer{fromG: gid, toG: newGid, shId: shardcfg.Tshid(shId)})
		}
	}

	return transfers
}

func (sck *ShardCtrler) freezeShard(oldConfig, newConfig *shardcfg.ShardConfig, transfer *Transfer) rpc.Err {
	servers, ok := oldConfig.Groups[transfer.fromG]
	if !ok {
		panic("freezeMovingShards: old config has no group for shard that needs to be frozen, idk how to handle it yet")
	}

	grpClerk := shardgrp.MakeClerk(sck.clnt, servers)
	state, err := grpClerk.FreezeShard(shardcfg.Tshid(transfer.shId), newConfig.Num)
	if err != rpc.OK {
		return err
	}

	transfer.state = state

	return rpc.OK
}

func (sck *ShardCtrler) installShard(newConfig *shardcfg.ShardConfig, transfer Transfer) rpc.Err {
	serversTo, ok := newConfig.Groups[transfer.toG]
	if !ok {
		panic("installShards: new config has no group for shard that needs to be installed, idk how to handle it yet")
	}

	grpcClerk := shardgrp.MakeClerk(sck.clnt, serversTo)
	err := grpcClerk.InstallShard(transfer.shId, transfer.state, newConfig.Num)
	if err != rpc.OK {
		return err
	}

	return rpc.OK
}

func (sck *ShardCtrler) deleteShards(oldConfig, newConfig *shardcfg.ShardConfig, transfer Transfer) rpc.Err {
	serversFrom, ok := oldConfig.Groups[transfer.fromG]
	if !ok {
		panic("deleteShards: old config has no group for shard that needs to be deleted, idk how to handle it yet")
	}

	grpcClerk := shardgrp.MakeClerk(sck.clnt, serversFrom)
	err := grpcClerk.DeleteShard(transfer.shId, newConfig.Num)
	if err != rpc.OK {
		return err
	}

	return rpc.OK
}

func (sck *ShardCtrler) tryPutValueWithRetires(key, value string, version rpc.Tversion) rpc.Err {
	err := sck.Put(key, value, version)
	retries := 0
	for err == rpc.ErrMaybe {
		err = sck.Put(key, value, version)
		retries++
	}

	utils.DPrintf("[schrdctrler] tryPutValueWithRetires: done trying to put value %v with err %v after %d retries", value, err, retries)

	return err
}

// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	// Your code here.
	value, version, err := sck.Get(configKey)
	utils.DPrintf("Query: get value %v version %v err %v", value, version, err)

	if err == rpc.OK {
		return shardcfg.FromString(value)
	}

	return nil
}
