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
	INITIAL_CONFIG_VERSION = 0
	CONFIG_KEY             = "config"
	NEXT_CONFIG_KEY        = "nextConfig"
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
	value, curVersion, curErr := sck.Get(CONFIG_KEY)
	utils.DPrintf("[schrdctrler] Query[cur]: get value %v version %v err %v", value, curVersion, curErr)

	nextValue, nextVersion, nextErr := sck.Get(NEXT_CONFIG_KEY)
	utils.DPrintf("[schrdctrler] Query[next]: get value %v version %v err %v", nextValue, nextVersion, nextErr)

	if curErr == rpc.ErrNoKey && curErr == nextErr {
		utils.DPrintf("[schrdctrler] InitController: both current and next config not found, starting with empty state")
		return
	}

	if curVersion == nextVersion {
		utils.DPrintf("[schrdctrler] InitController: cur version %v equals next version %v. The last operation was successful.", curVersion, nextVersion)
		return
	}

	utils.DPrintf("[schrdctrler] InitController: current and next configs are not in sync, need to recover")

	// two cases here
	// 1. no config, next config found - we start with empty state, need to set current config to next config value
	// 2. current and next config found, but versions are different - we need to recover by transfering shards according to the next config and then set current config to next config
	sck.ChangeConfigTo(shardcfg.FromString(nextValue))
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
	for {
		value, curVersion, curErr := sck.Get(CONFIG_KEY)
		utils.DPrintf("[schrdctrler] Query[cur]: get value %v version %v err %v", value, curVersion, curErr)

		nextValue, nextVersion, nextErr := sck.Get(NEXT_CONFIG_KEY)
		utils.DPrintf("[schrdctrler] Query[next]: get value %v version %v err %v", nextValue, nextVersion, nextErr)

		if curErr == rpc.ErrNoKey && curErr == nextErr {
			utils.DPrintf("[schrdctrler] InitController: both current and next config not found, starting with empty state")

			err := sck.handleBothEmptyConfig(new)
			if err != rpc.OK {
				utils.DPrintf("[schrdctrler] InitController: failed to handle both empty config, err = %v, retrying", err)
				continue
			}

			return
		}

		if curErr == rpc.ErrNoKey && nextErr == rpc.OK {
			utils.DPrintf("[schrdctrler] InitController: no current config found, but next config found, setting current config to next config value")

			err := sck.handleNoCurrentNextExists(new)
			if err != rpc.OK {
				utils.DPrintf("[schrdctrler] InitController: failed to handle no current config but next config exists, err = %v, retrying", err)
				continue
			}

			return
		}

		if curVersion == nextVersion {
			utils.DPrintf("[schrdctrler] InitController: cur version %v equals next version %v. The last operation was successful.", curVersion, nextVersion)
			err := sck.handleSameVersions(shardcfg.FromString(value), new, curVersion, nextVersion)
			if err != rpc.OK {
				utils.DPrintf("[schrdctrler] InitController: failed to handle same versions, err = %v, retrying", err)
				continue
			}

			return
		}

		if curVersion != nextVersion {
			utils.DPrintf("[schrdctrler] InitController: current and next config found, but versions are different, recovering by transfering shards according to the next config and then set current config to next config")
			err := sck.handleDifferentVersions(shardcfg.FromString(value), new, curVersion)
			if err != rpc.OK {
				utils.DPrintf("[schrdctrler] InitController: failed to handle different versions, err = %v, retrying", err)
				continue
			}

			return
		}

		panic("InitController: unexpected case where current and next config versions are different but not covered by the previous cases, idk how to handle it yet")
	}
}

func (sck *ShardCtrler) handleBothEmptyConfig(new *shardcfg.ShardConfig) rpc.Err {
	err := sck.setCofnigValueByKey(NEXT_CONFIG_KEY, shardcfg.FromString(new.String()), INITIAL_CONFIG_VERSION)
	if err != rpc.OK {
		utils.DPrintf("[schrdctrler] InitController: failed to set next config value, err = %v, retrying", err)
		return err
	}
	err = sck.setCofnigValueByKey(CONFIG_KEY, shardcfg.FromString(new.String()), INITIAL_CONFIG_VERSION)
	if err != rpc.OK {
		utils.DPrintf("[schrdctrler] InitController: failed to set current config value, err = %v, retrying", err)
		return err
	}
	return rpc.OK
}

func (sck *ShardCtrler) handleNoCurrentNextExists(new *shardcfg.ShardConfig) rpc.Err {
	err := sck.setCofnigValueByKey(CONFIG_KEY, new, INITIAL_CONFIG_VERSION)
	if err != rpc.OK {
		utils.DPrintf("[schrdctrler] InitController: failed to set current config value, err = %v, retrying", err)
		return err
	}

	return rpc.OK
}

func (sck *ShardCtrler) handleSameVersions(old, new *shardcfg.ShardConfig, curVersion, nextVersion rpc.Tversion) rpc.Err {
	err := sck.setCofnigValueByKey(NEXT_CONFIG_KEY, new, nextVersion)
	if err != rpc.OK {
		utils.DPrintf("[schrdctrler] InitController: failed to set next config value, err = %v, retrying", err)
		return err
	}
	err = sck.transferShards(old, new)
	if err != rpc.OK {
		utils.DPrintf("[schrdctrler] InitController: failed to transfer shards for new config, err = %v, retrying", err)
		return err
	}
	err = sck.setCofnigValueByKey(CONFIG_KEY, new, curVersion)
	if err != rpc.OK {
		utils.DPrintf("[schrdctrler] InitController: failed to set current config value, err = %v, retrying", err)
		return err
	}

	return rpc.OK

}
func (sck *ShardCtrler) handleDifferentVersions(old, new *shardcfg.ShardConfig, curVersion rpc.Tversion) rpc.Err {
	err := sck.transferShards(old, new)
	if err != rpc.OK {
		utils.DPrintf("[schrdctrler] InitController: failed to transfer shards for new config, err = %v, retrying", err)
		return err
	}
	err = sck.setCofnigValueByKey(CONFIG_KEY, new, curVersion)
	if err != rpc.OK {
		utils.DPrintf("[schrdctrler] InitController: failed to set current config value, err = %v, retrying", err)
		return err
	}

	return rpc.OK
}

func (sck *ShardCtrler) setCofnigValueByKey(key string, config *shardcfg.ShardConfig, version rpc.Tversion) rpc.Err {
	err := sck.tryPutValueWithRetires(key, config.String(), version)
	if err == rpc.OK {
		utils.DPrintf("[schrdctrler] Successfully put %s config with version %v", key, version)

		return err
	}

	utils.DPrintf("[schrdctrler] Failed to put %s config with version %v, err = %v, retrying", key, version, err)

	return err
}

func (sck *ShardCtrler) transferShards(old, new *shardcfg.ShardConfig) rpc.Err {
	err := sck.tryTransferShards(old, new)
	if err == rpc.OK {
		utils.DPrintf("[schrdctrler] Successfully transfered shards for new cofnig")

		return err
	}

	utils.DPrintf("[schrdctrler] Failed to transfer shards for new config, err = %v, retrying", err)

	return err
}

func (sck *ShardCtrler) tryTransferShards(old, new *shardcfg.ShardConfig) rpc.Err {
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

	return err
}

func (sck *ShardCtrler) deleteShards(oldConfig, newConfig *shardcfg.ShardConfig, transfer Transfer) rpc.Err {
	serversFrom, ok := oldConfig.Groups[transfer.fromG]
	if !ok {
		panic("deleteShards: old config has no group for shard that needs to be deleted, idk how to handle it yet")
	}

	grpcClerk := shardgrp.MakeClerk(sck.clnt, serversFrom)
	err := grpcClerk.DeleteShard(transfer.shId, newConfig.Num)

	return err
}

// possible errors: rpc.OK, rpc.ErrVersion
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
	value, version, err := sck.Get(CONFIG_KEY)
	utils.DPrintf("Query: get value %v version %v err %v", value, version, err)

	if err == rpc.OK {
		return shardcfg.FromString(value)
	}

	return nil
}
