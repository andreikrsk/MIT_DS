package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	kvsrv "6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	"6.5840/shardkv1/utils"
	tester "6.5840/tester1"
	"github.com/google/uuid"
)

const (
	CONFIG_KEY      = "config"
	NEXT_CONFIG_KEY = "nextConfig"
)

// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk

	killed int32 // set by Kill()

	me uuid.UUID
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
	sck.me = uuid.New()

	// Your code here.
	return sck
}

// The tester calls InitController() before starting a new
// controller. In part A, this method doesn't need to do anything. In
// B and C, this method implements recovery.
func (sck *ShardCtrler) InitController() {
	value, curVersion, curErr := sck.Get(CONFIG_KEY)
	utils.DPrintf("[schrdctrler][%s]  Query[cur]: get value %v version %v err %v", sck.me, value, curVersion, curErr)

	nextValue, nextVersion, nextErr := sck.Get(NEXT_CONFIG_KEY)
	utils.DPrintf("[schrdctrler][%s] Query[next]: get value %v version %v err %v", sck.me, nextValue, nextVersion, nextErr)

	if curErr == rpc.ErrNoKey && curErr == nextErr {
		utils.DPrintf("[schrdctrler][%s] InitController: both current and next config not found, starting with empty state", sck.me)
		return
	}

	if curVersion == nextVersion {
		utils.DPrintf("[schrdctrler][%s] InitController: cur version %v equals next version %v. The last operation was successful.", sck.me, curVersion, nextVersion)
		return
	}

	utils.DPrintf("[schrdctrler][%s] InitController: current and next configs are not in sync, need to recover", sck.me)

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
		utils.DPrintf("[schrdctrler][%s] ChangeConfigTo Query[cur]: get value %v version %v err %v", sck.me, value, curVersion, curErr)

		nextValue, nextVersion, nextErr := sck.Get(NEXT_CONFIG_KEY)
		utils.DPrintf("[schrdctrler][%s] ChangeConfigTo Query[next]: get value %v version %v err %v", sck.me, nextValue, nextVersion, nextErr)

		if curErr == rpc.ErrNoKey && curErr == nextErr {
			utils.DPrintf("[schrdctrler][%s] ChangeConfigTo: both current and next config not found, starting with empty state", sck.me)

			err := sck.handleBothEmptyConfig(new)
			if err != rpc.OK {
				utils.DPrintf("[schrdctrler][%s] ChangeConfigTo: failed to handle both empty config, err = %v, retrying", sck.me, err)
				continue
			}

			return
		}

		// the next version should be non-decreasing
		if nextErr == rpc.OK && (rpc.Tversion)(new.Num) < nextVersion {
			utils.DPrintf("[schrdctrler][%s] ChangeConfigTo: next config version %v is greater than new config version %v, returning", sck.me, nextVersion, new.Num)

			return
		}

		if curErr == rpc.ErrNoKey && nextErr == rpc.OK {
			utils.DPrintf("[schrdctrler][%s] ChangeConfigTo: no current config found, but next config found, setting current config to next config value", sck.me)

			if (rpc.Tversion)(new.Num) > nextVersion {
				utils.DPrintf("[sahrdctrler][%s] ChangeConfigTo: next config version %v equals new config version %v, but the old controller is still in progress. Need to retry", sck.me, nextVersion, new.Num)
				continue
			}

			err := sck.handleNoCurrentNextExists(shardcfg.FromString(nextValue))
			if err != rpc.OK {
				utils.DPrintf("[schrdctrler][%s] ChangeConfigTo: failed to handle no current config but next config exists, err = %v, retrying", sck.me, err)
				continue
			}

			return
		}

		if curVersion == nextVersion {
			utils.DPrintf("[schrdctrler][%s] ChangeConfigTo: cur version %v equals next version %v. The last operation was successful.", sck.me, curVersion, nextVersion)

			if (rpc.Tversion)(new.Num) == nextVersion {
				utils.DPrintf("[sahrdctrler][%s] ChangeConfigTo: next config version %v equals new config version %v. Both current and next config are in sync. Noting to do. Returning.", sck.me, nextVersion, new.Num)

				return
			}

			err := sck.handleSameVersions(shardcfg.FromString(value), new)
			if err != rpc.OK {
				utils.DPrintf("[schrdctrler][%s] ChangeConfigTo: failed to handle same versions, err = %v, retrying", sck.me, err)
				continue
			}

			return
		}

		if curVersion == nextVersion-1 {
			utils.DPrintf("[schrdctrler][%s] ChangeConfigTo: current and next config found, but versions are different, recovering by transfering shards according to the next config and then set current config to next config", sck.me)

			err := sck.handleDifferentVersions(shardcfg.FromString(value), shardcfg.FromString(nextValue))
			if err != rpc.OK {
				utils.DPrintf("[schrdctrler][%s] ChangeConfigTo: failed to handle different versions, err = %v, retrying", sck.me, err)
				continue
			}

			return
		}

		if math.Abs(float64(curVersion-nextVersion)) > 1 {
			panic(fmt.Sprintf("ChangeConfigTo: Corrupted state. Current: %v, Next: %v", curVersion, nextVersion))

		}

		panic(fmt.Sprintf("ChangeConfigTo: unexpected case where current and next config versions are different but not covered by the previous cases, idk how to handle it yet. Current: %v, Next: %v", curVersion, nextVersion))
	}
}

func (sck *ShardCtrler) handleBothEmptyConfig(new *shardcfg.ShardConfig) rpc.Err {
	newVersion := (rpc.Tversion)(new.Num - 1)

	err := sck.setCofnigValueByKey(NEXT_CONFIG_KEY, new, newVersion)
	if err != rpc.OK {
		utils.DPrintf("[schrdctrler][%s] handleBothEmptyConfig: failed to set next config value, err = %v, retrying", sck.me, err)
		return err
	}

	err = sck.setCofnigValueByKey(CONFIG_KEY, new, newVersion)
	if err != rpc.OK {
		utils.DPrintf("[schrdctrler][%s] handleBothEmptyConfig: failed to set current config value, err = %v, retrying", sck.me, err)
		return err
	}

	return rpc.OK
}

func (sck *ShardCtrler) handleNoCurrentNextExists(new *shardcfg.ShardConfig) rpc.Err {
	newVersion := (rpc.Tversion)(new.Num - 1)

	err := sck.setCofnigValueByKey(CONFIG_KEY, new, newVersion)
	if err != rpc.OK {
		utils.DPrintf("[schrdctrler][%s] handleNoCurrentNextExists: failed to set current config value, err = %v, retrying", sck.me, err)
		return err
	}

	return rpc.OK
}

func (sck *ShardCtrler) handleSameVersions(old, new *shardcfg.ShardConfig) rpc.Err {
	version := (rpc.Tversion)(new.Num - 1)

	err := sck.setCofnigValueByKey(NEXT_CONFIG_KEY, new, version)
	if err != rpc.OK {
		utils.DPrintf("[schrdctrler][%s] handleSameVersions: failed to set next config value, err = %v, retrying", sck.me, err)
		return err
	}

	err = sck.transferShards(old, new)
	if err != rpc.OK {
		utils.DPrintf("[schrdctrler][%s] handleSameVersions: failed to transfer shards for new config, err = %v, retrying", sck.me, err)
		return err
	}

	err = sck.setCofnigValueByKey(CONFIG_KEY, new, version)
	if err != rpc.OK {
		utils.DPrintf("[schrdctrler][%s] handleSameVersions: failed to set current config value, err = %v, retrying", sck.me, err)
		return err
	}

	return rpc.OK

}
func (sck *ShardCtrler) handleDifferentVersions(old, new *shardcfg.ShardConfig) rpc.Err {
	newVersion := (rpc.Tversion)(new.Num - 1)
	curVersion := newVersion

	err := sck.transferShards(old, new)
	if err != rpc.OK {
		utils.DPrintf("[schrdctrler][%s] handleDifferentVersions: failed to transfer shards for new config, err = %v, retrying", sck.me, err)
		return err
	}

	err = sck.setCofnigValueByKey(CONFIG_KEY, new, curVersion)
	if err != rpc.OK {
		utils.DPrintf("[schrdctrler][%s] handleDifferentVersions: failed to set current config value, err = %v, retrying", sck.me, err)
		return err
	}

	return rpc.OK
}

func (sck *ShardCtrler) setCofnigValueByKey(key string, config *shardcfg.ShardConfig, version rpc.Tversion) rpc.Err {
	err := sck.tryPutValueWithRetires(key, config.String(), version)
	if err == rpc.OK {
		utils.DPrintf("[schrdctrler][%s] Successfully put %s config with version %v", sck.me, key, version)

		return err
	}

	utils.DPrintf("[schrdctrler][%s] Failed to put %s config with version %v, err = %v, retrying", sck.me, key, version, err)

	return err
}

func (sck *ShardCtrler) transferShards(old, new *shardcfg.ShardConfig) rpc.Err {
	err := sck.tryTransferShards(old, new)
	if err == rpc.OK {
		utils.DPrintf("[schrdctrler][%s]  Successfully transfered shards for new cofnig", sck.me)

		return err
	}

	utils.DPrintf("[schrdctrler][%s] Failed to transfer shards for new config, err = %v, retrying", sck.me, err)

	return err
}

func (sck *ShardCtrler) tryTransferShards(old, new *shardcfg.ShardConfig) rpc.Err {
	transfers := sck.prepareTransfers(old, new)

	wg := sync.WaitGroup{}
	wg.Add(len(transfers))

	firstErr := atomic.Pointer[rpc.Err]{}
	ok := (rpc.Err)(rpc.OK)
	firstErr.Store(&ok)

	for _, transfer := range transfers {
		go func(transfer Transfer) {
			defer wg.Done()
			utils.DPrintf("[schrdctrler][%s] Starting transfer for shard %d from group %d to group %d", sck.me, transfer.shId, transfer.fromG, transfer.toG)
			err := sck.freezeShard(old, new, &transfer)
			if err != rpc.OK {
				utils.DPrintf("[schrdctrler][%s] Failed to freeze shards", sck.me)

				firstErr.CompareAndSwap(&ok, &err)
				return
			}

			err = sck.installShard(new, transfer)
			if err != rpc.OK {
				utils.DPrintf("[schrdctrler][%s] Failed to install shards", sck.me)

				firstErr.CompareAndSwap(&ok, &err)
				return
			}

			err = sck.deleteShards(old, new, transfer)
			if err != rpc.OK {
				utils.DPrintf("[schrdctrler][%s] Failed to delete shards", sck.me)

				firstErr.CompareAndSwap(&ok, &err)
				return
			}
		}(transfer)
	}

	wg.Wait()

	utils.DPrintf("[schrdctrler][%s] Finished all transfers for new config, first error: %v", sck.me, *firstErr.Load())

	return *firstErr.Load()
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

	utils.DPrintf("[schrdctrler][%s] tryPutValueWithRetires: done trying to put value %v with err %v after %d retries", sck.me, value, err, retries)

	return err
}

// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	// Your code here.
	for {
		value, version, err := sck.Get(CONFIG_KEY)
		utils.DPrintf("[schrdctrler][%s] loadConfigFromKvSrv: get value %v version %v err %v", sck.me, value, version, err)

		if err == rpc.OK {
			return shardcfg.FromString(value)
		}

		utils.DPrintf("[schrdctrler][%s] loadConfigFromKvSrv: failed to get config from kvsrv, retrying...", sck.me)
		time.Sleep(250 * time.Millisecond)
	}
}
