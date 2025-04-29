package lock

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

const Debug = false
const UseChannel = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck  kvtest.IKVClerk
	l   string
	lid string
}

type ICond interface {
	Wait()
	SignalAll()
}

var mu = sync.Mutex{}
var cond = sync.Cond{L: &mu}

type ClassicCond struct{}

func (c *ClassicCond) Wait() {
	cond.Wait()
}
func (c *ClassicCond) SignalAll() {
	cond.Broadcast()
}

var classicC = &ClassicCond{}

var cunlocked = make(chan struct{}, 1)

type ChannelCond struct{}

func (c *ChannelCond) Wait() {
	<-cunlocked
}
func (c *ChannelCond) SignalAll() {
	select {
	case cunlocked <- struct{}{}:
	default:
	}
}

var channelC = &ChannelCond{}

// MakeLock creates a lock object.  The lock object is a wrapper
// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck, l: l, lid: kvtest.RandValue(8)}
	return lk
}

func (lk *Lock) Acquire() {
	if UseChannel {
		acquireChanLogic(lk)
	} else {
		acquireMuLogic(lk)
	}
}

func acquireMuLogic(lk *Lock) {
	mu.Lock()
	defer mu.Unlock()
	acquireLogic(classicC, lk)
}

func acquireChanLogic(lk *Lock) {
	acquireLogic(channelC, lk)
}

func acquireLogic(cond ICond, lk *Lock) {
	success := false
	for !success {
		lstate, ver, err := lk.ck.Get(lk.l)
		if err == rpc.ErrNoKey {
			DPrintf("[%s]. Trying to lock. No lock existed.", lk.lid)
			success = trySwitch(lk.ck, lk.l, lk.lid, 0)
		} else {
			if lstate == "" {
				DPrintf("[%s]. Trying to lock.", lk.lid)
				success = trySwitch(lk.ck, lk.l, lk.lid, ver)
			} else {
				DPrintf("[%s]. Lock is already acquired.", lk.lid)
				cond.Wait()
				DPrintf("[%s]. After wait().", lk.lid)
			}
		}
	}
}

func trySwitch(ck kvtest.IKVClerk, l string, state string, ver rpc.Tversion) bool {
	err := ck.Put(l, state, ver)
	if err == rpc.ErrVersion {
		DPrintf("[%s]. Lock version mismatch. Can't change lock state.", state)
		return false
	} else if err == rpc.ErrMaybe {
		DPrintf("[%s]. Lock version mismatch. But state might be changed.", state)
		return evaluateStateAfterMaybe(ck, l, state)
	} else {
		DPrintf("[%s]. State changed.", state)
		return true
	}
}

func evaluateStateAfterMaybe(ck kvtest.IKVClerk, l string, state string) bool {
	lstate, _, err := ck.Get(l)
	if err == rpc.ErrNoKey {
		DPrintf("[%s]. Lock not found. Cannot change state.", state)
		return false
	} else {
		if state == lstate {
			DPrintf("[%s]. Lock state changed after retries.", state)
			return true
		} else {
			DPrintf("[%s]. Lock state has not been changed", state)
			return false
		}
	}
}

func (lk *Lock) Release() {
	if UseChannel {
		releaseChanLogic(lk)
	} else {
		releaseMuLogic(lk)
	}
}

func releaseMuLogic(lk *Lock) {
	mu.Lock()
	defer mu.Unlock()
	releaseLogic(classicC, lk)
}

func releaseChanLogic(lk *Lock) {
	releaseLogic(channelC, lk)
}

func releaseLogic(cond ICond, lk *Lock) {
	lstate, ver, err := lk.ck.Get(lk.l)
	if err == rpc.ErrNoKey {
		DPrintf("[%s]. Lock not found. Cannot release.", lk.lid)
	} else if lstate == "" {
		DPrintf("[%s]. Lock not acquired. Cannot release.", lk.lid)
	} else if lstate != lk.lid {
		DPrintf("[%s]. Lock not acquired by me. Cannot release.", lk.lid)
	} else if lstate == lk.lid {
		DPrintf("[%s]. Trying to reset lock.", lk.lid)
		if unlocked := trySwitch(lk.ck, lk.l, "", ver); unlocked {
			cond.SignalAll()
		}
	}
}
