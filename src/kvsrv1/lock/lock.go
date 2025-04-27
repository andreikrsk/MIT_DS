package lock

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

const Debug = false

var mu = sync.Mutex{}
var cond = sync.Cond{L: &mu}

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

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck, l: l, lid: kvtest.RandValue(8)}
	// lk.cond = sync.NewCond(&lk.mu)
	// You may add code here
	return lk
}

func (lk *Lock) Acquire() {
	mu.Lock()
	defer mu.Unlock()

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
		return false
	} else {
		DPrintf("[%s]. State changed.", state)
		return true
	}
}

func (lk *Lock) Release() {
	mu.Lock()
	defer mu.Unlock()

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
			cond.Broadcast()
		}
	}
}
