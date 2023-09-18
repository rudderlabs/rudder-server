// Package go-lock is a Golang library implementing an effcient read-write lock with the following built-in mechanism:
//
// 1) Mutex with timeout mechanism
//
// 2) Trylock
//
// 3) No-starve read-write solution
//
package lock

import (
	"context"
	"sync"
	"time"
)

// Mutex is a mutual exclusion lock, and must not be copied after first use.
type Mutex interface {
	// Lock acquires the lock.
	// If it is currently held by others, Lock will wait until it has a chance to acquire it.
	Lock()
	// TryLock attempts to acquire the lock without blocking.
	// Return false if someone is holding it now.
	TryLock() bool
	// TryLockWithTimeout attempts to acquire the lock within a period of time.
	// Return false if spending time is more than duration and no chance to acquire it.
	TryLockWithTimeout(time.Duration) bool
	// TryLockWithContext attempts to acquire the lock, blocking until resources
	// are available or ctx is done (timeout or cancellation).
	TryLockWithContext(ctx context.Context) bool
	// Unlock releases the lock.
	Unlock()
}

// RWMutex is a reader/writer mutual exclusion lock. The lock can be held by an arbitrary number of
// readers or a single writer. It must not be copied after first use.
type RWMutex interface {
	Mutex

	// RLock acquires the read lock.
	// If it is currently held by others writing, RLock will wait until it has a chance to acquire it.
	RLock()
	// RTryLock attempts to acquire the read lock without blocking.
	// Return false if someone is writing it now.
	RTryLock() bool
	// RTryLockWithTimeout attempts to acquire the read lock within a period of time.
	// Return false if spending time is more than duration and no chance to acquire it.
	RTryLockWithTimeout(time.Duration) bool
	// RTryLockWithContext attempts to acquire the read lock, blocking until resources
	// are available or ctx is done (timeout or cancellation).
	RTryLockWithContext(ctx context.Context) bool
	// RUnlock releases the read lock.
	RUnlock()
	// RLocker returns a Locker interface that implements the Lock and Unlock methods
	// by calling rw.RLock and rw.RUnlock.
	RLocker() sync.Locker
}
