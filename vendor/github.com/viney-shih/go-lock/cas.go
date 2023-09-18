package lock

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"golang.org/x/sync/semaphore"
)

// CASMutex is the struct implementing RWMutex with CAS mechanism.
type CASMutex struct {
	state     casState
	turnstile *semaphore.Weighted

	broadcastChan chan struct{}
	broadcastMut  sync.RWMutex
}

// NewCASMutex returns CASMutex
func NewCASMutex() *CASMutex {
	return &CASMutex{
		state:         casStateNoLock,
		turnstile:     semaphore.NewWeighted(1),
		broadcastChan: make(chan struct{}),
	}
}

type casState int32

const (
	casStateUndefined casState = iota - 2 // -2
	casStateWriteLock                     // -1
	casStateNoLock                        // 0
	casStateReadLock                      // >= 1
)

func (m *CASMutex) getState(n int32) casState {
	switch st := casState(n); {
	case st == casStateWriteLock:
		fallthrough
	case st == casStateNoLock:
		return st
	case st >= casStateReadLock:
		return casStateReadLock
	default:
		// actually, it should not happened.
		return casStateUndefined
	}
}

func (m *CASMutex) listen() <-chan struct{} {
	m.broadcastMut.RLock()
	defer m.broadcastMut.RUnlock()

	return m.broadcastChan
}

func (m *CASMutex) broadcast() {
	newCh := make(chan struct{})

	m.broadcastMut.Lock()
	ch := m.broadcastChan
	m.broadcastChan = newCh
	m.broadcastMut.Unlock()

	close(ch)
}

func (m *CASMutex) tryLock(ctx context.Context) bool {
	for {
		broker := m.listen()
		if atomic.CompareAndSwapInt32(
			(*int32)(unsafe.Pointer(&m.state)),
			int32(casStateNoLock),
			int32(casStateWriteLock),
		) {
			return true
		}

		if ctx == nil {
			return false
		}

		select {
		case <-ctx.Done():
			// timeout or cancellation
			return false
		case <-broker:
			// waiting for signal triggered by m.broadcast() and trying again.
		}
	}
}

// TryLockWithContext attempts to acquire the lock, blocking until resources
// are available or ctx is done (timeout or cancellation).
func (m *CASMutex) TryLockWithContext(ctx context.Context) bool {
	if err := m.turnstile.Acquire(ctx, 1); err != nil {
		// Acquire failed due to timeout or cancellation
		return false
	}

	defer m.turnstile.Release(1)

	return m.tryLock(ctx)
}

// Lock acquires the lock.
// If it is currently held by others, Lock will wait until it has a chance to acquire it.
func (m *CASMutex) Lock() {
	ctx := context.Background()

	m.TryLockWithContext(ctx)
}

// TryLock attempts to acquire the lock without blocking.
// Return false if someone is holding it now.
func (m *CASMutex) TryLock() bool {
	if !m.turnstile.TryAcquire(1) {
		return false
	}

	defer m.turnstile.Release(1)

	return m.tryLock(nil)
}

// TryLockWithTimeout attempts to acquire the lock within a period of time.
// Return false if spending time is more than duration and no chance to acquire it.
func (m *CASMutex) TryLockWithTimeout(duration time.Duration) bool {
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	return m.TryLockWithContext(ctx)
}

// Unlock releases the lock.
func (m *CASMutex) Unlock() {
	if ok := atomic.CompareAndSwapInt32(
		(*int32)(unsafe.Pointer(&m.state)),
		int32(casStateWriteLock),
		int32(casStateNoLock),
	); !ok {
		panic("Unlock failed")
	}

	m.broadcast()
}

func (m *CASMutex) rTryLock(ctx context.Context) bool {
	for {
		broker := m.listen()
		n := atomic.LoadInt32((*int32)(unsafe.Pointer(&m.state)))
		st := m.getState(n)
		switch st {
		case casStateNoLock, casStateReadLock:
			if atomic.CompareAndSwapInt32((*int32)(unsafe.Pointer(&m.state)), n, n+1) {
				return true
			}
		}

		if ctx == nil {
			return false
		}

		select {
		case <-ctx.Done():
			// timeout or cancellation
			return false
		default:
			switch st {
			// read-lock failed due to concurrence issue, try again immediately
			case casStateNoLock, casStateReadLock:
				runtime.Gosched() // allow other goroutines to do stuff.
				continue
			}
		}

		select {
		case <-ctx.Done():
			// timeout or cancellation
			return false
		case <-broker:
			// waiting for signal triggered by m.broadcast() and trying again.
		}
	}
}

// RTryLockWithContext attempts to acquire the read lock, blocking until resources
// are available or ctx is done (timeout or cancellation).
func (m *CASMutex) RTryLockWithContext(ctx context.Context) bool {
	if err := m.turnstile.Acquire(ctx, 1); err != nil {
		// Acquire failed due to timeout or cancellation
		return false
	}

	m.turnstile.Release(1)

	return m.rTryLock(ctx)
}

// RLock acquires the read lock.
// If it is currently held by others writing, RLock will wait until it has a chance to acquire it.
func (m *CASMutex) RLock() {
	ctx := context.Background()

	m.RTryLockWithContext(ctx)
}

// RTryLock attempts to acquire the read lock without blocking.
// Return false if someone is writing it now.
func (m *CASMutex) RTryLock() bool {
	if !m.turnstile.TryAcquire(1) {
		return false
	}

	m.turnstile.Release(1)

	return m.rTryLock(nil)
}

// RTryLockWithTimeout attempts to acquire the read lock within a period of time.
// Return false if spending time is more than duration and no chance to acquire it.
func (m *CASMutex) RTryLockWithTimeout(duration time.Duration) bool {
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	return m.RTryLockWithContext(ctx)
}

// RUnlock releases the read lock.
func (m *CASMutex) RUnlock() {
	n := atomic.AddInt32((*int32)(unsafe.Pointer(&m.state)), -1)
	switch m.getState(n) {
	case casStateUndefined, casStateWriteLock:
		panic("RUnlock failed")
	case casStateNoLock:
		m.broadcast()
	}
}

// RLocker returns a Locker interface that implements the Lock and Unlock methods
// by calling CASMutex.RLock and CASMutex.RUnlock.
func (m *CASMutex) RLocker() sync.Locker {
	return (*rlocker)(m)
}

type rlocker CASMutex

func (r *rlocker) Lock()   { (*CASMutex)(r).RLock() }
func (r *rlocker) Unlock() { (*CASMutex)(r).RUnlock() }
