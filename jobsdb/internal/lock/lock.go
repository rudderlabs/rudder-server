package lock

import (
	"context"
	"sync"
	"time"
)

// LockToken represents proof that a list lock has been acquired
type LockToken interface {
	lockToken()
}

// Locker
type Locker struct {
	m sync.RWMutex
}

// RLock acquires a read lock
func (r *Locker) RLock() {
	r.m.RLock()
}

// RLock releases a read lock
func (r *Locker) RUnlock() {
	r.m.RUnlock()
}

// WithLock acquires a lock for the duration that the provided function
// is being executed. A token as proof of the lock is passed to the function.
func (r *Locker) WithLock(f func(l LockToken)) {
	r.m.Lock()
	defer r.m.Unlock()
	f(&lockToken{})
}

// WithCtxAwareLock tries to acquires a lock until the context is cancelled or timed out.
// It return 'false' if context is cancelled or timed out while acquiring lock.
func (r *Locker) WithCtxAwareLock(ctx context.Context, f func(l LockToken)) (ok bool) {
	for {
		select {
		case <-time.After(time.Millisecond):
			if r.m.TryLock() {
				defer r.m.Unlock()
				f(&lockToken{})
				return true
			}
		case <-ctx.Done():
			return false
		}
	}
}

// AsyncLock acquires a lock until the token is returned to the receiving channel
func (r *Locker) AsyncLock() (LockToken, chan<- LockToken) {
	acquireDsListLock := make(chan LockToken)
	releaseDsListLock := make(chan LockToken)

	go func() {
		r.WithLock(func(l LockToken) {
			acquireDsListLock <- l
			<-releaseDsListLock
		})
	}()
	dsListLock := <-acquireDsListLock
	return dsListLock, releaseDsListLock
}

// AsyncLockWithCtx tries to acquires a lock until context is canceled or timed out.
// And if succeeds, lock is acquired until the token is returned to the receiving channel.
func (r *Locker) AsyncLockWithCtx(ctx context.Context) (LockToken, chan<- LockToken) {
	acquireDsListLock := make(chan LockToken)
	releaseDsListLock := make(chan LockToken)
	go func() {
		r.WithCtxAwareLock(ctx, func(l LockToken) {
			acquireDsListLock <- l
			<-releaseDsListLock
		})
	}()
	for {
		select {
		case <-ctx.Done():
			return nil, nil
		case dsListLock := <-acquireDsListLock:
			return dsListLock, releaseDsListLock
		}
	}
}

type lockToken struct{}

func (*lockToken) lockToken() {
	// no-op
}
