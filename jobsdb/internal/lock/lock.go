package lock

import (
	"context"
	"sync"
	"time"
)

// DSListLockToken represents proof that a list lock has been acquired
type DSListLockToken interface {
	listLockToken()
}

// DSListLocker
type DSListLocker struct {
	m sync.RWMutex
}

// RLock acquires a read lock
func (r *DSListLocker) RLock() {
	r.m.RLock()
}

// RLock releases a read lock
func (r *DSListLocker) RUnlock() {
	r.m.RUnlock()
}

// WithLock acquires a lock for the duration that the provided function
// is being executed. A token as proof of the lock is passed to the function.
func (r *DSListLocker) WithLock(f func(l DSListLockToken)) {
	r.m.Lock()
	defer r.m.Unlock()
	f(&listLockToken{})
}

// WithCtxAwareLock tries to acquires a lock until the context is cancelled or timed out.
// It return 'false' if context is cancelled or timed out while acquiring lock.
func (r *DSListLocker) WithCtxAwareLock(ctx context.Context, f func(l DSListLockToken)) (ok bool) {
	for {
		select {
		case <-time.After(time.Millisecond):
			if r.m.TryLock() {
				defer r.m.Unlock()
				f(&listLockToken{})
				return true
			}
		case <-ctx.Done():
			return false
		}
	}
}

// AsyncLock acquires a lock until the token is returned to the receiving channel
func (r *DSListLocker) AsyncLock() (DSListLockToken, chan<- DSListLockToken) {
	acquireDsListLock := make(chan DSListLockToken)
	releaseDsListLock := make(chan DSListLockToken)

	go func() {
		r.WithLock(func(l DSListLockToken) {
			acquireDsListLock <- l
			<-releaseDsListLock
		})
	}()
	dsListLock := <-acquireDsListLock
	return dsListLock, releaseDsListLock
}

// AsyncLockWithCtx tries to acquires a lock until context is canceled or timed out.
// And if succeeds, lock is acquired until the token is returned to the receiving channel.
func (r *DSListLocker) AsyncLockWithCtx(ctx context.Context) (DSListLockToken, chan<- DSListLockToken) {
	acquireDsListLock := make(chan DSListLockToken)
	releaseDsListLock := make(chan DSListLockToken)
	go func() {
		r.WithCtxAwareLock(ctx, func(l DSListLockToken) {
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

type listLockToken struct{}

func (*listLockToken) listLockToken() {
	// no-op
}
