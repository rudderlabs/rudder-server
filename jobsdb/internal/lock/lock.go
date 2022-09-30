package lock

import (
	"context"

	golock "github.com/viney-shih/go-lock"
)

// DSListLockToken represents proof that a list lock has been acquired
type DSListLockToken interface {
	listLockToken()
}

// DSListLocker
type DSListLocker struct {
	m *golock.CASMutex
}

func NewDSListLocker() *DSListLocker {
	return &DSListLocker{
		m: golock.NewCASMutex(),
	}
}

// RLock acquires a read lock
func (r *DSListLocker) RLock() {
	r.m.RLock()
}

// TryRLockWithCtx tries to acquires a read lock with context and returns false if context times out, otherwise returns true.
func (r *DSListLocker) TryRLockWithCtx(ctx context.Context) bool {
	return r.m.RTryLockWithContext(ctx)
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

// TryWithCtxAwareLock tries to acquires a lock until it succeeds or context times out. If it fails, return value is false otherwise true. And, executes the function `f`, if lock is acquired.
// A token as proof of the lock is passed to the function.
func (r *DSListLocker) TryWithCtxAwareLock(ctx context.Context, f func(l DSListLockToken)) bool {
	if r.m.TryLockWithContext(ctx) {
		defer r.m.Unlock()
		f(&listLockToken{})
		return true
	}
	return false
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

type listLockToken struct{}

func (*listLockToken) listLockToken() {
	// no-op
}
