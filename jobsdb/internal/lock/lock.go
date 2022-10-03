package lock

import (
	"context"

	golock "github.com/viney-shih/go-lock"
)

// LockToken represents proof that a list lock has been acquired
type LockToken interface {
	listLockToken()
}

// Locker
type Locker struct {
	m *golock.CASMutex
}

func NewLocker() *Locker {
	return &Locker{
		m: golock.NewCASMutex(),
	}
}

// RLock acquires a read lock
func (r *Locker) RLock() {
	r.m.RLock()
}

// RTryLockWithCtx tries to acquires a read lock with context and returns false if context times out, otherwise returns true.
func (r *Locker) RTryLockWithCtx(ctx context.Context) bool {
	return r.m.RTryLockWithContext(ctx)
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

// TryWithCtxAwareLock tries to acquires a lock until it succeeds or context times out. If it fails, return value is false otherwise true. And, executes the function `f`, if lock is acquired.
// A token as proof of the lock is passed to the function.
func (r *Locker) TryWithCtxAwareLock(ctx context.Context, f func(l LockToken)) bool {
	if r.m.TryLockWithContext(ctx) {
		defer r.m.Unlock()
		f(&lockToken{})
		return true
	}
	return false
}

// AsyncLock acquires a lock until the token is returned to the receiving channel
func (r *Locker) AsyncLock() (LockToken, chan<- LockToken) {
	acquireLock := make(chan LockToken)
	releaseLock := make(chan LockToken)

	go func() {
		r.WithLock(func(l LockToken) {
			acquireLock <- l
			<-releaseLock
		})
	}()
	dsListLock := <-acquireLock
	return dsListLock, releaseLock
}

type lockToken struct{}

func (*lockToken) listLockToken() {
	// no-op
}
