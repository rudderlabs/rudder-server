package lock

import (
	"context"
	"fmt"

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

// RTryLockWithCtx tries to acquires a read lock with context and returns false if context is done, otherwise returns true.
func (r *Locker) RTryLockWithCtx(ctx context.Context) bool {
	return r.m.RTryLockWithContext(ctx)
}

// RUnlock releases a read lock
func (r *Locker) RUnlock() {
	r.m.RUnlock()
}

// TryLockWithCtx tries to acquires a lock with context and returns false if context is done, otherwise returns true.
func (r *Locker) TryLockWithCtx(ctx context.Context) bool {
	return r.m.TryLockWithContext(ctx)
}

// Unlock releases a lock
func (r *Locker) Unlock() {
	r.m.Unlock()
}

// WithLock acquires a lock for the duration that the provided function
// is being executed. A token as proof of the lock is passed to the function.
func (r *Locker) WithLock(f func(l LockToken)) {
	r.m.Lock()
	defer r.m.Unlock()
	f(&lockToken{})
}

// WithLockInCtx tries to acquires a lock until it succeeds or context times out. If it fails, return value is false otherwise true. And, executes the function `f`, if lock is acquired.
// A token as proof of the lock is passed to the function.
func (r *Locker) WithLockInCtx(ctx context.Context, f func(l LockToken) error) error {
	if r.m.TryLockWithContext(ctx) {
		defer r.m.Unlock()
		return f(&lockToken{})
	}
	return fmt.Errorf("failed to acquire a lock: %w", ctx.Err())
}

// AsyncLock acquires a lock until the token is returned to the receiving channel
func (r *Locker) AsyncLockWithCtx(ctx context.Context) (LockToken, chan<- LockToken, error) {
	type tokenOrErr struct {
		token LockToken
		err   error
	}
	async := make(chan tokenOrErr)
	release := make(chan LockToken)

	go func() {
		if err := r.WithLockInCtx(ctx, func(l LockToken) error {
			async <- tokenOrErr{token: l}
			<-release
			return nil
		}); err != nil {
			async <- tokenOrErr{err: err}
		}
	}()

	asyncLock := <-async
	if asyncLock.err != nil {
		return nil, nil, fmt.Errorf("failed to acquire a lock: %w", ctx.Err())
	}
	return asyncLock.token, release, nil
}

type lockToken struct{}

func (*lockToken) listLockToken() {
	// no-op
}
