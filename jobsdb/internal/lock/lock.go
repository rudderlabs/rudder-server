package lock

import (
	"context"
	"fmt"
	"time"

	golock "github.com/viney-shih/go-lock"

	"github.com/rudderlabs/rudder-go-kit/stats"
)

// LockToken represents proof that a list lock has been acquired
type LockToken interface {
	listLockToken()
}

// Locker
type Locker struct {
	m *golock.CASMutex

	readLockWait            stats.Measurement
	writeLockWait           stats.Measurement
	writeLockWaitAsync      stats.Measurement
	writeLockTime           stats.Measurement
	writeLockTimeAsync      stats.Measurement
	writeLockTotalTime      stats.Measurement
	writeLockTotalTimeAsync stats.Measurement
}

func NewLocker(name, prefix string, s stats.Stats) *Locker {
	newTimer := func(metric, lockType, async string) stats.Measurement {
		return s.NewTaggedStat(metric, stats.TimerType, stats.Tags{
			"name":     name,
			"prefix":   prefix,
			"lockType": lockType,
			"async":    async,
		})
	}
	return &Locker{
		m:                       golock.NewCASMutex(),
		readLockWait:            newTimer("jobsdb_lock_wait_time", "read", "false"),
		writeLockWait:           newTimer("jobsdb_lock_wait_time", "write", "false"),
		writeLockWaitAsync:      newTimer("jobsdb_lock_wait_time", "write", "true"),
		writeLockTime:           newTimer("jobsdb_lock_time", "write", "false"),
		writeLockTimeAsync:      newTimer("jobsdb_lock_time", "write", "true"),
		writeLockTotalTime:      newTimer("jobsdb_lock_total_time", "write", "false"),
		writeLockTotalTimeAsync: newTimer("jobsdb_lock_total_time", "write", "true"),
	}
}

// RLock acquires a read lock
func (r *Locker) RLock() {
	start := time.Now()
	r.m.RLock()
	r.readLockWait.Since(start)
}

// RTryLockWithCtx tries to acquires a read lock with context and returns false if context is done, otherwise returns true.
func (r *Locker) RTryLockWithCtx(ctx context.Context) bool {
	start := time.Now()
	if r.m.RTryLockWithContext(ctx) {
		r.readLockWait.Since(start)
		return true
	}
	return false
}

// RUnlock releases a read lock
func (r *Locker) RUnlock() {
	r.m.RUnlock()
}

// TryLockWithCtx tries to acquires a lock with context and returns false if context is done, otherwise returns true.
func (r *Locker) TryLockWithCtx(ctx context.Context) bool {
	start := time.Now()
	if r.m.TryLockWithContext(ctx) {
		r.writeLockWait.Since(start)
		return true
	}
	return false
}

// Unlock releases a lock
func (r *Locker) Unlock() {
	r.m.Unlock()
}

// WithLock acquires a lock for the duration that the provided function
// is being executed. A token as proof of the lock is passed to the function.
func (r *Locker) WithLock(f func(l LockToken)) {
	start := time.Now()
	r.m.Lock()
	acquired := time.Now()
	r.writeLockWait.Since(start)
	defer func() {
		r.m.Unlock()
		r.writeLockTime.Since(acquired)
		r.writeLockTotalTime.Since(start)
	}()
	f(&lockToken{})
}

// WithLockInCtx tries to acquires a lock until it succeeds or context times out. If it fails, return value is false otherwise true. And, executes the function `f`, if lock is acquired.
// A token as proof of the lock is passed to the function.
func (r *Locker) WithLockInCtx(ctx context.Context, f func(l LockToken) error) error {
	start := time.Now()
	if r.m.TryLockWithContext(ctx) {
		acquired := time.Now()
		r.writeLockWait.Since(start)
		defer func() {
			r.m.Unlock()
			r.writeLockTime.Since(acquired)
			r.writeLockTotalTime.Since(start)
		}()
		return f(&lockToken{})
	}
	return fmt.Errorf("failed to acquire a lock: %w", ctx.Err())
}

// AsyncLockWithCtx acquires a lock until the token is returned to the receiving channel
func (r *Locker) AsyncLockWithCtx(ctx context.Context) (LockToken, chan<- LockToken, error) {
	type tokenOrErr struct {
		token LockToken
		err   error
	}
	async := make(chan tokenOrErr)
	release := make(chan LockToken)
	start := time.Now()

	go func() {
		if r.m.TryLockWithContext(ctx) {
			acquired := time.Now()
			r.writeLockWaitAsync.Since(start)
			async <- tokenOrErr{token: &lockToken{}}
			<-release
			r.m.Unlock()
			r.writeLockTimeAsync.Since(acquired)
			r.writeLockTotalTimeAsync.Since(start)
		} else {
			async <- tokenOrErr{err: ctx.Err()}
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
