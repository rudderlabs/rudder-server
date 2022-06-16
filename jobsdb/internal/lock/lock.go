package lock

import "sync"

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

type listLockToken struct{}

func (*listLockToken) listLockToken() {
	// no-op
}
