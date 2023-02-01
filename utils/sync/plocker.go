package sync

import "sync"

// PartitionLocker is a lock that can be used to lock different partitions at the same time.
type PartitionLocker struct {
	l sync.Mutex // protects s
	s map[string]*lockInfo
}

// NewPartitionLocker returns a new PartitionLocker.
func NewPartitionLocker() *PartitionLocker {
	return &PartitionLocker{
		s: make(map[string]*lockInfo),
	}
}

// Lock locks the lock. If the lock is locked, it waits until the lock is unlocked.
func (p *PartitionLocker) Lock(id string) {
	p.l.Lock()
	li := p.lockInfo(id)
	li.refs++
	p.l.Unlock() // unlock before locking mu to avoid unnecessary blocking
	li.mu.Lock()
}

// Unlock unlocks the lock. If the lock is not locked, it panics.
func (p *PartitionLocker) Unlock(id string) {
	p.l.Lock()
	defer p.l.Unlock()
	li := p.lockInfo(id)
	li.mu.Unlock()
	li.refs--
	if li.refs == 0 {
		delete(p.s, id)
	}
}

// lockInfo returns the lockInfo for the given id. If the lockInfo does not exist, it is created.
func (l *PartitionLocker) lockInfo(key string) *lockInfo {
	mu, ok := l.s[key]
	if !ok {
		mu = &lockInfo{}
		l.s[key] = mu
	}
	return mu
}

type lockInfo struct {
	mu   sync.Mutex // the partition lock
	refs int        // number of references to this lock
}
