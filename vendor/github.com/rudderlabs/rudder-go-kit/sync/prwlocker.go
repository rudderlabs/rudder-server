package sync

import "sync"

// PartitionRWLocker is a read-write lock that can be used to lock different partitions at the same time.
type PartitionRWLocker struct {
	l sync.Mutex // protects s
	s map[string]*rwLockInfo
}

// NewPartitionRWLocker returns a new PartitionRWLocker.
func NewPartitionRWLocker() *PartitionRWLocker {
	return &PartitionRWLocker{
		s: make(map[string]*rwLockInfo),
	}
}

// Lock locks the lock for writing. If the lock is locked for reading or writing, it waits until the lock is unlocked.
func (p *PartitionRWLocker) Lock(id string) {
	p.l.Lock()
	li := p.lockInfo(id)
	li.refs++
	p.l.Unlock() // unlock before locking mu to avoid unnecessary blocking
	li.mu.Lock()
}

// RLock locks the lock for reading. If the lock is locked for writing, it waits until the lock is unlocked.
func (p *PartitionRWLocker) RLock(id string) {
	p.l.Lock()
	li := p.lockInfo(id)
	li.refs++
	p.l.Unlock() // unlock before locking mu to avoid unnecessary blocking
	li.mu.RLock()
}

// Unlock unlocks the lock for writing. If the lock is locked for reading or not locked for writing, it panics.
func (p *PartitionRWLocker) Unlock(id string) {
	p.l.Lock()
	defer p.l.Unlock()
	li := p.lockInfo(id)
	li.mu.Unlock()
	li.refs--
	if li.refs == 0 {
		delete(p.s, id)
	}
}

// RUnlock unlocks the lock for reading. If the lock is locked for writing or not locked for reading, it panics.
func (p *PartitionRWLocker) RUnlock(id string) {
	p.l.Lock()
	defer p.l.Unlock()
	li := p.lockInfo(id)
	li.mu.RUnlock()
	li.refs--
	if li.refs == 0 {
		delete(p.s, id)
	}
}

// RWMutexFor returns a new RWMutex scoped to the given id.
func (p *PartitionRWLocker) RWMutexFor(id string) *RWMutex {
	return &RWMutex{
		plock: p,
		id:    id,
	}
}

// lockInfo returns the lockInfo for the given id. If the lockInfo does not exist, it is created.
func (p *PartitionRWLocker) lockInfo(id string) *rwLockInfo {
	li, ok := p.s[id]
	if !ok {
		li = &rwLockInfo{}
		p.s[id] = li
	}
	return li
}

type rwLockInfo struct {
	mu   sync.RWMutex // the partition lock
	refs int          // number of references to this lock
}

// RWMutex is a read-write lock
type RWMutex struct {
	plock *PartitionRWLocker
	id    string
}

// Lock locks the lock for writing. If the lock is locked for reading or writing, it waits until the lock is unlocked.
func (m *RWMutex) Lock() {
	m.plock.Lock(m.id)
}

// Unlock unlocks the lock for writing. If the lock is locked for reading or not locked for writing, it panics.
func (m *RWMutex) Unlock() {
	m.plock.Unlock(m.id)
}

// RLock locks the lock for reading. If the lock is locked for writing, it waits until the lock is unlocked.
func (m *RWMutex) RLock() {
	m.plock.RLock(m.id)
}

// RUnlock unlocks the lock for reading. If the lock is locked for writing or not locked for reading, it panics.

func (m *RWMutex) RUnlock() {
	m.plock.RUnlock(m.id)
}
