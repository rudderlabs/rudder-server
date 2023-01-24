package sync

import "sync"

// PartitionRWLocker is a read-write lock that can be used to lock different partitions at the same time.
type PartitionRWLocker struct {
	c *sync.Cond
	l sync.Locker
	s map[string]*lockInfo
}

// NewPartitionRWLocker returns a new PartitionRWLocker.
func NewPartitionRWLocker() *PartitionRWLocker {
	l := &sync.Mutex{}
	return &PartitionRWLocker{
		c: sync.NewCond(l),
		l: l,
		s: make(map[string]*lockInfo),
	}
}

// lockInfo returns the lockInfo for the given id. If the lockInfo does not exist, it is created.
func (p *PartitionRWLocker) lockInfo(id string) *lockInfo {
	li, ok := p.s[id]
	if !ok {
		li = &lockInfo{}
		p.s[id] = li
	}
	return li
}

// Lock locks the lock for writing. If the lock is locked for reading or writing, it waits until the lock is unlocked.
func (p *PartitionRWLocker) Lock(id string) {
	p.l.Lock()
	defer p.l.Unlock()
	for !p.lockInfo(id).TryLock() {
		p.c.Wait()
	}
}

// RLock locks the lock for reading. If the lock is locked for writing, it waits until the lock is unlocked.
func (p *PartitionRWLocker) RLock(id string) {
	p.l.Lock()
	defer p.l.Unlock()
	for !p.lockInfo(id).TryRLock() {
		p.c.Wait()
	}
}

// Unlock unlocks the lock for writing. If the lock is locked for reading or not locked for writing, it panics.
func (p *PartitionRWLocker) Unlock(id string) {
	p.l.Lock()
	defer p.l.Unlock()
	li := p.lockInfo(id)
	if li.readers > 0 {
		panic("unlock of a rlocked lock")
	}
	if !li.locked {
		panic("unlock of a non-locked lock")
	}

	delete(p.s, id)
	p.c.Broadcast()
}

// RUnlock unlocks the lock for reading. If the lock is locked for writing or not locked for reading, it panics.
func (p *PartitionRWLocker) RUnlock(id string) {
	p.l.Lock()
	defer p.l.Unlock()
	li := p.lockInfo(id)
	if li.locked {
		panic("runlock of a locked lock")
	}
	if li.readers == 0 {
		panic("runlock of a non-rlocked lock")
	}
	li.readers--
	if li.readers == 0 {
		delete(p.s, id)
	}
	p.c.Broadcast()
}

func (p *PartitionRWLocker) RWMutexFor(id string) *RWMutex {
	return &RWMutex{
		plock: p,
		id:    id,
	}
}

type lockInfo struct {
	locked  bool // true if locked for writing
	readers int  // number of readers
}

// TryLock tries to lock the lock for writing. It returns true if the lock was successfully acquired.
func (l *lockInfo) TryLock() bool {
	if l.locked || l.readers > 0 {
		return false
	}
	l.locked = true
	return true
}

// TryLock tries to lock the lock for reading. It returns true if the read lock was successfully acquired.
func (l *lockInfo) TryRLock() bool {
	if l.locked {
		return false
	}
	l.readers++
	return true
}

type RWMutex struct {
	plock *PartitionRWLocker
	id    string
}

func (m *RWMutex) Lock() {
	m.plock.Lock(m.id)
}

func (m *RWMutex) Unlock() {
	m.plock.Unlock(m.id)
}

func (m *RWMutex) RLock() {
	m.plock.RLock(m.id)
}

func (m *RWMutex) RUnlock() {
	m.plock.RUnlock(m.id)
}
