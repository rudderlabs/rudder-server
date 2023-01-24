package sync

import "sync"

// PartitionLocker is a lock that can be used to lock different partitions at the same time.
type PartitionLocker struct {
	c *sync.Cond
	l sync.Locker
	s map[string]struct{}
}

// NewPartitionLocker returns a new PartitionLocker.
func NewPartitionLocker() *PartitionLocker {
	l := &sync.Mutex{}
	return &PartitionLocker{
		c: sync.NewCond(l),
		l: l,
		s: make(map[string]struct{}),
	}
}

func (p *PartitionLocker) locked(id string) (ok bool) {
	_, ok = p.s[id]
	return
}

// Lock locks the lock. If the lock is locked, it waits until the lock is unlocked.
func (p *PartitionLocker) Lock(id string) {
	p.l.Lock()
	defer p.l.Unlock()
	for p.locked(id) {
		p.c.Wait()
	}
	p.s[id] = struct{}{}
}

// Unlock unlocks the lock. If the lock is not locked, it panics.
func (p *PartitionLocker) Unlock(id string) {
	p.l.Lock()
	defer p.l.Unlock()
	if !p.locked(id) {
		panic("unlock of unlocked lock")
	}
	delete(p.s, id)
	p.c.Broadcast()
}
