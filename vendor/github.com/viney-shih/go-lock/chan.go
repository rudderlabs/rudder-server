package lock

import (
	"context"
	"time"
)

// ChanMutex is the struct implementing Mutex by channel.
type ChanMutex struct {
	lockChan chan struct{}
}

// NewChanMutex returns ChanMutex.
func NewChanMutex() *ChanMutex {
	return &ChanMutex{
		lockChan: make(chan struct{}, 1),
	}
}

// Lock acquires the lock.
// If it is currently held by others, Lock will wait until it has a chance to acquire it.
func (m *ChanMutex) Lock() {
	m.lockChan <- struct{}{}
}

// Unlock releases the lock.
func (m *ChanMutex) Unlock() {
	<-m.lockChan
}

// TryLock attempts to acquire the lock without blocking.
// Return false if someone is holding it now.
func (m *ChanMutex) TryLock() bool {
	select {
	case m.lockChan <- struct{}{}:
		return true
	default:
		return false
	}
}

// TryLockWithContext attempts to acquire the lock, blocking until resources
// are available or ctx is done (timeout or cancellation).
func (m *ChanMutex) TryLockWithContext(ctx context.Context) bool {
	select {
	case m.lockChan <- struct{}{}:
		return true
	case <-ctx.Done():
		// timeout or cancellation
		return false
	}
}

// TryLockWithTimeout attempts to acquire the lock within a period of time.
// Return false if spending time is more than duration and no chance to acquire it.
func (m *ChanMutex) TryLockWithTimeout(duration time.Duration) bool {
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	return m.TryLockWithContext(ctx)
}
