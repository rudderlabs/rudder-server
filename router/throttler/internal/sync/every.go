package sync

import (
	"sync"
	"time"
)

// Every is a utility that allows executing a function at most once every specified interval.
type Every struct {
	mu       sync.Mutex
	interval time.Duration
	lastRun  time.Time
}

// NewEvery creates a new Every instance with the specified interval.
func NewEvery(interval time.Duration) *Every {
	return &Every{
		interval: interval,
	}
}

// Do runs the function f if interval has passed since the last execution.
func (e *Every) Do(f func()) {
	e.mu.Lock()
	defer e.mu.Unlock()

	now := time.Now()
	if now.Sub(e.lastRun) >= e.interval {
		e.lastRun = now
		f()
	}
}
