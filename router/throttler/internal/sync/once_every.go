package sync

import (
	"sync"
	"time"
)

// OnceEvery is a utility that allows executing a function at most once every specified interval.
type OnceEvery struct {
	mu       sync.Mutex
	interval time.Duration
	lastRun  time.Time
}

// NewOnceEvery creates a new Every instance with the specified interval.
func NewOnceEvery(interval time.Duration) *OnceEvery {
	return &OnceEvery{
		interval: interval,
	}
}

// Do runs the function f if interval has passed since the last execution.
func (e *OnceEvery) Do(f func()) {
	e.mu.Lock()
	defer e.mu.Unlock()

	now := time.Now()
	if now.Sub(e.lastRun) >= e.interval {
		e.lastRun = now
		f()
	}
}
