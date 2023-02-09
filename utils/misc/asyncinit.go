package misc

import (
	"context"
	"sync"
	"sync/atomic"
)

// NewAsyncInit returns a new AsyncInit object with the given expected initialization events count.
func NewAsyncInit(count int64) *AsyncInit {
	a := &AsyncInit{
		c: make(chan struct{}),
	}
	a.count.Store(count)
	return a
}

// AsyncInit is a helper object to wait for multiple asynchronous initialization events.
type AsyncInit struct {
	mu    sync.Mutex
	count atomic.Int64
	c     chan struct{}
}

// Done decrements the initialization events count
func (ia *AsyncInit) Done() {
	if ia.count.Add(-1) == 0 {
		close(ia.channel())
	}
}

// Wait returns the channel that will be closed when the initialization events count reaches zero.
func (ia *AsyncInit) Wait() chan struct{} {
	return ia.channel()
}

// WaitContext returns no error if initialization events happen before the provided context is done. It returns the context's error otherwise
func (ia *AsyncInit) WaitContext(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-ia.Wait():
		return nil
	}
}

func (ia *AsyncInit) channel() chan struct{} {
	ia.mu.Lock()
	defer ia.mu.Unlock()
	if ia.c == nil {
		ia.c = make(chan struct{})
	}
	return ia.c
}
