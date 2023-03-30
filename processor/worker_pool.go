package processor

import (
	"context"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/rruntime"
)

// withWorkerPoolCleanupPeriod option sets the cleanup period for the worker pool
func withWorkerPoolCleanupPeriod(cleanupPeriod time.Duration) func(*workerPool) {
	return func(wp *workerPool) {
		wp.cleanupPeriod = cleanupPeriod
	}
}

// withWorkerPoolIdleTimeout option sets the idle timeout for the worker pool
func withWorkerPoolIdleTimeout(idleTimeout time.Duration) func(*workerPool) {
	return func(wp *workerPool) {
		wp.idleTimeout = idleTimeout
	}
}

// newWorkerPool creates a new worker pool
func newWorkerPool(ctx context.Context, handle workerHandle, opts ...func(*workerPool)) *workerPool {
	wp := &workerPool{
		handle:        handle,
		logger:        handle.logger().Child("worker-pool"),
		workers:       make(map[string]*worker),
		cleanupPeriod: 10 * time.Second,
		idleTimeout:   5 * time.Minute,
	}
	for _, opt := range opts {
		opt(wp)
	}
	wp.lifecycle.ctx, wp.lifecycle.cancel = context.WithCancel(ctx)
	wp.startCleanupLoop()
	return wp
}

// workerPool manages a pool of workers
type workerPool struct {
	handle workerHandle
	logger logger.Logger

	cleanupPeriod time.Duration
	idleTimeout   time.Duration

	workersMu sync.RWMutex
	workers   map[string]*worker

	lifecycle struct {
		ctx    context.Context
		cancel context.CancelFunc
		wg     sync.WaitGroup
	}
}

// PingWorker pings the worker for the given partition
func (wp *workerPool) PingWorker(partition string) {
	wp.worker(partition).Ping()
}

// Shutdown stops all workers in the pull and waits for them to stop
func (wp *workerPool) Shutdown() {
	for _, w := range wp.workers {
		w.Stop()
	}
	wp.lifecycle.cancel()
	wp.lifecycle.wg.Wait()
}

// Size returns the number of workers in the pool
func (wp *workerPool) Size() int {
	wp.workersMu.RLock()
	defer wp.workersMu.RUnlock()
	return len(wp.workers)
}

// worker gets or creates a worker for the given partition
func (wp *workerPool) worker(partition string) *worker {
	wp.workersMu.Lock()
	defer wp.workersMu.Unlock()
	w, ok := wp.workers[partition]
	if !ok {
		wp.logger.Infof("Adding worker in the pool for partition: %s", partition)
		w = newWorker(wp.lifecycle.ctx, partition, wp.handle)
		wp.workers[partition] = w
	}
	return w
}

// startCleanupLoop starts a loop that cleans up idle workers
func (wp *workerPool) startCleanupLoop() {
	wp.lifecycle.wg.Add(1)
	rruntime.Go(func() {
		defer wp.lifecycle.wg.Done()
		for {
			select {
			case <-wp.lifecycle.ctx.Done():
				return
			case <-time.After(wp.cleanupPeriod):
			}
			wp.workersMu.Lock()
			for partition, w := range wp.workers {
				idleTime := w.IdleSince()
				if !idleTime.IsZero() && time.Since(idleTime) > wp.idleTimeout {
					wp.logger.Infof("Destroying idle worker for partition: %s", partition)
					w.Stop()
					delete(wp.workers, partition)
					wp.logger.Infof("Removed idle worker from pool for partition: %s", partition)
				}
			}
			wp.workersMu.Unlock()
		}
	})
}
