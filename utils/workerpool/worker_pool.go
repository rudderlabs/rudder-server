package workerpool

import (
	"context"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/rruntime"
)

// WorkerPool manages a pool of workers and their lifecycle
type WorkerPool interface {
	// PingWorker instructs the pool to ping the worker for the given partition
	PingWorker(partition string)

	// Shutdown stops all workers in the pool and waits for them to stop
	Shutdown()

	// Size returns the number of workers in the pool
	Size() int
}

// WorkerSupplier is a function able to create a new worker for the given partition
type WorkerSupplier func(partition string) Worker

// Worker is a worker that can be pinged for work and stopped by the worker pool when it is idle
type Worker interface {
	// Work triggers the worker to work and returns true if it did work, false otherwise.
	// The worker's idle time is calculated based on the return value of this method
	Work() bool

	// SleepDurations returns the sleep durations for the worker (min, max), i.e. how long the worker should sleep if it has no work to do
	SleepDurations() (min, max time.Duration)

	// Stop stops the worker and waits until all its goroutines have stopped
	Stop()
}

// WithCleanupPeriod option sets the cleanup period for the worker pool
func WithCleanupPeriod(cleanupPeriod time.Duration) func(*workerPool) {
	return func(wp *workerPool) {
		wp.cleanupPeriod = cleanupPeriod
	}
}

// WithIdleTimeout option sets the idle timeout for the worker pool
func WithIdleTimeout(idleTimeout time.Duration) func(*workerPool) {
	return func(wp *workerPool) {
		wp.idleTimeout = idleTimeout
	}
}

// New creates a new worker pool
func New(ctx context.Context, workerSupplier WorkerSupplier, logger logger.Logger, opts ...func(*workerPool)) WorkerPool {
	wp := &workerPool{
		logger:        logger.Child("worker-pool"),
		supplier:      workerSupplier,
		workers:       make(map[string]*internalWorker),
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
	logger   logger.Logger
	supplier WorkerSupplier

	cleanupPeriod time.Duration
	idleTimeout   time.Duration

	workersMu sync.RWMutex
	workers   map[string]*internalWorker

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
	wp.logger.Info("shutting down worker pool")
	start := time.Now()
	var wg sync.WaitGroup
	wg.Add(len(wp.workers))
	for _, w := range wp.workers {
		w := w
		go func() {
			wstart := time.Now()
			w.Stop()
			wg.Done()
			wp.logger.Debugf("worker %s stopped in %s", w.partition, time.Since(wstart))
		}()
	}
	wg.Wait()
	wp.logger.Infof("all workers stopped in %s", time.Since(start))
	wp.lifecycle.cancel()
	wp.lifecycle.wg.Wait()
	wp.logger.Info("worker pool was shut down successfully")
}

// Size returns the number of workers in the pool
func (wp *workerPool) Size() int {
	wp.workersMu.RLock()
	defer wp.workersMu.RUnlock()
	return len(wp.workers)
}

// worker gets or creates a worker for the given partition
func (wp *workerPool) worker(partition string) *internalWorker {
	wp.workersMu.Lock()
	defer wp.workersMu.Unlock()
	w, ok := wp.workers[partition]
	if !ok {
		wp.logger.Debugf("adding worker in the pool for partition: %q", partition)
		w = newInternalWorker(partition, wp.logger, wp.supplier(partition))
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
					wp.logger.Debugf("destroying idle worker for partition: %q", partition)
					w.Stop()
					delete(wp.workers, partition)
					wp.logger.Debugf("removed idle worker from pool for partition: %q", partition)
				}
			}
			wp.workersMu.Unlock()
		}
	})
}
