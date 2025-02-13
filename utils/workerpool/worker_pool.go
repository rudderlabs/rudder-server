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

// DistributedWorker extends Worker interface with support for work distribution across multiple workers
type DistributedWorker interface {
	Worker

	// WorkWithID is called instead of Work() when the worker supports distribution
	// workerID is the 0-based index of this worker in the partition
	// totalWorkers is the total number of workers in the partition
	WorkWithID(workerID, totalWorkers int) bool
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

// WithWorkersPerPartition option sets the number of workers per partition.
// If count is less than 1, it will default to 1 to maintain backwards compatibility
// where each partition has exactly one worker.
func WithWorkersPerPartition(count int) func(*workerPool) {
	return func(wp *workerPool) {
		if count < 1 {
			wp.logger.Warnf("invalid workers per partition count: %d, defaulting to 1", count)
			count = 1
		}
		wp.workersPerPartition = count
	}
}

// New creates a new worker pool
func New(ctx context.Context, workerSupplier WorkerSupplier, logger logger.Logger, opts ...func(*workerPool)) WorkerPool {
	wp := &workerPool{
		logger:              logger.Child("worker-pool"),
		supplier:            workerSupplier,
		workers:             make(map[string][]*internalWorker),
		cleanupPeriod:       10 * time.Second,
		idleTimeout:         5 * time.Minute,
		workersPerPartition: 1, // default to 1 worker per partition for backwards compatibility
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
	logger              logger.Logger
	supplier            WorkerSupplier
	workersPerPartition int

	cleanupPeriod time.Duration
	idleTimeout   time.Duration

	workersMu sync.RWMutex
	workers   map[string][]*internalWorker

	lifecycle struct {
		ctx    context.Context
		cancel context.CancelFunc
		wg     sync.WaitGroup
	}
}

// PingWorker pings all workers for the given partition
func (wp *workerPool) PingWorker(partition string) {
	workers := wp.getOrCreateWorkers(partition)
	for _, w := range workers {
		w := w // capture for goroutine
		go func() {
			if dw, ok := w.delegate.(DistributedWorker); ok {
				// If it's a distributed worker, use WorkWithID
				if ok := dw.WorkWithID(w.workerID, w.totalWorkers); !ok {
					w.Ping()
				}
			} else {
				// Fall back to regular Work() for backward compatibility
				w.Ping()
			}
		}()
	}
}

// Shutdown stops all workers in the pool and waits for them to stop
func (wp *workerPool) Shutdown() {
	wp.logger.Info("shutting down worker pool")
	start := time.Now()
	var wg sync.WaitGroup

	wp.workersMu.RLock()
	totalWorkers := 0
	for _, workers := range wp.workers {
		totalWorkers += len(workers)
	}
	wg.Add(totalWorkers)

	for _, workers := range wp.workers {
		for _, w := range workers {
			w := w
			go func() {
				wstart := time.Now()
				w.Stop()
				wg.Done()
				wp.logger.Debugf("worker %s stopped in %s", w.partition, time.Since(wstart))
			}()
		}
	}
	wp.workersMu.RUnlock()

	wg.Wait()
	wp.logger.Infof("all workers stopped in %s", time.Since(start))
	wp.lifecycle.cancel()
	wp.lifecycle.wg.Wait()
	wp.logger.Info("worker pool was shut down successfully")
}

// Size returns the total number of workers across all partitions
func (wp *workerPool) Size() int {
	wp.workersMu.RLock()
	defer wp.workersMu.RUnlock()
	total := 0
	for _, workers := range wp.workers {
		total += len(workers)
	}
	return total
}

// getOrCreateWorkers gets or creates workers for the given partition.
// For backwards compatibility, it ensures at least one worker exists per partition.
func (wp *workerPool) getOrCreateWorkers(partition string) []*internalWorker {
	wp.workersMu.Lock()
	defer wp.workersMu.Unlock()

	workers, ok := wp.workers[partition]
	if !ok {
		numWorkers := wp.workersPerPartition
		if numWorkers < 1 {
			numWorkers = 1 // ensure at least one worker for backwards compatibility
		}
		wp.logger.Debugf("adding %d workers in the pool for partition: %q", numWorkers, partition)
		workers = make([]*internalWorker, 0, numWorkers)
		for i := 0; i < numWorkers; i++ {
			worker := newInternalWorker(partition, wp.logger, wp.supplier(partition))
			worker.SetDistributionInfo(i, numWorkers)
			workers = append(workers, worker)
		}
		wp.workers[partition] = workers
	}
	return workers
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
			for partition, workers := range wp.workers {
				allIdle := true
				for _, w := range workers {
					idleTime := w.IdleSince()
					if idleTime.IsZero() || time.Since(idleTime) <= wp.idleTimeout {
						allIdle = false
						break
					}
				}
				if allIdle {
					wp.logger.Debugf("destroying idle workers for partition: %q", partition)
					for _, w := range workers {
						w.Stop()
					}
					delete(wp.workers, partition)
					wp.logger.Debugf("removed idle workers from pool for partition: %q", partition)
				}
			}
			wp.workersMu.Unlock()
		}
	})
}
