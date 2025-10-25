package router

import (
	"sync"

	"github.com/rudderlabs/rudder-go-kit/stats"
	kitsync "github.com/rudderlabs/rudder-go-kit/sync"
)

// workerBuffer represents a buffer for a worker to hold jobs before processing.
// It supports dynamic sizing based on a target size function
// and allows reservation of slots to control job intake.
type workerBuffer struct {
	maxCapacity    int
	targetCapacity func() int
	jobs           chan workerJob

	stats        *workerBufferStats
	mu           sync.RWMutex
	reservations int
	closed       bool
}

type workerBufferStats struct {
	onceEvery       *kitsync.OnceEvery
	currentCapacity stats.Histogram
	currentSize     stats.Histogram
}

// newWorkerBuffer creates a new worker buffer with the specified maximum size.
// If stats is provided, it will be used to record buffer metrics.
func newWorkerBuffer(maxCapacity int, targetCapacity func() int, stats *workerBufferStats) *workerBuffer {
	if maxCapacity < 1 {
		maxCapacity = 1
	}
	wb := &workerBuffer{
		maxCapacity:    maxCapacity,
		targetCapacity: targetCapacity,
		jobs:           make(chan workerJob, maxCapacity),
		stats:          stats,
	}
	return wb
}

// newSimpleWorkerBuffer creates a new worker buffer with a fixed capacity and no stats tracking.
func newSimpleWorkerBuffer(capacity int) *workerBuffer {
	if capacity < 1 {
		capacity = 1
	}
	wb := &workerBuffer{
		maxCapacity:    capacity,
		targetCapacity: func() int { return capacity },
		jobs:           make(chan workerJob, capacity),
		stats:          nil,
	}
	return wb
}

func (wb *workerBuffer) Jobs() <-chan workerJob {
	return wb.jobs
}

func (wb *workerBuffer) currentCapacity() int {
	capacity := wb.targetCapacity()
	if capacity < 1 {
		return 1
	}
	if capacity > wb.maxCapacity {
		capacity = wb.maxCapacity
	}
	if wb.stats != nil {
		wb.stats.onceEvery.Do(func() {
			wb.stats.currentCapacity.Observe(float64(capacity))
			wb.stats.currentSize.Observe(float64(len(wb.jobs)))
		})
	}
	return capacity
}

// AvailableSlots returns the number of available slots in the worker buffer
func (wb *workerBuffer) AvailableSlots() int {
	wb.mu.RLock()
	defer wb.mu.RUnlock()
	return wb.availableSlots()
}

func (wb *workerBuffer) availableSlots() int {
	if wb.closed {
		return 0
	}
	available := wb.currentCapacity() - wb.reservations - len(wb.jobs)
	if available < 0 {
		return 0
	}
	return available
}

// ReserveSlot reserves a slot in the worker buffer if available
func (wb *workerBuffer) ReserveSlot() *reservedSlot {
	wb.mu.Lock()
	defer wb.mu.Unlock()
	if wb.availableSlots() > 0 {
		wb.reservations++
		return &reservedSlot{wb: wb}
	}
	return nil
}

// Close closes the worker buffer's job channel
func (wb *workerBuffer) Close() {
	wb.mu.Lock()
	defer wb.mu.Unlock()
	if wb.closed {
		return
	}
	wb.closed = true
	close(wb.jobs)
}

// reservedSlot represents a reserved slot in the worker's buffer
type reservedSlot struct {
	wb *workerBuffer
}

// Use sends a job into the worker's buffer
func (rs *reservedSlot) Use(wj workerJob) {
	rs.wb.mu.Lock()
	defer rs.wb.mu.Unlock()
	rs.wb.reservations--
	rs.wb.jobs <- wj
}

// Release releases the reserved slot from the worker's buffer
func (rs *reservedSlot) Release() {
	rs.wb.mu.Lock()
	defer rs.wb.mu.Unlock()
	if rs.wb.reservations > 0 {
		rs.wb.reservations--
	}
}
