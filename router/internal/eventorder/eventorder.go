package eventorder

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/rudderlabs/rudder-server/jobsdb"
)

var ErrUnsupportedState = errors.New("unsupported state")

type OptFn func(b *Barrier)

// WithMetadata includes the provided metadata in the error messages
func WithMetadata(metadata map[string]string) OptFn {
	return func(b *Barrier) {
		b.metadata = metadata
	}
}

// WithConcurrencyLimit sets the maximum number of concurrent jobs for
// a given key when the limiter is enabled
func WithConcurrencyLimit(abortConcurrencyLimit int) OptFn {
	return func(b *Barrier) {
		b.concurrencyLimit = abortConcurrencyLimit
	}
}

// NewBarrier creates a new properly initialized Barrier
func NewBarrier(fns ...OptFn) *Barrier {
	b := &Barrier{
		barriers: make(map[string]*barrierInfo),
	}
	for _, fn := range fns {
		fn(b)
	}
	return b
}

// Barrier is an abstraction for applying event ordering guarantees in the router.
//
// Events for the same key need to be processed in order, thus when an event fails but will be retried later by the router,
// we need to put all subsequent events for that key on hold until the failed event succeeds or fails
// in a terminal way.
//
// A barrier controls the concurrency of events in two places:
//
// 1. At entrance, before the event enters the pipeline.
//
// 2. Before actually trying to send the event, since events after being accepted by the router, they are
// processed asynchronously through buffered channels by separate goroutine(s) aka workers.
type Barrier struct {
	concurrencyLimit int
	mu               sync.RWMutex // mutex to synchronize concurrent access to the barrier's methods
	queue            []command
	barriers         map[string]*barrierInfo
	metadata         map[string]string
}

// Enter the barrier for this key and jobID. If there is not already a barrier for this key
// returns true, otherwise false along with the previous failed jobID if this is the cause of the barrier.
// Another scenario where a barrier might exist for a key is when the previous job has failed in an unrecoverable manner and the concurrency limiter is enabled.
func (b *Barrier) Enter(key string, jobID int64) (accepted bool, previousFailedJobID *int64) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	barrier, ok := b.barriers[key]
	if !ok {
		return true, nil // no barrier, accept
	}

	// if there is a failed job in the barrier, only this job can enter the barrier
	if barrier.failedJobID != nil {
		failedJob := *barrier.failedJobID
		if failedJob > jobID {
			panic(fmt.Errorf("detected illegal job sequence during barrier enter %+v: key %q, previousFailedJob:%d > jobID:%d", b.metadata, key, failedJob, jobID))
		}
		return jobID == failedJob, &failedJob
	}

	// if the concurrency limiter is enabled, only the configured number of concurrent jobs can enter the barrier
	if barrier.concurrencyLimiter != nil {
		barrier.mu.Lock()
		defer barrier.mu.Unlock()
		if _, ok := barrier.concurrencyLimiter[jobID]; ok {
			return true, nil // if the job is already in the concurrent jobs map, accept it (poor job... it forgot to notify the barrier before leaving!)
		}
		if len(barrier.concurrencyLimiter) >= b.concurrencyLimit {
			return false, nil // if the concurrent jobs map is full, reject it
		}

		// add the job to the concurrent jobs map and accept it
		barrier.concurrencyLimiter[jobID] = struct{}{}
		return true, nil
	}

	return true, nil
}

// Leave the barrier for this key and jobID. Leave acts as an undo operation for Enter, i.e.
// when a previously-entered job leaves the barrier it is as if this key and jobID didn't enter the barrier.
// Calling Leave is idempotent.
func (b *Barrier) Leave(key string, jobID int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	barrier, ok := b.barriers[key]
	if !ok {
		return
	}
	barrier.mu.Lock()
	delete(barrier.concurrencyLimiter, jobID)
	barrier.mu.Unlock()
}

// Peek returns the previously failed jobID for the given key, if any
func (b *Barrier) Peek(key string) (previousFailedJobID *int64) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	barrier, ok := b.barriers[key]
	if !ok || barrier.failedJobID == nil {
		return nil
	}
	failedJob := *barrier.failedJobID
	return &failedJob
}

// Wait returns true if the job for this key shouldn't continue, but wait (transition to a waiting state)
func (b *Barrier) Wait(key string, jobID int64) (wait bool, previousFailedJobID *int64) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	barrier, ok := b.barriers[key]
	if !ok {
		return false, nil // no barrier, don't wait
	}
	if barrier.failedJobID != nil {
		failedJob := *barrier.failedJobID
		if failedJob > jobID {
			panic(fmt.Errorf("detected illegal job sequence during barrier wait %+v: key %q, previousFailedJob:%d > jobID:%d", b.metadata, key, failedJob, jobID))
		}
		return jobID > failedJob, &failedJob // wait if this is not the failed job
	}
	// no failed job, don't wait
	return false, nil
}

// StateChanged must be called at the end, after the job state change has been persisted.
// The only exception to this rule is when a job has failed in a retryable manner, in this scenario you should notify the barrier immediately after the failure.
// An [ErrUnsupportedState] error will be returned if the state is not supported.
func (b *Barrier) StateChanged(key string, jobID int64, state string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	var command command
	jsCmd := &cmd{key: key, jobID: jobID}
	switch state {
	case jobsdb.Succeeded.State:
		command = &jobSucceededCmd{jsCmd}
	case jobsdb.Failed.State:
		command = &jobFailedCommand{jsCmd}
	case jobsdb.Aborted.State:
		command = &jobAbortedCommand{jsCmd}
	case jobsdb.Waiting.State:
		command = jsCmd
	default:
		return ErrUnsupportedState
	}

	if command.enqueue(b) {
		b.queue = append(b.queue, command)
	} else {
		command.execute(b)
	}
	return nil
}

// Sync applies any enqueued commands to the barrier's state. It should be called at the beginning of every new iteration of the main loop
func (b *Barrier) Sync() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, c := range b.queue {
		c.execute(b)
	}
	flushed := len(b.queue)
	b.queue = nil
	return flushed
}

// Size returns the number of active barriers
func (b *Barrier) Size() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.barriers)
}

// String returns a string representation of the barrier
func (b *Barrier) String() string {
	var sb strings.Builder
	b.mu.RLock()
	defer b.mu.RUnlock()
	sb.WriteString(fmt.Sprintf("Barrier{%+v[", b.metadata))
	for key, barrier := range b.barriers {
		failedJobID := "<nil>"
		if barrier.failedJobID != nil {
			failedJobID = fmt.Sprintf("%d", *barrier.failedJobID)
		}
		sb.WriteString(fmt.Sprintf("{key: %s, failedJobID: %v, concurrentJobs: %v}", key, failedJobID, barrier.concurrencyLimiter))
	}
	sb.WriteString("]}")
	return sb.String()
}

type barrierInfo struct {
	failedJobID        *int64             // nil if no failed job
	mu                 sync.RWMutex       // protects concurrentJobs
	concurrencyLimiter map[int64]struct{} // nil if concurrency limiter is off
}

type command interface {
	enqueue(b *Barrier) bool
	execute(b *Barrier)
}

type cmd struct {
	key   string
	jobID int64
}

// default behaviour is to try and remove the jobID from the concurrent jobs map
func (c *cmd) execute(b *Barrier) {
	if barrier, ok := b.barriers[c.key]; ok {
		barrier.mu.Lock()
		defer barrier.mu.Unlock()
		delete(barrier.concurrencyLimiter, c.jobID)
	}
}

// default behaviour is to enqueue the command if a barrier for this key already exists
func (c *cmd) enqueue(b *Barrier) bool {
	_, ok := b.barriers[c.key]
	return ok
}

// jobFailedCommand is a command that is executed when a job has failed.
type jobFailedCommand struct {
	*cmd
}

// If no failed jobID is in the barrier make this jobID the failed job for this key. Removes the job from the concurrent jobs map if it exists there
func (c *jobFailedCommand) execute(b *Barrier) {
	barrier, ok := b.barriers[c.key]
	if !ok {
		barrier = &barrierInfo{}
		b.barriers[c.key] = barrier
	}
	barrier.mu.Lock()
	defer barrier.mu.Unlock()

	// it is unfortunately possible within a single batch for events to be processed out-of-order
	if barrier.failedJobID == nil {
		barrier.failedJobID = &c.jobID
	} else if *barrier.failedJobID > c.jobID {
		panic(fmt.Errorf("detected illegal job sequence during barrier job failed %+v: key %q, previousFailedJob:%d > jobID:%d", b.metadata, c.key, *barrier.failedJobID, c.jobID))
	}
	// reset concurrency limiter
	barrier.concurrencyLimiter = nil
}

// a failed command never gets enqueued
func (*jobFailedCommand) enqueue(_ *Barrier) bool {
	return false
}

// jobSucceededCmd is a command that is executed when a job has succeeded.
type jobSucceededCmd struct {
	*cmd
}

// removes the barrier for this key, if it exists
func (c *jobSucceededCmd) execute(b *Barrier) {
	if barrier, ok := b.barriers[c.key]; ok {
		if barrier.failedJobID != nil && *barrier.failedJobID != c.jobID { // out-of-sync command (failed commands get executed immediately)
			return
		}
	}
	delete(b.barriers, c.key)
}

// jobAbortedCommand is a command that is executed when a job has aborted.
type jobAbortedCommand struct {
	*cmd
}

// Creates a concurrent jobs map if none exists. Also removes the jobID from the concurrent jobs map if it exists there
func (c *jobAbortedCommand) execute(b *Barrier) {
	if barrier, ok := b.barriers[c.key]; ok {
		if barrier.failedJobID == nil {
			// no previously failed job, simply remove the barrier
			delete(b.barriers, c.key)
			return
		}
		if *barrier.failedJobID != c.jobID {
			// out-of-sync command (failed commands get executed immediately)
			return
		}
		// remove the failed jobID and enable the concurrency limiter
		barrier.failedJobID = nil
		barrier.mu.Lock()
		barrier.concurrencyLimiter = make(map[int64]struct{})
		barrier.mu.Unlock()
	}
}
