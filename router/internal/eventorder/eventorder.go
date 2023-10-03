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

// WithConcurrencyLimit sets the maximum number of concurrent jobs for a given key
func WithConcurrencyLimit(concurrencyLimit int) OptFn {
	return func(b *Barrier) {
		b.concurrencyLimit = concurrencyLimit
	}
}

// WithDrainConcurrencyLimit sets the maximum number of concurrent jobs for a given key when the limiter is enabled (after a failed job has been drained, i.e. aborted)
func WithDrainConcurrencyLimit(drainLimit int) OptFn {
	return func(b *Barrier) {
		b.drainLimit = drainLimit
	}
}

// WithDebugInfoProvider sets the debug info provider for the barrier (used for debugging purposes in case an illegal job sequence is detected)
func WithDebugInfoProvider(debugInfoProvider func(key string) string) OptFn {
	return func(b *Barrier) {
		b.debugInfo = debugInfoProvider
	}
}

// NewBarrier creates a new properly initialized Barrier
func NewBarrier(fns ...OptFn) *Barrier {
	b := &Barrier{
		barriers: make(map[string]*barrierInfo),
		metadata: make(map[string]string),
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
	mu       sync.RWMutex // mutex to synchronize concurrent access to the barrier's methods
	queue    []command
	barriers map[string]*barrierInfo
	metadata map[string]string

	concurrencyLimit int // maximum number of concurrent jobs for a given key (0 means no limit)
	drainLimit       int // maximum number of concurrent jobs to accept after a previously failed job has been aborted
	debugInfo        func(key string) string
}

// Enter the barrier for this key and jobID. If there is not already a barrier for this key
// returns true, otherwise false along with the previous failed jobID if this is the cause of the barrier.
// Another scenario where a barrier might exist for a key is when the previous job has failed in an unrecoverable manner and the concurrency limiter is enabled.
func (b *Barrier) Enter(key string, jobID int64) (accepted bool, previousFailedJobID *int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	barrier, ok := b.barriers[key]
	if !ok {
		if b.concurrencyLimit == 0 { // if not concurrency limit is set accept the job
			return true, nil
		}
		// create a new barrier with the concurrency limiter enabled
		barrier = &barrierInfo{
			concurrencyLimiter: make(map[int64]struct{}),
		}
		b.barriers[key] = barrier
	}

	// if any of our limits is reached, don't accept the job
	if barrier.ConcurrencyLimitReached(jobID, b.concurrencyLimit) {
		return false, nil
	}
	if barrier.DrainLimitReached(jobID, b.drainLimit) {
		return false, nil
	}

	// if there is a failed job in the barrier, only this job can enter the barrier
	if barrier.failedJobID != nil {
		failedJob := *barrier.failedJobID
		previousFailedJobID = &failedJob
		if failedJob > jobID {
			var debugInfo string
			if b.debugInfo != nil {
				debugInfo = "DEBUG INFO:\n" + b.debugInfo(key)
			}
			panic(fmt.Errorf("detected illegal job sequence during barrier enter %+v: key %q, previousFailedJob:%d > jobID:%d%s", b.metadata, key, failedJob, jobID, debugInfo))
		}
		accepted = jobID == failedJob
	} else {
		accepted = true
	}

	if accepted { // if the job is finally accepted, add it to the active limiters
		barrier.Enter(jobID)
	}
	return
}

// Leave the barrier for this key and jobID. Leave acts as an undo operation for Enter, i.e.
// when a previously-entered job leaves the barrier it is as if this key and jobID didn't enter the barrier.
// Calling Leave is idempotent.
func (b *Barrier) Leave(key string, jobID int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	// remove the job from the active limiters
	if barrier, ok := b.barriers[key]; ok {
		barrier.Leave(jobID)
		if barrier.Inactive() {
			delete(b.barriers, key)
		}
	}
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
			var debugInfo string
			if b.debugInfo != nil {
				debugInfo = "DEBUG INFO:\n" + b.debugInfo(key)
			}
			panic(fmt.Errorf("detected illegal job sequence during barrier wait %+v: key %q, previousFailedJob:%d > jobID:%d%s", b.metadata, key, failedJob, jobID, debugInfo))
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
	case jobsdb.Filtered.State:
		command = &jobFilteredCmd{jsCmd}
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
	failedJobID        *int64 // nil if no failed job
	concurrencyLimiter map[int64]struct{}
	drainLimiter       map[int64]struct{} // nil if limiter is off
}

// Enter adds the jobID to the barrier's active limiter(s)
func (bi *barrierInfo) Enter(jobID int64) {
	if bi.concurrencyLimiter != nil {
		bi.concurrencyLimiter[jobID] = struct{}{}
	}
	if bi.drainLimiter != nil {
		bi.drainLimiter[jobID] = struct{}{}
	}
}

// Leave removes the jobID from the barrier's limiter(s)
func (bi *barrierInfo) Leave(jobID int64) {
	delete(bi.concurrencyLimiter, jobID)
	delete(bi.drainLimiter, jobID)
}

// ConcurrencyLimitReached returns true if the barrier's concurrency limit has been reached
func (bi *barrierInfo) ConcurrencyLimitReached(jobID int64, limit int) bool {
	if bi.concurrencyLimiter != nil {
		if _, ok := bi.concurrencyLimiter[jobID]; !ok {
			return len(bi.concurrencyLimiter) >= limit
		}
	}
	return false
}

// DrainLimitReached returns true if the barrier's drain limit has been reached
func (bi *barrierInfo) DrainLimitReached(jobID int64, limit int) bool {
	if bi.drainLimiter != nil {
		if _, ok := bi.drainLimiter[jobID]; !ok {
			return len(bi.drainLimiter) >= limit
		}
	}
	return false
}

// Inactive returns true if there isn't a failed job and both limiters are inactive
func (bi *barrierInfo) Inactive() bool {
	return bi.failedJobID == nil && // no failed job
		len(bi.concurrencyLimiter) == 0 && // no concurrent jobs
		bi.drainLimiter == nil // drain limiter is off
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
		barrier.Leave(c.jobID)
		if barrier.Inactive() {
			delete(b.barriers, c.key)
		}
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
	barrier.Leave(c.jobID)
	if barrier.failedJobID == nil {
		barrier.failedJobID = &c.jobID
	} else if *barrier.failedJobID > c.jobID {
		var debugInfo string
		if b.debugInfo != nil {
			debugInfo = "DEBUG INFO:\n" + b.debugInfo(c.key)
		}
		panic(fmt.Errorf("detected illegal job sequence during barrier job failed %+v: key %q, previousFailedJob:%d > jobID:%d%s", b.metadata, c.key, *barrier.failedJobID, c.jobID, debugInfo))
	}
	barrier.drainLimiter = nil // turn off drain limiter
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
		barrier.Leave(c.jobID)
		if barrier.failedJobID != nil && *barrier.failedJobID != c.jobID { // out-of-sync command (failed commands get executed immediately)
			return
		}
		barrier.failedJobID = nil
		barrier.drainLimiter = nil
		if barrier.Inactive() {
			delete(b.barriers, c.key)
		}
	}
}

// jobFilteredCmd is a command that is executed when a job is filtered.
type jobFilteredCmd struct {
	*cmd
}

// removes the barrier for this key, if it exists
func (c *jobFilteredCmd) execute(b *Barrier) {
	if barrier, ok := b.barriers[c.key]; ok {
		barrier.Leave(c.jobID)
		if barrier.failedJobID != nil && *barrier.failedJobID != c.jobID { // out-of-sync command (failed commands get executed immediately)
			return
		}
		barrier.failedJobID = nil
		barrier.drainLimiter = nil
		if barrier.Inactive() {
			delete(b.barriers, c.key)
		}
	}
}

// jobAbortedCommand is a command that is executed when a job has aborted.
type jobAbortedCommand struct {
	*cmd
}

// Creates a concurrent jobs map if none exists. Also removes the jobID from the concurrent jobs map if it exists there
func (c *jobAbortedCommand) execute(b *Barrier) {
	if barrier, ok := b.barriers[c.key]; ok {
		barrier.Leave(c.jobID)
		if barrier.failedJobID != nil && *barrier.failedJobID != c.jobID { // out-of-sync command (failed commands get executed immediately)
			return
		}
		// previouslyFailed indicates whether the job that was aborted was previouly a failed job, which is the condition for enabling the drain limiter
		previouslyFailed := barrier.failedJobID != nil && *barrier.failedJobID == c.jobID
		barrier.failedJobID = nil                 // remove the failed job
		barrier.drainLimiter = nil                // turn off drain limiter
		if b.drainLimit > 0 && previouslyFailed { // enable the drain limiter only if a previously failed job has been aborted
			barrier.drainLimiter = make(map[int64]struct{})
		}
		if barrier.Inactive() {
			delete(b.barriers, c.key)
		}
	}
}
