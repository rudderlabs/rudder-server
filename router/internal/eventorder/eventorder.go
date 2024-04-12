package eventorder

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"

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

// WithEventOrderKeyThreshold sets the maximum number of concurrent jobs for a given key. After this limit is reached, the barrier will be disabled for this key.
func WithEventOrderKeyThreshold(eventOrderKeyThreshold config.ValueLoader[int]) OptFn {
	return func(b *Barrier) {
		b.eventOrderKeyThreshold = eventOrderKeyThreshold
	}
}

// WithDisabledStateDuration sets the duration for which the barrier will remain in the disabled state after the concurrency limit has been reached
func WithDisabledStateDuration(disabledStateDuration config.ValueLoader[time.Duration]) OptFn {
	return func(b *Barrier) {
		b.disabledStateDuration = disabledStateDuration
	}
}

// WithHalfEnabledStateDuration sets the duration for which the barrier will remain in the half-enabled state
func WithHalfEnabledStateDuration(halfEnabledStateDuration config.ValueLoader[time.Duration]) OptFn {
	return func(b *Barrier) {
		b.halfEnabledStateDuration = halfEnabledStateDuration
	}
}

// WithDrainConcurrencyLimit sets the maximum number of concurrent jobs for a given key when the limiter is enabled (after a failed job has been drained, i.e. aborted)
func WithDrainConcurrencyLimit(drainLimit config.ValueLoader[int]) OptFn {
	return func(b *Barrier) {
		b.drainLimit = drainLimit
	}
}

// WithDebugInfoProvider sets the debug info provider for the barrier (used for debugging purposes in case an illegal job sequence is detected)
func WithDebugInfoProvider(debugInfoProvider func(key BarrierKey) string) OptFn {
	return func(b *Barrier) {
		b.debugInfo = debugInfoProvider
	}
}

func WithOrderingDisabledCheckForBarrierKey(orderingDisabledForKey func(key BarrierKey) bool) OptFn {
	return func(b *Barrier) {
		b.orderingDisabledForKey = orderingDisabledForKey
	}
}

// NewBarrier creates a new properly initialized Barrier
func NewBarrier(fns ...OptFn) *Barrier {
	b := &Barrier{
		barriers:                 make(map[BarrierKey]*barrierInfo),
		metadata:                 make(map[string]string),
		eventOrderKeyThreshold:   config.SingleValueLoader(0),
		disabledStateDuration:    config.SingleValueLoader(10 * time.Minute),
		halfEnabledStateDuration: config.SingleValueLoader(5 * time.Minute),
		drainLimit:               config.SingleValueLoader(0),
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
	barriers map[BarrierKey]*barrierInfo
	metadata map[string]string

	eventOrderKeyThreshold   config.ValueLoader[int] // maximum number of concurrent jobs for a given key (0 means no threshold)
	disabledStateDuration    config.ValueLoader[time.Duration]
	halfEnabledStateDuration config.ValueLoader[time.Duration]

	drainLimit config.ValueLoader[int] // maximum number of concurrent jobs to accept after a previously failed job has been aborted

	debugInfo func(key BarrierKey) string

	orderingDisabledForKey func(key BarrierKey) bool
}

type BarrierKey struct {
	DestinationID, UserID, WorkspaceID string
}

func (bk *BarrierKey) String() string {
	return fmt.Sprintf("%s:%s:%s", bk.WorkspaceID, bk.DestinationID, bk.UserID)
}

// Enter the barrier for this key and jobID. If there is not already a barrier for this key
// returns true, otherwise false along with the previous failed jobID if this is the cause of the barrier.
// Another scenario where a barrier might exist for a key is when the previous job has failed in an unrecoverable manner and the drain limiter is enabled.
func (b *Barrier) Enter(key BarrierKey, jobID int64) (accepted bool, previousFailedJobID *int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	barrier, ok := b.barriers[key]
	if !ok {
		if b.eventOrderKeyThreshold.Load() == 0 { // if no key threshold is set accept the job
			return true, nil
		}
		// create a new barrier with the concurrency limiter enabled
		barrier = &barrierInfo{
			concurrencyLimiter: make(map[int64]struct{}),
		}
		b.barriers[key] = barrier
	}

	b.updateState(barrier, key)

	// if the barrier is in a disabled state, accept the job
	if barrier.state == stateDisabled {
		return true, nil
	}

	// if key threshold is reached, disable the barrier and accept the job
	if barrier.concurrencyLimitReached(jobID, b.eventOrderKeyThreshold.Load()) {
		b.barriers[key] = &barrierInfo{
			state:              stateDisabled,
			stateTime:          time.Now(),
			concurrencyLimiter: make(map[int64]struct{}),
		}
		return true, nil
	}

	// if drain limit is reached, don't accept the job
	if barrier.DrainLimitReached(jobID, b.drainLimit.Load()) {
		return false, nil
	}

	// if there is a failed job in the barrier, only this job can enter the barrier
	if barrier.failedJobID != nil {
		failedJob := *barrier.failedJobID
		previousFailedJobID = &failedJob
		if failedJob > jobID && barrier.state == stateEnabled {
			var debugInfo string
			if b.debugInfo != nil {
				debugInfo = "\nDEBUG INFO:\n" + b.debugInfo(key)
			}
			panic(fmt.Errorf("detected illegal job sequence during barrier enter %+v: key %q, previousFailedJob:%d > jobID:%d (previouslyDisabled: %t)%s",
				b.metadata,
				key,
				failedJob,
				jobID,
				!barrier.stateTime.IsZero(),
				debugInfo))
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
func (b *Barrier) Leave(key BarrierKey, jobID int64) {
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
func (b *Barrier) Peek(key BarrierKey) (previousFailedJobID *int64) {
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
func (b *Barrier) Wait(key BarrierKey, jobID int64) (wait bool, previousFailedJobID *int64) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	barrier, ok := b.barriers[key]
	if !ok || barrier.state == stateDisabled {
		return false, nil // no barrier, don't wait
	}
	if barrier.failedJobID != nil {
		failedJob := *barrier.failedJobID
		if failedJob > jobID && barrier.state == stateEnabled {
			var debugInfo string
			if b.debugInfo != nil {
				debugInfo = "\nDEBUG INFO:\n" + b.debugInfo(key)
			}
			panic(fmt.Errorf("detected illegal job sequence during barrier wait %+v: key %q, previousFailedJob:%d > jobID:%d  (previouslyDisabled: %t)%s",
				b.metadata,
				key,
				failedJob,
				jobID,
				!barrier.stateTime.IsZero(),
				debugInfo))
		}
		return jobID > failedJob, &failedJob // wait if this is not the failed job
	}
	// no failed job, don't wait
	return false, nil
}

// StateChanged must be called at the end, after the job state change has been persisted.
// The only exception to this rule is when a job has failed in a retryable manner, in this scenario you should notify the barrier immediately after the failure.
// An [ErrUnsupportedState] error will be returned if the state is not supported.
func (b *Barrier) StateChanged(key BarrierKey, jobID int64, state string) error {
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

// Disabled returns [true] if the barrier is disabled for this key, [false] otherwise
func (b *Barrier) Disabled(key BarrierKey) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	barrier, ok := b.barriers[key]
	return ok && barrier.state == stateDisabled
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

// updateState applies the state transitions for the barrier if necessary.
//
// 1. Disabled: transitions to half-enabled after disabledStateDuration
// 2. Half-enabled: transitions to enabled after halfEnabledStateDuration
func (b *Barrier) updateState(barrier *barrierInfo, key BarrierKey) {
	if b.orderingDisabledForKey != nil && b.orderingDisabledForKey(key) {
		barrier.state = stateDisabled
		barrier.stateTime = time.Now()
	}
	switch barrier.state {
	case stateDisabled:
		if time.Since(barrier.stateTime) > b.disabledStateDuration.Load() {
			barrier.state = stateHalfEnabled
			barrier.stateTime = time.Now()
		}
	case stateHalfEnabled:
		if time.Since(barrier.stateTime) > b.halfEnabledStateDuration.Load() {
			barrier.state = stateEnabled
			barrier.stateTime = time.Now()
		}
	}
}

type barrierState int

const (
	stateEnabled barrierState = iota
	stateDisabled
	stateHalfEnabled
)

type barrierInfo struct {
	state     barrierState
	stateTime time.Time

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

// concurrencyLimitReached returns true if the barrier's concurrency limit has been reached
func (bi *barrierInfo) concurrencyLimitReached(jobID int64, limit int) bool {
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

// Inactive returns true if the barrier is enabled, there isn't a failed job and both limiters are inactive
func (bi *barrierInfo) Inactive() bool {
	return bi.state == stateEnabled && // barrier is enabled
		bi.failedJobID == nil && // no failed job
		len(bi.concurrencyLimiter) == 0 && // no concurrent jobs
		bi.drainLimiter == nil // drain limiter is off
}

type command interface {
	enqueue(b *Barrier) bool
	execute(b *Barrier)
}

type cmd struct {
	key   BarrierKey
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
	} else if barrier.state == stateDisabled {
		return // don't do anything if the barrier is disabled
	}
	barrier.Leave(c.jobID)
	if barrier.failedJobID == nil {
		barrier.failedJobID = &c.jobID
	} else if *barrier.failedJobID > c.jobID && barrier.state == stateEnabled {
		var debugInfo string
		if b.debugInfo != nil {
			debugInfo = "\nDEBUG INFO:\n" + b.debugInfo(c.key)
		}
		panic(fmt.Errorf("detected illegal job sequence during barrier job failed %+v: key %q, previousFailedJob:%d > jobID:%d (previouslyDisabled: %t)%s",
			b.metadata,
			c.key,
			*barrier.failedJobID,
			c.jobID,
			!barrier.stateTime.IsZero(),
			debugInfo))
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
	if barrier, ok := b.barriers[c.key]; ok && barrier.state != stateDisabled {
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
	if barrier, ok := b.barriers[c.key]; ok && barrier.state != stateDisabled {
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
	if barrier, ok := b.barriers[c.key]; ok && barrier.state != stateDisabled {
		barrier.Leave(c.jobID)
		if barrier.failedJobID != nil && *barrier.failedJobID != c.jobID { // out-of-sync command (failed commands get executed immediately)
			return
		}
		// previouslyFailed indicates whether the job that was aborted was previouly a failed job, which is the condition for enabling the drain limiter
		previouslyFailed := barrier.failedJobID != nil && *barrier.failedJobID == c.jobID
		barrier.failedJobID = nil                        // remove the failed job
		barrier.drainLimiter = nil                       // turn off drain limiter
		if b.drainLimit.Load() > 0 && previouslyFailed { // enable the drain limiter only if a previously failed job has been aborted
			barrier.drainLimiter = make(map[int64]struct{})
		}
		if barrier.Inactive() {
			delete(b.barriers, c.key)
		}
	}
}
