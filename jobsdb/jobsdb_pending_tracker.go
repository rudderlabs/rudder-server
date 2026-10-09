package jobsdb

import (
	"context"
	"fmt"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/metric"

	"github.com/rudderlabs/rudder-server/services/rmetrics"
)

// NewPendingEventsTracker wraps a read-only handle with a pending-events gauge that is correct even
// when other processes store jobs into the handle's datasets, e.g. the gateway pods storing into gw.
//
// Jobs below a moving job-id boundary are frozen: their datasets are sealed, so the tracker counts
// them once and then subtracts their terminal statuses in memory as they commit. Jobs at or above the
// boundary are live: the tracker polls the last dataset for them, and only when its tables changed.
// Where a count and the in-memory subtractions overlap in time, the postgres snapshot of the count
// decides, using the transaction id that every status transaction records.
//
// Every status update of the process must go through the returned JobsDB.
func NewPendingEventsTracker(h *Handle) (JobsDB, error) {
	if h.ownerType != Read {
		return nil, fmt.Errorf("pending events tracker: %q is not a read-only handle", h.tablePrefix)
	}
	t := &pendingEventsTracker{
		JobsDB:          h,
		h:               h,
		registry:        metric.NewRegistry(),
		inflight:        make(map[uint64]int),
		log:             h.logger.Child("pendingEventsTracker"),
		measurementName: fmt.Sprintf(rmetrics.JobsdbPendingEventsCount, h.tablePrefix),
	}
	t.conf.interval = h.config.GetReloadableDurationVar(30, time.Second, h.configKeys("pendingEvents.interval")...)
	t.conf.queryTimeout = h.config.GetReloadableDurationVar(120, time.Second, h.configKeys("pendingEvents.queryTimeout")...)
	t.conf.snapshotTimeout = h.config.GetReloadableDurationVar(10, time.Second, h.configKeys("pendingEvents.snapshotTimeout")...)
	t.conf.maxDSPerTransition = h.config.GetReloadableIntVar(3, 1, h.configKeys("pendingEvents.maxDSPerTransition")...)
	t.trigger = func() <-chan time.Time { return time.After(t.conf.interval.Load()) }
	tags := stats.Tags{"tablePrefix": h.tablePrefix}
	t.stats.reset = h.stats.NewTaggedStat("jobsdb_pending_tracker_reset", stats.CountType, tags)
	t.stats.transition = h.stats.NewTaggedStat("jobsdb_pending_tracker_query", stats.TimerType, stats.Tags{"tablePrefix": h.tablePrefix, "op": "transition"})
	t.stats.poll = h.stats.NewTaggedStat("jobsdb_pending_tracker_query", stats.TimerType, stats.Tags{"tablePrefix": h.tablePrefix, "op": "poll"})
	return t, nil
}

type pendingEventsTracker struct {
	JobsDB
	h   *Handle
	log logger.Logger

	measurementName string
	stats           struct {
		reset            stats.Counter
		transition, poll stats.Timer
	}

	conf struct {
		interval           config.ValueLoader[time.Duration] // time between two loop iterations (transitions or poll, then export)
		queryTimeout       config.ValueLoader[time.Duration] // timeout of a transition or a poll
		snapshotTimeout    config.ValueLoader[time.Duration] // timeout of the snapshot read, which runs while mu is held for writing
		maxDSPerTransition config.ValueLoader[int]           // maximum number of sealed datasets that one transition counts
	}

	// mu protects boundary, transitions and the registry pointer. Listeners take it for reading, the
	// loop takes it for writing only to read a transition's snapshot and to reset.
	mu          sync.RWMutex
	boundary    int64             // first job id of the live region, 0 before the first transition
	transitions []transitionRange // one entry per transition, sorted by job id, non-overlapping
	registry    metric.Registry   // one gauge per key, plus one _all gauge per consumer

	// inflightMu protects inflight: the xids of status transactions whose listeners have not run yet.
	inflightMu sync.Mutex
	inflight   map[uint64]int

	// Owned by the loop goroutine.
	liveInGauge     map[pendingKey]int64 // the live share that each gauge holds right now
	liveFingerprint *tableFingerprint    // pg_stat counters of the last dataset, read before the last live count. nil until the first transition and after a reset

	// trigger returns a channel that fires when the next loop iteration is due. Tests replace it.
	trigger func() <-chan time.Time
	// test hooks, run by the loop if set
	beforeCompactionLock func()
	afterPin             func()
	afterSnapshot        func()
	afterCount           func()

	lifecycle struct {
		mu      sync.Mutex
		started bool
		cancel  context.CancelFunc
		group   *errgroup.Group
	}
}

// transitionRange is the job-id range [lo, hi) that a transition counted, and the snapshot it counted in.
type transitionRange struct {
	lo, hi int64
	snap   pgSnapshot
}

type pendingKey struct {
	workspaceID string
	sourceID    string
	consumer    string
}

// tableStats are the pg_stat counters of one table. The oid tells a recreated table apart.
type tableStats struct {
	oid           uint32
	ins, upd, del int64
}

// tableFingerprint is the change signal of a dataset: the counters of its jobs and status tables.
type tableFingerprint struct {
	jobs, status tableStats
}

type pendingMeasurement struct {
	name                  string
	workspaceID, sourceID string
	consumer              string
	all                   bool
}

func (m pendingMeasurement) GetName() string {
	if m.all {
		return m.name + "_all"
	}
	return m.name
}

func (m pendingMeasurement) GetTags() map[string]string {
	if m.all {
		return map[string]string{"consumer": m.consumer}
	}
	return map[string]string{"workspaceId": m.workspaceID, "sourceId": m.sourceID, "consumer": m.consumer}
}

// add applies delta to the gauge of key and to the _all gauge of its consumer.
func (t *pendingEventsTracker) add(registry metric.Registry, key pendingKey, delta int64) {
	if delta == 0 {
		return
	}
	registry.MustGetGauge(pendingMeasurement{name: t.measurementName, workspaceID: key.workspaceID, sourceID: key.sourceID, consumer: key.consumer}).Add(float64(delta))
	registry.MustGetGauge(pendingMeasurement{name: t.measurementName, consumer: key.consumer, all: true}).Add(float64(delta))
}
