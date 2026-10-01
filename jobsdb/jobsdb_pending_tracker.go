package jobsdb

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/lib/pq"
	"github.com/samber/lo"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonparser"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/metric"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	"github.com/rudderlabs/rudder-server/services/rmetrics"
	"github.com/rudderlabs/rudder-server/utils/crash"
)

// NewPendingEventsTracker wraps a read-only handle with a pending-events gauge that is correct even
// when other processes store jobs into the handle's datasets, e.g. the gateway pods storing into gw.
//
// Jobs below a moving job-id boundary are frozen: their datasets are sealed, so the tracker counts
// them once and then subtracts their terminal statuses in memory as they commit. Jobs at or above the
// boundary are live: the tracker polls the last dataset for them, and only when its tables changed.
// Where a count and the in-memory subtractions overlap in time, the Postgres snapshot of the count
// decides, using the transaction id that every status transaction records.
//
// Every status write of the process must go through the returned JobsDB.
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
	t.trigger = func() <-chan time.Time { return time.After(t.conf.interval.Load()) }
	tags := stats.Tags{"customVal": h.tablePrefix}
	t.stats.reset = h.stats.NewTaggedStat("jobsdb_pending_tracker_reset", stats.CountType, tags)
	t.stats.transition = h.stats.NewTaggedStat("pending_tracker_query", stats.TimerType, stats.Tags{"customVal": h.tablePrefix, "op": "transition"})
	t.stats.poll = h.stats.NewTaggedStat("pending_tracker_query", stats.TimerType, stats.Tags{"customVal": h.tablePrefix, "op": "poll"})
	return t, nil
}

type pendingEventsTracker struct {
	JobsDB
	h   *Handle
	log logger.Logger

	measurementName string
	stats           struct {
		reset, transition, poll stats.Measurement
	}

	conf struct {
		interval        config.ValueLoader[time.Duration]
		queryTimeout    config.ValueLoader[time.Duration]
		snapshotTimeout config.ValueLoader[time.Duration]
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

func (t *pendingEventsTracker) UpdateJobStatus(ctx context.Context, statusList []*JobStatusT) error {
	return t.WithUpdateSafeTx(ctx, func(tx UpdateSafeTx) error {
		return t.UpdateJobStatusInTx(ctx, tx, statusList)
	})
}

// UpdateJobStatusInTx records the transaction id, updates the statuses and, after the commit,
// subtracts the terminal statuses of frozen jobs that the counts have not already excluded.
func (t *pendingEventsTracker) UpdateJobStatusInTx(ctx context.Context, tx UpdateSafeTx, statusList []*JobStatusT) error {
	terminal := lo.Filter(statusList, func(status *JobStatusT, _ int) bool {
		_, ok := terminalStates[status.JobState]
		return ok
	})
	if len(terminal) == 0 { // nothing that can change the gauge, e.g. executing statuses
		return t.JobsDB.UpdateJobStatusInTx(ctx, tx, statusList)
	}
	var xid int64
	if err := tx.SqlTx().QueryRowContext(ctx, `SELECT pg_current_xact_id()::text::bigint`).Scan(&xid); err != nil {
		return fmt.Errorf("pending events tracker: reading transaction id: %w", err)
	}
	t.trackInflight(uint64(xid))
	tx.Tx().AddFinallyListener(func() { t.untrackInflight(uint64(xid)) })
	if err := t.JobsDB.UpdateJobStatusInTx(ctx, tx, statusList); err != nil {
		return err
	}
	tx.Tx().AddSuccessListener(func() { t.subtract(uint64(xid), terminal) })
	return nil
}

func (t *pendingEventsTracker) trackInflight(xid uint64) {
	t.inflightMu.Lock()
	t.inflight[xid]++
	t.inflightMu.Unlock()
}

func (t *pendingEventsTracker) untrackInflight(xid uint64) {
	t.inflightMu.Lock()
	if t.inflight[xid] <= 1 {
		delete(t.inflight, xid)
	} else {
		t.inflight[xid]--
	}
	t.inflightMu.Unlock()
}

// subtract runs after a status transaction commits. For each terminal status of a frozen job it
// asks the snapshot of the transition that counted the job whether that count saw this transaction.
// Seen: the count excluded the job already. Not seen: the count included the job, so subtract it.
func (t *pendingEventsTracker) subtract(xid uint64, statuses []*JobStatusT) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	deltas := make(map[pendingKey]int64)
	for _, status := range statuses {
		if status.JobID >= t.boundary { // live job, the poll counts it
			continue
		}
		if r := t.findTransitionLocked(status.JobID); r != nil && r.snap.sees(xid) {
			continue
		}
		deltas[pendingKey{
			workspaceID: status.WorkspaceId,
			sourceID:    jsonparser.GetStringOrEmpty(status.JobParameters, "source_id"),
			consumer:    status.Consumer,
		}]--
	}
	for key, delta := range deltas {
		t.add(t.registry, key, delta)
	}
}

// findTransitionLocked returns the transition range that contains jobID, or nil if it was pruned.
func (t *pendingEventsTracker) findTransitionLocked(jobID int64) *transitionRange {
	i := sort.Search(len(t.transitions), func(i int) bool { return t.transitions[i].hi > jobID })
	if i < len(t.transitions) && t.transitions[i].lo <= jobID {
		return &t.transitions[i]
	}
	return nil
}

// pruneLocked drops transition ranges that no pending listener can need anymore: a range is safe to
// drop once no in-flight status transaction has an xid below its snapshot's xmax. A later listener
// for that range then belongs to a transaction the snapshot did not see, so "subtract" is correct.
// The newest range is always kept.
func (t *pendingEventsTracker) pruneLocked() {
	if len(t.transitions) < 2 {
		return
	}
	t.inflightMu.Lock()
	minInflight := uint64(math.MaxUint64)
	for xid := range t.inflight {
		minInflight = min(minInflight, xid)
	}
	t.inflightMu.Unlock()
	last := len(t.transitions) - 1
	kept := t.transitions[:0]
	for i, r := range t.transitions {
		if i == last || r.snap.xmax > minInflight {
			kept = append(kept, r)
		}
	}
	t.transitions = kept
}

func (t *pendingEventsTracker) Start() error {
	if err := t.JobsDB.Start(); err != nil {
		return err
	}
	t.lifecycle.mu.Lock()
	defer t.lifecycle.mu.Unlock()
	if t.lifecycle.started {
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	g := &errgroup.Group{}
	g.Go(crash.Wrapper(func() error {
		t.loop(ctx)
		return nil
	}))
	t.lifecycle.started, t.lifecycle.cancel, t.lifecycle.group = true, cancel, g
	return nil
}

func (t *pendingEventsTracker) Stop() {
	t.lifecycle.mu.Lock()
	if t.lifecycle.started {
		t.lifecycle.cancel()
		_ = t.lifecycle.group.Wait()
		t.lifecycle.started = false
	}
	t.lifecycle.mu.Unlock()
	t.JobsDB.Stop()
}

func (t *pendingEventsTracker) loop(ctx context.Context) {
	for {
		if err := t.iterate(ctx); err != nil && ctx.Err() == nil {
			t.log.Warnn("Pending events iteration failed", obskit.Error(err))
		}
		select {
		case <-ctx.Done():
			return
		case <-t.trigger():
		}
	}
}

// iterate runs exactly one of the transition and the poll, then the export.
func (t *pendingEventsTracker) iterate(ctx context.Context) error {
	defer t.export()
	_, ranges := t.h.dsList.snapshot() // deciding reads no tables, so it needs no pin
	if t.shouldTransition(ranges) {
		return t.transition(ctx)
	}
	return t.poll(ctx)
}

// shouldTransition reports whether the tracker has not counted yet, or a dataset was sealed since its last transition.
func (t *pendingEventsTracker) shouldTransition(ranges dataSetRangeTList) bool {
	return t.liveFingerprint == nil || sealedBoundary(ranges) > t.currentBoundary()
}

func (t *pendingEventsTracker) currentBoundary() int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.boundary
}

// sealedBoundary returns the first job id after every sealed dataset. Empty datasets are left out of the
// ranges, so it takes the highest job id over all of them rather than the second-to-last dataset's.
func sealedBoundary(ranges dataSetRangeTList) int64 {
	var b int64
	for _, r := range ranges {
		b = max(b, r.maxJobID+1)
	}
	return b
}

// transition keeps compaction out, pins the dataset list and runs the transition on it, or the poll if
// the pinned list no longer needs a transition.
func (t *pendingEventsTracker) transition(ctx context.Context) error {
	if t.beforeCompactionLock != nil {
		t.beforeCompactionLock()
	}
	// A compaction that commits between pinning the dataset list and reading the snapshot moves pending
	// jobs out of the pinned datasets, so their later statuses would be invisible to the count while the
	// snapshot still sees them. Keep compaction out for the whole transition, as GetPileUpCounts does.
	if !t.h.compactionLock.RTryLockWithCtx(ctx) {
		return fmt.Errorf("acquiring the compaction read lock: %w", ctx.Err())
	}
	defer t.h.compactionLock.RUnlock()
	dsList, ranges, release, err := t.h.acquireDSListForRead(ctx)
	if err != nil {
		return err
	}
	defer release()
	if len(dsList) == 0 {
		return nil
	}
	// A compaction may have run before the lock was acquired and removed the newly sealed jobs, so decide
	// again on the list that the transition counts: a transition must never move the boundary backwards.
	if !t.shouldTransition(ranges) {
		return t.poll(ctx)
	}
	if t.afterPin != nil {
		t.afterPin()
	}
	return t.doTransition(ctx, ranges, dsList[len(dsList)-1], sealedBoundary(ranges))
}

// doTransition moves the boundary, counts the newly frozen jobs and the live jobs in one snapshot, and
// applies both to the gauges with one write per key.
func (t *pendingEventsTracker) doTransition(ctx context.Context, ranges dataSetRangeTList, last dataSetT, newBoundary int64) (err error) {
	defer t.stats.transition.RecordDuration()()
	ctx, cancel := context.WithTimeout(ctx, t.conf.queryTimeout.Load())
	defer cancel()
	// Begin before the lock: BEGIN takes no snapshot, the first statement does, so the lock covers
	// one round trip and not the wait for a pool connection.
	tx, err := t.h.maintenanceDB().BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return fmt.Errorf("beginning transition transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	t.mu.Lock()
	snap, err := t.readSnapshot(ctx, tx)
	if err != nil {
		t.mu.Unlock()
		return err // nothing changed, retry on the next iteration
	}
	oldBoundary := t.boundary
	t.boundary = newBoundary
	if oldBoundary < newBoundary {
		t.transitions = append(t.transitions, transitionRange{lo: oldBoundary, hi: newBoundary, snap: snap})
	}
	t.pruneLocked()
	t.mu.Unlock()

	// From here on the boundary has moved: a failure must reset, so that nothing is left half-counted.
	defer func() {
		if err != nil {
			t.reset()
		}
	}()
	if t.afterSnapshot != nil {
		t.afterSnapshot()
	}

	counts := make(map[pendingKey]int64)
	if oldBoundary < newBoundary {
		for _, r := range ranges {
			if r.maxJobID < oldBoundary || r.minJobID >= newBoundary {
				continue
			}
			if err = t.countInto(ctx, tx, r.ds, oldBoundary, newBoundary, counts); err != nil {
				return err
			}
		}
	}
	fp, err := t.fingerprint(ctx, tx, last)
	if err != nil {
		return err
	}
	liveCounts := make(map[pendingKey]int64)
	if err = t.countInto(ctx, tx, last, newBoundary, 0, liveCounts); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil { // read-only: ending the transaction releases the snapshot and the connection
		return fmt.Errorf("committing transition transaction: %w", err)
	}
	if t.afterCount != nil {
		t.afterCount()
	}

	t.mu.RLock()
	registry := t.registry
	t.mu.RUnlock()
	for _, key := range lo.UniqKeys(counts, liveCounts, t.liveInGauge) {
		t.add(registry, key, counts[key]+liveCounts[key]-t.liveInGauge[key])
	}
	t.liveInGauge, t.liveFingerprint = liveCounts, fp
	return nil
}

// readSnapshot runs the first statement of the transition transaction, which pins its snapshot.
func (t *pendingEventsTracker) readSnapshot(ctx context.Context, tx *sql.Tx) (pgSnapshot, error) {
	ctx, cancel := context.WithTimeout(ctx, t.conf.snapshotTimeout.Load())
	defer cancel()
	var text string
	if err := tx.QueryRowContext(ctx, `SELECT pg_current_snapshot()::text`).Scan(&text); err != nil {
		return pgSnapshot{}, fmt.Errorf("reading snapshot: %w", err)
	}
	return parsePgSnapshot(text)
}

// reset discards the tracked state after a failed transition. The gauges are discarded, not set to
// zero: zero would report that nothing is pending, which the tracker cannot know. While the boundary
// is 0 every listener sees a live job and does nothing, so the new gauges collect no subtractions.
func (t *pendingEventsTracker) reset() {
	t.mu.Lock()
	t.boundary = 0
	t.transitions = nil
	t.registry = metric.NewRegistry()
	t.mu.Unlock()
	t.liveInGauge, t.liveFingerprint = nil, nil
	t.stats.reset.Increment()
}

// poll recounts the live jobs of the last dataset, if its tables changed since the last live count.
func (t *pendingEventsTracker) poll(ctx context.Context) error {
	dsList, _, release, err := t.h.acquireDSListForRead(ctx)
	if err != nil {
		return err
	}
	defer release()
	if len(dsList) == 0 {
		return nil
	}
	last, boundary := dsList[len(dsList)-1], t.currentBoundary()
	ctx, cancel := context.WithTimeout(ctx, t.conf.queryTimeout.Load())
	defer cancel()
	db := t.h.maintenanceDB()
	fp, err := t.fingerprint(ctx, db, last)
	if err != nil {
		return err
	}
	if t.liveFingerprint != nil && *fp == *t.liveFingerprint {
		return nil
	}
	defer t.stats.poll.RecordDuration()()
	liveCounts := make(map[pendingKey]int64)
	if err := t.countInto(ctx, db, last, boundary, 0, liveCounts); err != nil {
		return err
	}
	t.mu.RLock()
	registry := t.registry
	t.mu.RUnlock()
	for _, key := range lo.UniqKeys(liveCounts, t.liveInGauge) {
		t.add(registry, key, liveCounts[key]-t.liveInGauge[key])
	}
	t.liveInGauge, t.liveFingerprint = liveCounts, fp
	return nil
}

// export emits every gauge. Nothing is emitted before the first transition completes.
func (t *pendingEventsTracker) export() {
	if t.liveFingerprint == nil {
		return
	}
	t.mu.RLock()
	registry := t.registry
	t.mu.RUnlock()
	registry.Range(func(key, value any) bool {
		m := key.(metric.Measurement)
		if g, ok := value.(metric.Gauge); ok {
			t.h.stats.NewTaggedStat(m.GetName(), stats.GaugeType, m.GetTags()).Gauge(g.Value())
		}
		return true
	})
}

type queryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// fingerprint reads the pg_stat counters of the dataset's jobs and status tables.
func (t *pendingEventsTracker) fingerprint(ctx context.Context, db queryer, ds dataSetT) (*tableFingerprint, error) {
	var fp tableFingerprint
	var jobsOID, statusOID int64
	if err := db.QueryRowContext(ctx, `SELECT
		j::oid::bigint, pg_stat_get_tuples_inserted(j), pg_stat_get_tuples_updated(j), pg_stat_get_tuples_deleted(j),
		s::oid::bigint, pg_stat_get_tuples_inserted(s), pg_stat_get_tuples_updated(s), pg_stat_get_tuples_deleted(s)
		FROM (SELECT $1::regclass AS j, $2::regclass AS s) t`, ds.JobTable, ds.JobStatusTable).Scan(
		&jobsOID, &fp.jobs.ins, &fp.jobs.upd, &fp.jobs.del,
		&statusOID, &fp.status.ins, &fp.status.upd, &fp.status.del,
	); err != nil {
		return nil, fmt.Errorf("reading fingerprint of %s: %w", ds.Index, err)
	}
	fp.jobs.oid, fp.status.oid = uint32(jobsOID), uint32(statusOID)
	return &fp, nil
}

// countInto adds the pending jobs of ds with fromID <= job_id < toID to counts, per key. A toID of 0 means
// no upper bound. A job pending for several consumers counts once per consumer.
func (t *pendingEventsTracker) countInto(ctx context.Context, db queryer, ds dataSetT, fromID, toID int64, counts map[pendingKey]int64) error {
	args := []any{pq.Array(validNonTerminalStates), fromID}
	jobRange := func(column string) string {
		if toID == 0 {
			return column + " >= $2"
		}
		return column + " >= $2 AND " + column + " < $3"
	}
	if toID > 0 {
		args = append(args, toID)
	}
	query := `SELECT j.workspace_id, COALESCE(` + SourceID.string() + `, ''), '', COUNT(*)
	FROM %[1]q j
	LEFT JOIN (
		SELECT DISTINCT ON (job_id) job_id, job_state FROM %[2]q
		WHERE %[3]s
		ORDER BY job_id ASC, id DESC
	) s ON s.job_id = j.job_id
	WHERE %[4]s AND (s.job_id IS NULL OR s.job_state = ANY($1))
	GROUP BY 1, 2, 3`
	if t.h.conf.multiConsumer {
		query = `SELECT j.workspace_id, COALESCE(` + SourceID.string() + `, ''), c.consumer, COUNT(*)
	FROM %[1]q j
	CROSS JOIN LATERAL unnest(j.consumers) AS c(consumer)
	LEFT JOIN LATERAL (
		SELECT job_state FROM %[2]q WHERE job_id = j.job_id AND consumer = c.consumer ORDER BY id DESC LIMIT 1
	) s ON true
	WHERE %[4]s AND (s.job_state IS NULL OR s.job_state = ANY($1))
	GROUP BY 1, 2, 3`
	}
	rows, err := db.QueryContext(ctx, fmt.Sprintf(query, ds.JobTable, ds.JobStatusTable, jobRange("job_id"), jobRange("j.job_id")), args...)
	if err != nil {
		return fmt.Errorf("counting pending jobs of %s from job id %d: %w", ds.Index, fromID, err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var key pendingKey
		var count int64
		if err := rows.Scan(&key.workspaceID, &key.sourceID, &key.consumer, &count); err != nil {
			return fmt.Errorf("scanning pending jobs of %s: %w", ds.Index, err)
		}
		counts[key] += count
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterating pending jobs of %s: %w", ds.Index, err)
	}
	return nil
}
