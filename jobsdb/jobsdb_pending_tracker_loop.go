package jobsdb

import (
	"context"
	"database/sql"
	"fmt"
	"math"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/metric"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
)

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

// iterate runs transitions, up to maxDSPerTransition sealed datasets at a time, until one counts the live
// dataset, or else the poll, then the export. Transition and poll check the decision again on their pinned
// list, and if the list changed in between, iterate decides again.
func (t *pendingEventsTracker) iterate(ctx context.Context) (err error) {
	defer func() {
		if err == nil {
			t.export()
		}
	}()
	for {
		_, ranges := t.h.dsList.snapshot() // avoid locking if not needed
		if t.shouldTransition(ranges) {
			live, err := t.transition(ctx)
			if err != nil || live {
				return err
			}
			continue
		}
		transitionDue, err := t.poll(ctx)
		if err != nil || !transitionDue {
			return err
		}
	}
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

// nextBoundary returns the boundary after the next transition: the end of the maxDS-th sealed dataset with
// jobs at or above boundary, or of the last one if there are fewer, or boundary itself if there is none.
// Job ids increase across the ranges.
func nextBoundary(ranges dataSetRangeTList, boundary int64, maxDS int) int64 {
	next := boundary
	for _, r := range ranges {
		if r.maxJobID < boundary {
			continue
		}
		next = r.maxJobID + 1
		if maxDS--; maxDS <= 0 {
			break
		}
	}
	return next
}

// transition keeps compaction out, pins the dataset list and moves the boundary past up to
// maxDSPerTransition sealed datasets. It reports whether it also counted the live dataset, i.e. whether the
// boundary reached the last sealed dataset.
func (t *pendingEventsTracker) transition(ctx context.Context) (bool, error) {
	if t.beforeCompactionLock != nil {
		t.beforeCompactionLock()
	}
	// A compaction that commits between pinning the dataset list and reading the snapshot moves pending
	// jobs out of the pinned datasets, so their later statuses would be invisible to the count while the
	// snapshot still sees them. Keep compaction out for the whole transition, as GetPileUpCounts does.
	if !t.h.compactionLock.RTryLockWithCtx(ctx) {
		return false, fmt.Errorf("acquiring the compaction read lock: %w", ctx.Err())
	}
	defer t.h.compactionLock.RUnlock()
	dsList, ranges, release, err := t.h.acquireDSListForRead(ctx)
	if err != nil {
		return false, err
	}
	defer release()
	if len(dsList) == 0 {
		return true, nil // nothing to count: end the iteration
	}
	// A compaction may have run before the lock was acquired and removed the newly sealed jobs, so decide
	// again on the list that the transition counts: a transition must never move the boundary backwards.
	if !t.shouldTransition(ranges) {
		return false, nil
	}
	if t.afterPin != nil {
		t.afterPin()
	}
	newBoundary := nextBoundary(ranges, t.currentBoundary(), t.conf.maxDSPerTransition.Load())
	live := newBoundary == sealedBoundary(ranges)
	return live, t.doTransition(ctx, ranges, dsList[len(dsList)-1], newBoundary, live)
}

// doTransition moves the boundary to newBoundary and counts the newly frozen jobs in one snapshot. If the
// boundary reaches the last sealed dataset (live), it also counts the live jobs in the same snapshot. It
// applies the counts to the gauges with one write per key.
func (t *pendingEventsTracker) doTransition(ctx context.Context, ranges dataSetRangeTList, last dataSetT, newBoundary int64, live bool) (err error) {
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
	for _, r := range ranges {
		if r.maxJobID < oldBoundary || r.minJobID >= newBoundary {
			continue
		}
		if err = t.countInto(ctx, tx, r.ds, oldBoundary, counts); err != nil {
			return err
		}
	}
	var fp *tableFingerprint
	liveCounts := make(map[pendingKey]int64)
	if live {
		if fp, err = t.fingerprint(ctx, tx, last); err != nil {
			return err
		}
		if err = t.countInto(ctx, tx, last, newBoundary, liveCounts); err != nil {
			return err
		}
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
	if !live { // the live share stays until the transition that reaches the last sealed dataset replaces it
		for key, count := range counts {
			t.add(registry, key, count)
		}
		return nil
	}
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

// poll recounts the live jobs of the last dataset, if its tables changed since the last live count. It
// reports true, without counting, if the pinned list needs a transition first.
func (t *pendingEventsTracker) poll(ctx context.Context) (bool, error) {
	dsList, ranges, release, err := t.h.acquireDSListForRead(ctx)
	if err != nil {
		return false, err
	}
	defer release()
	if len(dsList) == 0 {
		return false, nil
	}
	// A dataset sealed since iterate decided is not the last one anymore, so the poll would drop its jobs
	// from the gauge: report that a transition is due instead.
	if t.shouldTransition(ranges) {
		return true, nil
	}
	last, boundary := dsList[len(dsList)-1], t.currentBoundary()
	ctx, cancel := context.WithTimeout(ctx, t.conf.queryTimeout.Load())
	defer cancel()
	db := t.h.maintenanceDB()
	fp, err := t.fingerprint(ctx, db, last)
	if err != nil {
		return false, err
	}
	if t.liveFingerprint != nil && *fp == *t.liveFingerprint {
		return false, nil
	}
	defer t.stats.poll.RecordDuration()()
	liveCounts := make(map[pendingKey]int64)
	if err := t.countInto(ctx, db, last, boundary, liveCounts); err != nil {
		return false, err
	}
	t.mu.RLock()
	registry := t.registry
	t.mu.RUnlock()
	for _, key := range lo.UniqKeys(liveCounts, t.liveInGauge) {
		t.add(registry, key, liveCounts[key]-t.liveInGauge[key])
	}
	t.liveInGauge, t.liveFingerprint = liveCounts, fp
	return false, nil
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

// pruneLocked drops transition ranges that no pending listener needs anymore: a range is safe to
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
