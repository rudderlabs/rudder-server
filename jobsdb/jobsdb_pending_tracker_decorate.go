package jobsdb

import (
	"context"
	"fmt"
	"sort"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/jsonparser"
)

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
	var txid int64
	if err := tx.SqlTx().QueryRowContext(ctx, `SELECT pg_current_xact_id()::text::bigint`).Scan(&txid); err != nil {
		return fmt.Errorf("pending events tracker: reading transaction id: %w", err)
	}
	xid := uint64(txid)
	t.inflightMu.Lock()
	t.inflight[xid]++
	t.inflightMu.Unlock()
	tx.Tx().AddFinallyListener(func() {
		t.inflightMu.Lock()
		defer t.inflightMu.Unlock()
		if t.inflight[xid]--; t.inflight[xid] <= 0 {
			delete(t.inflight, xid)
		}
	})
	if err := t.JobsDB.UpdateJobStatusInTx(ctx, tx, statusList); err != nil {
		return err
	}
	tx.Tx().AddSuccessListener(func() { t.subtract(xid, terminal) })
	return nil
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
