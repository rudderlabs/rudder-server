package jobsdb

import (
	"context"
	"fmt"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/jsonparser"
)

func (rc *readCursor) UpdateJobStatus(ctx context.Context, statusList []*JobStatusT) error {
	return rc.WithUpdateSafeTx(ctx, func(tx UpdateSafeTx) error {
		return rc.UpdateJobStatusInTx(ctx, tx, statusList)
	})
}

// UpdateJobStatusInTx makes the jobs of non-terminal statuses pending before the statuses commit, so that no
// read misses them, and the jobs of terminal statuses no longer pending after they commit. A rolled back
// write leaves its jobs pending until they are terminal.
func (rc *readCursor) UpdateJobStatusInTx(ctx context.Context, tx UpdateSafeTx, statusList []*JobStatusT) error {
	if rc.disabled.Load() {
		return rc.JobsDB.UpdateJobStatusInTx(ctx, tx, statusList)
	}
	// the keys are computed outside pipelinesMu, so that a new pipeline's write lock, and the reads queued
	// behind it, don't wait for a large batch to be parsed; shapes only grows, so the snapshot stays valid
	rc.pipelinesMu.RLock()
	shapes := rc.shapes
	rc.pipelinesMu.RUnlock()
	keys := make([]pipelineKey, 0, len(statusList)*len(shapes)) // len(shapes) keys per status
	for _, status := range statusList {
		var err error
		if keys, err = rc.appendStatusKeys(keys, shapes, status); err != nil {
			rc.disable(err)
			return rc.JobsDB.UpdateJobStatusInTx(ctx, tx, statusList)
		}
	}
	pending, terminal := map[*pipeline][]int64{}, map[*pipeline][]int64{}
	rc.pipelinesMu.RLock()
	for i, status := range statusList {
		jobs := pending
		if _, ok := terminalStates[status.JobState]; ok {
			jobs = terminal
		}
		for _, key := range keys[i*len(shapes) : (i+1)*len(shapes)] {
			if p, ok := rc.pipelines[key]; ok {
				jobs[p] = append(jobs[p], status.JobID)
			}
		}
	}
	rc.pipelinesMu.RUnlock()
	for p, jobIDs := range pending {
		p.addPending(jobIDs)
	}
	if len(terminal) > 0 {
		tx.Tx().AddSuccessListener(func() {
			for p, jobIDs := range terminal {
				p.removePending(jobIDs)
			}
		})
	}
	return rc.JobsDB.UpdateJobStatusInTx(ctx, tx, statusList)
}

// appendStatusKeys appends the key that the job of status has for each of shapes. It fails if status lacks the
// custom value, workspace or parameters of its job, or the source or destination that a shape filters on.
func (rc *readCursor) appendStatusKeys(keys []pipelineKey, shapes []pipelineKeyShape, status *JobStatusT) ([]pipelineKey, error) {
	if status.CustomVal == "" || status.WorkspaceId == "" || len(status.JobParameters) == 0 {
		return keys, fmt.Errorf("status lacks the custom value, workspace or parameters of its job: %s", statusAttributes(status))
	}
	consumer := lo.Ternary(rc.h.conf.multiConsumer, status.Consumer, "")
	for _, shape := range shapes {
		key := pipelineKey{CustomVal: lo.Ternary(shape.CustomVal, status.CustomVal, ""), FilterType: shape.FilterType, Consumer: consumer}
		switch shape.FilterType {
		case workspaceFilterType:
			key.FilterValue = status.WorkspaceId
		case sourceFilterType:
			key.FilterValue = jsonparser.GetStringOrEmpty(status.JobParameters, "source_id")
		case destinationFilterType:
			key.FilterValue = jsonparser.GetStringOrEmpty(status.JobParameters, "destination_id")
		}
		if key.FilterValue == "" && shape.FilterType != noneFilterType {
			return keys, fmt.Errorf("status lacks the %s of its job: %s", shape.FilterType, statusAttributes(status))
		}
		keys = append(keys, key)
	}
	return keys, nil
}

// statusAttributes describes the attributes of status that pipelines filter on.
func statusAttributes(status *JobStatusT) string {
	return fmt.Sprintf("job %d, state %q, custom value %q, workspace %q, source %q, destination %q, consumer %q",
		status.JobID, status.JobState, status.CustomVal, status.WorkspaceId,
		jsonparser.GetStringOrEmpty(status.JobParameters, "source_id"), jsonparser.GetStringOrEmpty(status.JobParameters, "destination_id"), status.Consumer)
}
