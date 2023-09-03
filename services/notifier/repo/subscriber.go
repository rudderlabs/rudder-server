package repo

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/rudderlabs/rudder-server/services/notifier/model"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

// Claim claims a job for a worker.
func (n *Notifier) Claim(
	ctx context.Context,
	workerID string,
) (*model.Job, model.JobMetadata, error) {
	row := n.db.QueryRowContext(ctx, `
		UPDATE
  		  `+notifierTableName+`
		SET
		  status = $1,
		  updated_at = $2,
		  last_exec_time = $2,
		  worker_id = $3
		WHERE
		  id = (
			SELECT
			  id
			FROM
      		  `+notifierTableName+`
			WHERE
			  (status = $4 OR status = $5) AND
              topic = $6
			ORDER BY
			  priority ASC,
			  id ASC
			FOR
			UPDATE
			SKIP LOCKED
			LIMIT
			  1
		  ) RETURNING `+notifierTableColumns+`;
`,
		model.Executing,
		n.now().Format(timeFormat),
		workerID,
		model.Waiting,
		model.Failed,
		version,
	)

	var notifier model.Job
	err := scanJob(row.Scan, &notifier)
	if err != nil {
		return nil, nil, fmt.Errorf("claim for workerID %s: scan: %w", workerID, err)
	}

	metadata, err := n.metadataForBatchID(ctx, notifier.BatchID)
	if err != nil {
		return nil, nil, fmt.Errorf("claim for workerID %s: metadata: %w", workerID, err)
	}

	return &notifier, metadata, nil
}

// OnClaimFailed updates the status of a job to failed.
func (n *Notifier) OnClaimFailed(
	ctx context.Context,
	job *model.Job,
	claimError error,
	maxAttempt int,
) error {
	query := fmt.Sprintf(`
		UPDATE
		  `+notifierTableName+`
		SET
		  status =(
			CASE WHEN attempt > $1 THEN CAST (
			  '%[1]s' AS pg_notifier_status_type
			) ELSE CAST(
			  '%[2]s' AS pg_notifier_status_type
			) END
		  ),
		  attempt = attempt + 1,
		  updated_at = $2,
		  error = $3
		WHERE
		  id = $4;
	`,
		model.Aborted,
		model.Failed,
	)

	_, err := n.db.ExecContext(ctx,
		query,
		maxAttempt,
		n.now().UTC().Format(timeFormat),
		misc.QuoteLiteral(claimError.Error()),
		job.ID,
	)
	if err != nil {
		return fmt.Errorf("on claim failed: %w", err)
	}

	return nil
}

// OnClaimSuccess updates the status of a job to succeed.
func (n *Notifier) OnClaimSuccess(
	ctx context.Context,
	job *model.Job,
	payload json.RawMessage,
) error {
	_, err := n.db.ExecContext(ctx, `
		UPDATE
		  `+notifierTableName+`
		SET
		  status = $1,
		  updated_at = $2,
		  payload = $3
		WHERE
		  id = $4;
	`,
		model.Succeeded,
		n.now().UTC().Format(timeFormat),
		string(payload),
		job.ID,
	)
	if err != nil {
		return fmt.Errorf("on claim success: %w", err)
	}

	return nil
}
