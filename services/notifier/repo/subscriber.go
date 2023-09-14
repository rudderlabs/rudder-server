package repo

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/rudderlabs/rudder-server/services/notifier/model"
)

// Claim claims a job for a worker.
func (n *Notifier) Claim(
	ctx context.Context,
	workerID string,
) (*model.Job, error) {
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
			  (status = $4 OR status = $5)
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
		n.now(),
		workerID,
		model.Waiting,
		model.Failed,
	)

	var notifier model.Job
	err := scanJob(row.Scan, &notifier)
	if err != nil {
		return nil, fmt.Errorf("claim for workerID %s: scan: %w", workerID, err)
	}
	return &notifier, nil
}

// OnClaimFailed updates the status of a job to failed.
func (n *Notifier) OnClaimFailed(
	ctx context.Context,
	job *model.Job,
	claimError error,
	maxAttempt int,
) error {
	query := fmt.Sprint(`
		UPDATE
		  ` + notifierTableName + `
		SET
		  status =(
			CASE WHEN attempt > $1 THEN CAST (
			  '` + model.Aborted + `' AS pg_notifier_status_type
			) ELSE CAST(
			   '` + model.Failed + `'  AS pg_notifier_status_type
			) END
		  ),
		  attempt = attempt + 1,
		  updated_at = $2,
		  error = $3
		WHERE
		  id = $4;
	`,
	)

	_, err := n.db.ExecContext(ctx,
		query,
		maxAttempt,
		n.now(),
		claimError.Error(),
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
		n.now(),
		string(payload),
		job.ID,
	)
	if err != nil {
		return fmt.Errorf("on claim success: %w", err)
	}

	return nil
}
