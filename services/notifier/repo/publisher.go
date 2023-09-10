package repo

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/lib/pq"

	"github.com/rudderlabs/rudder-server/services/notifier/model"
)

// Insert inserts a jobs into the notifier queue.
func (n *Notifier) Insert(
	ctx context.Context,
	publishRequest *model.PublishRequest,
	workspaceIdentifier string,
	batchID string,
) error {
	txn, err := n.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return fmt.Errorf("inserting: begin transaction: %w", err)
	}
	defer func() {
		if err != nil {
			_ = txn.Rollback()
			return
		}
	}()

	now := n.now()

	stmt, err := txn.PrepareContext(
		ctx,
		pq.CopyIn(
			notifierTableName,
			"batch_id",
			"status",
			"payload",
			"workspace",
			"priority",
			"job_type",
			"topic",
			"created_at",
			"updated_at",
		),
	)
	if err != nil {
		return fmt.Errorf(`inserting: CopyIn: %w`, err)
	}
	defer func() { _ = stmt.Close() }()

	for _, payload := range publishRequest.Payloads {
		_, err = stmt.ExecContext(
			ctx,
			batchID,
			model.Waiting,
			string(payload),
			workspaceIdentifier,
			publishRequest.Priority,
			publishRequest.JobType,
			topic,
			now.UTC(),
			now.UTC(),
		)
		if err != nil {
			return fmt.Errorf(`inserting: CopyIn exec: %w`, err)
		}
	}
	if _, err = stmt.ExecContext(ctx); err != nil {
		return fmt.Errorf(`inserting: CopyIn final exec: %w`, err)
	}

	if publishRequest.PayloadMetadata != nil {
		_, err = txn.ExecContext(ctx, `
			INSERT INTO `+notifierMetadataTableName+` (batch_id, metadata)
			VALUES ($1, $2);
	`,
			batchID,
			string(publishRequest.PayloadMetadata),
		)
		if err != nil {
			return fmt.Errorf(`inserting: metadata: %w`, err)
		}
	}

	if err = txn.Commit(); err != nil {
		return fmt.Errorf(`inserting: commit: %w`, err)
	}
	return nil
}

// PendingByBatchID returns the number of pending jobs for a batchID.
func (n *Notifier) PendingByBatchID(
	ctx context.Context,
	batchID string,
) (int64, error) {
	var count int64

	err := n.db.QueryRowContext(ctx, `
		SELECT
		  COUNT(*)
		FROM
  		  `+notifierTableName+`
		WHERE
		  batch_id = $1 AND
          status != $2  AND
		  status != $3
`,
		batchID,
		model.Succeeded,
		model.Aborted,
	).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("pending by batchID: %w", err)
	}

	return count, err
}

// GetByBatchID returns all the jobs for a batchID.
func (n *Notifier) GetByBatchID(
	ctx context.Context,
	batchID string,
) ([]model.Job, model.JobMetadata, error) {
	query := `SELECT ` + notifierTableColumns + ` FROM ` + notifierTableName + ` WHERE batch_id = $1 ORDER BY id;`

	rows, err := n.db.QueryContext(ctx, query, batchID)
	if err != nil {
		return nil, nil, fmt.Errorf("getting by batchID: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var jobs []model.Job
	for rows.Next() {
		var job model.Job
		err := scanJob(rows.Scan, &job)
		if err != nil {
			return nil, nil, fmt.Errorf("getting by batchID: scan: %w", err)
		}

		jobs = append(jobs, job)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("getting by batchID: rows err: %w", err)
	}
	if len(jobs) == 0 {
		return nil, nil, fmt.Errorf("getting by batchID: no jobs found")
	}

	metadata, err := n.metadataForBatchID(ctx, batchID)
	if err != nil {
		return nil, nil, fmt.Errorf("getting by batchID: metadata: %w", err)
	}

	return jobs, metadata, err
}

func scanJob(scan scanFn, job *model.Job) error {
	var (
		jobTypeRaw  sql.NullString
		errorRaw    sql.NullString
		workerIDRaw sql.NullString
		lasExecTime sql.NullTime
	)

	err := scan(
		&job.ID,
		&job.BatchID,
		&workerIDRaw,
		&job.WorkspaceIdentifier,
		&job.Attempt,
		&job.Status,
		&jobTypeRaw,
		&job.Priority,
		&errorRaw,
		&job.Payload,
		&job.CreatedAt,
		&job.UpdatedAt,
		&lasExecTime,
	)
	if err != nil {
		return fmt.Errorf("scanning: %w", err)
	}

	if workerIDRaw.Valid {
		job.WorkerID = workerIDRaw.String
	}
	if jobTypeRaw.Valid {
		switch jobTypeRaw.String {
		case string(model.JobTypeUpload), string(model.JobTypeAsync):
			job.Type = model.JobType(jobTypeRaw.String)
		default:
			return fmt.Errorf("scanning: unknown job type: %s", jobTypeRaw.String)
		}
	} else {
		job.Type = model.JobTypeUpload
	}
	if errorRaw.Valid {
		job.Error = errors.New(errorRaw.String)
	}
	if lasExecTime.Valid {
		job.LastExecTime = lasExecTime.Time
	}

	return nil
}

func (n *Notifier) metadataForBatchID(
	ctx context.Context,
	batchID string,
) (model.JobMetadata, error) {
	var metadata model.JobMetadata

	err := n.db.QueryRowContext(ctx, `
		SELECT
		  metadata
		FROM
		  `+notifierMetadataTableName+`
		WHERE
		  batch_id = $1;
`,
		batchID,
	).Scan(&metadata)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("metadata by batchID: %w", err)
	}
	return metadata, nil
}

// DeleteByBatchID deletes all the jobs for a batchID.
func (n *Notifier) DeleteByBatchID(
	ctx context.Context,
	batchID string,
) error {
	txn, err := n.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return fmt.Errorf("reset: begin transaction: %w", err)
	}
	defer func() {
		if err != nil {
			_ = txn.Rollback()
			return
		}
	}()

	_, err = txn.ExecContext(ctx, `DELETE FROM `+notifierTableName+` WHERE batch_id = $1;`, batchID)
	if err != nil {
		return fmt.Errorf("deleting by batchID: %w", err)
	}

	_, err = txn.ExecContext(ctx, `DELETE FROM `+notifierMetadataTableName+` WHERE batch_id = $1`, batchID)
	if err != nil {
		return fmt.Errorf("deleting metadata by batchID: %w", err)
	}

	if err = txn.Commit(); err != nil {
		return fmt.Errorf("deleting by batchID: commit: %w", err)
	}

	return nil
}
