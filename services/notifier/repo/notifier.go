package repo

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/lib/pq"

	"github.com/rudderlabs/rudder-server/utils/misc"

	"github.com/rudderlabs/rudder-server/services/notifier/model"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	sqlmw "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
)

const (
	notifierTableName    = "pg_notifier_queue"
	notifierTableColumns = `
		id,
		batch_id,
		worker_id,
		workspace,
		attempt,
		status,
		job_type,
		priority,
		error,
		payload,
		created_at,
		updated_at,
		last_exec_time
`

	notifierMetadataTableName = "pg_notifier_queue_metadata"

	timeFormat = "2006-01-02 15:04:05"
	version    = "warehouse/v1"
)

type Opt func(*Notifier)

type scanFn func(dest ...any) error

func WithNow(now func() time.Time) Opt {
	return func(r *Notifier) {
		r.now = now
	}
}

type Notifier struct {
	db  *sqlmw.DB
	now func() time.Time
}

func NewNotifier(db *sqlmw.DB, opts ...Opt) *Notifier {
	r := &Notifier{
		db:  db,
		now: timeutil.Now,
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

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
			version,
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

// ResetForWorkspace deletes all the jobs for a specified workspace.
func (n *Notifier) ResetForWorkspace(ctx context.Context, workspaceIdentifier string) error {
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

	_, err = txn.ExecContext(ctx, `
		DELETE FROM `+notifierMetadataTableName+`
		WHERE batch_id IN (
			SELECT DISTINCT batch_id FROM `+notifierTableName+`
			WHERE workspace = $1
		);
	`,
		workspaceIdentifier,
	)
	if err != nil {
		return fmt.Errorf("reset: delete metadata for workspace %s: %w", workspaceIdentifier, err)
	}

	_, err = txn.ExecContext(ctx, `
		DELETE FROM `+notifierTableName+`
		WHERE workspace = $1;
	`,
		workspaceIdentifier,
	)
	if err != nil {
		return fmt.Errorf("reset: delete for workspace %s: %w", workspaceIdentifier, err)
	}

	if err = txn.Commit(); err != nil {
		return fmt.Errorf("reset: commit: %w", err)
	}

	return nil
}

// GetByBatchID returns all the jobs for a batchID.
func (n *Notifier) GetByBatchID(ctx context.Context, batchID string) ([]model.Job, model.JobMetadata, error) {
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

// DeleteByBatchID deletes all the jobs for a batchID.
func (n *Notifier) DeleteByBatchID(ctx context.Context, batchID string) error {
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

// PendingByBatchID returns the number of pending jobs for a batchID.
func (n *Notifier) PendingByBatchID(ctx context.Context, batchID string) (int64, error) {
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
	if err == sql.ErrNoRows {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("pending by batchID: %w", err)
	}

	return count, err
}

// OrphanJobIDs returns the IDs of the jobs that are in executing state for more than the given interval.
func (n *Notifier) OrphanJobIDs(ctx context.Context, intervalInSeconds int) ([]int64, error) {
	rows, err := n.db.QueryContext(ctx, `
		UPDATE
          `+notifierTableName+`
		SET
		  status = $1,
		  updated_at = $2
		WHERE
		  id IN (
			SELECT
			  id
			FROM
              `+notifierTableName+`
			WHERE
			  status = $3
			AND last_exec_time <= NOW() - $4 * INTERVAL '1 SECOND'
		    FOR
			UPDATE
		  	SKIP LOCKED
	  	) RETURNING id;
`,
		model.Waiting,
		n.now().Format(timeFormat),
		model.Executing,
		intervalInSeconds,
	)
	if err != nil {
		return nil, fmt.Errorf("orphan jobs ids: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("orphan jobs ids: scanning: %w", err)
		}

		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("orphan jobs ids: iterating over rows: %w", err)
	}

	return ids, nil
}

// Claim claims a job for a worker.
func (n *Notifier) Claim(ctx context.Context, workerID string) (*model.Job, model.JobMetadata, error) {
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

func (n *Notifier) metadataForBatchID(ctx context.Context, batchID string) (model.JobMetadata, error) {
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

// OnClaimFailed updates the status of a job to failed.
func (n *Notifier) OnClaimFailed(ctx context.Context, job *model.Job, claimError error, maxAttempt int) error {
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
func (n *Notifier) OnClaimSuccess(ctx context.Context, job *model.Job, payload json.RawMessage) error {
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
