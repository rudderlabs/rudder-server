package notifier

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/lib/pq"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/config"

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
)

type Opt func(*repo)

type scanFn func(dest ...any) error

func WithNow(now func() time.Time) Opt {
	return func(r *repo) {
		r.now = now
	}
}

type repo struct {
	db  *sqlmw.DB
	now func() time.Time
}

func newRepo(db *sqlmw.DB, opts ...Opt) *repo {
	r := &repo{
		db:  db,
		now: timeutil.Now,
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// ResetForWorkspace deletes all the jobs for a specified workspace.
func (n *repo) resetForWorkspace(
	ctx context.Context,
	workspaceIdentifier string,
) error {
	_, err := n.db.ExecContext(ctx, `
		DELETE FROM `+notifierTableName+`
		WHERE workspace = $1;
	`,
		workspaceIdentifier,
	)
	if err != nil {
		return fmt.Errorf("reset: delete for workspace %s: %w", workspaceIdentifier, err)
	}
	return nil
}

// Insert inserts a jobs into the notifier queue.
func (n *repo) insert(
	ctx context.Context,
	publishRequest *PublishRequest,
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
			Waiting,
			string(payload),
			workspaceIdentifier,
			publishRequest.Priority,
			publishRequest.JobType,
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

	if publishRequest.UploadSchema != nil {
		_, err = txn.ExecContext(ctx, `
			UPDATE
  			  `+notifierTableName+`
			SET
			  payload = payload || $1
			WHERE
			  batch_id = $2;
	`,
			publishRequest.UploadSchema,
			batchID,
		)
		if err != nil {
			return fmt.Errorf(`updating: metadata: %w`, err)
		}
	}

	if err = txn.Commit(); err != nil {
		return fmt.Errorf(`inserting: commit: %w`, err)
	}
	return nil
}

// PendingByBatchID returns the number of pending jobs for a batchID.
func (n *repo) pendingByBatchID(
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
		Succeeded,
		Aborted,
	).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("pending by batchID: %w", err)
	}

	return count, err
}

// GetByBatchID returns all the jobs for a batchID.
// TODO: ATM Hack to remove `UploadSchema` from the payload to have the similar implementation as the old notifier.
func (n *repo) getByBatchID(
	ctx context.Context,
	batchID string,
) ([]Job, error) {
	query := `
		SELECT
			id,
			batch_id,
			worker_id,
			workspace,
			attempt,
			status,
			job_type,
			priority,
			error,
			payload - 'UploadSchema',
			created_at,
			updated_at,
			last_exec_time
		FROM
			` + notifierTableName + `
		WHERE
			batch_id = $1
		ORDER BY
			id;`

	rows, err := n.db.QueryContext(ctx, query, batchID)
	if err != nil {
		return nil, fmt.Errorf("getting by batchID: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var jobs []Job
	for rows.Next() {
		var job Job
		err := scanJob(rows.Scan, &job)
		if err != nil {
			return nil, fmt.Errorf("getting by batchID: scan: %w", err)
		}

		jobs = append(jobs, job)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("getting by batchID: rows err: %w", err)
	}
	if len(jobs) == 0 {
		return nil, fmt.Errorf("getting by batchID: no jobs found")
	}

	return jobs, err
}

func scanJob(scan scanFn, job *Job) error {
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
		case string(JobTypeUpload), string(JobTypeAsync):
			job.Type = JobType(jobTypeRaw.String)
		default:
			return fmt.Errorf("scanning: unknown job type: %s", jobTypeRaw.String)
		}
	} else {
		job.Type = JobTypeUpload
	}
	if errorRaw.Valid {
		job.Error = errors.New(errorRaw.String)
	}
	if lasExecTime.Valid {
		job.LastExecTime = lasExecTime.Time
	}

	return nil
}

// DeleteByBatchID deletes all the jobs for a batchID.
func (n *repo) deleteByBatchID(
	ctx context.Context,
	batchID string,
) error {
	_, err := n.db.ExecContext(ctx, `
		DELETE FROM `+notifierTableName+` WHERE batch_id = $1;
	`,
		batchID,
	)
	if err != nil {
		return fmt.Errorf("deleting by batchID: %w", err)
	}

	var sizeEstimate int64
	if err := n.db.QueryRowContext(
		ctx,
		fmt.Sprintf(`SELECT pg_table_size(oid) from pg_class where relname='%s';`, notifierTableName),
	).Scan(&sizeEstimate); err != nil {
		return fmt.Errorf("size estimate for notifierTable failed with: %w", err)
	}
	if sizeEstimate > config.GetInt64("Notifier.vacuumThresholdBytes", 5*bytesize.GB) {
		if _, err := n.db.ExecContext(ctx, fmt.Sprintf(`vacuum full analyze %q;`, notifierTableName)); err != nil {
			return fmt.Errorf("deleting by batchID: vacuum: %w", err)
		}
	}
	return nil
}

func (n *repo) claim(
	ctx context.Context,
	workerID string,
) (*Job, error) {
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
		Executing,
		n.now(),
		workerID,
		Waiting,
		Failed,
	)

	var job Job
	err := scanJob(row.Scan, &job)
	if err != nil {
		return nil, fmt.Errorf("claim for workerID %s: scan: %w", workerID, err)
	}
	return &job, nil
}

// OnClaimFailed updates the status of a job to failed.
func (n *repo) onClaimFailed(
	ctx context.Context,
	job *Job,
	claimError error,
	maxAttempt int,
) error {
	query := fmt.Sprint(`
		UPDATE
		  ` + notifierTableName + `
		SET
		  status =(
			CASE WHEN attempt > $1 THEN CAST (
			  '` + Aborted + `' AS pg_notifier_status_type
			) ELSE CAST(
			   '` + Failed + `'  AS pg_notifier_status_type
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
func (n *repo) onClaimSuccess(
	ctx context.Context,
	job *Job,
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
		Succeeded,
		n.now(),
		string(payload),
		job.ID,
	)
	if err != nil {
		return fmt.Errorf("on claim success: %w", err)
	}

	return nil
}

// OrphanJobIDs returns the IDs of the jobs that are in executing state for more than the given interval.
func (n *repo) orphanJobIDs(
	ctx context.Context,
	intervalInSeconds int,
) ([]int64, error) {
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
		Waiting,
		n.now(),
		Executing,
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
