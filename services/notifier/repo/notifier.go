package repo

import (
	"context"
	"database/sql"
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
	tableName    = "pg_notifier_queue"
	tableColumns = `
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

	timeFormat = "2006-01-02 15:04:05"
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

// Insert inserts a job into the notifier table.
//
// NOTE: The following fields are ignored and set by the database:
// - CreatedAt
// - UpdatedAt
func (n *Notifier) Insert(
	ctx context.Context,
	publishPayload *model.PublishRequest,
	workspaceIdentifier string,
	batchID string,
) error {
	txn, err := n.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return fmt.Errorf("inserting into notifier: begin transaction: %w", err)
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
			tableName,
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
		return fmt.Errorf(`inserting into notifier: CopyIn: %w`, err)
	}
	defer func() { _ = stmt.Close() }()

	priority, jobType, schema := publishPayload.Priority, publishPayload.Type, publishPayload.Schema

	for _, job := range publishPayload.Jobs {
		_, err = stmt.ExecContext(
			ctx,
			batchID,
			model.Waiting,
			string(job),
			workspaceIdentifier,
			priority,
			jobType,
			now.UTC(),
			now.UTC(),
		)
		if err != nil {
			return fmt.Errorf(`inserting into notifier: CopyIn exec: %w`, err)
		}
	}
	if _, err = stmt.ExecContext(ctx); err != nil {
		return fmt.Errorf(`inserting into notifier: CopyIn final exec: %w`, err)
	}

	// Currently, we are doing this separately, since we don't want to keep huge schema in memory
	_, err = txn.ExecContext(ctx, `
			UPDATE
			  `+tableName+`
			SET
			  payload = payload || $1
			WHERE
			  batch_id = $2;
	`,
		string(schema),
		batchID,
	)
	if err != nil {
		return fmt.Errorf(`inserting into notifier: update schema: %w`, err)
	}

	if err := txn.Commit(); err != nil {
		return fmt.Errorf(`inserting into notifier: commit: %w`, err)
	}
	return nil
}

func (n *Notifier) ResetForWorkspace(ctx context.Context, workspaceIdentifier string) error {
	_, err := n.db.ExecContext(ctx, `
		DELETE FROM
  			`+tableName+`
		WHERE
  			workspace = $1;
	`,
		workspaceIdentifier,
	)
	if err != nil {
		return fmt.Errorf("reset for workspace %s: %w", workspaceIdentifier, err)
	}
	return nil
}

func (n *Notifier) GetByBatchID(ctx context.Context, batchID string) ([]model.Notifier, error) {
	query := `SELECT ` + tableColumns + ` FROM ` + tableName + ` WHERE batch_id = $1 ORDER BY id;`

	rows, err := n.db.QueryContext(ctx, query, batchID)
	if err != nil {
		return nil, fmt.Errorf("getting by batchID: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var notifiers []model.Notifier
	for rows.Next() {
		var notifier model.Notifier
		err := scanNotifier(rows.Scan, &notifier)
		if err != nil {
			return nil, fmt.Errorf("getting by batchID: scanning notifier: %w", err)
		}

		notifiers = append(notifiers, notifier)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("getting by batchID: iterating over rows: %w", err)
	}
	if len(notifiers) == 0 {
		return nil, fmt.Errorf("getting by batchID: no notifiers found")
	}

	return notifiers, err
}

func (n *Notifier) DeleteByBatchID(ctx context.Context, batchID string) (int64, error) {
	r, err := n.db.ExecContext(ctx, `
		DELETE FROM
		  `+tableName+`
		WHERE
	  	  batch_id = $1;
`,
		batchID,
	)
	if err != nil {
		return 0, fmt.Errorf("deleting by batchID: %w", err)
	}

	rowsAffected, err := r.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("deleting by batchID: rows affected: %w", err)
	} else if rowsAffected == 0 {
		return 0, fmt.Errorf("deleting by batchID: no rows affected")
	}

	return rowsAffected, nil
}

func (n *Notifier) PendingByBatchID(ctx context.Context, batchID string) (int64, error) {
	var count int64

	err := n.db.QueryRowContext(ctx, `
		SELECT
		  COUNT(*)
		FROM
  		  `+tableName+`
		WHERE
		  batch_id = $1
          AND status != $2
		  AND status != $3
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

func (n *Notifier) OrphanJobIDs(ctx context.Context, intervalInSeconds int) ([]int64, error) {
	rows, err := n.db.QueryContext(ctx, `
		UPDATE
          `+tableName+`
		SET
		  status = $1,
		  updated_at = $2
		WHERE
		  id IN (
			SELECT
			  id
			FROM
              `+tableName+`
			WHERE
			  status = $3
			AND last_exec_time <= NOW() - $4 * INTERVAL '1 SECOND' FOR
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

func (n *Notifier) Claim(ctx context.Context, workerID string) (*model.Notifier, error) {
	row := n.db.QueryRowContext(ctx, `
		UPDATE
  		  `+tableName+`
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
      		  `+tableName+`
			WHERE
			  status = $4
			  OR status = $5
			ORDER BY
			  priority ASC,
			  id ASC FOR
			UPDATE
			  SKIP LOCKED
			LIMIT
			  1
		  ) RETURNING `+tableColumns+`;
`,
		model.Executing,
		n.now().Format(timeFormat),
		workerID,
		model.Waiting,
		model.Failed,
	)

	var notifier model.Notifier
	if err := scanNotifier(row.Scan, &notifier); err != nil {
		return nil, fmt.Errorf("claiming job: %w", err)
	}

	return &notifier, nil
}

func (n *Notifier) OnFailed(ctx context.Context, notifier *model.Notifier, maxAttempt int, claimError error) error {
	query := fmt.Sprintf(`
		UPDATE
		  `+tableName+`
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

	r, err := n.db.ExecContext(ctx,
		query,
		maxAttempt,
		n.now().UTC().Format(timeFormat),
		misc.QuoteLiteral(claimError.Error()),
		notifier.ID,
	)
	if err != nil {
		return fmt.Errorf("on claim failed: %w", err)
	}

	rowsAffected, err := r.RowsAffected()
	if err != nil {
		return fmt.Errorf("on claim failed: rows affected: %w", err)
	}
	if rowsAffected == 0 {
		return errors.New("on claim failed: no rows affected")
	}

	return nil
}

func (n *Notifier) OnSuccess(ctx context.Context, notifier *model.Notifier) error {
	r, err := n.db.ExecContext(ctx, `
		UPDATE
		  `+tableName+`
		SET
		  status = $1,
		  updated_at = $2,
		  payload = $3
		WHERE
		  id = $4;
	`,
		model.Succeeded,
		n.now().UTC().Format(timeFormat),
		notifier.Payload,
		notifier.ID,
	)
	if err != nil {
		return fmt.Errorf("on claim success: %w", err)
	}

	rowsAffected, err := r.RowsAffected()
	if err != nil {
		return fmt.Errorf("on claim success: rows affected: %w", err)
	}
	if rowsAffected == 0 {
		return errors.New("on claim success: no rows affected")
	}

	return nil
}

func scanNotifier(scan scanFn, notifier *model.Notifier) error {
	var (
		jobTypeRaw  sql.NullString
		errorRaw    sql.NullString
		workerIDRaw sql.NullString
		lasExecTime sql.NullTime
	)

	err := scan(
		&notifier.ID,
		&notifier.BatchID,
		&workerIDRaw,
		&notifier.WorkspaceIdentifier,
		&notifier.Attempt,
		&notifier.Status,
		&jobTypeRaw,
		&notifier.Priority,
		&errorRaw,
		&notifier.Payload,
		&notifier.CreatedAt,
		&notifier.UpdatedAt,
		&lasExecTime,
	)
	if err != nil {
		return fmt.Errorf("scanning: %w", err)
	}

	if workerIDRaw.Valid {
		notifier.WorkerID = workerIDRaw.String
	}
	if jobTypeRaw.Valid {
		notifier.JobType = jobTypeRaw.String
	} else {
		notifier.JobType = "upload"
	}
	if errorRaw.Valid {
		notifier.Error = errors.New(errorRaw.String)
	}
	if lasExecTime.Valid {
		notifier.LastExecTime = lasExecTime.Time
	}

	return nil
}
