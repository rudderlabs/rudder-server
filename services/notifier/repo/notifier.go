package repo

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/lib/pq"

	"github.com/rudderlabs/rudder-server/services/notifier/model"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	sqlmw "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
)

const (
	notifierTableName = "pg_notifier_queue"

	timeFormat = "2006-01-02 15:04:05"
)

type Opt func(*Notifier)

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

func (n *Notifier) ResetForWorkspace(ctx context.Context, workspaceID string) error {
	_, err := n.db.ExecContext(ctx, `
		DELETE FROM
  			`+notifierTableName+`
		WHERE
  			workspace = $1;
	`,
		workspaceID,
	)
	if err != nil {
		return fmt.Errorf("reset for workspaceID %s: %w", workspaceID, err)
	}
	return nil
}

// Insert inserts a job into the notifier table.
//
// NOTE: The following fields are ignored and set by the database:
// - CreatedAt
// - UpdatedAt
func (n *Notifier) Insert(
	ctx context.Context,
	publishPayload *model.PublishPayload,
	workspaceIdentifier string,
	batchID string,
) error {
	txn, err := n.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
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

	// TODO: We should probably have a separate table for storing metadata like schema
	// Currently we are doing this separately, since we don't want to keep huge schema in memory
	_, err = txn.ExecContext(ctx, `
		UPDATE
		  `+notifierTableName+`
		SET
		  payload = payload || $1
		WHERE
		  batch_id = $2;
`,
		batchID,
		schema,
	)
	if err != nil {
		return fmt.Errorf(`inserting into notifier: update schema: %w`, err)
	}

	if err := txn.Commit(); err != nil {
		return fmt.Errorf(`inserting into notifier: commit: %w`, err)
	}
	return nil
}

func (n *Notifier) OrphanJobIDs(ctx context.Context) ([]int64, error) {
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
		  AND last_exec_time <= NOW() - INTERVAL '%[5]v seconds' FOR
		UPDATE
		  SKIP LOCKED
	  ) RETURNING id;
`,
		model.Waiting,
		n.now().Format(timeFormat),
		model.Executing,
	)
	if err != nil {
		return nil, fmt.Errorf("getting orphan jobs: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("getting orphan jobs: scan: %w", err)
		}

		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("getting orphan jobs: %w", err)
	}

	return ids, nil
}

func (n *Notifier) Claim(ctx context.Context, workerID string) (*model.Notifier, error) {
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
			  status = $4
			  OR status = $5
			ORDER BY
			  priority ASC,
			  id ASC FOR
			UPDATE
			  SKIP LOCKED
			LIMIT
			  1
		  ) RETURNING id,
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
		  last_exec_time;
`,
		model.Executing,
		n.now().Format(timeFormat),
		workerID,
		model.Waiting,
		model.Failed,
	)
	var notifier model.Notifier

	if err := n.scan(row, &notifier); err != nil {
		return nil, fmt.Errorf("claiming job scan: %w", err)
	}

	return &notifier, nil
}

func (n *Notifier) scan(row *sqlmw.Row, notifier *model.Notifier) error {
	var (
		jobTypeRaw  sql.NullString
		errorRaw    sql.NullString
		workerIDRaw sql.NullString
		lasExecTime sql.NullTime
	)

	err := row.Scan(
		&notifier.ID,
		&notifier.BatchID,
		&workerIDRaw,
		&notifier.WorkspaceID,
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
