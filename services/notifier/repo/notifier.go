package repo

import (
	"context"
	"database/sql"
	"fmt"
	"time"

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

// ResetForWorkspace deletes all the jobs for a specified workspace.
func (n *Notifier) ResetForWorkspace(
	ctx context.Context,
	workspaceIdentifier string,
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

// OrphanJobIDs returns the IDs of the jobs that are in executing state for more than the given interval.
func (n *Notifier) OrphanJobIDs(
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
