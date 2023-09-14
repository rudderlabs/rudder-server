package repo

import (
	"context"
	"fmt"
	"time"

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
