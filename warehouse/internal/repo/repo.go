package repo

import (
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
)

var repoTimeout = config.GetDuration("Warehouse.repoTimeout", 5, time.Minute)

type repo struct {
	db  *sqlquerywrapper.DB
	now func() time.Time
}

type Opt func(*repo)

func WithNow(now func() time.Time) Opt {
	return func(r *repo) {
		r.now = now
	}
}

// type repoI interface {
// 	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
// 	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
// 	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
// 	BeginTx(ctx context.Context, opts *sql.TxOptions) (txI, error)
// }

// type txI interface {
// 	Commit() error
// 	Rollback() error
// 	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
// 	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
// 	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
// }
