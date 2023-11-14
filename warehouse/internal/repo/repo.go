package repo

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	sqlmiddleware "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
)

type repo struct {
	db  *sqlmiddleware.DB
	now func() time.Time
}

type Opt func(*repo)

func WithNow(now func() time.Time) Opt {
	return func(r *repo) {
		r.now = now
	}
}

func (r *repo) WithTx(ctx context.Context, f func(tx *sqlmiddleware.Tx) error) error {
	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}

	if err := f(tx); err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			return fmt.Errorf("rollback transaction for %w: %w", err, rollbackErr)
		}
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}
	return nil
}

type UpdateField func(v interface{}) UpdateKeyValue

type UpdateKeyValue interface {
	key() string
	value() interface{}
}

type keyValue struct {
	k string
	v interface{}
}

func (u keyValue) key() string        { return u.k }
func (u keyValue) value() interface{} { return u.v }
