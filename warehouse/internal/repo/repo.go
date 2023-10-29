package repo

import (
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

func (r *repo) WithTx(f func(tx *sqlmiddleware.Tx) error) error {
	txn, err := r.db.Begin()
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}

	if err := f(txn); err != nil {
		if rollbackErr := txn.Rollback(); rollbackErr != nil {
			return fmt.Errorf("%w; %s", err, rollbackErr)
		}
		return err
	}
	return txn.Commit()
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
