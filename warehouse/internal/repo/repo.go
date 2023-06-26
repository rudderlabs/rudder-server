package repo

import (
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
