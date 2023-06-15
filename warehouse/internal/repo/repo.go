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
