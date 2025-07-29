package repo

import (
	"time"

	"github.com/rudderlabs/rudder-go-kit/stats"
)

type Opt func(*repo)

func WithNow(now func() time.Time) Opt {
	return func(r *repo) {
		r.now = now
	}
}

func WithStats(s stats.Stats) Opt {
	return func(r *repo) {
		r.statsFactory = s
	}
}
