package repo

import (
	"database/sql"
	"time"
)

type repo struct {
	db  *sql.DB
	now func() time.Time
}

type Opt func(*repo)

func WithNow(now func() time.Time) Opt {
	return func(r *repo) {
		r.now = now
	}
}
