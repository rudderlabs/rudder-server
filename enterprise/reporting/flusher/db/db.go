//go:generate mockgen -destination=./db_mock.go -package=db -source=./db.go DB
package db

import (
	"context"
	"time"
)

type DB interface {
	GetStart(ctx context.Context, table string) (time.Time, error)
	Delete(ctx context.Context, table string, start, end time.Time) error
	Close() error
}
