//go:generate mockgen -destination=./db_mock.go -package=db -source=./db.go DB
package db

import (
	"context"
	"time"
)

type DB interface {
	InitDB() error
	GetStart(ctx context.Context, table string) (time.Time, error)
	FetchBatch(ctx context.Context, table string, start, end time.Time, limit, offset int) ([]map[string]interface{}, error)
	Delete(ctx context.Context, table string, start, end time.Time) error
	CloseDB() error
}
