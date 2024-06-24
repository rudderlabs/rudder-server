package db

import (
	"context"
	"time"
)

type Database interface {
	GetStart(ctx context.Context, table string) (time.Time, error)
	FetchBatch(ctx context.Context, table string, start, end time.Time, limit, offset int) ([]map[string]interface{}, error)
	Delete(ctx context.Context, table string, start, end time.Time) error
}
