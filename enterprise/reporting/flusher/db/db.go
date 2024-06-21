package db

import (
	"context"
	"time"
)

type Database interface {
	GetMinReportedAt(ctx context.Context, table string) (time.Time, error)
	FetchReports(ctx context.Context, table string, min, max time.Time, limit, offset int) ([]map[string]interface{}, error)
	DeleteReports(ctx context.Context, table string, min, max time.Time) error
}
