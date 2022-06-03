package extension

import (
	"context"
	"database/sql"
)

type Extension interface {
	setupStatsTable(ctx context.Context) error
	GetReadDB() *sql.DB
	DropStats(ctx context.Context, jobRunId string) error
	CleanupLoop(ctx context.Context) error
}
