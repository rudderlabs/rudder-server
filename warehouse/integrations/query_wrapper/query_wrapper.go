package query_wrapper

import (
	"context"
	"database/sql"
	"time"

	"github.com/rudderlabs/rudder-server/utils/misc"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/warehouse/logfield"
)

const ExecutingQuery = "executing query"

type Opt func(*DB)

type DB struct {
	*sql.DB

	since               func(time.Time) time.Duration
	logger              logger.Logger
	keysAndValues       []any
	queryThresholdInSec time.Duration
	secretsRegex        map[string]string
}

func WithLogger(logger logger.Logger) Opt {
	return func(s *DB) {
		s.logger = logger
	}
}

func WithKeyAndValues(keyAndValues ...any) Opt {
	return func(s *DB) {
		s.keysAndValues = keyAndValues
	}
}

func WithSince(since func(time.Time) time.Duration) Opt {
	return func(s *DB) {
		s.since = since
	}
}

func WithQueryThresholdInSec(queryThresholdInSec time.Duration) Opt {
	return func(s *DB) {
		s.queryThresholdInSec = queryThresholdInSec
	}
}

func WithSecretsRegex(secretsRegex map[string]string) Opt {
	return func(s *DB) {
		s.secretsRegex = secretsRegex
	}
}

func NewSQLQueryWrapper(db *sql.DB, opts ...Opt) *DB {
	queryThreshold := config.GetDuration("Warehouse.queryThresholdInSec", 300, time.Second)

	s := &DB{
		DB:                  db,
		since:               time.Since,
		queryThresholdInSec: queryThreshold,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

func (db *DB) Exec(query string, args ...interface{}) (sql.Result, error) {
	startedAt := time.Now()
	defer func() {
		db.logQuery(startedAt, query)
	}()

	return db.DB.Exec(query, args...)
}

func (db *DB) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	startedAt := time.Now()
	defer func() {
		db.logQuery(startedAt, query)
	}()

	return db.DB.ExecContext(ctx, query, args...)
}

func (db *DB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	startedAt := time.Now()
	defer func() {
		db.logQuery(startedAt, query)
	}()

	return db.DB.Query(query, args...)
}

func (db *DB) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	startedAt := time.Now()
	defer func() {
		db.logQuery(startedAt, query)
	}()

	return db.DB.QueryContext(ctx, query, args...)
}

func (db *DB) QueryRow(query string, args ...interface{}) *sql.Row {
	startedAt := time.Now()
	defer func() {
		db.logQuery(startedAt, query)
	}()

	return db.DB.QueryRow(query, args...)
}

func (db *DB) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	startedAt := time.Now()
	defer func() {
		db.logQuery(startedAt, query)
	}()

	return db.DB.QueryRowContext(ctx, query, args...)
}

func (db *DB) logQuery(startedAt time.Time, query string) {
	if executionTime := db.since(startedAt); executionTime > db.queryThresholdInSec {
		sanitizedQuery, _ := misc.ReplaceMultiRegex(query, db.secretsRegex)

		keysAndValues := []any{
			logfield.Query, sanitizedQuery,
			logfield.QueryExecutionTimeInSec, int64(executionTime.Seconds()),
		}
		keysAndValues = append(keysAndValues, db.keysAndValues...)

		db.logger.Infow(ExecutingQuery, keysAndValues...)
	}
}
