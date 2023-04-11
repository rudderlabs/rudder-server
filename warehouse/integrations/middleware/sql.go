package middleware

import (
	"context"
	"database/sql"
	"time"

	"github.com/rudderlabs/rudder-server/utils/misc"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/warehouse/logfield"
)

const ExecutingQuery = "executing query"

type Opt func(*DB)

type DB struct {
	*sql.DB

	since              func(time.Time) time.Duration
	logger             logger.Logger
	keysAndValues      []any
	slowQueryThreshold time.Duration
	secretsRegex       map[string]string
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

func WIthSlowQueryThreshold(slowQueryThreshold time.Duration) Opt {
	return func(s *DB) {
		s.slowQueryThreshold = slowQueryThreshold
	}
}

func WithSecretsRegex(secretsRegex map[string]string) Opt {
	return func(s *DB) {
		s.secretsRegex = secretsRegex
	}
}

func NewSQLQueryWrapper(db *sql.DB, opts ...Opt) *DB {
	s := &DB{
		DB:                 db,
		since:              time.Since,
		slowQueryThreshold: 300 * time.Second,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

func (db *DB) Exec(query string, args ...interface{}) (sql.Result, error) {
	startedAt := time.Now()
	result, err := db.DB.Exec(query, args...)
	db.logQuery(startedAt, query)
	return result, err
}

func (db *DB) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	startedAt := time.Now()
	result, err := db.DB.ExecContext(ctx, query, args...)
	db.logQuery(startedAt, query)
	return result, err
}

func (db *DB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	startedAt := time.Now()
	rows, err := db.DB.Query(query, args...)
	db.logQuery(startedAt, query)
	return rows, err
}

func (db *DB) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	startedAt := time.Now()
	rows, err := db.DB.QueryContext(ctx, query, args...)
	db.logQuery(startedAt, query)
	return rows, err
}

func (db *DB) QueryRow(query string, args ...interface{}) *sql.Row {
	startedAt := time.Now()
	row := db.DB.QueryRow(query, args...)
	db.logQuery(startedAt, query)
	return row
}

func (db *DB) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	startedAt := time.Now()
	row := db.DB.QueryRowContext(ctx, query, args...)
	db.logQuery(startedAt, query)
	return row
}

func (db *DB) logQuery(startedAt time.Time, query string) {
	if executionTime := db.since(startedAt); executionTime > db.slowQueryThreshold {
		sanitizedQuery, _ := misc.ReplaceMultiRegex(query, db.secretsRegex)

		keysAndValues := []any{
			logfield.Query, sanitizedQuery,
			logfield.QueryExecutionTimeInSec, int64(executionTime.Seconds()),
		}
		keysAndValues = append(keysAndValues, db.keysAndValues...)

		db.logger.Infow(ExecutingQuery, keysAndValues...)
	}
}
