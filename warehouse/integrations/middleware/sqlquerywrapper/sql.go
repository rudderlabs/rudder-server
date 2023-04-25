package sqlquerywrapper

import (
	"context"
	"database/sql"
	"time"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/logfield"
)

type Opt func(*DB)

type logger interface {
	Infow(msg string, keysAndValues ...interface{})
}

type DB struct {
	*sql.DB

	since              func(time.Time) time.Duration
	logger             logger
	keysAndValues      []any
	slowQueryThreshold time.Duration
	secretsRegex       map[string]string
}

type Tx struct {
	*sql.Tx
	db *DB
}

func WithLogger(logger logger) Opt {
	return func(s *DB) {
		s.logger = logger
	}
}

func WithKeyAndValues(keyAndValues ...any) Opt {
	return func(s *DB) {
		s.keysAndValues = keyAndValues
	}
}

func WithSlowQueryThreshold(slowQueryThreshold time.Duration) Opt {
	return func(s *DB) {
		s.slowQueryThreshold = slowQueryThreshold
	}
}

func WithSecretsRegex(secretsRegex map[string]string) Opt {
	return func(s *DB) {
		s.secretsRegex = secretsRegex
	}
}

func New(db *sql.DB, opts ...Opt) *DB {
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
	db.logQuery(query, db.since(startedAt))
	return result, err
}

func (db *DB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	startedAt := time.Now()
	result, err := db.DB.ExecContext(ctx, query, args...)
	db.logQuery(query, db.since(startedAt))
	return result, err
}

func (db *DB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	startedAt := time.Now()
	rows, err := db.DB.Query(query, args...)
	db.logQuery(query, db.since(startedAt))
	return rows, err
}

func (db *DB) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	startedAt := time.Now()
	rows, err := db.DB.QueryContext(ctx, query, args...)
	db.logQuery(query, db.since(startedAt))
	return rows, err
}

func (db *DB) QueryRow(query string, args ...interface{}) *sql.Row {
	startedAt := time.Now()
	row := db.DB.QueryRow(query, args...)
	db.logQuery(query, db.since(startedAt))
	return row
}

func (db *DB) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	startedAt := time.Now()
	row := db.DB.QueryRowContext(ctx, query, args...)
	db.logQuery(query, db.since(startedAt))
	return row
}

func (db *DB) logQuery(query string, elapsed time.Duration) {
	if elapsed < db.slowQueryThreshold {
		return
	}

	sanitizedQuery, _ := misc.ReplaceMultiRegex(query, db.secretsRegex)

	keysAndValues := []any{
		logfield.Query, sanitizedQuery,
		logfield.QueryExecutionTime, elapsed,
	}
	keysAndValues = append(keysAndValues, db.keysAndValues...)

	db.logger.Infow("executing query", keysAndValues...)
}

func (db *DB) Begin() (*Tx, error) {
	if tx, err := db.DB.Begin(); err != nil {
		return nil, err
	} else {
		return &Tx{tx, db}, nil
	}
}

func (tx *Tx) Exec(query string, args ...interface{}) (sql.Result, error) {
	startedAt := time.Now()
	result, err := tx.Tx.Exec(query, args...)
	tx.db.logQuery(query, tx.db.since(startedAt))
	return result, err
}

func (tx *Tx) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	startedAt := time.Now()
	result, err := tx.Tx.ExecContext(ctx, query, args...)
	tx.db.logQuery(query, tx.db.since(startedAt))
	return result, err
}

func (tx *Tx) Query(query string, args ...interface{}) (*sql.Rows, error) {
	startedAt := time.Now()
	rows, err := tx.Tx.Query(query, args...)
	tx.db.logQuery(query, tx.db.since(startedAt))
	return rows, err
}

func (tx *Tx) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	startedAt := time.Now()
	rows, err := tx.Tx.QueryContext(ctx, query, args...)
	tx.db.logQuery(query, tx.db.since(startedAt))
	return rows, err
}

func (tx *Tx) QueryRow(query string, args ...interface{}) *sql.Row {
	startedAt := time.Now()
	row := tx.Tx.QueryRow(query, args...)
	tx.db.logQuery(query, tx.db.since(startedAt))
	return row
}

func (tx *Tx) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	startedAt := time.Now()
	row := tx.Tx.QueryRowContext(ctx, query, args...)
	tx.db.logQuery(query, tx.db.since(startedAt))
	return row
}
