package sqlquerywrapper

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	rslogger "github.com/rudderlabs/rudder-go-kit/logger"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/logfield"
)

type Opt func(*DB)

type logger interface {
	Infow(msg string, keysAndValues ...interface{})
	Warnw(msg string, keysAndValues ...interface{})
}

type DB struct {
	*sql.DB

	since              func(time.Time) time.Duration
	logger             logger
	keysAndValues      []any
	slowQueryThreshold time.Duration
	queryTimeout       time.Duration
	transactionTimeout time.Duration
	rollbackThreshold  time.Duration
	commitThreshold    time.Duration
	secretsRegex       map[string]string
}

type Rows struct {
	*sql.Rows
	context.CancelFunc
	logQ
}

func (r *Rows) Close() error {
	defer r.CancelFunc()
	r.logQ()
	return r.Rows.Close()
}

func (r *Rows) Next() bool {
	return r.Rows.Next()
}

func (r *Rows) Scan(dest ...interface{}) error {
	return r.Rows.Scan(dest...)
}

func (r *Rows) Err() error {
	return r.Rows.Err()
}

type Row struct {
	*sql.Row
	context.CancelFunc
	logQ
}

func (r *Row) Scan(dest ...interface{}) error {
	defer r.CancelFunc()
	r.logQ()
	return r.Row.Scan(dest...)
}

func (r *Row) Err() error {
	defer r.CancelFunc()
	return r.Row.Err()
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

// imposes a timeout on each query
func WithQueryTimeout(timeout time.Duration) Opt {
	return func(s *DB) {
		s.queryTimeout = timeout
	}
}

// imposes a timeout on the transaction
func WithTransactionTimeout(timeout time.Duration) Opt {
	return func(s *DB) {
		s.transactionTimeout = timeout
	}
}

func New(db *sql.DB, opts ...Opt) *DB {
	s := &DB{
		DB:                db,
		since:             time.Since,
		rollbackThreshold: 30 * time.Second,
		commitThreshold:   30 * time.Second,
		logger:            rslogger.NOP,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

func (db *DB) Exec(query string, args ...interface{}) (sql.Result, error) {
	return db.ExecContext(context.Background(), query, args...)
}

func (db *DB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	startedAt := time.Now()
	ctx, cancel := queryContextWithTimeout(ctx, db.queryTimeout)
	defer cancel()
	result, err := db.DB.ExecContext(ctx, query, args...)
	db.logQuery(query, db.since(startedAt))()
	return result, err
}

func (db *DB) Query(query string, args ...interface{}) (*Rows, error) {
	return db.QueryContext(context.Background(), query, args...)
}

func (db *DB) QueryContext(ctx context.Context, query string, args ...interface{}) (*Rows, error) {
	startedAt := time.Now()
	ctx, cancel := queryContextWithTimeout(ctx, db.queryTimeout)
	rows, err := db.DB.QueryContext(ctx, query, args...)
	if err != nil {
		defer cancel()
		defer db.logQuery(query, db.since(startedAt))()
		return nil, err
	}
	if err := rows.Err(); err != nil {
		cancel()
		db.logQuery(query, db.since(startedAt))()
		func() { _ = rows.Close() }()
		return nil, err
	}
	return &Rows{
		Rows:       rows,
		CancelFunc: cancel,
		logQ:       db.logQuery(query, db.since(startedAt)),
	}, err
}

func (db *DB) QueryRow(query string, args ...interface{}) *Row {
	return db.QueryRowContext(context.Background(), query, args...)
}

func (db *DB) QueryRowContext(ctx context.Context, query string, args ...interface{}) *Row {
	startedAt := time.Now()
	ctx, cancel := queryContextWithTimeout(ctx, db.queryTimeout)
	return &Row{
		Row:        db.DB.QueryRowContext(ctx, query, args...),
		CancelFunc: cancel,
		logQ:       db.logQuery(query, db.since(startedAt)),
	}
}

func (db *DB) WithTx(ctx context.Context, fn func(*Tx) error) error {
	ctx, cancel := queryContextWithTimeout(ctx, db.queryTimeout)
	defer cancel()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}

	if err = fn(tx); err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			keysAndValues := []any{logfield.Error, fmt.Errorf("executing transaction: %s, rollback: %s", err.Error(), rollbackErr.Error()).Error()}
			keysAndValues = append(keysAndValues, db.keysAndValues...)

			db.logger.Warnw("failed rollback transaction", keysAndValues...)
		}
		return fmt.Errorf("executing transaction: %w", err)
	}

	return tx.Commit()
}

func (db *DB) logQuery(query string, elapsed time.Duration) logQ {
	if db.slowQueryThreshold <= 0 {
		return func() {}
	}
	if elapsed < db.slowQueryThreshold {
		return func() {}
	}

	sanitizedQuery, _ := misc.ReplaceMultiRegex(query, db.secretsRegex)

	keysAndValues := []any{
		logfield.Query, sanitizedQuery,
		logfield.QueryExecutionTime, elapsed,
	}
	keysAndValues = append(keysAndValues, db.keysAndValues...)

	return func() { db.logger.Infow("executing query", keysAndValues...) }
}

type logQ func()

func (tx *Tx) GetTx() *sql.Tx {
	return tx.Tx
}

// Begin starts a transaction.
//
// Use BeginTx to pass context and options to the underlying driver.
func (db *DB) Begin() (*Tx, error) {
	return db.BeginTx(context.Background(), nil)
}

func (db *DB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	ctx, cancel := queryContextWithTimeout(ctx, db.transactionTimeout)
	defer cancel()
	tx, err := db.DB.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &Tx{tx, db}, nil
}

func (tx *Tx) Exec(query string, args ...interface{}) (sql.Result, error) {
	return tx.ExecContext(context.Background(), query, args...)
}

func (tx *Tx) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	startedAt := time.Now()
	ctx, cancel := queryContextWithTimeout(ctx, tx.db.queryTimeout)
	defer cancel()
	result, err := tx.Tx.ExecContext(ctx, query, args...)
	tx.db.logQuery(query, tx.db.since(startedAt))()
	return result, err
}

func (tx *Tx) Query(query string, args ...interface{}) (*Rows, error) {
	return tx.QueryContext(context.Background(), query, args...)
}

func (tx *Tx) QueryContext(ctx context.Context, query string, args ...interface{}) (*Rows, error) {
	startedAt := time.Now()
	ctx, cancel := queryContextWithTimeout(ctx, tx.db.queryTimeout)
	rows, err := tx.Tx.QueryContext(ctx, query, args...)
	if err != nil {
		defer cancel()
		defer tx.db.logQuery(query, tx.db.since(startedAt))()
		return nil, err
	}
	if err := rows.Err(); err != nil {
		cancel()
		tx.db.logQuery(query, tx.db.since(startedAt))()
		func() { _ = rows.Close() }()
		return nil, err
	}
	return &Rows{
		Rows:       rows,
		CancelFunc: cancel,
		logQ:       tx.db.logQuery(query, tx.db.since(startedAt)),
	}, err
}

func (tx *Tx) QueryRow(query string, args ...interface{}) *Row {
	return tx.QueryRowContext(context.Background(), query, args...)
}

func (tx *Tx) QueryRowContext(ctx context.Context, query string, args ...interface{}) *Row {
	startedAt := time.Now()
	ctx, cancel := queryContextWithTimeout(ctx, tx.db.queryTimeout)
	return &Row{
		Row:        tx.Tx.QueryRowContext(ctx, query, args...),
		CancelFunc: cancel,
		logQ:       tx.db.logQuery(query, tx.db.since(startedAt)),
	}
}

func (tx *Tx) Rollback() error {
	startedAt := time.Now()
	err := tx.Tx.Rollback()
	if elapsed := tx.db.since(startedAt); elapsed > tx.db.rollbackThreshold {
		tx.db.logger.Warnw("rollback threshold exceeded", tx.db.keysAndValues...)
	}
	return err
}

func (tx *Tx) Commit() error {
	startedAt := time.Now()
	err := tx.Tx.Commit()
	if elapsed := tx.db.since(startedAt); elapsed > tx.db.commitThreshold {
		tx.db.logger.Warnw("commit threshold exceeded", tx.db.keysAndValues...)
	}
	return err
}

func queryContextWithTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout.Abs() <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, timeout)
}
