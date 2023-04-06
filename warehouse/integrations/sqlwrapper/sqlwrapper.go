package sqlwrapper

import (
	"context"
	"database/sql"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/logfield"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"time"
)

type SQLWrapper interface {
	Exec(string, ...interface{}) (sql.Result, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)

	Query(string, ...interface{}) (*sql.Rows, error)
	QueryRow(string, ...interface{}) *sql.Row
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row

	Close() error

	Begin() (*sql.Tx, error)
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)

	Ping() error
	PingContext(ctx context.Context) error
}

type sqlWrapper struct {
	db             *sql.DB
	logger         logger.Logger
	warehouse      model.Warehouse
	slowQueryInSec time.Duration
}

func NewSQLWrapper(db *sql.DB, logger logger.Logger, warehouse model.Warehouse) SQLWrapper {
	return &sqlWrapper{
		db:             db,
		logger:         logger,
		warehouse:      warehouse,
		slowQueryInSec: config.GetDuration("Warehouse.slowQueryInSec", 300, time.Second),
	}
}

func (s *sqlWrapper) Exec(query string, args ...interface{}) (sql.Result, error) {
	startedAt := time.Now()

	result, err := s.db.Exec(query, args...)
	if err != nil {
		return result, err
	}

	s.log(startedAt, query)

	return result, nil
}

func (s *sqlWrapper) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	startedAt := time.Now()

	result, err := s.db.ExecContext(ctx, query, args...)
	if err != nil {
		return result, err
	}

	s.log(startedAt, query)

	return result, nil
}

func (s *sqlWrapper) Query(query string, args ...interface{}) (*sql.Rows, error) {
	startedAt := time.Now()

	rows, err := s.db.Query(query, args...)
	if err != nil {
		return rows, err
	}

	s.log(startedAt, query)

	return rows, nil
}

func (s *sqlWrapper) QueryRow(query string, args ...interface{}) *sql.Row {
	startedAt := time.Now()

	row := s.db.QueryRow(query, args...)

	s.log(startedAt, query)

	return row
}

func (s *sqlWrapper) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	startedAt := time.Now()

	row := s.db.QueryRowContext(ctx, query, args...)

	s.log(startedAt, query)

	return row
}

func (s *sqlWrapper) Close() error {
	return s.db.Close()
}

func (s *sqlWrapper) Ping() error {
	return s.db.Ping()
}

func (s *sqlWrapper) PingContext(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

func (s *sqlWrapper) Begin() (*sql.Tx, error) {
	return s.db.Begin()
}

func (s *sqlWrapper) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return s.db.BeginTx(ctx, opts)
}

func (s *sqlWrapper) log(startedAt time.Time, query string) {
	if executionTime := time.Since(startedAt); executionTime > s.slowQueryInSec {
		keysAndValues := []any{
			logfield.Query, query,
			logfield.QueryExecutionTimeInSec, warehouseutils.DurationInSecs(executionTime),
			logfield.SourceID, s.warehouse.Source.ID,
			logfield.SourceType, s.warehouse.Source.SourceDefinition.Name,
			logfield.DestinationID, s.warehouse.Destination.ID,
			logfield.DestinationType, s.warehouse.Destination.DestinationDefinition.Name,
			logfield.WorkspaceID, s.warehouse.WorkspaceID,
			logfield.Namespace, s.warehouse.Namespace,
		}
		s.logger.Infow("executing query", keysAndValues...)
	}
}
