package dbwrapper

import (
	"context"
	"database/sql"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/logfield"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type DB struct {
	*sql.DB

	logger         logger.Logger
	warehouse      model.Warehouse
	slowQueryInSec time.Duration
}

func NewSQLWrapper(db *sql.DB, logger logger.Logger, warehouse model.Warehouse) *DB {
	return &DB{
		DB:             db,
		logger:         logger,
		warehouse:      warehouse,
		slowQueryInSec: config.GetDuration("Warehouse.slowQueryInSec", 300, time.Second),
	}
}

func (s *DB) Exec(query string, args ...interface{}) (sql.Result, error) {
	defer s.log(time.Now(), query)
	return s.DB.Exec(query, args...)
}

func (s *DB) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	defer s.log(time.Now(), query)
	return s.DB.ExecContext(ctx, query, args...)
}

func (s *DB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	defer s.log(time.Now(), query)
	return s.DB.Query(query, args...)
}

func (s *DB) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	defer s.log(time.Now(), query)
	return s.DB.QueryContext(ctx, query, args...)
}

func (s *DB) QueryRow(query string, args ...interface{}) *sql.Row {
	defer s.log(time.Now(), query)
	return s.DB.QueryRow(query, args...)
}

func (s *DB) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	defer s.log(time.Now(), query)
	return s.DB.QueryRowContext(ctx, query, args...)
}

func (s *DB) log(startedAt time.Time, query string) {
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
