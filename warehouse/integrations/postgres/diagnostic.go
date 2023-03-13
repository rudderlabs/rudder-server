package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/warehouse/logfield"
	"golang.org/x/exp/slices"
)

type Diagnostic struct {
	Logger    logger.Logger
	Namespace string
	Warehouse *model.Warehouse
	Stats     stats.Stats
	Config    *config.Config
}

type Tx interface {
	Rollback() error
}

// TxnRollback rolls back a transaction and logs the error.
func (d *Diagnostic) TxnRollback(txn Tx, tableName, stage string) {
	var (
		c                  = make(chan struct{})
		txnRollbackTimeout = d.Config.GetDuration("Warehouse.postgres.txnRollbackTimeout", 30, time.Second)
	)

	go func() {
		defer close(c)

		if err := txn.Rollback(); err != nil {
			d.Logger.Warnw("rollback transaction",
				logfield.SourceID, d.Warehouse.Source.ID,
				logfield.SourceType, d.Warehouse.Source.SourceDefinition.Name,
				logfield.DestinationID, d.Warehouse.Destination.ID,
				logfield.DestinationType, d.Warehouse.Destination.DestinationDefinition.Name,
				logfield.WorkspaceID, d.Warehouse.WorkspaceID,
				logfield.Namespace, d.Namespace,
				logfield.TableName, tableName,
				logfield.Error, err.Error(),
			)
		}
	}()

	select {
	case <-c:
	case <-time.After(txnRollbackTimeout):
		tags := stats.Tags{
			"workspaceId": d.Warehouse.WorkspaceID,
			"namespace":   d.Namespace,
			"destType":    d.Warehouse.Destination.DestinationDefinition.Name,
			"destID":      d.Warehouse.Destination.ID,
			"tableName":   tableName,
			"stage":       stage,
		}

		d.Stats.NewTaggedStat("pg_rollback_timeout", stats.CountType, tags).Count(1)
	}
}

// TxnExecute
// Print execution plan if enableWithQueryPlan is set to true else return result set.
// Currently, these statements are supported by EXPLAIN
// Any INSERT, UPDATE, DELETE whose execution plan you wish to see.
func (d *Diagnostic) TxnExecute(ctx context.Context, txn *sql.Tx, tableName, query string) error {
	var (
		enableSQLStatementExecutionPlanWorkspaceIDs = d.Config.GetStringSlice("Warehouse.postgres.EnableSQLStatementExecutionPlanWorkspaceIDs", nil)
		enableSQLStatementExecutionPlan             = d.Config.GetBool("Warehouse.postgres.enableSQLStatementExecutionPlan", false)
		canUseQueryPlanner                          = enableSQLStatementExecutionPlan || slices.Contains(enableSQLStatementExecutionPlanWorkspaceIDs, d.Warehouse.WorkspaceID)
	)

	if !canUseQueryPlanner {
		if _, err := txn.ExecContext(ctx, query); err != nil {
			return fmt.Errorf("executing query: %w", err)
		}
		return nil
	}

	var (
		err      error
		rows     *sql.Rows
		response []string
		s        string
	)

	explainQuery := fmt.Sprintf("EXPLAIN %s", query)
	if rows, err = txn.QueryContext(ctx, explainQuery); err != nil {
		return fmt.Errorf("executing explain query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		if err = rows.Scan(&s); err != nil {
			return fmt.Errorf("scanning explain query: %w", err)
		}

		response = append(response, s)
	}
	d.Logger.Infow("execution query plan",
		logfield.SourceID, d.Warehouse.Source.ID,
		logfield.SourceType, d.Warehouse.Source.SourceDefinition.Name,
		logfield.DestinationID, d.Warehouse.Destination.ID,
		logfield.DestinationType, d.Warehouse.Destination.DestinationDefinition.Name,
		logfield.WorkspaceID, d.Warehouse.WorkspaceID,
		logfield.TableName, tableName,
		logfield.Query, explainQuery,
		logfield.QueryPlanner, strings.Join(response, "\n"),
	)

	if _, err := txn.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("executing query: %w", err)
	}
	return nil
}
