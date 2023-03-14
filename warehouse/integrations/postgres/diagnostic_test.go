package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"

	"github.com/golang/mock/gomock"
	"github.com/ory/dockertest/v3"
	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	mock_logger "github.com/rudderlabs/rudder-server/mocks/utils/logger"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/services/stats/memstats"
	"github.com/rudderlabs/rudder-server/testhelper/destination"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/postgres"
	"github.com/rudderlabs/rudder-server/warehouse/logfield"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/stretchr/testify/require"
)

type mockTxn struct {
	op func() error
}

func (m *mockTxn) Rollback() error {
	return m.op()
}

func TestDiagnostic_Exec(t *testing.T) {
	misc.Init()
	warehouseutils.Init()
	postgres.Init()

	var (
		namespace       = "test_namespace"
		tableName       = "test_table"
		stage           = "test_stage"
		sourceID        = "test_source_id"
		sourceType      = "test_source_type"
		destinationID   = "test_dest_id"
		destinationType = "test_dest_type"
		workspaceID     = "test_workspace_id"
	)

	warehouse := model.Warehouse{
		Source: backendconfig.SourceT{
			ID: sourceID,
			SourceDefinition: backendconfig.SourceDefinitionT{
				Name: sourceType,
			},
		},
		Destination: backendconfig.DestinationT{
			ID: destinationID,
			DestinationDefinition: backendconfig.DestinationDefinitionT{
				Name: destinationType,
			},
		},
		Namespace:   namespace,
		WorkspaceID: workspaceID,
	}

	tags := stats.Tags{
		"workspaceId": workspaceID,
		"namespace":   namespace,
		"destType":    destinationType,
		"destID":      destinationID,
		"tableName":   tableName,
		"stage":       stage,
	}

	t.Run("txn rollback after timeout", func(t *testing.T) {
		t.Parallel()

		c := config.New()
		c.Set("Warehouse.postgres.txnRollbackTimeout", "1ms")

		store := memstats.New()

		d := postgres.Diagnostic{
			Logger:    logger.NOP,
			Config:    c,
			Namespace: namespace,
			Stats:     store,
			Warehouse: &warehouse,
		}

		blocker := make(chan struct{})
		defer close(blocker)

		mockTxn := mockTxn{
			op: func() error {
				<-blocker
				return nil
			},
		}

		d.TxnRollback(&mockTxn, tableName, stage)

		m := store.Get("pg_rollback_timeout", tags)
		require.NotNil(t, m)
		require.EqualValues(t, m.LastValue(), 1)
	})

	testCases := []struct {
		name          string
		rollbackError error
	}{
		{
			name:          "txn rollback with error",
			rollbackError: fmt.Errorf("rollback error"),
		},
		{
			name: "txn rollback without error",
		},
	}

	for _, tc := range testCases {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := config.New()
			store := memstats.New()

			d := postgres.Diagnostic{
				Logger:    logger.NOP,
				Config:    c,
				Namespace: namespace,
				Stats:     store,
				Warehouse: &warehouse,
			}

			mockTxn := mockTxn{
				op: func() error {
					return tc.rollbackError
				},
			}

			d.TxnRollback(&mockTxn, tableName, stage)

			m := store.Get("pg_rollback_error", tags)
			require.Nil(t, m)
		})
	}
}

func TestDiagnostic_Execute(t *testing.T) {
	misc.Init()
	warehouseutils.Init()
	postgres.Init()

	var (
		namespace       = "test_namespace"
		tableName       = "test_table"
		sourceID        = "test_source_id"
		sourceType      = "test_source_type"
		destinationID   = "test_dest_id"
		destinationType = "test_dest_type"
		workspaceID     = "test_workspace_id"
	)

	warehouse := model.Warehouse{
		Source: backendconfig.SourceT{
			ID: sourceID,
			SourceDefinition: backendconfig.SourceDefinitionT{
				Name: sourceType,
			},
		},
		Destination: backendconfig.DestinationT{
			ID: destinationID,
			DestinationDefinition: backendconfig.DestinationDefinitionT{
				Name: destinationType,
			},
		},
		Namespace:   namespace,
		WorkspaceID: workspaceID,
	}

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pgResource, err := destination.SetupPostgres(pool, t)
	require.NoError(t, err)

	testCases := []struct {
		name                                        string
		enableSQLStatementExecutionPlan             bool
		EnableSQLStatementExecutionPlanWorkspaceIDs []string
		cancelContext                               bool
		wantError                                   error
		wantLog                                     bool
	}{
		{
			name: "without query planner",
		},
		{
			name:          "without query planner and context cancelled",
			cancelContext: true,
			wantError:     errors.New("executing query: context canceled"),
		},
		{
			name:                            "with query planner",
			enableSQLStatementExecutionPlan: true,
			EnableSQLStatementExecutionPlanWorkspaceIDs: []string{workspaceID},
			wantLog: true,
		},
	}

	for _, tc := range testCases {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := config.New()
			c.Set("Warehouse.postgres.enableSQLStatementExecutionPlan", tc.enableSQLStatementExecutionPlan)
			c.Set("Warehouse.postgres.enableSQLStatementExecutionPlanWorkspaceIDs", tc.EnableSQLStatementExecutionPlanWorkspaceIDs)

			store := memstats.New()

			var (
				query        = "SELECT 1"
				explainQuery = "EXPLAIN SELECT 1"
				queryPlanner = "Result  (cost=0.00..0.01 rows=1 width=4)"
			)

			ctrl := gomock.NewController(t)
			mockLogger := mock_logger.NewMockLogger(ctrl)
			if tc.wantLog {
				mockLogger.EXPECT().Infow("execution query plan",
					logfield.SourceID, sourceID,
					logfield.SourceType, sourceType,
					logfield.DestinationID, destinationID,
					logfield.DestinationType, destinationType,
					logfield.WorkspaceID, workspaceID,
					logfield.TableName, tableName,
					logfield.Query, explainQuery,
					logfield.QueryPlanner, queryPlanner,
				).Times(1)
			}

			ctx, cancel := context.WithCancel(context.Background())
			if tc.cancelContext {
				cancel()
			} else {
				defer cancel()
			}

			d := postgres.Diagnostic{
				Logger:    mockLogger,
				Config:    c,
				Namespace: namespace,
				Stats:     store,
				Warehouse: &warehouse,
			}

			txn, err := pgResource.DB.Begin()
			require.NoError(t, err)

			err = d.TxnExecute(ctx, txn, tableName, query)
			if tc.wantError != nil {
				require.EqualError(t, err, tc.wantError.Error())
				return
			}
			require.NoError(t, err)
		})
	}
}
