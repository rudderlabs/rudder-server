package router

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/services/alerta"
	migrator "github.com/rudderlabs/rudder-server/services/sql-migrator"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/manager"
	sqlmiddleware "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/redshift"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestExtractUploadErrorsByState(t *testing.T) {
	input := []struct {
		InitialErrorState []byte
		CurrentErrorState string
		CurrentError      error
		ErrorCount        int
	}{
		{
			InitialErrorState: []byte(`{}`),
			CurrentErrorState: InternalProcessingFailed,
			CurrentError:      errors.New("account locked"),
			ErrorCount:        1,
		},
		{
			InitialErrorState: []byte(`{"internal_processing_failed": {"errors": ["account locked"], "attempt": 1}}`),
			CurrentErrorState: InternalProcessingFailed,
			CurrentError:      errors.New("account locked again"),
			ErrorCount:        2,
		},
		{
			InitialErrorState: []byte(`{"internal_processing_failed": {"errors": ["account locked", "account locked again"], "attempt": 2}}`),
			CurrentErrorState: model.TableUploadExportingFailed,
			CurrentError:      errors.New("failed to load data because failed in earlier job"),
			ErrorCount:        1,
		},
	}

	for _, ip := range input {

		uploadErrors, err := extractAndUpdateUploadErrorsByState(ip.InitialErrorState, ip.CurrentErrorState, ip.CurrentError)
		if err != nil {
			t.Errorf("extracting upload errors by state should have passed: %v", err)
		}

		stateErrors := uploadErrors[ip.CurrentErrorState]
		// Below switch clause mirrors how we are
		// adding data in generic interface.

		var errorLength int
		switch stateErrors["errors"].(type) {
		case []string:
			errorLength = len(stateErrors["errors"].([]string))
		case []interface{}:
			errorLength = len(stateErrors["errors"].([]interface{}))
		}

		if errorLength != ip.ErrorCount {
			t.Errorf("expected error to be addded to list of state errors")
		}

		if stateErrors["attempt"].(int) != ip.ErrorCount {
			t.Errorf("expected attempts to be: %d, got: %d", ip.ErrorCount, stateErrors["attempt"].(int))
		}
	}
}

func TestColumnCountStat(t *testing.T) {
	var (
		workspaceID     = "test-workspaceID"
		destinationID   = "test-destinationID"
		destinationName = "test-destinationName"
		sourceID        = "test-sourceID"
		sourceName      = "test-sourceName"
		tableName       = "test-table"
	)

	inputs := []struct {
		name             string
		columnCountLimit int
		destinationType  string
		statExpected     bool
	}{
		{
			name:            "Datalakes destination",
			destinationType: warehouseutils.S3Datalake,
		},
		{
			name:            "Unknown destination",
			destinationType: "unknown-destination",
		},
		{
			name:             "Greater than threshold",
			destinationType:  warehouseutils.RS,
			columnCountLimit: 1,
			statExpected:     true,
		},
		{
			name:             "Lesser than threshold",
			destinationType:  warehouseutils.RS,
			columnCountLimit: 10,
			statExpected:     true,
		},
	}

	statsStore, err := memstats.New()
	require.NoError(t, err)

	for _, tc := range inputs {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			conf := config.New()
			conf.Set(fmt.Sprintf("Warehouse.%s.columnCountLimit", strings.ToLower(warehouseutils.WHDestNameMap[tc.destinationType])), tc.columnCountLimit)

			pool, err := dockertest.NewPool("")
			require.NoError(t, err)

			pgResource, err := postgres.Setup(pool, t)
			require.NoError(t, err)
			db := sqlmiddleware.New(pgResource.DB)
			err = (&migrator.Migrator{
				Handle:          pgResource.DB,
				MigrationsTable: "wh_schema_migrations",
			}).Migrate("warehouse")
			require.NoError(t, err)

			uploadJobFactory := &UploadJobFactory{
				logger:       logger.NOP,
				statsFactory: statsStore,
				conf:         conf,
				db:           db,
			}
			whManager, err := manager.New(warehouseutils.POSTGRES, conf, logger.NOP, statsStore)
			require.NoError(t, err)
			ctx := context.Background()
			j := uploadJobFactory.NewUploadJob(ctx, &model.UploadJob{
				Upload: model.Upload{
					WorkspaceID:   workspaceID,
					DestinationID: destinationID,
					SourceID:      sourceID,
				},
				Warehouse: model.Warehouse{
					Type: tc.destinationType,
					Destination: backendconfig.DestinationT{
						ID:   destinationID,
						Name: destinationName,
					},
					Source: backendconfig.SourceT{
						ID:   sourceID,
						Name: sourceName,
					},
				},
			}, whManager)
			err = j.schemaHandle.UpdateWarehouseTableSchema(ctx, tableName, model.TableSchema{
				"test-column-1": "string",
				"test-column-2": "string",
				"test-column-3": "string",
			})
			require.NoError(t, err)

			tags := j.buildTags()
			tags["tableName"] = warehouseutils.TableNameForStats(tableName)

			j.columnCountStat(tableName)

			m1 := statsStore.Get("warehouse_load_table_column_count", tags)
			m2 := statsStore.Get("warehouse_load_table_column_limit", tags)

			if tc.statExpected {
				columnsCount, err := j.schemaHandle.GetColumnsCountInWarehouseSchema(ctx, tableName)
				require.NoError(t, err)
				require.EqualValues(t, m1.LastValue(), columnsCount)
				require.EqualValues(t, m2.LastValue(), tc.columnCountLimit)
			} else {
				require.Nil(t, m1)
				require.Nil(t, m2)
			}
		})
	}
}

type mockAlertSender struct {
	mockError error
}

func (m *mockAlertSender) SendAlert(context.Context, string, alerta.SendAlertOpts) error {
	return m.mockError
}

func TestUploadJobT_UpdateTableSchema(t *testing.T) {
	t.Parallel()

	var (
		testNamespace       = "test_namespace"
		testTable           = "test_table"
		testColumn          = "test_column"
		testColumnType      = "text"
		testDestinationID   = "test_destination_id"
		testDestinationType = "test_destination_type"
	)

	t.Run("alter column", func(t *testing.T) {
		t.Parallel()

		t.Run("basic", func(t *testing.T) {
			t.Parallel()

			testCases := []struct {
				name           string
				createView     bool
				mockAlertError error
				wantError      error
			}{
				{
					name: "success",
				},
				{
					name:       "with view attached to table",
					createView: true,
				},
				{
					name:           "with alert error",
					createView:     true,
					mockAlertError: errors.New("alert error"),
					wantError:      errors.New("alert error"),
				},
				{
					name:           "skipping columns",
					createView:     true,
					mockAlertError: errors.New("alert error"),
					wantError:      errors.New("alert error"),
				},
			}

			for _, tc := range testCases {
				tc := tc

				t.Run(tc.name, func(t *testing.T) {
					t.Parallel()

					pool, err := dockertest.NewPool("")
					require.NoError(t, err)

					pgResource, err := postgres.Setup(pool, t)
					require.NoError(t, err)

					t.Log("db:", pgResource.DBDsn)

					rs := redshift.New(config.New(), logger.NOP, stats.NOP)
					rs.DB = sqlmiddleware.New(pgResource.DB)
					rs.Namespace = testNamespace

					ujf := &UploadJobFactory{
						conf:         config.New(),
						logger:       logger.NOP,
						statsFactory: stats.NOP,
						db:           sqlmiddleware.New(pgResource.DB),
					}

					job := ujf.NewUploadJob(context.Background(), &model.UploadJob{
						Upload: model.Upload{
							DestinationID:   testDestinationID,
							DestinationType: testDestinationType,
						},
						Warehouse: model.Warehouse{
							Type: testDestinationType,
						},
					}, rs)
					job.alertSender = &mockAlertSender{
						mockError: tc.mockAlertError,
					}

					_, err = rs.DB.Exec(
						fmt.Sprintf("CREATE SCHEMA %s;",
							testNamespace,
						),
					)
					require.NoError(t, err)

					_, err = rs.DB.Exec(
						fmt.Sprintf("CREATE TABLE %q.%q (%s VARCHAR(512));",
							testNamespace,
							testTable,
							testColumn,
						),
					)
					require.NoError(t, err)

					if tc.createView {
						_, err = rs.DB.Exec(
							fmt.Sprintf("CREATE VIEW %[1]q.%[2]q AS SELECT * FROM %[1]q.%[3]q;",
								testNamespace,
								fmt.Sprintf("%s_view", testTable),
								testTable,
							),
						)
						require.NoError(t, err)
					}

					err = job.UpdateTableSchema(testTable, warehouseutils.TableSchemaDiff{
						AlteredColumnMap: model.TableSchema{
							testColumn: testColumnType,
						},
					})
					if tc.wantError != nil {
						require.ErrorContains(t, err, tc.wantError.Error())
					} else {
						require.NoError(t, err)
					}
				})
			}
		})

		t.Run("process all columns", func(t *testing.T) {
			t.Parallel()

			pool, err := dockertest.NewPool("")
			require.NoError(t, err)

			pgResource, err := postgres.Setup(pool, t)
			require.NoError(t, err)

			t.Log("db:", pgResource.DBDsn)

			rs := redshift.New(config.New(), logger.NOP, stats.NOP)
			rs.DB = sqlmiddleware.New(pgResource.DB)
			rs.Namespace = testNamespace

			ujf := &UploadJobFactory{
				conf:         config.New(),
				logger:       logger.NOP,
				statsFactory: stats.NOP,
				db:           sqlmiddleware.New(pgResource.DB),
			}

			job := ujf.NewUploadJob(context.Background(), &model.UploadJob{
				Upload: model.Upload{
					DestinationID:   testDestinationID,
					DestinationType: testDestinationType,
				},
				Warehouse: model.Warehouse{
					Type: testDestinationType,
				},
			}, rs)
			job.alertSender = &mockAlertSender{}

			_, err = rs.DB.Exec(
				fmt.Sprintf("CREATE SCHEMA %s;",
					testNamespace,
				),
			)
			require.NoError(t, err)

			_, err = rs.DB.Exec(
				fmt.Sprintf("CREATE TABLE %q.%q (%s VARCHAR(512));",
					testNamespace,
					testTable,
					testColumn,
				),
			)
			require.NoError(t, err)

			for i := range [10]int{} {
				if i%3 == 0 {
					continue
				}

				_, err = rs.DB.Exec(
					fmt.Sprintf("ALTER TABLE %q.%q ADD COLUMN %s_%d VARCHAR(512);",
						testNamespace,
						testTable,
						testColumn,
						i,
					),
				)
				require.NoError(t, err)
			}

			_, err = rs.DB.Exec(
				fmt.Sprintf("CREATE VIEW %[1]q.%[2]q AS SELECT * FROM %[1]q.%[3]q;",
					testNamespace,
					fmt.Sprintf("%s_view", testTable),
					testTable,
				),
			)
			require.NoError(t, err)

			alteredColumnsMap := model.TableSchema{}
			for i := range [10]int{} {
				alteredColumnsMap[fmt.Sprintf("%s_%d", testColumn, i)] = testColumnType
			}

			err = job.UpdateTableSchema(testTable, warehouseutils.TableSchemaDiff{
				AlteredColumnMap: alteredColumnsMap,
			})
			require.Error(t, err)

			for i := range [10]int{} {
				column := fmt.Sprintf("test_column_%d", i)

				if i%3 == 0 {
					require.Contains(t, err.Error(), column)
				} else {
					require.NotContains(t, err.Error(), column)
				}
			}
		})
	})
}

func TestUploadJobT_Aborted(t *testing.T) {
	t.Parallel()

	var (
		minAttempts    = 3
		minRetryWindow = 3 * time.Hour
		now            = time.Date(2021, 1, 1, 6, 0, 0, 0, time.UTC)
	)

	testCases := []struct {
		name      string
		attempts  int
		startTime time.Time
		expected  bool
	}{
		{
			name:      "empty start time",
			startTime: time.Time{},
			expected:  false,
		},
		{
			name:      "crossing max attempts but not retry window",
			attempts:  5,
			startTime: time.Date(2021, 1, 1, 5, 30, 0, 0, time.UTC),
			expected:  false,
		},
		{
			name:      "crossing max retry window but not attempts",
			attempts:  2,
			startTime: time.Date(2021, 1, 1, 2, 0, 0, 0, time.UTC),
			expected:  false,
		},
		{
			name:      "crossing max retry window but not attempts",
			attempts:  5,
			startTime: time.Date(2021, 1, 1, 2, 0, 0, 0, time.UTC),
			expected:  true,
		},
	}

	for _, tc := range testCases {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			job := &UploadJob{
				now: func() time.Time { return now },
				ctx: context.Background(),
			}
			job.config.minRetryAttempts = minAttempts
			job.config.retryTimeWindow = minRetryWindow

			require.Equal(t, tc.expected, job.Aborted(tc.attempts, tc.startTime))
		})
	}
}

type mockPendingTablesRepo struct {
	pendingTables []model.PendingTableUpload
	err           error
	called        int
}

func (m *mockPendingTablesRepo) PendingTableUploads(context.Context, string, int64, string) ([]model.PendingTableUpload, error) {
	m.called++
	return m.pendingTables, m.err
}

func TestUploadJobT_TablesToSkip(t *testing.T) {
	t.Parallel()

	t.Run("repo error", func(t *testing.T) {
		t.Parallel()

		job := &UploadJob{
			upload: model.Upload{
				ID: 1,
			},
			pendingTableUploadsRepo: &mockPendingTablesRepo{
				err: errors.New("some error"),
			},
			ctx: context.Background(),
		}

		previouslyFailedTables, currentJobSucceededTables, err := job.TablesToSkip()
		require.EqualError(t, err, "pending table uploads: some error")
		require.Empty(t, previouslyFailedTables)
		require.Empty(t, currentJobSucceededTables)
	})

	t.Run("should populate only once", func(t *testing.T) {
		t.Parallel()

		ptRepo := &mockPendingTablesRepo{}

		job := &UploadJob{
			upload: model.Upload{
				ID: 1,
			},
			pendingTableUploadsRepo: ptRepo,
			ctx:                     context.Background(),
		}

		for i := 0; i < 5; i++ {
			_, _, _ = job.TablesToSkip()
			require.Equal(t, 1, ptRepo.called)
		}
	})

	t.Run("skip tables", func(t *testing.T) {
		t.Parallel()

		const (
			namespace = "namespace"
			destID    = "destID"
		)

		pendingTables := []model.PendingTableUpload{
			{
				UploadID:      1,
				DestinationID: destID,
				Namespace:     namespace,
				Status:        model.TableUploadExportingFailed,
				TableName:     "previously_failed_table_1",
				Error:         "some error",
			},
			{
				UploadID:      1,
				DestinationID: destID,
				Namespace:     namespace,
				Status:        model.TableUploadUpdatingSchemaFailed,
				TableName:     "previously_failed_table_2",
				Error:         "",
			},
			{
				UploadID:      1,
				DestinationID: destID,
				Namespace:     namespace,
				Status:        model.TableUploadExported,
				TableName:     "previously_succeeded_table_1",
				Error:         "",
			},
			{
				UploadID:      5,
				DestinationID: destID,
				Namespace:     namespace,
				Status:        model.TableUploadExportingFailed,
				TableName:     "current_failed_table_1",
				Error:         "some error",
			},
			{
				UploadID:      5,
				DestinationID: destID,
				Namespace:     namespace,
				Status:        model.TableUploadExported,
				TableName:     "current_succeeded_table_1",
				Error:         "",
			},
		}

		job := &UploadJob{
			upload: model.Upload{
				ID: 5,
			},
			pendingTableUploadsRepo: &mockPendingTablesRepo{
				pendingTables: pendingTables,
			},
			ctx: context.Background(),
		}

		previouslyFailedTables, currentJobSucceededTables, err := job.TablesToSkip()
		require.NoError(t, err)
		require.Equal(t, previouslyFailedTables, map[string]model.PendingTableUpload{
			"previously_failed_table_1": pendingTables[0],
		})
		require.Equal(t, currentJobSucceededTables, map[string]model.PendingTableUpload{
			"current_succeeded_table_1": pendingTables[4],
		})
	})
}

func TestUploadJob_DurationBeforeNextAttempt(t *testing.T) {
	testCases := []struct {
		name     string
		attempt  int
		expected time.Duration
	}{
		{
			name:     "attempt 0",
			attempt:  0,
			expected: time.Duration(0),
		},
		{
			name:     "attempt 1",
			attempt:  1,
			expected: time.Second * 60,
		},
		{
			name:     "attempt 2",
			attempt:  2,
			expected: time.Second * 120,
		},
	}

	for _, tc := range testCases {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			job := &UploadJob{}
			job.config.minUploadBackoff = time.Second * 60
			job.config.maxUploadBackoff = time.Second * 1800
			require.Equal(t, tc.expected, job.durationBeforeNextAttempt(int64(tc.attempt)))
		})
	}
}

func TestUploadJob_CanAppend(t *testing.T) {
	testCases := []struct {
		name           string
		sourceCategory string
		sourceJobRunID string // if not empty then it's an ETL source
		originalID     string // if not empty then it's a replay
		expected       bool
	}{
		{
			name:           "not a merge category",
			sourceCategory: "event-stream",
			sourceJobRunID: "",
			originalID:     "",
			expected:       true,
		},
		{
			name:           "cloud merge category",
			sourceCategory: "cloud",
			sourceJobRunID: "",
			originalID:     "",
			expected:       false,
		},
		{
			name:           "singer-protocol merge category",
			sourceCategory: "singer-protocol",
			sourceJobRunID: "",
			originalID:     "",
			expected:       false,
		},
		{
			name:           "etl source",
			sourceCategory: "event-stream",
			sourceJobRunID: "some-job-run-id",
			originalID:     "",
			expected:       false,
		},
		{
			name:           "replay",
			sourceCategory: "event-stream",
			sourceJobRunID: "",
			originalID:     "some-original-id",
			expected:       false,
		},
		{
			name:           "replay of etl source in merge category map",
			sourceCategory: "cloud",
			sourceJobRunID: "some-job-run-id",
			originalID:     "some-original-id",
			expected:       false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			uj := UploadJob{
				upload: model.Upload{
					SourceJobRunID: tc.sourceJobRunID,
				},
				warehouse: model.Warehouse{
					Source: backendconfig.SourceT{
						OriginalID: tc.originalID,
						SourceDefinition: backendconfig.SourceDefinitionT{
							Category: tc.sourceCategory,
						},
					},
				},
			}
			require.Equal(t, uj.CanAppend(), tc.expected)
		})
	}
}
