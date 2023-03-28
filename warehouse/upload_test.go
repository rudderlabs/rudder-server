package warehouse

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource"
	"github.com/rudderlabs/rudder-server/services/alerta"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/redshift"

	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
	"github.com/stretchr/testify/require"

	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/rudderlabs/rudder-go-kit/logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"
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
	Init()
	Init4()

	var (
		workspaceID     = "test-workspaceID"
		destinationID   = "test-desinationID"
		destinationName = "test-desinationName"
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
			name:             "Datalakes destination",
			destinationType:  warehouseutils.S3_DATALAKE,
			columnCountLimit: 1,
		},
		{
			name:            "Unknown destination",
			destinationType: "unknown-destination",
		},
		{
			name:             "Greater than threshold",
			destinationType:  "test-destination",
			columnCountLimit: 1,
			statExpected:     true,
		},
		{
			name:             "Lesser than threshold",
			destinationType:  "test-destination",
			columnCountLimit: 10,
			statExpected:     true,
		},
	}

	store := memstats.New()

	for _, tc := range inputs {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			columnCountLimitMap = map[string]int{
				"test-destination": tc.columnCountLimit,
			}

			j := UploadJob{
				upload: model.Upload{
					WorkspaceID:   workspaceID,
					DestinationID: destinationID,
					SourceID:      sourceID,
				},
				warehouse: model.Warehouse{
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
				stats: store,
				schemaHandle: &SchemaHandle{
					schemaInWarehouse: model.Schema{
						tableName: model.TableSchema{
							"test-column-1": "string",
							"test-column-2": "string",
							"test-column-3": "string",
						},
					},
				},
			}

			tags := stats.Tags{
				"module":      moduleName,
				"destType":    tc.destinationType,
				"warehouseID": j.warehouseID(),
				"workspaceId": workspaceID,
				"destID":      destinationID,
				"sourceID":    sourceID,
				"tableName":   tableName,
			}

			j.columnCountStat(tableName)

			m1 := store.Get("warehouse_load_table_column_count", tags)
			m2 := store.Get("warehouse_load_table_column_limit", tags)

			if tc.statExpected {
				require.EqualValues(t, m1.LastValue(), len(j.schemaHandle.schemaInWarehouse[tableName]))
				require.EqualValues(t, m2.LastValue(), tc.columnCountLimit)
			} else {
				require.Nil(t, m1)
				require.Nil(t, m2)
			}
		})
	}
}

var _ = Describe("Upload", Ordered, func() {
	var (
		sourceID        = "test-sourceID"
		destinationID   = "test-destinationID"
		destinationName = "test-destinationName"
		namespace       = "test-namespace"
		destinationType = "POSTGRES"
		g               = GinkgoT()
	)

	var (
		pgResource *resource.PostgresResource
		job        *UploadJob
	)

	BeforeAll(func() {
		pool, err := dockertest.NewPool("")
		Expect(err).To(BeNil())

		pgResource = setupWarehouseJobs(pool, GinkgoT())

		initWarehouse()

		err = setupDB(context.TODO(), getConnectionString())
		Expect(err).To(BeNil())

		sqlStatement, err := os.ReadFile("testdata/sql/upload_test.sql")
		Expect(err).To(BeNil())

		_, err = pgResource.DB.Exec(string(sqlStatement))
		Expect(err).To(BeNil())

		pkgLogger = logger.NOP
	})

	BeforeEach(func() {
		job = &UploadJob{
			warehouse: model.Warehouse{
				Type: destinationType,
				Destination: backendconfig.DestinationT{
					ID:   destinationID,
					Name: destinationName,
				},
				Source: backendconfig.SourceT{
					ID:   sourceID,
					Name: destinationName,
				},
			},
			upload: model.Upload{
				ID:                 1,
				DestinationID:      destinationID,
				SourceID:           sourceID,
				StagingFileStartID: 1,
				StagingFileEndID:   5,
				Namespace:          namespace,
			},
			stagingFileIDs: []int64{1, 2, 3, 4, 5},
			dbHandle:       pgResource.DB,
		}
	})

	It("Total rows in load files", func() {
		count := job.getTotalRowsInLoadFiles()
		Expect(count).To(BeEquivalentTo(5))
	})

	It("Total rows in staging files", func() {
		count, err := repo.NewStagingFiles(pgResource.DB).TotalEventsForUpload(context.TODO(), job.upload)
		Expect(err).To(BeNil())
		Expect(count).To(BeEquivalentTo(5))
	})

	It("Get uploads timings", func() {
		exportedData, err := time.Parse(time.RFC3339, "2020-04-21T15:26:34.344356Z")
		Expect(err).To(BeNil())
		exportingData, err := time.Parse(time.RFC3339, "2020-04-21T15:16:19.687716Z")
		Expect(err).To(BeNil())
		Expect(repo.NewUploads(job.dbHandle).UploadTimings(context.TODO(), job.upload.ID)).
			To(BeEquivalentTo(model.Timings{
				{
					"exported_data":  exportedData,
					"exporting_data": exportingData,
				},
			}))
	})

	Describe("Staging files and load files events match", func() {
		When("Matched", func() {
			It("Should not send stats", func() {
				job.matchRowsInStagingAndLoadFiles(context.TODO())
			})
		})

		When("Not matched", func() {
			It("Should send stats", func() {
				mockStats, mockMeasurement := getMockStats(g)
				mockStats.EXPECT().NewTaggedStat(gomock.Any(), gomock.Any(), gomock.Any()).Times(1).Return(mockMeasurement)
				mockMeasurement.EXPECT().Gauge(gomock.Any()).Times(1)

				job.stats = mockStats
				job.stagingFileIDs = []int64{1, 2}
				job.matchRowsInStagingAndLoadFiles(context.TODO())
			})
		})
	})
})

type mockAlertSender struct {
	mockError error
}

func (m *mockAlertSender) SendAlert(context.Context, string, alerta.SendAlertOpts) error {
	return m.mockError
}

func TestUploadJobT_UpdateTableSchema(t *testing.T) {
	Init()
	Init4()

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

					pgResource, err := resource.SetupPostgres(pool, t)
					require.NoError(t, err)

					rs := redshift.NewRedshift()
					redshift.WithConfig(rs, config.Default)

					rs.DB = pgResource.DB
					rs.Namespace = testNamespace

					job := &UploadJob{
						whManager: rs,
						upload: model.Upload{
							DestinationID:   testDestinationID,
							DestinationType: testDestinationType,
						},
						AlertSender: &mockAlertSender{
							mockError: tc.mockAlertError,
						},
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

			pgResource, err := resource.SetupPostgres(pool, t)
			require.NoError(t, err)

			rs := redshift.NewRedshift()
			redshift.WithConfig(rs, config.Default)

			rs.DB = pgResource.DB
			rs.Namespace = testNamespace

			job := &UploadJob{
				whManager: rs,
				upload: model.Upload{
					DestinationID:   testDestinationID,
					DestinationType: testDestinationType,
				},
				AlertSender: &mockAlertSender{},
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
				MinRetryAttempts: minAttempts,
				RetryTimeWindow:  minRetryWindow,
				Now:              func() time.Time { return now },
			}

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
	t.Run("repo error", func(t *testing.T) {
		t.Parallel()

		job := &UploadJob{
			upload: model.Upload{
				ID: 1,
			},
			pendingTableUploadsRepo: &mockPendingTablesRepo{
				err: errors.New("some error"),
			},
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
