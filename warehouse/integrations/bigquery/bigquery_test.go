package bigquery_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"golang.org/x/exp/slices"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/logger"
	mockuploader "github.com/rudderlabs/rudder-server/warehouse/internal/mocks/utils"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"

	"cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"

	"github.com/rudderlabs/compose-test/compose"
	"github.com/rudderlabs/compose-test/testcompose"
	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/runner"
	"github.com/rudderlabs/rudder-server/testhelper/health"
	"github.com/rudderlabs/rudder-server/testhelper/workspaceConfig"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	whbigquery "github.com/rudderlabs/rudder-server/warehouse/integrations/bigquery"
	bqHelper "github.com/rudderlabs/rudder-server/warehouse/integrations/bigquery/testhelper"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/testhelper"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/rudderlabs/rudder-server/warehouse/validations"
)

func TestIntegration(t *testing.T) {
	if os.Getenv("SLOW") != "1" {
		t.Skip("Skipping tests. Add 'SLOW=1' env var to run test.")
	}
	if !bqHelper.IsBQTestCredentialsAvailable() {
		t.Skipf("Skipping %s as %s is not set", t.Name(), bqHelper.TestKey)
	}

	c := testcompose.New(t, compose.FilePaths([]string{"../testdata/docker-compose.jobsdb.yml"}))
	c.Start(context.Background())

	misc.Init()
	validations.Init()
	warehouseutils.Init()

	jobsDBPort := c.Port("jobsDb", 5432)

	httpPort, err := kithelper.GetFreePort()
	require.NoError(t, err)

	workspaceID := warehouseutils.RandHex()
	sourceID := warehouseutils.RandHex()
	destinationID := warehouseutils.RandHex()
	writeKey := warehouseutils.RandHex()
	sourcesSourceID := warehouseutils.RandHex()
	sourcesDestinationID := warehouseutils.RandHex()
	sourcesWriteKey := warehouseutils.RandHex()

	destType := warehouseutils.BQ

	namespace := testhelper.RandSchema(destType)
	sourcesNamespace := testhelper.RandSchema(destType)

	bqTestCredentials, err := bqHelper.GetBQTestCredentials()
	require.NoError(t, err)

	escapedCredentials, err := json.Marshal(bqTestCredentials.Credentials)
	require.NoError(t, err)

	escapedCredentialsTrimmedStr := strings.Trim(string(escapedCredentials), `"`)

	templateConfigurations := map[string]any{
		"workspaceID":          workspaceID,
		"sourceID":             sourceID,
		"destinationID":        destinationID,
		"writeKey":             writeKey,
		"sourcesSourceID":      sourcesSourceID,
		"sourcesDestinationID": sourcesDestinationID,
		"sourcesWriteKey":      sourcesWriteKey,
		"namespace":            namespace,
		"project":              bqTestCredentials.ProjectID,
		"location":             bqTestCredentials.Location,
		"bucketName":           bqTestCredentials.BucketName,
		"credentials":          escapedCredentialsTrimmedStr,
		"sourcesNamespace":     sourcesNamespace,
	}
	workspaceConfigPath := workspaceConfig.CreateTempFile(t, "testdata/template.json", templateConfigurations)

	testhelper.EnhanceWithDefaultEnvs(t)
	t.Setenv("JOBS_DB_PORT", strconv.Itoa(jobsDBPort))
	t.Setenv("WAREHOUSE_JOBS_DB_PORT", strconv.Itoa(jobsDBPort))
	t.Setenv("RSERVER_WAREHOUSE_BIGQUERY_MAX_PARALLEL_LOADS", "8")
	t.Setenv("RSERVER_WAREHOUSE_BIGQUERY_ENABLE_DELETE_BY_JOBS", "true")
	t.Setenv("RSERVER_WAREHOUSE_WEB_PORT", strconv.Itoa(httpPort))
	t.Setenv("RSERVER_BACKEND_CONFIG_CONFIG_JSONPATH", workspaceConfigPath)
	t.Setenv("RSERVER_WAREHOUSE_BIGQUERY_SLOW_QUERY_THRESHOLD", "0s")

	svcDone := make(chan struct{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		r := runner.New(runner.ReleaseInfo{})
		_ = r.Run(ctx, []string{"bigquery-integration-test"})

		close(svcDone)
	}()
	t.Cleanup(func() { <-svcDone })

	serviceHealthEndpoint := fmt.Sprintf("http://localhost:%d/health", httpPort)
	health.WaitUntilReady(ctx, t, serviceHealthEndpoint, time.Minute, time.Second, "serviceHealthEndpoint")

	db, err := bigquery.NewClient(
		ctx,
		bqTestCredentials.ProjectID, option.WithCredentialsJSON([]byte(bqTestCredentials.Credentials)),
	)
	require.NoError(t, err)

	t.Run("Event flow", func(t *testing.T) {
		jobsDB := testhelper.JobsDB(t, jobsDBPort)

		t.Cleanup(func() {
			for _, dataset := range []string{namespace, sourcesNamespace} {
				require.Eventually(t, func() bool {
					if err := db.Dataset(dataset).DeleteWithContents(ctx); err != nil {
						t.Logf("error deleting dataset: %v", err)
						return false
					}
					return true
				},
					time.Minute,
					time.Second,
				)
			}
		})

		testcase := []struct {
			name                                string
			writeKey                            string
			schema                              string
			sourceID                            string
			destinationID                       string
			tables                              []string
			stagingFilesEventsMap               testhelper.EventsCountMap
			stagingFilesModifiedEventsMap       testhelper.EventsCountMap
			loadFilesEventsMap                  testhelper.EventsCountMap
			tableUploadsEventsMap               testhelper.EventsCountMap
			warehouseEventsMap                  testhelper.EventsCountMap
			asyncJob                            bool
			skipModifiedEvents                  bool
			prerequisite                        func(t testing.TB)
			isDedupEnabled                      bool
			customPartitionsEnabledWorkspaceIDs string
			stagingFilePrefix                   string
		}{
			{
				name:     "Merge mode",
				writeKey: writeKey,
				schema:   namespace,
				tables: []string{
					"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups",
				},
				sourceID:                      sourceID,
				destinationID:                 destinationID,
				stagingFilesEventsMap:         stagingFilesEventsMap(),
				stagingFilesModifiedEventsMap: stagingFilesEventsMap(),
				loadFilesEventsMap:            loadFilesEventsMap(),
				tableUploadsEventsMap:         tableUploadsEventsMap(),
				warehouseEventsMap:            mergeEventsMap(),
				isDedupEnabled:                true,
				prerequisite: func(t testing.TB) {
					t.Helper()

					_ = db.Dataset(namespace).DeleteWithContents(ctx)
				},
				stagingFilePrefix: "testdata/upload-job-merge-mode",
			},
			{
				name:          "Async Job",
				writeKey:      sourcesWriteKey,
				sourceID:      sourcesSourceID,
				destinationID: sourcesDestinationID,
				schema:        sourcesNamespace,
				tables:        []string{"tracks", "google_sheet"},
				stagingFilesEventsMap: testhelper.EventsCountMap{
					"wh_staging_files": 9, // 8 + 1 (merge events because of ID resolution)
				},
				stagingFilesModifiedEventsMap: testhelper.EventsCountMap{
					"wh_staging_files": 8, // 8 (de-duped by encounteredMergeRuleMap)
				},
				loadFilesEventsMap:    testhelper.SourcesLoadFilesEventsMap(),
				tableUploadsEventsMap: testhelper.SourcesTableUploadsEventsMap(),
				warehouseEventsMap:    testhelper.SourcesWarehouseEventsMap(),
				asyncJob:              true,
				isDedupEnabled:        false,
				prerequisite: func(t testing.TB) {
					t.Helper()

					_ = db.Dataset(namespace).DeleteWithContents(ctx)
				},
				stagingFilePrefix: "testdata/sources-job",
			},
			{
				name:   "Append mode",
				schema: namespace,
				tables: []string{
					"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups",
				},
				writeKey:                      writeKey,
				sourceID:                      sourceID,
				destinationID:                 destinationID,
				stagingFilesEventsMap:         stagingFilesEventsMap(),
				stagingFilesModifiedEventsMap: stagingFilesEventsMap(),
				loadFilesEventsMap:            loadFilesEventsMap(),
				tableUploadsEventsMap:         tableUploadsEventsMap(),
				warehouseEventsMap:            appendEventsMap(),
				skipModifiedEvents:            true,
				isDedupEnabled:                false,
				prerequisite: func(t testing.TB) {
					t.Helper()

					_ = db.Dataset(namespace).DeleteWithContents(ctx)
				},
				stagingFilePrefix: "testdata/upload-job-append-mode",
			},
			{
				name:   "Append mode with custom partition",
				schema: namespace,
				tables: []string{
					"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups",
				},
				writeKey:                            writeKey,
				sourceID:                            sourceID,
				destinationID:                       destinationID,
				stagingFilesEventsMap:               stagingFilesEventsMap(),
				stagingFilesModifiedEventsMap:       stagingFilesEventsMap(),
				loadFilesEventsMap:                  loadFilesEventsMap(),
				tableUploadsEventsMap:               tableUploadsEventsMap(),
				warehouseEventsMap:                  appendEventsMap(),
				skipModifiedEvents:                  true,
				isDedupEnabled:                      false,
				customPartitionsEnabledWorkspaceIDs: workspaceID,
				prerequisite: func(t testing.TB) {
					t.Helper()

					_ = db.Dataset(namespace).DeleteWithContents(ctx)

					err = db.Dataset(namespace).Create(context.Background(), &bigquery.DatasetMetadata{
						Location: "US",
					})
					require.NoError(t, err)

					err = db.Dataset(namespace).Table("tracks").Create(
						context.Background(),
						&bigquery.TableMetadata{
							Schema: []*bigquery.FieldSchema{{
								Name: "timestamp",
								Type: bigquery.TimestampFieldType,
							}},
							TimePartitioning: &bigquery.TimePartitioning{
								Field: "timestamp",
							},
						},
					)
					require.NoError(t, err)
				},
				stagingFilePrefix: "testdata/upload-job-append-mode-custom-partition",
			},
		}

		for _, tc := range testcase {
			tc := tc

			t.Run(tc.name, func(t *testing.T) {
				t.Setenv("RSERVER_WAREHOUSE_BIGQUERY_IS_DEDUP_ENABLED", strconv.FormatBool(tc.isDedupEnabled))
				t.Setenv("RSERVER_WAREHOUSE_BIGQUERY_CUSTOM_PARTITIONS_ENABLED_WORKSPACE_IDS", tc.customPartitionsEnabledWorkspaceIDs)

				if tc.prerequisite != nil {
					tc.prerequisite(t)
				}

				sqlClient := &client.Client{
					BQ:   db,
					Type: client.BQClient,
				}

				conf := map[string]interface{}{
					"bucketName":  bqTestCredentials.BucketName,
					"credentials": bqTestCredentials.Credentials,
				}

				t.Log("verifying test case 1")
				ts1 := testhelper.TestConfig{
					WriteKey:              tc.writeKey,
					Schema:                tc.schema,
					Tables:                tc.tables,
					SourceID:              tc.sourceID,
					DestinationID:         tc.destinationID,
					StagingFilesEventsMap: tc.stagingFilesEventsMap,
					LoadFilesEventsMap:    tc.loadFilesEventsMap,
					TableUploadsEventsMap: tc.tableUploadsEventsMap,
					WarehouseEventsMap:    tc.warehouseEventsMap,
					Config:                conf,
					WorkspaceID:           workspaceID,
					DestinationType:       destType,
					JobsDB:                jobsDB,
					HTTPPort:              httpPort,
					Client:                sqlClient,
					JobRunID:              misc.FastUUID().String(),
					TaskRunID:             misc.FastUUID().String(),
					StagingFilePath:       tc.stagingFilePrefix + ".staging-1.json",
					UserID:                testhelper.GetUserId(destType),
				}
				ts1.VerifyEvents(t)

				if tc.skipModifiedEvents {
					return
				}

				t.Log("verifying test case 2")
				ts2 := testhelper.TestConfig{
					WriteKey:              tc.writeKey,
					Schema:                tc.schema,
					Tables:                tc.tables,
					SourceID:              tc.sourceID,
					DestinationID:         tc.destinationID,
					StagingFilesEventsMap: tc.stagingFilesModifiedEventsMap,
					LoadFilesEventsMap:    tc.loadFilesEventsMap,
					TableUploadsEventsMap: tc.tableUploadsEventsMap,
					WarehouseEventsMap:    tc.warehouseEventsMap,
					AsyncJob:              tc.asyncJob,
					Config:                conf,
					WorkspaceID:           workspaceID,
					DestinationType:       destType,
					JobsDB:                jobsDB,
					HTTPPort:              httpPort,
					Client:                sqlClient,
					JobRunID:              misc.FastUUID().String(),
					TaskRunID:             misc.FastUUID().String(),
					StagingFilePath:       tc.stagingFilePrefix + ".staging-2.json",
					UserID:                testhelper.GetUserId(destType),
				}
				if tc.asyncJob {
					ts2.UserID = ts1.UserID
				}
				ts2.VerifyEvents(t)
			})
		}
	})

	t.Run("Validations", func(t *testing.T) {
		t.Cleanup(func() {
			require.Eventually(t, func() bool {
				if err := db.Dataset(namespace).DeleteWithContents(ctx); err != nil {
					t.Logf("error deleting dataset: %v", err)
					return false
				}
				return true
			},
				time.Minute,
				time.Second,
			)
		})

		dest := backendconfig.DestinationT{
			ID: destinationID,
			Config: map[string]interface{}{
				"project":       bqTestCredentials.ProjectID,
				"location":      bqTestCredentials.Location,
				"bucketName":    bqTestCredentials.BucketName,
				"credentials":   bqTestCredentials.Credentials,
				"prefix":        "",
				"namespace":     namespace,
				"syncFrequency": "30",
			},
			DestinationDefinition: backendconfig.DestinationDefinitionT{
				ID:          "1UmeD7xhVGHsPDEHoCiSPEGytS3",
				Name:        "BQ",
				DisplayName: "BigQuery",
			},
			Name:       "bigquery-integration",
			Enabled:    true,
			RevisionID: destinationID,
		}
		testhelper.VerifyConfigurationTest(t, dest)
	})

	t.Run("Load Table", func(t *testing.T) {
		const (
			sourceID        = "test_source_id"
			destinationID   = "test_destination_id"
			workspaceID     = "test_workspace_id"
			destinationType = warehouseutils.BQ
		)

		namespace := testhelper.RandSchema(destinationType)

		credentials, err := bqHelper.GetBQTestCredentials()
		require.NoError(t, err)

		t.Cleanup(func() {
			require.Eventually(t, func() bool {
				if err := db.Dataset(namespace).DeleteWithContents(ctx); err != nil {
					t.Logf("error deleting dataset: %v", err)
					return false
				}
				return true
			},
				time.Minute,
				time.Second,
			)
		})

		warehouseModel := func(namespace string) model.Warehouse {
			return model.Warehouse{
				Source: backendconfig.SourceT{
					ID: sourceID,
				},
				Destination: backendconfig.DestinationT{
					ID: destinationID,
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						Name: destinationType,
					},
					Config: map[string]any{
						"project":     credentials.ProjectID,
						"location":    credentials.Location,
						"bucketName":  credentials.BucketName,
						"credentials": credentials.Credentials,
						"namespace":   namespace,
					},
				},
				WorkspaceID: workspaceID,
				Namespace:   namespace,
			}
		}

		schemaInUpload := model.TableSchema{
			"test_bool":     "boolean",
			"test_datetime": "datetime",
			"test_float":    "float",
			"test_int":      "int",
			"test_string":   "string",
			"id":            "string",
			"received_at":   "datetime",
		}
		schemaInWarehouse := model.TableSchema{
			"test_bool":           "boolean",
			"test_datetime":       "datetime",
			"test_float":          "float",
			"test_int":            "int",
			"test_string":         "string",
			"id":                  "string",
			"received_at":         "datetime",
			"extra_test_bool":     "boolean",
			"extra_test_datetime": "datetime",
			"extra_test_float":    "float",
			"extra_test_int":      "int",
			"extra_test_string":   "string",
		}

		fm, err := filemanager.New(&filemanager.Settings{
			Provider: warehouseutils.GCS,
			Config: map[string]any{
				"project":     credentials.ProjectID,
				"location":    credentials.Location,
				"bucketName":  credentials.BucketName,
				"credentials": credentials.Credentials,
			},
		})
		require.NoError(t, err)

		uploader := func(
			t testing.TB,
			loadFiles []warehouseutils.LoadFile,
			tableName string,
			schemaInUpload model.TableSchema,
			schemaInWarehouse model.TableSchema,
		) warehouseutils.Uploader {
			ctrl := gomock.NewController(t)
			t.Cleanup(ctrl.Finish)

			mockUploader := mockuploader.NewMockUploader(ctrl)
			mockUploader.EXPECT().UseRudderStorage().Return(false).AnyTimes()
			mockUploader.EXPECT().GetLoadFilesMetadata(gomock.Any(), gomock.Any()).DoAndReturn(
				func(ctx context.Context, options warehouseutils.GetLoadFilesOptions) []warehouseutils.LoadFile {
					return slices.Clone(loadFiles)
				},
			).AnyTimes()
			mockUploader.EXPECT().GetTableSchemaInUpload(tableName).Return(schemaInUpload).AnyTimes()
			mockUploader.EXPECT().GetTableSchemaInWarehouse(tableName).Return(schemaInWarehouse).AnyTimes()

			return mockUploader
		}

		t.Run("schema does not exists", func(t *testing.T) {
			tableName := "schema_not_exists_test_table"

			uploadOutput := testhelper.UploadLoadFile(t, fm, "../testdata/load.json.gz", tableName)

			loadFiles := []warehouseutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := uploader(
				t, loadFiles, tableName, schemaInUpload,
				schemaInWarehouse,
			)

			warehouse := warehouseModel(namespace)

			bq := whbigquery.New(config.Default, logger.NOP)
			err := bq.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			loadTableStat, err := bq.LoadTable(ctx, tableName)
			require.ErrorContains(t, err, "googleapi: Error 404: Not found: Dataset "+credentials.ProjectID+":"+namespace+", notFound")
			require.Nil(t, loadTableStat)
		})
		t.Run("table does not exists", func(t *testing.T) {
			tableName := "table_not_exists_test_table"

			uploadOutput := testhelper.UploadLoadFile(t, fm, "../testdata/load.json.gz", tableName)

			loadFiles := []warehouseutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := uploader(
				t, loadFiles, tableName, schemaInUpload,
				schemaInWarehouse,
			)

			warehouse := warehouseModel(namespace)

			bq := whbigquery.New(config.Default, logger.NOP)
			err := bq.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = bq.CreateSchema(ctx)
			require.NoError(t, err)

			loadTableStat, err := bq.LoadTable(ctx, tableName)
			require.ErrorContains(t, err, "Partitioning specification must be provided in order to create partitioned table")
			require.Nil(t, loadTableStat)
		})
		t.Run("load table stats", func(t *testing.T) {
			tableName := "load_table_stats_test_table"

			uploadOutput := testhelper.UploadLoadFile(t, fm, "../testdata/load.json.gz", tableName)

			loadFiles := []warehouseutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := uploader(
				t, loadFiles, tableName, schemaInUpload,
				schemaInWarehouse,
			)

			warehouse := warehouseModel(namespace)

			c := config.New()
			c.Set("Warehouse.bigquery.isDedupEnabled", true)

			bq := whbigquery.New(c, logger.NOP)
			err := bq.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = bq.CreateSchema(ctx)
			require.NoError(t, err)

			err = bq.CreateTable(ctx, tableName, schemaInWarehouse)
			require.NoError(t, err)

			loadTableStat, err := bq.LoadTable(ctx, tableName)
			require.NoError(t, err)
			require.Equal(t, loadTableStat.RowsInserted, int64(14))
			require.Equal(t, loadTableStat.RowsUpdated, int64(0))

			loadTableStat, err = bq.LoadTable(ctx, tableName)
			require.NoError(t, err)
			require.Equal(t, loadTableStat.RowsInserted, int64(0))
			require.Equal(t, loadTableStat.RowsUpdated, int64(14))

			records := bqHelper.RetrieveRecordsFromWarehouse(t, db,
				fmt.Sprintf(`
				SELECT
				  id,
				  received_at,
				  test_bool,
				  test_datetime,
				  test_float,
				  test_int,
				  test_string
				FROM
				  %s
				ORDER BY
				  id`,
					fmt.Sprintf("`%s`.`%s`", namespace, tableName),
				),
			)
			require.Equal(t, records, testhelper.SampleTestRecords())
		})
		t.Run("append", func(t *testing.T) {
			tableName := "append_test_table"

			uploadOutput := testhelper.UploadLoadFile(t, fm, "../testdata/load.json.gz", tableName)

			loadFiles := []warehouseutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := uploader(
				t, loadFiles, tableName, schemaInUpload,
				schemaInWarehouse,
			)

			warehouse := warehouseModel(namespace)

			bq := whbigquery.New(config.Default, logger.NOP)
			err := bq.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = bq.CreateSchema(ctx)
			require.NoError(t, err)

			err = bq.CreateTable(ctx, tableName, schemaInWarehouse)
			require.NoError(t, err)

			loadTableStat, err := bq.LoadTable(ctx, tableName)
			require.NoError(t, err)
			require.Equal(t, loadTableStat.RowsInserted, int64(14))
			require.Equal(t, loadTableStat.RowsUpdated, int64(0))

			loadTableStat, err = bq.LoadTable(ctx, tableName)
			require.NoError(t, err)
			require.Equal(t, loadTableStat.RowsInserted, int64(14))
			require.Equal(t, loadTableStat.RowsUpdated, int64(0))

			records := bqHelper.RetrieveRecordsFromWarehouse(t, db,
				fmt.Sprintf(`
				SELECT
				  id,
				  received_at,
				  test_bool,
				  test_datetime,
				  test_float,
				  test_int,
				  test_string
				FROM
				  %s.%s
				WHERE
				  _PARTITIONTIME BETWEEN TIMESTAMP('%s') AND TIMESTAMP('%s')
				ORDER BY
				  id`,
					namespace,
					tableName,
					time.Now().Add(-24*time.Hour).Format("2006-01-02"),
					time.Now().Add(+24*time.Hour).Format("2006-01-02"),
				),
			)
			require.Equal(t, records, testhelper.AppendTestRecords())
		})
		t.Run("load file does not exists", func(t *testing.T) {
			tableName := "load_file_not_exists_test_table"

			loadFiles := []warehouseutils.LoadFile{{
				Location: "https://storage.googleapis.com/project/rudder-warehouse-load-objects/load_file_not_exists_test_table/test_source_id/2e04b6bd-8007-461e-a338-91224a8b7d3d-load_file_not_exists_test_table/load.json.gz",
			}}
			mockUploader := uploader(
				t, loadFiles, tableName, schemaInUpload,
				schemaInWarehouse,
			)

			warehouse := warehouseModel(namespace)

			bq := whbigquery.New(config.Default, logger.NOP)
			err := bq.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = bq.CreateSchema(ctx)
			require.NoError(t, err)

			err = bq.CreateTable(ctx, tableName, schemaInWarehouse)
			require.NoError(t, err)

			loadTableStat, err := bq.LoadTable(ctx, tableName)
			require.ErrorContains(t, err, "Access Denied: File gs://project/rudder-warehouse-load-objects/load_file_not_exists_test_table/test_source_id/2e04b6bd-8007-461e-a338-91224a8b7d3d-load_file_not_exists_test_table/load.json.gz")
			require.Nil(t, loadTableStat)
		})
		t.Run("mismatch in number of columns", func(t *testing.T) {
			tableName := "mismatch_columns_test_table"

			uploadOutput := testhelper.UploadLoadFile(t, fm, "../testdata/mismatch-columns.json.gz", tableName)

			loadFiles := []warehouseutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := uploader(
				t, loadFiles, tableName, schemaInUpload,
				schemaInWarehouse,
			)

			warehouse := warehouseModel(namespace)

			bq := whbigquery.New(config.Default, logger.NOP)
			err := bq.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = bq.CreateSchema(ctx)
			require.NoError(t, err)

			err = bq.CreateTable(ctx, tableName, schemaInWarehouse)
			require.NoError(t, err)

			loadTableStat, err := bq.LoadTable(ctx, tableName)
			require.ErrorContains(t, err, "JSON table encountered too many errors")
			require.Nil(t, loadTableStat)
		})
		t.Run("mismatch in schema", func(t *testing.T) {
			tableName := "mismatch_schema_test_table"

			uploadOutput := testhelper.UploadLoadFile(t, fm, "../testdata/mismatch-schema.json.gz", tableName)

			loadFiles := []warehouseutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := uploader(
				t, loadFiles, tableName, schemaInUpload,
				schemaInWarehouse,
			)

			warehouse := warehouseModel(namespace)

			bq := whbigquery.New(config.Default, logger.NOP)
			err := bq.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = bq.CreateSchema(ctx)
			require.NoError(t, err)

			err = bq.CreateTable(ctx, tableName, schemaInWarehouse)
			require.NoError(t, err)

			loadTableStat, err := bq.LoadTable(ctx, tableName)
			require.ErrorContains(t, err, "JSON table encountered too many errors")
			require.Nil(t, loadTableStat)
		})
		t.Run("discards", func(t *testing.T) {
			tableName := warehouseutils.DiscardsTable

			uploadOutput := testhelper.UploadLoadFile(t, fm, "../testdata/discards.json.gz", tableName)

			loadFiles := []warehouseutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := uploader(
				t, loadFiles, tableName, warehouseutils.DiscardsSchema,
				warehouseutils.DiscardsSchema,
			)

			warehouse := warehouseModel(namespace)

			bq := whbigquery.New(config.Default, logger.NOP)
			err := bq.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = bq.CreateSchema(ctx)
			require.NoError(t, err)

			err = bq.CreateTable(ctx, tableName, warehouseutils.DiscardsSchema)
			require.NoError(t, err)

			loadTableStat, err := bq.LoadTable(ctx, tableName)
			require.NoError(t, err)
			require.Equal(t, loadTableStat.RowsInserted, int64(6))
			require.Equal(t, loadTableStat.RowsUpdated, int64(0))

			records := bqHelper.RetrieveRecordsFromWarehouse(t, db,
				fmt.Sprintf(`
				SELECT
				  column_name,
				  column_value,
				  received_at,
				  row_id,
				  table_name,
				  uuid_ts
				FROM
				  %s
				ORDER BY row_id ASC;`,
					fmt.Sprintf("`%s`.`%s`", namespace, tableName),
				),
			)
			require.Equal(t, records, testhelper.DiscardTestRecords())
		})
		t.Run("custom partition", func(t *testing.T) {
			tableName := "partition_test_table"

			uploadOutput := testhelper.UploadLoadFile(t, fm, "../testdata/load.json.gz", tableName)

			loadFiles := []warehouseutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := uploader(
				t, loadFiles, tableName, schemaInUpload,
				schemaInWarehouse,
			)

			warehouse := warehouseModel(namespace)

			c := config.New()
			c.Set("Warehouse.bigquery.customPartitionsEnabled", true)
			c.Set("Warehouse.bigquery.customPartitionsEnabledWorkspaceIDs", []string{workspaceID})

			bq := whbigquery.New(c, logger.NOP)
			err := bq.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = bq.CreateSchema(ctx)
			require.NoError(t, err)

			err = bq.CreateTable(ctx, tableName, schemaInWarehouse)
			require.NoError(t, err)

			loadTableStat, err := bq.LoadTable(ctx, tableName)
			require.NoError(t, err)
			require.Equal(t, loadTableStat.RowsInserted, int64(14))
			require.Equal(t, loadTableStat.RowsUpdated, int64(0))

			records := bqHelper.RetrieveRecordsFromWarehouse(t, db,
				fmt.Sprintf(`
				SELECT
				  id,
				  received_at,
				  test_bool,
				  test_datetime,
				  test_float,
				  test_int,
				  test_string
				FROM
				  %s.%s
				WHERE
				  _PARTITIONTIME BETWEEN TIMESTAMP('%s') AND TIMESTAMP('%s')
				ORDER BY
				  id`,
					namespace,
					tableName,
					time.Now().Add(-24*time.Hour).Format("2006-01-02"),
					time.Now().Add(+24*time.Hour).Format("2006-01-02"),
				),
			)
			require.Equal(t, records, testhelper.SampleTestRecords())
		})
	})

	t.Run("IsEmpty", func(t *testing.T) {
		namespace := testhelper.RandSchema(warehouseutils.BQ)

		credentials, err := bqHelper.GetBQTestCredentials()
		require.NoError(t, err)

		t.Cleanup(func() {
			require.Eventually(t, func() bool {
				if err := db.Dataset(namespace).DeleteWithContents(ctx); err != nil {
					t.Logf("error deleting dataset: %v", err)
					return false
				}
				return true
			},
				time.Minute,
				time.Second,
			)
		})

		ctx := context.Background()

		warehouse := model.Warehouse{
			Source: backendconfig.SourceT{
				ID: sourceID,
			},
			Destination: backendconfig.DestinationT{
				ID: destinationID,
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Name: warehouseutils.BQ,
				},
				Config: map[string]any{
					"project":     credentials.ProjectID,
					"location":    credentials.Location,
					"bucketName":  credentials.BucketName,
					"credentials": credentials.Credentials,
					"namespace":   namespace,
				},
			},
			WorkspaceID: workspaceID,
			Namespace:   namespace,
		}

		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)

		mockUploader := mockuploader.NewMockUploader(ctrl)
		mockUploader.EXPECT().UseRudderStorage().Return(false).AnyTimes()

		insertRecords := func(t testing.TB, tableName string) {
			t.Helper()

			query := db.Query(`
				INSERT INTO ` + tableName + ` (
				  id, received_at, test_bool, test_datetime,
				  test_float, test_int, test_string
				)
				VALUES
				  (
					'1', '2020-01-01 00:00:00', true,
					'2020-01-01 00:00:00', 1.1, 1, 'test'
				  );`,
			)
			job, err := query.Run(ctx)
			require.NoError(t, err)

			status, err := job.Wait(ctx)
			require.NoError(t, err)
			require.Nil(t, status.Err())
		}

		t.Run("tables doesn't exists", func(t *testing.T) {
			bq := whbigquery.New(config.Default, logger.NOP)
			err := bq.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			isEmpty, err := bq.IsEmpty(ctx, warehouse)
			require.NoError(t, err)
			require.True(t, isEmpty)
		})
		t.Run("tables empty", func(t *testing.T) {
			bq := whbigquery.New(config.Default, logger.NOP)
			err := bq.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = bq.CreateSchema(ctx)
			require.NoError(t, err)

			tables := []string{"pages", "screens"}
			for _, table := range tables {
				err = bq.CreateTable(ctx, table, model.TableSchema{
					"test_bool":     "boolean",
					"test_datetime": "datetime",
					"test_float":    "float",
					"test_int":      "int",
					"test_string":   "string",
					"id":            "string",
					"received_at":   "datetime",
				})
				require.NoError(t, err)
			}

			isEmpty, err := bq.IsEmpty(ctx, warehouse)
			require.NoError(t, err)
			require.True(t, isEmpty)
		})
		t.Run("tables not empty", func(t *testing.T) {
			bq := whbigquery.New(config.Default, logger.NOP)
			err := bq.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			insertRecords(t, fmt.Sprintf("`%s`.`%s`", namespace, "pages"))
			insertRecords(t, fmt.Sprintf("`%s`.`%s`", namespace, "screens"))

			isEmpty, err := bq.IsEmpty(ctx, warehouse)
			require.NoError(t, err)
			require.False(t, isEmpty)
		})
	})
}

func loadFilesEventsMap() testhelper.EventsCountMap {
	return testhelper.EventsCountMap{
		"identifies":    4,
		"users":         4,
		"tracks":        4,
		"product_track": 4,
		"pages":         4,
		"screens":       4,
		"aliases":       4,
		"groups":        1,
		"_groups":       3,
	}
}

func tableUploadsEventsMap() testhelper.EventsCountMap {
	return testhelper.EventsCountMap{
		"identifies":    4,
		"users":         4,
		"tracks":        4,
		"product_track": 4,
		"pages":         4,
		"screens":       4,
		"aliases":       4,
		"groups":        1,
		"_groups":       3,
	}
}

func stagingFilesEventsMap() testhelper.EventsCountMap {
	return testhelper.EventsCountMap{
		"wh_staging_files": 34, // Since extra 2 merge events because of ID resolution
	}
}

func mergeEventsMap() testhelper.EventsCountMap {
	return testhelper.EventsCountMap{
		"identifies":    1,
		"users":         1,
		"tracks":        1,
		"product_track": 1,
		"pages":         1,
		"screens":       1,
		"aliases":       1,
		"groups":        1,
		"_groups":       1,
	}
}

func appendEventsMap() testhelper.EventsCountMap {
	return testhelper.EventsCountMap{
		"identifies":    4,
		"users":         1,
		"tracks":        4,
		"product_track": 4,
		"pages":         4,
		"screens":       4,
		"aliases":       4,
		"groups":        1,
		"_groups":       3,
	}
}

func TestUnsupportedCredentials(t *testing.T) {
	credentials := whbigquery.BQCredentials{
		ProjectID:   "projectId",
		Credentials: "{\"installed\":{\"client_id\":\"1234.apps.googleusercontent.com\",\"project_id\":\"project_id\",\"auth_uri\":\"https://accounts.google.com/o/oauth2/auth\",\"token_uri\":\"https://oauth2.googleapis.com/token\",\"auth_provider_x509_cert_url\":\"https://www.googleapis.com/oauth2/v1/certs\",\"client_secret\":\"client_secret\",\"redirect_uris\":[\"urn:ietf:wg:oauth:2.0:oob\",\"http://localhost\"]}}",
	}

	_, err := whbigquery.Connect(context.Background(), &credentials)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "client_credentials.json file is not supported")
}
