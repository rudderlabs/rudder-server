package deltalake_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"

	"go.uber.org/mock/gomock"

	dbsql "github.com/databricks/databricks-sql-go"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/compose-test/compose"
	"github.com/rudderlabs/compose-test/testcompose"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/logger"
	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	th "github.com/rudderlabs/rudder-server/testhelper"
	"github.com/rudderlabs/rudder-server/testhelper/backendconfigtest"
	"github.com/rudderlabs/rudder-server/utils/misc"
	warehouseclient "github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/deltalake"
	whth "github.com/rudderlabs/rudder-server/warehouse/integrations/testhelper"
	mockuploader "github.com/rudderlabs/rudder-server/warehouse/internal/mocks/utils"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/rudderlabs/rudder-server/warehouse/validations"
)

type testCredentials struct {
	Host          string `json:"host"`
	Port          string `json:"port"`
	Path          string `json:"path"`
	Token         string `json:"token"`
	AccountName   string `json:"accountName"`
	AccountKey    string `json:"accountKey"`
	ContainerName string `json:"containerName"`
}

const testKey = "DATABRICKS_INTEGRATION_TEST_CREDENTIALS"

func deltaLakeTestCredentials() (*testCredentials, error) {
	cred, exists := os.LookupEnv(testKey)
	if !exists {
		return nil, errors.New("deltaLake test credentials not found")
	}

	var credentials testCredentials
	err := json.Unmarshal([]byte(cred), &credentials)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal deltaLake test credentials: %w", err)
	}
	return &credentials, nil
}

func TestIntegration(t *testing.T) {
	if os.Getenv("SLOW") != "1" {
		t.Skip("Skipping tests. Add 'SLOW=1' env var to run test.")
	}
	if _, exists := os.LookupEnv(testKey); !exists {
		if os.Getenv("FORCE_RUN_INTEGRATION_TESTS") == "true" {
			t.Fatalf("%s environment variable not set", testKey)
		}
		t.Skipf("Skipping %s as %s is not set", t.Name(), testKey)
	}

	misc.Init()
	validations.Init()
	whutils.Init()

	destType := whutils.DELTALAKE

	credentials, err := deltaLakeTestCredentials()
	require.NoError(t, err)

	t.Run("Event flow", func(t *testing.T) {
		httpPort, err := kithelper.GetFreePort()
		require.NoError(t, err)

		c := testcompose.New(t, compose.FilePaths([]string{"../testdata/docker-compose.jobsdb.yml"}))
		c.Start(context.Background())

		workspaceID := whutils.RandHex()
		jobsDBPort := c.Port("jobsDb", 5432)

		jobsDB := whth.JobsDB(t, jobsDBPort)

		testCases := []struct {
			name                string
			messageID           string
			warehouseEventsMap  whth.EventsCountMap
			useParquetLoadFiles bool
			stagingFilePrefix   string
			jobRunID            string
			configOverride      map[string]any
		}{
			{
				name:                "Merge Mode",
				warehouseEventsMap:  mergeEventsMap(),
				useParquetLoadFiles: false,
				stagingFilePrefix:   "testdata/upload-job-merge-mode",
				jobRunID:            misc.FastUUID().String(),
				configOverride: map[string]any{
					"preferAppend": false,
				},
			},
			{
				name:                "Append Mode",
				warehouseEventsMap:  appendEventsMap(),
				useParquetLoadFiles: false,
				stagingFilePrefix:   "testdata/upload-job-append-mode",
				// an empty jobRunID means that the source is not an ETL one
				// see Uploader.CanAppend()
				jobRunID: "",
				configOverride: map[string]any{
					"preferAppend": true,
				},
			},
			{
				name:                "Undefined preferAppend",
				warehouseEventsMap:  mergeEventsMap(),
				useParquetLoadFiles: false,
				stagingFilePrefix:   "testdata/upload-job-undefined-preferAppend-mode",
				jobRunID:            misc.FastUUID().String(),
			},
			{
				name:                "Parquet load files",
				warehouseEventsMap:  mergeEventsMap(),
				useParquetLoadFiles: true,
				stagingFilePrefix:   "testdata/upload-job-parquet",
				jobRunID:            misc.FastUUID().String(),
				configOverride: map[string]any{
					"preferAppend": false,
				},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				var (
					sourceID      = whutils.RandHex()
					destinationID = whutils.RandHex()
					writeKey      = whutils.RandHex()
					namespace     = whth.RandSchema(destType)
				)

				destinationBuilder := backendconfigtest.NewDestinationBuilder(destType).
					WithID(destinationID).
					WithRevisionID(destinationID).
					WithConfigOption("host", credentials.Host).
					WithConfigOption("port", credentials.Port).
					WithConfigOption("path", credentials.Path).
					WithConfigOption("token", credentials.Token).
					WithConfigOption("namespace", namespace).
					WithConfigOption("bucketProvider", "AZURE_BLOB").
					WithConfigOption("containerName", credentials.ContainerName).
					WithConfigOption("useSTSTokens", false).
					WithConfigOption("enableSSE", false).
					WithConfigOption("accountName", credentials.AccountName).
					WithConfigOption("accountKey", credentials.AccountKey).
					WithConfigOption("syncFrequency", "30")
				for k, v := range tc.configOverride {
					destinationBuilder = destinationBuilder.WithConfigOption(k, v)
				}

				workspaceConfig := backendconfigtest.NewConfigBuilder().
					WithSource(
						backendconfigtest.NewSourceBuilder().
							WithID(sourceID).
							WithWriteKey(writeKey).
							WithWorkspaceID(workspaceID).
							WithConnection(destinationBuilder.Build()).
							Build(),
					).
					WithWorkspaceID(workspaceID).
					Build()

				t.Setenv("RSERVER_WAREHOUSE_DELTALAKE_MAX_PARALLEL_LOADS", "8")
				t.Setenv("RSERVER_WAREHOUSE_DELTALAKE_SLOW_QUERY_THRESHOLD", "0s")
				t.Setenv("RSERVER_WAREHOUSE_DELTALAKE_USE_PARQUET_LOAD_FILES", strconv.FormatBool(tc.useParquetLoadFiles))

				whth.BootstrapSvc(t, workspaceConfig, httpPort, jobsDBPort)

				port, err := strconv.Atoi(credentials.Port)
				require.NoError(t, err)

				connector, err := dbsql.NewConnector(
					dbsql.WithServerHostname(credentials.Host),
					dbsql.WithPort(port),
					dbsql.WithHTTPPath(credentials.Path),
					dbsql.WithAccessToken(credentials.Token),
					dbsql.WithSessionParams(map[string]string{
						"ansi_mode": "false",
					}),
				)
				require.NoError(t, err)

				db := sql.OpenDB(connector)
				require.NoError(t, db.Ping())
				t.Cleanup(func() { _ = db.Close() })
				t.Cleanup(func() {
					dropSchema(t, db, namespace)
				})

				sqlClient := &warehouseclient.Client{
					SQL:  db,
					Type: warehouseclient.SQLClient,
				}

				conf := map[string]interface{}{
					"bucketProvider": "AZURE_BLOB",
					"containerName":  credentials.ContainerName,
					"prefix":         "",
					"useSTSTokens":   false,
					"enableSSE":      false,
					"accountName":    credentials.AccountName,
					"accountKey":     credentials.AccountKey,
				}
				tables := []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"}

				t.Log("verifying test case 1")
				ts1 := whth.TestConfig{
					WriteKey:      writeKey,
					Schema:        namespace,
					Tables:        tables,
					SourceID:      sourceID,
					DestinationID: destinationID,
					JobRunID:      tc.jobRunID,
					WarehouseEventsMap: whth.EventsCountMap{
						"identifies":    1,
						"users":         1,
						"tracks":        1,
						"product_track": 1,
						"pages":         1,
						"screens":       1,
						"aliases":       1,
						"groups":        1,
					},
					Config:          conf,
					WorkspaceID:     workspaceID,
					DestinationType: destType,
					JobsDB:          jobsDB,
					HTTPPort:        httpPort,
					Client:          sqlClient,
					StagingFilePath: tc.stagingFilePrefix + ".staging-1.json",
					UserID:          whth.GetUserId(destType),
				}
				ts1.VerifyEvents(t)

				t.Log("verifying test case 2")
				ts2 := whth.TestConfig{
					WriteKey:           writeKey,
					Schema:             namespace,
					Tables:             tables,
					SourceID:           sourceID,
					DestinationID:      destinationID,
					JobRunID:           tc.jobRunID,
					WarehouseEventsMap: tc.warehouseEventsMap,
					Config:             conf,
					WorkspaceID:        workspaceID,
					DestinationType:    destType,
					JobsDB:             jobsDB,
					HTTPPort:           httpPort,
					Client:             sqlClient,
					StagingFilePath:    tc.stagingFilePrefix + ".staging-2.json",
					UserID:             ts1.UserID,
				}
				ts2.VerifyEvents(t)
			})
		}
	})

	t.Run("Validation", func(t *testing.T) {
		namespace := whth.RandSchema(destType)

		port, err := strconv.Atoi(credentials.Port)
		require.NoError(t, err)

		connector, err := dbsql.NewConnector(
			dbsql.WithServerHostname(credentials.Host),
			dbsql.WithPort(port),
			dbsql.WithHTTPPath(credentials.Path),
			dbsql.WithAccessToken(credentials.Token),
			dbsql.WithSessionParams(map[string]string{
				"ansi_mode": "false",
			}),
		)
		require.NoError(t, err)

		db := sql.OpenDB(connector)
		require.NoError(t, db.Ping())
		t.Cleanup(func() { _ = db.Close() })
		t.Cleanup(func() {
			dropSchema(t, db, namespace)
		})

		dest := backendconfig.DestinationT{
			ID: "test_destination_id",
			Config: map[string]interface{}{
				"host":            credentials.Host,
				"port":            credentials.Port,
				"path":            credentials.Path,
				"token":           credentials.Token,
				"namespace":       namespace,
				"bucketProvider":  "AZURE_BLOB",
				"containerName":   credentials.ContainerName,
				"prefix":          "",
				"useSTSTokens":    false,
				"enableSSE":       false,
				"accountName":     credentials.AccountName,
				"accountKey":      credentials.AccountKey,
				"syncFrequency":   "30",
				"eventDelivery":   false,
				"eventDeliveryTS": 1648195480174,
			},
			DestinationDefinition: backendconfig.DestinationDefinitionT{
				ID:          "23HLpnDJnIg7DsBvDWGU6DQzFEo",
				Name:        "DELTALAKE",
				DisplayName: "Databricks (Delta Lake)",
			},
			Name:       "deltalake-demo",
			Enabled:    true,
			RevisionID: "39eClxJQQlaWzMWyqnQctFDP5T2",
		}

		testCases := []struct {
			name                string
			useParquetLoadFiles bool
			conf                map[string]interface{}
		}{
			{
				name:                "Parquet load files",
				useParquetLoadFiles: true,
			},
			{
				name:                "CSV load files",
				useParquetLoadFiles: false,
			},
			{
				name:                "External location",
				useParquetLoadFiles: true,
				conf: map[string]interface{}{
					"enableExternalLocation": true,
					"externalLocation":       "/path/to/delta/table",
				},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				t.Setenv(
					"RSERVER_WAREHOUSE_DELTALAKE_USE_PARQUET_LOAD_FILES",
					strconv.FormatBool(tc.useParquetLoadFiles),
				)

				for k, v := range tc.conf {
					dest.Config[k] = v
				}

				whth.VerifyConfigurationTest(t, dest)
			})
		}
	})

	t.Run("Load Table", func(t *testing.T) {
		ctx := context.Background()
		namespace := whth.RandSchema(destType)

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

		warehouse := model.Warehouse{
			Source: backendconfig.SourceT{
				ID: "test_source_id",
			},
			Destination: backendconfig.DestinationT{
				ID: "test_destination_id",
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Name: destType,
				},
				Config: map[string]any{
					"host":           credentials.Host,
					"port":           credentials.Port,
					"path":           credentials.Path,
					"token":          credentials.Token,
					"namespace":      namespace,
					"bucketProvider": whutils.AzureBlob,
					"containerName":  credentials.ContainerName,
					"accountName":    credentials.AccountName,
					"accountKey":     credentials.AccountKey,
				},
			},
			WorkspaceID: "test_workspace_id",
			Namespace:   namespace,
		}

		fm, err := filemanager.New(&filemanager.Settings{
			Provider: whutils.AzureBlob,
			Config: map[string]any{
				"containerName":  credentials.ContainerName,
				"accountName":    credentials.AccountName,
				"accountKey":     credentials.AccountKey,
				"bucketProvider": whutils.AzureBlob,
			},
		})
		require.NoError(t, err)

		t.Run("schema does not exists", func(t *testing.T) {
			tableName := "schema_not_exists_test_table"

			uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/load.csv.gz", tableName)

			loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, whutils.LoadFileTypeCsv, false, false, "2022-12-15T06:53:49.640Z")

			d := deltalake.New(config.New(), logger.NOP, stats.NOP)
			err := d.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			loadTableStat, err := d.LoadTable(ctx, tableName)
			require.Error(t, err)
			require.Nil(t, loadTableStat)
		})
		t.Run("table does not exists", func(t *testing.T) {
			tableName := "table_not_exists_test_table"

			uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/load.csv.gz", tableName)

			loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, whutils.LoadFileTypeCsv, false, false, "2022-12-15T06:53:49.640Z")

			d := deltalake.New(config.New(), logger.NOP, stats.NOP)
			err := d.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = d.CreateSchema(ctx)
			require.NoError(t, err)
			t.Cleanup(func() {
				dropSchema(t, d.DB.DB, namespace)
			})

			loadTableStat, err := d.LoadTable(ctx, tableName)
			require.Error(t, err)
			require.Nil(t, loadTableStat)
		})
		t.Run("merge", func(t *testing.T) {
			t.Run("without dedup", func(t *testing.T) {
				tableName := "merge_without_dedup_test_table"
				uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/load.csv.gz", tableName)

				loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
				mockUploader := newMockUploader(
					t, loadFiles, tableName, schemaInUpload, schemaInWarehouse,
					whutils.LoadFileTypeCsv, false, false, "2022-12-15T06:53:49.640Z",
				)

				d := deltalake.New(config.New(), logger.NOP, stats.NOP)
				err := d.Setup(ctx, warehouse, mockUploader)
				require.NoError(t, err)

				err = d.CreateSchema(ctx)
				require.NoError(t, err)
				t.Cleanup(func() {
					dropSchema(t, d.DB.DB, namespace)
				})

				err = d.CreateTable(ctx, tableName, schemaInWarehouse)
				require.NoError(t, err)

				loadTableStat, err := d.LoadTable(ctx, tableName)
				require.NoError(t, err)
				require.Equal(t, loadTableStat.RowsInserted, int64(14))
				require.Equal(t, loadTableStat.RowsUpdated, int64(0))

				loadTableStat, err = d.LoadTable(ctx, tableName)
				require.NoError(t, err)
				require.Equal(t, loadTableStat.RowsInserted, int64(0))
				require.Equal(t, loadTableStat.RowsUpdated, int64(14))

				records := whth.RetrieveRecordsFromWarehouse(t, d.DB.DB,
					fmt.Sprintf(`
						SELECT
						  id,
						  received_at,
						  test_bool,
						  test_datetime,
						  test_float,
						  test_int,
						  test_string
						FROM %s.%s
						ORDER BY id;`,
						namespace,
						tableName,
					),
				)
				require.Equal(t, records, whth.SampleTestRecords())
			})
			t.Run("with dedup use new record", func(t *testing.T) {
				tableName := "merge_with_dedup_use_new_record_test_table"
				uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/dedup.csv.gz", tableName)

				loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
				mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, whutils.LoadFileTypeCsv, true, true, "2022-12-15T06:53:49.640Z")

				d := deltalake.New(config.New(), logger.NOP, stats.NOP)
				err := d.Setup(ctx, warehouse, mockUploader)
				require.NoError(t, err)

				err = d.CreateSchema(ctx)
				require.NoError(t, err)
				t.Cleanup(func() {
					dropSchema(t, d.DB.DB, namespace)
				})

				err = d.CreateTable(ctx, tableName, schemaInWarehouse)
				require.NoError(t, err)

				loadTableStat, err := d.LoadTable(ctx, tableName)
				require.NoError(t, err)
				require.Equal(t, loadTableStat.RowsInserted, int64(14))
				require.Equal(t, loadTableStat.RowsUpdated, int64(0))

				retrieveRecordsSQL := fmt.Sprintf(`
						SELECT
						  id,
						  received_at,
						  test_bool,
						  test_datetime,
						  test_float,
						  test_int,
						  test_string
						FROM %s.%s
						ORDER BY id;`,
					namespace,
					tableName,
				)
				records := whth.RetrieveRecordsFromWarehouse(t, d.DB.DB, retrieveRecordsSQL)
				require.Equal(t, records, whth.DedupTestRecords())

				loadTableStat, err = d.LoadTable(ctx, tableName)
				require.NoError(t, err)
				require.Equal(t, loadTableStat.RowsInserted, int64(0))
				require.Equal(t, loadTableStat.RowsUpdated, int64(14))

				records = whth.RetrieveRecordsFromWarehouse(t, d.DB.DB, retrieveRecordsSQL)
				require.Equal(t, records, whth.DedupTestRecords())
			})
			t.Run("with no overlapping partition with preferAppend false", func(t *testing.T) {
				tableName := "merge_with_no_overlapping_partition_test_table"
				uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/dedup.csv.gz", tableName)

				loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
				mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, whutils.LoadFileTypeCsv, true, false, "2022-11-15T06:53:49.640Z")

				appendWarehouse := th.Clone(t, warehouse)
				appendWarehouse.Destination.Config["preferAppend"] = false

				d := deltalake.New(config.New(), logger.NOP, stats.NOP)
				err := d.Setup(ctx, appendWarehouse, mockUploader)
				require.NoError(t, err)

				err = d.CreateSchema(ctx)
				require.NoError(t, err)
				t.Cleanup(func() {
					dropSchema(t, d.DB.DB, namespace)
				})

				err = d.CreateTable(ctx, tableName, schemaInWarehouse)
				require.NoError(t, err)

				loadTableStat, err := d.LoadTable(ctx, tableName)
				require.NoError(t, err)
				require.Equal(t, loadTableStat.RowsInserted, int64(14))
				require.Equal(t, loadTableStat.RowsUpdated, int64(0))

				loadTableStat, err = d.LoadTable(ctx, tableName)
				require.NoError(t, err)
				require.Equal(t, loadTableStat.RowsInserted, int64(0))
				require.Equal(t, loadTableStat.RowsUpdated, int64(14))

				records := whth.RetrieveRecordsFromWarehouse(t, d.DB.DB,
					fmt.Sprintf(
						`SELECT
							id,
							received_at,
							test_bool,
							test_datetime,
							test_float,
							test_int,
							test_string
						FROM %s.%s
						ORDER BY id;`,
						namespace,
						tableName,
					),
				)
				require.Equal(t, records, whth.DedupTestRecords())
			})
			t.Run("with no overlapping partition with preferAppend true", func(t *testing.T) {
				tableName := "merge_with_no_overlapping_partition_test_table"
				uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/dedup.csv.gz", tableName)

				loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
				mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, whutils.LoadFileTypeCsv, true, false, "2022-11-15T06:53:49.640Z")

				appendWarehouse := th.Clone(t, warehouse)
				appendWarehouse.Destination.Config["preferAppend"] = true

				d := deltalake.New(config.New(), logger.NOP, stats.NOP)
				err := d.Setup(ctx, appendWarehouse, mockUploader)
				require.NoError(t, err)

				err = d.CreateSchema(ctx)
				require.NoError(t, err)
				t.Cleanup(func() {
					dropSchema(t, d.DB.DB, namespace)
				})

				err = d.CreateTable(ctx, tableName, schemaInWarehouse)
				require.NoError(t, err)

				loadTableStat, err := d.LoadTable(ctx, tableName)
				require.NoError(t, err)
				require.Equal(t, loadTableStat.RowsInserted, int64(14))
				require.Equal(t, loadTableStat.RowsUpdated, int64(0))

				loadTableStat, err = d.LoadTable(ctx, tableName)
				require.NoError(t, err)
				require.Equal(t, loadTableStat.RowsInserted, int64(14))
				require.Equal(t, loadTableStat.RowsUpdated, int64(0))

				records := whth.RetrieveRecordsFromWarehouse(t, d.DB.DB,
					fmt.Sprintf(
						`SELECT
							id,
							received_at,
							test_bool,
							test_datetime,
							test_float,
							test_int,
							test_string
						FROM %s.%s
						ORDER BY id;`,
						namespace,
						tableName,
					),
				)
				require.Equal(t, records, whth.DedupTwiceTestRecords())
			})
		})
		t.Run("append", func(t *testing.T) {
			tableName := "append_test_table"

			uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/load.csv.gz", tableName)

			loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, whutils.LoadFileTypeCsv, true, false, "2022-12-15T06:53:49.640Z")

			appendWarehouse := th.Clone(t, warehouse)
			appendWarehouse.Destination.Config[model.PreferAppendSetting.String()] = true

			d := deltalake.New(config.New(), logger.NOP, stats.NOP)
			err := d.Setup(ctx, appendWarehouse, mockUploader)
			require.NoError(t, err)

			err = d.CreateSchema(ctx)
			require.NoError(t, err)
			t.Cleanup(func() {
				dropSchema(t, d.DB.DB, namespace)
			})

			err = d.CreateTable(ctx, tableName, schemaInWarehouse)
			require.NoError(t, err)

			loadTableStat, err := d.LoadTable(ctx, tableName)
			require.NoError(t, err)
			require.Equal(t, loadTableStat.RowsInserted, int64(14))
			require.Equal(t, loadTableStat.RowsUpdated, int64(0))

			loadTableStat, err = d.LoadTable(ctx, tableName)
			require.NoError(t, err)
			require.Equal(t, loadTableStat.RowsInserted, int64(14))
			require.Equal(t, loadTableStat.RowsUpdated, int64(0))

			records := whth.RetrieveRecordsFromWarehouse(t, d.DB.DB,
				fmt.Sprintf(
					`SELECT
						id,
						received_at,
						test_bool,
						test_datetime,
						test_float,
						test_int,
						test_string
					FROM %s.%s
					ORDER BY id;`,
					namespace,
					tableName,
				),
			)
			require.Equal(t, records, whth.AppendTestRecords())
		})
		t.Run("load file does not exists", func(t *testing.T) {
			tableName := "load_file_not_exists_test_table"

			loadFiles := []whutils.LoadFile{{
				Location: fmt.Sprintf("https://%s.blob.core.windows.net/%s/rudder-warehouse-load-objects/load_file_not_exists_test_table/test_source_id/a01af26e-4548-49ff-a895-258829cc1a83-load_file_not_exists_test_table/load.csv.gz",
					credentials.AccountName,
					credentials.ContainerName,
				),
			}}
			mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, whutils.LoadFileTypeCsv, false, false, "2022-12-15T06:53:49.640Z")

			d := deltalake.New(config.New(), logger.NOP, stats.NOP)
			err := d.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = d.CreateSchema(ctx)
			require.NoError(t, err)
			t.Cleanup(func() {
				dropSchema(t, d.DB.DB, namespace)
			})

			err = d.CreateTable(ctx, tableName, schemaInWarehouse)
			require.NoError(t, err)

			loadTableStat, err := d.LoadTable(ctx, tableName)
			require.Error(t, err)
			require.Nil(t, loadTableStat)
		})
		t.Run("mismatch in number of columns", func(t *testing.T) {
			tableName := "mismatch_columns_test_table"

			uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/mismatch-columns.csv.gz", tableName)

			loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, whutils.LoadFileTypeCsv, false, false, "2022-12-15T06:53:49.640Z")

			d := deltalake.New(config.New(), logger.NOP, stats.NOP)
			err := d.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = d.CreateSchema(ctx)
			require.NoError(t, err)
			t.Cleanup(func() {
				dropSchema(t, d.DB.DB, namespace)
			})

			err = d.CreateTable(ctx, tableName, schemaInWarehouse)
			require.NoError(t, err)

			loadTableStat, err := d.LoadTable(ctx, tableName)
			require.NoError(t, err)
			require.Equal(t, loadTableStat.RowsInserted, int64(14))
			require.Equal(t, loadTableStat.RowsUpdated, int64(0))

			records := whth.RetrieveRecordsFromWarehouse(t, d.DB.DB,
				fmt.Sprintf(
					`SELECT
						id,
						received_at,
						test_bool,
						test_datetime,
						test_float,
						test_int,
						test_string
					FROM %s.%s
					ORDER BY id;`,
					namespace,
					tableName,
				),
			)
			require.Equal(t, records, whth.SampleTestRecords())
		})
		t.Run("mismatch in schema", func(t *testing.T) {
			tableName := "mismatch_schema_test_table"

			uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/mismatch-schema.csv.gz", tableName)

			loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, whutils.LoadFileTypeCsv, false, false, "2022-12-15T06:53:49.640Z")

			d := deltalake.New(config.New(), logger.NOP, stats.NOP)
			err := d.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = d.CreateSchema(ctx)
			require.NoError(t, err)
			t.Cleanup(func() {
				dropSchema(t, d.DB.DB, namespace)
			})

			err = d.CreateTable(ctx, tableName, schemaInWarehouse)
			require.NoError(t, err)

			loadTableStat, err := d.LoadTable(ctx, tableName)
			require.NoError(t, err)
			require.Equal(t, loadTableStat.RowsInserted, int64(14))
			require.Equal(t, loadTableStat.RowsUpdated, int64(0))

			records := whth.RetrieveRecordsFromWarehouse(t, d.DB.DB,
				fmt.Sprintf(
					`SELECT
						id,
						received_at,
						test_bool,
						test_datetime,
						test_float,
						test_int,
						test_string
					FROM %s.%s
					ORDER BY id;`,
					namespace,
					tableName,
				),
			)
			require.Equal(t, records, whth.MismatchSchemaTestRecords())
		})
		t.Run("discards", func(t *testing.T) {
			tableName := whutils.DiscardsTable

			file, err := whth.CreateDiscardFileCSV(t)
			require.NoError(t, err)
			defer func() {
				_ = file.Close()
			}()
			uploadOutput := whth.UploadLoadFile(t, fm, file.Name(), tableName)

			loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := newMockUploader(t, loadFiles, tableName, whutils.DiscardsSchema, whutils.DiscardsSchema, whutils.LoadFileTypeCsv, false, false, "2022-12-15T06:53:49.640Z")

			d := deltalake.New(config.New(), logger.NOP, stats.NOP)
			err = d.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = d.CreateSchema(ctx)
			require.NoError(t, err)
			t.Cleanup(func() {
				dropSchema(t, d.DB.DB, namespace)
			})

			err = d.CreateTable(ctx, tableName, whutils.DiscardsSchema)
			require.NoError(t, err)

			loadTableStat, err := d.LoadTable(ctx, tableName)
			require.NoError(t, err)
			require.Equal(t, loadTableStat.RowsInserted, int64(6))
			require.Equal(t, loadTableStat.RowsUpdated, int64(0))

			records := whth.RetrieveRecordsFromWarehouse(t, d.DB.DB,
				fmt.Sprintf(
					`SELECT
						column_name,
						column_value,
						received_at,
						row_id,
						table_name,
						uuid_ts
					FROM %s.%s
					ORDER BY row_id ASC;`,
					namespace,
					tableName,
				),
			)
			require.Equal(t, records, whth.DiscardTestRecords())
		})
		t.Run("parquet", func(t *testing.T) {
			tableName := "parquet_test_table"

			uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/load.parquet", tableName)

			loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, whutils.LoadFileTypeParquet, false, false, "2022-12-15T06:53:49.640Z")

			d := deltalake.New(config.New(), logger.NOP, stats.NOP)
			err := d.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = d.CreateSchema(ctx)
			require.NoError(t, err)
			t.Cleanup(func() {
				dropSchema(t, d.DB.DB, namespace)
			})

			err = d.CreateTable(ctx, tableName, schemaInWarehouse)
			require.NoError(t, err)

			loadTableStat, err := d.LoadTable(ctx, tableName)
			require.NoError(t, err)
			require.Equal(t, loadTableStat.RowsInserted, int64(14))
			require.Equal(t, loadTableStat.RowsUpdated, int64(0))

			records := whth.RetrieveRecordsFromWarehouse(t, d.DB.DB,
				fmt.Sprintf(
					`SELECT
						id,
						received_at,
						test_bool,
						test_datetime,
						test_float,
						test_int,
						test_string
					FROM %s.%s
					ORDER BY id;`,
					namespace,
					tableName,
				),
			)
			require.Equal(t, records, whth.SampleTestRecords())
		})
		t.Run("partition pruning", func(t *testing.T) {
			t.Run("not partitioned", func(t *testing.T) {
				tableName := "not_partitioned_test_table"

				uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/load.csv.gz", tableName)

				loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
				mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, whutils.LoadFileTypeCsv, false, false, "2022-12-15T06:53:49.640Z")

				d := deltalake.New(config.New(), logger.NOP, stats.NOP)
				err := d.Setup(ctx, warehouse, mockUploader)
				require.NoError(t, err)

				err = d.CreateSchema(ctx)
				require.NoError(t, err)
				t.Cleanup(func() {
					dropSchema(t, d.DB.DB, namespace)
				})

				_, err = d.DB.QueryContext(ctx,
					`CREATE TABLE IF NOT EXISTS `+namespace+`.`+tableName+` (
						extra_test_bool BOOLEAN,
						extra_test_datetime TIMESTAMP,
						extra_test_float DOUBLE,
						extra_test_int BIGINT,
						extra_test_string STRING,
						id STRING,
						received_at TIMESTAMP,
						event_date DATE GENERATED ALWAYS AS (
							CAST(received_at AS DATE)
						),
						test_bool BOOLEAN,
						test_datetime TIMESTAMP,
						test_float DOUBLE,
						test_int BIGINT,
						test_string STRING
					) USING DELTA;`)
				require.NoError(t, err)

				loadTableStat, err := d.LoadTable(ctx, tableName)
				require.NoError(t, err)
				require.Equal(t, loadTableStat.RowsInserted, int64(14))
				require.Equal(t, loadTableStat.RowsUpdated, int64(0))

				records := whth.RetrieveRecordsFromWarehouse(t, d.DB.DB,
					fmt.Sprintf(
						`SELECT
							id,
							received_at,
							test_bool,
							test_datetime,
							test_float,
							test_int,
							test_string
						FROM %s.%s
						ORDER BY id;`,
						namespace,
						tableName,
					),
				)
				require.Equal(t, records, whth.SampleTestRecords())
			})
			t.Run("event_date is not in partition", func(t *testing.T) {
				tableName := "not_event_date_partition_test_table"

				uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/load.csv.gz", tableName)

				loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
				mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, whutils.LoadFileTypeCsv, false, false, "2022-12-15T06:53:49.640Z")

				d := deltalake.New(config.New(), logger.NOP, stats.NOP)
				err := d.Setup(ctx, warehouse, mockUploader)
				require.NoError(t, err)

				err = d.CreateSchema(ctx)
				require.NoError(t, err)
				t.Cleanup(func() {
					dropSchema(t, d.DB.DB, namespace)
				})

				_, err = d.DB.QueryContext(ctx,
					`CREATE TABLE IF NOT EXISTS `+namespace+`.`+tableName+` (
						extra_test_bool BOOLEAN,
						extra_test_datetime TIMESTAMP,
						extra_test_float DOUBLE,
						extra_test_int BIGINT,
						extra_test_string STRING,
						id STRING,
						received_at TIMESTAMP,
						event_date DATE GENERATED ALWAYS AS (
							CAST(received_at AS DATE)
						),
						test_bool BOOLEAN,
						test_datetime TIMESTAMP,
						test_float DOUBLE,
						test_int BIGINT,
						test_string STRING
					) USING DELTA PARTITIONED BY(id);`)
				require.NoError(t, err)

				loadTableStat, err := d.LoadTable(ctx, tableName)
				require.NoError(t, err)
				require.Equal(t, loadTableStat.RowsInserted, int64(14))
				require.Equal(t, loadTableStat.RowsUpdated, int64(0))

				records := whth.RetrieveRecordsFromWarehouse(t, d.DB.DB,
					fmt.Sprintf(
						`SELECT
							id,
							received_at,
							test_bool,
							test_datetime,
							test_float,
							test_int,
							test_string
						FROM %s.%s
						ORDER BY id;`,
						namespace,
						tableName,
					),
				)
				require.Equal(t, records, whth.SampleTestRecords())
			})
		})
	})

	t.Run("Fetch Schema", func(t *testing.T) {
		ctx := context.Background()
		namespace := whth.RandSchema(destType)

		warehouse := model.Warehouse{
			Source: backendconfig.SourceT{
				ID: "test_source_id",
			},
			Destination: backendconfig.DestinationT{
				ID: "test_destination_id",
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Name: destType,
				},
				Config: map[string]any{
					"host":           credentials.Host,
					"port":           credentials.Port,
					"path":           credentials.Path,
					"token":          credentials.Token,
					"namespace":      namespace,
					"bucketProvider": whutils.AzureBlob,
					"containerName":  credentials.ContainerName,
					"accountName":    credentials.AccountName,
					"accountKey":     credentials.AccountKey,
				},
			},
			WorkspaceID: "test_workspace_id",
			Namespace:   namespace,
		}

		t.Run("create schema if not exists", func(t *testing.T) {
			ctrl := gomock.NewController(t)
			mockUploader := mockuploader.NewMockUploader(ctrl)
			mockUploader.EXPECT().UseRudderStorage().Return(false).AnyTimes()

			d := deltalake.New(config.New(), logger.NOP, stats.NOP)
			err := d.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			var schema string
			err = d.DB.QueryRowContext(ctx, fmt.Sprintf(`SHOW SCHEMAS LIKE '%s';`, d.Namespace)).Scan(&schema)
			require.ErrorIs(t, err, sql.ErrNoRows)
			require.Empty(t, schema)

			warehouseSchema, unrecognizedWarehouseSchema, err := d.FetchSchema(ctx)
			require.NoError(t, err)
			require.Empty(t, warehouseSchema)
			require.Empty(t, unrecognizedWarehouseSchema)
			t.Cleanup(func() {
				dropSchema(t, d.DB.DB, namespace)
			})

			err = d.DB.QueryRowContext(ctx, fmt.Sprintf(`SHOW SCHEMAS LIKE '%s';`, d.Namespace)).Scan(&schema)
			require.NoError(t, err)
			require.Equal(t, schema, d.Namespace)
		})

		t.Run("schema already exists with some missing datatype", func(t *testing.T) {
			ctrl := gomock.NewController(t)
			mockUploader := mockuploader.NewMockUploader(ctrl)
			mockUploader.EXPECT().UseRudderStorage().Return(false).AnyTimes()

			statsStore, err := memstats.New()
			require.NoError(t, err)

			d := deltalake.New(config.New(), logger.NOP, statsStore)
			err = d.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = d.CreateSchema(ctx)
			require.NoError(t, err)
			t.Cleanup(func() {
				dropSchema(t, d.DB.DB, namespace)
			})

			_, err = d.DB.ExecContext(ctx, fmt.Sprintf(`CREATE TABLE %s.test_table (c1 bigint, c2 binary, c3 boolean, c4 date, c5 decimal(10,2), c6 double, c7 float, c8 int, c9 void, c10 smallint, c11 string, c12 timestamp, c13 timestamp_ntz, c14 tinyint, c15 array<int>, c16 map<timestamp, int>, c17 struct<Field1:timestamp,Field2:int>, received_at timestamp, event_date date GENERATED ALWAYS AS (CAST(received_at AS DATE)));`, d.Namespace))
			require.NoError(t, err)
			_, err = d.DB.ExecContext(ctx, fmt.Sprintf(`CREATE TABLE %s.rudder_staging_123 (c1 string, c2 int);`, d.Namespace))
			require.NoError(t, err)

			warehouseSchema, unrecognizedWarehouseSchema, err := d.FetchSchema(ctx)
			require.NoError(t, err)
			require.NotEmpty(t, warehouseSchema)
			require.NotEmpty(t, unrecognizedWarehouseSchema)
			require.Contains(t, warehouseSchema, "test_table")
			require.NotContains(t, warehouseSchema["test_table"], "event_date")
			require.NotContains(t, warehouseSchema, "rudder_staging_123")

			require.Equal(t, model.TableSchema{
				"c1":          "int",
				"c11":         "string",
				"c4":          "date",
				"c14":         "int",
				"c3":          "boolean",
				"received_at": "datetime",
				"c7":          "float",
				"c12":         "datetime",
				"c10":         "int",
				"c6":          "float",
				"c8":          "int",
			},
				warehouseSchema["test_table"],
			)
			require.Equal(t, model.Schema{
				"test_table": {
					"c13": "<missing_datatype>",
					"c16": "<missing_datatype>",
					"c5":  "<missing_datatype>",
					"c2":  "<missing_datatype>",
					"c15": "<missing_datatype>",
					"c9":  "<missing_datatype>",
					"c17": "<missing_datatype>",
				},
			},
				unrecognizedWarehouseSchema,
			)

			missingDatatypeStats := []string{"void", "timestamp_ntz", "struct", "array", "binary", "map", "decimal(10,2)"}
			for _, missingDatatype := range missingDatatypeStats {
				require.EqualValues(t, 1, statsStore.Get(whutils.RudderMissingDatatype, stats.Tags{
					"module":      "warehouse",
					"destType":    warehouse.Type,
					"workspaceId": warehouse.WorkspaceID,
					"destID":      warehouse.Destination.ID,
					"sourceID":    warehouse.Source.ID,
					"datatype":    missingDatatype,
				}).LastValue())
			}
		})
	})
}

func dropSchema(t *testing.T, db *sql.DB, namespace string) {
	t.Helper()
	t.Log("dropping schema", namespace)

	require.Eventually(t,
		func() bool {
			_, err := db.ExecContext(context.Background(), fmt.Sprintf(`DROP SCHEMA %s CASCADE;`, namespace))
			if err != nil {
				t.Logf("error deleting schema %q: %v", namespace, err)
				return false
			}
			return true
		},
		time.Minute,
		time.Second,
	)
}

func TestDeltalake_TrimErrorMessage(t *testing.T) {
	tempError := errors.New("temp error")

	testCases := []struct {
		name          string
		inputError    error
		expectedError error
	}{
		{
			name:          "error message is above max length",
			inputError:    errors.New(strings.Repeat(tempError.Error(), 100)),
			expectedError: errors.New(strings.Repeat(tempError.Error(), 25)),
		},
		{
			name:          "error message is below max length",
			inputError:    errors.New(strings.Repeat(tempError.Error(), 25)),
			expectedError: errors.New(strings.Repeat(tempError.Error(), 25)),
		},
		{
			name:          "error message is equal to max length",
			inputError:    errors.New(strings.Repeat(tempError.Error(), 10)),
			expectedError: errors.New(strings.Repeat(tempError.Error(), 10)),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			c := config.New()
			c.Set("Warehouse.deltalake.maxErrorLength", len(tempError.Error())*25)

			d := deltalake.New(c, logger.NOP, stats.NOP)
			require.Equal(t, tc.expectedError, d.TrimErrorMessage(tc.inputError))
		})
	}
}

func TestDeltalake_ShouldMerge(t *testing.T) {
	testCases := []struct {
		name                  string
		preferAppend          bool
		uploaderCanAppend     bool
		uploaderExpectedCalls int
		expected              bool
	}{
		{
			name:                  "uploader says we can append and user prefers to append",
			preferAppend:          true,
			uploaderCanAppend:     true,
			uploaderExpectedCalls: 1,
			expected:              false,
		},
		{
			name:                  "uploader says we can append and users prefers not to append",
			preferAppend:          false,
			uploaderCanAppend:     true,
			uploaderExpectedCalls: 1,
			expected:              true,
		},
		{
			name:                  "uploader says we cannot append and user prefers to append",
			preferAppend:          true,
			uploaderCanAppend:     false,
			uploaderExpectedCalls: 1,
			expected:              true,
		},
		{
			name:                  "uploader says we cannot append and users prefers not to append",
			preferAppend:          false,
			uploaderCanAppend:     false,
			uploaderExpectedCalls: 1,
			expected:              true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			d := deltalake.New(config.New(), logger.NOP, stats.NOP)
			d.Warehouse = model.Warehouse{
				Destination: backendconfig.DestinationT{
					Config: map[string]any{
						model.PreferAppendSetting.String(): tc.preferAppend,
					},
				},
			}

			mockCtrl := gomock.NewController(t)
			uploader := mockuploader.NewMockUploader(mockCtrl)
			uploader.EXPECT().CanAppend().Times(tc.uploaderExpectedCalls).Return(tc.uploaderCanAppend)

			d.Uploader = uploader
			require.Equal(t, d.ShouldMerge(), tc.expected)
		})
	}
}

func newMockUploader(
	t testing.TB,
	loadFiles []whutils.LoadFile,
	tableName string,
	schemaInUpload model.TableSchema,
	schemaInWarehouse model.TableSchema,
	loadFileType string,
	canAppend bool,
	onDedupUseNewRecords bool,
	eventTS string,
) whutils.Uploader {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	firstLastEventTS, err := time.Parse(time.RFC3339, eventTS)
	require.NoError(t, err)

	mockUploader := mockuploader.NewMockUploader(ctrl)
	mockUploader.EXPECT().UseRudderStorage().Return(false).AnyTimes()
	mockUploader.EXPECT().ShouldOnDedupUseNewRecord().Return(onDedupUseNewRecords).AnyTimes()
	mockUploader.EXPECT().CanAppend().Return(canAppend).AnyTimes()
	mockUploader.EXPECT().GetLoadFilesMetadata(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, options whutils.GetLoadFilesOptions) ([]whutils.LoadFile, error) {
			return slices.Clone(loadFiles), nil
		},
	).AnyTimes()
	mockUploader.EXPECT().GetSampleLoadFileLocation(gomock.Any(), gomock.Any()).Return(loadFiles[0].Location, nil).AnyTimes()
	mockUploader.EXPECT().GetTableSchemaInUpload(tableName).Return(schemaInUpload).AnyTimes()
	mockUploader.EXPECT().GetTableSchemaInWarehouse(tableName).Return(schemaInWarehouse).AnyTimes()
	mockUploader.EXPECT().GetLoadFileType().Return(loadFileType).AnyTimes()
	mockUploader.EXPECT().GetFirstLastEvent().Return(firstLastEventTS, firstLastEventTS).AnyTimes()

	return mockUploader
}

func mergeEventsMap() whth.EventsCountMap {
	return whth.EventsCountMap{
		"identifies":    1,
		"users":         1,
		"tracks":        1,
		"product_track": 1,
		"pages":         1,
		"screens":       1,
		"aliases":       1,
		"groups":        1,
	}
}

func appendEventsMap() whth.EventsCountMap {
	return whth.EventsCountMap{
		"identifies":    2,
		"users":         2,
		"tracks":        2,
		"product_track": 2,
		"pages":         2,
		"screens":       2,
		"aliases":       2,
		"groups":        2,
	}
}
