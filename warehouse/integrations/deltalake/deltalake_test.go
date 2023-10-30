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

	dbsql "github.com/databricks/databricks-sql-go"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/compose-test/compose"
	"github.com/rudderlabs/compose-test/testcompose"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/runner"
	th "github.com/rudderlabs/rudder-server/testhelper"
	"github.com/rudderlabs/rudder-server/testhelper/health"
	"github.com/rudderlabs/rudder-server/testhelper/workspaceConfig"
	"github.com/rudderlabs/rudder-server/utils/misc"
	warehouseclient "github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/deltalake"
	whth "github.com/rudderlabs/rudder-server/warehouse/integrations/testhelper"
	mockuploader "github.com/rudderlabs/rudder-server/warehouse/internal/mocks/utils"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
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

func testCredentialsAvailable() bool {
	_, err := deltaLakeTestCredentials()
	return err == nil
}

func TestIntegration(t *testing.T) {
	if os.Getenv("SLOW") != "1" {
		t.Skip("Skipping tests. Add 'SLOW=1' env var to run test.")
	}
	if !testCredentialsAvailable() {
		t.Skipf("Skipping %s as %s is not set", t.Name(), testKey)
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
	destType := warehouseutils.DELTALAKE
	namespace := whth.RandSchema(destType)

	deltaLakeCredentials, err := deltaLakeTestCredentials()
	require.NoError(t, err)

	port, err := strconv.Atoi(deltaLakeCredentials.Port)
	require.NoError(t, err)

	connector, err := dbsql.NewConnector(
		dbsql.WithServerHostname(deltaLakeCredentials.Host),
		dbsql.WithPort(port),
		dbsql.WithHTTPPath(deltaLakeCredentials.Path),
		dbsql.WithAccessToken(deltaLakeCredentials.Token),
		dbsql.WithSessionParams(map[string]string{
			"ansi_mode": "false",
		}),
	)
	require.NoError(t, err)

	db := sql.OpenDB(connector)
	require.NoError(t, db.Ping())

	bootstrapSvc := func(t *testing.T, enableMerge bool) {
		templateConfigurations := map[string]any{
			"workspaceID":   workspaceID,
			"sourceID":      sourceID,
			"destinationID": destinationID,
			"writeKey":      writeKey,
			"host":          deltaLakeCredentials.Host,
			"port":          deltaLakeCredentials.Port,
			"path":          deltaLakeCredentials.Path,
			"token":         deltaLakeCredentials.Token,
			"namespace":     namespace,
			"containerName": deltaLakeCredentials.ContainerName,
			"accountName":   deltaLakeCredentials.AccountName,
			"accountKey":    deltaLakeCredentials.AccountKey,
			"enableMerge":   enableMerge,
		}
		workspaceConfigPath := workspaceConfig.CreateTempFile(t, "testdata/template.json", templateConfigurations)

		whth.EnhanceWithDefaultEnvs(t)
		t.Setenv("JOBS_DB_PORT", strconv.Itoa(jobsDBPort))
		t.Setenv("WAREHOUSE_JOBS_DB_PORT", strconv.Itoa(jobsDBPort))
		t.Setenv("RSERVER_WAREHOUSE_DELTALAKE_MAX_PARALLEL_LOADS", "8")
		t.Setenv("RSERVER_WAREHOUSE_WEB_PORT", strconv.Itoa(httpPort))
		t.Setenv("RSERVER_BACKEND_CONFIG_CONFIG_JSONPATH", workspaceConfigPath)
		t.Setenv("RSERVER_WAREHOUSE_DELTALAKE_SLOW_QUERY_THRESHOLD", "0s")

		svcDone := make(chan struct{})
		ctx, cancel := context.WithCancel(context.Background())

		go func() {
			r := runner.New(runner.ReleaseInfo{})
			_ = r.Run(ctx, []string{"deltalake-integration-test"})
			close(svcDone)
		}()

		t.Cleanup(func() { <-svcDone })
		t.Cleanup(cancel)

		serviceHealthEndpoint := fmt.Sprintf("http://localhost:%d/health", httpPort)
		health.WaitUntilReady(ctx, t,
			serviceHealthEndpoint, time.Minute, time.Second, "serviceHealthEndpoint",
		)
	}

	t.Run("Event flow", func(t *testing.T) {
		jobsDB := whth.JobsDB(t, jobsDBPort)

		t.Cleanup(func() {
			require.Eventually(t,
				func() bool {
					if _, err := db.Exec(fmt.Sprintf(`DROP SCHEMA %s CASCADE;`, namespace)); err != nil {
						t.Logf("error deleting schema %q: %v", namespace, err)
						return false
					}
					return true
				},
				time.Minute,
				time.Second,
			)
		})

		testCases := []struct {
			name                string
			writeKey            string
			schema              string
			sourceID            string
			destinationID       string
			messageID           string
			warehouseEventsMap  whth.EventsCountMap
			enableMerge         bool
			useParquetLoadFiles bool
			stagingFilePrefix   string
			jobRunID            string
		}{
			{
				name:                "Merge Mode",
				writeKey:            writeKey,
				schema:              namespace,
				sourceID:            sourceID,
				destinationID:       destinationID,
				warehouseEventsMap:  mergeEventsMap(),
				enableMerge:         true,
				useParquetLoadFiles: false,
				stagingFilePrefix:   "testdata/upload-job-merge-mode",
				jobRunID:            misc.FastUUID().String(),
			},
			{
				name:                "Append Mode",
				writeKey:            writeKey,
				schema:              namespace,
				sourceID:            sourceID,
				destinationID:       destinationID,
				warehouseEventsMap:  appendEventsMap(),
				enableMerge:         false,
				useParquetLoadFiles: false,
				stagingFilePrefix:   "testdata/upload-job-append-mode",
				// an empty jobRunID means that the source is not an ETL one
				// see Uploader.CanAppend()
				jobRunID: "",
			},
			{
				name:                "Parquet load files",
				writeKey:            writeKey,
				schema:              namespace,
				sourceID:            sourceID,
				destinationID:       destinationID,
				warehouseEventsMap:  mergeEventsMap(),
				enableMerge:         true,
				useParquetLoadFiles: true,
				stagingFilePrefix:   "testdata/upload-job-parquet",
				jobRunID:            misc.FastUUID().String(),
			},
		}

		for _, tc := range testCases {
			tc := tc
			t.Run(tc.name, func(t *testing.T) {
				bootstrapSvc(t, tc.enableMerge)
				t.Setenv("RSERVER_WAREHOUSE_DELTALAKE_USE_PARQUET_LOAD_FILES", strconv.FormatBool(tc.useParquetLoadFiles))

				sqlClient := &warehouseclient.Client{
					SQL:  db,
					Type: warehouseclient.SQLClient,
				}

				conf := map[string]interface{}{
					"bucketProvider": "AZURE_BLOB",
					"containerName":  deltaLakeCredentials.ContainerName,
					"prefix":         "",
					"useSTSTokens":   false,
					"enableSSE":      false,
					"accountName":    deltaLakeCredentials.AccountName,
					"accountKey":     deltaLakeCredentials.AccountKey,
				}
				tables := []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"}

				t.Log("verifying test case 1")
				ts1 := whth.TestConfig{
					WriteKey:      writeKey,
					Schema:        tc.schema,
					Tables:        tables,
					SourceID:      tc.sourceID,
					DestinationID: tc.destinationID,
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
					Schema:             tc.schema,
					Tables:             tables,
					SourceID:           tc.sourceID,
					DestinationID:      tc.destinationID,
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
		t.Cleanup(func() {
			require.Eventually(t,
				func() bool {
					if _, err := db.Exec(fmt.Sprintf(`DROP SCHEMA %s CASCADE;`, namespace)); err != nil {
						t.Logf("error deleting schema %q: %v", namespace, err)
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
				"host":            deltaLakeCredentials.Host,
				"port":            deltaLakeCredentials.Port,
				"path":            deltaLakeCredentials.Path,
				"token":           deltaLakeCredentials.Token,
				"namespace":       namespace,
				"bucketProvider":  "AZURE_BLOB",
				"containerName":   deltaLakeCredentials.ContainerName,
				"prefix":          "",
				"useSTSTokens":    false,
				"enableSSE":       false,
				"accountName":     deltaLakeCredentials.AccountName,
				"accountKey":      deltaLakeCredentials.AccountKey,
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
			tc := tc
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
		const (
			sourceID      = "test_source_id"
			destinationID = "test_destination_id"
			workspaceID   = "test_workspace_id"
		)

		ctx := context.Background()
		namespace := whth.RandSchema(destType)
		cleanupSchema := func() {
			require.Eventually(t,
				func() bool {
					_, err := db.Exec(fmt.Sprintf(`DROP SCHEMA %s CASCADE;`, namespace))
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
				ID: sourceID,
			},
			Destination: backendconfig.DestinationT{
				ID: destinationID,
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Name: destType,
				},
				Config: map[string]any{
					"host":           deltaLakeCredentials.Host,
					"port":           deltaLakeCredentials.Port,
					"path":           deltaLakeCredentials.Path,
					"token":          deltaLakeCredentials.Token,
					"namespace":      namespace,
					"bucketProvider": warehouseutils.AzureBlob,
					"containerName":  deltaLakeCredentials.ContainerName,
					"accountName":    deltaLakeCredentials.AccountName,
					"accountKey":     deltaLakeCredentials.AccountKey,
				},
			},
			WorkspaceID: workspaceID,
			Namespace:   namespace,
		}

		fm, err := filemanager.New(&filemanager.Settings{
			Provider: warehouseutils.AzureBlob,
			Config: map[string]any{
				"containerName":  deltaLakeCredentials.ContainerName,
				"accountName":    deltaLakeCredentials.AccountName,
				"accountKey":     deltaLakeCredentials.AccountKey,
				"bucketProvider": warehouseutils.AzureBlob,
			},
		})
		require.NoError(t, err)

		t.Run("schema does not exists", func(t *testing.T) {
			tableName := "schema_not_exists_test_table"

			uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/load.csv.gz", tableName)

			loadFiles := []warehouseutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, warehouseutils.LoadFileTypeCsv, false, false, "2022-12-15T06:53:49.640Z")

			d := deltalake.New(config.New(), logger.NOP, memstats.New())
			err := d.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			loadTableStat, err := d.LoadTable(ctx, tableName)
			require.Error(t, err)
			require.Nil(t, loadTableStat)
		})
		t.Run("table does not exists", func(t *testing.T) {
			tableName := "table_not_exists_test_table"

			uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/load.csv.gz", tableName)

			loadFiles := []warehouseutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, warehouseutils.LoadFileTypeCsv, false, false, "2022-12-15T06:53:49.640Z")

			d := deltalake.New(config.New(), logger.NOP, memstats.New())
			err := d.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = d.CreateSchema(ctx)
			require.NoError(t, err)
			t.Cleanup(cleanupSchema)

			loadTableStat, err := d.LoadTable(ctx, tableName)
			require.Error(t, err)
			require.Nil(t, loadTableStat)
		})
		t.Run("merge", func(t *testing.T) {
			tableName := "merge_test_table"

			t.Run("without dedup", func(t *testing.T) {
				uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/load.csv.gz", tableName)

				loadFiles := []warehouseutils.LoadFile{{Location: uploadOutput.Location}}
				mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, warehouseutils.LoadFileTypeCsv, false, false, "2022-12-15T06:53:49.640Z")

				d := deltalake.New(config.New(), logger.NOP, memstats.New())
				err := d.Setup(ctx, warehouse, mockUploader)
				require.NoError(t, err)

				err = d.CreateSchema(ctx)
				require.NoError(t, err)
				t.Cleanup(cleanupSchema)

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
				uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/dedup.csv.gz", tableName)

				loadFiles := []warehouseutils.LoadFile{{Location: uploadOutput.Location}}
				mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, warehouseutils.LoadFileTypeCsv, true, true, "2022-12-15T06:53:49.640Z")

				mergeWarehouse := th.Clone(t, warehouse)
				mergeWarehouse.Destination.Config[string(model.EnableMergeSetting)] = true

				d := deltalake.New(config.New(), logger.NOP, memstats.New())
				err := d.Setup(ctx, mergeWarehouse, mockUploader)
				require.NoError(t, err)

				err = d.CreateSchema(ctx)
				require.NoError(t, err)
				t.Cleanup(cleanupSchema)

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
			t.Run("with no overlapping partition", func(t *testing.T) {
				uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/dedup.csv.gz", tableName)

				loadFiles := []warehouseutils.LoadFile{{Location: uploadOutput.Location}}
				mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, warehouseutils.LoadFileTypeCsv, false, false, "2022-11-15T06:53:49.640Z")

				mergeWarehouse := th.Clone(t, warehouse)
				mergeWarehouse.Destination.Config[string(model.EnableMergeSetting)] = true

				d := deltalake.New(config.New(), logger.NOP, memstats.New())
				err := d.Setup(ctx, mergeWarehouse, mockUploader)
				require.NoError(t, err)

				err = d.CreateSchema(ctx)
				require.NoError(t, err)
				t.Cleanup(cleanupSchema)

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

			loadFiles := []warehouseutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, warehouseutils.LoadFileTypeCsv, true, false, "2022-12-15T06:53:49.640Z")

			d := deltalake.New(config.New(), logger.NOP, memstats.New())
			err := d.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = d.CreateSchema(ctx)
			require.NoError(t, err)
			t.Cleanup(cleanupSchema)

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

			loadFiles := []warehouseutils.LoadFile{{
				Location: "https://account.blob.core.windows.net/container/rudder-warehouse-load-objects/load_file_not_exists_test_table/test_source_id/a01af26e-4548-49ff-a895-258829cc1a83-load_file_not_exists_test_table/load.csv.gz",
			}}
			mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, warehouseutils.LoadFileTypeCsv, false, false, "2022-12-15T06:53:49.640Z")

			d := deltalake.New(config.New(), logger.NOP, memstats.New())
			err := d.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = d.CreateSchema(ctx)
			require.NoError(t, err)
			t.Cleanup(cleanupSchema)

			err = d.CreateTable(ctx, tableName, schemaInWarehouse)
			require.NoError(t, err)

			loadTableStat, err := d.LoadTable(ctx, tableName)
			require.Error(t, err)
			require.Nil(t, loadTableStat)
		})
		t.Run("mismatch in number of columns", func(t *testing.T) {
			tableName := "mismatch_columns_test_table"

			uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/mismatch-columns.csv.gz", tableName)

			loadFiles := []warehouseutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, warehouseutils.LoadFileTypeCsv, false, false, "2022-12-15T06:53:49.640Z")

			d := deltalake.New(config.New(), logger.NOP, memstats.New())
			err := d.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = d.CreateSchema(ctx)
			require.NoError(t, err)
			t.Cleanup(cleanupSchema)

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

			loadFiles := []warehouseutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, warehouseutils.LoadFileTypeCsv, false, false, "2022-12-15T06:53:49.640Z")

			d := deltalake.New(config.New(), logger.NOP, memstats.New())
			err := d.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = d.CreateSchema(ctx)
			require.NoError(t, err)
			t.Cleanup(cleanupSchema)

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
			tableName := warehouseutils.DiscardsTable

			uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/discards.csv.gz", tableName)

			loadFiles := []warehouseutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := newMockUploader(t, loadFiles, tableName, warehouseutils.DiscardsSchema, warehouseutils.DiscardsSchema, warehouseutils.LoadFileTypeCsv, false, false, "2022-12-15T06:53:49.640Z")

			d := deltalake.New(config.New(), logger.NOP, memstats.New())
			err := d.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = d.CreateSchema(ctx)
			require.NoError(t, err)
			t.Cleanup(cleanupSchema)

			err = d.CreateTable(ctx, tableName, warehouseutils.DiscardsSchema)
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

			loadFiles := []warehouseutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, warehouseutils.LoadFileTypeParquet, false, false, "2022-12-15T06:53:49.640Z")

			d := deltalake.New(config.New(), logger.NOP, memstats.New())
			err := d.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = d.CreateSchema(ctx)
			require.NoError(t, err)
			t.Cleanup(cleanupSchema)

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

				loadFiles := []warehouseutils.LoadFile{{Location: uploadOutput.Location}}
				mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, warehouseutils.LoadFileTypeCsv, false, false, "2022-12-15T06:53:49.640Z")

				d := deltalake.New(config.New(), logger.NOP, memstats.New())
				err := d.Setup(ctx, warehouse, mockUploader)
				require.NoError(t, err)

				err = d.CreateSchema(ctx)
				require.NoError(t, err)
				t.Cleanup(cleanupSchema)

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

				loadFiles := []warehouseutils.LoadFile{{Location: uploadOutput.Location}}
				mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, warehouseutils.LoadFileTypeCsv, false, false, "2022-12-15T06:53:49.640Z")

				d := deltalake.New(config.New(), logger.NOP, memstats.New())
				err := d.Setup(ctx, warehouse, mockUploader)
				require.NoError(t, err)

				err = d.CreateSchema(ctx)
				require.NoError(t, err)
				t.Cleanup(cleanupSchema)

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
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			c := config.New()
			c.Set("Warehouse.deltalake.maxErrorLength", len(tempError.Error())*25)

			d := deltalake.New(c, logger.NOP, memstats.New())
			require.Equal(t, tc.expectedError, d.TrimErrorMessage(tc.inputError))
		})
	}
}

func TestDeltalake_ShouldMerge(t *testing.T) {
	testCases := []struct {
		name                  string
		enableMerge           bool
		uploaderCanAppend     bool
		uploaderExpectedCalls int
		expected              bool
	}{
		{
			name:                  "uploader says we can append and merge is not enabled",
			enableMerge:           false,
			uploaderCanAppend:     true,
			uploaderExpectedCalls: 1,
			expected:              false,
		},
		{
			name:                  "uploader says we can append and merge is enabled",
			enableMerge:           true,
			uploaderCanAppend:     true,
			uploaderExpectedCalls: 1,
			expected:              true,
		},
		{
			name:                  "uploader says we cannot append so enableMerge false is ignored",
			enableMerge:           false,
			uploaderCanAppend:     false,
			uploaderExpectedCalls: 1,
			expected:              true,
		},
		{
			name:                  "uploader says we cannot append so enableMerge true is ignored",
			enableMerge:           true,
			uploaderCanAppend:     false,
			uploaderExpectedCalls: 1,
			expected:              true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			d := deltalake.New(config.New(), logger.NOP, memstats.New())
			d.Warehouse = model.Warehouse{
				Destination: backendconfig.DestinationT{
					Config: map[string]any{
						string(model.EnableMergeSetting): tc.enableMerge,
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
	loadFiles []warehouseutils.LoadFile,
	tableName string,
	schemaInUpload model.TableSchema,
	schemaInWarehouse model.TableSchema,
	loadFileType string,
	canAppend bool,
	onDedupUseNewRecords bool,
	eventTS string,
) warehouseutils.Uploader {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	firstLastEventTS, err := time.Parse(time.RFC3339, eventTS)
	require.NoError(t, err)

	mockUploader := mockuploader.NewMockUploader(ctrl)
	mockUploader.EXPECT().UseRudderStorage().Return(false).AnyTimes()
	mockUploader.EXPECT().ShouldOnDedupUseNewRecord().Return(onDedupUseNewRecords).AnyTimes()
	mockUploader.EXPECT().CanAppend().Return(canAppend).AnyTimes()
	mockUploader.EXPECT().GetLoadFilesMetadata(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, options warehouseutils.GetLoadFilesOptions) ([]warehouseutils.LoadFile, error) {
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
