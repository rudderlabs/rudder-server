package deltalake_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"golang.org/x/exp/slices"

	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"

	dbsql "github.com/databricks/databricks-sql-go"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/compose-test/compose"
	"github.com/rudderlabs/compose-test/testcompose"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/runner"
	"github.com/rudderlabs/rudder-server/testhelper/health"
	"github.com/rudderlabs/rudder-server/testhelper/workspaceConfig"
	"github.com/rudderlabs/rudder-server/utils/misc"
	warehouseclient "github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/deltalake"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/testhelper"
	mockuploader "github.com/rudderlabs/rudder-server/warehouse/internal/mocks/utils"
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

	namespace := testhelper.RandSchema(destType)

	deltaLakeCredentials, err := deltaLakeTestCredentials()
	require.NoError(t, err)

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
	}
	workspaceConfigPath := workspaceConfig.CreateTempFile(t, "testdata/template.json", templateConfigurations)

	testhelper.EnhanceWithDefaultEnvs(t)
	t.Setenv("JOBS_DB_PORT", strconv.Itoa(jobsDBPort))
	t.Setenv("WAREHOUSE_JOBS_DB_PORT", strconv.Itoa(jobsDBPort))
	t.Setenv("RSERVER_WAREHOUSE_DELTALAKE_MAX_PARALLEL_LOADS", "8")
	t.Setenv("RSERVER_WAREHOUSE_WEB_PORT", strconv.Itoa(httpPort))
	t.Setenv("RSERVER_BACKEND_CONFIG_CONFIG_JSONPATH", workspaceConfigPath)
	t.Setenv("RSERVER_WAREHOUSE_DELTALAKE_SLOW_QUERY_THRESHOLD", "0s")

	svcDone := make(chan struct{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		r := runner.New(runner.ReleaseInfo{})
		_ = r.Run(ctx, []string{"deltalake-integration-test"})

		close(svcDone)
	}()
	t.Cleanup(func() { <-svcDone })

	serviceHealthEndpoint := fmt.Sprintf("http://localhost:%d/health", httpPort)
	health.WaitUntilReady(ctx, t, serviceHealthEndpoint, time.Minute, time.Second, "serviceHealthEndpoint")

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

	t.Run("Event flow", func(t *testing.T) {
		jobsDB := testhelper.JobsDB(t, jobsDBPort)

		t.Cleanup(func() {
			require.Eventually(t, func() bool {
				if _, err := db.Exec(fmt.Sprintf(`DROP SCHEMA %[1]s CASCADE;`, namespace)); err != nil {
					t.Logf("error deleting schema: %v", err)
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
			warehouseEventsMap  testhelper.EventsCountMap
			loadTableStrategy   string
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
				loadTableStrategy:   "MERGE",
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
				loadTableStrategy:   "APPEND",
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
				loadTableStrategy:   "MERGE",
				useParquetLoadFiles: true,
				stagingFilePrefix:   "testdata/upload-job-parquet",
				jobRunID:            misc.FastUUID().String(),
			},
		}

		for _, tc := range testCases {
			tc := tc

			t.Run(tc.name, func(t *testing.T) {
				t.Setenv("RSERVER_WAREHOUSE_DELTALAKE_LOAD_TABLE_STRATEGY", tc.loadTableStrategy)
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
				ts1 := testhelper.TestConfig{
					WriteKey:      writeKey,
					Schema:        tc.schema,
					Tables:        tables,
					SourceID:      tc.sourceID,
					DestinationID: tc.destinationID,
					JobRunID:      tc.jobRunID,
					WarehouseEventsMap: testhelper.EventsCountMap{
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
					UserID:          testhelper.GetUserId(destType),
				}
				ts1.VerifyEvents(t)

				t.Log("verifying test case 2")
				ts2 := testhelper.TestConfig{
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
			require.Eventually(t, func() bool {
				if _, err := db.Exec(fmt.Sprintf(`DROP SCHEMA %[1]s CASCADE;`, namespace)); err != nil {
					t.Logf("error deleting schema: %v", err)
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
				t.Setenv("RSERVER_WAREHOUSE_DELTALAKE_USE_PARQUET_LOAD_FILES", strconv.FormatBool(tc.useParquetLoadFiles))

				for k, v := range tc.conf {
					dest.Config[k] = v
				}

				testhelper.VerifyConfigurationTest(t, dest)
			})
		}
	})

	t.Run("Load Table", func(t *testing.T) {
		const (
			sourceID        = "test_source-id"
			destinationID   = "test_destination-id"
			workspaceID     = "test_workspace-id"
			destinationType = warehouseutils.DELTALAKE
		)

		misc.Init()
		validations.Init()
		warehouseutils.Init()

		ctx := context.Background()

		namespace := testhelper.RandSchema(destinationType)

		t.Cleanup(func() {
			require.Eventually(t, func() bool {
				if _, err := db.Exec(fmt.Sprintf(`DROP SCHEMA %[1]s CASCADE;`, namespace)); err != nil {
					t.Logf("error deleting schema: %v", err)
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
			Provider: warehouseutils.AzureBlob,
			Config: map[string]any{
				"containerName":  deltaLakeCredentials.ContainerName,
				"accountName":    deltaLakeCredentials.AccountName,
				"accountKey":     deltaLakeCredentials.AccountKey,
				"bucketProvider": warehouseutils.AzureBlob,
			},
		})
		require.NoError(t, err)

		uploader := func(
			t testing.TB,
			loadFiles []warehouseutils.LoadFile,
			tableName string,
			schemaInUpload model.TableSchema,
			schemaInWarehouse model.TableSchema,
			loadFileType string,
			canAppend bool,
		) warehouseutils.Uploader {
			ctrl := gomock.NewController(t)
			t.Cleanup(ctrl.Finish)

			eventTS, err := time.Parse(time.RFC3339, "2022-12-15T06:53:49.640Z")
			require.NoError(t, err)

			mockUploader := mockuploader.NewMockUploader(ctrl)
			mockUploader.EXPECT().UseRudderStorage().Return(false).AnyTimes()
			mockUploader.EXPECT().CanAppend().Return(canAppend).AnyTimes()
			mockUploader.EXPECT().GetLoadFilesMetadata(gomock.Any(), gomock.Any()).DoAndReturn(
				func(ctx context.Context, options warehouseutils.GetLoadFilesOptions) []warehouseutils.LoadFile {
					return slices.Clone(loadFiles)
				},
			).AnyTimes()
			mockUploader.EXPECT().GetSampleLoadFileLocation(gomock.Any(), gomock.Any()).Return(loadFiles[0].Location, nil).AnyTimes()
			mockUploader.EXPECT().GetTableSchemaInUpload(tableName).Return(schemaInUpload).AnyTimes()
			mockUploader.EXPECT().GetTableSchemaInWarehouse(tableName).Return(schemaInWarehouse).AnyTimes()
			mockUploader.EXPECT().GetLoadFileType().Return(loadFileType).AnyTimes()
			mockUploader.EXPECT().GetFirstLastEvent().Return(eventTS, eventTS).AnyTimes()

			return mockUploader
		}

		t.Run("schema does not exists", func(t *testing.T) {
			tableName := "schema_not_exists_test_table"

			uploadOutput := testhelper.Upload(t, fm, "../testdata/load.csv.gz", tableName)

			loadFiles := []warehouseutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := uploader(
				t, loadFiles, tableName, schemaInUpload,
				schemaInWarehouse, warehouseutils.LoadFileTypeCsv, false,
			)

			warehouse := warehouseModel(namespace)

			d := deltalake.New(config.Default, logger.NOP, stats.Default)
			err := d.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			loadTableStat, err := d.LoadTable(ctx, tableName)
			require.ErrorContains(t, err, "The schema `"+namespace+"` cannot be found")
			require.Nil(t, loadTableStat)
		})
		t.Run("table does not exists", func(t *testing.T) {
			tableName := "table_not_exists_test_table"

			uploadOutput := testhelper.Upload(t, fm, "../testdata/load.csv.gz", tableName)

			loadFiles := []warehouseutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := uploader(
				t, loadFiles, tableName, schemaInUpload,
				schemaInWarehouse, warehouseutils.LoadFileTypeCsv, false,
			)

			warehouse := warehouseModel(namespace)

			d := deltalake.New(config.Default, logger.NOP, stats.Default)
			err := d.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = d.CreateSchema(ctx)
			require.NoError(t, err)

			loadTableStat, err := d.LoadTable(ctx, tableName)
			require.ErrorContains(t, err, "The table or view `"+namespace+"`.`table_not_exists_test_table` cannot be found")
			require.Nil(t, loadTableStat)
		})
		t.Run("load table stats", func(t *testing.T) {
			tableName := "load_table_stats_test_table"

			uploadOutput := testhelper.Upload(t, fm, "../testdata/load.csv.gz", tableName)

			loadFiles := []warehouseutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := uploader(
				t, loadFiles, tableName, schemaInUpload,
				schemaInWarehouse, warehouseutils.LoadFileTypeCsv, false,
			)

			warehouse := warehouseModel(namespace)

			d := deltalake.New(config.Default, logger.NOP, stats.Default)
			err := d.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = d.CreateSchema(ctx)
			require.NoError(t, err)

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

			records := testhelper.RecordsFromWarehouse(t, d.DB.DB,
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
				ORDER BY
				  id`,
					namespace,
					tableName,
				),
			)

			require.Equal(t, records, [][]string{
				{"6734e5db-f918-4efe-1421-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "", "", "", "125", ""},
				{"6734e5db-f918-4efe-2314-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "", "", "125.75", "", ""},
				{"6734e5db-f918-4efe-2352-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "", "2022-12-15 06:53:49.64 +0000 UTC", "", "", ""},
				{"6734e5db-f918-4efe-2414-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "false", "2022-12-15 06:53:49.64 +0000 UTC", "126.75", "126", "hello-world"},
				{"6734e5db-f918-4efe-3555-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "false", "", "", "", ""},
				{"6734e5db-f918-4efe-5152-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "", "", "", "", "hello-world"},
				{"6734e5db-f918-4efe-5323-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "", "", "", "", ""},
				{"7274e5db-f918-4efe-1212-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "true", "2022-12-15 06:53:49.64 +0000 UTC", "125.75", "125", "hello-world"},
				{"7274e5db-f918-4efe-1454-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "", "", "", "125", ""},
				{"7274e5db-f918-4efe-1511-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "", "", "", "", ""},
				{"7274e5db-f918-4efe-2323-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "", "", "125.75", "", ""},
				{"7274e5db-f918-4efe-4524-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "true", "", "", "", ""},
				{"7274e5db-f918-4efe-5151-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "", "", "", "", "hello-world"},
				{"7274e5db-f918-4efe-5322-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "", "2022-12-15 06:53:49.64 +0000 UTC", "", "", ""},
			})
		})
		t.Run("load file does not exists", func(t *testing.T) {
			tableName := "load_file_not_exists_test_table"

			loadFiles := []warehouseutils.LoadFile{{
				Location: "https://account.blob.core.windows.net/container/rudder-warehouse-load-objects/load_file_not_exists_test_table/test_source-id/a01af26e-4548-49ff-a895-258829cc1a83-load_file_not_exists_test_table/load.csv.gz",
			}}
			mockUploader := uploader(
				t, loadFiles, tableName, schemaInUpload,
				schemaInWarehouse, warehouseutils.LoadFileTypeCsv, false,
			)

			warehouse := warehouseModel(namespace)

			d := deltalake.New(config.Default, logger.NOP, stats.Default)
			err := d.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = d.CreateSchema(ctx)
			require.NoError(t, err)

			err = d.CreateTable(ctx, tableName, schemaInWarehouse)
			require.NoError(t, err)

			loadTableStat, err := d.LoadTable(ctx, tableName)
			require.ErrorContains(t, err, "Container container in account account.blob.core.windows.net not found")
			require.Nil(t, loadTableStat)
		})
		t.Run("mismatch in number of columns", func(t *testing.T) {
			tableName := "mismatch_columns_test_table"

			uploadOutput := testhelper.Upload(t, fm, "../testdata/mismatch-columns.csv.gz", tableName)

			loadFiles := []warehouseutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := uploader(
				t, loadFiles, tableName, schemaInUpload,
				schemaInWarehouse, warehouseutils.LoadFileTypeCsv, false,
			)

			warehouse := warehouseModel(namespace)

			d := deltalake.New(config.Default, logger.NOP, stats.Default)
			err := d.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = d.CreateSchema(ctx)
			require.NoError(t, err)

			err = d.CreateTable(ctx, tableName, schemaInWarehouse)
			require.NoError(t, err)

			loadTableStat, err := d.LoadTable(ctx, tableName)
			require.NoError(t, err)
			require.Equal(t, loadTableStat.RowsInserted, int64(14))
			require.Equal(t, loadTableStat.RowsUpdated, int64(0))

			records := testhelper.RecordsFromWarehouse(t, d.DB.DB,
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
				ORDER BY
				  id`,
					namespace,
					tableName,
				),
			)

			require.Equal(t, records, [][]string{
				{"6734e5db-f918-4efe-1421-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "", "", "", "125", ""},
				{"6734e5db-f918-4efe-2314-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "", "", "125.75", "", ""},
				{"6734e5db-f918-4efe-2352-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "", "2022-12-15 06:53:49.64 +0000 UTC", "", "", ""},
				{"6734e5db-f918-4efe-2414-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "false", "2022-12-15 06:53:49.64 +0000 UTC", "126.75", "126", "hello-world"},
				{"6734e5db-f918-4efe-3555-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "false", "", "", "", ""},
				{"6734e5db-f918-4efe-5152-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "", "", "", "", "hello-world"},
				{"6734e5db-f918-4efe-5323-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "", "", "", "", ""},
				{"7274e5db-f918-4efe-1212-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "true", "2022-12-15 06:53:49.64 +0000 UTC", "125.75", "125", "hello-world"},
				{"7274e5db-f918-4efe-1454-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "", "", "", "125", ""},
				{"7274e5db-f918-4efe-1511-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "", "", "", "", ""},
				{"7274e5db-f918-4efe-2323-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "", "", "125.75", "", ""},
				{"7274e5db-f918-4efe-4524-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "true", "", "", "", ""},
				{"7274e5db-f918-4efe-5151-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "", "", "", "", "hello-world"},
				{"7274e5db-f918-4efe-5322-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "", "2022-12-15 06:53:49.64 +0000 UTC", "", "", ""},
			})
		})
		t.Run("mismatch in schema", func(t *testing.T) {
			tableName := "mismatch_schema_test_table"

			uploadOutput := testhelper.Upload(t, fm, "../testdata/mismatch-schema.csv.gz", tableName)

			loadFiles := []warehouseutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := uploader(
				t, loadFiles, tableName, schemaInUpload,
				schemaInWarehouse, warehouseutils.LoadFileTypeCsv, false,
			)

			warehouse := warehouseModel(namespace)

			d := deltalake.New(config.Default, logger.NOP, stats.Default)
			err := d.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = d.CreateSchema(ctx)
			require.NoError(t, err)

			err = d.CreateTable(ctx, tableName, schemaInWarehouse)
			require.NoError(t, err)

			loadTableStat, err := d.LoadTable(ctx, tableName)
			require.NoError(t, err)
			require.Equal(t, loadTableStat.RowsInserted, int64(14))
			require.Equal(t, loadTableStat.RowsUpdated, int64(0))

			records := testhelper.RecordsFromWarehouse(t, d.DB.DB,
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
				ORDER BY
				  id`,
					namespace,
					tableName,
				),
			)

			require.Equal(t, records, [][]string{
				{"6734e5db-f918-4efe-1421-872f66e235c5", "", "", "", "", "", ""},
				{"6734e5db-f918-4efe-2314-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "", "", "125.75", "", ""},
				{"6734e5db-f918-4efe-2352-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "", "2022-12-15 06:53:49.64 +0000 UTC", "", "", ""},
				{"6734e5db-f918-4efe-2414-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "false", "2022-12-15 06:53:49.64 +0000 UTC", "126.75", "126", "hello-world"},
				{"6734e5db-f918-4efe-3555-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "false", "", "", "", ""},
				{"6734e5db-f918-4efe-5152-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "", "", "", "", "hello-world"},
				{"6734e5db-f918-4efe-5323-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "", "", "", "", ""},
				{"7274e5db-f918-4efe-1212-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "true", "2022-12-15 06:53:49.64 +0000 UTC", "125.75", "125", "hello-world"},
				{"7274e5db-f918-4efe-1454-872f66e235c5", "", "", "", "", "", ""},
				{"7274e5db-f918-4efe-1511-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "", "", "", "", ""},
				{"7274e5db-f918-4efe-2323-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "", "", "125.75", "", ""},
				{"7274e5db-f918-4efe-4524-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "true", "", "", "", ""},
				{"7274e5db-f918-4efe-5151-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "", "", "", "", "hello-world"},
				{"7274e5db-f918-4efe-5322-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "", "2022-12-15 06:53:49.64 +0000 UTC", "", "", ""},
			})
		})
		t.Run("discards", func(t *testing.T) {
			tableName := warehouseutils.DiscardsTable

			uploadOutput := testhelper.Upload(t, fm, "../testdata/discards.csv.gz", tableName)

			loadFiles := []warehouseutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := uploader(
				t, loadFiles, tableName, warehouseutils.DiscardsSchema,
				warehouseutils.DiscardsSchema, warehouseutils.LoadFileTypeCsv, false,
			)

			warehouse := warehouseModel(namespace)

			d := deltalake.New(config.Default, logger.NOP, stats.Default)
			err := d.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = d.CreateSchema(ctx)
			require.NoError(t, err)

			err = d.CreateTable(ctx, tableName, warehouseutils.DiscardsSchema)
			require.NoError(t, err)

			loadTableStat, err := d.LoadTable(ctx, tableName)
			require.NoError(t, err)
			require.Equal(t, loadTableStat.RowsInserted, int64(6))
			require.Equal(t, loadTableStat.RowsUpdated, int64(0))

			records := testhelper.RecordsFromWarehouse(t, d.DB.DB,
				fmt.Sprintf(`
				SELECT
				  column_name,
				  column_value,
				  received_at,
				  row_id,
				  table_name,
				  uuid_ts
				FROM
				  %s.%s
				ORDER BY row_id ASC;`,
					namespace,
					tableName,
				),
			)
			require.Equal(t, records, [][]string{
				{"context_screen_density", "125.75", "2022-12-15 06:53:49.64 +0000 UTC", "1", "test_table", "2022-12-15 06:53:49.64 +0000 UTC"},
				{"context_screen_density", "125", "2022-12-15 06:53:49.64 +0000 UTC", "2", "test_table", "2022-12-15 06:53:49.64 +0000 UTC"},
				{"context_screen_density", "true", "2022-12-15 06:53:49.64 +0000 UTC", "3", "test_table", "2022-12-15 06:53:49.64 +0000 UTC"},
				{"context_screen_density", "7274e5db-f918-4efe-1212-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "4", "test_table", "2022-12-15 06:53:49.64 +0000 UTC"},
				{"context_screen_density", "hello-world", "2022-12-15 06:53:49.64 +0000 UTC", "5", "test_table", "2022-12-15 06:53:49.64 +0000 UTC"},
				{"context_screen_density", "2022-12-15T06:53:49.640Z", "2022-12-15 06:53:49.64 +0000 UTC", "6", "test_table", "2022-12-15 06:53:49.64 +0000 UTC"},
			})
		})
		t.Run("Parquet", func(t *testing.T) {
			tableName := "parquet_test_table"

			uploadOutput := testhelper.Upload(t, fm, "../testdata/load.parquet", tableName)

			loadFiles := []warehouseutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := uploader(
				t, loadFiles, tableName, schemaInUpload,
				schemaInWarehouse, warehouseutils.LoadFileTypeParquet, false,
			)

			warehouse := warehouseModel(namespace)

			d := deltalake.New(config.Default, logger.NOP, stats.Default)
			err := d.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = d.CreateSchema(ctx)
			require.NoError(t, err)

			err = d.CreateTable(ctx, tableName, schemaInWarehouse)
			require.NoError(t, err)

			loadTableStat, err := d.LoadTable(ctx, tableName)
			require.NoError(t, err)
			require.Equal(t, loadTableStat.RowsInserted, int64(14))
			require.Equal(t, loadTableStat.RowsUpdated, int64(0))

			records := testhelper.RecordsFromWarehouse(t, d.DB.DB,
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
				ORDER BY
				  id`,
					namespace,
					tableName,
				),
			)

			require.Equal(t, records, [][]string{
				{"6734e5db-f918-4efe-1421-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "", "", "", "125", ""},
				{"6734e5db-f918-4efe-2314-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "", "", "125.75", "", ""},
				{"6734e5db-f918-4efe-2352-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "", "2022-12-15 06:53:49.64 +0000 UTC", "", "", ""},
				{"6734e5db-f918-4efe-2414-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "false", "2022-12-15 06:53:49.64 +0000 UTC", "126.75", "126", "hello-world"},
				{"6734e5db-f918-4efe-3555-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "false", "", "", "", ""},
				{"6734e5db-f918-4efe-5152-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "", "", "", "", "hello-world"},
				{"6734e5db-f918-4efe-5323-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "", "", "", "", ""},
				{"7274e5db-f918-4efe-1212-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "true", "2022-12-15 06:53:49.64 +0000 UTC", "125.75", "125", "hello-world"},
				{"7274e5db-f918-4efe-1454-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "", "", "", "125", ""},
				{"7274e5db-f918-4efe-1511-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "", "", "", "", ""},
				{"7274e5db-f918-4efe-2323-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "", "", "125.75", "", ""},
				{"7274e5db-f918-4efe-4524-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "true", "", "", "", ""},
				{"7274e5db-f918-4efe-5151-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "", "", "", "", "hello-world"},
				{"7274e5db-f918-4efe-5322-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "", "2022-12-15 06:53:49.64 +0000 UTC", "", "", ""},
			})
		})
		t.Run("append", func(t *testing.T) {
			tableName := "append_test_table"

			uploadOutput := testhelper.Upload(t, fm, "../testdata/load.csv.gz", tableName)

			loadFiles := []warehouseutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := uploader(
				t, loadFiles, tableName, schemaInUpload,
				schemaInWarehouse, warehouseutils.LoadFileTypeCsv, true,
			)

			warehouse := warehouseModel(namespace)

			c := config.New()
			c.Set("Warehouse.deltalake.loadTableStrategy", "APPEND")

			d := deltalake.New(c, logger.NOP, stats.Default)
			err := d.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = d.CreateSchema(ctx)
			require.NoError(t, err)

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

			records := testhelper.RecordsFromWarehouse(t, d.DB.DB,
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
				ORDER BY
				  id`,
					namespace,
					tableName,
				),
			)

			require.Equal(t, records, [][]string{
				{"6734e5db-f918-4efe-1421-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "", "", "", "125", ""},
				{"6734e5db-f918-4efe-1421-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "", "", "", "125", ""},
				{"6734e5db-f918-4efe-2314-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "", "", "125.75", "", ""},
				{"6734e5db-f918-4efe-2314-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "", "", "125.75", "", ""},
				{"6734e5db-f918-4efe-2352-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "", "2022-12-15 06:53:49.64 +0000 UTC", "", "", ""},
				{"6734e5db-f918-4efe-2352-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "", "2022-12-15 06:53:49.64 +0000 UTC", "", "", ""},
				{"6734e5db-f918-4efe-2414-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "false", "2022-12-15 06:53:49.64 +0000 UTC", "126.75", "126", "hello-world"},
				{"6734e5db-f918-4efe-2414-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "false", "2022-12-15 06:53:49.64 +0000 UTC", "126.75", "126", "hello-world"},
				{"6734e5db-f918-4efe-3555-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "false", "", "", "", ""},
				{"6734e5db-f918-4efe-3555-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "false", "", "", "", ""},
				{"6734e5db-f918-4efe-5152-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "", "", "", "", "hello-world"},
				{"6734e5db-f918-4efe-5152-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "", "", "", "", "hello-world"},
				{"6734e5db-f918-4efe-5323-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "", "", "", "", ""},
				{"6734e5db-f918-4efe-5323-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "", "", "", "", ""},
				{"7274e5db-f918-4efe-1212-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "true", "2022-12-15 06:53:49.64 +0000 UTC", "125.75", "125", "hello-world"},
				{"7274e5db-f918-4efe-1212-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "true", "2022-12-15 06:53:49.64 +0000 UTC", "125.75", "125", "hello-world"},
				{"7274e5db-f918-4efe-1454-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "", "", "", "125", ""},
				{"7274e5db-f918-4efe-1454-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "", "", "", "125", ""},
				{"7274e5db-f918-4efe-1511-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "", "", "", "", ""},
				{"7274e5db-f918-4efe-1511-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "", "", "", "", ""},
				{"7274e5db-f918-4efe-2323-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "", "", "125.75", "", ""},
				{"7274e5db-f918-4efe-2323-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "", "", "125.75", "", ""},
				{"7274e5db-f918-4efe-4524-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "true", "", "", "", ""},
				{"7274e5db-f918-4efe-4524-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "true", "", "", "", ""},
				{"7274e5db-f918-4efe-5151-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "", "", "", "", "hello-world"},
				{"7274e5db-f918-4efe-5151-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "", "", "", "", "hello-world"},
				{"7274e5db-f918-4efe-5322-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "", "2022-12-15 06:53:49.64 +0000 UTC", "", "", ""},
				{"7274e5db-f918-4efe-5322-872f66e235c5", "2022-12-15 06:53:49.64 +0000 UTC", "", "2022-12-15 06:53:49.64 +0000 UTC", "", "", ""},
			})
		})
		t.Run("no partition", func(t *testing.T) {
			tableName := "no_partition_stats_test_table"

			uploadOutput := testhelper.Upload(t, fm, "../testdata/load.csv.gz", tableName)

			loadFiles := []warehouseutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := uploader(
				t, loadFiles, tableName, schemaInUpload,
				schemaInWarehouse, warehouseutils.LoadFileTypeCsv, false,
			)

			warehouse := warehouseModel(namespace)

			d := deltalake.New(config.Default, logger.NOP, stats.Default)
			err := d.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = d.CreateSchema(ctx)
			require.NoError(t, err)

			_, err = d.DB.QueryContext(ctx, `
			CREATE TABLE IF NOT EXISTS `+namespace+`.`+tableName+` (
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
			) USING DELTA;
`)
			require.NoError(t, err)

			loadTableStat, err := d.LoadTable(ctx, tableName)
			require.NoError(t, err)
			require.Equal(t, loadTableStat.RowsInserted, int64(14))
			require.Equal(t, loadTableStat.RowsUpdated, int64(0))
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

			d := deltalake.New(c, logger.NOP, stats.Default)
			require.Equal(t, d.TrimErrorMessage(tc.inputError), tc.expectedError)
		})
	}
}

func TestDeltalake_ShouldAppend(t *testing.T) {
	testCases := []struct {
		name                  string
		loadTableStrategy     string
		uploaderCanAppend     bool
		uploaderExpectedCalls int
		expected              bool
	}{
		{
			name:                  "uploader says we can append and we are in append mode",
			loadTableStrategy:     "APPEND",
			uploaderCanAppend:     true,
			uploaderExpectedCalls: 1,
			expected:              true,
		},
		{
			name:                  "uploader says we cannot append and we are in append mode",
			loadTableStrategy:     "APPEND",
			uploaderCanAppend:     false,
			uploaderExpectedCalls: 1,
			expected:              false,
		},
		{
			name:                  "uploader says we can append and we are in merge mode",
			loadTableStrategy:     "MERGE",
			uploaderCanAppend:     true,
			uploaderExpectedCalls: 0,
			expected:              false,
		},
		{
			name:                  "uploader says we cannot append and we are in merge mode",
			loadTableStrategy:     "MERGE",
			uploaderCanAppend:     false,
			uploaderExpectedCalls: 0,
			expected:              false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			c := config.New()
			c.Set("Warehouse.deltalake.loadTableStrategy", tc.loadTableStrategy)

			d := deltalake.New(c, logger.NOP, stats.Default)

			mockCtrl := gomock.NewController(t)
			uploader := mockuploader.NewMockUploader(mockCtrl)
			uploader.EXPECT().CanAppend().Times(tc.uploaderExpectedCalls).Return(tc.uploaderCanAppend)

			d.Uploader = uploader
			require.Equal(t, d.ShouldAppend(), tc.expected)
		})
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
	}
}

func appendEventsMap() testhelper.EventsCountMap {
	return testhelper.EventsCountMap{
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
