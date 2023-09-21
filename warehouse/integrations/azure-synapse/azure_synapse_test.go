package azuresynapse_test

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	azuresynapse "github.com/rudderlabs/rudder-server/warehouse/integrations/azure-synapse"
	mockuploader "github.com/rudderlabs/rudder-server/warehouse/internal/mocks/utils"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/rudderlabs/compose-test/compose"

	"github.com/rudderlabs/rudder-server/testhelper/workspaceConfig"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/compose-test/testcompose"
	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/runner"
	"github.com/rudderlabs/rudder-server/testhelper/health"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/testhelper"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/rudderlabs/rudder-server/warehouse/validations"
)

func TestIntegration(t *testing.T) {
	if os.Getenv("SLOW") != "1" {
		t.Skip("Skipping tests. Add 'SLOW=1' env var to run test.")
	}

	c := testcompose.New(t, compose.FilePaths([]string{
		"testdata/docker-compose.yml",
		"../testdata/docker-compose.jobsdb.yml",
		"../testdata/docker-compose.minio.yml",
	}))
	c.Start(context.Background())

	misc.Init()
	validations.Init()
	warehouseutils.Init()

	jobsDBPort := c.Port("jobsDb", 5432)
	minioPort := c.Port("minio", 9000)
	azureSynapsePort := c.Port("azure_synapse", 1433)

	httpPort, err := kithelper.GetFreePort()
	require.NoError(t, err)

	workspaceID := warehouseutils.RandHex()
	sourceID := warehouseutils.RandHex()
	destinationID := warehouseutils.RandHex()
	writeKey := warehouseutils.RandHex()

	destType := warehouseutils.AzureSynapse

	namespace := testhelper.RandSchema(destType)

	host := "localhost"
	database := "master"
	user := "SA"
	password := "reallyStrongPwd123"

	bucketName := "testbucket"
	accessKeyID := "MYACCESSKEY"
	secretAccessKey := "MYSECRETKEY"

	minioEndpoint := fmt.Sprintf("localhost:%d", minioPort)

	templateConfigurations := map[string]any{
		"workspaceID":     workspaceID,
		"sourceID":        sourceID,
		"destinationID":   destinationID,
		"writeKey":        writeKey,
		"host":            host,
		"database":        database,
		"user":            user,
		"password":        password,
		"port":            strconv.Itoa(azureSynapsePort),
		"namespace":       namespace,
		"bucketName":      bucketName,
		"accessKeyID":     accessKeyID,
		"secretAccessKey": secretAccessKey,
		"endPoint":        minioEndpoint,
	}
	workspaceConfigPath := workspaceConfig.CreateTempFile(t, "testdata/template.json", templateConfigurations)

	testhelper.EnhanceWithDefaultEnvs(t)
	t.Setenv("JOBS_DB_PORT", strconv.Itoa(jobsDBPort))
	t.Setenv("WAREHOUSE_JOBS_DB_PORT", strconv.Itoa(jobsDBPort))
	t.Setenv("MINIO_ACCESS_KEY_ID", accessKeyID)
	t.Setenv("MINIO_SECRET_ACCESS_KEY", secretAccessKey)
	t.Setenv("MINIO_MINIO_ENDPOINT", minioEndpoint)
	t.Setenv("MINIO_SSL", "false")
	t.Setenv("RSERVER_WAREHOUSE_AZURE_SYNAPSE_MAX_PARALLEL_LOADS", "8")
	t.Setenv("RSERVER_WAREHOUSE_WEB_PORT", strconv.Itoa(httpPort))
	t.Setenv("RSERVER_BACKEND_CONFIG_CONFIG_JSONPATH", workspaceConfigPath)
	t.Setenv("RSERVER_WAREHOUSE_AZURE_SYNAPSE_SLOW_QUERY_THRESHOLD", "0s")

	svcDone := make(chan struct{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		r := runner.New(runner.ReleaseInfo{})
		_ = r.Run(ctx, []string{"azure-synapse-integration-test"})

		close(svcDone)
	}()
	t.Cleanup(func() { <-svcDone })

	serviceHealthEndpoint := fmt.Sprintf("http://localhost:%d/health", httpPort)
	health.WaitUntilReady(ctx, t, serviceHealthEndpoint, time.Minute, time.Second, "serviceHealthEndpoint")

	t.Run("Events flow", func(t *testing.T) {
		jobsDB := testhelper.JobsDB(t, jobsDBPort)

		dsn := fmt.Sprintf("sqlserver://%s:%s@%s:%d?TrustServerCertificate=true&database=%s&encrypt=disable",
			user,
			password,
			host,
			azureSynapsePort,
			database,
		)
		db, err := sql.Open("sqlserver", dsn)
		require.NoError(t, err)
		require.NoError(t, db.Ping())

		testcase := []struct {
			name          string
			writeKey      string
			schema        string
			sourceID      string
			destinationID string
			tables        []string
		}{
			{
				name:          "Upload Job",
				writeKey:      writeKey,
				schema:        namespace,
				tables:        []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"},
				sourceID:      sourceID,
				destinationID: destinationID,
			},
		}

		for _, tc := range testcase {
			tc := tc

			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()

				sqlClient := &client.Client{
					SQL:  db,
					Type: client.SQLClient,
				}

				conf := map[string]interface{}{
					"bucketProvider":   "MINIO",
					"bucketName":       bucketName,
					"accessKeyID":      accessKeyID,
					"secretAccessKey":  secretAccessKey,
					"useSSL":           false,
					"endPoint":         minioEndpoint,
					"useRudderStorage": false,
				}

				t.Log("verifying test case 1")
				ts1 := testhelper.TestConfig{
					WriteKey:        tc.writeKey,
					Schema:          tc.schema,
					Tables:          tc.tables,
					SourceID:        tc.sourceID,
					DestinationID:   tc.destinationID,
					Config:          conf,
					WorkspaceID:     workspaceID,
					DestinationType: destType,
					JobsDB:          jobsDB,
					HTTPPort:        httpPort,
					Client:          sqlClient,
					JobRunID:        misc.FastUUID().String(),
					TaskRunID:       misc.FastUUID().String(),
					StagingFilePath: "testdata/upload-job.staging-1.json",
					UserID:          testhelper.GetUserId(destType),
				}
				ts1.VerifyEvents(t)

				t.Log("verifying test case 2")
				ts2 := testhelper.TestConfig{
					WriteKey:        tc.writeKey,
					Schema:          tc.schema,
					Tables:          tc.tables,
					SourceID:        tc.sourceID,
					DestinationID:   tc.destinationID,
					Config:          conf,
					WorkspaceID:     workspaceID,
					DestinationType: destType,
					JobsDB:          jobsDB,
					HTTPPort:        httpPort,
					Client:          sqlClient,
					JobRunID:        misc.FastUUID().String(),
					TaskRunID:       misc.FastUUID().String(),
					StagingFilePath: "testdata/upload-job.staging-2.json",
					UserID:          testhelper.GetUserId(destType),
				}
				ts2.VerifyEvents(t)
			})
		}
	})

	t.Run("Validations", func(t *testing.T) {
		dest := backendconfig.DestinationT{
			ID: destinationID,
			Config: map[string]interface{}{
				"host":             host,
				"database":         database,
				"user":             user,
				"password":         password,
				"port":             strconv.Itoa(azureSynapsePort),
				"sslMode":          "disable",
				"namespace":        "",
				"bucketProvider":   "MINIO",
				"bucketName":       bucketName,
				"accessKeyID":      accessKeyID,
				"secretAccessKey":  secretAccessKey,
				"useSSL":           false,
				"endPoint":         minioEndpoint,
				"syncFrequency":    "30",
				"useRudderStorage": false,
			},
			DestinationDefinition: backendconfig.DestinationDefinitionT{
				ID:          "1qvbUYC2xVQ7lvI9UUYkkM4IBt9",
				Name:        "AZURE_SYNAPSE",
				DisplayName: "Microsoft SQL Server",
			},
			Name:       "azure-synapse-demo",
			Enabled:    true,
			RevisionID: destinationID,
		}
		testhelper.VerifyConfigurationTest(t, dest)
	})
}

func TestAzureSynapse_LoadTable(t *testing.T) {
	const (
		sourceID         = "test_source-id"
		destinationID    = "test_destination-id"
		namespace        = "test_namespace"
		workspaceID      = "test_workspace-id"
		loadObjectFolder = "rudder-warehouse-load-objects"
		destinationType  = warehouseutils.AzureSynapse

		host     = "localhost"
		database = "master"
		user     = "SA"
		password = "reallyStrongPwd123"

		bucketName      = "testbucket"
		accessKeyID     = "MYACCESSKEY"
		secretAccessKey = "MYSECRETKEY"
		region          = "us-east-1"
	)

	misc.Init()
	validations.Init()
	warehouseutils.Init()

	c := testcompose.New(t, compose.FilePaths([]string{
		"testdata/docker-compose.yml",
		"../testdata/docker-compose.minio.yml",
	}))
	c.Start(context.Background())

	minioPort := c.Port("minio", 9000)
	azureSynapsePort := c.Port("azure_synapse", 1433)
	minioEndpoint := fmt.Sprintf("localhost:%d", minioPort)

	ctx := context.Background()

	warehouse := model.Warehouse{
		Source: backendconfig.SourceT{
			ID: sourceID,
		},
		Destination: backendconfig.DestinationT{
			ID: destinationID,
			DestinationDefinition: backendconfig.DestinationDefinitionT{
				Name: destinationType,
			},
			Config: map[string]interface{}{
				"host":             host,
				"database":         database,
				"user":             user,
				"password":         password,
				"port":             strconv.Itoa(azureSynapsePort),
				"sslMode":          "disable",
				"namespace":        "",
				"bucketProvider":   "MINIO",
				"bucketName":       bucketName,
				"accessKeyID":      accessKeyID,
				"secretAccessKey":  secretAccessKey,
				"useSSL":           false,
				"endPoint":         minioEndpoint,
				"syncFrequency":    "30",
				"useRudderStorage": false,
			},
		},
		WorkspaceID: workspaceID,
		Namespace:   namespace,
	}

	fm, err := filemanager.New(&filemanager.Settings{
		Provider: warehouseutils.MINIO,
		Config: map[string]any{
			"bucketName":       bucketName,
			"accessKeyID":      accessKeyID,
			"secretAccessKey":  secretAccessKey,
			"endPoint":         minioEndpoint,
			"forcePathStyle":   true,
			"s3ForcePathStyle": true,
			"disableSSL":       true,
			"region":           region,
			"enableSSE":        false,
			"bucketProvider":   warehouseutils.MINIO,
		},
	})
	require.NoError(t, err)

	t.Run("load table stats", func(t *testing.T) {
		tableName := "load_table_stats_test_table"

		f, err := os.Open("testdata/load.csv.gz")
		require.NoError(t, err)

		defer func() { _ = f.Close() }()

		uploadOutput, err := fm.Upload(
			context.Background(), f, loadObjectFolder,
			tableName, sourceID, uuid.New().String()+"-"+tableName,
		)
		require.NoError(t, err)

		ctrl := gomock.NewController(t)
		mockUploader := mockuploader.NewMockUploader(ctrl)
		mockUploader.EXPECT().UseRudderStorage().Return(false).AnyTimes()
		mockUploader.EXPECT().GetLoadFilesMetadata(gomock.Any(), gomock.Any()).Return([]warehouseutils.LoadFile{{
			Location: uploadOutput.Location,
		}}).AnyTimes()
		mockUploader.EXPECT().GetTableSchemaInUpload(tableName).Return(model.TableSchema{
			"test_bool":     "boolean",
			"test_datetime": "datetime",
			"test_float":    "float",
			"test_int":      "int",
			"test_string":   "string",
			"id":            "string",
			"received_at":   "datetime",
		}).AnyTimes()
		mockUploader.EXPECT().GetTableSchemaInWarehouse(tableName).Return(model.TableSchema{
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
		}).AnyTimes()

		az := azuresynapse.New(config.Default, logger.NOP, stats.Default)
		err = az.Setup(ctx, warehouse, mockUploader)
		require.NoError(t, err)

		err = az.CreateSchema(ctx)
		require.NoError(t, err)

		err = az.CreateTable(ctx, tableName, model.TableSchema{
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
		})
		require.NoError(t, err)

		loadTableStat, err := az.LoadTable(ctx, tableName)
		require.NoError(t, err)
		require.Equal(t, loadTableStat.RowsInserted, int64(14))
		require.Equal(t, loadTableStat.RowsUpdated, int64(0))

		loadTableStat, err = az.LoadTable(ctx, tableName)
		require.NoError(t, err)
		require.Equal(t, loadTableStat.RowsInserted, int64(0))
		require.Equal(t, loadTableStat.RowsUpdated, int64(14))
	})
	t.Run("schema does not exists", func(t *testing.T) {
		tableName := "schema_not_exists_test_table"

		f, err := os.Open("testdata/load.csv.gz")
		require.NoError(t, err)

		defer func() { _ = f.Close() }()

		uploadOutput, err := fm.Upload(
			context.Background(), f, loadObjectFolder,
			tableName, sourceID, uuid.New().String()+"-"+tableName,
		)
		require.NoError(t, err)

		ctrl := gomock.NewController(t)
		mockUploader := mockuploader.NewMockUploader(ctrl)
		mockUploader.EXPECT().UseRudderStorage().Return(false).AnyTimes()
		mockUploader.EXPECT().GetLoadFilesMetadata(gomock.Any(), gomock.Any()).Return([]warehouseutils.LoadFile{{
			Location: uploadOutput.Location,
		}}).AnyTimes()
		mockUploader.EXPECT().GetTableSchemaInUpload(tableName).Return(model.TableSchema{
			"test_bool":     "boolean",
			"test_datetime": "datetime",
			"test_float":    "float",
			"test_int":      "int",
			"test_string":   "string",
			"id":            "string",
			"received_at":   "datetime",
		}).AnyTimes()
		mockUploader.EXPECT().GetTableSchemaInWarehouse(tableName).Return(model.TableSchema{
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
		}).AnyTimes()

		az := azuresynapse.New(config.Default, logger.NOP, stats.Default)
		err = az.Setup(ctx, warehouse, mockUploader)
		require.NoError(t, err)

		loadTableStat, err := az.LoadTable(ctx, tableName)
		require.ErrorContains(t, err, "reating temporary table: mssql: Invalid object name 'test_namespace.schema_not_exists_test_table'.")
		require.Nil(t, loadTableStat)
	})
	t.Run("table does not exists", func(t *testing.T) {
		tableName := "table_not_exists_test_table"

		f, err := os.Open("testdata/load.csv.gz")
		require.NoError(t, err)

		defer func() { _ = f.Close() }()

		uploadOutput, err := fm.Upload(
			context.Background(), f, loadObjectFolder,
			tableName, sourceID, uuid.New().String()+"-"+tableName,
		)
		require.NoError(t, err)

		ctrl := gomock.NewController(t)
		mockUploader := mockuploader.NewMockUploader(ctrl)
		mockUploader.EXPECT().UseRudderStorage().Return(false).AnyTimes()
		mockUploader.EXPECT().GetLoadFilesMetadata(gomock.Any(), gomock.Any()).Return([]warehouseutils.LoadFile{{
			Location: uploadOutput.Location,
		}}).AnyTimes()
		mockUploader.EXPECT().GetTableSchemaInUpload(tableName).Return(model.TableSchema{
			"test_bool":     "boolean",
			"test_datetime": "datetime",
			"test_float":    "float",
			"test_int":      "int",
			"test_string":   "string",
			"id":            "string",
			"received_at":   "datetime",
		}).AnyTimes()
		mockUploader.EXPECT().GetTableSchemaInWarehouse(tableName).Return(model.TableSchema{
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
		}).AnyTimes()

		az := azuresynapse.New(config.Default, logger.NOP, stats.Default)
		err = az.Setup(ctx, warehouse, mockUploader)
		require.NoError(t, err)

		err = az.CreateSchema(ctx)
		require.NoError(t, err)

		loadTableStat, err := az.LoadTable(ctx, tableName)
		require.ErrorContains(t, err, "creating temporary table: mssql: Invalid object name 'test_namespace.table_not_exists_test_table'.")
		require.Nil(t, loadTableStat)
	})
	t.Run("load file does not exists", func(t *testing.T) {
		tableName := "load_file_not_exists_test_table"

		ctrl := gomock.NewController(t)
		mockUploader := mockuploader.NewMockUploader(ctrl)
		mockUploader.EXPECT().UseRudderStorage().Return(false).AnyTimes()
		mockUploader.EXPECT().GetLoadFilesMetadata(gomock.Any(), gomock.Any()).Return([]warehouseutils.LoadFile{{
			Location: "http://localhost:1234/testbucket/rudder-warehouse-load-objects/load_file_not_exists_test_table/test_source-id/f31af97e-03e8-46d0-8a1a-1786cb85b22c-load_file_not_exists_test_table/load.csv.gz",
		}}).AnyTimes()
		mockUploader.EXPECT().GetTableSchemaInUpload(tableName).Return(model.TableSchema{
			"test_bool":     "boolean",
			"test_datetime": "datetime",
			"test_float":    "float",
			"test_int":      "int",
			"test_string":   "string",
			"id":            "string",
			"received_at":   "datetime",
		}).AnyTimes()
		mockUploader.EXPECT().GetTableSchemaInWarehouse(tableName).Return(model.TableSchema{
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
		}).AnyTimes()

		az := azuresynapse.New(config.Default, logger.NOP, stats.Default)
		err = az.Setup(ctx, warehouse, mockUploader)
		require.NoError(t, err)

		err = az.CreateSchema(ctx)
		require.NoError(t, err)

		err = az.CreateTable(ctx, tableName, model.TableSchema{
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
		})
		require.NoError(t, err)

		loadTableStat, err := az.LoadTable(ctx, tableName)
		require.ErrorContains(t, err, "downloading load files")
		require.Nil(t, loadTableStat)
	})
	t.Run("mismatch in number of columns", func(t *testing.T) {
		tableName := "mismatch_test_table"

		f, err := os.Open("testdata/mismatch.csv.gz")
		require.NoError(t, err)

		defer func() { _ = f.Close() }()

		uploadOutput, err := fm.Upload(
			context.Background(), f, loadObjectFolder,
			tableName, sourceID, uuid.New().String()+"-"+tableName,
		)
		require.NoError(t, err)

		ctrl := gomock.NewController(t)
		mockUploader := mockuploader.NewMockUploader(ctrl)
		mockUploader.EXPECT().UseRudderStorage().Return(false).AnyTimes()
		mockUploader.EXPECT().GetLoadFilesMetadata(gomock.Any(), gomock.Any()).Return([]warehouseutils.LoadFile{{
			Location: uploadOutput.Location,
		}}).AnyTimes()
		mockUploader.EXPECT().GetTableSchemaInUpload(tableName).Return(model.TableSchema{
			"test_bool":     "boolean",
			"test_datetime": "datetime",
			"test_float":    "float",
			"test_int":      "int",
			"test_string":   "string",
			"id":            "string",
			"received_at":   "datetime",
		}).AnyTimes()
		mockUploader.EXPECT().GetTableSchemaInWarehouse(tableName).Return(model.TableSchema{
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
		}).AnyTimes()

		az := azuresynapse.New(config.Default, logger.NOP, stats.Default)
		err = az.Setup(ctx, warehouse, mockUploader)
		require.NoError(t, err)

		err = az.CreateSchema(ctx)
		require.NoError(t, err)

		err = az.CreateTable(ctx, tableName, model.TableSchema{
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
		})
		require.NoError(t, err)

		loadTableStat, err := az.LoadTable(ctx, tableName)
		require.ErrorContains(t, err, "mismatch in number of columns")
		require.Nil(t, loadTableStat)
	})
}
