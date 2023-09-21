package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"github.com/rudderlabs/compose-test/compose"
	"github.com/rudderlabs/compose-test/testcompose"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/mssql"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/postgres"
	"github.com/rudderlabs/rudder-server/warehouse/validations"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/golang/mock/gomock"

	mockuploader "github.com/rudderlabs/rudder-server/warehouse/internal/mocks/utils"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"

	"github.com/docker/docker/pkg/fileutils"
	"github.com/google/uuid"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/utils/misc"
	sqlmiddleware "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type mockLoadFileUploader struct {
	mockFiles map[string][]string
	mockError map[string]error
}

func (m *mockLoadFileUploader) Download(_ context.Context, tableName string) ([]string, error) {
	return m.mockFiles[tableName], m.mockError[tableName]
}

func cloneFiles(t *testing.T, files []string) []string {
	tempFiles := make([]string, len(files))
	for i, file := range files {
		src := fmt.Sprintf("testdata/%s", file)
		dst := fmt.Sprintf("testdata/%s-%s", uuid.New().String(), file)

		dirPath := filepath.Dir(dst)
		err := os.MkdirAll(dirPath, os.ModePerm)
		require.NoError(t, err)

		_, err = fileutils.CopyFile(src, dst)
		require.NoError(t, err)

		t.Cleanup(func() { _ = os.Remove(dst) })

		tempFiles[i] = dst
	}
	return tempFiles
}

func TestLoadTable(t *testing.T) {
	const (
		sourceID         = "test_source-id"
		destinationID    = "test_destination-id"
		namespace        = "test_namespace"
		workspaceID      = "test_workspace-id"
		loadObjectFolder = "rudder-warehouse-load-objects"
		destinationType  = warehouseutils.POSTGRES

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
		"testdata/docker-compose.postgres.yml",
		"../testdata/docker-compose.minio.yml",
	}))
	c.Start(context.Background())

	minioPort := c.Port("minio", 9000)
	postgresPort := c.Port("postgres", 1433)
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
				"port":             strconv.Itoa(postgresPort),
				"sslMode":          "disable",
				"namespace":        "",
				"bucketProvider":   warehouseutils.MINIO,
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
		tableName := t.Name() + "_test_table"

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

		ms := postgres.New(config.Default, logger.NOP, stats.Default)
		err = ms.Setup(ctx, warehouse, mockUploader)
		require.NoError(t, err)

		err = ms.CreateSchema(ctx)
		require.NoError(t, err)

		err = ms.CreateTable(ctx, tableName, model.TableSchema{
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

		loadTableStat, err := ms.LoadTable(ctx, tableName)
		require.NoError(t, err)
		require.Equal(t, loadTableStat.RowsInserted, int64(14))
		require.Equal(t, loadTableStat.RowsUpdated, int64(0))

		loadTableStat, err = ms.LoadTable(ctx, tableName)
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

		ms := mssql.New(config.Default, logger.NOP, stats.Default)
		err = ms.Setup(ctx, warehouse, mockUploader)
		require.NoError(t, err)

		loadTableStat, err := ms.LoadTable(ctx, tableName)
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

		pg := postgres.New(config.Default, logger.NOP, stats.Default)
		err = pg.Setup(ctx, warehouse, mockUploader)
		require.NoError(t, err)

		err = pg.CreateSchema(ctx)
		require.NoError(t, err)

		loadTableStat, err := pg.LoadTable(ctx, tableName)
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

		pg := postgres.New(config.Default, logger.NOP, stats.Default)
		err = pg.Setup(ctx, warehouse, mockUploader)
		require.NoError(t, err)

		err = pg.CreateSchema(ctx)
		require.NoError(t, err)

		err = pg.CreateTable(ctx, tableName, model.TableSchema{
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

		loadTableStat, err := pg.LoadTable(ctx, tableName)
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

		pg := postgres.New(config.Default, logger.NOP, stats.Default)
		err = pg.Setup(ctx, warehouse, mockUploader)
		require.NoError(t, err)

		err = pg.CreateSchema(ctx)
		require.NoError(t, err)

		err = pg.CreateTable(ctx, tableName, model.TableSchema{
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

		loadTableStat, err := pg.LoadTable(ctx, tableName)
		require.ErrorContains(t, err, "mismatch in number of columns")
		require.Nil(t, loadTableStat)
	})

	//t.Run("Regular tables", func(t *testing.T) {
	//	t.Parallel()
	//
	//	tableName := "test_table"
	//
	//	testCases := []struct {
	//		name                         string
	//		wantError                    error
	//		mockError                    error
	//		skipSchemaCreation           bool
	//		skipTableCreation            bool
	//		cancelContext                bool
	//		mockFiles                    []string
	//		additionalFiles              []string
	//		queryExecEnabledWorkspaceIDs []string
	//	}{
	//		{
	//			name:               "schema not present",
	//			skipSchemaCreation: true,
	//			mockFiles:          []string{"load.csv.gz"},
	//			wantError:          errors.New("loading table: executing transaction: creating temporary table: pq: schema \"test_namespace\" does not exist"),
	//		},
	//		{
	//			name:              "table not present",
	//			skipTableCreation: true,
	//			mockFiles:         []string{"load.csv.gz"},
	//			wantError:         errors.New("loading table: executing transaction: creating temporary table: pq: relation \"test_namespace.test_table\" does not exist"),
	//		},
	//		{
	//			name:      "download error",
	//			mockFiles: []string{"load.csv.gz"},
	//			mockError: errors.New("test error"),
	//			wantError: errors.New("loading table: executing transaction: downloading load files: test error"),
	//		},
	//		{
	//			name:            "load file not present",
	//			additionalFiles: []string{"testdata/random.csv.gz"},
	//			wantError:       errors.New("loading table: executing transaction: loading data into staging table: opening load file: open testdata/random.csv.gz: no such file or directory"),
	//		},
	//		{
	//			name:      "less records than expected",
	//			mockFiles: []string{"less-records.csv.gz"},
	//			wantError: errors.New("loading table: executing transaction: loading data into staging table: mismatch in number of columns"),
	//		},
	//		{
	//			name:      "bad records",
	//			mockFiles: []string{"bad.csv.gz"},
	//			wantError: errors.New("loading table: executing transaction: executing copyIn statement: pq: invalid input syntax for type timestamp: \"1\""),
	//		},
	//		{
	//			name:      "success",
	//			mockFiles: []string{"load.csv.gz"},
	//		},
	//		{
	//			name:                         "enable query execution",
	//			mockFiles:                    []string{"load.csv.gz"},
	//			queryExecEnabledWorkspaceIDs: []string{workspaceID},
	//		},
	//		{
	//			name:          "context cancelled",
	//			mockFiles:     []string{"load.csv.gz"},
	//			wantError:     errors.New("loading table: begin transaction: context canceled"),
	//			cancelContext: true,
	//		},
	//	}
	//
	//	for _, tc := range testCases {
	//		tc := tc
	//
	//		t.Run(tc.name, func(t *testing.T) {
	//			t.Parallel()
	//
	//			pgResource, err := resource.SetupPostgres(pool, t)
	//			require.NoError(t, err)
	//
	//			t.Log("db:", pgResource.DBDsn)
	//
	//			db := sqlmiddleware.New(pgResource.DB)
	//
	//			store := memstats.New()
	//			c := config.New()
	//			c.Set("Warehouse.postgres.EnableSQLStatementExecutionPlanWorkspaceIDs", tc.queryExecEnabledWorkspaceIDs)
	//
	//			ctx, cancel := context.WithCancel(context.Background())
	//			if tc.cancelContext {
	//				cancel()
	//			} else {
	//				defer cancel()
	//			}
	//
	//			if !tc.skipSchemaCreation {
	//				_, err = db.Exec("CREATE SCHEMA IF NOT EXISTS " + namespace)
	//				require.NoError(t, err)
	//			}
	//			if !tc.skipTableCreation && !tc.skipSchemaCreation {
	//				_, err = db.Exec(fmt.Sprintf(`
	//				CREATE TABLE IF NOT EXISTS %s.%s (
	//					test_bool boolean,
	//					test_datetime timestamp,
	//					test_float float,
	//					test_int int,
	//					test_string varchar(255),
	//					id varchar(255),
	//					received_at timestamptz
	//				)
	//			`,
	//					namespace,
	//					tableName,
	//				))
	//				require.NoError(t, err)
	//			}
	//
	//			loadFiles := cloneFiles(t, tc.mockFiles)
	//			loadFiles = append(loadFiles, tc.additionalFiles...)
	//			require.NotEmpty(t, loadFiles)
	//
	//			pg := New(c, logger.NOP, store)
	//			pg.DB = db
	//			pg.Namespace = namespace
	//			pg.Warehouse = warehouse
	//			pg.stats = store
	//			pg.LoadFileDownloader = &mockLoadFileUploader{
	//				mockFiles: map[string][]string{
	//					tableName: loadFiles,
	//				},
	//				mockError: map[string]error{
	//					tableName: tc.mockError,
	//				},
	//			}
	//			pg.Uploader = newMockUploader(t, map[string]model.TableSchema{
	//				tableName: {
	//					"test_bool":     "boolean",
	//					"test_datetime": "datetime",
	//					"test_float":    "float",
	//					"test_int":      "int",
	//					"test_string":   "string",
	//					"id":            "string",
	//					"received_at":   "datetime",
	//				},
	//			})
	//
	//			_, err = pg.LoadTable(ctx, tableName)
	//			if tc.wantError != nil {
	//				require.ErrorContains(t, err, tc.wantError.Error())
	//				return
	//			}
	//			require.NoError(t, err)
	//		})
	//	}
	//})
	//
	//t.Run("Discards tables", func(t *testing.T) {
	//	t.Parallel()
	//
	//	tableName := warehouseutils.DiscardsTable
	//
	//	testCases := []struct {
	//		name               string
	//		wantError          error
	//		mockError          error
	//		skipSchemaCreation bool
	//		skipTableCreation  bool
	//		cancelContext      bool
	//		mockFiles          []string
	//	}{
	//		{
	//			name:               "schema not present",
	//			skipSchemaCreation: true,
	//			mockFiles:          []string{"discards.csv.gz"},
	//			wantError:          errors.New("loading table: executing transaction: creating temporary table: pq: schema \"test_namespace\" does not exist"),
	//		},
	//		{
	//			name:              "table not present",
	//			skipTableCreation: true,
	//			mockFiles:         []string{"discards.csv.gz"},
	//			wantError:         errors.New("loading table: executing transaction: creating temporary table: pq: relation \"test_namespace.rudder_discards\" does not exist"),
	//		},
	//		{
	//			name:      "download error",
	//			mockFiles: []string{"discards.csv.gz"},
	//			wantError: errors.New("loading table: executing transaction: downloading load files: test error"),
	//			mockError: errors.New("test error"),
	//		},
	//		{
	//			name:      "success",
	//			mockFiles: []string{"discards.csv.gz"},
	//		},
	//	}
	//
	//	for _, tc := range testCases {
	//		tc := tc
	//
	//		t.Run(tc.name, func(t *testing.T) {
	//			t.Parallel()
	//
	//			pgResource, err := resource.SetupPostgres(pool, t)
	//			require.NoError(t, err)
	//
	//			t.Log("db:", pgResource.DBDsn)
	//
	//			db := sqlmiddleware.New(pgResource.DB)
	//
	//			store := memstats.New()
	//			c := config.New()
	//
	//			ctx, cancel := context.WithCancel(context.Background())
	//			if tc.cancelContext {
	//				cancel()
	//			} else {
	//				defer cancel()
	//			}
	//
	//			if !tc.skipSchemaCreation {
	//				_, err = db.Exec("CREATE SCHEMA IF NOT EXISTS " + namespace)
	//				require.NoError(t, err)
	//			}
	//			if !tc.skipTableCreation && !tc.skipSchemaCreation {
	//				_, err = db.Exec(fmt.Sprintf(`
	//				CREATE TABLE IF NOT EXISTS %s.%s (
	//					"column_name"  "varchar",
	//					"column_value" "varchar",
	//					"received_at"  "timestamptz",
	//					"row_id"       "varchar",
	//					"table_name"   "varchar",
	//					"uuid_ts"      "timestamptz"
	//				)
	//			`,
	//					namespace,
	//					tableName,
	//				))
	//				require.NoError(t, err)
	//			}
	//
	//			loadFiles := cloneFiles(t, tc.mockFiles)
	//			require.NotEmpty(t, loadFiles)
	//
	//			pg := New(c, logger.NOP, store)
	//			pg.DB = db
	//			pg.Namespace = namespace
	//			pg.Warehouse = warehouse
	//			pg.stats = store
	//			pg.LoadFileDownloader = &mockLoadFileUploader{
	//				mockFiles: map[string][]string{
	//					tableName: loadFiles,
	//				},
	//				mockError: map[string]error{
	//					tableName: tc.mockError,
	//				},
	//			}
	//			pg.Uploader = newMockUploader(t, map[string]model.TableSchema{
	//				tableName: warehouseutils.DiscardsSchema,
	//			})
	//
	//			_, err = pg.LoadTable(ctx, tableName)
	//			if tc.wantError != nil {
	//				require.EqualError(t, err, tc.wantError.Error())
	//				return
	//			}
	//			require.NoError(t, err)
	//		})
	//	}
	//})
}

func TestLoadUsersTable(t *testing.T) {
	t.Parallel()

	misc.Init()
	warehouseutils.Init()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	const (
		namespace   = "test_namespace"
		sourceID    = "test_source_id"
		destID      = "test_dest_id"
		sourceType  = "test_source_type"
		destType    = "test_dest_type"
		workspaceID = "test_workspace_id"
	)

	warehouse := model.Warehouse{
		Source: backendconfig.SourceT{
			ID: sourceID,
			SourceDefinition: backendconfig.SourceDefinitionT{
				Name: sourceType,
			},
		},
		Destination: backendconfig.DestinationT{
			ID: destID,
			DestinationDefinition: backendconfig.DestinationDefinitionT{
				Name: destType,
			},
		},
		WorkspaceID: workspaceID,
	}

	testCases := []struct {
		name                       string
		wantErrorsMap              map[string]error
		mockUsersFiles             []string
		mockIdentifiesFiles        []string
		mockUsersError             error
		mockIdentifiesError        error
		skipSchemaCreation         bool
		skipTableCreation          bool
		skipUserTraitsWorkspaceIDs []string
		usersSchemaInUpload        model.TableSchema
	}{
		{
			name:                "success",
			mockUsersFiles:      []string{"users.csv.gz"},
			mockIdentifiesFiles: []string{"identifies.csv.gz"},
			wantErrorsMap: map[string]error{
				warehouseutils.IdentifiesTable: nil,
				warehouseutils.UsersTable:      nil,
			},
		},
		{
			name:                       "skip computing users traits",
			mockUsersFiles:             []string{"users.csv.gz"},
			mockIdentifiesFiles:        []string{"identifies.csv.gz"},
			skipUserTraitsWorkspaceIDs: []string{workspaceID},
			wantErrorsMap: map[string]error{
				warehouseutils.IdentifiesTable: nil,
				warehouseutils.UsersTable:      nil,
			},
		},
		{
			name:                "empty users schema",
			mockUsersFiles:      []string{"users.csv.gz"},
			mockIdentifiesFiles: []string{"identifies.csv.gz"},
			usersSchemaInUpload: model.TableSchema{},
			wantErrorsMap: map[string]error{
				warehouseutils.IdentifiesTable: nil,
			},
		},
		{
			name:                "download error for identifies",
			mockUsersFiles:      []string{"users.csv.gz"},
			mockIdentifiesFiles: []string{"identifies.csv.gz"},
			wantErrorsMap: map[string]error{
				warehouseutils.IdentifiesTable: errors.New("loading identifies table: downloading load files: test error"),
			},
			mockIdentifiesError: errors.New("test error"),
		},
		{
			name:                "download error for users",
			mockUsersFiles:      []string{"users.csv.gz"},
			mockIdentifiesFiles: []string{"identifies.csv.gz"},
			wantErrorsMap: map[string]error{
				warehouseutils.IdentifiesTable: nil,
				warehouseutils.UsersTable:      errors.New("loading users table: downloading load files: test error"),
			},
			mockUsersError:             errors.New("test error"),
			skipUserTraitsWorkspaceIDs: []string{workspaceID},
		},
	}

	for _, tc := range testCases {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			pgResource, err := resource.SetupPostgres(pool, t)
			require.NoError(t, err)

			t.Log("db:", pgResource.DBDsn)

			db := sqlmiddleware.New(pgResource.DB)

			store := memstats.New()
			c := config.New()
			c.Set("Warehouse.postgres.SkipComputingUserLatestTraitsWorkspaceIDs", tc.skipUserTraitsWorkspaceIDs)
			ctx := context.Background()

			if !tc.skipSchemaCreation {
				_, err = db.Exec("CREATE SCHEMA IF NOT EXISTS " + namespace)
				require.NoError(t, err)
			}
			if !tc.skipTableCreation && !tc.skipSchemaCreation {
				for _, table := range []string{warehouseutils.UsersTable, warehouseutils.IdentifiesTable} {
					_, err = db.Exec(fmt.Sprintf(`
					CREATE TABLE IF NOT EXISTS %s.%s (
						test_bool boolean,
						test_datetime timestamp,
						test_float float,
						test_int int,
						test_string varchar(255),
						id varchar(255),
						user_id varchar(255),
						received_at timestamptz
					)
				`,
						namespace,
						table,
					))
					require.NoError(t, err)
				}
			}

			usersLoadFiles := cloneFiles(t, tc.mockUsersFiles)
			require.NotEmpty(t, usersLoadFiles)

			identifiesLoadFiles := cloneFiles(t, tc.mockIdentifiesFiles)
			require.NotEmpty(t, identifiesLoadFiles)

			pg := postgres.New(c, logger.NOP, store)

			var (
				identifiesSchemaInUpload = model.TableSchema{
					"test_bool":     "boolean",
					"test_datetime": "datetime",
					"test_float":    "float",
					"test_int":      "int",
					"test_string":   "string",
					"id":            "string",
					"received_at":   "datetime",
					"user_id":       "string",
				}
				usersSchamaInUpload = model.TableSchema{
					"test_bool":     "boolean",
					"test_datetime": "datetime",
					"test_float":    "float",
					"test_int":      "int",
					"test_string":   "string",
					"id":            "string",
					"received_at":   "datetime",
					"user_id":       "string",
				}
			)

			if tc.usersSchemaInUpload != nil {
				usersSchamaInUpload = tc.usersSchemaInUpload
			}

			pg.DB = db
			pg.Namespace = namespace
			pg.Warehouse = warehouse
			pg.LoadFileDownloader = &mockLoadFileUploader{
				mockFiles: map[string][]string{
					warehouseutils.UsersTable:      usersLoadFiles,
					warehouseutils.IdentifiesTable: identifiesLoadFiles,
				},
				mockError: map[string]error{
					warehouseutils.UsersTable:      tc.mockUsersError,
					warehouseutils.IdentifiesTable: tc.mockIdentifiesError,
				},
			}
			pg.Uploader = newMockUploader(t, map[string]model.TableSchema{
				warehouseutils.UsersTable:      usersSchamaInUpload,
				warehouseutils.IdentifiesTable: identifiesSchemaInUpload,
			})

			errorsMap := pg.LoadUserTables(ctx)
			require.NotEmpty(t, errorsMap)
			require.Len(t, errorsMap, len(tc.wantErrorsMap))
			for table, err := range errorsMap {
				require.Contains(t, tc.wantErrorsMap, table)
				if err != nil {
					require.NotNil(t, tc.wantErrorsMap[table])
					require.Equal(t, tc.wantErrorsMap[table].Error(), err.Error())
				} else {
					require.Nil(t, tc.wantErrorsMap[table])
				}
			}
		})
	}
}

func newMockUploader(t testing.TB, schema model.Schema) *mockuploader.MockUploader {
	ctrl := gomock.NewController(t)
	f := func(tableName string) model.TableSchema { return schema[tableName] }
	u := mockuploader.NewMockUploader(ctrl)
	u.EXPECT().GetTableSchemaInUpload(gomock.Any()).AnyTimes().DoAndReturn(f)
	u.EXPECT().GetTableSchemaInWarehouse(gomock.Any()).AnyTimes().DoAndReturn(f)
	return u
}
