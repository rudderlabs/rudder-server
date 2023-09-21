package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/rudderlabs/compose-test/compose"
	"github.com/rudderlabs/compose-test/testcompose"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/postgres"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/testhelper"
	"github.com/rudderlabs/rudder-server/warehouse/validations"

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

func TestPostgres_LoadTable(t *testing.T) {
	const (
		sourceID        = "test_source-id"
		destinationID   = "test_destination-id"
		workspaceID     = "test_workspace-id"
		destinationType = warehouseutils.POSTGRES

		host     = "localhost"
		database = "rudderdb"
		user     = "rudder"
		password = "rudder-password"

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
	postgresPort := c.Port("postgres", 5432)
	minioEndpoint := net.JoinHostPort("localhost", strconv.Itoa(minioPort))

	ctx := context.Background()

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
					"host":             host,
					"database":         database,
					"user":             user,
					"password":         password,
					"port":             strconv.Itoa(postgresPort),
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
		mockUploader.EXPECT().GetLoadFilesMetadata(gomock.Any(), gomock.Any()).Return(loadFiles).AnyTimes() // Try removing this
		mockUploader.EXPECT().GetTableSchemaInUpload(tableName).Return(schemaInUpload).AnyTimes()
		mockUploader.EXPECT().GetTableSchemaInWarehouse(tableName).Return(schemaInWarehouse).AnyTimes()

		return mockUploader
	}

	t.Run("load table stats", func(t *testing.T) {
		namespace := "load_table_stats_test_namespace"
		tableName := "load_table_stats_test_table"

		uploadOutput := testhelper.Upload(t, fm, "../testdata/load.csv.gz", tableName)

		loadFiles := []warehouseutils.LoadFile{{Location: uploadOutput.Location}}
		mockUploader := uploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse)

		warehouse := warehouseModel(namespace)

		c := config.New()
		c.Set("Warehouse.postgres.EnableSQLStatementExecutionPlanWorkspaceIDs", workspaceID)

		pg := postgres.New(c, logger.NOP, stats.Default)
		err := pg.Setup(ctx, warehouse, mockUploader)
		require.NoError(t, err)

		err = pg.CreateSchema(ctx)
		require.NoError(t, err)

		err = pg.CreateTable(ctx, tableName, schemaInWarehouse)
		require.NoError(t, err)

		loadTableStat, err := pg.LoadTable(ctx, tableName)
		require.NoError(t, err)
		require.Equal(t, loadTableStat.RowsInserted, int64(14))
		require.Equal(t, loadTableStat.RowsUpdated, int64(0))

		loadTableStat, err = pg.LoadTable(ctx, tableName)
		require.NoError(t, err)
		require.Equal(t, loadTableStat.RowsInserted, int64(0))
		require.Equal(t, loadTableStat.RowsUpdated, int64(14))

		records := testhelper.RecordsFromWarehouse(t, pg.DB.DB,
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
				  %q.%q
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
	t.Run("schema does not exists", func(t *testing.T) {
		namespace := "schema_not_exists_test_namespace"
		tableName := "schema_not_exists_test_table"

		uploadOutput := testhelper.Upload(t, fm, "../testdata/load.csv.gz", tableName)

		loadFiles := []warehouseutils.LoadFile{{Location: uploadOutput.Location}}
		mockUploader := uploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse)

		warehouse := warehouseModel(namespace)

		pg := postgres.New(config.Default, logger.NOP, stats.Default)
		err := pg.Setup(ctx, warehouse, mockUploader)
		require.NoError(t, err)

		loadTableStat, err := pg.LoadTable(ctx, tableName)
		require.ErrorContains(t, err, "creating temporary table: pq: schema \"schema_not_exists_test_namespace\" does not exist")
		require.Nil(t, loadTableStat)
	})
	t.Run("table does not exists", func(t *testing.T) {
		namespace := "table_not_exists_test_namespace"
		tableName := "table_not_exists_test_table"

		uploadOutput := testhelper.Upload(t, fm, "../testdata/load.csv.gz", tableName)

		loadFiles := []warehouseutils.LoadFile{{Location: uploadOutput.Location}}
		mockUploader := uploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse)

		warehouse := warehouseModel(namespace)

		pg := postgres.New(config.Default, logger.NOP, stats.Default)
		err := pg.Setup(ctx, warehouse, mockUploader)
		require.NoError(t, err)

		err = pg.CreateSchema(ctx)
		require.NoError(t, err)

		loadTableStat, err := pg.LoadTable(ctx, tableName)
		require.ErrorContains(t, err, "creating temporary table: pq: relation \"table_not_exists_test_namespace.table_not_exists_test_table\"")
		require.Nil(t, loadTableStat)
	})
	t.Run("load file does not exists", func(t *testing.T) {
		namespace := "load_file_not_exists_test_namespace"
		tableName := "load_file_not_exists_test_table"

		loadFiles := []warehouseutils.LoadFile{{
			Location: "http://localhost:1234/testbucket/rudder-warehouse-load-objects/load_file_not_exists_test_table/test_source-id/f31af97e-03e8-46d0-8a1a-1786cb85b22c-load_file_not_exists_test_table/load.csv.gz",
		}}
		mockUploader := uploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse)

		warehouse := warehouseModel(namespace)

		pg := postgres.New(config.Default, logger.NOP, stats.Default)
		err := pg.Setup(ctx, warehouse, mockUploader)
		require.NoError(t, err)

		err = pg.CreateSchema(ctx)
		require.NoError(t, err)

		err = pg.CreateTable(ctx, tableName, schemaInWarehouse)
		require.NoError(t, err)

		loadTableStat, err := pg.LoadTable(ctx, tableName)
		require.ErrorContains(t, err, "downloading load files")
		require.Nil(t, loadTableStat)
	})
	t.Run("mismatch in number of columns", func(t *testing.T) {
		namespace := "mismatch_columns_test_namespace"
		tableName := "mismatch_columns_test_table"

		uploadOutput := testhelper.Upload(t, fm, "../testdata/mismatch-columns.csv.gz", tableName)

		loadFiles := []warehouseutils.LoadFile{{Location: uploadOutput.Location}}
		mockUploader := uploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse)

		warehouse := warehouseModel(namespace)

		pg := postgres.New(config.Default, logger.NOP, stats.Default)
		err := pg.Setup(ctx, warehouse, mockUploader)
		require.NoError(t, err)

		err = pg.CreateSchema(ctx)
		require.NoError(t, err)

		err = pg.CreateTable(ctx, tableName, schemaInWarehouse)
		require.NoError(t, err)

		loadTableStat, err := pg.LoadTable(ctx, tableName)
		require.ErrorContains(t, err, "mismatch in number of columns")
		require.Nil(t, loadTableStat)
	})
	t.Run("mismatch in schema", func(t *testing.T) {
		namespace := "mismatch_schema_test_namespace"
		tableName := "mismatch_schema_test_table"

		uploadOutput := testhelper.Upload(t, fm, "../testdata/mismatch-schema.csv.gz", tableName)

		loadFiles := []warehouseutils.LoadFile{{Location: uploadOutput.Location}}
		mockUploader := uploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse)

		warehouse := warehouseModel(namespace)

		pg := postgres.New(config.Default, logger.NOP, stats.Default)
		err := pg.Setup(ctx, warehouse, mockUploader)
		require.NoError(t, err)

		err = pg.CreateSchema(ctx)
		require.NoError(t, err)

		err = pg.CreateTable(ctx, tableName, schemaInWarehouse)
		require.NoError(t, err)

		loadTableStat, err := pg.LoadTable(ctx, tableName)
		require.ErrorContains(t, err, "executing copyIn statement: pq: invalid input syntax for type timestamp with time zone: \"125\"")
		require.Nil(t, loadTableStat)
	})
	t.Run("discards", func(t *testing.T) {
		namespace := "discards_test_namespace"
		tableName := warehouseutils.DiscardsTable

		uploadOutput := testhelper.Upload(t, fm, "../testdata/discards.csv.gz", tableName)

		loadFiles := []warehouseutils.LoadFile{{Location: uploadOutput.Location}}
		mockUploader := uploader(t, loadFiles, tableName, warehouseutils.DiscardsSchema, warehouseutils.DiscardsSchema)

		warehouse := warehouseModel(namespace)

		pg := postgres.New(config.Default, logger.NOP, stats.Default)
		err := pg.Setup(ctx, warehouse, mockUploader)
		require.NoError(t, err)

		err = pg.CreateSchema(ctx)
		require.NoError(t, err)

		err = pg.CreateTable(ctx, tableName, warehouseutils.DiscardsSchema)
		require.NoError(t, err)

		loadTableStat, err := pg.LoadTable(ctx, tableName)
		require.NoError(t, err)
		require.Equal(t, loadTableStat.RowsInserted, int64(6))
		require.Equal(t, loadTableStat.RowsUpdated, int64(0))

		records := testhelper.RecordsFromWarehouse(t, pg.DB.DB,
			fmt.Sprintf(`
				SELECT
				  column_name,
				  column_value,
				  received_at,
				  row_id,
				  table_name,
				  uuid_ts
				FROM
				  %q.%q
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
