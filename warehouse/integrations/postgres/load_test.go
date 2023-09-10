package postgres

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

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

type mockUploader struct {
	schema model.Schema
}

func (*mockUploader) GetSchemaInWarehouse() model.Schema { return model.Schema{} }
func (*mockUploader) GetLocalSchema(context.Context) (model.Schema, error) {
	return model.Schema{}, nil
}
func (*mockUploader) UpdateLocalSchema(context.Context, model.Schema) error { return nil }
func (*mockUploader) ShouldOnDedupUseNewRecord() bool                       { return false }
func (*mockUploader) UseRudderStorage() bool                                { return false }
func (*mockUploader) GetLoadFileGenStartTIme() time.Time                    { return time.Time{} }
func (*mockUploader) GetLoadFileType() string                               { return "JSON" }
func (*mockUploader) GetFirstLastEvent() (time.Time, time.Time)             { return time.Time{}, time.Time{} }
func (*mockUploader) GetLoadFilesMetadata(context.Context, warehouseutils.GetLoadFilesOptions) []warehouseutils.LoadFile {
	return []warehouseutils.LoadFile{}
}

func (*mockUploader) GetSingleLoadFile(context.Context, string) (warehouseutils.LoadFile, error) {
	return warehouseutils.LoadFile{}, nil
}

func (*mockUploader) GetSampleLoadFileLocation(context.Context, string) (string, error) {
	return "", nil
}

func (m *mockUploader) GetTableSchemaInUpload(tableName string) model.TableSchema {
	return m.schema[tableName]
}

func (m *mockUploader) GetTableSchemaInWarehouse(tableName string) model.TableSchema {
	return m.schema[tableName]
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

	t.Run("Regular tables", func(t *testing.T) {
		t.Parallel()

		tableName := "test_table"

		testCases := []struct {
			name                         string
			wantError                    error
			mockError                    error
			skipSchemaCreation           bool
			skipTableCreation            bool
			cancelContext                bool
			mockFiles                    []string
			additionalFiles              []string
			queryExecEnabledWorkspaceIDs []string
		}{
			{
				name:               "schema not present",
				skipSchemaCreation: true,
				mockFiles:          []string{"load.csv.gz"},
				wantError:          errors.New("loading table: executing transaction: creating temporary table: pq: schema \"test_namespace\" does not exist"),
			},
			{
				name:              "table not present",
				skipTableCreation: true,
				mockFiles:         []string{"load.csv.gz"},
				wantError:         errors.New("loading table: executing transaction: creating temporary table: pq: relation \"test_namespace.test_table\" does not exist"),
			},
			{
				name:      "download error",
				mockFiles: []string{"load.csv.gz"},
				mockError: errors.New("test error"),
				wantError: errors.New("loading table: executing transaction: downloading load files: test error"),
			},
			{
				name:            "load file not present",
				additionalFiles: []string{"testdata/random.csv.gz"},
				wantError:       errors.New("loading table: executing transaction: loading data into staging table: opening load file: open testdata/random.csv.gz: no such file or directory"),
			},
			{
				name:      "less records than expected",
				mockFiles: []string{"less-records.csv.gz"},
				wantError: errors.New("loading table: executing transaction: loading data into staging table: mismatch in number of columns"),
			},
			{
				name:      "bad records",
				mockFiles: []string{"bad.csv.gz"},
				wantError: errors.New("loading table: executing transaction: executing copyIn statement: pq: invalid input syntax for type timestamp: \"1\""),
			},
			{
				name:      "success",
				mockFiles: []string{"load.csv.gz"},
			},
			{
				name:                         "enable query execution",
				mockFiles:                    []string{"load.csv.gz"},
				queryExecEnabledWorkspaceIDs: []string{workspaceID},
			},
			{
				name:          "context cancelled",
				mockFiles:     []string{"load.csv.gz"},
				wantError:     errors.New("loading table: begin transaction: context canceled"),
				cancelContext: true,
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
				c.Set("Warehouse.postgres.EnableSQLStatementExecutionPlanWorkspaceIDs", tc.queryExecEnabledWorkspaceIDs)

				ctx, cancel := context.WithCancel(context.Background())
				if tc.cancelContext {
					cancel()
				} else {
					defer cancel()
				}

				if !tc.skipSchemaCreation {
					_, err = db.Exec("CREATE SCHEMA IF NOT EXISTS " + namespace)
					require.NoError(t, err)
				}
				if !tc.skipTableCreation && !tc.skipSchemaCreation {
					_, err = db.Exec(fmt.Sprintf(`
					CREATE TABLE IF NOT EXISTS %s.%s (
						test_bool boolean,
						test_datetime timestamp,
						test_float float,
						test_int int,
						test_string varchar(255),
						id varchar(255),
						received_at timestamptz
					)
				`,
						namespace,
						tableName,
					))
					require.NoError(t, err)
				}

				loadFiles := cloneFiles(t, tc.mockFiles)
				loadFiles = append(loadFiles, tc.additionalFiles...)
				require.NotEmpty(t, loadFiles)

				pg := New(c, logger.NOP, store)
				pg.DB = db
				pg.Namespace = namespace
				pg.Warehouse = warehouse
				pg.stats = store
				pg.LoadFileDownloader = &mockLoadFileUploader{
					mockFiles: map[string][]string{
						tableName: loadFiles,
					},
					mockError: map[string]error{
						tableName: tc.mockError,
					},
				}
				pg.Uploader = &mockUploader{
					schema: map[string]model.TableSchema{
						tableName: {
							"test_bool":     "boolean",
							"test_datetime": "datetime",
							"test_float":    "float",
							"test_int":      "int",
							"test_string":   "string",
							"id":            "string",
							"received_at":   "datetime",
						},
					},
				}

				_, err = pg.LoadTable(ctx, tableName)
				if tc.wantError != nil {
					require.ErrorContains(t, err, tc.wantError.Error())
					return
				}
				require.NoError(t, err)
			})
		}
	})

	t.Run("Discards tables", func(t *testing.T) {
		t.Parallel()

		tableName := warehouseutils.DiscardsTable

		testCases := []struct {
			name               string
			wantError          error
			mockError          error
			skipSchemaCreation bool
			skipTableCreation  bool
			cancelContext      bool
			mockFiles          []string
		}{
			{
				name:               "schema not present",
				skipSchemaCreation: true,
				mockFiles:          []string{"discards.csv.gz"},
				wantError:          errors.New("loading table: executing transaction: creating temporary table: pq: schema \"test_namespace\" does not exist"),
			},
			{
				name:              "table not present",
				skipTableCreation: true,
				mockFiles:         []string{"discards.csv.gz"},
				wantError:         errors.New("loading table: executing transaction: creating temporary table: pq: relation \"test_namespace.rudder_discards\" does not exist"),
			},
			{
				name:      "download error",
				mockFiles: []string{"discards.csv.gz"},
				wantError: errors.New("loading table: executing transaction: downloading load files: test error"),
				mockError: errors.New("test error"),
			},
			{
				name:      "success",
				mockFiles: []string{"discards.csv.gz"},
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

				ctx, cancel := context.WithCancel(context.Background())
				if tc.cancelContext {
					cancel()
				} else {
					defer cancel()
				}

				if !tc.skipSchemaCreation {
					_, err = db.Exec("CREATE SCHEMA IF NOT EXISTS " + namespace)
					require.NoError(t, err)
				}
				if !tc.skipTableCreation && !tc.skipSchemaCreation {
					_, err = db.Exec(fmt.Sprintf(`
					CREATE TABLE IF NOT EXISTS %s.%s (
						"column_name"  "varchar",
						"column_value" "varchar",
						"received_at"  "timestamptz",
						"row_id"       "varchar",
						"table_name"   "varchar",
						"uuid_ts"      "timestamptz"
					)
				`,
						namespace,
						tableName,
					))
					require.NoError(t, err)
				}

				loadFiles := cloneFiles(t, tc.mockFiles)
				require.NotEmpty(t, loadFiles)

				pg := New(c, logger.NOP, store)
				pg.DB = db
				pg.Namespace = namespace
				pg.Warehouse = warehouse
				pg.stats = store
				pg.LoadFileDownloader = &mockLoadFileUploader{
					mockFiles: map[string][]string{
						tableName: loadFiles,
					},
					mockError: map[string]error{
						tableName: tc.mockError,
					},
				}
				pg.Uploader = &mockUploader{
					schema: map[string]model.TableSchema{
						tableName: warehouseutils.DiscardsSchema,
					},
				}

				_, err = pg.LoadTable(ctx, tableName)
				if tc.wantError != nil {
					require.EqualError(t, err, tc.wantError.Error())
					return
				}
				require.NoError(t, err)
			})
		}
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

			pg := New(c, logger.NOP, store)

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
			pg.stats = store
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
			pg.Uploader = &mockUploader{
				schema: map[string]model.TableSchema{
					warehouseutils.UsersTable:      usersSchamaInUpload,
					warehouseutils.IdentifiesTable: identifiesSchemaInUpload,
				},
			}

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
