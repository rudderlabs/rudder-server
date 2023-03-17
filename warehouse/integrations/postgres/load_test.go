package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"

	"github.com/docker/docker/pkg/fileutils"
	"github.com/google/uuid"

	"github.com/ory/dockertest/v3"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/postgres"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/stretchr/testify/require"
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

func TestLoadTable_Load(t *testing.T) {
	misc.Init()
	warehouseutils.Init()
	postgres.Init()

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

	warehouse := &model.Warehouse{
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
				wantError:          errors.New("creating temporary table: pq: schema \"test_namespace\" does not exist"),
			},
			{
				name:              "table not present",
				skipTableCreation: true,
				mockFiles:         []string{"load.csv.gz"},
				wantError:         errors.New("creating temporary table: pq: relation \"test_namespace.test_table\" does not exist"),
			},
			{
				name:      "download error",
				mockFiles: []string{"load.csv.gz"},
				wantError: errors.New("downloading load files: test error"),
				mockError: errors.New("test error"),
			},
			{
				name:            "load file not present",
				additionalFiles: []string{"testdata/random.csv.gz"},
				wantError:       errors.New("opening load file: open testdata/random.csv.gz: no such file or directory"),
			},
			{
				name:      "less records than expected",
				mockFiles: []string{"less-records.csv.gz"},
				wantError: errors.New("missing columns in csv file: number of columns in files: 12, upload schema: 7, processed rows until now: 0"),
			},
			{
				name:      "bad records",
				mockFiles: []string{"bad.csv.gz"},
				wantError: errors.New("exec statement: pq: invalid input syntax for type timestamp: \"1\""),
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
				wantError:     errors.New("setting search path: context canceled"),
				cancelContext: true,
			},
		}

		for _, tc := range testCases {
			tc := tc

			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()

				pgResource, err := resource.SetupPostgres(pool, t)
				require.NoError(t, err)

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
					_, err = pgResource.DB.Exec("CREATE SCHEMA IF NOT EXISTS " + namespace)
					require.NoError(t, err)
				}
				if !tc.skipTableCreation && !tc.skipSchemaCreation {
					_, err = pgResource.DB.Exec(fmt.Sprintf(`
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

				lt := postgres.LoadTable{
					Logger:    logger.NOP,
					DB:        pgResource.DB,
					Namespace: namespace,
					Warehouse: warehouse,
					Stats:     store,
					Config:    c,
					LoadFileDownloader: &mockLoadFileUploader{
						mockFiles: map[string][]string{
							tableName: loadFiles,
						},
						mockError: map[string]error{
							tableName: tc.mockError,
						},
					},
				}
				_, err = lt.Load(ctx, tableName, model.TableSchema{
					"test_bool":     "boolean",
					"test_datetime": "datetime",
					"test_float":    "float",
					"test_int":      "int",
					"test_string":   "string",
					"id":            "string",
					"received_at":   "datetime",
				})
				if tc.wantError != nil {
					require.EqualError(t, err, tc.wantError.Error())
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
				wantError:          errors.New("creating temporary table: pq: schema \"test_namespace\" does not exist"),
			},
			{
				name:              "table not present",
				skipTableCreation: true,
				mockFiles:         []string{"discards.csv.gz"},
				wantError:         errors.New("creating temporary table: pq: relation \"test_namespace.rudder_discards\" does not exist"),
			},
			{
				name:      "download error",
				mockFiles: []string{"discards.csv.gz"},
				wantError: errors.New("downloading load files: test error"),
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

				store := memstats.New()
				c := config.New()

				ctx, cancel := context.WithCancel(context.Background())
				if tc.cancelContext {
					cancel()
				} else {
					defer cancel()
				}

				if !tc.skipSchemaCreation {
					_, err = pgResource.DB.Exec("CREATE SCHEMA IF NOT EXISTS " + namespace)
					require.NoError(t, err)
				}
				if !tc.skipTableCreation && !tc.skipSchemaCreation {
					_, err = pgResource.DB.Exec(fmt.Sprintf(`
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

				lt := postgres.LoadTable{
					Logger:    logger.NOP,
					DB:        pgResource.DB,
					Namespace: namespace,
					Warehouse: warehouse,
					Stats:     store,
					Config:    c,
					LoadFileDownloader: &mockLoadFileUploader{
						mockFiles: map[string][]string{
							tableName: loadFiles,
						},
						mockError: map[string]error{
							tableName: tc.mockError,
						},
					},
				}
				_, err = lt.Load(ctx, tableName, warehouseutils.DiscardsSchema)
				if tc.wantError != nil {
					require.EqualError(t, err, tc.wantError.Error())
					return
				}
				require.NoError(t, err)
			})
		}
	})
}

func TestLoadUsersTable_Load(t *testing.T) {
	misc.Init()
	warehouseutils.Init()
	postgres.Init()

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

			store := memstats.New()
			c := config.New()
			c.Set("Warehouse.postgres.SkipComputingUserLatestTraitsWorkspaceIDs", tc.skipUserTraitsWorkspaceIDs)
			ctx := context.Background()

			if !tc.skipSchemaCreation {
				_, err = pgResource.DB.Exec("CREATE SCHEMA IF NOT EXISTS " + namespace)
				require.NoError(t, err)
			}
			if !tc.skipTableCreation && !tc.skipSchemaCreation {
				for _, table := range []string{warehouseutils.UsersTable, warehouseutils.IdentifiesTable} {
					_, err = pgResource.DB.Exec(fmt.Sprintf(`
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

			lt := postgres.LoadUsersTable{
				Logger:    logger.NOP,
				DB:        pgResource.DB,
				Namespace: namespace,
				Warehouse: &model.Warehouse{
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
				},
				Stats:  store,
				Config: c,
				LoadFileDownloader: &mockLoadFileUploader{
					mockFiles: map[string][]string{
						warehouseutils.UsersTable:      usersLoadFiles,
						warehouseutils.IdentifiesTable: identifiesLoadFiles,
					},
					mockError: map[string]error{
						warehouseutils.UsersTable:      tc.mockUsersError,
						warehouseutils.IdentifiesTable: tc.mockIdentifiesError,
					},
				},
			}
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
				usersSchamaInWarehouse = model.TableSchema{
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

			errorsMap := lt.Load(ctx, identifiesSchemaInUpload, usersSchamaInUpload, usersSchamaInWarehouse)
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
