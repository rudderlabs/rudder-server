package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"go.uber.org/mock/gomock"

	"github.com/docker/docker/pkg/fileutils"
	"github.com/google/uuid"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	pgdocker "github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/utils/misc"
	sqlmiddleware "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/postgres"
	mockuploader "github.com/rudderlabs/rudder-server/warehouse/internal/mocks/utils"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
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

			pgResource, err := pgdocker.Setup(pool, t)
			require.NoError(t, err)

			t.Log("db:", pgResource.DBDsn)

			db := sqlmiddleware.New(pgResource.DB)

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

			pg := postgres.New(c, logger.NOP, stats.NOP)

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

			ctrl := gomock.NewController(t)
			t.Cleanup(ctrl.Finish)

			schema := map[string]model.TableSchema{
				warehouseutils.UsersTable:      usersSchamaInUpload,
				warehouseutils.IdentifiesTable: identifiesSchemaInUpload,
			}
			f := func(tableName string) model.TableSchema {
				return schema[tableName]
			}
			mockUploader := mockuploader.NewMockUploader(ctrl)
			mockUploader.EXPECT().GetTableSchemaInUpload(gomock.Any()).AnyTimes().DoAndReturn(f)
			mockUploader.EXPECT().GetTableSchemaInWarehouse(gomock.Any()).AnyTimes().DoAndReturn(f)
			mockUploader.EXPECT().CanAppend().AnyTimes().Return(true)

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
			pg.Uploader = mockUploader

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
