package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/ory/dockertest/v3"
	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/services/stats/memstats"
	"github.com/rudderlabs/rudder-server/testhelper/destination"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/postgres"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/stretchr/testify/require"
)

type mockLoadFileUploader struct {
	mockFiles []string
	mockError error
}

func (m *mockLoadFileUploader) Download(ctx context.Context, tableName string) ([]string, error) {
	return m.mockFiles, m.mockError
}

func TestLoadTable_Load(t *testing.T) {
	misc.Init()
	warehouseutils.Init()
	postgres.Init()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	const (
		namespace   = "test_namespace"
		tableName   = "test_table"
		sourceID    = "test_source_id"
		destID      = "test_dest_id"
		sourceType  = "test_source_type"
		destType    = "test_dest_type"
		workspaceID = "test_workspace_id"
	)

	testCases := []struct {
		name               string
		wantError          error
		mockFiles          []string
		mockError          error
		skipSchemaCreation bool
		skipTableCreation  bool
	}{
		{
			name:               "schema not present",
			skipSchemaCreation: true,
			mockFiles:          []string{"testdata/load.csv.gz"},
			wantError:          errors.New("creating temporary table: pq: schema \"test_namespace\" does not exist"),
		},
		{
			name:              "table not present",
			skipTableCreation: true,
			mockFiles:         []string{"testdata/load.csv.gz"},
			wantError:         errors.New("creating temporary table: pq: relation \"test_namespace.test_table\" does not exist"),
		},
		{
			name:      "download error",
			mockFiles: []string{"testdata/load.csv.gz"},
			wantError: errors.New("downloading files: test error"),
			mockError: errors.New("test error"),
		},
		{
			name:      "load file not present",
			mockFiles: []string{"testdata/random.csv.gz"},
			wantError: errors.New("opening load file: open testdata/random.csv.gz: no such file or directory"),
		},
		{
			name:      "less records than expected",
			mockFiles: []string{"testdata/less-records.csv.gz"},
			wantError: errors.New("mismatch in number of columns in csv file: 12, number of columns in upload schema: 7, processed rows until now: 0"),
		},
		{
			name:      "success",
			mockFiles: []string{"testdata/load.csv.gz"},
		},
	}

	for _, tc := range testCases {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			pgResource, err := destination.SetupPostgres(pool, t)
			require.NoError(t, err)

			store := memstats.New()
			c := config.New()
			ctx := context.Background()

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

			lt := postgres.LoadTable{
				Logger:    logger.NOP,
				DB:        pgResource.DB,
				Namespace: namespace,
				Warehouse: &warehouseutils.Warehouse{
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
					mockFiles: tc.mockFiles,
					mockError: tc.mockError,
				},
			}
			_, err = lt.Load(ctx, tableName, warehouseutils.TableSchemaT{
				"test_bool":     "boolean",
				"test_datetime": "datetime",
				"test_float":    "float",
				"test_int":      "int",
				"test_string":   "string",
				"id":            "string",
				"received_at":   "datetime",
			})
			if tc.wantError != nil {
				require.Equal(t, tc.wantError.Error(), err.Error())
				return
			}
			require.NoError(t, err)
		})
	}
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
		name               string
		wantError          error
		mockFiles          []string
		mockError          error
		skipSchemaCreation bool
		skipTableCreation  bool
	}{
		{
			name:      "success",
			mockFiles: []string{"testdata/users.csv.gz"},
		},
	}

	for _, tc := range testCases {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			pgResource, err := destination.SetupPostgres(pool, t)
			require.NoError(t, err)

			store := memstats.New()
			c := config.New()
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

			lt := postgres.LoadUsersTable{
				Logger:    logger.NOP,
				DB:        pgResource.DB,
				Namespace: namespace,
				Warehouse: &warehouseutils.Warehouse{
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
					mockFiles: tc.mockFiles,
					mockError: tc.mockError,
				},
			}
			errrosMap := lt.Load(ctx, warehouseutils.TableSchemaT{
				"test_bool":     "boolean",
				"test_datetime": "datetime",
				"test_float":    "float",
				"test_int":      "int",
				"test_string":   "string",
				"id":            "string",
				"received_at":   "datetime",
				"user_id":       "string",
			},
				warehouseutils.TableSchemaT{
					"test_bool":     "boolean",
					"test_datetime": "datetime",
					"test_float":    "float",
					"test_int":      "int",
					"test_string":   "string",
					"id":            "string",
					"received_at":   "datetime",
					"user_id":       "string",
				},
			)
			require.NotEmpty(t, errrosMap)
			for _, err := range errrosMap {
				if tc.wantError != nil {
					require.Equal(t, tc.wantError.Error(), err.Error())
					return
				}
				require.NoError(t, err)
			}
		})
	}
}
