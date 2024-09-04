package clickhouse_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/stats"

	"go.uber.org/mock/gomock"

	clickhousestd "github.com/ClickHouse/clickhouse-go"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/compose-test/compose"
	"github.com/rudderlabs/compose-test/testcompose"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/logger"
	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/testhelper/backendconfigtest"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/clickhouse"
	whth "github.com/rudderlabs/rudder-server/warehouse/integrations/testhelper"
	mockuploader "github.com/rudderlabs/rudder-server/warehouse/internal/mocks/utils"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/rudderlabs/rudder-server/warehouse/validations"
)

func TestIntegration(t *testing.T) {
	if os.Getenv("SLOW") != "1" {
		t.Skip("Skipping tests. Add 'SLOW=1' env var to run test.")
	}

	misc.Init()
	validations.Init()
	whutils.Init()

	destType := whutils.CLICKHOUSE

	host := "localhost"
	database := "rudderdb"
	user := "rudder"
	password := "rudder-password"
	cluster := "rudder_cluster"
	bucketName := "testbucket"
	accessKeyID := "MYACCESSKEY"
	secretAccessKey := "MYSECRETKEY"

	t.Run("Events flow", func(t *testing.T) {
		httpPort, err := kithelper.GetFreePort()
		require.NoError(t, err)

		c := testcompose.New(t, compose.FilePaths([]string{"testdata/docker-compose.clickhouse.yml", "testdata/docker-compose.clickhouse-cluster.yml", "../testdata/docker-compose.jobsdb.yml", "../testdata/docker-compose.minio.yml"}))
		c.Start(context.Background())

		workspaceID := whutils.RandHex()
		jobsDBPort := c.Port("jobsDb", 5432)
		clickhousePort := c.Port("clickhouse", 9000)
		clickhouseClusterPort1 := c.Port("clickhouse01", 9000)
		clickhouseClusterPort2 := c.Port("clickhouse02", 9000)
		clickhouseClusterPort3 := c.Port("clickhouse03", 9000)
		clickhouseClusterPort4 := c.Port("clickhouse04", 9000)
		minioEndpoint := fmt.Sprintf("localhost:%d", c.Port("minio", 9000))

		jobsDB := whth.JobsDB(t, jobsDBPort)

		testCases := []struct {
			name                    string
			warehouseEvents         whth.EventsCountMap
			warehouseModifiedEvents whth.EventsCountMap
			clusterSetup            func(*testing.T, context.Context)
			setupDB                 func(testing.TB, context.Context) *sql.DB
			stagingFilePrefix       string
			configOverride          map[string]any
		}{
			{
				name: "Single Setup",
				setupDB: func(t testing.TB, ctx context.Context) *sql.DB {
					t.Helper()
					dsn := fmt.Sprintf("tcp://%s:%d?compress=false&database=%s&password=%s&secure=false&skip_verify=true&username=%s",
						host, clickhousePort, database, password, user,
					)
					return connectClickhouseDB(t, ctx, dsn)
				},
				stagingFilePrefix: "testdata/upload-job",
				configOverride: map[string]any{
					"port": strconv.Itoa(clickhousePort),
				},
			},
			{
				name: "Cluster Mode Setup",
				setupDB: func(t testing.TB, ctx context.Context) *sql.DB {
					t.Helper()
					dsn := fmt.Sprintf("tcp://%s:%d?compress=false&database=%s&password=%s&secure=false&skip_verify=true&username=%s",
						host, clickhouseClusterPort1, database, password, user,
					)
					return connectClickhouseDB(t, ctx, dsn)
				},
				warehouseModifiedEvents: whth.EventsCountMap{
					"identifies":    8,
					"users":         2,
					"tracks":        8,
					"product_track": 8,
					"pages":         8,
					"screens":       8,
					"aliases":       8,
					"groups":        8,
				},
				clusterSetup: func(t *testing.T, ctx context.Context) {
					t.Helper()

					clusterPorts := []int{clickhouseClusterPort2, clickhouseClusterPort3, clickhouseClusterPort4}
					dbs := lo.Map(clusterPorts, func(port, _ int) *sql.DB {
						dsn := fmt.Sprintf("tcp://%s:%d?compress=false&database=%s&password=%s&secure=false&skip_verify=true&username=%s",
							host, port, database, password, user,
						)
						return connectClickhouseDB(t, ctx, dsn)
					})
					tables := []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"}
					initializeClickhouseClusterMode(t, dbs, tables, clickhouseClusterPort1)
				},
				stagingFilePrefix: "testdata/upload-cluster-job",
				configOverride: map[string]any{
					"cluster": cluster,
					"port":    strconv.Itoa(clickhouseClusterPort1),
				},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				var (
					sourceID      = whutils.RandHex()
					destinationID = whutils.RandHex()
					writeKey      = whutils.RandHex()
				)

				destinationBuilder := backendconfigtest.NewDestinationBuilder(destType).
					WithID(destinationID).
					WithRevisionID(destinationID).
					WithConfigOption("host", host).
					WithConfigOption("database", database).
					WithConfigOption("user", user).
					WithConfigOption("password", password).
					WithConfigOption("bucketProvider", whutils.MINIO).
					WithConfigOption("bucketName", bucketName).
					WithConfigOption("accessKeyID", accessKeyID).
					WithConfigOption("secretAccessKey", secretAccessKey).
					WithConfigOption("useSSL", false).
					WithConfigOption("secure", false).
					WithConfigOption("endPoint", minioEndpoint).
					WithConfigOption("useRudderStorage", false).
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

				t.Setenv("RSERVER_WAREHOUSE_CLICKHOUSE_MAX_PARALLEL_LOADS", "8")
				t.Setenv("RSERVER_WAREHOUSE_CLICKHOUSE_SLOW_QUERY_THRESHOLD", "0s")

				whth.BootstrapSvc(t, workspaceConfig, httpPort, jobsDBPort)

				db := tc.setupDB(t, context.Background())
				t.Cleanup(func() { _ = db.Close() })
				tables := []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"}

				sqlClient := &client.Client{
					SQL:  db,
					Type: client.SQLClient,
				}

				conf := map[string]interface{}{
					"bucketProvider":   whutils.MINIO,
					"bucketName":       bucketName,
					"accessKeyID":      accessKeyID,
					"secretAccessKey":  secretAccessKey,
					"useSSL":           false,
					"endPoint":         minioEndpoint,
					"useRudderStorage": false,
				}

				t.Log("verifying test case 1")
				ts1 := whth.TestConfig{
					WriteKey:           writeKey,
					Schema:             database,
					Tables:             tables,
					SourceID:           sourceID,
					DestinationID:      destinationID,
					WarehouseEventsMap: tc.warehouseEvents,
					Config:             conf,
					WorkspaceID:        workspaceID,
					DestinationType:    destType,
					JobsDB:             jobsDB,
					HTTPPort:           httpPort,
					Client:             sqlClient,
					UserID:             whth.GetUserId(destType),
					StagingFilePath:    tc.stagingFilePrefix + ".staging-1.json",
				}
				ts1.VerifyEvents(t)

				t.Log("setting up cluster")
				if tc.clusterSetup != nil {
					tc.clusterSetup(t, context.Background())
				}

				t.Log("verifying test case 2")
				ts2 := whth.TestConfig{
					WriteKey:           writeKey,
					Schema:             database,
					Tables:             tables,
					SourceID:           sourceID,
					DestinationID:      destinationID,
					WarehouseEventsMap: tc.warehouseModifiedEvents,
					Config:             conf,
					WorkspaceID:        workspaceID,
					DestinationType:    destType,
					JobsDB:             jobsDB,
					HTTPPort:           httpPort,
					Client:             sqlClient,
					UserID:             whth.GetUserId(destType),
					StagingFilePath:    tc.stagingFilePrefix + ".staging-2.json",
				}
				ts2.VerifyEvents(t)
			})
		}
	})

	t.Run("Validations", func(t *testing.T) {
		c := testcompose.New(t, compose.FilePaths([]string{"testdata/docker-compose.clickhouse.yml", "../testdata/docker-compose.minio.yml"}))
		c.Start(context.Background())

		clickhousePort := c.Port("clickhouse", 9000)
		minioEndpoint := fmt.Sprintf("localhost:%d", c.Port("minio", 9000))

		dest := backendconfig.DestinationT{
			ID: "21Ev6TI6emCFDKph2Zn6XfTP7PI",
			Config: map[string]any{
				"host":             host,
				"database":         database,
				"cluster":          "",
				"user":             user,
				"password":         password,
				"port":             strconv.Itoa(clickhousePort),
				"secure":           false,
				"bucketProvider":   whutils.MINIO,
				"bucketName":       bucketName,
				"accessKeyID":      accessKeyID,
				"secretAccessKey":  secretAccessKey,
				"useSSL":           false,
				"endPoint":         minioEndpoint,
				"syncFrequency":    "30",
				"useRudderStorage": false,
			},
			DestinationDefinition: backendconfig.DestinationDefinitionT{
				ID:          "test_destination_id",
				Name:        "CLICKHOUSE",
				DisplayName: "ClickHouse",
			},
			Name:       "clickhouse-demo",
			Enabled:    true,
			RevisionID: "29eeuTnqbBKn0XVTj5z9XQIbaru",
		}
		whth.VerifyConfigurationTest(t, dest)
	})

	t.Run("Fetch schema", func(t *testing.T) {
		c := testcompose.New(t, compose.FilePaths([]string{"testdata/docker-compose.clickhouse.yml"}))
		c.Start(context.Background())

		workspaceID := whutils.RandHex()
		clickhousePort := c.Port("clickhouse", 9000)

		ctx := context.Background()
		namespace := "test_namespace"
		table := "test_table"

		dsn := fmt.Sprintf("tcp://%s:%d?compress=false&database=%s&password=%s&secure=false&skip_verify=true&username=%s",
			host, clickhousePort, database, password, user,
		)
		db := connectClickhouseDB(t, ctx, dsn)
		defer func() { _ = db.Close() }()

		t.Run("Success", func(t *testing.T) {
			ch := clickhouse.New(config.New(), logger.NOP, stats.NOP)

			warehouse := model.Warehouse{
				Namespace:   fmt.Sprintf("%s_success", namespace),
				WorkspaceID: workspaceID,
				Destination: backendconfig.DestinationT{
					Config: map[string]any{
						"host":     host,
						"port":     strconv.Itoa(clickhousePort),
						"database": database,
						"user":     user,
						"password": password,
					},
				},
			}

			err := ch.Setup(ctx, warehouse, newMockUploader(t, "", nil, nil))
			require.NoError(t, err)

			err = ch.CreateSchema(ctx)
			require.NoError(t, err)

			err = ch.CreateTable(ctx, table, model.TableSchema{
				"id":                  "string",
				"test_int":            "int",
				"test_float":          "float",
				"test_bool":           "boolean",
				"test_string":         "string",
				"test_datetime":       "datetime",
				"received_at":         "datetime",
				"test_array_bool":     "array(boolean)",
				"test_array_datetime": "array(datetime)",
				"test_array_float":    "array(float)",
				"test_array_int":      "array(int)",
				"test_array_string":   "array(string)",
			})
			require.NoError(t, err)

			schema, unrecognizedSchema, err := ch.FetchSchema(ctx)
			require.NoError(t, err)
			require.NotEmpty(t, schema)
			require.Empty(t, unrecognizedSchema)
		})

		t.Run("Invalid host", func(t *testing.T) {
			ch := clickhouse.New(config.New(), logger.NOP, stats.NOP)

			warehouse := model.Warehouse{
				Namespace:   fmt.Sprintf("%s_invalid_host", namespace),
				WorkspaceID: workspaceID,
				Destination: backendconfig.DestinationT{
					Config: map[string]any{
						"host":     "clickhouse",
						"port":     strconv.Itoa(clickhousePort),
						"database": database,
						"user":     user,
						"password": password,
					},
				},
			}

			err := ch.Setup(ctx, warehouse, newMockUploader(t, "", nil, nil))
			require.NoError(t, err)

			schema, unrecognizedSchema, err := ch.FetchSchema(ctx)
			require.ErrorContains(t, err, errors.New("dial tcp: lookup clickhouse").Error())
			require.Empty(t, schema)
			require.Empty(t, unrecognizedSchema)
		})

		t.Run("Invalid database", func(t *testing.T) {
			ch := clickhouse.New(config.New(), logger.NOP, stats.NOP)

			warehouse := model.Warehouse{
				Namespace:   fmt.Sprintf("%s_invalid_database", namespace),
				WorkspaceID: workspaceID,
				Destination: backendconfig.DestinationT{
					Config: map[string]any{
						"host":     host,
						"port":     strconv.Itoa(clickhousePort),
						"database": "invalid_database",
						"user":     user,
						"password": password,
					},
				},
			}

			err := ch.Setup(ctx, warehouse, newMockUploader(t, "", nil, nil))
			require.NoError(t, err)

			schema, unrecognizedSchema, err := ch.FetchSchema(ctx)
			require.NoError(t, err)
			require.Empty(t, schema)
			require.Empty(t, unrecognizedSchema)
		})

		t.Run("Empty schema", func(t *testing.T) {
			ch := clickhouse.New(config.New(), logger.NOP, stats.NOP)

			warehouse := model.Warehouse{
				Namespace:   fmt.Sprintf("%s_empty_schema", namespace),
				WorkspaceID: workspaceID,
				Destination: backendconfig.DestinationT{
					Config: map[string]any{
						"host":     host,
						"port":     strconv.Itoa(clickhousePort),
						"database": database,
						"user":     user,
						"password": password,
					},
				},
			}

			err := ch.Setup(ctx, warehouse, newMockUploader(t, "", nil, nil))
			require.NoError(t, err)

			err = ch.CreateSchema(ctx)
			require.NoError(t, err)

			schema, unrecognizedSchema, err := ch.FetchSchema(ctx)
			require.NoError(t, err)
			require.Empty(t, schema)
			require.Empty(t, unrecognizedSchema)
		})

		t.Run("Unrecognized schema", func(t *testing.T) {
			ch := clickhouse.New(config.New(), logger.NOP, stats.NOP)

			warehouse := model.Warehouse{
				Namespace:   fmt.Sprintf("%s_unrecognized_schema", namespace),
				WorkspaceID: workspaceID,
				Destination: backendconfig.DestinationT{
					Config: map[string]any{
						"host":     host,
						"port":     strconv.Itoa(clickhousePort),
						"database": database,
						"user":     user,
						"password": password,
					},
				},
			}

			err := ch.Setup(ctx, warehouse, newMockUploader(t, "", nil, nil))
			require.NoError(t, err)

			err = ch.CreateSchema(ctx)
			require.NoError(t, err)

			_, err = ch.DB.Exec(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s (x Enum('hello' = 1, 'world' = 2)) ENGINE = TinyLog;",
				warehouse.Namespace,
				table,
			))
			require.NoError(t, err)

			schema, unrecognizedSchema, err := ch.FetchSchema(ctx)
			require.NoError(t, err)
			require.NotEmpty(t, schema)
			require.NotEmpty(t, unrecognizedSchema)

			require.Equal(t, unrecognizedSchema, model.Schema{
				table: {
					"x": "<missing_datatype>",
				},
			})
		})
	})

	t.Run("Load Table round trip", func(t *testing.T) {
		c := testcompose.New(t, compose.FilePaths([]string{"testdata/docker-compose.clickhouse.yml", "../testdata/docker-compose.minio.yml"}))
		c.Start(context.Background())

		workspaceID := whutils.RandHex()
		minioPort := c.Port("minio", 9000)
		clickhousePort := c.Port("clickhouse", 9000)
		minioEndpoint := fmt.Sprintf("localhost:%d", minioPort)

		region := "us-east-1"
		table := "test_table"

		dsn := fmt.Sprintf("tcp://%s:%d?compress=false&database=%s&password=%s&secure=false&skip_verify=true&username=%s",
			host, clickhousePort, database, password, user,
		)
		db := connectClickhouseDB(t, context.Background(), dsn)
		defer func() { _ = db.Close() }()

		testCases := []struct {
			name                        string
			fileName                    string
			S3EngineEnabledWorkspaceIDs []string
			disableNullable             bool
		}{
			{
				name:     "normal loading using downloading of load files",
				fileName: "testdata/load.csv.gz",
			},
			{
				name:                        "using s3 engine for loading",
				S3EngineEnabledWorkspaceIDs: []string{workspaceID},
				fileName:                    "testdata/load-copy.csv.gz",
			},
			{
				name:            "normal loading using downloading of load files with disable nullable",
				fileName:        "testdata/load.csv.gz",
				disableNullable: true,
			},
			{
				name:                        "using s3 engine for loading with disable nullable",
				S3EngineEnabledWorkspaceIDs: []string{workspaceID},
				fileName:                    "testdata/load-copy.csv.gz",
				disableNullable:             true,
			},
		}

		for i, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				conf := config.New()
				conf.Set("Warehouse.clickhouse.s3EngineEnabledWorkspaceIDs", tc.S3EngineEnabledWorkspaceIDs)
				conf.Set("Warehouse.clickhouse.disableNullable", tc.disableNullable)

				ch := clickhouse.New(conf, logger.NOP, stats.NOP)

				warehouse := model.Warehouse{
					Namespace:   fmt.Sprintf("test_namespace_%d", i),
					WorkspaceID: workspaceID,
					Destination: backendconfig.DestinationT{
						Config: map[string]any{
							"bucketProvider":  whutils.MINIO,
							"host":            host,
							"port":            strconv.Itoa(clickhousePort),
							"database":        database,
							"user":            user,
							"password":        password,
							"bucketName":      bucketName,
							"accessKeyID":     accessKeyID,
							"secretAccessKey": secretAccessKey,
							"endPoint":        minioEndpoint,
						},
					},
				}

				t.Log("Preparing load files metadata")
				f, err := os.Open(tc.fileName)
				require.NoError(t, err)
				defer func() { _ = f.Close() }()

				fm, err := filemanager.New(&filemanager.Settings{
					Provider: whutils.MINIO,
					Config: map[string]any{
						"bucketName":      bucketName,
						"accessKeyID":     accessKeyID,
						"secretAccessKey": secretAccessKey,
						"endPoint":        minioEndpoint,
						"forcePathStyle":  true,
						"disableSSL":      true,
						"region":          region,
						"enableSSE":       false,
					},
				})
				require.NoError(t, err)

				ctx := context.Background()
				uploadOutput, err := fm.Upload(ctx, f, fmt.Sprintf("test_prefix_%d", i))
				require.NoError(t, err)

				mockUploader := newMockUploader(t,
					strconv.Itoa(minioPort),
					model.TableSchema{
						"alter_test_bool":     "boolean",
						"alter_test_datetime": "datetime",
						"alter_test_float":    "float",
						"alter_test_int":      "int",
						"alter_test_string":   "string",
						"id":                  "string",
						"received_at":         "datetime",
						"test_array_bool":     "array(boolean)",
						"test_array_datetime": "array(datetime)",
						"test_array_float":    "array(float)",
						"test_array_int":      "array(int)",
						"test_array_string":   "array(string)",
						"test_bool":           "boolean",
						"test_datetime":       "datetime",
						"test_float":          "float",
						"test_int":            "int",
						"test_string":         "string",
					},
					[]whutils.LoadFile{{Location: uploadOutput.Location}},
				)

				t.Log("Setting up clickhouse")
				err = ch.Setup(ctx, warehouse, mockUploader)
				require.NoError(t, err)

				t.Log("Verifying connection")
				_, err = ch.Connect(ctx, warehouse)
				require.NoError(t, err)

				t.Log("Verifying empty schema")
				schema, unrecognizedSchema, err := ch.FetchSchema(ctx)
				require.NoError(t, err)
				require.Empty(t, schema)
				require.Empty(t, unrecognizedSchema)

				t.Log("Creating schema")
				err = ch.CreateSchema(ctx)
				require.NoError(t, err)

				t.Log("Creating schema twice should not fail")
				err = ch.CreateSchema(ctx)
				require.NoError(t, err)

				t.Log("Creating table")
				err = ch.CreateTable(ctx, table, model.TableSchema{
					"id":                  "string",
					"test_int":            "int",
					"test_float":          "float",
					"test_bool":           "boolean",
					"test_string":         "string",
					"test_datetime":       "datetime",
					"received_at":         "datetime",
					"test_array_bool":     "array(boolean)",
					"test_array_datetime": "array(datetime)",
					"test_array_float":    "array(float)",
					"test_array_int":      "array(int)",
					"test_array_string":   "array(string)",
				})
				require.NoError(t, err)

				t.Log("Adding columns")
				err = ch.AddColumns(ctx, table, []whutils.ColumnInfo{
					{Name: "alter_test_int", Type: "int"},
					{Name: "alter_test_float", Type: "float"},
					{Name: "alter_test_bool", Type: "boolean"},
					{Name: "alter_test_string", Type: "string"},
					{Name: "alter_test_datetime", Type: "datetime"},
				})
				require.NoError(t, err)

				t.Log("Verifying schema")
				schema, unrecognizedSchema, err = ch.FetchSchema(ctx)
				require.NoError(t, err)
				require.NotEmpty(t, schema)
				require.Empty(t, unrecognizedSchema)

				t.Log("verifying if columns are not like Nullable(T) if disableNullable set to true")
				if tc.disableNullable {
					rows, err := ch.DB.Query(fmt.Sprintf(`select table, name, type from system.columns where database = '%s'`, warehouse.Namespace))
					require.NoError(t, err)

					defer func() { _ = rows.Close() }()

					var (
						tableName  string
						columnName string
						columnType string
					)

					for rows.Next() {
						err = rows.Scan(&tableName, &columnName, &columnType)
						require.NoError(t, err)

						if strings.Contains(columnType, "Nullable") {
							require.Fail(t, fmt.Sprintf("table %s column %s is of Nullable type even when disableNullable is set to true", tableName, columnName))
						}
					}
					require.NoError(t, rows.Err())
				}

				t.Log("Loading data into table")
				_, err = ch.LoadTable(ctx, table)
				require.NoError(t, err)

				t.Log("Drop table")
				err = ch.DropTable(ctx, table)
				require.NoError(t, err)

				t.Log("Creating users identifies and table")
				for _, tableName := range []string{whutils.IdentifiesTable, whutils.UsersTable} {
					err = ch.CreateTable(ctx, tableName, model.TableSchema{
						"id":            "string",
						"user_id":       "string",
						"test_int":      "int",
						"test_float":    "float",
						"test_bool":     "boolean",
						"test_string":   "string",
						"test_datetime": "datetime",
						"received_at":   "datetime",
					})
					require.NoError(t, err)
				}

				t.Log("Drop users identifies and table")
				for _, tableName := range []string{whutils.IdentifiesTable, whutils.UsersTable} {
					err = ch.DropTable(ctx, tableName)
					require.NoError(t, err)
				}

				t.Log("Verifying empty schema")
				schema, unrecognizedSchema, err = ch.FetchSchema(ctx)
				require.NoError(t, err)
				require.Empty(t, schema)
				require.Empty(t, unrecognizedSchema)
			})
		}
	})

	t.Run("Test connection", func(t *testing.T) {
		c := testcompose.New(t, compose.FilePaths([]string{"testdata/docker-compose.clickhouse.yml"}))
		c.Start(context.Background())

		workspaceID := whutils.RandHex()
		clickhousePort := c.Port("clickhouse", 9000)

		ctx := context.Background()
		namespace := "test_namespace"
		timeout := 5 * time.Second

		dsn := fmt.Sprintf("tcp://%s:%d?compress=false&database=%s&password=%s&secure=false&skip_verify=true&username=%s",
			host, clickhousePort, database, password, user,
		)
		db := connectClickhouseDB(t, context.Background(), dsn)
		defer func() { _ = db.Close() }()

		testCases := []struct {
			name      string
			host      string
			tlConfig  string
			timeout   time.Duration
			wantError error
		}{
			{
				name:      "DeadlineExceeded",
				wantError: errors.New("connection timeout: context deadline exceeded"),
			},
			{
				name:    "Success",
				timeout: timeout,
			},
			{
				name:     "TLS config",
				timeout:  timeout,
				tlConfig: "test-tls-config",
			},
			{
				name:      "No such host",
				timeout:   timeout,
				wantError: errors.New(`dial tcp: lookup clickhouse`),
				host:      "clickhouse",
			},
		}

		for i, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				ch := clickhouse.New(config.New(), logger.NOP, stats.NOP)

				host := host
				if tc.host != "" {
					host = tc.host
				}

				warehouse := model.Warehouse{
					Namespace:   namespace,
					WorkspaceID: workspaceID,
					Destination: backendconfig.DestinationT{
						ID: fmt.Sprintf("test-destination-%d", i),
						Config: map[string]any{
							"bucketProvider": whutils.MINIO,
							"host":           host,
							"port":           strconv.Itoa(clickhousePort),
							"database":       database,
							"user":           user,
							"password":       password,
							"caCertificate":  tc.tlConfig,
						},
					},
				}

				err := ch.Setup(ctx, warehouse, newMockUploader(t, "", nil, nil))
				require.NoError(t, err)

				ch.SetConnectionTimeout(tc.timeout)

				ctx, cancel := context.WithTimeout(context.Background(), tc.timeout)
				defer cancel()

				err = ch.TestConnection(ctx, warehouse)
				if tc.wantError != nil {
					require.ErrorContains(t, err, tc.wantError.Error())
					return
				}
				require.NoError(t, err)
			})
		}
	})

	t.Run("Load test table", func(t *testing.T) {
		c := testcompose.New(t, compose.FilePaths([]string{"testdata/docker-compose.clickhouse.yml"}))
		c.Start(context.Background())

		workspaceID := whutils.RandHex()
		clickhousePort := c.Port("clickhouse", 9000)

		ctx := context.Background()
		namespace := "test_namespace"
		tableName := whutils.CTStagingTablePrefix + "_test_table"
		testColumns := model.TableSchema{
			"id":  "int",
			"val": "string",
		}
		testPayload := map[string]any{
			"id":  1,
			"val": "RudderStack",
		}

		dsn := fmt.Sprintf("tcp://%s:%d?compress=false&database=%s&password=%s&secure=false&skip_verify=true&username=%s",
			host, clickhousePort, database, password, user,
		)
		db := connectClickhouseDB(t, context.Background(), dsn)
		defer func() { _ = db.Close() }()

		testCases := []struct {
			name      string
			wantError error
			payload   map[string]any
		}{
			{
				name: "Success",
			},
			{
				name: "Invalid columns",
				payload: map[string]any{
					"invalid_val": "Invalid Data",
				},
				wantError: errors.New("code: 16, message: No such column invalid_val in table test_namespace.setup_test_staging"),
			},
		}

		for i, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				ch := clickhouse.New(config.New(), logger.NOP, stats.NOP)

				warehouse := model.Warehouse{
					Namespace:   namespace,
					WorkspaceID: workspaceID,
					Destination: backendconfig.DestinationT{
						Config: map[string]any{
							"bucketProvider": whutils.MINIO,
							"host":           host,
							"port":           strconv.Itoa(clickhousePort),
							"database":       database,
							"user":           user,
							"password":       password,
						},
					},
				}

				payload := make(map[string]any)
				for k, v := range tc.payload {
					payload[k] = v
				}
				for k, v := range testPayload {
					payload[k] = v
				}

				err := ch.Setup(ctx, warehouse, newMockUploader(t, "", nil, nil))
				require.NoError(t, err)

				err = ch.CreateSchema(ctx)
				require.NoError(t, err)

				tableName := fmt.Sprintf("%s_%d", tableName, i)

				err = ch.CreateTable(ctx, tableName, testColumns)
				require.NoError(t, err)

				err = ch.LoadTestTable(ctx, "", tableName, payload, "")
				if tc.wantError != nil {
					require.ErrorContains(t, err, tc.wantError.Error())
					return
				}
				require.NoError(t, err)
			})
		}
	})
}

func connectClickhouseDB(t testing.TB, ctx context.Context, dsn string) *sql.DB {
	t.Helper()

	db, err := sql.Open("clickhouse", dsn)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	require.Eventually(t, func() bool {
		if err := db.PingContext(ctx); err != nil {
			t.Log("Ping failed:", err)
			return false
		}
		return true
	}, time.Minute, time.Second)

	require.NoError(t, db.PingContext(ctx))
	return db
}

func initializeClickhouseClusterMode(t *testing.T, clusterDBs []*sql.DB, tables []string, clusterPost int) {
	t.Helper()

	type columnInfo struct {
		columnName string
		columnType string
	}

	tableColumnInfoMap := map[string][]columnInfo{
		"identifies": {
			{
				columnName: "context_passed_ip",
				columnType: "Nullable(String)",
			},
			{
				columnName: "context_library_name",
				columnType: "Nullable(String)",
			},
		},
		"product_track": {
			{
				columnName: "revenue",
				columnType: "Nullable(Float64)",
			},
			{
				columnName: "context_passed_ip",
				columnType: "Nullable(String)",
			},
			{
				columnName: "context_library_name",
				columnType: "Nullable(String)",
			},
		},
		"tracks": {
			{
				columnName: "context_passed_ip",
				columnType: "Nullable(String)",
			},
			{
				columnName: "context_library_name",
				columnType: "Nullable(String)",
			},
		},
		"users": {
			{
				columnName: "context_passed_ip",
				columnType: "Nullable(String)",
			},
			{
				columnName: "context_library_name",
				columnType: "SimpleAggregateFunction(anyLast, Nullable(String))",
			},
		},
		"pages": {
			{
				columnName: "context_passed_ip",
				columnType: "Nullable(String)",
			},
			{
				columnName: "context_library_name",
				columnType: "Nullable(String)",
			},
		},
		"screens": {
			{
				columnName: "context_passed_ip",
				columnType: "Nullable(String)",
			},
			{
				columnName: "context_library_name",
				columnType: "Nullable(String)",
			},
		},
		"aliases": {
			{
				columnName: "context_passed_ip",
				columnType: "Nullable(String)",
			},
			{
				columnName: "context_library_name",
				columnType: "Nullable(String)",
			},
		},
		"groups": {
			{
				columnName: "context_passed_ip",
				columnType: "Nullable(String)",
			},
			{
				columnName: "context_library_name",
				columnType: "Nullable(String)",
			},
		},
	}

	clusterDB := clusterDBs[0]

	// Rename tables to tables_shard
	for _, table := range tables {
		sqlStatement := fmt.Sprintf("RENAME TABLE %[1]s to %[1]s_shard ON CLUSTER rudder_cluster;", table)
		log.Printf("Renaming tables to sharded tables for distribution view for clickhouse cluster with sqlStatement: %s", sqlStatement)

		require.NoError(t, whth.WithConstantRetries(func() error {
			_, err := clusterDB.Exec(sqlStatement)
			return err
		}))
	}

	// Create distribution views for tables
	for _, table := range tables {
		sqlStatement := fmt.Sprintf(`
			CREATE TABLE rudderdb.%[1]s ON CLUSTER 'rudder_cluster' AS rudderdb.%[1]s_shard ENGINE = Distributed(
			  'rudder_cluster',
			  rudderdb,
			  %[1]s_shard,
			  cityHash64(
				concat(
				  toString(
					toDate(received_at)
				  ),
				  id
				)
			  )
			);`,
			table,
		)
		log.Printf("Creating distribution view for clickhouse cluster with sqlStatement: %s", sqlStatement)

		require.NoError(t, whth.WithConstantRetries(func() error {
			_, err := clusterDB.Exec(sqlStatement)
			return err
		}))
	}

	t.Run("Create Drop Create", func(t *testing.T) {
		clusterDB := connectClickhouseDB(t, context.Background(), fmt.Sprintf("tcp://%s:%d?compress=false&database=%s&password=%s&secure=false&skip_verify=true&username=%s",
			"localhost", clusterPost, "rudderdb", "rudder-password", "rudder",
		))
		defer func() {
			_ = clusterDB.Close()
		}()

		t.Run("with UUID", func(t *testing.T) {
			testTable := "test_table_with_uuid"

			createTableSQLStatement := func() string {
				return fmt.Sprintf(`
					CREATE TABLE IF NOT EXISTS "rudderdb".%[1]q ON CLUSTER "rudder_cluster" (
					  "id" String, "received_at" DateTime
					) ENGINE = ReplicatedReplacingMergeTree(
					  '/clickhouse/{cluster}/tables/%[2]s/{database}/{table}',
					  '{replica}'
					)
					ORDER BY
					  ("received_at", "id") PARTITION BY toDate(received_at);`,
					testTable,
					uuid.New().String(),
				)
			}

			require.NoError(t, whth.WithConstantRetries(func() error {
				_, err := clusterDB.Exec(createTableSQLStatement())
				return err
			}))
			require.NoError(t, whth.WithConstantRetries(func() error {
				_, err := clusterDB.Exec(fmt.Sprintf(`DROP TABLE rudderdb.%[1]s ON CLUSTER "rudder_cluster";`, testTable))
				return err
			}))
			require.NoError(t, whth.WithConstantRetries(func() error {
				_, err := clusterDB.Exec(createTableSQLStatement())
				return err
			}))
		})
		t.Run("without UUID", func(t *testing.T) {
			testTable := "test_table_without_uuid"

			createTableSQLStatement := func() string {
				return fmt.Sprintf(`
					CREATE TABLE IF NOT EXISTS "rudderdb".%[1]q ON CLUSTER "rudder_cluster" (
					  "id" String, "received_at" DateTime
					) ENGINE = ReplicatedReplacingMergeTree(
					  '/clickhouse/{cluster}/tables/{database}/{table}',
					  '{replica}'
					)
					ORDER BY
					  ("received_at", "id") PARTITION BY toDate(received_at);`,
					testTable,
				)
			}

			require.NoError(t, whth.WithConstantRetries(func() error {
				_, err := clusterDB.Exec(createTableSQLStatement())
				return err
			}))
			require.NoError(t, whth.WithConstantRetries(func() error {
				_, err := clusterDB.Exec(fmt.Sprintf(`DROP TABLE rudderdb.%[1]s ON CLUSTER "rudder_cluster";`, testTable))
				return err
			}))

			err := whth.WithConstantRetries(func() error {
				_, err := clusterDB.Exec(createTableSQLStatement())
				return err
			})
			require.Error(t, err)

			var clickhouseErr *clickhousestd.Exception
			require.ErrorAs(t, err, &clickhouseErr)
			require.Equal(t, int32(253), clickhouseErr.Code)
		})
	})
	// Alter columns to all the cluster tables
	for _, clusterDB := range clusterDBs {
		for tableName, columnInfos := range tableColumnInfoMap {
			sqlStatement := fmt.Sprintf(`
				ALTER TABLE rudderdb.%[1]s_shard`,
				tableName,
			)
			for _, columnInfo := range columnInfos {
				sqlStatement += fmt.Sprintf(`
					ADD COLUMN IF NOT EXISTS %[1]s %[2]s,`,
					columnInfo.columnName,
					columnInfo.columnType,
				)
			}
			sqlStatement = strings.TrimSuffix(sqlStatement, ",")
			log.Printf("Altering columns for distribution view for clickhouse cluster with sqlStatement: %s", sqlStatement)

			require.NoError(t, whth.WithConstantRetries(func() error {
				_, err := clusterDB.Exec(sqlStatement)
				return err
			}))
		}
	}
}

func newMockUploader(
	t testing.TB,
	minioPort string,
	tableSchema model.TableSchema,
	metadata []whutils.LoadFile,
) *mockuploader.MockUploader {
	var sampleLocation string
	if len(metadata) > 0 {
		minioHostPort := fmt.Sprintf("localhost:%s", minioPort)
		sampleLocation = strings.Replace(metadata[0].Location, minioHostPort, "minio:9000", 1)
	}

	ctrl := gomock.NewController(t)
	u := mockuploader.NewMockUploader(ctrl)
	u.EXPECT().GetSampleLoadFileLocation(gomock.Any(), gomock.Any()).Return(sampleLocation, nil).AnyTimes()
	u.EXPECT().GetTableSchemaInUpload(gomock.Any()).Return(tableSchema).AnyTimes()
	u.EXPECT().GetLoadFilesMetadata(gomock.Any(), gomock.Any()).Return(metadata, nil).AnyTimes()
	u.EXPECT().UseRudderStorage().Return(false).AnyTimes()
	u.EXPECT().IsWarehouseSchemaEmpty().Return(true).AnyTimes()

	return u
}
