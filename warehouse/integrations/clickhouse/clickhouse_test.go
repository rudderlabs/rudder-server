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

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
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
	"github.com/rudderlabs/rudder-server/testhelper/health"
	"github.com/rudderlabs/rudder-server/testhelper/workspaceConfig"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/clickhouse"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/testhelper"
	mockuploader "github.com/rudderlabs/rudder-server/warehouse/internal/mocks/utils"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/rudderlabs/rudder-server/warehouse/validations"
)

func TestIntegration(t *testing.T) {
	if os.Getenv("SLOW") != "1" {
		t.Skip("Skipping tests. Add 'SLOW=1' env var to run test.")
	}

	c := testcompose.New(t, compose.FilePaths([]string{"testdata/docker-compose.clickhouse.yml", "testdata/docker-compose.clickhouse-cluster.yml", "../testdata/docker-compose.jobsdb.yml", "../testdata/docker-compose.minio.yml"}))
	c.Start(context.Background())

	misc.Init()
	validations.Init()
	warehouseutils.Init()

	jobsDBPort := c.Port("jobsDb", 5432)
	minioPort := c.Port("minio", 9000)
	port := c.Port("clickhouse", 9000)
	clusterPort1 := c.Port("clickhouse01", 9000)
	clusterPort2 := c.Port("clickhouse02", 9000)
	clusterPort3 := c.Port("clickhouse03", 9000)
	clusterPort4 := c.Port("clickhouse04", 9000)

	httpPort, err := kithelper.GetFreePort()
	require.NoError(t, err)

	workspaceID := warehouseutils.RandHex()
	sourceID := warehouseutils.RandHex()
	destinationID := warehouseutils.RandHex()
	writeKey := warehouseutils.RandHex()
	clusterSourceID := warehouseutils.RandHex()
	clusterDestinationID := warehouseutils.RandHex()
	clusterWriteKey := warehouseutils.RandHex()

	destType := warehouseutils.CLICKHOUSE

	host := "localhost"
	database := "rudderdb"
	user := "rudder"
	password := "rudder-password"
	cluster := "rudder_cluster"

	bucketName := "testbucket"
	accessKeyID := "MYACCESSKEY"
	secretAccessKey := "MYSECRETKEY"

	minioEndpoint := fmt.Sprintf("localhost:%d", minioPort)

	templateConfigurations := map[string]any{
		"workspaceID":          workspaceID,
		"sourceID":             sourceID,
		"destinationID":        destinationID,
		"clusterSourceID":      clusterSourceID,
		"clusterDestinationID": clusterDestinationID,
		"writeKey":             writeKey,
		"clusterWriteKey":      clusterWriteKey,
		"host":                 host,
		"database":             database,
		"user":                 user,
		"password":             password,
		"port":                 strconv.Itoa(port),
		"cluster":              cluster,
		"clusterHost":          host,
		"clusterDatabase":      database,
		"clusterCluster":       cluster,
		"clusterUser":          user,
		"clusterPassword":      password,
		"clusterPort":          strconv.Itoa(clusterPort1),
		"bucketName":           bucketName,
		"accessKeyID":          accessKeyID,
		"secretAccessKey":      secretAccessKey,
		"endPoint":             minioEndpoint,
	}
	workspaceConfigPath := workspaceConfig.CreateTempFile(t, "testdata/template.json", templateConfigurations)

	testhelper.EnhanceWithDefaultEnvs(t)
	t.Setenv("JOBS_DB_PORT", strconv.Itoa(jobsDBPort))
	t.Setenv("WAREHOUSE_JOBS_DB_PORT", strconv.Itoa(jobsDBPort))
	t.Setenv("MINIO_ACCESS_KEY_ID", accessKeyID)
	t.Setenv("MINIO_SECRET_ACCESS_KEY", secretAccessKey)
	t.Setenv("MINIO_MINIO_ENDPOINT", minioEndpoint)
	t.Setenv("MINIO_SSL", "false")
	t.Setenv("RSERVER_WAREHOUSE_CLICKHOUSE_MAX_PARALLEL_LOADS", "8")
	t.Setenv("RSERVER_WAREHOUSE_WEB_PORT", strconv.Itoa(httpPort))
	t.Setenv("RSERVER_BACKEND_CONFIG_CONFIG_JSONPATH", workspaceConfigPath)
	t.Setenv("RSERVER_WAREHOUSE_CLICKHOUSE_SLOW_QUERY_THRESHOLD", "0s")

	svcDone := make(chan struct{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		r := runner.New(runner.ReleaseInfo{})
		_ = r.Run(ctx, []string{"clickhouse-integration-test"})

		close(svcDone)
	}()
	t.Cleanup(func() { <-svcDone })

	serviceHealthEndpoint := fmt.Sprintf("http://localhost:%d/health", httpPort)
	health.WaitUntilReady(ctx, t, serviceHealthEndpoint, time.Minute, time.Second, "serviceHealthEndpoint")

	t.Run("Events flow", func(t *testing.T) {
		var dbs []*sql.DB

		for _, port := range []int{port, clusterPort1, clusterPort2, clusterPort3, clusterPort4} {
			dsn := fmt.Sprintf("tcp://%s:%d?compress=false&database=%s&password=%s&secure=false&skip_verify=true&username=%s",
				"localhost", port, "rudderdb", "rudder-password", "rudder",
			)

			db := connectClickhouseDB(ctx, t, dsn)
			dbs = append(dbs, db)
		}

		jobsDB := testhelper.JobsDB(t, jobsDBPort)

		tables := []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"}

		testCases := []struct {
			name                    string
			writeKey                string
			sourceID                string
			destinationID           string
			warehouseEvents         testhelper.EventsCountMap
			warehouseModifiedEvents testhelper.EventsCountMap
			clusterSetup            func(t testing.TB)
			db                      *sql.DB
			stagingFilePrefix       string
		}{
			{
				name:              "Single Setup",
				writeKey:          writeKey,
				sourceID:          sourceID,
				destinationID:     destinationID,
				db:                dbs[0],
				stagingFilePrefix: "testdata/upload-job",
			},
			{
				name:          "Cluster Mode Setup",
				writeKey:      clusterWriteKey,
				sourceID:      clusterSourceID,
				destinationID: clusterDestinationID,
				db:            dbs[1],
				warehouseModifiedEvents: testhelper.EventsCountMap{
					"identifies":    8,
					"users":         2,
					"tracks":        8,
					"product_track": 8,
					"pages":         8,
					"screens":       8,
					"aliases":       8,
					"groups":        8,
				},
				clusterSetup: func(t testing.TB) {
					t.Helper()
					initializeClickhouseClusterMode(t, dbs[1:], tables)
				},
				stagingFilePrefix: "testdata/upload-cluster-job",
			},
		}

		for _, tc := range testCases {
			tc := tc

			t.Run(tc.name, func(t *testing.T) {
				sqlClient := &client.Client{
					SQL:  tc.db,
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
					WriteKey:           tc.writeKey,
					Schema:             database,
					Tables:             tables,
					SourceID:           tc.sourceID,
					DestinationID:      tc.destinationID,
					WarehouseEventsMap: tc.warehouseEvents,
					Config:             conf,
					WorkspaceID:        workspaceID,
					DestinationType:    destType,
					JobsDB:             jobsDB,
					HTTPPort:           httpPort,
					Client:             sqlClient,
					UserID:             testhelper.GetUserId(destType),
					StagingFilePath:    tc.stagingFilePrefix + ".staging-1.json",
				}
				ts1.VerifyEvents(t)

				t.Log("setting up cluster")
				if tc.clusterSetup != nil {
					tc.clusterSetup(t)
				}

				t.Log("verifying test case 2")
				ts2 := testhelper.TestConfig{
					WriteKey:           tc.writeKey,
					Schema:             database,
					Tables:             tables,
					SourceID:           tc.sourceID,
					DestinationID:      tc.destinationID,
					WarehouseEventsMap: tc.warehouseModifiedEvents,
					Config:             conf,
					WorkspaceID:        workspaceID,
					DestinationType:    destType,
					JobsDB:             jobsDB,
					HTTPPort:           httpPort,
					Client:             sqlClient,
					UserID:             testhelper.GetUserId(destType),
					StagingFilePath:    tc.stagingFilePrefix + ".staging-2.json",
				}
				ts2.VerifyEvents(t)
			})
		}
	})

	t.Run("Validations", func(t *testing.T) {
		dest := backendconfig.DestinationT{
			ID: "21Ev6TI6emCFDKph2Zn6XfTP7PI",
			Config: map[string]any{
				"host":             host,
				"database":         database,
				"cluster":          "",
				"user":             user,
				"password":         password,
				"port":             strconv.Itoa(port),
				"secure":           false,
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
				ID:          destinationID,
				Name:        "CLICKHOUSE",
				DisplayName: "ClickHouse",
			},
			Name:       "clickhouse-demo",
			Enabled:    true,
			RevisionID: "29eeuTnqbBKn0XVTj5z9XQIbaru",
		}
		testhelper.VerifyConfigurationTest(t, dest)
	})
}

func TestClickhouse_UseS3CopyEngineForLoading(t *testing.T) {
	S3EngineEnabledWorkspaceIDs := []string{"BpLnfgDsc2WD8F2qNfHK5a84jjJ"}

	testCases := []struct {
		name          string
		ObjectStorage string
		workspaceID   string
		useS3Engine   bool
	}{
		{
			name:          "incompatible object storage(AZURE BLOB)",
			ObjectStorage: warehouseutils.AzureBlob,
			workspaceID:   "test-workspace-id",
		},
		{
			name:          "incompatible object storage(GCS)",
			ObjectStorage: warehouseutils.GCS,
			workspaceID:   "test-workspace-id",
		},
		{
			name:          "incompatible workspace",
			ObjectStorage: warehouseutils.S3,
			workspaceID:   "test-workspace-id",
		},
		{
			name:          "compatible workspace with incompatible object storage",
			ObjectStorage: warehouseutils.GCS,
			workspaceID:   "BpLnfgDsc2WD8F2qNfHK5a84jjJ",
		},
		{
			name:          "compatible workspace(S3)",
			ObjectStorage: warehouseutils.S3,
			workspaceID:   "BpLnfgDsc2WD8F2qNfHK5a84jjJ",
			useS3Engine:   true,
		},
		{
			name:          "compatible workspace(MINIO)",
			ObjectStorage: warehouseutils.MINIO,
			workspaceID:   "BpLnfgDsc2WD8F2qNfHK5a84jjJ",
			useS3Engine:   true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			c := config.New()
			c.Set("Warehouse.clickhouse.s3EngineEnabledWorkspaceIDs", S3EngineEnabledWorkspaceIDs)

			ch := clickhouse.New(c, logger.NOP, memstats.New())
			ch.Warehouse = model.Warehouse{
				WorkspaceID: tc.workspaceID,
			}
			ch.ObjectStorage = tc.ObjectStorage

			require.Equal(t, tc.useS3Engine, ch.UseS3CopyEngineForLoading())
		})
	}
}

func TestClickhouse_LoadTableRoundTrip(t *testing.T) {
	c := testcompose.New(t, compose.FilePaths([]string{
		"testdata/docker-compose.clickhouse.yml",
		"../testdata/docker-compose.minio.yml",
	}))
	c.Start(context.Background())

	misc.Init()
	warehouseutils.Init()

	minioPort := c.Port("minio", 9000)
	clickhousePort := c.Port("clickhouse", 9000)

	bucketName := "testbucket"
	accessKeyID := "MYACCESSKEY"
	secretAccessKey := "MYSECRETKEY"
	region := "us-east-1"
	databaseName := "rudderdb"
	password := "rudder-password"
	user := "rudder"
	table := "test_table"
	workspaceID := "test_workspace_id"
	provider := "MINIO"

	minioEndpoint := fmt.Sprintf("localhost:%d", minioPort)

	dsn := fmt.Sprintf("tcp://%s:%d?compress=false&database=%s&password=%s&secure=false&skip_verify=true&username=%s",
		"localhost", clickhousePort, databaseName, password, user,
	)

	db := connectClickhouseDB(context.Background(), t, dsn)
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
		i := i
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			c := config.New()
			c.Set("Warehouse.clickhouse.s3EngineEnabledWorkspaceIDs", tc.S3EngineEnabledWorkspaceIDs)
			c.Set("Warehouse.clickhouse.disableNullable", tc.disableNullable)

			ch := clickhouse.New(c, logger.NOP, memstats.New())

			warehouse := model.Warehouse{
				Namespace:   fmt.Sprintf("test_namespace_%d", i),
				WorkspaceID: workspaceID,
				Destination: backendconfig.DestinationT{
					Config: map[string]any{
						"bucketProvider":  provider,
						"host":            "localhost",
						"port":            strconv.Itoa(clickhousePort),
						"database":        databaseName,
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
				Provider: provider,
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
				[]warehouseutils.LoadFile{{Location: uploadOutput.Location}},
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
			err = ch.AddColumns(ctx, table, []warehouseutils.ColumnInfo{
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
			for _, tableName := range []string{warehouseutils.IdentifiesTable, warehouseutils.UsersTable} {
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
			for _, tableName := range []string{warehouseutils.IdentifiesTable, warehouseutils.UsersTable} {
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
}

func TestClickhouse_TestConnection(t *testing.T) {
	c := testcompose.New(t, compose.FilePaths([]string{"testdata/docker-compose.clickhouse.yml"}))
	c.Start(context.Background())

	misc.Init()
	warehouseutils.Init()

	clickhousePort := c.Port("clickhouse", 9000)

	databaseName := "rudderdb"
	password := "rudder-password"
	user := "rudder"
	workspaceID := "test_workspace_id"
	namespace := "test_namespace"
	provider := "MINIO"
	timeout := 5 * time.Second

	dsn := fmt.Sprintf("tcp://%s:%d?compress=false&database=%s&password=%s&secure=false&skip_verify=true&username=%s",
		"localhost", clickhousePort, databaseName, password, user,
	)

	db := connectClickhouseDB(context.Background(), t, dsn)
	defer func() { _ = db.Close() }()

	ctx := context.Background()

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
		i := i
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			ch := clickhouse.New(config.New(), logger.NOP, memstats.New())

			host := "localhost"
			if tc.host != "" {
				host = tc.host
			}

			warehouse := model.Warehouse{
				Namespace:   namespace,
				WorkspaceID: workspaceID,
				Destination: backendconfig.DestinationT{
					ID: fmt.Sprintf("test-destination-%d", i),
					Config: map[string]any{
						"bucketProvider": provider,
						"host":           host,
						"port":           strconv.Itoa(clickhousePort),
						"database":       databaseName,
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
}

func TestClickhouse_LoadTestTable(t *testing.T) {
	c := testcompose.New(t, compose.FilePaths([]string{"testdata/docker-compose.clickhouse.yml"}))
	c.Start(context.Background())

	misc.Init()
	warehouseutils.Init()

	clickhousePort := c.Port("clickhouse", 9000)

	databaseName := "rudderdb"
	password := "rudder-password"
	user := "rudder"
	workspaceID := "test_workspace_id"
	namespace := "test_namespace"
	provider := "MINIO"
	host := "localhost"
	tableName := warehouseutils.CTStagingTablePrefix + "_test_table"
	testColumns := model.TableSchema{
		"id":  "int",
		"val": "string",
	}
	testPayload := map[string]any{
		"id":  1,
		"val": "RudderStack",
	}

	dsn := fmt.Sprintf("tcp://%s:%d?compress=false&database=%s&password=%s&secure=false&skip_verify=true&username=%s",
		"localhost", clickhousePort, databaseName, password, user,
	)

	db := connectClickhouseDB(context.Background(), t, dsn)
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

	ctx := context.Background()

	for i, tc := range testCases {
		tc := tc
		i := i

		t.Run(tc.name, func(t *testing.T) {
			ch := clickhouse.New(config.New(), logger.NOP, memstats.New())

			warehouse := model.Warehouse{
				Namespace:   namespace,
				WorkspaceID: workspaceID,
				Destination: backendconfig.DestinationT{
					Config: map[string]any{
						"bucketProvider": provider,
						"host":           host,
						"port":           strconv.Itoa(clickhousePort),
						"database":       databaseName,
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
}

func TestClickhouse_FetchSchema(t *testing.T) {
	c := testcompose.New(t, compose.FilePaths([]string{"testdata/docker-compose.clickhouse.yml"}))
	c.Start(context.Background())

	misc.Init()
	warehouseutils.Init()

	clickhousePort := c.Port("clickhouse", 9000)

	databaseName := "rudderdb"
	password := "rudder-password"
	user := "rudder"
	workspaceID := "test_workspace_id"
	namespace := "test_namespace"
	table := "test_table"

	dsn := fmt.Sprintf("tcp://%s:%d?compress=false&database=%s&password=%s&secure=false&skip_verify=true&username=%s",
		"localhost", clickhousePort, databaseName, password, user,
	)

	db := connectClickhouseDB(context.Background(), t, dsn)
	defer func() { _ = db.Close() }()

	ctx := context.Background()

	t.Run("Success", func(t *testing.T) {
		ch := clickhouse.New(config.New(), logger.NOP, memstats.New())

		warehouse := model.Warehouse{
			Namespace:   fmt.Sprintf("%s_success", namespace),
			WorkspaceID: workspaceID,
			Destination: backendconfig.DestinationT{
				Config: map[string]any{
					"host":     "localhost",
					"port":     strconv.Itoa(clickhousePort),
					"database": databaseName,
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
		ch := clickhouse.New(config.New(), logger.NOP, memstats.New())

		warehouse := model.Warehouse{
			Namespace:   fmt.Sprintf("%s_invalid_host", namespace),
			WorkspaceID: workspaceID,
			Destination: backendconfig.DestinationT{
				Config: map[string]any{
					"host":     "clickhouse",
					"port":     strconv.Itoa(clickhousePort),
					"database": databaseName,
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
		ch := clickhouse.New(config.New(), logger.NOP, memstats.New())

		warehouse := model.Warehouse{
			Namespace:   fmt.Sprintf("%s_invalid_database", namespace),
			WorkspaceID: workspaceID,
			Destination: backendconfig.DestinationT{
				Config: map[string]any{
					"host":     "localhost",
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
		ch := clickhouse.New(config.New(), logger.NOP, memstats.New())

		warehouse := model.Warehouse{
			Namespace:   fmt.Sprintf("%s_empty_schema", namespace),
			WorkspaceID: workspaceID,
			Destination: backendconfig.DestinationT{
				Config: map[string]any{
					"host":     "localhost",
					"port":     strconv.Itoa(clickhousePort),
					"database": databaseName,
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
		ch := clickhouse.New(config.New(), logger.NOP, memstats.New())

		warehouse := model.Warehouse{
			Namespace:   fmt.Sprintf("%s_unrecognized_schema", namespace),
			WorkspaceID: workspaceID,
			Destination: backendconfig.DestinationT{
				Config: map[string]any{
					"host":     "localhost",
					"port":     strconv.Itoa(clickhousePort),
					"database": databaseName,
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
}

func connectClickhouseDB(ctx context.Context, t testing.TB, dsn string) *sql.DB {
	t.Helper()

	db, err := sql.Open("clickhouse", dsn)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	require.Eventually(t, func() bool {
		return db.PingContext(ctx) == nil
	}, time.Minute, time.Second)

	err = db.PingContext(ctx)
	require.NoError(t, err)

	return db
}

func initializeClickhouseClusterMode(t testing.TB, clusterDBs []*sql.DB, tables []string) {
	t.Helper()

	type ColumnInfoT struct {
		ColumnName string
		ColumnType string
	}

	tableColumnInfoMap := map[string][]ColumnInfoT{
		"identifies": {
			{
				ColumnName: "context_passed_ip",
				ColumnType: "Nullable(String)",
			},
			{
				ColumnName: "context_library_name",
				ColumnType: "Nullable(String)",
			},
		},
		"product_track": {
			{
				ColumnName: "revenue",
				ColumnType: "Nullable(Float64)",
			},
			{
				ColumnName: "context_passed_ip",
				ColumnType: "Nullable(String)",
			},
			{
				ColumnName: "context_library_name",
				ColumnType: "Nullable(String)",
			},
		},
		"tracks": {
			{
				ColumnName: "context_passed_ip",
				ColumnType: "Nullable(String)",
			},
			{
				ColumnName: "context_library_name",
				ColumnType: "Nullable(String)",
			},
		},
		"users": {
			{
				ColumnName: "context_passed_ip",
				ColumnType: "Nullable(String)",
			},
			{
				ColumnName: "context_library_name",
				ColumnType: "SimpleAggregateFunction(anyLast, Nullable(String))",
			},
		},
		"pages": {
			{
				ColumnName: "context_passed_ip",
				ColumnType: "Nullable(String)",
			},
			{
				ColumnName: "context_library_name",
				ColumnType: "Nullable(String)",
			},
		},
		"screens": {
			{
				ColumnName: "context_passed_ip",
				ColumnType: "Nullable(String)",
			},
			{
				ColumnName: "context_library_name",
				ColumnType: "Nullable(String)",
			},
		},
		"aliases": {
			{
				ColumnName: "context_passed_ip",
				ColumnType: "Nullable(String)",
			},
			{
				ColumnName: "context_library_name",
				ColumnType: "Nullable(String)",
			},
		},
		"groups": {
			{
				ColumnName: "context_passed_ip",
				ColumnType: "Nullable(String)",
			},
			{
				ColumnName: "context_library_name",
				ColumnType: "Nullable(String)",
			},
		},
	}

	clusterDB := clusterDBs[0]

	// Rename tables to tables_shard
	for _, table := range tables {
		sqlStatement := fmt.Sprintf("RENAME TABLE %[1]s to %[1]s_shard ON CLUSTER rudder_cluster;", table)
		log.Printf("Renaming tables to sharded tables for distribution view for clickhouse cluster with sqlStatement: %s", sqlStatement)

		require.NoError(t, testhelper.WithConstantRetries(func() error {
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

		require.NoError(t, testhelper.WithConstantRetries(func() error {
			_, err := clusterDB.Exec(sqlStatement)
			return err
		}))
	}

	for _, clusterDB := range clusterDBs {
		tableName := "test_table"
		generateSqlStatement := func() string {
			return fmt.Sprintf(`
				CREATE TABLE IF NOT EXISTS "rudderdb".%[1]q ON CLUSTER "rudder_cluster" ("a" Int64,  "received_at" DateTime, "id" String) ENGINE = ReplicatedReplacingMergeTree(
					'/clickhouse/{cluster}/tables/%[2]s/{database}/{table}', '{replica}')
				ORDER BY ("received_at", "id") PARTITION BY toDate(received_at)
				;`,
				fmt.Sprintf("%s_cluster_distributed", tableName),
				uuid.New().String(),
			)
		}

		require.NoError(t, testhelper.WithConstantRetries(func() error {
			sqlStatement := generateSqlStatement()
			_, err := clusterDB.Exec(sqlStatement)
			t.Log("Creating cluster table with sqlStatement: ", sqlStatement)
			return err
		}))

		sqlStatementDropTable := fmt.Sprintf(`
				DROP TABLE rudderdb.%[1]s ON CLUSTER "rudder_cluster"`,
			fmt.Sprintf("%s_cluster_distributed", tableName),
		)
		require.NoError(t, testhelper.WithConstantRetries(func() error {
			_, err := clusterDB.Exec(sqlStatementDropTable)
			return err
		}))

		require.NoError(t, testhelper.WithConstantRetries(func() error {
			sqlStatement := generateSqlStatement()
			_, err := clusterDB.Exec(sqlStatement)
			t.Log("Creating cluster table with sqlStatement: ", sqlStatement)
			return err
		}))

	}

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
					columnInfo.ColumnName,
					columnInfo.ColumnType,
				)
			}
			sqlStatement = strings.TrimSuffix(sqlStatement, ",")
			log.Printf("Altering columns for distribution view for clickhouse cluster with sqlStatement: %s", sqlStatement)

			require.NoError(t, testhelper.WithConstantRetries(func() error {
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
	metadata []warehouseutils.LoadFile,
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
