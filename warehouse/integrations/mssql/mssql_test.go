package mssql_test

import (
	"context"
	"fmt"
	"github.com/rudderlabs/compose-test/testcompose"
	kitHelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	"github.com/rudderlabs/rudder-server/runner"
	"github.com/rudderlabs/rudder-server/testhelper/health"
	"github.com/rudderlabs/rudder-server/testhelper/workspaceConfig"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/encoding"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/mssql"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/postgres"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/testhelper"
	"github.com/rudderlabs/rudder-server/warehouse/validations"
	"github.com/stretchr/testify/require"
	"os"
	"strconv"
	"testing"
	"time"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/utils/misc"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestIntegration(t *testing.T) {
	if os.Getenv("SLOW") != "1" {
		t.Skip("Skipping tests. Add 'SLOW=1' env var to run test.")
	}

	c := testcompose.New(t, "testdata/docker-compose.yml")

	t.Cleanup(func() {
		c.Stop(context.Background())
	})
	c.Start(context.Background())

	misc.Init()
	validations.Init()
	warehouseutils.Init()
	encoding.Init()

	jobsDBPort := c.Port("wh-jobsDb", 5432)
	minioPort := c.Port("wh-minio", 9000)
	transformerPort := c.Port("wh-transformer", 9090)
	mssqlPort := c.Port("wh-mssql", 1433)

	httpPort, err := kitHelper.GetFreePort()
	require.NoError(t, err)
	httpAdminPort, err := kitHelper.GetFreePort()
	require.NoError(t, err)

	templateConfigurations := map[string]any{
		"workspaceId":          "BpLnfgDsc2WD8F2qNfHK5a84jjJ",
		"mssqlWriteKey":        "YSQ3n267l1VQKGNbSuJE9fQbzON",
		"mssqlHost":            "localhost",
		"mssqlDatabase":        "master",
		"mssqlUser":            "SA",
		"mssqlPassword":        "reallyStrongPwd123",
		"mssqlPort":            fmt.Sprint(mssqlPort),
		"mssqlSourcesWriteKey": "2DkCpXZcEvPG2fcpUD3LmjPI7J6",
		"minioBucketName":      "testbucket",
		"minioAccesskeyID":     "MYACCESSKEY",
		"minioSecretAccessKey": "MYSECRETKEY",
		"minioEndpoint":        fmt.Sprintf("localhost:%d", minioPort),
	}
	workspaceConfigPath := workspaceConfig.CreateTempFile(t, "testdata/template.json", templateConfigurations)

	t.Setenv("JOBS_DB_HOST", "localhost")
	t.Setenv("JOBS_DB_NAME", "jobsdb")
	t.Setenv("JOBS_DB_DB_NAME", "jobsdb")
	t.Setenv("JOBS_DB_USER", "rudder")
	t.Setenv("JOBS_DB_PASSWORD", "password")
	t.Setenv("JOBS_DB_SSL_MODE", "disable")
	t.Setenv("JOBS_DB_PORT", fmt.Sprint(jobsDBPort))
	t.Setenv("JOBS_BACKUP_STORAGE_PROVIDER", "MINIO")
	t.Setenv("JOBS_BACKUP_BUCKET", "devintegrationtest")
	t.Setenv("JOBS_BACKUP_PREFIX", "test")
	t.Setenv("WAREHOUSE_JOBS_DB_HOST", "localhost")
	t.Setenv("WAREHOUSE_JOBS_DB_NAME", "jobsdb")
	t.Setenv("WAREHOUSE_JOBS_DB_DB_NAME", "jobsdb")
	t.Setenv("WAREHOUSE_JOBS_DB_USER", "rudder")
	t.Setenv("WAREHOUSE_JOBS_DB_PASSWORD", "password")
	t.Setenv("WAREHOUSE_JOBS_DB_SSL_MODE", "disable")
	t.Setenv("WAREHOUSE_JOBS_DB_PORT", fmt.Sprint(jobsDBPort))
	t.Setenv("MINIO_ACCESS_KEY_ID", "MYACCESSKEY")
	t.Setenv("MINIO_SECRET_ACCESS_KEY", "MYSECRETKEY")
	t.Setenv("MINIO_MINIO_ENDPOINT", fmt.Sprintf("localhost:%d", minioPort))
	t.Setenv("MINIO_SSL", "false")
	t.Setenv("GO_ENV", "production")
	t.Setenv("LOG_LEVEL", "INFO")
	t.Setenv("INSTANCE_ID", "1")
	t.Setenv("ALERT_PROVIDER", "pagerduty")
	t.Setenv("CONFIG_PATH", "../../../config/config.yaml")
	t.Setenv("DEST_TRANSFORM_URL", fmt.Sprintf("http://localhost:%d", transformerPort))
	t.Setenv("RSERVER_WAREHOUSE_MSSQL_MAX_PARALLEL_LOADS", "8")
	t.Setenv("RSERVER_WAREHOUSE_WAREHOUSE_SYNC_FREQ_IGNORE", "true")
	t.Setenv("RSERVER_WAREHOUSE_UPLOAD_FREQ_IN_S", "10")
	t.Setenv("RSERVER_WAREHOUSE_ENABLE_JITTER_FOR_SYNCS", "false")
	t.Setenv("RSERVER_BACKEND_CONFIG_CONFIG_FROM_FILE", "true")
	t.Setenv("RUDDER_ADMIN_PASSWORD", "password")
	t.Setenv("RUDDER_GRACEFUL_SHUTDOWN_TIMEOUT_EXIT", "false")
	t.Setenv("RSERVER_WAREHOUSE_MSSQL_ENABLE_DELETE_BY_JOBS", "true")
	t.Setenv("RSERVER_GATEWAY_WEB_PORT", strconv.Itoa(httpPort))
	t.Setenv("RSERVER_GATEWAY_ADMIN_WEB_PORT", strconv.Itoa(httpAdminPort))
	t.Setenv("RSERVER_ENABLE_STATS", "false")
	t.Setenv("RSERVER_BACKEND_CONFIG_CONFIG_JSONPATH", workspaceConfigPath)
	t.Setenv("RUDDER_TMPDIR", t.TempDir())

	svcDone := make(chan struct{})
	ctx, ctxCancel := context.WithCancel(context.Background())
	go func() {
		r := runner.New(runner.ReleaseInfo{EnterpriseToken: os.Getenv("ENTERPRISE_TOKEN")})
		_ = r.Run(ctx, []string{"mssql-integration-test"})

		close(svcDone)
	}()

	serviceHealthEndpoint := fmt.Sprintf("http://localhost:%d/health", httpPort)
	health.WaitUntilReady(ctx, t, serviceHealthEndpoint, time.Minute, time.Second, "serviceHealthEndpoint")

	t.Run("Events flow", func(t *testing.T) {
		db, err := mssql.Connect(mssql.Credentials{
			DBName:   "master",
			Password: "reallyStrongPwd123",
			User:     "SA",
			Host:     "localhost",
			SSLMode:  "disable",
			Port:     fmt.Sprint(mssqlPort),
		})
		require.NoError(t, err)
		require.NoError(t, db.Ping())

		jobsDB, err := postgres.Connect(postgres.Credentials{
			DBName:   "jobsdb",
			Password: "password",
			User:     "rudder",
			Host:     "localhost",
			SSLMode:  "disable",
			Port:     fmt.Sprint(jobsDBPort),
		})
		require.NoError(t, err)
		require.NoError(t, jobsDB.Ping())

		provider := warehouseutils.MSSQL

		testcase := []struct {
			name                  string
			writeKey              string
			schema                string
			sourceID              string
			destinationID         string
			eventsMap             testhelper.EventsCountMap
			stagingFilesEventsMap testhelper.EventsCountMap
			loadFilesEventsMap    testhelper.EventsCountMap
			tableUploadsEventsMap testhelper.EventsCountMap
			warehouseEventsMap    testhelper.EventsCountMap
			asyncJob              bool
			tables                []string
		}{
			{
				name:          "Upload Job",
				writeKey:      "YSQ3n267l1VQKGNbSuJE9fQbzON",
				schema:        "mssql_wh_integration",
				tables:        []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"},
				sourceID:      "1wRvLmEnMOONMbdspwaZhyCqXRE",
				destinationID: "21Ezdq58khNMj07VJB0VJmxLvgu",
			},
			{
				name:                  "Async Job",
				writeKey:              "2DkCpXZcEvPG2fcpUD3LmjPI7J6",
				schema:                "mssql_wh_sources_integration",
				tables:                []string{"tracks", "google_sheet"},
				sourceID:              "2DkCpUr0xfiINRJxIwqyqfyHdq4",
				destinationID:         "21Ezdq58kMNMj07VJB0VJmxLvgu",
				eventsMap:             testhelper.SourcesSendEventsMap(),
				stagingFilesEventsMap: testhelper.SourcesStagingFilesEventsMap(),
				loadFilesEventsMap:    testhelper.SourcesLoadFilesEventsMap(),
				tableUploadsEventsMap: testhelper.SourcesTableUploadsEventsMap(),
				warehouseEventsMap:    testhelper.SourcesWarehouseEventsMap(),
				asyncJob:              true,
			},
		}

		for _, tc := range testcase {
			tc := tc

			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()

				ts := testhelper.WareHouseTest{
					Schema:                tc.schema,
					WriteKey:              tc.writeKey,
					SourceID:              tc.sourceID,
					DestinationID:         tc.destinationID,
					Tables:                tc.tables,
					EventsMap:             tc.eventsMap,
					StagingFilesEventsMap: tc.stagingFilesEventsMap,
					LoadFilesEventsMap:    tc.loadFilesEventsMap,
					TableUploadsEventsMap: tc.tableUploadsEventsMap,
					WarehouseEventsMap:    tc.warehouseEventsMap,
					AsyncJob:              tc.asyncJob,
					UserID:                testhelper.GetUserId(provider),
					Provider:              provider,
					JobsDB:                jobsDB,
					JobRunID:              misc.FastUUID().String(),
					TaskRunID:             misc.FastUUID().String(),
					Client: &client.Client{
						SQL:  db,
						Type: client.SQLClient,
					},
					HTTPPort: httpPort,
				}
				ts.VerifyEvents(t)

				if !tc.asyncJob {
					ts.UserID = testhelper.GetUserId(provider)
				}
				ts.JobRunID = misc.FastUUID().String()
				ts.TaskRunID = misc.FastUUID().String()
				ts.VerifyModifiedEvents(t)
			})
		}
	})

	t.Run("Validations", func(t *testing.T) {
		destination := backendconfig.DestinationT{
			ID: "21Ezdq58khNMj07VJB0VJmxLvgu",
			Config: map[string]interface{}{
				"host":             templateConfigurations["mssqlHost"],
				"database":         templateConfigurations["mssqlDatabase"],
				"user":             templateConfigurations["mssqlUser"],
				"password":         templateConfigurations["mssqlPassword"],
				"port":             templateConfigurations["mssqlPort"],
				"sslMode":          "disable",
				"namespace":        "",
				"bucketProvider":   "MINIO",
				"bucketName":       templateConfigurations["minioBucketName"],
				"accessKeyID":      templateConfigurations["minioAccesskeyID"],
				"secretAccessKey":  templateConfigurations["minioSecretAccessKey"],
				"useSSL":           false,
				"endPoint":         templateConfigurations["minioEndpoint"],
				"syncFrequency":    "30",
				"useRudderStorage": false,
			},
			DestinationDefinition: backendconfig.DestinationDefinitionT{
				ID:          "1qvbUYC2xVQ7lvI9UUYkkM4IBt9",
				Name:        "MSSQL",
				DisplayName: "Microsoft SQL Server",
			},
			Name:       "mssql-demo",
			Enabled:    true,
			RevisionID: "29eeuUb21cuDBeFKPTUA9GaQ9Aq",
		}
		testhelper.VerifyConfigurationTest(t, destination)
	})

	ctxCancel()
	<-svcDone
}
