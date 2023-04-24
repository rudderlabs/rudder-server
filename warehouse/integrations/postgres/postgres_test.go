package postgres_test

import (
	"context"
	"fmt"
	"github.com/rudderlabs/compose-test/testcompose"
	kitHelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	"github.com/rudderlabs/rudder-server/runner"
	"github.com/rudderlabs/rudder-server/testhelper/health"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/rudderlabs/rudder-server/warehouse/encoding"

	"github.com/rudderlabs/rudder-server/warehouse/integrations/testhelper"

	"github.com/rudderlabs/rudder-server/warehouse/integrations/postgres"

	"github.com/rudderlabs/rudder-server/warehouse/tunnelling"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/validations"
	"github.com/stretchr/testify/require"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/warehouse/client"
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
	postgresPort := c.Port("wh-postgres", 5432)
	//privatePostgresPort := c.Port("db-private-postgres", 5432)
	sshPort := c.Port("wh-ssh-server", 2222)

	httpPort, err := kitHelper.GetFreePort()
	require.NoError(t, err)
	httpAdminPort, err := kitHelper.GetFreePort()
	require.NoError(t, err)

	templateConfigurations := map[string]string{
		"workspaceId":               "BpLnfgDsc2WD8F2qNfHK5a84jjJ",
		"postgresWriteKey":          "kwzDkh9h2fhfUVuS9jZ8uVbhV3v",
		"postgresHost":              "localhost",
		"postgresDatabase":          "rudderdb",
		"postgresUser":              "rudder",
		"postgresPassword":          "rudder-password",
		"postgresPort":              fmt.Sprint(postgresPort),
		"postgresSourcesWriteKey":   "2DkCpXZcEvJK2fcpUD3LmjPI7J6",
		"postgresTunnelledWriteKey": "kwzDkh9h2fhfUVuS9jZ8uVbhV3w",
		"sshUser":                   "rudderstack",
		"sshPort":                   fmt.Sprint(sshPort),
		"sshHost":                   "localhost",
		"sshPrivateKey":             "-----BEGIN OPENSSH PRIVATE KEY-----\\n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\\nbG9jYWwBAgMEBQYH\\n-----END OPENSSH PRIVATE KEY-----",
		"privatePostgresHost":       "localhost",
		"privatePostgresDatabase":   "postgres",
		"privatePostgresPort":       "5432",
		"privatePostgresUser":       "postgres",
		"privatePostgresPassword":   "postgres",
		"minioBucketName":           "testbucket",
		"minioAccesskeyID":          "MYACCESSKEY",
		"minioSecretAccessKey":      "MYSECRETKEY",
		"minioEndpoint":             fmt.Sprintf("localhost:%d", minioPort),
	}
	workspaceConfigPath := testhelper.CreateTempFile(t, "testdata/template.json", templateConfigurations)

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
	t.Setenv("RSERVER_WAREHOUSE_POSTGRES_MAX_PARALLEL_LOADS", "8")
	t.Setenv("RSERVER_WAREHOUSE_WAREHOUSE_SYNC_FREQ_IGNORE", "true")
	t.Setenv("RSERVER_WAREHOUSE_UPLOAD_FREQ_IN_S", "10")
	t.Setenv("RSERVER_WAREHOUSE_ENABLE_JITTER_FOR_SYNCS", "false")
	t.Setenv("RSERVER_BACKEND_CONFIG_CONFIG_FROM_FILE", "true")
	t.Setenv("RUDDER_ADMIN_PASSWORD", "password")
	t.Setenv("RUDDER_GRACEFUL_SHUTDOWN_TIMEOUT_EXIT", "false")
	t.Setenv("RSERVER_WAREHOUSE_POSTGRES_SKIP_COMPUTING_USER_LATEST_TRAITS_WORKSPACE_IDS", "BpLnfgDsc2WD8F2qNfHK5a84jjJ")
	t.Setenv("RSERVER_WAREHOUSE_POSTGRES_ENABLE_SQLSTATEMENT_EXECUTION_PLAN_WORKSPACE_IDS", "BpLnfgDsc2WD8F2qNfHK5a84jjJ")
	t.Setenv("RSERVER_WAREHOUSE_POSTGRES_ENABLE_DELETE_BY_JOBS", "true")
	t.Setenv("RSERVER_GATEWAY_WEB_PORT", strconv.Itoa(httpPort))
	t.Setenv("RSERVER_GATEWAY_ADMIN_WEB_PORT", strconv.Itoa(httpAdminPort))
	t.Setenv("RSERVER_ENABLE_STATS", "false")
	t.Setenv("RSERVER_BACKEND_CONFIG_CONFIG_JSONPATH", workspaceConfigPath)
	t.Setenv("RUDDER_TMPDIR", t.TempDir())

	svcDone := make(chan struct{})
	ctx, ctxCancel := context.WithCancel(context.Background())
	go func() {
		r := runner.New(runner.ReleaseInfo{EnterpriseToken: os.Getenv("ENTERPRISE_TOKEN")})
		_ = r.Run(ctx, []string{"postgres-integration-test"})

		close(svcDone)
	}()

	serviceHealthEndpoint := fmt.Sprintf("http://localhost:%d/health", httpPort)
	health.WaitUntilReady(ctx, t, serviceHealthEndpoint, time.Minute, time.Second, "serviceHealthEndpoint")

	t.Run("Events flow", func(t *testing.T) {
		db, err := postgres.Connect(postgres.Credentials{
			DBName:   "rudderdb",
			Password: "rudder-password",
			User:     "rudder",
			Host:     "localhost",
			SSLMode:  "disable",
			Port:     fmt.Sprint(postgresPort),
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

		provider := warehouseutils.POSTGRES

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
				writeKey:      "kwzDkh9h2fhfUVuS9jZ8uVbhV3v",
				schema:        "postgres_wh_integration",
				tables:        []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"},
				sourceID:      "1wRvLmEnMOOxSQD9pwaZhyCqXRE",
				destinationID: "216ZvbavR21Um6eGKQCagZHqLGZ",
			},
			{
				name:                  "Async Job",
				writeKey:              "2DkCpXZcEvJK2fcpUD3LmjPI7J6",
				schema:                "postgres_wh_sources_integration",
				tables:                []string{"tracks", "google_sheet"},
				sourceID:              "2DkCpUr0xfiGBPJxIwqyqfyHdq4",
				destinationID:         "308ZvbavR21Um6eGKQCagZHqLGZ",
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

	t.Run("Events flow with ssh tunnel", func(t *testing.T) {
		// Check with @Abhimanyu around this
		t.Skip()

		db, err := postgres.Connect(postgres.Credentials{
			DBName:   templateConfigurations["privatePostgresDatabase"],
			Password: templateConfigurations["privatePostgresPassword"],
			User:     templateConfigurations["privatePostgresUser"],
			Host:     templateConfigurations["privatePostgresHost"],
			Port:     templateConfigurations["privatePostgresPort"],
			SSLMode:  "disable",
			TunnelInfo: &tunnelling.TunnelInfo{
				Config: map[string]interface{}{
					"sshUser":       templateConfigurations["sshUser"],
					"sshPort":       templateConfigurations["sshPort"],
					"sshHost":       templateConfigurations["sshHost"],
					"sshPrivateKey": strings.ReplaceAll(templateConfigurations["sshPrivateKey"], "\\n", "\n"),
				},
			},
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

		testcases := []struct {
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
				name:          "upload job through ssh tunnelling",
				writeKey:      "kwzDkh9h2fhfUVuS9jZ8uVbhV3w",
				schema:        "postgres_wh_ssh_tunnelled_integration",
				tables:        []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"},
				sourceID:      "1wRvLmEnMOOxSQD9pwaZhyCqXRF",
				destinationID: "216ZvbavR21Um6eGKQCagZHqLGZ",
			},
		}

		for _, tc := range testcases {
			tc := tc

			t.Run(tc.name, func(t *testing.T) {
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
					UserID:                testhelper.GetUserId(warehouseutils.POSTGRES),
					Provider:              warehouseutils.POSTGRES,
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

				ts.UserID = testhelper.GetUserId(warehouseutils.POSTGRES)
				ts.JobRunID = misc.FastUUID().String()
				ts.TaskRunID = misc.FastUUID().String()
				ts.VerifyModifiedEvents(t)
			})
		}
	})

	t.Run("Validations", func(t *testing.T) {
		destination := backendconfig.DestinationT{
			ID: "216ZvbavR21Um6eGKQCagZHqLGZ",
			Config: map[string]interface{}{
				"host":             templateConfigurations["postgresHost"],
				"database":         templateConfigurations["postgresDatabase"],
				"user":             templateConfigurations["postgresUser"],
				"password":         templateConfigurations["postgresPassword"],
				"port":             templateConfigurations["postgresPort"],
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
				ID:          "1bJ4YC7INdkvBTzotNh0zta5jDm",
				Name:        "POSTGRES",
				DisplayName: "Postgres",
			},
			Name:       "postgres-demo",
			Enabled:    true,
			RevisionID: "29eeuu9kywWsRAybaXcxcnTVEl8",
		}
		testhelper.VerifyConfigurationTest(t, destination)
	})

	ctxCancel()
	<-svcDone
}
