package redshift_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/rudderlabs/compose-test/testcompose"
	kitHelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	"github.com/rudderlabs/rudder-server/runner"
	"github.com/rudderlabs/rudder-server/testhelper/health"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/postgres"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/rudderlabs/rudder-server/warehouse/encoding"

	"github.com/lib/pq"
	"github.com/ory/dockertest/v3"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource"

	"github.com/rudderlabs/rudder-server/warehouse/integrations/testhelper"

	"github.com/rudderlabs/rudder-server/warehouse/integrations/redshift"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/validations"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/warehouse/client"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type rsTestCredentials struct {
	Host        string `json:"host"`
	Port        string `json:"port"`
	UserName    string `json:"userName"`
	Password    string `json:"password"`
	DbName      string `json:"dbName"`
	BucketName  string `json:"bucketName"`
	AccessKeyID string `json:"accessKeyID"`
	AccessKey   string `json:"accessKey"`
}

func getRSTestCredentials() (*rsTestCredentials, error) {
	cred, exists := os.LookupEnv(testhelper.RedshiftIntegrationTestCredentials)
	if !exists {
		return nil, errors.New("redshift test credentials not found")
	}

	var credentials rsTestCredentials
	err := json.Unmarshal([]byte(cred), &credentials)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal redshift test credentials: %w", err)
	}
	return &credentials, nil
}

func isRSTestCredentialsAvailable() bool {
	_, err := getRSTestCredentials()
	return err == nil
}

func TestIntegration(t *testing.T) {
	if os.Getenv("SLOW") != "1" {
		t.Skip("Skipping tests. Add 'SLOW=1' env var to run test.")
	}
	if !isRSTestCredentialsAvailable() {
		t.Skipf("Skipping %s as %s is not set", t.Name(), testhelper.RedshiftIntegrationTestCredentials)
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
	transformerPort := c.Port("wh-transformer", 9090)

	httpPort, err := kitHelper.GetFreePort()
	require.NoError(t, err)
	httpAdminPort, err := kitHelper.GetFreePort()
	require.NoError(t, err)

	schema := testhelper.RandSchema(warehouseutils.RS)
	sourcesSchema := fmt.Sprintf("%s_%s", schema, "sources")

	rsTestCredentials, err := getRSTestCredentials()
	require.NoError(t, err)

	templateConfigurations := map[string]string{
		"workspaceId":              "BpLnfgDsc2WD8F2qNfHK5a84jjJ",
		"redshiftWriteKey":         "JAAwdCxmM8BIabKERsUhPNmMmdf",
		"redshiftHost":             rsTestCredentials.Host,
		"redshiftPort":             rsTestCredentials.Port,
		"redshiftUsername":         rsTestCredentials.UserName,
		"redshiftPassword":         rsTestCredentials.Password,
		"redshiftDbName":           rsTestCredentials.DbName,
		"redshiftBucketName":       rsTestCredentials.BucketName,
		"redshiftAccessKeyID":      rsTestCredentials.AccessKeyID,
		"redshiftAccessKey":        rsTestCredentials.AccessKey,
		"redshiftNamespace":        schema,
		"redshiftSourcesNamespace": sourcesSchema,
		"redshiftSourcesWriteKey":  "BNAwdCxmM8BIabKERsUhPNmMmdf",
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
	t.Setenv("GO_ENV", "production")
	t.Setenv("LOG_LEVEL", "INFO")
	t.Setenv("INSTANCE_ID", "1")
	t.Setenv("ALERT_PROVIDER", "pagerduty")
	t.Setenv("CONFIG_PATH", "../../../config/config.yaml")
	t.Setenv("DEST_TRANSFORM_URL", fmt.Sprintf("http://localhost:%d", transformerPort))
	t.Setenv("RSERVER_WAREHOUSE_REDSHIFT_MAX_PARALLEL_LOADS", "8")
	t.Setenv("RSERVER_WAREHOUSE_WAREHOUSE_SYNC_FREQ_IGNORE", "true")
	t.Setenv("RSERVER_WAREHOUSE_UPLOAD_FREQ_IN_S", "10")
	t.Setenv("RSERVER_WAREHOUSE_ENABLE_JITTER_FOR_SYNCS", "false")
	t.Setenv("RSERVER_BACKEND_CONFIG_CONFIG_FROM_FILE", "true")
	t.Setenv("RUDDER_ADMIN_PASSWORD", "password")
	t.Setenv("RUDDER_GRACEFUL_SHUTDOWN_TIMEOUT_EXIT", "false")
	t.Setenv("RSERVER_WAREHOUSE_REDSHIFT_MAX_PARALLEL_LOADS_WORKSPACE_IDS", "{\"BpLnfgDsc2WD8F2qNfHK5a84jjJ\":4}")
	t.Setenv("RSERVER_WAREHOUSE_REDSHIFT_ENABLE_DELETE_BY_JOBS", "true")
	t.Setenv("RSERVER_WAREHOUSE_REDSHIFT_DEDUP_WINDOW", "true")
	t.Setenv("RSERVER_WAREHOUSE_REDSHIFT_DEDUP_WINDOW_IN_HOURS", "5")
	t.Setenv("RSERVER_GATEWAY_WEB_PORT", strconv.Itoa(httpPort))
	t.Setenv("RSERVER_GATEWAY_ADMIN_WEB_PORT", strconv.Itoa(httpAdminPort))
	t.Setenv("RSERVER_ENABLE_STATS", "false")
	t.Setenv("RSERVER_BACKEND_CONFIG_CONFIG_JSONPATH", workspaceConfigPath)
	t.Setenv("RUDDER_TMPDIR", t.TempDir())

	svcDone := make(chan struct{})
	ctx, ctxCancel := context.WithCancel(context.Background())
	go func() {
		r := runner.New(runner.ReleaseInfo{EnterpriseToken: os.Getenv("ENTERPRISE_TOKEN")})
		_ = r.Run(ctx, []string{"redshift-integration-test"})

		close(svcDone)
	}()

	serviceHealthEndpoint := fmt.Sprintf("http://localhost:%d/health", httpPort)
	health.WaitUntilReady(ctx, t, serviceHealthEndpoint, time.Minute, time.Second, "serviceHealthEndpoint")

	t.Run("Event flow", func(t *testing.T) {
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

		provider := warehouseutils.RS

		testcase := []struct {
			name                  string
			schema                string
			writeKey              string
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
				schema:        schema,
				tables:        []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"},
				writeKey:      "JAAwdCxmM8BIabKERsUhPNmMmdf",
				sourceID:      "279L3gEKqwruBoKGsXZtSVX7vIy",
				destinationID: "27SthahyhhqZE74HT4NTtNPl06V",
			},
			{
				name:                  "Async Job",
				schema:                sourcesSchema,
				tables:                []string{"tracks", "google_sheet"},
				writeKey:              "BNAwdCxmM8BIabKERsUhPNmMmdf",
				sourceID:              "2DkCpUr0xgjfsdJxIwqyqfyHdq4",
				destinationID:         "27Sthahyhhsdas4HT4NTtNPl06V",
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

				db, err := redshift.Connect(redshift.RedshiftCredentials{
					Host:     rsTestCredentials.Host,
					Port:     rsTestCredentials.Port,
					DbName:   rsTestCredentials.DbName,
					Username: rsTestCredentials.UserName,
					Password: rsTestCredentials.Password,
				})
				require.NoError(t, err)
				require.NoError(t, db.Ping())

				t.Cleanup(func() {
					require.NoError(t, testhelper.WithConstantRetries(func() error {
						_, err := db.Exec(fmt.Sprintf(`DROP SCHEMA %q CASCADE;`, tc.schema))
						return err
					}))
				})

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
					Provider:              provider,
					JobsDB:                jobsDB,
					JobRunID:              misc.FastUUID().String(),
					TaskRunID:             misc.FastUUID().String(),
					UserID:                testhelper.GetUserId(provider),
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

	t.Run("Validation", func(t *testing.T) {
		destination := backendconfig.DestinationT{
			ID: "27SthahyhhqZE74HT4NTtNPl06V",
			Config: map[string]interface{}{
				"host":             templateConfigurations["redshiftHost"],
				"port":             templateConfigurations["redshiftPort"],
				"database":         templateConfigurations["redshiftDbName"],
				"user":             templateConfigurations["redshiftUsername"],
				"password":         templateConfigurations["redshiftPassword"],
				"bucketName":       templateConfigurations["redshiftBucketName"],
				"accessKeyID":      templateConfigurations["redshiftAccessKeyID"],
				"accessKey":        templateConfigurations["redshiftAccessKey"],
				"prefix":           "",
				"namespace":        templateConfigurations["redshiftNamespace"],
				"syncFrequency":    "30",
				"enableSSE":        false,
				"useRudderStorage": false,
			},
			DestinationDefinition: backendconfig.DestinationDefinitionT{
				ID:          "1UVZiJF7OgLaiIY2Jts8XOQE3M6",
				Name:        "RS",
				DisplayName: "Redshift",
			},
			Name:       "redshift-demo",
			Enabled:    true,
			RevisionID: "29HgOWobrn0RYZLpaSwPIbN2987",
		}
		testhelper.VerifyConfigurationTest(t, destination)
	})

	ctxCancel()
	<-svcDone
}

func TestCheckAndIgnoreColumnAlreadyExistError(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "nil error",
			expected: true,
		},
		{
			name: "column already exists error",
			err: &pq.Error{
				Code: "42701",
			},
			expected: true,
		},
		{
			name:     "other error",
			err:      errors.New("other error"),
			expected: false,
		},
	}

	for _, tc := range testCases {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, tc.expected, redshift.CheckAndIgnoreColumnAlreadyExistError(tc.err))
		})
	}
}

func TestRedshift_AlterColumn(t *testing.T) {
	t.Parallel()

	var (
		bigString      = strings.Repeat("a", 1024)
		smallString    = strings.Repeat("a", 510)
		testNamespace  = "test_namespace"
		testTable      = "test_table"
		testColumn     = "test_column"
		testColumnType = "text"
	)

	testCases := []struct {
		name       string
		createView bool
	}{
		{
			name: "success",
		},
		{
			name:       "view/rule",
			createView: true,
		},
	}

	for _, tc := range testCases {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			pool, err := dockertest.NewPool("")
			require.NoError(t, err)

			pgResource, err := resource.SetupPostgres(pool, t)
			require.NoError(t, err)

			rs := redshift.New()
			redshift.WithConfig(rs, config.Default)

			rs.DB = pgResource.DB
			rs.Namespace = testNamespace

			_, err = rs.DB.Exec(
				fmt.Sprintf("CREATE SCHEMA %s;",
					testNamespace,
				),
			)
			require.NoError(t, err)

			_, err = rs.DB.Exec(
				fmt.Sprintf("CREATE TABLE %q.%q (%s VARCHAR(512));",
					testNamespace,
					testTable,
					testColumn,
				),
			)
			require.NoError(t, err)

			if tc.createView {
				_, err = rs.DB.Exec(
					fmt.Sprintf("CREATE VIEW %[1]q.%[2]q AS SELECT * FROM %[1]q.%[3]q;",
						testNamespace,
						fmt.Sprintf("%s_view", testTable),
						testTable,
					),
				)
				require.NoError(t, err)
			}

			_, err = rs.DB.Exec(
				fmt.Sprintf("INSERT INTO %q.%q (%s) VALUES ('%s');",
					testNamespace,
					testTable,
					testColumn,
					smallString,
				),
			)
			require.NoError(t, err)

			_, err = rs.DB.Exec(
				fmt.Sprintf("INSERT INTO %q.%q (%s) VALUES ('%s');",
					testNamespace,
					testTable,
					testColumn,
					bigString,
				),
			)
			require.ErrorContains(t, err, errors.New("pq: value too long for type character varying(512)").Error())

			res, err := rs.AlterColumn(testTable, testColumn, testColumnType)
			require.NoError(t, err)

			if tc.createView {
				require.True(t, res.IsDependent)
				require.NotEmpty(t, res.Query)
			}

			_, err = rs.DB.Exec(
				fmt.Sprintf("INSERT INTO %q.%q (%s) VALUES ('%s');",
					testNamespace,
					testTable,
					testColumn,
					bigString,
				),
			)
			require.NoError(t, err)
		})
	}
}
