package snowflake_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/rudderlabs/rudder-server/testhelper/workspaceConfig"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/rudderlabs/compose-test/testcompose"
	kitHelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	"github.com/rudderlabs/rudder-server/runner"
	"github.com/rudderlabs/rudder-server/testhelper/health"
	snowflakedb "github.com/snowflakedb/gosnowflake"

	"github.com/rudderlabs/rudder-server/warehouse/encoding"

	"github.com/rudderlabs/rudder-server/warehouse/integrations/testhelper"

	"github.com/rudderlabs/rudder-server/warehouse/integrations/snowflake"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/validations"

	"github.com/rudderlabs/rudder-server/warehouse/client"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"

	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/stretchr/testify/require"
)

type testCredentials struct {
	Account     string `json:"account"`
	User        string `json:"user"`
	Password    string `json:"password"`
	Role        string `json:"role"`
	Database    string `json:"database"`
	Warehouse   string `json:"warehouse"`
	BucketName  string `json:"bucketName"`
	AccessKeyID string `json:"accessKeyID"`
	AccessKey   string `json:"accessKey"`
}

const (
	testKey     = "SNOWFLAKE_INTEGRATION_TEST_CREDENTIALS"
	testRBACKey = "SNOWFLAKE_RBAC_INTEGRATION_TEST_CREDENTIALS"
)

func getSnowflakeTestCredentials(key string) (*testCredentials, error) {
	cred, exists := os.LookupEnv(key)
	if !exists {
		return nil, errors.New("snowflake test credentials not found")
	}

	var credentials testCredentials
	err := json.Unmarshal([]byte(cred), &credentials)
	if err != nil {
		return nil, fmt.Errorf("failed to snowflake redshift test credentials: %w", err)
	}
	return &credentials, nil
}

func isSnowflakeTestCredentialsAvailable() bool {
	_, err := getSnowflakeTestCredentials(testKey)
	return err == nil
}

func isSnowflakeTestRBACCredentialsAvailable() bool {
	_, err := getSnowflakeTestCredentials(testRBACKey)
	return err == nil
}

func TestIntegration(t *testing.T) {
	if os.Getenv("SLOW") != "1" {
		t.Skip("Skipping tests. Add 'SLOW=1' env var to run test.")
	}
	if !isSnowflakeTestCredentialsAvailable() {
		t.Skipf("Skipping %s as %s is not set", t.Name(), testKey)
	}
	if !isSnowflakeTestRBACCredentialsAvailable() {
		t.Skipf("Skipping %s as %s is not set", t.Name(), testRBACKey)
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

	jobsDBPort := c.Port("jobsDb", 5432)
	transformerPort := c.Port("transformer", 9090)

	httpPort, err := kitHelper.GetFreePort()
	require.NoError(t, err)
	httpAdminPort, err := kitHelper.GetFreePort()
	require.NoError(t, err)

	workspaceID := warehouseutils.RandHex()
	sourceID := warehouseutils.RandHex()
	destinationID := warehouseutils.RandHex()
	writeKey := warehouseutils.RandHex()
	caseSensitiveSourceID := warehouseutils.RandHex()
	caseSensitiveDestinationID := warehouseutils.RandHex()
	caseSensitiveWriteKey := warehouseutils.RandHex()
	rbacSourceID := warehouseutils.RandHex()
	rbacDestinationID := warehouseutils.RandHex()
	rbacWriteKey := warehouseutils.RandHex()
	sourcesSourceID := warehouseutils.RandHex()
	sourcesDestinationID := warehouseutils.RandHex()
	sourcesWriteKey := warehouseutils.RandHex()

	provider := warehouseutils.SNOWFLAKE

	namespace := testhelper.RandSchema(provider)
	rbacNamespace := testhelper.RandSchema(provider)
	sourcesNamespace := testhelper.RandSchema(provider)
	caseSensitiveNamespace := testhelper.RandSchema(provider)

	credentials, err := getSnowflakeTestCredentials(testKey)
	require.NoError(t, err)

	rbacCredentials, err := getSnowflakeTestCredentials(testRBACKey)
	require.NoError(t, err)

	templateConfigurations := map[string]any{
		"workspaceID":                workspaceID,
		"sourceID":                   sourceID,
		"destinationID":              destinationID,
		"writeKey":                   writeKey,
		"caseSensitiveSourceID":      caseSensitiveSourceID,
		"caseSensitiveDestinationID": caseSensitiveDestinationID,
		"caseSensitiveWriteKey":      caseSensitiveWriteKey,
		"rbacSourceID":               rbacSourceID,
		"rbacDestinationID":          rbacDestinationID,
		"rbacWriteKey":               rbacWriteKey,
		"sourcesSourceID":            sourcesSourceID,
		"sourcesDestinationID":       sourcesDestinationID,
		"sourcesWriteKey":            sourcesWriteKey,
		"account":                    credentials.Account,
		"user":                       credentials.User,
		"password":                   credentials.Password,
		"role":                       credentials.Role,
		"database":                   credentials.Database,
		"caseSensitiveDatabase":      strings.ToLower(credentials.Database),
		"warehouse":                  credentials.Warehouse,
		"bucketName":                 credentials.BucketName,
		"accessKeyID":                credentials.AccessKeyID,
		"accessKey":                  credentials.AccessKey,
		"namespace":                  namespace,
		"sourcesNamespace":           sourcesNamespace,
		"caseSensitiveNamespace":     caseSensitiveNamespace,
		"rbacNamespace":              rbacNamespace,
		"rbacAccount":                rbacCredentials.Account,
		"rbacUser":                   rbacCredentials.User,
		"rbacPassword":               rbacCredentials.Password,
		"rbacRole":                   rbacCredentials.Role,
		"rbacDatabase":               rbacCredentials.Database,
		"rbacWarehouse":              rbacCredentials.Warehouse,
		"rbacBucketName":             rbacCredentials.BucketName,
		"rbacAccessKeyID":            rbacCredentials.AccessKeyID,
		"rbacAccessKey":              rbacCredentials.AccessKey,
	}
	workspaceConfigPath := workspaceConfig.CreateTempFile(t, "testdata/template.json", templateConfigurations)

	t.Setenv("JOBS_DB_HOST", "localhost")
	t.Setenv("JOBS_DB_NAME", "jobsdb")
	t.Setenv("JOBS_DB_DB_NAME", "jobsdb")
	t.Setenv("JOBS_DB_USER", "rudder")
	t.Setenv("JOBS_DB_PASSWORD", "password")
	t.Setenv("JOBS_DB_SSL_MODE", "disable")
	t.Setenv("JOBS_DB_PORT", fmt.Sprint(jobsDBPort))
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
	t.Setenv("RSERVER_WAREHOUSE_SNOWFLAKE_MAX_PARALLEL_LOADS", "8")
	t.Setenv("RSERVER_WAREHOUSE_WAREHOUSE_SYNC_FREQ_IGNORE", "true")
	t.Setenv("RSERVER_WAREHOUSE_UPLOAD_FREQ_IN_S", "10")
	t.Setenv("RSERVER_WAREHOUSE_ENABLE_JITTER_FOR_SYNCS", "false")
	t.Setenv("RSERVER_WAREHOUSE_ENABLE_IDRESOLUTION", "true")
	t.Setenv("RSERVER_BACKEND_CONFIG_CONFIG_FROM_FILE", "true")
	t.Setenv("RUDDER_ADMIN_PASSWORD", "password")
	t.Setenv("RUDDER_GRACEFUL_SHUTDOWN_TIMEOUT_EXIT", "false")
	t.Setenv("RSERVER_WAREHOUSE_SNOWFLAKE_ENABLE_DELETE_BY_JOBS", "true")
	t.Setenv("RSERVER_GATEWAY_WEB_PORT", strconv.Itoa(httpPort))
	t.Setenv("RSERVER_GATEWAY_ADMIN_WEB_PORT", strconv.Itoa(httpAdminPort))
	t.Setenv("RSERVER_ENABLE_STATS", "false")
	t.Setenv("RSERVER_BACKEND_CONFIG_CONFIG_JSONPATH", workspaceConfigPath)
	t.Setenv("RUDDER_TMPDIR", t.TempDir())

	svcDone := make(chan struct{})
	ctx, ctxCancel := context.WithCancel(context.Background())
	go func() {
		r := runner.New(runner.ReleaseInfo{})
		_ = r.Run(ctx, []string{"snowflake-integration-test"})

		close(svcDone)
	}()

	serviceHealthEndpoint := fmt.Sprintf("http://localhost:%d/health", httpPort)
	health.WaitUntilReady(ctx, t, serviceHealthEndpoint, time.Minute, time.Second, "serviceHealthEndpoint")

	t.Run("Event flow", func(t *testing.T) {
		dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
			"rudder", "password", "localhost", fmt.Sprint(jobsDBPort), "jobsdb",
		)
		jobsDB, err := sql.Open("postgres", dsn)
		require.NoError(t, err)
		require.NoError(t, jobsDB.Ping())

		database := credentials.Database

		testcase := []struct {
			name                          string
			credentials                   snowflake.Credentials
			database                      string
			schema                        string
			writeKey                      string
			sourceID                      string
			destinationID                 string
			eventsMap                     testhelper.EventsCountMap
			stagingFilesEventsMap         testhelper.EventsCountMap
			stagingFilesModifiedEventsMap testhelper.EventsCountMap
			loadFilesEventsMap            testhelper.EventsCountMap
			tableUploadsEventsMap         testhelper.EventsCountMap
			warehouseEventsMap            testhelper.EventsCountMap
			asyncJob                      bool
			tables                        []string
		}{
			{
				name: "Upload Job with Normal Database",
				credentials: snowflake.Credentials{
					Account:   credentials.Account,
					Warehouse: credentials.Warehouse,
					Database:  credentials.Database,
					User:      credentials.User,
					Role:      credentials.Role,
					Password:  credentials.Password,
				},
				database:      database,
				schema:        namespace,
				tables:        []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"},
				writeKey:      writeKey,
				sourceID:      sourceID,
				destinationID: destinationID,
				stagingFilesEventsMap: testhelper.EventsCountMap{
					"wh_staging_files": 34, // 32 + 2 (merge events because of ID resolution)
				},
				stagingFilesModifiedEventsMap: testhelper.EventsCountMap{
					"wh_staging_files": 34, // 32 + 2 (merge events because of ID resolution)
				},
			},
			{
				name: "Upload Job with Role",
				credentials: snowflake.Credentials{
					Account:   rbacCredentials.Account,
					Warehouse: rbacCredentials.Warehouse,
					Database:  rbacCredentials.Database,
					User:      rbacCredentials.User,
					Role:      rbacCredentials.Role,
					Password:  rbacCredentials.Password,
				},
				database:      database,
				schema:        rbacNamespace,
				tables:        []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"},
				writeKey:      rbacWriteKey,
				sourceID:      rbacSourceID,
				destinationID: rbacDestinationID,
				stagingFilesEventsMap: testhelper.EventsCountMap{
					"wh_staging_files": 34, // 32 + 2 (merge events because of ID resolution)
				},
				stagingFilesModifiedEventsMap: testhelper.EventsCountMap{
					"wh_staging_files": 34, // 32 + 2 (merge events because of ID resolution)
				},
			},
			{
				name: "Upload Job with Case Sensitive Database",
				credentials: snowflake.Credentials{
					Account:   credentials.Account,
					Warehouse: credentials.Warehouse,
					Database:  credentials.Database,
					User:      credentials.User,
					Role:      credentials.Role,
					Password:  credentials.Password,
				},
				database:      strings.ToLower(database),
				schema:        caseSensitiveNamespace,
				tables:        []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"},
				writeKey:      caseSensitiveWriteKey,
				sourceID:      caseSensitiveSourceID,
				destinationID: caseSensitiveDestinationID,
				stagingFilesEventsMap: testhelper.EventsCountMap{
					"wh_staging_files": 34, // 32 + 2 (merge events because of ID resolution)
				},
				stagingFilesModifiedEventsMap: testhelper.EventsCountMap{
					"wh_staging_files": 34, // 32 + 2 (merge events because of ID resolution)
				},
			},
			{
				name: "Async Job with Sources",
				credentials: snowflake.Credentials{
					Account:   credentials.Account,
					Warehouse: credentials.Warehouse,
					Database:  credentials.Database,
					User:      credentials.User,
					Role:      credentials.Role,
					Password:  credentials.Password,
				},
				database:      database,
				schema:        sourcesNamespace,
				tables:        []string{"tracks", "google_sheet"},
				writeKey:      sourcesWriteKey,
				sourceID:      sourcesSourceID,
				destinationID: sourcesDestinationID,
				eventsMap:     testhelper.SourcesSendEventsMap(),
				stagingFilesEventsMap: testhelper.EventsCountMap{
					"wh_staging_files": 9, // 8 + 1 (merge events because of ID resolution)
				},
				stagingFilesModifiedEventsMap: testhelper.EventsCountMap{
					"wh_staging_files": 8, // 8 (de-duped by encounteredMergeRuleMap)
				},
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

				cred := tc.credentials
				cred.Database = tc.database

				urlConfig := snowflakedb.Config{
					Account:     cred.Account,
					User:        cred.User,
					Role:        cred.Role,
					Password:    cred.Password,
					Database:    cred.Database,
					Warehouse:   cred.Warehouse,
					Application: snowflake.Application,
				}

				dsn, err := snowflakedb.DSN(&urlConfig)
				require.NoError(t, err)

				db, err := sql.Open("snowflake", dsn)
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
					WorkspaceID:           workspaceID,
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
				ts.StagingFilesEventsMap = tc.stagingFilesModifiedEventsMap
				ts.JobRunID = misc.FastUUID().String()
				ts.TaskRunID = misc.FastUUID().String()
				ts.VerifyModifiedEvents(t)
			})
		}
	})

	t.Run("Validation", func(t *testing.T) {
		dest := backendconfig.DestinationT{
			ID: "24qeADObp6eIhjjDnEppO6P1SNc",
			Config: map[string]interface{}{
				"account":            credentials.Account,
				"database":           credentials.Database,
				"warehouse":          credentials.Warehouse,
				"user":               credentials.User,
				"password":           credentials.Password,
				"cloudProvider":      "AWS",
				"bucketName":         credentials.BucketName,
				"storageIntegration": "",
				"accessKeyID":        credentials.AccessKeyID,
				"accessKey":          credentials.AccessKey,
				"namespace":          namespace,
				"prefix":             "snowflake-prefix",
				"syncFrequency":      "30",
				"enableSSE":          false,
				"useRudderStorage":   false,
			},
			DestinationDefinition: backendconfig.DestinationDefinitionT{
				ID:          "1XjvXnzw34UMAz1YOuKqL1kwzh6",
				Name:        "SNOWFLAKE",
				DisplayName: "Snowflake",
			},
			Name:       "snowflake-demo",
			Enabled:    true,
			RevisionID: "29HgdgvNPwqFDMONSgmIZ3YSehV",
		}
		testhelper.VerifyConfigurationTest(t, dest)
	})

	ctxCancel()
	<-svcDone
}
