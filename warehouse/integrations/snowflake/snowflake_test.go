package snowflake_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
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

const (
	snowflakeTestKey     = "SNOWFLAKE_INTEGRATION_TEST_CREDENTIALS"
	snowflakeTestRBACKey = "SNOWFLAKE_RBAC_INTEGRATION_TEST_CREDENTIALS"
)

type snowflakeTestCredentials struct {
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

func getSnowflakeTestCredentials(key string) (*snowflakeTestCredentials, error) {
	cred, exists := os.LookupEnv(key)
	if !exists {
		return nil, errors.New("snowflake test credentials not found")
	}

	var credentials snowflakeTestCredentials
	err := json.Unmarshal([]byte(cred), &credentials)
	if err != nil {
		return nil, fmt.Errorf("failed to snowflake redshift test credentials: %w", err)
	}
	return &credentials, nil
}

func isSnowflakeTestCredentialsAvailable() bool {
	_, err := getSnowflakeTestCredentials(snowflakeTestKey)
	return err == nil
}

func isSnowflakeTestRBACCredentialsAvailable() bool {
	_, err := getSnowflakeTestCredentials(snowflakeTestRBACKey)
	return err == nil
}

func TestIntegration(t *testing.T) {
	if os.Getenv("SLOW") != "1" {
		t.Skip("Skipping tests. Add 'SLOW=1' env var to run test.")
	}
	if !isSnowflakeTestCredentialsAvailable() {
		t.Skipf("Skipping %s as %s is not set", t.Name(), snowflakeTestKey)
	}
	if !isSnowflakeTestRBACCredentialsAvailable() {
		t.Skipf("Skipping %s as %s is not set", t.Name(), snowflakeTestRBACKey)
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

	schema := testhelper.RandSchema(warehouseutils.SNOWFLAKE)
	roleSchema := fmt.Sprintf("%s_%s", schema, "ROLE")
	sourcesSchema := fmt.Sprintf("%s_%s", schema, "SOURCES")
	caseSensitiveSchema := fmt.Sprintf("%s_%s", schema, "CS")

	credentials, err := getSnowflakeTestCredentials(snowflakeTestKey)
	require.NoError(t, err)

	rbacCrecentials, err := getSnowflakeTestCredentials(snowflakeTestRBACKey)
	require.NoError(t, err)

	templateConfigurations := map[string]string{
		"workspaceId":                     "BpLnfgDsc2WD8F2qNfHK5a84jjJ",
		"snowflakeWriteKey":               "2eSJyYtqwcFiUILzXv2fcNIrWO7",
		"snowflakeCaseSensitiveWriteKey":  "2eSJyYtqwcFYUILzXv2fcNIrWO7",
		"snowflakeRBACWriteKey":           "2eSafstqwcFYUILzXv2fcNIrWO7",
		"snowflakeSourcesWriteKey":        "2eSJyYtqwcFYerwzXv2fcNIrWO7",
		"snowflakeAccount":                credentials.Account,
		"snowflakeUser":                   credentials.User,
		"snowflakePassword":               credentials.Password,
		"snowflakeRole":                   credentials.Role,
		"snowflakeDatabase":               credentials.Database,
		"snowflakeCaseSensitiveDatabase":  strings.ToLower(credentials.Database),
		"snowflakeWarehouse":              credentials.Warehouse,
		"snowflakeBucketName":             credentials.BucketName,
		"snowflakeAccessKeyID":            credentials.AccessKeyID,
		"snowflakeAccessKey":              credentials.AccessKey,
		"snowflakeNamespace":              schema,
		"snowflakeSourcesNamespace":       sourcesSchema,
		"snowflakeCaseSensitiveNamespace": caseSensitiveSchema,
		"snowflakeRBACNamespace":          roleSchema,
		"snowflakeRBACAccount":            rbacCrecentials.Account,
		"snowflakeRBACUser":               rbacCrecentials.User,
		"snowflakeRBACPassword":           rbacCrecentials.Password,
		"snowflakeRBACRole":               rbacCrecentials.Role,
		"snowflakeRBACDatabase":           rbacCrecentials.Database,
		"snowflakeRBACWarehouse":          rbacCrecentials.Warehouse,
		"snowflakeRBACBucketName":         rbacCrecentials.BucketName,
		"snowflakeRBACAccessKeyID":        rbacCrecentials.AccessKeyID,
		"snowflakeRBACAccessKey":          rbacCrecentials.AccessKey,
	}
	workspaceConfigPath := testhelper.CreateTempFile(t, "testdata/template.json", templateConfigurations)

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
		r := runner.New(runner.ReleaseInfo{EnterpriseToken: os.Getenv("ENTERPRISE_TOKEN")})
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

		provider := warehouseutils.SNOWFLAKE
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
				schema:        schema,
				tables:        []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"},
				writeKey:      "2eSJyYtqwcFiUILzXv2fcNIrWO7",
				sourceID:      "24p1HhPk09FW25Kuzvx7GshCLKR",
				destinationID: "24qeADObp6eIhjjDnEppO6P1SNc",
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
					Account:   rbacCrecentials.Account,
					Warehouse: rbacCrecentials.Warehouse,
					Database:  rbacCrecentials.Database,
					User:      rbacCrecentials.User,
					Role:      rbacCrecentials.Role,
					Password:  rbacCrecentials.Password,
				},
				database:      database,
				schema:        roleSchema,
				tables:        []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"},
				writeKey:      "2eSafstqwcFYUILzXv2fcNIrWO7",
				sourceID:      "24p1HhPsafaFBMKuzvx7GshCLKR",
				destinationID: "24qeADObsdsJhijDnEppO6P1SNc",
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
				schema:        caseSensitiveSchema,
				tables:        []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"},
				writeKey:      "2eSJyYtqwcFYUILzXv2fcNIrWO7",
				sourceID:      "24p1HhPk09FBMKuzvx7GshCLKR",
				destinationID: "24qeADObp6eJhijDnEppO6P1SNc",
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
				schema:        sourcesSchema,
				tables:        []string{"tracks", "google_sheet"},
				writeKey:      "2eSJyYtqwcFYerwzXv2fcNIrWO7",
				sourceID:      "2DkCpUr0xgjaNRJxIwqyqfyHdq4",
				destinationID: "24qeADObp6eIsfjDnEppO6P1SNc",
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
		destination := backendconfig.DestinationT{
			ID: "24qeADObp6eIhjjDnEppO6P1SNc",
			Config: map[string]interface{}{
				"account":            templateConfigurations["snowflakeAccount"],
				"database":           templateConfigurations["snowflakeDatabase"],
				"warehouse":          templateConfigurations["snowflakeWarehouse"],
				"user":               templateConfigurations["snowflakeUser"],
				"password":           templateConfigurations["snowflakePassword"],
				"cloudProvider":      "AWS",
				"bucketName":         templateConfigurations["snowflakeBucketName"],
				"storageIntegration": "",
				"accessKeyID":        templateConfigurations["snowflakeAccessKeyID"],
				"accessKey":          templateConfigurations["snowflakeAccessKey"],
				"namespace":          templateConfigurations["snowflakeNamespace"],
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
		testhelper.VerifyConfigurationTest(t, destination)
	})

	ctxCancel()
	<-svcDone
}
