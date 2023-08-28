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

	sfdb "github.com/snowflakedb/gosnowflake"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/compose-test/compose"
	"github.com/rudderlabs/compose-test/testcompose"
	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/runner"
	"github.com/rudderlabs/rudder-server/testhelper/health"
	"github.com/rudderlabs/rudder-server/testhelper/workspaceConfig"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/testhelper"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/rudderlabs/rudder-server/warehouse/validations"
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

	c := testcompose.New(t, compose.FilePaths([]string{"../testdata/docker-compose.jobsdb.yml"}))
	c.Start(context.Background())

	misc.Init()
	validations.Init()
	whutils.Init()

	jobsDBPort := c.Port("jobsDb", 5432)

	httpPort, err := kithelper.GetFreePort()
	require.NoError(t, err)

	workspaceID := whutils.RandHex()
	sourceID := whutils.RandHex()
	destinationID := whutils.RandHex()
	writeKey := whutils.RandHex()
	caseSensitiveSourceID := whutils.RandHex()
	caseSensitiveDestinationID := whutils.RandHex()
	caseSensitiveWriteKey := whutils.RandHex()
	rbacSourceID := whutils.RandHex()
	rbacDestinationID := whutils.RandHex()
	rbacWriteKey := whutils.RandHex()
	sourcesSourceID := whutils.RandHex()
	sourcesDestinationID := whutils.RandHex()
	sourcesWriteKey := whutils.RandHex()

	destType := whutils.SNOWFLAKE

	namespace := testhelper.RandSchema(destType)
	rbacNamespace := testhelper.RandSchema(destType)
	sourcesNamespace := testhelper.RandSchema(destType)
	caseSensitiveNamespace := testhelper.RandSchema(destType)

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

	bootstrap := func(t testing.TB, appendMode bool) func() {
		loadTableStrategy := "MERGE"
		if appendMode {
			loadTableStrategy = "APPEND"
		}
		testhelper.EnhanceWithDefaultEnvs(t)
		t.Setenv("JOBS_DB_PORT", strconv.Itoa(jobsDBPort))
		t.Setenv("WAREHOUSE_JOBS_DB_PORT", strconv.Itoa(jobsDBPort))
		t.Setenv("RSERVER_WAREHOUSE_SNOWFLAKE_MAX_PARALLEL_LOADS", "8")
		t.Setenv("RSERVER_WAREHOUSE_SNOWFLAKE_ENABLE_DELETE_BY_JOBS", "true")
		t.Setenv("RSERVER_WAREHOUSE_SNOWFLAKE_LOAD_TABLE_STRATEGY", loadTableStrategy)
		t.Setenv("RSERVER_WAREHOUSE_WEB_PORT", strconv.Itoa(httpPort))
		t.Setenv("RSERVER_BACKEND_CONFIG_CONFIG_JSONPATH", workspaceConfigPath)
		t.Setenv("RSERVER_WAREHOUSE_SNOWFLAKE_SLOW_QUERY_THRESHOLD", "0s")

		ctx, cancel := context.WithCancel(context.Background())

		svcDone := make(chan struct{})
		go func() {
			r := runner.New(runner.ReleaseInfo{})
			_ = r.Run(ctx, []string{"snowflake-integration-test"})

			close(svcDone)
		}()
		t.Cleanup(func() { <-svcDone })

		serviceHealthEndpoint := fmt.Sprintf("http://localhost:%d/health", httpPort)
		health.WaitUntilReady(ctx, t, serviceHealthEndpoint, time.Minute, 100*time.Millisecond, "serviceHealthEndpoint")

		return cancel
	}

	t.Run("Event flow", func(t *testing.T) {
		jobsDB := testhelper.JobsDB(t, jobsDBPort)

		database := credentials.Database

		testcase := []struct {
			name                          string
			writeKey                      string
			schema                        string
			sourceID                      string
			destinationID                 string
			tables                        []string
			stagingFilesEventsMap         testhelper.EventsCountMap
			stagingFilesModifiedEventsMap testhelper.EventsCountMap
			loadFilesEventsMap            testhelper.EventsCountMap
			tableUploadsEventsMap         testhelper.EventsCountMap
			warehouseEventsMap            testhelper.EventsCountMap
			warehouseEventsMap2           testhelper.EventsCountMap
			cred                          *testCredentials
			database                      string
			asyncJob                      bool
			stagingFilePrefix             string
			appendMode                    bool
			customUserID                  string
		}{
			{
				name:     "Upload Job with Normal Database",
				writeKey: writeKey,
				schema:   namespace,
				tables: []string{
					"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups",
				},
				sourceID:      sourceID,
				destinationID: destinationID,
				cred:          credentials,
				database:      database,
				stagingFilesEventsMap: testhelper.EventsCountMap{
					"wh_staging_files": 34, // 32 + 2 (merge events because of ID resolution)
				},
				stagingFilesModifiedEventsMap: testhelper.EventsCountMap{
					"wh_staging_files": 34, // 32 + 2 (merge events because of ID resolution)
				},
				stagingFilePrefix: "testdata/upload-job",
			},
			{
				name:     "Upload Job with Role",
				writeKey: rbacWriteKey,
				schema:   rbacNamespace,
				tables: []string{
					"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups",
				},
				sourceID:      rbacSourceID,
				destinationID: rbacDestinationID,
				cred:          rbacCredentials,
				database:      database,
				stagingFilesEventsMap: testhelper.EventsCountMap{
					"wh_staging_files": 34, // 32 + 2 (merge events because of ID resolution)
				},
				stagingFilesModifiedEventsMap: testhelper.EventsCountMap{
					"wh_staging_files": 34, // 32 + 2 (merge events because of ID resolution)
				},
				stagingFilePrefix: "testdata/upload-job-with-role",
			},
			{
				name:     "Upload Job with Case Sensitive Database",
				writeKey: caseSensitiveWriteKey,
				schema:   caseSensitiveNamespace,
				tables: []string{
					"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups",
				},
				sourceID:      caseSensitiveSourceID,
				destinationID: caseSensitiveDestinationID,
				cred:          credentials,
				database:      strings.ToLower(database),
				stagingFilesEventsMap: testhelper.EventsCountMap{
					"wh_staging_files": 34, // 32 + 2 (merge events because of ID resolution)
				},
				stagingFilesModifiedEventsMap: testhelper.EventsCountMap{
					"wh_staging_files": 34, // 32 + 2 (merge events because of ID resolution)
				},
				stagingFilePrefix: "testdata/upload-job-case-sensitive",
			},
			{
				name:          "Async Job with Sources",
				writeKey:      sourcesWriteKey,
				schema:        sourcesNamespace,
				tables:        []string{"tracks", "google_sheet"},
				sourceID:      sourcesSourceID,
				destinationID: sourcesDestinationID,
				cred:          credentials,
				database:      database,
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
				stagingFilePrefix:     "testdata/sources-job",
			},
			{
				name:                          "Upload Job in append mode",
				writeKey:                      writeKey,
				schema:                        namespace,
				tables:                        []string{"identifies", "users", "tracks"},
				sourceID:                      sourceID,
				destinationID:                 destinationID,
				cred:                          credentials,
				database:                      database,
				stagingFilesEventsMap:         testhelper.EventsCountMap{"wh_staging_files": 3},
				stagingFilesModifiedEventsMap: testhelper.EventsCountMap{"wh_staging_files": 3},
				loadFilesEventsMap:            map[string]int{"identifies": 1, "users": 1, "tracks": 1},
				tableUploadsEventsMap:         map[string]int{"identifies": 1, "users": 1, "tracks": 1},
				warehouseEventsMap:            map[string]int{"identifies": 1, "users": 1, "tracks": 1},
				warehouseEventsMap2:           map[string]int{"identifies": 2, "users": 1, "tracks": 2},
				stagingFilePrefix:             "testdata/append-job",
				appendMode:                    true,
				customUserID:                  testhelper.GetUserId("append_test"),
			},
		}

		for _, tc := range testcase {
			tc := tc
			t.Run(tc.name, func(t *testing.T) {
				cancel := bootstrap(t, tc.appendMode)
				defer cancel()

				cred := tc.cred
				cred.Database = tc.database

				urlConfig := sfdb.Config{
					Account:   cred.Account,
					User:      cred.User,
					Role:      cred.Role,
					Password:  cred.Password,
					Database:  cred.Database,
					Warehouse: cred.Warehouse,
				}

				dsn, err := sfdb.DSN(&urlConfig)
				require.NoError(t, err)

				db := getSnowflakeDB(t, dsn)

				t.Cleanup(func() {
					var err error
					require.Eventuallyf(t,
						func() bool {
							_, err = db.Exec(fmt.Sprintf(`DROP SCHEMA %q CASCADE;`, tc.schema))
							return err == nil
						},
						time.Minute, 100*time.Millisecond,
						"error deleting schema: %v", err,
					)
				})

				sqlClient := &client.Client{
					SQL:  db,
					Type: client.SQLClient,
				}

				conf := map[string]interface{}{
					"cloudProvider":      "AWS",
					"bucketName":         credentials.BucketName,
					"storageIntegration": "",
					"accessKeyID":        credentials.AccessKeyID,
					"accessKey":          credentials.AccessKey,
					"prefix":             "snowflake-prefix",
					"enableSSE":          false,
					"useRudderStorage":   false,
				}

				t.Log("verifying test case 1")
				userID := tc.customUserID
				if userID == "" {
					userID = testhelper.GetUserId(destType)
				}
				ts1 := testhelper.TestConfig{
					WriteKey:              tc.writeKey,
					Schema:                tc.schema,
					Tables:                tc.tables,
					SourceID:              tc.sourceID,
					DestinationID:         tc.destinationID,
					StagingFilesEventsMap: tc.stagingFilesEventsMap,
					LoadFilesEventsMap:    tc.loadFilesEventsMap,
					TableUploadsEventsMap: tc.tableUploadsEventsMap,
					WarehouseEventsMap:    tc.warehouseEventsMap,
					Config:                conf,
					WorkspaceID:           workspaceID,
					DestinationType:       destType,
					JobsDB:                jobsDB,
					HTTPPort:              httpPort,
					Client:                sqlClient,
					JobRunID:              misc.FastUUID().String(),
					TaskRunID:             misc.FastUUID().String(),
					StagingFilePath:       tc.stagingFilePrefix + ".staging-1.json",
					UserID:                userID,
				}
				ts1.VerifyEvents(t)

				t.Log("verifying test case 2")
				userID = tc.customUserID
				if userID == "" {
					userID = testhelper.GetUserId(destType)
				}
				whEventsMap := tc.warehouseEventsMap2
				if whEventsMap == nil {
					whEventsMap = tc.warehouseEventsMap
				}
				ts2 := testhelper.TestConfig{
					WriteKey:              tc.writeKey,
					Schema:                tc.schema,
					Tables:                tc.tables,
					SourceID:              tc.sourceID,
					DestinationID:         tc.destinationID,
					StagingFilesEventsMap: tc.stagingFilesModifiedEventsMap,
					LoadFilesEventsMap:    tc.loadFilesEventsMap,
					TableUploadsEventsMap: tc.tableUploadsEventsMap,
					WarehouseEventsMap:    whEventsMap,
					AsyncJob:              tc.asyncJob,
					Config:                conf,
					WorkspaceID:           workspaceID,
					DestinationType:       destType,
					JobsDB:                jobsDB,
					HTTPPort:              httpPort,
					Client:                sqlClient,
					JobRunID:              misc.FastUUID().String(),
					TaskRunID:             misc.FastUUID().String(),
					StagingFilePath:       tc.stagingFilePrefix + ".staging-2.json",
					UserID:                userID,
				}
				if tc.asyncJob {
					ts2.UserID = ts1.UserID
				}
				ts2.VerifyEvents(t)
			})
		}
	})

	t.Run("Validation", func(t *testing.T) {
		dsn, err := sfdb.DSN(&sfdb.Config{
			Account:   credentials.Account,
			User:      credentials.User,
			Role:      credentials.Role,
			Password:  credentials.Password,
			Database:  credentials.Database,
			Warehouse: credentials.Warehouse,
		})
		require.NoError(t, err)

		db := getSnowflakeDB(t, dsn)

		t.Cleanup(func() {
			var err error
			require.Eventuallyf(t,
				func() bool {
					_, err = db.Exec(fmt.Sprintf(`DROP SCHEMA %q CASCADE;`, namespace))
					return err == nil
				},
				time.Minute, 100*time.Millisecond,
				"error deleting schema: %v", err,
			)
		})

		dest := backendconfig.DestinationT{
			ID: destinationID,
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
			RevisionID: destinationID,
		}
		testhelper.VerifyConfigurationTest(t, dest)
	})
}

func getSnowflakeDB(t testing.TB, dsn string) *sql.DB {
	t.Helper()
	db, err := sql.Open("snowflake", dsn)
	require.NoError(t, err)
	require.NoError(t, db.Ping())
	return db
}
