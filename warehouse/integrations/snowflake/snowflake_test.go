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

	"github.com/samber/lo"
	"golang.org/x/exp/slices"

	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"

	"github.com/golang/mock/gomock"
	sfdb "github.com/snowflakedb/gosnowflake"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/compose-test/compose"
	"github.com/rudderlabs/compose-test/testcompose"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/runner"
	"github.com/rudderlabs/rudder-server/testhelper/health"
	"github.com/rudderlabs/rudder-server/testhelper/workspaceConfig"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/snowflake"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/testhelper"
	mockuploader "github.com/rudderlabs/rudder-server/warehouse/internal/mocks/utils"
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
		return nil, fmt.Errorf("failed to unmarshal snowflake test credentials: %w", err)
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
		t.Setenv("RSERVER_WAREHOUSE_SNOWFLAKE_DEBUG_DUPLICATE_WORKSPACE_IDS", workspaceID)
		t.Setenv("RSERVER_WAREHOUSE_SNOWFLAKE_DEBUG_DUPLICATE_TABLES", strings.Join(
			[]string{
				"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups",
			},
			" ",
		))

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
			emptyJobRunID                 bool
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
				// an empty jobRunID means that the source is not an ETL one
				// see Uploader.CanAppend()
				emptyJobRunID: true,
				appendMode:    true,
				customUserID:  testhelper.GetUserId("append_test"),
			},
		}

		for _, tc := range testcase {
			tc := tc
			t.Run(tc.name, func(t *testing.T) {
				cancel := bootstrap(t, tc.appendMode)
				defer cancel()

				urlConfig := sfdb.Config{
					Account:   tc.cred.Account,
					User:      tc.cred.User,
					Role:      tc.cred.Role,
					Password:  tc.cred.Password,
					Database:  tc.database,
					Warehouse: tc.cred.Warehouse,
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
				jobRunID := ""
				if !tc.emptyJobRunID {
					jobRunID = misc.FastUUID().String()
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
					JobRunID:              jobRunID,
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
				jobRunID = ""
				if !tc.emptyJobRunID {
					jobRunID = misc.FastUUID().String()
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
					JobRunID:              jobRunID,
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

func TestSnowflake_ShouldAppend(t *testing.T) {
	testCases := []struct {
		name                  string
		loadTableStrategy     string
		uploaderCanAppend     bool
		uploaderExpectedCalls int
		expected              bool
	}{
		{
			name:                  "uploader says we can append and we are in append mode",
			loadTableStrategy:     "APPEND",
			uploaderCanAppend:     true,
			uploaderExpectedCalls: 1,
			expected:              true,
		},
		{
			name:                  "uploader says we cannot append and we are in append mode",
			loadTableStrategy:     "APPEND",
			uploaderCanAppend:     false,
			uploaderExpectedCalls: 1,
			expected:              false,
		},
		{
			name:                  "uploader says we can append and we are in merge mode",
			loadTableStrategy:     "MERGE",
			uploaderCanAppend:     true,
			uploaderExpectedCalls: 0,
			expected:              false,
		},
		{
			name:                  "uploader says we cannot append and we are in merge mode",
			loadTableStrategy:     "MERGE",
			uploaderCanAppend:     false,
			uploaderExpectedCalls: 0,
			expected:              false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			c := config.New()
			c.Set("Warehouse.snowflake.loadTableStrategy", tc.loadTableStrategy)

			sf, err := snowflake.New(c, logger.NOP, stats.Default)
			require.NoError(t, err)

			mockCtrl := gomock.NewController(t)
			uploader := mockuploader.NewMockUploader(mockCtrl)
			uploader.EXPECT().CanAppend().Times(tc.uploaderExpectedCalls).Return(tc.uploaderCanAppend)

			sf.Uploader = uploader
			require.Equal(t, sf.ShouldAppend(), tc.expected)
		})
	}
}

func TestSnowflake_LoadTable(t *testing.T) {
	if !isSnowflakeTestCredentialsAvailable() {
		t.Skipf("Skipping %s as %s is not set", t.Name(), testKey)
	}

	const (
		sourceID        = "test_source-id"
		destinationID   = "test_destination-id"
		workspaceID     = "test_workspace-id"
		destinationType = whutils.SNOWFLAKE
	)

	misc.Init()
	validations.Init()
	whutils.Init()

	ctx := context.Background()

	namespace := testhelper.RandSchema(destinationType)

	credentials, err := getSnowflakeTestCredentials(testKey)
	require.NoError(t, err)

	urlConfig := sfdb.Config{
		Account:   credentials.Account,
		User:      credentials.User,
		Role:      credentials.Role,
		Password:  credentials.Password,
		Database:  credentials.Database,
		Warehouse: credentials.Warehouse,
	}

	dsn, err := sfdb.DSN(&urlConfig)
	require.NoError(t, err)

	db := getSnowflakeDB(t, dsn)
	require.NoError(t, db.Ping())

	t.Cleanup(func() {
		require.Eventually(t, func() bool {
			if _, err := db.Exec(fmt.Sprintf(`DROP SCHEMA %q CASCADE;`, namespace)); err != nil {
				t.Logf("error deleting schema: %v", err)
				return false
			}
			return true
		},
			time.Minute,
			time.Second,
		)
	})

	warehouseModel := func(namespace string) model.Warehouse {
		return model.Warehouse{
			Source: backendconfig.SourceT{
				ID: sourceID,
			},
			Destination: backendconfig.DestinationT{
				ID: destinationID,
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Name: destinationType,
				},
				Config: map[string]any{
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
				},
			},
			WorkspaceID: workspaceID,
			Namespace:   namespace,
		}
	}

	schemaInUpload := model.TableSchema{
		"TEST_BOOL":     "boolean",
		"TEST_DATETIME": "datetime",
		"TEST_FLOAT":    "float",
		"TEST_INT":      "int",
		"TEST_STRING":   "string",
		"ID":            "string",
		"RECEIVED_AT":   "datetime",
	}
	schemaInWarehouse := model.TableSchema{
		"TEST_BOOL":           "boolean",
		"TEST_DATETIME":       "datetime",
		"TEST_FLOAT":          "float",
		"TEST_INT":            "int",
		"TEST_STRING":         "string",
		"ID":                  "string",
		"RECEIVED_AT":         "datetime",
		"EXTRA_TEST_BOOL":     "boolean",
		"EXTRA_TEST_DATETIME": "datetime",
		"EXTRA_TEST_FLOAT":    "float",
		"EXTRA_TEST_INT":      "int",
		"EXTRA_TEST_STRING":   "string",
	}

	fm, err := filemanager.New(&filemanager.Settings{
		Provider: whutils.S3,
		Config: map[string]any{
			"bucketName":     credentials.BucketName,
			"accessKeyID":    credentials.AccessKeyID,
			"accessKey":      credentials.AccessKey,
			"bucketProvider": whutils.S3,
		},
	})
	require.NoError(t, err)

	uploader := func(
		t testing.TB,
		loadFiles []whutils.LoadFile,
		tableName string,
		schemaInUpload model.TableSchema,
		schemaInWarehouse model.TableSchema,
		loadFileType string,
		canAppend bool,
		dedupUseNewRecord bool,
	) whutils.Uploader {
		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)

		mockUploader := mockuploader.NewMockUploader(ctrl)
		mockUploader.EXPECT().UseRudderStorage().Return(false).AnyTimes()
		mockUploader.EXPECT().CanAppend().Return(canAppend).AnyTimes()
		mockUploader.EXPECT().ShouldOnDedupUseNewRecord().Return(dedupUseNewRecord).AnyTimes()
		mockUploader.EXPECT().GetLoadFilesMetadata(gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, options whutils.GetLoadFilesOptions) []whutils.LoadFile {
				return slices.Clone(loadFiles)
			},
		).AnyTimes()
		mockUploader.EXPECT().GetSampleLoadFileLocation(gomock.Any(), gomock.Any()).Return(loadFiles[0].Location, nil).AnyTimes()
		mockUploader.EXPECT().GetTableSchemaInUpload(tableName).Return(schemaInUpload).AnyTimes()
		mockUploader.EXPECT().GetTableSchemaInWarehouse(tableName).Return(schemaInWarehouse).AnyTimes()
		mockUploader.EXPECT().GetLoadFileType().Return(loadFileType).AnyTimes()

		return mockUploader
	}

	t.Run("schema does not exists", func(t *testing.T) {
		tableName := whutils.ToProviderCase(whutils.SNOWFLAKE, "schema_not_exists_test_table")

		uploadOutput := testhelper.Upload(t, fm, "../testdata/load.csv.gz", tableName)

		loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
		mockUploader := uploader(
			t, loadFiles, tableName, schemaInUpload,
			schemaInWarehouse, whutils.LoadFileTypeCsv, false, false,
		)

		warehouse := warehouseModel(namespace)

		sf, err := snowflake.New(config.Default, logger.NOP, stats.Default)
		require.NoError(t, err)
		err = sf.Setup(ctx, warehouse, mockUploader)
		require.NoError(t, err)

		loadTableStat, err := sf.LoadTable(ctx, tableName)
		require.ErrorContains(t, err, "The requested schema does not exist or not authorized.")
		require.Nil(t, loadTableStat)
	})
	t.Run("table does not exists", func(t *testing.T) {
		tableName := whutils.ToProviderCase(whutils.SNOWFLAKE, "table_not_exists_test_table")

		uploadOutput := testhelper.Upload(t, fm, "../testdata/load.csv.gz", tableName)

		loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
		mockUploader := uploader(
			t, loadFiles, tableName, schemaInUpload,
			schemaInWarehouse, whutils.LoadFileTypeCsv, false, false,
		)

		warehouse := warehouseModel(namespace)

		sf, err := snowflake.New(config.Default, logger.NOP, stats.Default)
		require.NoError(t, err)
		err = sf.Setup(ctx, warehouse, mockUploader)
		require.NoError(t, err)

		err = sf.CreateSchema(ctx)
		require.NoError(t, err)

		loadTableStat, err := sf.LoadTable(ctx, tableName)
		require.ErrorContains(t, err, "Object '"+credentials.Database+"."+namespace+"."+"TABLE_NOT_EXISTS_TEST_TABLE' does not exist or not authorized.")
		require.Nil(t, loadTableStat)
	})
	t.Run("load table stats", func(t *testing.T) {
		tableName := whutils.ToProviderCase(whutils.SNOWFLAKE, "load_table_stats_test_table")

		uploadOutput := testhelper.Upload(t, fm, "../testdata/load.csv.gz", tableName)

		loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
		mockUploader := uploader(
			t, loadFiles, tableName, schemaInUpload,
			schemaInWarehouse, whutils.LoadFileTypeCsv, false, false,
		)

		warehouse := warehouseModel(namespace)

		c := config.New()
		c.Set("Warehouse.snowflake.debugDuplicateWorkspaceIDs", []string{workspaceID})
		c.Set("Warehouse.snowflake.debugDuplicateIntervalInDays", 1000)
		c.Set("Warehouse.snowflake.debugDuplicateTables", []string{whutils.ToProviderCase(
			whutils.SNOWFLAKE,
			tableName,
		)})

		sf, err := snowflake.New(c, logger.NOP, stats.Default)
		require.NoError(t, err)
		err = sf.Setup(ctx, warehouse, mockUploader)
		require.NoError(t, err)

		err = sf.CreateSchema(ctx)
		require.NoError(t, err)

		err = sf.CreateTable(ctx, tableName, schemaInWarehouse)
		require.NoError(t, err)

		loadTableStat, err := sf.LoadTable(ctx, tableName)
		require.NoError(t, err)
		require.Equal(t, loadTableStat.RowsInserted, int64(14))
		require.Equal(t, loadTableStat.RowsUpdated, int64(0))

		loadTableStat, err = sf.LoadTable(ctx, tableName)
		require.NoError(t, err)
		require.Equal(t, loadTableStat.RowsInserted, int64(0))
		require.Equal(t, loadTableStat.RowsUpdated, int64(14))

		records := testhelper.RecordsFromWarehouse(t, sf.DB.DB,
			fmt.Sprintf(`
				SELECT
				  id,
				  received_at,
				  test_bool,
				  test_datetime,
				  test_float,
				  test_int,
				  test_string
				FROM
				  %q.%q
				ORDER BY
				  id`,
				namespace,
				tableName,
			),
		)

		require.Equal(t, records, [][]string{
			{"6734e5db-f918-4efe-1421-872f66e235c5", "2022-12-15 06:53:49.64 +0000 +0000", "", "", "", "125", ""},
			{"6734e5db-f918-4efe-2314-872f66e235c5", "2022-12-15 06:53:49.64 +0000 +0000", "", "", "125.75", "", ""},
			{"6734e5db-f918-4efe-2352-872f66e235c5", "2022-12-15 06:53:49.64 +0000 +0000", "", "2022-12-15 06:53:49.64 +0000 +0000", "", "", ""},
			{"6734e5db-f918-4efe-2414-872f66e235c5", "2022-12-15 06:53:49.64 +0000 +0000", "false", "2022-12-15 06:53:49.64 +0000 +0000", "126.75", "126", "hello-world"},
			{"6734e5db-f918-4efe-3555-872f66e235c5", "2022-12-15 06:53:49.64 +0000 +0000", "false", "", "", "", ""},
			{"6734e5db-f918-4efe-5152-872f66e235c5", "2022-12-15 06:53:49.64 +0000 +0000", "", "", "", "", "hello-world"},
			{"6734e5db-f918-4efe-5323-872f66e235c5", "2022-12-15 06:53:49.64 +0000 +0000", "", "", "", "", ""},
			{"7274e5db-f918-4efe-1212-872f66e235c5", "2022-12-15 06:53:49.64 +0000 +0000", "true", "2022-12-15 06:53:49.64 +0000 +0000", "125.75", "125", "hello-world"},
			{"7274e5db-f918-4efe-1454-872f66e235c5", "2022-12-15 06:53:49.64 +0000 +0000", "", "", "", "125", ""},
			{"7274e5db-f918-4efe-1511-872f66e235c5", "2022-12-15 06:53:49.64 +0000 +0000", "", "", "", "", ""},
			{"7274e5db-f918-4efe-2323-872f66e235c5", "2022-12-15 06:53:49.64 +0000 +0000", "", "", "125.75", "", ""},
			{"7274e5db-f918-4efe-4524-872f66e235c5", "2022-12-15 06:53:49.64 +0000 +0000", "true", "", "", "", ""},
			{"7274e5db-f918-4efe-5151-872f66e235c5", "2022-12-15 06:53:49.64 +0000 +0000", "", "", "", "", "hello-world"},
			{"7274e5db-f918-4efe-5322-872f66e235c5", "2022-12-15 06:53:49.64 +0000 +0000", "", "2022-12-15 06:53:49.64 +0000 +0000", "", "", ""},
		})
	})
	t.Run("load file does not exists", func(t *testing.T) {
		tableName := whutils.ToProviderCase(whutils.SNOWFLAKE, "load_file_not_exists_test_table")

		loadFiles := []whutils.LoadFile{{
			Location: "https://bucket.s3.amazonaws.com/rudder-warehouse-load-objects/load_table_stats_test_table/test_source-id/0ef75cb0-3fd0-4408-98b9-2bea9e476916-load_table_stats_test_table/load.csv.gz",
		}}
		mockUploader := uploader(
			t, loadFiles, tableName, schemaInUpload,
			schemaInWarehouse, whutils.LoadFileTypeCsv, false, false,
		)

		warehouse := warehouseModel(namespace)

		sf, err := snowflake.New(config.Default, logger.NOP, stats.Default)
		require.NoError(t, err)
		err = sf.Setup(ctx, warehouse, mockUploader)
		require.NoError(t, err)

		err = sf.CreateSchema(ctx)
		require.NoError(t, err)

		err = sf.CreateTable(ctx, tableName, schemaInWarehouse)
		require.NoError(t, err)

		loadTableStat, err := sf.LoadTable(ctx, tableName)
		require.ErrorContains(t, err, "Failure using stage area. Cause: [Access Denied (Status Code: 403; Error Code: AccessDenied)]")
		require.Nil(t, loadTableStat)
	})
	t.Run("mismatch in number of columns", func(t *testing.T) {
		tableName := whutils.ToProviderCase(whutils.SNOWFLAKE, "mismatch_columns_test_table")

		uploadOutput := testhelper.Upload(t, fm, "../testdata/mismatch-columns.csv.gz", tableName)

		loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
		mockUploader := uploader(
			t, loadFiles, tableName, schemaInUpload,
			schemaInWarehouse, whutils.LoadFileTypeCsv, false, false,
		)

		warehouse := warehouseModel(namespace)

		sf, err := snowflake.New(config.Default, logger.NOP, stats.Default)
		require.NoError(t, err)
		err = sf.Setup(ctx, warehouse, mockUploader)
		require.NoError(t, err)

		err = sf.CreateSchema(ctx)
		require.NoError(t, err)

		err = sf.CreateTable(ctx, tableName, schemaInWarehouse)
		require.NoError(t, err)

		loadTableStat, err := sf.LoadTable(ctx, tableName)
		require.NoError(t, err)
		require.Equal(t, loadTableStat.RowsInserted, int64(14))
		require.Equal(t, loadTableStat.RowsUpdated, int64(0))

		records := testhelper.RecordsFromWarehouse(t, sf.DB.DB,
			fmt.Sprintf(`
				SELECT
				  id,
				  received_at,
				  test_bool,
				  test_datetime,
				  test_float,
				  test_int,
				  test_string
				FROM
				  %q.%q
				ORDER BY
				  id`,
				namespace,
				tableName,
			),
		)

		require.Equal(t, records, [][]string{
			{"6734e5db-f918-4efe-1421-872f66e235c5", "2022-12-15 06:53:49.64 +0000 +0000", "", "", "", "125", ""},
			{"6734e5db-f918-4efe-2314-872f66e235c5", "2022-12-15 06:53:49.64 +0000 +0000", "", "", "125.75", "", ""},
			{"6734e5db-f918-4efe-2352-872f66e235c5", "2022-12-15 06:53:49.64 +0000 +0000", "", "2022-12-15 06:53:49.64 +0000 +0000", "", "", ""},
			{"6734e5db-f918-4efe-2414-872f66e235c5", "2022-12-15 06:53:49.64 +0000 +0000", "false", "2022-12-15 06:53:49.64 +0000 +0000", "126.75", "126", "hello-world"},
			{"6734e5db-f918-4efe-3555-872f66e235c5", "2022-12-15 06:53:49.64 +0000 +0000", "false", "", "", "", ""},
			{"6734e5db-f918-4efe-5152-872f66e235c5", "2022-12-15 06:53:49.64 +0000 +0000", "", "", "", "", "hello-world"},
			{"6734e5db-f918-4efe-5323-872f66e235c5", "2022-12-15 06:53:49.64 +0000 +0000", "", "", "", "", ""},
			{"7274e5db-f918-4efe-1212-872f66e235c5", "2022-12-15 06:53:49.64 +0000 +0000", "true", "2022-12-15 06:53:49.64 +0000 +0000", "125.75", "125", "hello-world"},
			{"7274e5db-f918-4efe-1454-872f66e235c5", "2022-12-15 06:53:49.64 +0000 +0000", "", "", "", "125", ""},
			{"7274e5db-f918-4efe-1511-872f66e235c5", "2022-12-15 06:53:49.64 +0000 +0000", "", "", "", "", ""},
			{"7274e5db-f918-4efe-2323-872f66e235c5", "2022-12-15 06:53:49.64 +0000 +0000", "", "", "125.75", "", ""},
			{"7274e5db-f918-4efe-4524-872f66e235c5", "2022-12-15 06:53:49.64 +0000 +0000", "true", "", "", "", ""},
			{"7274e5db-f918-4efe-5151-872f66e235c5", "2022-12-15 06:53:49.64 +0000 +0000", "", "", "", "", "hello-world"},
			{"7274e5db-f918-4efe-5322-872f66e235c5", "2022-12-15 06:53:49.64 +0000 +0000", "", "2022-12-15 06:53:49.64 +0000 +0000", "", "", ""},
		})
	})
	t.Run("mismatch in schema", func(t *testing.T) {
		tableName := whutils.ToProviderCase(whutils.SNOWFLAKE, "mismatch_schema_test_table")

		uploadOutput := testhelper.Upload(t, fm, "../testdata/mismatch-schema.csv.gz", tableName)

		loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
		mockUploader := uploader(
			t, loadFiles, tableName, schemaInUpload,
			schemaInWarehouse, whutils.LoadFileTypeCsv, false, false,
		)

		warehouse := warehouseModel(namespace)

		sf, err := snowflake.New(config.Default, logger.NOP, stats.Default)
		require.NoError(t, err)
		err = sf.Setup(ctx, warehouse, mockUploader)
		require.NoError(t, err)

		err = sf.CreateSchema(ctx)
		require.NoError(t, err)

		err = sf.CreateTable(ctx, tableName, schemaInWarehouse)
		require.NoError(t, err)

		loadTableStat, err := sf.LoadTable(ctx, tableName)
		require.ErrorContains(t, err, "Numeric value '2022-12-15T06:53:49.640Z' is not recognized")
		require.Nil(t, loadTableStat)
	})
	t.Run("discards", func(t *testing.T) {
		tableName := whutils.ToProviderCase(whutils.SNOWFLAKE, whutils.DiscardsTable)

		uploadOutput := testhelper.Upload(t, fm, "../testdata/discards.csv.gz", tableName)

		discardsSchema := lo.MapKeys(whutils.DiscardsSchema, func(_, key string) string {
			return whutils.ToProviderCase(whutils.SNOWFLAKE, key)
		})

		loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
		mockUploader := uploader(
			t, loadFiles, tableName, discardsSchema,
			discardsSchema, whutils.LoadFileTypeCsv, false, false,
		)

		warehouse := warehouseModel(namespace)

		sf, err := snowflake.New(config.Default, logger.NOP, stats.Default)
		require.NoError(t, err)
		err = sf.Setup(ctx, warehouse, mockUploader)
		require.NoError(t, err)

		err = sf.CreateSchema(ctx)
		require.NoError(t, err)

		err = sf.CreateTable(ctx, tableName, discardsSchema)
		require.NoError(t, err)

		loadTableStat, err := sf.LoadTable(ctx, tableName)
		require.NoError(t, err)
		require.Equal(t, loadTableStat.RowsInserted, int64(6))
		require.Equal(t, loadTableStat.RowsUpdated, int64(0))

		records := testhelper.RecordsFromWarehouse(t, sf.DB.DB,
			fmt.Sprintf(`
				SELECT
				  COLUMN_NAME,
				  COLUMN_VALUE,
				  RECEIVED_AT,
				  ROW_ID,
				  TABLE_NAME,
				  UUID_TS
				FROM
				  %q.%q
				ORDER BY ROW_ID ASC;`,
				namespace,
				tableName,
			),
		)
		require.Equal(t, records, [][]string{
			{"context_screen_density", "125.75", "2022-12-15 06:53:49.64 +0000 +0000", "1", "test_table", "2022-12-15 06:53:49.64 +0000 +0000"},
			{"context_screen_density", "125", "2022-12-15 06:53:49.64 +0000 +0000", "2", "test_table", "2022-12-15 06:53:49.64 +0000 +0000"},
			{"context_screen_density", "true", "2022-12-15 06:53:49.64 +0000 +0000", "3", "test_table", "2022-12-15 06:53:49.64 +0000 +0000"},
			{"context_screen_density", "7274e5db-f918-4efe-1212-872f66e235c5", "2022-12-15 06:53:49.64 +0000 +0000", "4", "test_table", "2022-12-15 06:53:49.64 +0000 +0000"},
			{"context_screen_density", "hello-world", "2022-12-15 06:53:49.64 +0000 +0000", "5", "test_table", "2022-12-15 06:53:49.64 +0000 +0000"},
			{"context_screen_density", "2022-12-15T06:53:49.640Z", "2022-12-15 06:53:49.64 +0000 +0000", "6", "test_table", "2022-12-15 06:53:49.64 +0000 +0000"},
		})
	})
	t.Run("dedup", func(t *testing.T) {
		tableName := whutils.ToProviderCase(whutils.SNOWFLAKE, "dedup_test_table")

		run := func(
			fileName string,
			insertedRows int64,
			updatedRows int64,
		) {
			uploadOutput := testhelper.Upload(t, fm, fileName, tableName)

			loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := uploader(
				t, loadFiles, tableName, schemaInUpload,
				schemaInWarehouse, whutils.LoadFileTypeCsv, false, true,
			)

			warehouse := warehouseModel(namespace)

			sf, err := snowflake.New(config.Default, logger.NOP, stats.Default)
			require.NoError(t, err)
			err = sf.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = sf.CreateSchema(ctx)
			require.NoError(t, err)

			err = sf.CreateTable(ctx, tableName, schemaInWarehouse)
			require.NoError(t, err)

			loadTableStat, err := sf.LoadTable(ctx, tableName)
			require.NoError(t, err)
			require.Equal(t, loadTableStat.RowsInserted, insertedRows)
			require.Equal(t, loadTableStat.RowsUpdated, updatedRows)
		}

		run("../testdata/load.csv.gz", 14, 0)
		run("../testdata/dedup.csv.gz", 0, 14)

		records := testhelper.RecordsFromWarehouse(t, db,
			fmt.Sprintf(`
				SELECT
				  id,
				  received_at,
				  test_bool,
				  test_datetime,
				  test_float,
				  test_int,
				  test_string
				FROM
				  %q.%q
				ORDER BY
				  id`,
				namespace,
				tableName,
			),
		)

		require.Equal(t, records, [][]string{
			{"6734e5db-f918-4efe-1421-872f66e235c5", "2022-12-15 06:53:49.64 +0000 +0000", "", "", "", "521", ""},
			{"6734e5db-f918-4efe-2314-872f66e235c5", "2022-12-15 06:53:49.64 +0000 +0000", "", "", "75.125", "", ""},
			{"6734e5db-f918-4efe-2352-872f66e235c5", "2022-12-15 06:53:49.64 +0000 +0000", "", "2022-12-15 06:53:49.64 +0000 +0000", "", "", ""},
			{"6734e5db-f918-4efe-2414-872f66e235c5", "2022-12-15 06:53:49.64 +0000 +0000", "true", "2022-12-15 06:53:49.64 +0000 +0000", "75.125", "521", "world-hello"},
			{"6734e5db-f918-4efe-3555-872f66e235c5", "2022-12-15 06:53:49.64 +0000 +0000", "true", "", "", "", ""},
			{"6734e5db-f918-4efe-5152-872f66e235c5", "2022-12-15 06:53:49.64 +0000 +0000", "", "", "", "", "world-hello"},
			{"6734e5db-f918-4efe-5323-872f66e235c5", "2022-12-15 06:53:49.64 +0000 +0000", "", "", "", "", ""},
			{"7274e5db-f918-4efe-1212-872f66e235c5", "2022-12-15 06:53:49.64 +0000 +0000", "false", "2022-12-15 06:53:49.64 +0000 +0000", "75.125", "521", "world-hello"},
			{"7274e5db-f918-4efe-1454-872f66e235c5", "2022-12-15 06:53:49.64 +0000 +0000", "", "", "", "521", ""},
			{"7274e5db-f918-4efe-1511-872f66e235c5", "2022-12-15 06:53:49.64 +0000 +0000", "", "", "", "", ""},
			{"7274e5db-f918-4efe-2323-872f66e235c5", "2022-12-15 06:53:49.64 +0000 +0000", "", "", "75.125", "", ""},
			{"7274e5db-f918-4efe-4524-872f66e235c5", "2022-12-15 06:53:49.64 +0000 +0000", "false", "", "", "", ""},
			{"7274e5db-f918-4efe-5151-872f66e235c5", "2022-12-15 06:53:49.64 +0000 +0000", "", "", "", "", "world-hello"},
			{"7274e5db-f918-4efe-5322-872f66e235c5", "2022-12-15 06:53:49.64 +0000 +0000", "", "2022-12-15 06:53:49.64 +0000 +0000", "", "", ""},
		})
	})
	t.Run("append", func(t *testing.T) {
		tableName := whutils.ToProviderCase(whutils.SNOWFLAKE, "append_test_table")

		run := func() {
			uploadOutput := testhelper.Upload(t, fm, "../testdata/load.csv.gz", tableName)

			loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := uploader(
				t, loadFiles, tableName, schemaInUpload,
				schemaInWarehouse, whutils.LoadFileTypeCsv, true, false,
			)

			warehouse := warehouseModel(namespace)

			c := config.New()
			c.Set("Warehouse.snowflake.loadTableStrategy", "APPEND")

			sf, err := snowflake.New(c, logger.NOP, stats.Default)
			require.NoError(t, err)
			err = sf.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = sf.CreateSchema(ctx)
			require.NoError(t, err)

			err = sf.CreateTable(ctx, tableName, schemaInWarehouse)
			require.NoError(t, err)

			loadTableStat, err := sf.LoadTable(ctx, tableName)
			require.NoError(t, err)
			require.Equal(t, loadTableStat.RowsInserted, int64(14))
			require.Equal(t, loadTableStat.RowsUpdated, int64(0))
		}

		run()
		run()

		records := testhelper.RecordsFromWarehouse(t, db,
			fmt.Sprintf(`
				SELECT
				  id,
				  received_at,
				  test_bool,
				  test_datetime,
				  test_float,
				  test_int,
				  test_string
				FROM
				  %q.%q
				ORDER BY
				  id`,
				namespace,
				tableName,
			),
		)

		require.Equal(t, records, [][]string{
			{"6734e5db-f918-4efe-1421-872f66e235c5", "2022-12-15 06:53:49.64 +0000 +0000", "", "", "", "125", ""},
			{"6734e5db-f918-4efe-1421-872f66e235c5", "2022-12-15 06:53:49.64 +0000 +0000", "", "", "", "125", ""},
			{"6734e5db-f918-4efe-2314-872f66e235c5", "2022-12-15 06:53:49.64 +0000 +0000", "", "", "125.75", "", ""},
			{"6734e5db-f918-4efe-2314-872f66e235c5", "2022-12-15 06:53:49.64 +0000 +0000", "", "", "125.75", "", ""},
			{"6734e5db-f918-4efe-2352-872f66e235c5", "2022-12-15 06:53:49.64 +0000 +0000", "", "2022-12-15 06:53:49.64 +0000 +0000", "", "", ""},
			{"6734e5db-f918-4efe-2352-872f66e235c5", "2022-12-15 06:53:49.64 +0000 +0000", "", "2022-12-15 06:53:49.64 +0000 +0000", "", "", ""},
			{"6734e5db-f918-4efe-2414-872f66e235c5", "2022-12-15 06:53:49.64 +0000 +0000", "false", "2022-12-15 06:53:49.64 +0000 +0000", "126.75", "126", "hello-world"},
			{"6734e5db-f918-4efe-2414-872f66e235c5", "2022-12-15 06:53:49.64 +0000 +0000", "false", "2022-12-15 06:53:49.64 +0000 +0000", "126.75", "126", "hello-world"},
			{"6734e5db-f918-4efe-3555-872f66e235c5", "2022-12-15 06:53:49.64 +0000 +0000", "false", "", "", "", ""},
			{"6734e5db-f918-4efe-3555-872f66e235c5", "2022-12-15 06:53:49.64 +0000 +0000", "false", "", "", "", ""},
			{"6734e5db-f918-4efe-5152-872f66e235c5", "2022-12-15 06:53:49.64 +0000 +0000", "", "", "", "", "hello-world"},
			{"6734e5db-f918-4efe-5152-872f66e235c5", "2022-12-15 06:53:49.64 +0000 +0000", "", "", "", "", "hello-world"},
			{"6734e5db-f918-4efe-5323-872f66e235c5", "2022-12-15 06:53:49.64 +0000 +0000", "", "", "", "", ""},
			{"6734e5db-f918-4efe-5323-872f66e235c5", "2022-12-15 06:53:49.64 +0000 +0000", "", "", "", "", ""},
			{"7274e5db-f918-4efe-1212-872f66e235c5", "2022-12-15 06:53:49.64 +0000 +0000", "true", "2022-12-15 06:53:49.64 +0000 +0000", "125.75", "125", "hello-world"},
			{"7274e5db-f918-4efe-1212-872f66e235c5", "2022-12-15 06:53:49.64 +0000 +0000", "true", "2022-12-15 06:53:49.64 +0000 +0000", "125.75", "125", "hello-world"},
			{"7274e5db-f918-4efe-1454-872f66e235c5", "2022-12-15 06:53:49.64 +0000 +0000", "", "", "", "125", ""},
			{"7274e5db-f918-4efe-1454-872f66e235c5", "2022-12-15 06:53:49.64 +0000 +0000", "", "", "", "125", ""},
			{"7274e5db-f918-4efe-1511-872f66e235c5", "2022-12-15 06:53:49.64 +0000 +0000", "", "", "", "", ""},
			{"7274e5db-f918-4efe-1511-872f66e235c5", "2022-12-15 06:53:49.64 +0000 +0000", "", "", "", "", ""},
			{"7274e5db-f918-4efe-2323-872f66e235c5", "2022-12-15 06:53:49.64 +0000 +0000", "", "", "125.75", "", ""},
			{"7274e5db-f918-4efe-2323-872f66e235c5", "2022-12-15 06:53:49.64 +0000 +0000", "", "", "125.75", "", ""},
			{"7274e5db-f918-4efe-4524-872f66e235c5", "2022-12-15 06:53:49.64 +0000 +0000", "true", "", "", "", ""},
			{"7274e5db-f918-4efe-4524-872f66e235c5", "2022-12-15 06:53:49.64 +0000 +0000", "true", "", "", "", ""},
			{"7274e5db-f918-4efe-5151-872f66e235c5", "2022-12-15 06:53:49.64 +0000 +0000", "", "", "", "", "hello-world"},
			{"7274e5db-f918-4efe-5151-872f66e235c5", "2022-12-15 06:53:49.64 +0000 +0000", "", "", "", "", "hello-world"},
			{"7274e5db-f918-4efe-5322-872f66e235c5", "2022-12-15 06:53:49.64 +0000 +0000", "", "2022-12-15 06:53:49.64 +0000 +0000", "", "", ""},
			{"7274e5db-f918-4efe-5322-872f66e235c5", "2022-12-15 06:53:49.64 +0000 +0000", "", "2022-12-15 06:53:49.64 +0000 +0000", "", "", ""},
		})
	})
}

func getSnowflakeDB(t testing.TB, dsn string) *sql.DB {
	t.Helper()
	db, err := sql.Open("snowflake", dsn)
	require.NoError(t, err)
	require.NoError(t, db.Ping())
	return db
}
