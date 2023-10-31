package snowflake_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/samber/lo"
	sfdb "github.com/snowflakedb/gosnowflake"
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
	"github.com/rudderlabs/rudder-server/warehouse/integrations/snowflake"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/testhelper"
	mockuploader "github.com/rudderlabs/rudder-server/warehouse/internal/mocks/utils"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
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

	bootstrapSvc := func(t testing.TB, enableMerge bool) {
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
			"enableMerge":                enableMerge,
		}
		workspaceConfigPath := workspaceConfig.CreateTempFile(t, "testdata/template.json", templateConfigurations)

		testhelper.EnhanceWithDefaultEnvs(t)
		t.Setenv("JOBS_DB_PORT", strconv.Itoa(jobsDBPort))
		t.Setenv("WAREHOUSE_JOBS_DB_PORT", strconv.Itoa(jobsDBPort))
		t.Setenv("RSERVER_WAREHOUSE_SNOWFLAKE_MAX_PARALLEL_LOADS", "8")
		t.Setenv("RSERVER_WAREHOUSE_SNOWFLAKE_ENABLE_DELETE_BY_JOBS", "true")
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
		t.Cleanup(cancel)

		serviceHealthEndpoint := fmt.Sprintf("http://localhost:%d/health", httpPort)
		health.WaitUntilReady(ctx, t, serviceHealthEndpoint, time.Minute, 100*time.Millisecond, "serviceHealthEndpoint")
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
			enableMerge                   bool
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
				enableMerge:       true,
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
				enableMerge:       true,
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
				enableMerge:       true,
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
				enableMerge:           true,
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
				enableMerge:   false,
				customUserID:  testhelper.GetUserId("append_test"),
			},
		}

		for _, tc := range testcase {
			tc := tc
			t.Run(tc.name, func(t *testing.T) {
				bootstrapSvc(t, tc.enableMerge)

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
				"enableMerge":        true,
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

	t.Run("Load Table", func(t *testing.T) {
		const (
			sourceID      = "test_source_id"
			destinationID = "test_destination_id"
			workspaceID   = "test_workspace_id"
		)

		namespace := testhelper.RandSchema(destType)

		ctx := context.Background()

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

		warehouse := model.Warehouse{
			Source: backendconfig.SourceT{
				ID: sourceID,
			},
			Destination: backendconfig.DestinationT{
				ID: destinationID,
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Name: destType,
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

		t.Run("schema does not exists", func(t *testing.T) {
			tableName := whutils.ToProviderCase(whutils.SNOWFLAKE, "schema_not_exists_test_table")

			uploadOutput := testhelper.UploadLoadFile(t, fm, "../testdata/load.csv.gz", tableName)

			loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, false, false)

			sf, err := snowflake.New(config.New(), logger.NOP, memstats.New())
			require.NoError(t, err)
			err = sf.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			loadTableStat, err := sf.LoadTable(ctx, tableName)
			require.Error(t, err)
			require.Nil(t, loadTableStat)
		})
		t.Run("table does not exists", func(t *testing.T) {
			tableName := whutils.ToProviderCase(whutils.SNOWFLAKE, "table_not_exists_test_table")

			uploadOutput := testhelper.UploadLoadFile(t, fm, "../testdata/load.csv.gz", tableName)

			loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, false, false)

			sf, err := snowflake.New(config.New(), logger.NOP, memstats.New())
			require.NoError(t, err)
			err = sf.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = sf.CreateSchema(ctx)
			require.NoError(t, err)

			loadTableStat, err := sf.LoadTable(ctx, tableName)
			require.Error(t, err)
			require.Nil(t, loadTableStat)
		})
		t.Run("merge", func(t *testing.T) {
			tableName := whutils.ToProviderCase(whutils.SNOWFLAKE, "merge_test_table")

			t.Run("without dedup", func(t *testing.T) {
				uploadOutput := testhelper.UploadLoadFile(t, fm, "../testdata/load.csv.gz", tableName)

				loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
				mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, true, false)

				sf, err := snowflake.New(config.New(), logger.NOP, memstats.New())
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
				require.Equal(t, loadTableStat.RowsInserted, int64(0),
					"2nd copy on the same table with the same data should not have any 'rows_loaded'")
				require.Equal(t, loadTableStat.RowsUpdated, int64(0),
					"2nd copy on the same table with the same data should not have any 'rows_loaded'")

				records := testhelper.RetrieveRecordsFromWarehouse(t, sf.DB.DB,
					fmt.Sprintf(
						`SELECT
						  id,
						  received_at,
						  test_bool,
						  test_datetime,
						  test_float,
						  test_int,
						  test_string
						FROM %q.%q
						ORDER BY id;`,
						namespace,
						tableName,
					),
				)
				require.Equal(t, testhelper.SampleTestRecords(), records)
			})
			t.Run("with dedup use new record", func(t *testing.T) {
				uploadOutput := testhelper.UploadLoadFile(t, fm, "../testdata/dedup.csv.gz", tableName)

				loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
				mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, false, true)

				sf, err := snowflake.New(config.New(), logger.NOP, memstats.New())
				require.NoError(t, err)
				err = sf.Setup(ctx, warehouse, mockUploader)
				require.NoError(t, err)

				err = sf.CreateSchema(ctx)
				require.NoError(t, err)

				err = sf.CreateTable(ctx, tableName, schemaInWarehouse)
				require.NoError(t, err)

				loadTableStat, err := sf.LoadTable(ctx, tableName)
				require.NoError(t, err)
				require.Equal(t, loadTableStat.RowsInserted, int64(0))
				require.Equal(t, loadTableStat.RowsUpdated, int64(14))

				records := testhelper.RetrieveRecordsFromWarehouse(t, db,
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
						  id;
					`,
						namespace,
						tableName,
					),
				)
				require.Equal(t, records, testhelper.DedupTestRecords())
			})
		})
		t.Run("append", func(t *testing.T) {
			tableName := whutils.ToProviderCase(whutils.SNOWFLAKE, "append_test_table")

			run := func() {
				uploadOutput := testhelper.UploadLoadFile(t, fm, "../testdata/load.csv.gz", tableName)

				loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
				mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, true, false)

				sf, err := snowflake.New(config.New(), logger.NOP, memstats.New())
				require.NoError(t, err)
				err = sf.Setup(ctx, warehouse, mockUploader)
				require.NoError(t, err)

				err = sf.CreateSchema(ctx)
				require.NoError(t, err)

				err = sf.CreateTable(ctx, tableName, schemaInWarehouse)
				require.NoError(t, err)

				t.Run("loading once should copy everything", func(t *testing.T) {
					loadTableStat, err := sf.LoadTable(ctx, tableName)
					require.NoError(t, err)
					require.Equal(t, loadTableStat.RowsInserted, int64(14))
					require.Equal(t, loadTableStat.RowsUpdated, int64(0))
				})
				t.Run("loading twice should not copy anything", func(t *testing.T) {
					loadTableStat, err := sf.LoadTable(ctx, tableName)
					require.NoError(t, err)
					require.Equal(t, loadTableStat.RowsInserted, int64(0))
					require.Equal(t, loadTableStat.RowsUpdated, int64(0))
				})
			}

			run()
			run()

			records := testhelper.RetrieveRecordsFromWarehouse(t, db,
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
				  id;
				`,
					namespace,
					tableName,
				),
			)
			require.Equal(t, records, testhelper.AppendTestRecords())
		})
		t.Run("load file does not exists", func(t *testing.T) {
			tableName := whutils.ToProviderCase(whutils.SNOWFLAKE, "load_file_not_exists_test_table")

			loadFiles := []whutils.LoadFile{{
				Location: "https://bucket.s3.amazonaws.com/rudder-warehouse-load-objects/load_file_not_exists_test_table/test_source_id/0ef75cb0-3fd0-4408-98b9-2bea9e476916-load_file_not_exists_test_table/load.csv.gz",
			}}
			mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, false, false)

			sf, err := snowflake.New(config.New(), logger.NOP, memstats.New())
			require.NoError(t, err)
			err = sf.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = sf.CreateSchema(ctx)
			require.NoError(t, err)

			err = sf.CreateTable(ctx, tableName, schemaInWarehouse)
			require.NoError(t, err)

			loadTableStat, err := sf.LoadTable(ctx, tableName)
			require.Error(t, err)
			require.Nil(t, loadTableStat)
		})
		t.Run("mismatch in number of columns", func(t *testing.T) {
			tableName := whutils.ToProviderCase(whutils.SNOWFLAKE, "mismatch_columns_test_table")

			uploadOutput := testhelper.UploadLoadFile(t, fm, "../testdata/mismatch-columns.csv.gz", tableName)

			loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, false, false)

			sf, err := snowflake.New(config.New(), logger.NOP, memstats.New())
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

			records := testhelper.RetrieveRecordsFromWarehouse(t, sf.DB.DB,
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
				  id;
				`,
					namespace,
					tableName,
				),
			)
			require.Equal(t, records, testhelper.SampleTestRecords())
		})
		t.Run("mismatch in schema", func(t *testing.T) {
			tableName := whutils.ToProviderCase(whutils.SNOWFLAKE, "mismatch_schema_test_table")

			uploadOutput := testhelper.UploadLoadFile(t, fm, "../testdata/mismatch-schema.csv.gz", tableName)

			loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, false, false)

			sf, err := snowflake.New(config.New(), logger.NOP, memstats.New())
			require.NoError(t, err)
			err = sf.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = sf.CreateSchema(ctx)
			require.NoError(t, err)

			err = sf.CreateTable(ctx, tableName, schemaInWarehouse)
			require.NoError(t, err)

			loadTableStat, err := sf.LoadTable(ctx, tableName)
			require.Error(t, err)
			require.Nil(t, loadTableStat)
		})
		t.Run("discards", func(t *testing.T) {
			tableName := whutils.ToProviderCase(whutils.SNOWFLAKE, whutils.DiscardsTable)

			uploadOutput := testhelper.UploadLoadFile(t, fm, "../testdata/discards.csv.gz", tableName)

			discardsSchema := lo.MapKeys(whutils.DiscardsSchema, func(_, key string) string {
				return whutils.ToProviderCase(whutils.SNOWFLAKE, key)
			})

			loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := newMockUploader(t, loadFiles, tableName, discardsSchema, discardsSchema, false, false)

			sf, err := snowflake.New(config.New(), logger.NOP, memstats.New())
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

			records := testhelper.RetrieveRecordsFromWarehouse(t, sf.DB.DB,
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
					ORDER BY ROW_ID ASC;
					`,
					namespace,
					tableName,
				),
			)
			require.Equal(t, records, testhelper.DiscardTestRecords())
		})
	})
}

func TestSnowflake_ShouldMerge(t *testing.T) {
	testCases := []struct {
		name                  string
		enableMerge           bool
		uploaderCanAppend     bool
		uploaderExpectedCalls int
		expected              bool
	}{
		{
			name:                  "uploader says we can append and merge is not enabled",
			enableMerge:           false,
			uploaderCanAppend:     true,
			uploaderExpectedCalls: 1,
			expected:              false,
		},
		{
			name:                  "uploader says we cannot append and merge is not enabled",
			enableMerge:           false,
			uploaderCanAppend:     false,
			uploaderExpectedCalls: 1,
			expected:              true,
		},
		{
			name:                  "uploader says we can append and merge is enabled",
			enableMerge:           true,
			uploaderCanAppend:     true,
			uploaderExpectedCalls: 1,
			expected:              true,
		},
		{
			name:                  "uploader says we cannot append and we are in merge mode",
			enableMerge:           true,
			uploaderCanAppend:     false,
			uploaderExpectedCalls: 1,
			expected:              true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sf, err := snowflake.New(config.New(), logger.NOP, memstats.New())
			require.NoError(t, err)

			sf.Warehouse = model.Warehouse{
				Destination: backendconfig.DestinationT{
					Config: map[string]any{
						string(model.EnableMergeSetting): tc.enableMerge,
					},
				},
			}

			mockCtrl := gomock.NewController(t)
			uploader := mockuploader.NewMockUploader(mockCtrl)
			uploader.EXPECT().CanAppend().Times(tc.uploaderExpectedCalls).Return(tc.uploaderCanAppend)

			sf.Uploader = uploader
			require.Equal(t, sf.ShouldMerge(), tc.expected)
		})
	}
}

func newMockUploader(
	t testing.TB,
	loadFiles []whutils.LoadFile,
	tableName string,
	schemaInUpload model.TableSchema,
	schemaInWarehouse model.TableSchema,
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
		func(ctx context.Context, options whutils.GetLoadFilesOptions) ([]whutils.LoadFile, error) {
			return slices.Clone(loadFiles), nil
		},
	).AnyTimes()
	mockUploader.EXPECT().GetSampleLoadFileLocation(gomock.Any(), gomock.Any()).Return(loadFiles[0].Location, nil).AnyTimes()
	mockUploader.EXPECT().GetTableSchemaInUpload(tableName).Return(schemaInUpload).AnyTimes()
	mockUploader.EXPECT().GetTableSchemaInWarehouse(tableName).Return(schemaInWarehouse).AnyTimes()
	mockUploader.EXPECT().GetLoadFileType().Return(whutils.LoadFileTypeCsv).AnyTimes()

	return mockUploader
}

func getSnowflakeDB(t testing.TB, dsn string) *sql.DB {
	t.Helper()
	db, err := sql.Open("snowflake", dsn)
	require.NoError(t, err)
	require.NoError(t, db.Ping())
	return db
}
