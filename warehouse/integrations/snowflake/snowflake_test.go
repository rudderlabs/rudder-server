package snowflake_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/samber/lo"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	sqlconnectconfig "github.com/rudderlabs/sqlconnect-go/sqlconnect/config"

	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/stats"

	"go.uber.org/mock/gomock"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/compose-test/compose"
	"github.com/rudderlabs/compose-test/testcompose"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	th "github.com/rudderlabs/rudder-server/testhelper"
	"github.com/rudderlabs/rudder-server/testhelper/backendconfigtest"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/snowflake"
	whth "github.com/rudderlabs/rudder-server/warehouse/integrations/testhelper"
	mockuploader "github.com/rudderlabs/rudder-server/warehouse/internal/mocks/utils"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/rudderlabs/rudder-server/warehouse/validations"
)

type testCredentials struct {
	Account              string `json:"account"`
	User                 string `json:"user"`
	Password             string `json:"password"`
	Role                 string `json:"role"`
	Database             string `json:"database"`
	Warehouse            string `json:"warehouse"`
	BucketName           string `json:"bucketName"`
	AccessKeyID          string `json:"accessKeyID"`
	AccessKey            string `json:"accessKey"`
	UseKeyPairAuth       bool   `json:"useKeyPairAuth"`
	PrivateKey           string `json:"privateKey"`
	PrivateKeyPassphrase string `json:"privateKeyPassphrase"`
}

const (
	testKey                = "SNOWFLAKE_INTEGRATION_TEST_CREDENTIALS"
	testRBACKey            = "SNOWFLAKE_RBAC_INTEGRATION_TEST_CREDENTIALS"
	testKeyPairEncrypted   = "SNOWFLAKE_KEYPAIR_ENCRYPTED_INTEGRATION_TEST_CREDENTIALS"
	testKeyPairUnencrypted = "SNOWFLAKE_KEYPAIR_UNENCRYPTED_INTEGRATION_TEST_CREDENTIALS"
)

func getSnowflakeTestCredentials(key string) (*testCredentials, error) {
	cred, exists := os.LookupEnv(key)
	if !exists {
		return nil, errors.New("snowflake test credentials not found")
	}

	var credentials testCredentials
	err := json.Unmarshal([]byte(cred), &credentials)
	if err != nil {
		return nil, fmt.Errorf("unable to marshall %s to snowflake test credentials: %v", key, err)
	}
	return &credentials, nil
}

func TestIntegration(t *testing.T) {
	if os.Getenv("SLOW") != "1" {
		t.Skip("Skipping tests. Add 'SLOW=1' env var to run test.")
	}
	if _, exists := os.LookupEnv(testKey); !exists {
		if os.Getenv("FORCE_RUN_INTEGRATION_TESTS") == "true" {
			t.Fatalf("%s environment variable not set", testKey)
		}
		t.Skipf("Skipping %s as %s is not set", t.Name(), testKey)
	}

	misc.Init()
	validations.Init()
	whutils.Init()

	destType := whutils.SNOWFLAKE

	credentials, err := getSnowflakeTestCredentials(testKey)
	require.NoError(t, err)

	t.Run("Event flow", func(t *testing.T) {
		for _, key := range []string{
			testRBACKey,
			testKeyPairEncrypted,
			testKeyPairUnencrypted,
		} {
			if _, exists := os.LookupEnv(key); !exists {
				if os.Getenv("FORCE_RUN_INTEGRATION_TESTS") == "true" {
					t.Fatalf("%s environment variable not set", key)
				}
				t.Skipf("Skipping %s as %s is not set", t.Name(), key)
			}
		}

		rbacCredentials, err := getSnowflakeTestCredentials(testRBACKey)
		require.NoError(t, err)
		keyPairEncryptedCredentials, err := getSnowflakeTestCredentials(testKeyPairEncrypted)
		require.NoError(t, err)
		keyPairUnEncryptedCredentials, err := getSnowflakeTestCredentials(testKeyPairUnencrypted)
		require.NoError(t, err)

		httpPort, err := kithelper.GetFreePort()
		require.NoError(t, err)

		c := testcompose.New(t, compose.FilePaths([]string{"../testdata/docker-compose.jobsdb.yml"}))
		c.Start(context.Background())

		workspaceID := whutils.RandHex()
		jobsDBPort := c.Port("jobsDb", 5432)

		jobsDB := whth.JobsDB(t, jobsDBPort)

		testcase := []struct {
			name                          string
			tables                        []string
			stagingFilesEventsMap         whth.EventsCountMap
			stagingFilesModifiedEventsMap whth.EventsCountMap
			loadFilesEventsMap            whth.EventsCountMap
			tableUploadsEventsMap         whth.EventsCountMap
			warehouseEventsMap            whth.EventsCountMap
			warehouseEventsMap2           whth.EventsCountMap
			cred                          *testCredentials
			database                      string
			sourceJob                     bool
			stagingFilePrefix             string
			emptyJobRunID                 bool
			customUserID                  string
			configOverride                map[string]any
		}{
			{
				name: "Upload Job with Normal Database",
				tables: []string{
					"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups",
				},
				cred:     credentials,
				database: credentials.Database,
				stagingFilesEventsMap: whth.EventsCountMap{
					"wh_staging_files": 34, // 32 + 2 (merge events because of ID resolution)
				},
				stagingFilesModifiedEventsMap: whth.EventsCountMap{
					"wh_staging_files": 34, // 32 + 2 (merge events because of ID resolution)
				},
				stagingFilePrefix: "testdata/upload-job",
				configOverride: map[string]any{
					"preferAppend": false,
					"password":     credentials.Password,
				},
			},
			{
				name: "Upload Job with Role",
				tables: []string{
					"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups",
				},
				cred:     rbacCredentials,
				database: rbacCredentials.Database,
				stagingFilesEventsMap: whth.EventsCountMap{
					"wh_staging_files": 34, // 32 + 2 (merge events because of ID resolution)
				},
				stagingFilesModifiedEventsMap: whth.EventsCountMap{
					"wh_staging_files": 34, // 32 + 2 (merge events because of ID resolution)
				},
				stagingFilePrefix: "testdata/upload-job-with-role",
				configOverride: map[string]any{
					"preferAppend": false,
					"role":         rbacCredentials.Role,
					"password":     rbacCredentials.Password,
				},
			},
			{
				name: "Upload Job with Case Sensitive Database",
				tables: []string{
					"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups",
				},
				cred:     credentials,
				database: strings.ToLower(credentials.Database),
				stagingFilesEventsMap: whth.EventsCountMap{
					"wh_staging_files": 34, // 32 + 2 (merge events because of ID resolution)
				},
				stagingFilesModifiedEventsMap: whth.EventsCountMap{
					"wh_staging_files": 34, // 32 + 2 (merge events because of ID resolution)
				},
				stagingFilePrefix: "testdata/upload-job-case-sensitive",
				configOverride: map[string]any{
					"preferAppend": false,
					"password":     credentials.Password,
				},
			},
			{
				name: "Upload Job with Key Pair Unencrypted Key",
				tables: []string{
					"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups",
				},
				cred:     keyPairUnEncryptedCredentials,
				database: keyPairUnEncryptedCredentials.Database,
				stagingFilesEventsMap: whth.EventsCountMap{
					"wh_staging_files": 34, // 32 + 2 (merge events because of ID resolution)
				},
				stagingFilesModifiedEventsMap: whth.EventsCountMap{
					"wh_staging_files": 34, // 32 + 2 (merge events because of ID resolution)
				},
				stagingFilePrefix: "testdata/upload-job-case-sensitive",
				configOverride: map[string]any{
					"preferAppend":   false,
					"useKeyPairAuth": true,
					"privateKey":     keyPairUnEncryptedCredentials.PrivateKey,
				},
			},
			{
				name: "Upload Job with Key Pair Encrypted Key",
				tables: []string{
					"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups",
				},
				cred:     keyPairEncryptedCredentials,
				database: keyPairEncryptedCredentials.Database,
				stagingFilesEventsMap: whth.EventsCountMap{
					"wh_staging_files": 34, // 32 + 2 (merge events because of ID resolution)
				},
				stagingFilesModifiedEventsMap: whth.EventsCountMap{
					"wh_staging_files": 34, // 32 + 2 (merge events because of ID resolution)
				},
				stagingFilePrefix: "testdata/upload-job-case-sensitive",
				configOverride: map[string]any{
					"preferAppend":         false,
					"useKeyPairAuth":       true,
					"privateKey":           keyPairEncryptedCredentials.PrivateKey,
					"privateKeyPassphrase": keyPairEncryptedCredentials.PrivateKeyPassphrase,
				},
			},
			{
				name:     "Source Job with Sources",
				tables:   []string{"tracks", "google_sheet"},
				cred:     credentials,
				database: credentials.Database,
				stagingFilesEventsMap: whth.EventsCountMap{
					"wh_staging_files": 9, // 8 + 1 (merge events because of ID resolution)
				},
				stagingFilesModifiedEventsMap: whth.EventsCountMap{
					"wh_staging_files": 8, // 8 (de-duped by encounteredMergeRuleMap)
				},
				loadFilesEventsMap:    whth.SourcesLoadFilesEventsMap(),
				tableUploadsEventsMap: whth.SourcesTableUploadsEventsMap(),
				warehouseEventsMap:    whth.SourcesWarehouseEventsMap(),
				sourceJob:             true,
				stagingFilePrefix:     "testdata/sources-job",
				configOverride: map[string]any{
					"preferAppend": false,
					"password":     credentials.Password,
				},
			},
			{
				name:                          "Upload Job in append mode",
				tables:                        []string{"identifies", "users", "tracks"},
				cred:                          credentials,
				database:                      credentials.Database,
				stagingFilesEventsMap:         whth.EventsCountMap{"wh_staging_files": 3},
				stagingFilesModifiedEventsMap: whth.EventsCountMap{"wh_staging_files": 3},
				loadFilesEventsMap:            map[string]int{"identifies": 1, "users": 1, "tracks": 1},
				tableUploadsEventsMap:         map[string]int{"identifies": 1, "users": 1, "tracks": 1},
				warehouseEventsMap:            map[string]int{"identifies": 1, "users": 1, "tracks": 1},
				warehouseEventsMap2:           map[string]int{"identifies": 2, "users": 1, "tracks": 2},
				stagingFilePrefix:             "testdata/append-job",
				// an empty jobRunID means that the source is not an ETL one
				// see Uploader.CanAppend()
				emptyJobRunID: true,
				configOverride: map[string]any{
					"preferAppend": true,
					"password":     credentials.Password,
				},
				customUserID: whth.GetUserId("append_test"),
			},
			{
				name: "Undefined preferAppend",
				tables: []string{
					"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups",
				},
				cred:     credentials,
				database: credentials.Database,
				stagingFilesEventsMap: whth.EventsCountMap{
					"wh_staging_files": 34, // 32 + 2 (merge events because of ID resolution)
				},
				stagingFilesModifiedEventsMap: whth.EventsCountMap{
					"wh_staging_files": 34, // 32 + 2 (merge events because of ID resolution)
				},
				stagingFilePrefix: "testdata/upload-job-undefined-preferAppend-mode",
				configOverride: map[string]any{
					"password": credentials.Password,
				},
			},
		}

		for _, tc := range testcase {
			t.Run(tc.name, func(t *testing.T) {
				var (
					sourceID      = whutils.RandHex()
					destinationID = whutils.RandHex()
					writeKey      = whutils.RandHex()
					namespace     = whth.RandSchema(destType)
				)

				destinationBuilder := backendconfigtest.NewDestinationBuilder(destType).
					WithID(destinationID).
					WithRevisionID(destinationID).
					WithConfigOption("account", tc.cred.Account).
					WithConfigOption("database", tc.database).
					WithConfigOption("warehouse", tc.cred.Warehouse).
					WithConfigOption("user", tc.cred.User).
					WithConfigOption("cloudProvider", "AWS").
					WithConfigOption("bucketName", tc.cred.BucketName).
					WithConfigOption("accessKeyID", tc.cred.AccessKeyID).
					WithConfigOption("accessKey", tc.cred.AccessKey).
					WithConfigOption("namespace", namespace).
					WithConfigOption("enableSSE", false).
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

				t.Setenv("RSERVER_WAREHOUSE_SNOWFLAKE_MAX_PARALLEL_LOADS", "8")
				t.Setenv("RSERVER_WAREHOUSE_SNOWFLAKE_ENABLE_DELETE_BY_JOBS", "true")
				t.Setenv("RSERVER_WAREHOUSE_SNOWFLAKE_SLOW_QUERY_THRESHOLD", "0s")
				t.Setenv("RSERVER_WAREHOUSE_SNOWFLAKE_DEBUG_DUPLICATE_WORKSPACE_IDS", workspaceID)
				t.Setenv("RSERVER_WAREHOUSE_SNOWFLAKE_DEBUG_DUPLICATE_TABLES", strings.Join(
					[]string{
						"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups",
					},
					" ",
				))

				whth.BootstrapSvc(t, workspaceConfig, httpPort, jobsDBPort)

				credentialsJSON, err := json.Marshal(sqlconnectconfig.Snowflake{
					Account:              tc.cred.Account,
					User:                 tc.cred.User,
					Role:                 tc.cred.Role,
					DBName:               tc.database,
					Warehouse:            tc.cred.Warehouse,
					Password:             tc.cred.Password,
					UseKeyPairAuth:       tc.cred.UseKeyPairAuth,
					PrivateKey:           tc.cred.PrivateKey,
					PrivateKeyPassphrase: tc.cred.PrivateKeyPassphrase,
				})
				require.NoError(t, err)

				sqlConnectDB, err := sqlconnect.NewDB("snowflake", credentialsJSON)
				require.NoError(t, err)

				db := sqlConnectDB.SqlDB()
				require.NoError(t, db.Ping())
				t.Cleanup(func() { _ = db.Close() })
				t.Cleanup(func() {
					dropSchema(t, db, namespace)
				})

				sqlClient := &client.Client{
					SQL:  db,
					Type: client.SQLClient,
				}

				conf := map[string]interface{}{
					"cloudProvider":      "AWS",
					"bucketName":         tc.cred.BucketName,
					"storageIntegration": "",
					"accessKeyID":        tc.cred.AccessKeyID,
					"accessKey":          tc.cred.AccessKey,
					"prefix":             "snowflake-prefix",
					"enableSSE":          false,
					"useRudderStorage":   false,
				}

				t.Log("verifying test case 1")
				userID := tc.customUserID
				if userID == "" {
					userID = whth.GetUserId(destType)
				}
				jobRunID := ""
				if !tc.emptyJobRunID {
					jobRunID = misc.FastUUID().String()
				}
				ts1 := whth.TestConfig{
					WriteKey:              writeKey,
					Schema:                namespace,
					Tables:                tc.tables,
					SourceID:              sourceID,
					DestinationID:         destinationID,
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
					userID = whth.GetUserId(destType)
				}
				jobRunID = ""
				if !tc.emptyJobRunID {
					jobRunID = misc.FastUUID().String()
				}
				whEventsMap := tc.warehouseEventsMap2
				if whEventsMap == nil {
					whEventsMap = tc.warehouseEventsMap
				}
				ts2 := whth.TestConfig{
					WriteKey:              writeKey,
					Schema:                namespace,
					Tables:                tc.tables,
					SourceID:              sourceID,
					DestinationID:         destinationID,
					StagingFilesEventsMap: tc.stagingFilesModifiedEventsMap,
					LoadFilesEventsMap:    tc.loadFilesEventsMap,
					TableUploadsEventsMap: tc.tableUploadsEventsMap,
					WarehouseEventsMap:    whEventsMap,
					SourceJob:             tc.sourceJob,
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
				if tc.sourceJob {
					ts2.UserID = ts1.UserID
				}
				ts2.VerifyEvents(t)
			})
		}
	})

	t.Run("Validation", func(t *testing.T) {
		namespace := whth.RandSchema(destType)

		credentialsJSON, err := json.Marshal(sqlconnectconfig.Snowflake{
			Account:   credentials.Account,
			User:      credentials.User,
			Role:      credentials.Role,
			Password:  credentials.Password,
			DBName:    credentials.Database,
			Warehouse: credentials.Warehouse,
		})
		require.NoError(t, err)

		sqlConnectDB, err := sqlconnect.NewDB("snowflake", credentialsJSON)
		require.NoError(t, err)

		db := sqlConnectDB.SqlDB()
		require.NoError(t, db.Ping())
		t.Cleanup(func() { _ = db.Close() })
		t.Cleanup(func() {
			dropSchema(t, db, namespace)
		})

		dest := backendconfig.DestinationT{
			ID: "test_destination_id",
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
				"preferAppend":       false,
			},
			DestinationDefinition: backendconfig.DestinationDefinitionT{
				ID:          "1XjvXnzw34UMAz1YOuKqL1kwzh6",
				Name:        "SNOWFLAKE",
				DisplayName: "Snowflake",
			},
			Name:       "snowflake-demo",
			Enabled:    true,
			RevisionID: "test_destination_id",
		}
		whth.VerifyConfigurationTest(t, dest)
	})

	t.Run("Load Table", func(t *testing.T) {
		ctx := context.Background()
		namespace := whth.RandSchema(destType)

		credentialsJSON, err := json.Marshal(sqlconnectconfig.Snowflake{
			Account:   credentials.Account,
			User:      credentials.User,
			Role:      credentials.Role,
			Password:  credentials.Password,
			DBName:    credentials.Database,
			Warehouse: credentials.Warehouse,
		})
		require.NoError(t, err)

		sqlConnectDB, err := sqlconnect.NewDB("snowflake", credentialsJSON)
		require.NoError(t, err)

		db := sqlConnectDB.SqlDB()
		require.NoError(t, db.Ping())
		t.Cleanup(func() { _ = db.Close() })
		t.Cleanup(func() {
			dropSchema(t, db, namespace)
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
				ID: "test_source_id",
			},
			Destination: backendconfig.DestinationT{
				ID: "test_destination_id",
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
			WorkspaceID: "test_workspace_id",
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
			tableName := whutils.ToProviderCase(destType, "schema_not_exists_test_table")

			uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/load.csv.gz", tableName)

			loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, false, false)

			sf := snowflake.New(config.New(), logger.NOP, stats.NOP)
			err := sf.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			loadTableStat, err := sf.LoadTable(ctx, tableName)
			require.Error(t, err)
			require.Nil(t, loadTableStat)
		})
		t.Run("table does not exists", func(t *testing.T) {
			tableName := whutils.ToProviderCase(destType, "table_not_exists_test_table")

			uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/load.csv.gz", tableName)

			loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, false, false)

			sf := snowflake.New(config.New(), logger.NOP, stats.NOP)
			err := sf.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = sf.CreateSchema(ctx)
			require.NoError(t, err)

			loadTableStat, err := sf.LoadTable(ctx, tableName)
			require.Error(t, err)
			require.Nil(t, loadTableStat)
		})
		t.Run("merge", func(t *testing.T) {
			tableName := whutils.ToProviderCase(destType, "merge_test_table")

			t.Run("without dedup", func(t *testing.T) {
				uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/load.csv.gz", tableName)

				loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
				mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, true, false)

				appendWarehouse := th.Clone(t, warehouse)
				appendWarehouse.Destination.Config[model.PreferAppendSetting.String()] = true

				sf := snowflake.New(config.New(), logger.NOP, stats.NOP)
				err := sf.Setup(ctx, appendWarehouse, mockUploader)
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

				records := whth.RetrieveRecordsFromWarehouse(t, sf.DB.DB,
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
				require.Equal(t, whth.SampleTestRecords(), records)
			})
			t.Run("with dedup use new record", func(t *testing.T) {
				uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/dedup.csv.gz", tableName)

				loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
				mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, false, true)

				sf := snowflake.New(config.New(), logger.NOP, stats.NOP)
				err := sf.Setup(ctx, warehouse, mockUploader)
				require.NoError(t, err)

				err = sf.CreateSchema(ctx)
				require.NoError(t, err)

				err = sf.CreateTable(ctx, tableName, schemaInWarehouse)
				require.NoError(t, err)

				loadTableStat, err := sf.LoadTable(ctx, tableName)
				require.NoError(t, err)
				require.Equal(t, loadTableStat.RowsInserted, int64(0))
				require.Equal(t, loadTableStat.RowsUpdated, int64(14))

				records := whth.RetrieveRecordsFromWarehouse(t, db,
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
				require.Equal(t, records, whth.DedupTestRecords())
			})
		})
		t.Run("append", func(t *testing.T) {
			tableName := whutils.ToProviderCase(destType, "append_test_table")

			run := func() {
				uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/load.csv.gz", tableName)

				loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
				mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, true, false)

				appendWarehouse := th.Clone(t, warehouse)
				appendWarehouse.Destination.Config[model.PreferAppendSetting.String()] = true

				sf := snowflake.New(config.New(), logger.NOP, stats.NOP)
				err := sf.Setup(ctx, appendWarehouse, mockUploader)
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

			records := whth.RetrieveRecordsFromWarehouse(t, db,
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
			require.Equal(t, records, whth.AppendTestRecords())
		})
		t.Run("load file does not exists", func(t *testing.T) {
			tableName := whutils.ToProviderCase(destType, "load_file_not_exists_test_table")

			loadFiles := []whutils.LoadFile{{
				Location: "https://bucket.s3.amazonaws.com/rudder-warehouse-load-objects/load_file_not_exists_test_table/test_source_id/0ef75cb0-3fd0-4408-98b9-2bea9e476916-load_file_not_exists_test_table/load.csv.gz",
			}}
			mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, false, false)

			sf := snowflake.New(config.New(), logger.NOP, stats.NOP)
			err := sf.Setup(ctx, warehouse, mockUploader)
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
			tableName := whutils.ToProviderCase(destType, "mismatch_columns_test_table")

			uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/mismatch-columns.csv.gz", tableName)

			loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, false, false)

			sf := snowflake.New(config.New(), logger.NOP, stats.NOP)
			err := sf.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = sf.CreateSchema(ctx)
			require.NoError(t, err)

			err = sf.CreateTable(ctx, tableName, schemaInWarehouse)
			require.NoError(t, err)

			loadTableStat, err := sf.LoadTable(ctx, tableName)
			require.NoError(t, err)
			require.Equal(t, loadTableStat.RowsInserted, int64(14))
			require.Equal(t, loadTableStat.RowsUpdated, int64(0))

			records := whth.RetrieveRecordsFromWarehouse(t, sf.DB.DB,
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
			require.Equal(t, records, whth.SampleTestRecords())
		})
		t.Run("mismatch in schema", func(t *testing.T) {
			tableName := whutils.ToProviderCase(destType, "mismatch_schema_test_table")

			uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/mismatch-schema.csv.gz", tableName)

			loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, false, false)

			sf := snowflake.New(config.New(), logger.NOP, stats.NOP)
			err := sf.Setup(ctx, warehouse, mockUploader)
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
			tableName := whutils.ToProviderCase(destType, whutils.DiscardsTable)

			file, err := whth.CreateDiscardFileCSV(t)
			require.NoError(t, err)
			defer func() {
				_ = file.Close()
			}()
			uploadOutput := whth.UploadLoadFile(t, fm, file.Name(), tableName)

			discardsSchema := lo.MapKeys(whutils.DiscardsSchema, func(_, key string) string {
				return whutils.ToProviderCase(destType, key)
			})

			loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := newMockUploader(t, loadFiles, tableName, discardsSchema, discardsSchema, false, false)

			sf := snowflake.New(config.New(), logger.NOP, stats.NOP)
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

			records := whth.RetrieveRecordsFromWarehouse(t, sf.DB.DB,
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
			require.Equal(t, records, whth.DiscardTestRecords())
		})
	})

	t.Run("Delete By", func(t *testing.T) {
		ctx := context.Background()
		namespace := whth.RandSchema(destType)

		credentialsJSON, err := json.Marshal(sqlconnectconfig.Snowflake{
			Account:   credentials.Account,
			User:      credentials.User,
			Role:      credentials.Role,
			Password:  credentials.Password,
			DBName:    credentials.Database,
			Warehouse: credentials.Warehouse,
		})
		require.NoError(t, err)

		sqlConnectDB, err := sqlconnect.NewDB("snowflake", credentialsJSON)
		require.NoError(t, err)

		db := sqlConnectDB.SqlDB()
		require.NoError(t, db.Ping())
		t.Cleanup(func() { _ = db.Close() })
		t.Cleanup(func() {
			dropSchema(t, db, namespace)
		})

		conf := config.New()
		conf.Set("Warehouse.snowflake.enableDeleteByJobs", true)

		sf := snowflake.New(conf, logger.NOP, stats.NOP)
		sf.DB = sqlquerywrapper.New(db)
		sf.Namespace = namespace

		now := time.Now()

		_, err = sf.DB.ExecContext(ctx, fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s`, namespace))
		require.NoError(t, err, "should create schema")

		_, err = sf.DB.ExecContext(ctx, "CREATE TABLE "+namespace+".TEST_TABLE (id INT, context_sources_job_run_id STRING, context_sources_task_run_id STRING, context_source_id STRING, received_at DATETIME)")
		require.NoError(t, err, "should create table")

		_, err = sf.DB.ExecContext(ctx, "INSERT INTO "+namespace+".TEST_TABLE VALUES (1, 'job_run_id_2', 'task_run_id_1_2', 'source_id_1', ?)", now.Add(-time.Hour))
		require.NoError(t, err, "should insert records")
		_, err = sf.DB.ExecContext(ctx, "INSERT INTO "+namespace+".TEST_TABLE VALUES (2, 'job_run_id_2', 'task_run_id_1', 'source_id_2', ?)", now.Add(-time.Hour))
		require.NoError(t, err, "should insert records")

		require.NoError(t, sf.DeleteBy(ctx, []string{"TEST_TABLE"}, whutils.DeleteByParams{
			SourceId:  "source_id_1",
			JobRunId:  "new_job_run_id",
			TaskRunId: "new_task_job_run_id",
			StartTime: now,
		}), "should delete records")

		rows, err := sf.DB.QueryContext(ctx, "SELECT id FROM "+namespace+".TEST_TABLE")
		require.NoError(t, err, "should see a successful query for ids")

		var recordIDs []int
		for rows.Next() {
			var id int
			err := rows.Scan(&id)
			require.NoError(t, err, "should scan rows")

			recordIDs = append(recordIDs, id)
		}
		require.NoError(t, rows.Err())
		require.Equal(t, []int{2}, recordIDs, "got the correct set of ids after deletion")

		require.NoError(t, sf.DeleteBy(ctx, []string{"TEST_TABLE"}, whutils.DeleteByParams{
			SourceId:  "source_id_2",
			JobRunId:  "new_job_run_id",
			TaskRunId: "new_task_job_run_id",
			StartTime: time.Time{},
		}), "delete should succeed even if start time is zero value - no records must be deleted")

		rows, err = sf.DB.QueryContext(ctx, "SELECT id FROM "+namespace+".TEST_TABLE")
		require.NoError(t, err, "should see a successful query for ids")

		var ids1 []int
		for rows.Next() {
			var id int
			err := rows.Scan(&id)
			require.NoError(t, err, "should scan rows")

			ids1 = append(ids1, id)
		}
		require.NoError(t, rows.Err())
		require.Equal(t, []int{2}, ids1, "got the same set of ids after deletion")

		require.NoError(t, sf.DeleteBy(ctx, []string{"TEST_TABLE"}, whutils.DeleteByParams{
			SourceId:  "source_id_2",
			JobRunId:  "new_job_run_id",
			TaskRunId: "new_task_job_run_id",
			StartTime: now,
		}), "should delete records")

		rows, err = sf.DB.QueryContext(ctx, "SELECT id FROM "+namespace+".TEST_TABLE")
		require.NoError(t, err, "should see a successful query for ids")
		var ids2 []int
		for rows.Next() {
			var id int
			err := rows.Scan(&id)
			require.NoError(t, err, "should scan rows")

			ids2 = append(ids2, id)
		}
		require.NoError(t, rows.Err())
		require.Empty(t, ids2, "no more rows left")
	})
}

func TestSnowflake_ShouldMerge(t *testing.T) {
	testCases := []struct {
		name              string
		preferAppend      bool
		tableName         string
		appendOnlyTables  []string
		uploaderCanAppend bool
		expected          bool
	}{
		{
			name:              "uploader says we can append and user prefers append",
			preferAppend:      true,
			uploaderCanAppend: true,
			tableName:         "tracks",
			expected:          false,
		},
		{
			name:              "uploader says we cannot append and user prefers append",
			preferAppend:      true,
			uploaderCanAppend: false,
			tableName:         "tracks",
			expected:          true,
		},
		{
			name:              "uploader says we can append and user prefers not to append",
			preferAppend:      false,
			uploaderCanAppend: true,
			tableName:         "tracks",
			expected:          true,
		},
		{
			name:              "uploader says we cannot append and user prefers not to append",
			preferAppend:      false,
			uploaderCanAppend: false,
			tableName:         "tracks",
			expected:          true,
		},
		{
			name:              "uploader says we can append, in merge mode, but table is in append only",
			preferAppend:      false,
			uploaderCanAppend: true,
			tableName:         "tracks",
			appendOnlyTables:  []string{"tracks"},
			expected:          false,
		},
		{
			name:              "uploader says we can append, in append mode, but table is in append only",
			preferAppend:      true,
			uploaderCanAppend: true,
			tableName:         "tracks",
			appendOnlyTables:  []string{"tracks"},
			expected:          false,
		},
		{
			name:              "uploader says we can append, in append mode, but table is not in append only",
			preferAppend:      true,
			uploaderCanAppend: true,
			tableName:         "page_views",
			appendOnlyTables:  []string{"tracks"},
			expected:          false,
		},
		{
			name:              "uploader says we cannot append, in merge mode, but table is in append only",
			preferAppend:      false,
			uploaderCanAppend: false,
			tableName:         "tracks",
			appendOnlyTables:  []string{"tracks"},
			expected:          true,
		},
		{
			name:              "uploader says we can append, in merge mode, but table is not in append only",
			preferAppend:      false,
			uploaderCanAppend: true,
			tableName:         "page_views",
			appendOnlyTables:  []string{"tracks"},
			expected:          true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			conf := config.New()
			conf.Set("Warehouse.snowflake.appendOnlyTables", tc.appendOnlyTables)

			sf := snowflake.New(conf, logger.NOP, stats.NOP)
			sf.Warehouse = model.Warehouse{
				Destination: backendconfig.DestinationT{
					Config: map[string]any{
						model.PreferAppendSetting.String(): tc.preferAppend,
					},
				},
			}

			mockCtrl := gomock.NewController(t)
			uploader := mockuploader.NewMockUploader(mockCtrl)
			uploader.EXPECT().CanAppend().AnyTimes().Return(tc.uploaderCanAppend)

			sf.Uploader = uploader
			require.Equal(t, sf.ShouldMerge(tc.tableName), tc.expected)
		})
	}
}

func dropSchema(t *testing.T, db *sql.DB, namespace string) {
	t.Helper()
	t.Log("dropping schema", namespace)

	require.Eventually(t,
		func() bool {
			_, err := db.ExecContext(context.Background(), fmt.Sprintf(`DROP SCHEMA %q CASCADE;`, namespace))
			if err != nil {
				t.Logf("error deleting schema %q: %v", namespace, err)
				return false
			}
			return true
		},
		time.Minute,
		time.Second,
	)
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
