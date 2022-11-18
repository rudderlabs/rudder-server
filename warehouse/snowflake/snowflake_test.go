package snowflake_test

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/validations"

	"github.com/rudderlabs/rudder-server/utils/timeutil"
	"github.com/rudderlabs/rudder-server/warehouse/client"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"

	"github.com/rudderlabs/rudder-server/warehouse/snowflake"
	"github.com/rudderlabs/rudder-server/warehouse/testhelper"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/stretchr/testify/require"
)

func TestSnowflakeIntegration(t *testing.T) {
	if os.Getenv("SLOW") == "0" {
		t.Skip("Skipping tests. Remove 'SLOW=0' env var to run them.")
	}

	//t.SkipNow()
	t.Parallel()

	if _, exists := os.LookupEnv(testhelper.SnowflakeIntegrationTestCredentials); !exists {
		t.Skipf("Skipping %s as %s is not set", t.Name(), testhelper.SnowflakeIntegrationTestCredentials)
	}

	snowflake.Init()

	credentials, err := testhelper.SnowflakeCredentials()
	require.NoError(t, err)

	testcase := []struct {
		name                  string
		dbName                string
		schema                string
		writeKey              string
		sourceID              string
		destinationID         string
		tables                []string
		skipUserCreation      bool
		eventsMap             testhelper.EventsCountMap
		stagingFilesEventsMap testhelper.EventsCountMap
		loadFilesEventsMap    testhelper.EventsCountMap
		tableUploadsEventsMap testhelper.EventsCountMap
		warehouseEventsMap    testhelper.EventsCountMap
		asyncJob              func(t testing.TB, wareHouseTest *testhelper.WareHouseTest)
	}{
		{
			name:          "Upload Job with Normal Database",
			dbName:        credentials.DBName,
			schema:        testhelper.Schema(warehouseutils.SNOWFLAKE, testhelper.SnowflakeIntegrationTestSchema),
			tables:        []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"},
			writeKey:      "2eSJyYtqwcFiUILzXv2fcNIrWO7",
			sourceID:      "24p1HhPk09FW25Kuzvx7GshCLKR",
			destinationID: "24qeADObp6eIhjjDnEppO6P1SNc",
			eventsMap:     testhelper.SendEventsMap(),
			stagingFilesEventsMap: testhelper.EventsCountMap{
				"wh_staging_files": 34, // 32 + 2 (merge events because of ID resolution)
			},
			loadFilesEventsMap:    testhelper.DefaultLoadFilesEventsMap(),
			tableUploadsEventsMap: testhelper.DefaultTableUploadsEventsMap(),
			warehouseEventsMap:    testhelper.DefaultWarehouseEventsMap(),
		},
		{
			name:          "Upload Job with Case Sensitive Database",
			dbName:        strings.ToLower(credentials.DBName),
			schema:        fmt.Sprintf("%s_%s", testhelper.Schema(warehouseutils.SNOWFLAKE, testhelper.SnowflakeIntegrationTestSchema), "CS"),
			tables:        []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"},
			writeKey:      "2eSJyYtqwcFYUILzXv2fcNIrWO7",
			sourceID:      "24p1HhPk09FBMKuzvx7GshCLKR",
			destinationID: "24qeADObp6eJhijDnEppO6P1SNc",
			eventsMap:     testhelper.SendEventsMap(),
			stagingFilesEventsMap: testhelper.EventsCountMap{
				"wh_staging_files": 34, // 32 + 2 (merge events because of ID resolution)
			},
			loadFilesEventsMap:    testhelper.DefaultLoadFilesEventsMap(),
			tableUploadsEventsMap: testhelper.DefaultTableUploadsEventsMap(),
			warehouseEventsMap:    testhelper.DefaultWarehouseEventsMap(),
		},
		{
			name:          "Async Job with Sources",
			dbName:        credentials.DBName,
			schema:        fmt.Sprintf("%s_%s", testhelper.Schema(warehouseutils.SNOWFLAKE, testhelper.SnowflakeIntegrationTestSchema), "SOURCES"),
			tables:        []string{"tracks", "google_sheet"},
			writeKey:      "2eSJyYtqwcFYerwzXv2fcNIrWO7",
			sourceID:      "2DkCpUr0xgjaNRJxIwqyqfyHdq4",
			destinationID: "24qeADObp6eIsfjDnEppO6P1SNc",
			eventsMap: testhelper.EventsCountMap{
				"wh_staging_files": 9, // 8 + 1 (merge events because of ID resolution)
			},
			stagingFilesEventsMap: testhelper.SourcesStagingFilesEventsMap(),
			loadFilesEventsMap:    testhelper.SourcesLoadFilesEventsMap(),
			tableUploadsEventsMap: testhelper.SourcesTableUploadsEventsMap(),
			warehouseEventsMap:    testhelper.SourcesWarehouseEventsMap(),
			asyncJob:              testhelper.VerifyAsyncJob,
			skipUserCreation:      true,
		},
	}

	jobsDB := testhelper.SetUpJobsDB(t)

	for _, tc := range testcase {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			credentialsCopy := credentials
			credentialsCopy.DBName = tc.dbName

			db, err := snowflake.Connect(credentialsCopy)
			require.NoError(t, err)

			t.Cleanup(func() {
				require.NoError(t, testhelper.WithConstantBackoff(func() (err error) {
					_, err = db.Exec(fmt.Sprintf(`DROP SCHEMA "%s" CASCADE;`, tc.schema))
					return
				}), fmt.Sprintf("Failed dropping schema %s for Snowflake", tc.schema))
			})

			warehouseTest := &testhelper.WareHouseTest{
				Client: &client.Client{
					SQL:  db,
					Type: client.SQLClient,
				},
				WriteKey:      tc.writeKey,
				Schema:        tc.schema,
				SourceID:      tc.sourceID,
				DestinationID: tc.destinationID,
				Tables:        tc.tables,
				Provider:      warehouseutils.SNOWFLAKE,
			}

			// Scenario 1
			warehouseTest.TimestampBeforeSendingEvents = timeutil.Now()
			warehouseTest.UserId = testhelper.GetUserId(warehouseutils.SNOWFLAKE)
			warehouseTest.JobRunID = misc.FastUUID().String()
			warehouseTest.TaskRunID = misc.FastUUID().String()

			testhelper.SendEvents(t, warehouseTest, tc.eventsMap)
			testhelper.SendEvents(t, warehouseTest, tc.eventsMap)
			testhelper.SendEvents(t, warehouseTest, tc.eventsMap)
			testhelper.SendIntegratedEvents(t, warehouseTest, tc.eventsMap)

			testhelper.VerifyEventsInStagingFiles(t, jobsDB, warehouseTest, tc.stagingFilesEventsMap)
			testhelper.VerifyEventsInLoadFiles(t, jobsDB, warehouseTest, tc.loadFilesEventsMap)
			testhelper.VerifyEventsInTableUploads(t, jobsDB, warehouseTest, tc.tableUploadsEventsMap)
			testhelper.VerifyEventsInWareHouse(t, warehouseTest, tc.warehouseEventsMap)

			// Scenario 2
			warehouseTest.TimestampBeforeSendingEvents = timeutil.Now()
			warehouseTest.JobRunID = misc.FastUUID().String()
			warehouseTest.TaskRunID = misc.FastUUID().String()
			if !tc.skipUserCreation {
				warehouseTest.UserId = testhelper.GetUserId(warehouseutils.SNOWFLAKE)
			}

			testhelper.SendModifiedEvents(t, warehouseTest, tc.eventsMap)
			testhelper.SendModifiedEvents(t, warehouseTest, tc.eventsMap)
			testhelper.SendModifiedEvents(t, warehouseTest, tc.eventsMap)
			testhelper.SendIntegratedEvents(t, warehouseTest, tc.eventsMap)

			testhelper.VerifyEventsInStagingFiles(t, jobsDB, warehouseTest, tc.stagingFilesEventsMap)
			testhelper.VerifyEventsInLoadFiles(t, jobsDB, warehouseTest, tc.loadFilesEventsMap)
			testhelper.VerifyEventsInTableUploads(t, jobsDB, warehouseTest, tc.tableUploadsEventsMap)
			if tc.asyncJob != nil {
				tc.asyncJob(t, warehouseTest)
			}
			testhelper.VerifyEventsInWareHouse(t, warehouseTest, tc.warehouseEventsMap)

			testhelper.VerifyWorkspaceIDInStats(t)
		})
	}
}

func TestSnowflakeConfigurationValidation(t *testing.T) {
	if os.Getenv("SLOW") == "0" {
		t.Skip("Skipping tests. Remove 'SLOW=0' env var to run them.")
	}

	//t.SkipNow()
	t.Parallel()

	if _, exists := os.LookupEnv(testhelper.SnowflakeIntegrationTestCredentials); !exists {
		t.Skipf("Skipping %s as %s is not set", t.Name(), testhelper.SnowflakeIntegrationTestCredentials)
	}

	misc.Init()
	validations.Init()
	warehouseutils.Init()
	snowflake.Init()

	configurations := testhelper.PopulateTemplateConfigurations()
	destination := backendconfig.DestinationT{
		ID: "24qeADObp6eIhjjDnEppO6P1SNc",
		Config: map[string]interface{}{
			"account":            configurations["snowflakeAccount"],
			"database":           configurations["snowflakeDBName"],
			"warehouse":          configurations["snowflakeWHName"],
			"user":               configurations["snowflakeUsername"],
			"password":           configurations["snowflakePassword"],
			"cloudProvider":      "AWS",
			"bucketName":         configurations["snowflakeBucketName"],
			"storageIntegration": "",
			"accessKeyID":        configurations["snowflakeAccessKeyID"],
			"accessKey":          configurations["snowflakeAccessKey"],
			"namespace":          configurations["snowflakeNamespace"],
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
}
