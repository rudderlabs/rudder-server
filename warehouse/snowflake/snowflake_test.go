//go:build warehouse_integration && !sources_integration

package snowflake_test

import (
	"fmt"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/validations"
	"os"
	"strings"
	"testing"

	"github.com/rudderlabs/rudder-server/utils/timeutil"
	"github.com/rudderlabs/rudder-server/warehouse/client"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"

	"github.com/rudderlabs/rudder-server/warehouse/snowflake"
	"github.com/rudderlabs/rudder-server/warehouse/testhelper"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/stretchr/testify/require"
)

func TestSnowflakeIntegration(t *testing.T) {
	t.SkipNow()
	t.Parallel()

	if _, exists := os.LookupEnv(testhelper.SnowflakeIntegrationTestCredentials); !exists {
		t.Skipf("Skipping %s as %s is not set", t.Name(), testhelper.SnowflakeIntegrationTestCredentials)
	}

	snowflake.Init()

	credentials, err := testhelper.SnowflakeCredentials()
	require.NoError(t, err)

	testcase := []struct {
		name          string
		dbName        string
		schema        string
		writeKey      string
		sourceID      string
		destinationID string
	}{
		{
			name:          "Normal Database",
			dbName:        credentials.DBName,
			schema:        testhelper.Schema(warehouseutils.SNOWFLAKE, testhelper.SnowflakeIntegrationTestSchema),
			writeKey:      "2eSJyYtqwcFiUILzXv2fcNIrWO7",
			sourceID:      "24p1HhPk09FW25Kuzvx7GshCLKR",
			destinationID: "24qeADObp6eIhjjDnEppO6P1SNc",
		},
		{
			name:          "Case Sensitive Database",
			dbName:        strings.ToLower(credentials.DBName),
			schema:        fmt.Sprintf("%s_%s", testhelper.Schema(warehouseutils.SNOWFLAKE, testhelper.SnowflakeIntegrationTestSchema), "CS"),
			writeKey:      "2eSJyYtqwcFYUILzXv2fcNIrWO7",
			sourceID:      "24p1HhPk09FBMKuzvx7GshCLKR",
			destinationID: "24qeADObp6eJhijDnEppO6P1SNc",
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
				Tables:        []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"},
				Provider:      warehouseutils.SNOWFLAKE,
			}

			// Scenario 1
			warehouseTest.TimestampBeforeSendingEvents = timeutil.Now()
			warehouseTest.UserId = testhelper.GetUserId(warehouseutils.SNOWFLAKE)

			sendEventsMap := testhelper.SendEventsMap()
			testhelper.SendEvents(t, warehouseTest, sendEventsMap)
			testhelper.SendEvents(t, warehouseTest, sendEventsMap)
			testhelper.SendEvents(t, warehouseTest, sendEventsMap)
			testhelper.SendIntegratedEvents(t, warehouseTest, sendEventsMap)

			testhelper.VerifyEventsInStagingFiles(t, jobsDB, warehouseTest, testhelper.DefaultStagingFilesEventsMap())
			testhelper.VerifyEventsInLoadFiles(t, jobsDB, warehouseTest, testhelper.DefaultLoadFilesEventsMap())
			testhelper.VerifyEventsInTableUploads(t, jobsDB, warehouseTest, testhelper.DefaultTableUploadsEventsMap())
			testhelper.VerifyEventsInWareHouse(t, warehouseTest, testhelper.DefaultWarehouseEventsMap())

			// Scenario 2
			warehouseTest.TimestampBeforeSendingEvents = timeutil.Now()
			warehouseTest.UserId = testhelper.GetUserId(warehouseutils.SNOWFLAKE)

			sendEventsMap = testhelper.SendEventsMap()
			testhelper.SendModifiedEvents(t, warehouseTest, sendEventsMap)
			testhelper.SendModifiedEvents(t, warehouseTest, sendEventsMap)
			testhelper.SendModifiedEvents(t, warehouseTest, sendEventsMap)
			testhelper.SendIntegratedEvents(t, warehouseTest, sendEventsMap)

			testhelper.VerifyEventsInStagingFiles(t, jobsDB, warehouseTest, testhelper.DefaultStagingFilesEventsMap())
			testhelper.VerifyEventsInLoadFiles(t, jobsDB, warehouseTest, testhelper.DefaultLoadFilesEventsMap())
			testhelper.VerifyEventsInTableUploads(t, jobsDB, warehouseTest, testhelper.DefaultTableUploadsEventsMap())
			testhelper.VerifyEventsInWareHouse(t, warehouseTest, testhelper.DefaultWarehouseEventsMap())

			testhelper.VerifyWorkspaceIDInStats(t)
		})
	}
}

func TestSnowflakeConfigurationValidation(t *testing.T) {
	t.SkipNow()
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
