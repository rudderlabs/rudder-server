//go:build warehouse_integration && !sources_integration

package deltalake_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/rudderlabs/rudder-server/warehouse/validations"

	"github.com/rudderlabs/rudder-server/utils/misc"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"

	"github.com/rudderlabs/rudder-server/utils/timeutil"

	proto "github.com/rudderlabs/rudder-server/proto/databricks"

	"github.com/rudderlabs/rudder-server/warehouse/client"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/warehouse/deltalake"
	"github.com/rudderlabs/rudder-server/warehouse/testhelper"
)

var statsToVerify = []string{
	"warehouse_deltalake_grpcExecTime",
	"warehouse_deltalake_healthTimeouts",
}

func TestDeltalakeIntegration(t *testing.T) {
	t.Parallel()

	if _, exists := os.LookupEnv(testhelper.DeltalakeIntegrationTestCredentials); !exists {
		t.Skipf("Skipping %s as %s is not set", t.Name(), testhelper.DeltalakeIntegrationTestCredentials)
	}

	deltalake.Init()

	credentials, err := testhelper.DatabricksCredentials()
	require.NoError(t, err)

	db, err := deltalake.Connect(&credentials, 0)
	require.NoError(t, err)

	schema := testhelper.Schema(warehouseutils.DELTALAKE, testhelper.DeltalakeIntegrationTestSchema)

	t.Cleanup(func() {
		require.NoError(t, testhelper.WithConstantBackoff(func() (err error) {
			dropSchemaResponse, err := db.Client.Execute(db.Context, &proto.ExecuteRequest{
				Config:       db.CredConfig,
				Identifier:   db.CredIdentifier,
				SqlStatement: fmt.Sprintf(`DROP SCHEMA %[1]s CASCADE;`, schema),
			})
			if err != nil {
				return fmt.Errorf("failed dropping schema %s for Deltalake, error: %s", schema, err.Error())
			}
			if dropSchemaResponse.GetErrorCode() != "" {
				return fmt.Errorf("failed dropping schema %s for Deltalake, errorCode: %s, errorMessage: %s", schema, dropSchemaResponse.GetErrorCode(), dropSchemaResponse.GetErrorMessage())
			}
			return
		}))
	})

	testCases := []struct {
		name             string
		skipUserCreation bool
		warehouseEvents  testhelper.EventsCountMap
		prerequisite     func(t *testing.T)
	}{
		{
			name:            "Merge Mode",
			warehouseEvents: mergeEventsMap(),
			prerequisite: func(t *testing.T) {
				t.Helper()
				require.NoError(t, testhelper.SetConfig([]warehouseutils.KeyValue{
					{
						Key:   "Warehouse.deltalake.loadTableStrategy",
						Value: "MERGE",
					},
				}))
			},
		},
		{
			name:             "Append Mode",
			skipUserCreation: true,
			warehouseEvents:  appendEventsMap(),
			prerequisite: func(t *testing.T) {
				t.Helper()
				require.NoError(t, testhelper.SetConfig([]warehouseutils.KeyValue{
					{
						Key:   "Warehouse.deltalake.loadTableStrategy",
						Value: "APPEND",
					},
				}))
			},
		},
	}

	jobsDB := testhelper.SetUpJobsDB(t)

	for _, tc := range testCases {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			if tc.prerequisite != nil {
				tc.prerequisite(t)
			}

			warehouseTest := &testhelper.WareHouseTest{
				Client: &client.Client{
					DBHandleT: db,
					Type:      client.DBClient,
				},
				WriteKey:      "sToFgoilA0U1WxNeW1gdgUVDsEW",
				Schema:        schema,
				Tables:        []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"},
				MessageId:     misc.FastUUID().String(),
				Provider:      warehouseutils.DELTALAKE,
				SourceID:      "25H5EpYzojqQSepRSaGBrrPx3e4",
				DestinationID: "25IDjdnoEus6DDNrth3SWO1FOpu",
			}

			// Scenario 1
			warehouseTest.TimestampBeforeSendingEvents = timeutil.Now()
			warehouseTest.UserId = testhelper.GetUserId(warehouseutils.DELTALAKE)

			sendEventsMap := testhelper.SendEventsMap()
			testhelper.SendEvents(t, warehouseTest, sendEventsMap)
			testhelper.SendEvents(t, warehouseTest, sendEventsMap)
			testhelper.SendEvents(t, warehouseTest, sendEventsMap)
			testhelper.SendIntegratedEvents(t, warehouseTest, sendEventsMap)

			testhelper.VerifyEventsInStagingFiles(t, jobsDB, warehouseTest, testhelper.DefaultStagingFilesEventsMap())
			testhelper.VerifyEventsInLoadFiles(t, jobsDB, warehouseTest, testhelper.DefaultLoadFilesEventsMap())
			testhelper.VerifyEventsInTableUploads(t, jobsDB, warehouseTest, testhelper.DefaultTableUploadsEventsMap())
			testhelper.VerifyEventsInWareHouse(t, warehouseTest, mergeEventsMap())

			// Scenario 2
			warehouseTest.TimestampBeforeSendingEvents = timeutil.Now()
			if !tc.skipUserCreation {
				warehouseTest.UserId = testhelper.GetUserId(warehouseutils.DELTALAKE)
			}

			sendEventsMap = testhelper.SendEventsMap()
			testhelper.SendModifiedEvents(t, warehouseTest, sendEventsMap)
			testhelper.SendModifiedEvents(t, warehouseTest, sendEventsMap)
			testhelper.SendModifiedEvents(t, warehouseTest, sendEventsMap)
			testhelper.SendIntegratedEvents(t, warehouseTest, sendEventsMap)

			testhelper.VerifyEventsInStagingFiles(t, jobsDB, warehouseTest, testhelper.DefaultStagingFilesEventsMap())
			testhelper.VerifyEventsInLoadFiles(t, jobsDB, warehouseTest, testhelper.DefaultLoadFilesEventsMap())
			testhelper.VerifyEventsInTableUploads(t, jobsDB, warehouseTest, testhelper.DefaultTableUploadsEventsMap())
			testhelper.VerifyEventsInWareHouse(t, warehouseTest, tc.warehouseEvents)

			testhelper.VerifyWorkspaceIDInStats(t, statsToVerify...)
		})
	}
}

func TestDeltalakeConfigurationValidation(t *testing.T) {
	t.Parallel()

	if _, exists := os.LookupEnv(testhelper.DeltalakeIntegrationTestCredentials); !exists {
		t.Skipf("Skipping %s as %s is not set", t.Name(), testhelper.DeltalakeIntegrationTestCredentials)
	}

	misc.Init()
	validations.Init()
	warehouseutils.Init()
	deltalake.Init()

	configurations := testhelper.PopulateTemplateConfigurations()
	destination := backendconfig.DestinationT{
		ID: "25IDjdnoEus6DDNrth3SWO1FOpu",
		Config: map[string]interface{}{
			"host":            configurations["deltalakeHost"],
			"port":            configurations["deltalakePort"],
			"path":            configurations["deltalakePath"],
			"token":           configurations["deltalakeToken"],
			"namespace":       configurations["deltalakeNamespace"],
			"bucketProvider":  "AZURE_BLOB",
			"containerName":   configurations["deltalakeContainerName"],
			"prefix":          "",
			"useSTSTokens":    false,
			"enableSSE":       false,
			"accountName":     configurations["deltalakeAccountName"],
			"accountKey":      configurations["deltalakeAccountKey"],
			"syncFrequency":   "30",
			"eventDelivery":   false,
			"eventDeliveryTS": 1648195480174,
		},
		DestinationDefinition: backendconfig.DestinationDefinitionT{
			ID:          "23HLpnDJnIg7DsBvDWGU6DQzFEo",
			Name:        "DELTALAKE",
			DisplayName: "Databricks (Delta Lake)",
		},
		Name:       "deltalake-demo",
		Enabled:    true,
		RevisionID: "29eClxJQQlaWzMWyqnQctFDP5T2",
	}
	testhelper.VerifyConfigurationTest(t, destination)
}

func mergeEventsMap() testhelper.EventsCountMap {
	return testhelper.EventsCountMap{
		"identifies":    1,
		"users":         1,
		"tracks":        1,
		"product_track": 1,
		"pages":         1,
		"screens":       1,
		"aliases":       1,
		"groups":        1,
	}
}

func appendEventsMap() testhelper.EventsCountMap {
	return testhelper.EventsCountMap{
		"identifies":    2,
		"users":         2,
		"tracks":        2,
		"product_track": 2,
		"pages":         2,
		"screens":       2,
		"aliases":       2,
		"groups":        2,
	}
}
