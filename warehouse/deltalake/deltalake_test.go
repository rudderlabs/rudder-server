//go:build warehouse_integration && !sources_integration

package deltalake_test

import (
	"fmt"
	"log"
	"os"
	"testing"

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

type TestHandle struct{}

var statsToVerify = []string{
	"warehouse_deltalake_grpcExecTime",
	"warehouse_deltalake_healthTimeouts",
}

func (*TestHandle) VerifyConnection() error {
	credentials, err := testhelper.DatabricksCredentials()
	if err != nil {
		return err
	}
	return testhelper.WithConstantBackoff(func() (err error) {
		_, err = deltalake.Connect(&credentials, 0)
		if err != nil {
			err = fmt.Errorf("could not connect to warehouse deltalake with error: %w", err)
			return
		}
		return
	})
}

func TestDeltalakeIntegration(t *testing.T) {
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
		name            string
		sourceID        string
		destinationID   string
		warehouseEvents testhelper.EventsCountMap
		prerequisite    func(t *testing.T)
	}{
		{
			name:            "Merge Mode",
			sourceID:        "24p1HhPk09FW25Kuzvx7GshCLKR",
			destinationID:   "24qeADObp6eIhjjDnEppO6P1SNc",
			warehouseEvents: mergeEventsMap(),
			prerequisite: func(t *testing.T) {
				require.NoError(t, testhelper.SetConfig([]warehouseutils.KeyValue{
					{
						Key:   "Warehouse.deltalake.loadTableStrategy",
						Value: "MERGE",
					},
				}))
			},
		},
		{
			name:            "Append Mode",
			sourceID:        "25H5EpYzojqQSepRSaGBrrPx3e4",
			destinationID:   "25IDjdnoEus6DDNrth3SWO1FOpu",
			warehouseEvents: appendEventsMap(),
			prerequisite: func(t *testing.T) {
				require.NoError(t, testhelper.SetConfig([]warehouseutils.KeyValue{
					{
						Key:   "Warehouse.deltalake.loadTableStrategy",
						Value: "APPEND",
					},
				}))
			},
		},
	}

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
				WriteKey:      "2eSJyYtqwcFiUILzXv2fcNIrWO7",
				Schema:        schema,
				Tables:        []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"},
				MessageId:     misc.FastUUID().String(),
				Provider:      warehouseutils.DELTALAKE,
				SourceID:      tc.sourceID,
				DestinationID: tc.destinationID,
			}

			// Scenario 1
			warehouseTest.TimestampBeforeSendingEvents = timeutil.Now()
			warehouseTest.UserId = testhelper.GetUserId(warehouseutils.DELTALAKE)

			sendEventsMap := testhelper.SendEventsMap()
			testhelper.SendEvents(t, warehouseTest, sendEventsMap)
			testhelper.SendEvents(t, warehouseTest, sendEventsMap)
			testhelper.SendEvents(t, warehouseTest, sendEventsMap)
			testhelper.SendIntegratedEvents(t, warehouseTest, sendEventsMap)

			testhelper.VerifyEventsInStagingFiles(t, warehouseTest, testhelper.StagingFilesEventsMap())
			testhelper.VerifyEventsInLoadFiles(t, warehouseTest, testhelper.LoadFilesEventsMap())
			testhelper.VerifyEventsInTableUploads(t, warehouseTest, testhelper.TableUploadsEventsMap())
			testhelper.VerifyEventsInWareHouse(t, warehouseTest, tc.warehouseEvents)

			// Scenario 2
			warehouseTest.TimestampBeforeSendingEvents = timeutil.Now()
			warehouseTest.UserId = testhelper.GetUserId(warehouseutils.DELTALAKE)

			sendEventsMap = testhelper.SendEventsMap()
			testhelper.SendModifiedEvents(t, warehouseTest, sendEventsMap)
			testhelper.SendModifiedEvents(t, warehouseTest, sendEventsMap)
			testhelper.SendModifiedEvents(t, warehouseTest, sendEventsMap)
			testhelper.SendIntegratedEvents(t, warehouseTest, sendEventsMap)

			testhelper.VerifyEventsInStagingFiles(t, warehouseTest, testhelper.StagingFilesEventsMap())
			testhelper.VerifyEventsInLoadFiles(t, warehouseTest, testhelper.LoadFilesEventsMap())
			testhelper.VerifyEventsInTableUploads(t, warehouseTest, testhelper.TableUploadsEventsMap())
			testhelper.VerifyEventsInWareHouse(t, warehouseTest, tc.warehouseEvents)

			testhelper.VerifyWorkspaceIDInStats(t, statsToVerify...)
		})
	}
}

func TestDeltalakeConfigurationValidation(t *testing.T) {
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
	testhelper.VerifyingConfigurationTest(t, destination)
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

func TestMain(m *testing.M) {
	_, exists := os.LookupEnv(testhelper.DeltalakeIntegrationTestCredentials)
	if !exists {
		log.Println("Skipping Deltalake Test as the Test credentials does not exists.")
		return
	}
	os.Exit(testhelper.Run(m, &TestHandle{}))
}
