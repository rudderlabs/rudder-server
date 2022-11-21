package deltalake_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/rudderlabs/rudder-server/warehouse/validations"

	"github.com/rudderlabs/rudder-server/utils/misc"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"

	proto "github.com/rudderlabs/rudder-server/proto/databricks"

	"github.com/rudderlabs/rudder-server/warehouse/client"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/warehouse/deltalake"
	"github.com/rudderlabs/rudder-server/warehouse/testhelper"
)

func TestDeltalakeIntegration(t *testing.T) {
	if os.Getenv("SLOW") == "0" {
		t.Skip("Skipping tests. Remove 'SLOW=0' env var to run them.")
	}
	if _, exists := os.LookupEnv(testhelper.DeltalakeIntegrationTestCredentials); !exists {
		t.Skipf("Skipping %s as %s is not set", t.Name(), testhelper.DeltalakeIntegrationTestCredentials)
	}

	t.Parallel()

	deltalake.Init()

	credentials, err := testhelper.DatabricksCredentials()
	require.NoError(t, err)

	db, err := deltalake.Connect(&credentials, 0)
	require.NoError(t, err)

	var (
		jobsDB           = testhelper.SetUpJobsDB(t)
		provider         = warehouseutils.DELTALAKE
		appendModeUserID = testhelper.GetUserId(provider)
		schema           = testhelper.Schema(provider, testhelper.DeltalakeIntegrationTestSchema)
	)

	t.Cleanup(func() {
		require.NoError(t,
			testhelper.WithConstantBackoff(func() (err error) {
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
			}),
		)
	})

	testCases := []struct {
		name               string
		schema             string
		writeKey           string
		sourceID           string
		destinationID      string
		messageID          string
		scenarioOneUserID  string
		scenarioTwoUserID  string
		warehouseEventsMap testhelper.EventsCountMap
		prerequisite       func(t testing.TB)
	}{
		{
			name:               "Merge Mode",
			writeKey:           "sToFgoilA0U1WxNeW1gdgUVDsEW",
			schema:             schema,
			sourceID:           "25H5EpYzojqQSepRSaGBrrPx3e4",
			destinationID:      "25IDjdnoEus6DDNrth3SWO1FOpu",
			scenarioOneUserID:  testhelper.GetUserId(provider),
			scenarioTwoUserID:  testhelper.GetUserId(provider),
			warehouseEventsMap: mergeEventsMap(),
			prerequisite: func(t testing.TB) {
				t.Helper()
				testhelper.SetConfig(t, []warehouseutils.KeyValue{
					{
						Key:   "Warehouse.deltalake.loadTableStrategy",
						Value: "MERGE",
					},
				})
			},
		},
		{
			name:               "Append Mode",
			writeKey:           "sToFgoilA0U1WxNeW1gdgUVDsEW",
			schema:             schema,
			sourceID:           "25H5EpYzojqQSepRSaGBrrPx3e4",
			destinationID:      "25IDjdnoEus6DDNrth3SWO1FOpu",
			warehouseEventsMap: appendEventsMap(),
			scenarioOneUserID:  appendModeUserID,
			scenarioTwoUserID:  appendModeUserID,
			prerequisite: func(t testing.TB) {
				t.Helper()
				testhelper.SetConfig(t, []warehouseutils.KeyValue{
					{
						Key:   "Warehouse.deltalake.loadTableStrategy",
						Value: "APPEND",
					},
				})
			},
		},
	}

	for _, tc := range testCases {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			ts := testhelper.WareHouseTest{
				Schema:        tc.schema,
				WriteKey:      tc.writeKey,
				SourceID:      tc.sourceID,
				DestinationID: tc.destinationID,
				UserID:        tc.scenarioOneUserID,
				Prerequisite:  tc.prerequisite,
				JobsDB:        jobsDB,
				Provider:      provider,
				MessageID:     misc.FastUUID().String(),
				Tables:        []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"},
				WarehouseEventsMap: testhelper.EventsCountMap{
					"identifies":    1,
					"users":         1,
					"tracks":        1,
					"product_track": 1,
					"pages":         1,
					"screens":       1,
					"aliases":       1,
					"groups":        1,
				},
				Client: &client.Client{
					DBHandleT: db,
					Type:      client.DBClient,
				},
				StatsToVerify: []string{
					"warehouse_deltalake_grpcExecTime",
					"warehouse_deltalake_healthTimeouts",
				},
			}
			ts.TestScenarioOne(t)

			ts.UserID = tc.scenarioTwoUserID
			ts.WarehouseEventsMap = tc.warehouseEventsMap
			ts.TestScenarioTwo(t)
		})
	}
}

func TestDeltalakeConfigurationValidation(t *testing.T) {
	if os.Getenv("SLOW") == "0" {
		t.Skip("Skipping tests. Remove 'SLOW=0' env var to run them.")
	}
	if _, exists := os.LookupEnv(testhelper.DeltalakeIntegrationTestCredentials); !exists {
		t.Skipf("Skipping %s as %s is not set", t.Name(), testhelper.DeltalakeIntegrationTestCredentials)
	}

	t.Parallel()

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
