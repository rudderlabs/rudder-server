package deltalake_test

import (
	"fmt"
	"github.com/rudderlabs/rudder-server/warehouse/encoding"
	"os"
	"testing"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-server/warehouse/validations"

	"github.com/rudderlabs/rudder-server/utils/misc"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"

	warehouseclient "github.com/rudderlabs/rudder-server/warehouse/client"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/warehouse/integrations/deltalake"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/testhelper"
)

func TestIntegrationDeltalake(t *testing.T) {
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

	dl := deltalake.NewDeltalake()
	deltalake.WithConfig(dl, config.Default)

	db, err := deltalake.Connect(credentials)
	require.NoError(t, err)

	var (
		jobsDB   = testhelper.SetUpJobsDB(t)
		provider = warehouseutils.DELTALAKE
		schema   = testhelper.Schema(provider, testhelper.DeltalakeIntegrationTestSchema)
	)

	t.Cleanup(func() {
		require.NoError(
			t,
			testhelper.WithConstantBackoff(func() (err error) {
				_, err = db.Exec(fmt.Sprintf(`DROP SCHEMA %[1]s CASCADE;`, schema))
				return
			}),
			fmt.Sprintf("Failed dropping schema %s for Deltalake", schema),
		)
	})

	testCases := []struct {
		name               string
		schema             string
		writeKey           string
		sourceID           string
		destinationID      string
		messageID          string
		warehouseEventsMap testhelper.EventsCountMap
		prerequisite       func(t testing.TB)
	}{
		{
			name:               "Merge Mode",
			writeKey:           "sToFgoilA0U1WxNeW1gdgUVDsEW",
			schema:             schema,
			sourceID:           "25H5EpYzojqQSepRSaGBrrPx3e4",
			destinationID:      "25IDjdnoEus6DDNrth3SWO1FOpu",
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
				Prerequisite:  tc.prerequisite,
				JobsDB:        jobsDB,
				Provider:      provider,
				UserID:        testhelper.GetUserId(provider),
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
				Client: &warehouseclient.Client{
					SQL:  db,
					Type: warehouseclient.SQLClient,
				},
				StatsToVerify: []string{
					"warehouse_deltalake_grpcExecTime",
					"warehouse_deltalake_healthTimeouts",
				},
			}
			ts.VerifyEvents(t)

			ts.WarehouseEventsMap = tc.warehouseEventsMap
			ts.VerifyModifiedEvents(t)
		})
	}
}

func TestConfigurationValidationDeltalake(t *testing.T) {
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
	encoding.Init()
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
