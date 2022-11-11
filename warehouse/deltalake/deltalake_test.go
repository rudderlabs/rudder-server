//go:build warehouse_integration && !sources_integration

package deltalake_test

import (
	"fmt"
	"log"
	"os"
	"testing"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"

	"github.com/rudderlabs/rudder-server/utils/timeutil"

	"github.com/gofrs/uuid"

	proto "github.com/rudderlabs/rudder-server/proto/databricks"

	"github.com/rudderlabs/rudder-server/warehouse/client"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/warehouse/deltalake"
	"github.com/rudderlabs/rudder-server/warehouse/deltalake/databricks"
	"github.com/rudderlabs/rudder-server/warehouse/testhelper"
)

type TestHandle struct {
	DB       *databricks.DBHandleT
	WriteKey string
	Schema   string
	Tables   []string
}

var handle *TestHandle

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
		handle.DB, err = deltalake.Connect(&credentials, 0)
		if err != nil {
			err = fmt.Errorf("could not connect to warehouse deltalake with error: %w", err)
			return
		}
		return
	})
}

func TestDeltalakeIntegration(t *testing.T) {
	t.Cleanup(func() {
		require.NoError(t, testhelper.WithConstantBackoff(func() (err error) {
			dropSchemaResponse, err := handle.DB.Client.Execute(handle.DB.Context, &proto.ExecuteRequest{
				Config:       handle.DB.CredConfig,
				Identifier:   handle.DB.CredIdentifier,
				SqlStatement: fmt.Sprintf(`DROP SCHEMA %[1]s CASCADE;`, handle.Schema),
			})
			if err != nil {
				return fmt.Errorf("failed dropping schema %s for Deltalake, error: %s", handle.Schema, err.Error())
			}

			if dropSchemaResponse.GetErrorCode() != "" {
				return fmt.Errorf("failed dropping schema %s for Deltalake, errorCode: %s, errorMessage: %s", handle.Schema, dropSchemaResponse.GetErrorCode(), dropSchemaResponse.GetErrorMessage())
			}
			return
		}))
	})

	t.Run("Merge Mode", func(t *testing.T) {
		require.NoError(t, testhelper.SetConfig([]warehouseutils.KeyValue{
			{
				Key:   "Warehouse.deltalake.loadTableStrategy",
				Value: "MERGE",
			},
		}))

		warehouseTest := &testhelper.WareHouseTest{
			Client: &client.Client{
				DBHandleT: handle.DB,
				Type:      client.DBClient,
			},
			WriteKey:      handle.WriteKey,
			Schema:        handle.Schema,
			Tables:        handle.Tables,
			MessageId:     uuid.Must(uuid.NewV4()).String(),
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

		testhelper.VerifyEventsInStagingFiles(t, warehouseTest, testhelper.StagingFilesEventsMap())
		testhelper.VerifyEventsInLoadFiles(t, warehouseTest, testhelper.LoadFilesEventsMap())
		testhelper.VerifyEventsInTableUploads(t, warehouseTest, testhelper.TableUploadsEventsMap())
		testhelper.VerifyEventsInWareHouse(t, warehouseTest, mergeEventsMap())

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
		testhelper.VerifyEventsInWareHouse(t, warehouseTest, mergeEventsMap())

		testhelper.VerifyWorkspaceIDInStats(t, statsToVerify...)
	})

	t.Run("Append Mode", func(t *testing.T) {
		require.NoError(t, testhelper.SetConfig([]warehouseutils.KeyValue{
			{
				Key:   "Warehouse.deltalake.loadTableStrategy",
				Value: "APPEND",
			},
		}))

		warehouseTest := &testhelper.WareHouseTest{
			Client: &client.Client{
				DBHandleT: handle.DB,
				Type:      client.DBClient,
			},
			WriteKey:      handle.WriteKey,
			Schema:        handle.Schema,
			Tables:        handle.Tables,
			MessageId:     uuid.Must(uuid.NewV4()).String(),
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

		testhelper.VerifyEventsInStagingFiles(t, warehouseTest, testhelper.StagingFilesEventsMap())
		testhelper.VerifyEventsInLoadFiles(t, warehouseTest, testhelper.LoadFilesEventsMap())
		testhelper.VerifyEventsInTableUploads(t, warehouseTest, testhelper.TableUploadsEventsMap())
		testhelper.VerifyEventsInWareHouse(t, warehouseTest, mergeEventsMap())

		// Scenario 2
		warehouseTest.TimestampBeforeSendingEvents = timeutil.Now()

		sendEventsMap = testhelper.SendEventsMap()
		testhelper.SendModifiedEvents(t, warehouseTest, sendEventsMap)
		testhelper.SendModifiedEvents(t, warehouseTest, sendEventsMap)
		testhelper.SendModifiedEvents(t, warehouseTest, sendEventsMap)
		testhelper.SendIntegratedEvents(t, warehouseTest, sendEventsMap)

		testhelper.VerifyEventsInStagingFiles(t, warehouseTest, testhelper.StagingFilesEventsMap())
		testhelper.VerifyEventsInLoadFiles(t, warehouseTest, testhelper.LoadFilesEventsMap())
		testhelper.VerifyEventsInTableUploads(t, warehouseTest, testhelper.TableUploadsEventsMap())
		testhelper.VerifyEventsInWareHouse(t, warehouseTest, appendEventsMap())

		testhelper.VerifyWorkspaceIDInStats(t, statsToVerify...)
	})
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

	handle = &TestHandle{
		WriteKey: "sToFgoilA0U1WxNeW1gdgUVDsEW",
		Schema:   testhelper.Schema(warehouseutils.DELTALAKE, testhelper.DeltalakeIntegrationTestSchema),
		Tables:   []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"},
	}
	os.Exit(testhelper.Run(m, handle))
}
