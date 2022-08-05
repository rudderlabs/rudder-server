//go:build warehouse_integration

package deltalake_test

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/gofrs/uuid"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/warehouse/client"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"

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

const (
	TestCredentialsKey = testhelper.DeltalakeIntegrationTestCredentials
)

// databricksCredentials extracting deltalake credentials
func databricksCredentials() (databricksCredentials databricks.CredentialsT, err error) {
	cred, exists := os.LookupEnv(TestCredentialsKey)
	if !exists {
		err = fmt.Errorf("following %s does not exists while running the Deltalake test", TestCredentialsKey)
		return
	}

	err = json.Unmarshal([]byte(cred), &databricksCredentials)
	if err != nil {
		err = fmt.Errorf("error occurred while unmarshalling databricks test credentials with err: %s", err.Error())
		return
	}
	return
}

// TestConnection test connection for deltalake
func (*TestHandle) TestConnection() error {
	credentials, err := databricksCredentials()
	if err != nil {
		return err
	}

	err = testhelper.ConnectWithBackoff(func() (err error) {
		handle.DB, err = deltalake.Connect(&credentials, 0)
		if err != nil {
			err = fmt.Errorf("could not connect to warehouse deltalake with error: %w", err)
			return
		}
		return
	})
	if err != nil {
		return fmt.Errorf("error while running test connection for deltalake with err: %s", err.Error())
	}
	return nil
}

func TestDeltalakeIntegration(t *testing.T) {
	t.Run("Merge Mode", func(t *testing.T) {
		// Setting up the test configuration
		require.NoError(t, testhelper.SetConfig([]warehouseutils.KeyValue{
			{
				Key:   "Warehouse.deltalake.loadTableStrategy",
				Value: "MERGE",
			},
		}))

		// Setting up the warehouseTest
		warehouseTest := &testhelper.WareHouseTest{
			Client: &client.Client{
				DBHandleT: handle.DB,
				Type:      client.DBClient,
			},
			WriteKey:             handle.WriteKey,
			Schema:               handle.Schema,
			Tables:               handle.Tables,
			TablesQueryFrequency: testhelper.LongRunningQueryFrequency,
			EventsCountMap:       testhelper.DefaultEventMap(),
			MessageId:            uuid.Must(uuid.NewV4()).String(),
			UserId:               testhelper.GetUserId(warehouseutils.DELTALAKE),
		}

		// Scenario 1
		// Sending the first set of events.
		// Since we handle dedupe on the staging table, we need to check if the first set of events reached the destination.
		testhelper.SendEvents(t, warehouseTest)
		testhelper.SendEvents(t, warehouseTest)
		testhelper.SendEvents(t, warehouseTest)
		testhelper.SendEvents(t, warehouseTest)

		// Setting up the events map
		// Checking for Gateway and Batch router events
		// Checking for the events count for each table
		warehouseTest.EventsCountMap = testhelper.EventsCountMap{
			"identifies":    1,
			"users":         1,
			"tracks":        1,
			"product_track": 1,
			"pages":         1,
			"screens":       1,
			"aliases":       1,
			"groups":        1,
			"_groups":       1,
			"gateway":       24,
			"batchRT":       32,
		}
		testhelper.VerifyingGatewayEvents(t, warehouseTest)
		testhelper.VerifyingBatchRouterEvents(t, warehouseTest)
		testhelper.VerifyingTablesEventCount(t, warehouseTest)

		// Scenario 2
		// Re-Setting up the events map
		// Sending the second set of events.
		// This time we will not be resetting the MessageID. We will be using the same one to check for the dedupe.
		warehouseTest.EventsCountMap = testhelper.DefaultEventMap()
		testhelper.SendModifiedEvents(t, warehouseTest)
		testhelper.SendModifiedEvents(t, warehouseTest)
		testhelper.SendModifiedEvents(t, warehouseTest)
		testhelper.SendModifiedEvents(t, warehouseTest)

		// Setting up the events map
		// Checking for Gateway and Batch router events
		// Checking for the events count for each table
		// Since because of merge everything comes down to a single event in warehouse
		warehouseTest.EventsCountMap = testhelper.EventsCountMap{
			"identifies":    1,
			"users":         1,
			"tracks":        1,
			"product_track": 1,
			"pages":         1,
			"screens":       1,
			"aliases":       1,
			"groups":        1,
			"gateway":       48,
			"batchRT":       64,
		}
		testhelper.VerifyingGatewayEvents(t, warehouseTest)
		testhelper.VerifyingBatchRouterEvents(t, warehouseTest)
		testhelper.VerifyingTablesEventCount(t, warehouseTest)
	})

	t.Run("Append Mode", func(t *testing.T) {
		// Setting up the test configuration
		require.NoError(t, testhelper.SetConfig([]warehouseutils.KeyValue{
			{
				Key:   "Warehouse.deltalake.loadTableStrategy",
				Value: "APPEND",
			},
		}))

		// Setting up the warehouseTest
		warehouseTest := &testhelper.WareHouseTest{
			Client: &client.Client{
				DBHandleT: handle.DB,
				Type:      client.DBClient,
			},
			WriteKey:             handle.WriteKey,
			Schema:               handle.Schema,
			Tables:               handle.Tables,
			TablesQueryFrequency: testhelper.LongRunningQueryFrequency,
			EventsCountMap:       testhelper.DefaultEventMap(),
			MessageId:            uuid.Must(uuid.NewV4()).String(),
			UserId:               testhelper.GetUserId(warehouseutils.DELTALAKE),
		}

		// Scenario 1
		// Sending the first set of events.
		// Since we handle dedupe on the staging table, we need to check if the first set of events reached the destination.
		testhelper.SendEvents(t, warehouseTest)
		testhelper.SendEvents(t, warehouseTest)
		testhelper.SendEvents(t, warehouseTest)
		testhelper.SendEvents(t, warehouseTest)

		// Setting up the events map
		// Checking for Gateway and Batch router events
		// Checking for the events count for each table
		warehouseTest.EventsCountMap = testhelper.EventsCountMap{
			"identifies":    1,
			"users":         1,
			"tracks":        1,
			"product_track": 1,
			"pages":         1,
			"screens":       1,
			"aliases":       1,
			"groups":        1,
			"gateway":       24,
			"batchRT":       32,
		}
		testhelper.VerifyingGatewayEvents(t, warehouseTest)
		testhelper.VerifyingBatchRouterEvents(t, warehouseTest)
		testhelper.VerifyingTablesEventCount(t, warehouseTest)

		// Scenario 2
		// Re-Setting up the events map
		// Sending the second set of events.
		// This time we will not be resetting the MessageID. We will be using the same one to check for the dedupe.
		warehouseTest.EventsCountMap = testhelper.DefaultEventMap()
		testhelper.SendModifiedEvents(t, warehouseTest)
		testhelper.SendModifiedEvents(t, warehouseTest)
		testhelper.SendModifiedEvents(t, warehouseTest)
		testhelper.SendModifiedEvents(t, warehouseTest)

		// Setting up the events map
		// Checking for Gateway and Batch router events
		// Checking for the events count for each table
		// Since because of merge everything comes down to a single event in warehouse
		warehouseTest.EventsCountMap = testhelper.EventsCountMap{
			"identifies":    2,
			"users":         2,
			"tracks":        2,
			"product_track": 2,
			"pages":         2,
			"screens":       2,
			"aliases":       2,
			"groups":        2,
			"gateway":       48,
			"batchRT":       64,
		}
		testhelper.VerifyingGatewayEvents(t, warehouseTest)
		testhelper.VerifyingBatchRouterEvents(t, warehouseTest)
		testhelper.VerifyingTablesEventCount(t, warehouseTest)
	})
}

func TestMain(m *testing.M) {
	_, exists := os.LookupEnv(TestCredentialsKey)
	if !exists {
		log.Println("Skipping Deltalake Test as the Test credentials does not exits.")
		return
	}

	handle = &TestHandle{
		WriteKey: "sToFgoilA0U1WxNeW1gdgUVDsEW",
		Schema:   "deltalake_wh_integration",
		Tables:   []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"},
	}
	os.Exit(testhelper.Run(m, handle))
}
