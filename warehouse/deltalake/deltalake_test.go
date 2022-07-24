package deltalake_test

import (
	"encoding/json"
	"fmt"
	"github.com/gofrs/uuid"
	"github.com/stretchr/testify/require"
	"log"
	"os"
	"testing"

	"github.com/rudderlabs/rudder-server/warehouse/client"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/warehouse/deltalake"
	"github.com/rudderlabs/rudder-server/warehouse/deltalake/databricks"
	"github.com/rudderlabs/rudder-server/warehouse/testhelper"
)

type TestHandle struct {
	DB        *databricks.DBHandleT
	EventsMap testhelper.EventsCountMap
	WriteKey  string
}

var (
	handle *TestHandle
)

const (
	TestCredentialsKey = "DATABRICKS_INTEGRATION_TEST_USER_CRED"
)

func databricksCredentials() (databricksCredentia databricks.CredentialsT, err error) {
	cred, exists := os.LookupEnv(TestCredentialsKey)
	if !exists {
		err = fmt.Errorf("following %s does not exists while running the Deltalake test", TestCredentialsKey)
		return
	}

	err = json.Unmarshal([]byte(cred), &databricksCredentia)
	if err != nil {
		err = fmt.Errorf("error occurred while unmarshalling databricks test credentials with err: %s", err.Error())
		return
	}
	return
}

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
		require.NoError(t, config.SetsAndWriteConfig([]config.KeyValue{
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
			WriteKey:                 handle.WriteKey,
			Schema:                   "deltalake_wh_integration",
			EventsCountMap:           handle.EventsMap,
			VerifyingTablesFrequency: testhelper.LongRunningQueryFrequency,
		}

		warehouseTest.MessageId = uuid.Must(uuid.NewV4()).String()
		warehouseTest.SetUserId(warehouseutils.DELTALAKE)

		testhelper.SendEvents(t, warehouseTest)
		testhelper.SendEvents(t, warehouseTest)
		testhelper.SendModifiedEvents(t, warehouseTest)
		testhelper.SendModifiedEvents(t, warehouseTest)

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
		testhelper.VerifyingDestination(t, warehouseTest)
	})
	t.Run("Append Mode", func(t *testing.T) {
		require.NoError(t, config.SetsAndWriteConfig([]config.KeyValue{
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
			WriteKey:                 handle.WriteKey,
			Schema:                   "deltalake_wh_integration",
			EventsCountMap:           handle.EventsMap,
			VerifyingTablesFrequency: testhelper.LongRunningQueryFrequency,
		}

		warehouseTest.MessageId = uuid.Must(uuid.NewV4()).String()
		warehouseTest.SetUserId(warehouseutils.DELTALAKE)

		testhelper.SendEvents(t, warehouseTest)
		testhelper.SendEvents(t, warehouseTest)
		testhelper.SendModifiedEvents(t, warehouseTest)
		testhelper.SendModifiedEvents(t, warehouseTest)

		warehouseTest.EventsCountMap = testhelper.EventsCountMap{
			"identifies":    4,
			"users":         4,
			"tracks":        4,
			"product_track": 4,
			"pages":         4,
			"screens":       4,
			"aliases":       4,
			"groups":        4,
			"gateway":       24,
			"batchRT":       32,
		}
		testhelper.VerifyingDestination(t, warehouseTest)
	})
}

func TestMain(m *testing.M) {
	_, exists := os.LookupEnv(TestCredentialsKey)
	if !exists {
		log.Println("Skipping Deltalake Test as the Test credentials does not exits.")
		return
	}

	handle = &TestHandle{
		WriteKey:  "sToFgoilA0U1WxNeW1gdgUVDsEW",
		EventsMap: testhelper.DefaultEventMap(),
	}
	os.Exit(testhelper.Run(m, handle))
}
