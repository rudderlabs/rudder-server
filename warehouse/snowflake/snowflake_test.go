package snowflake_test

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/snowflake"
	"github.com/rudderlabs/rudder-server/warehouse/testhelper"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type TestHandle struct {
	DB        *sql.DB
	EventsMap testhelper.EventsCountMap
	WriteKey  string
}

var (
	handle *TestHandle
)

const (
	TestCredentialsKey = "SNOWFLAKE_INTEGRATION_TEST_USER_CRED"
)

func snowflakeCredentials() (snowflakeCredentials snowflake.SnowflakeCredentialsT, err error) {
	cred, exists := os.LookupEnv(TestCredentialsKey)
	if !exists {
		err = fmt.Errorf("following %s does not exists while running the Snowflake test", TestCredentialsKey)
		return
	}

	err = json.Unmarshal([]byte(cred), &snowflakeCredentials)
	if err != nil {
		err = fmt.Errorf("error occurred while unmarshalling snowflake test credentials with err: %s", err.Error())
		return
	}
	return
}

func (*TestHandle) TestConnection() error {
	credentials, err := snowflakeCredentials()
	if err != nil {
		return err
	}

	err = testhelper.ConnectWithBackoff(func() (err error) {
		handle.DB, err = snowflake.Connect(credentials)
		if err != nil {
			err = fmt.Errorf("could not connect to warehouse snowflake with error: %w", err)
			return
		}
		return
	})
	if err != nil {
		return fmt.Errorf("error while running test connection for snowflake with err: %s", err.Error())
	}
	return nil
}

func TestSnowflakeIntegration(t *testing.T) {
	warehouseTest := &testhelper.WareHouseTest{
		Client: &client.Client{
			SQL:  handle.DB,
			Type: client.SQLClient,
		},
		WriteKey:                 handle.WriteKey,
		Schema:                   "SNOWFLAKE_WH_INTEGRATION",
		EventsCountMap:           handle.EventsMap,
		VerifyingTablesFrequency: testhelper.LongRunningQueryFrequency,
	}

	warehouseTest.Reset(warehouseutils.SNOWFLAKE, true)
	testhelper.SendEvents(t, warehouseTest)
	testhelper.VerifyingDestination(t, warehouseTest)

	warehouseTest.Reset(warehouseutils.SNOWFLAKE, true)
	testhelper.SendModifiedEvents(t, warehouseTest)
	testhelper.VerifyingDestination(t, warehouseTest)
}

func TestMain(m *testing.M) {
	_, exists := os.LookupEnv(TestCredentialsKey)
	if !exists {
		log.Println("Skipping Snowflake Test as the Test credentials does not exits.")
		return
	}

	handle = &TestHandle{
		WriteKey:  "2eSJyYtqwcFiUILzXv2fcNIrWO7",
		EventsMap: testhelper.DefaultEventMap(),
	}
	os.Exit(testhelper.Run(m, handle))
}
