package redshift_test

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/redshift"
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
	TestCredentialsKey = "REDSHIFT_INTEGRATION_TEST_USER_CRED"
)

func redshiftCredentials() (rsCredentials redshift.RedshiftCredentialsT, err error) {
	cred, exists := os.LookupEnv(TestCredentialsKey)
	if !exists {
		err = fmt.Errorf("following %s does not exists while running the Redshift test", TestCredentialsKey)
		return
	}

	err = json.Unmarshal([]byte(cred), &rsCredentials)
	if err != nil {
		err = fmt.Errorf("error occurred while unmarshalling redshift test credentials with err: %s", err.Error())
	}
	return
}

func (*TestHandle) TestConnection() error {
	credentials, err := redshiftCredentials()
	if err != nil {
		return err
	}

	err = testhelper.ConnectWithBackoff(func() (err error) {
		handle.DB, err = redshift.Connect(credentials)
		if err != nil {
			err = fmt.Errorf("could not connect to warehouse redshift with error: %w", err)
			return
		}
		return
	})
	if err != nil {
		return fmt.Errorf("error while running test connection for redshift with err: %s", err.Error())
	}
	return nil
}

func TestRedshiftIntegration(t *testing.T) {
	warehouseTest := &testhelper.WareHouseTest{
		Client: &client.Client{
			SQL:  handle.DB,
			Type: client.SQLClient,
		},
		WriteKey:                 handle.WriteKey,
		Schema:                   "redshift_wh_integration",
		EventsCountMap:           handle.EventsMap,
		VerifyingTablesFrequency: testhelper.LongRunningQueryFrequency,
	}

	warehouseTest.Reset(warehouseutils.RS, true)
	testhelper.SendEvents(t, warehouseTest)
	testhelper.VerifyingDestination(t, warehouseTest)

	warehouseTest.Reset(warehouseutils.RS, true)
	testhelper.SendModifiedEvents(t, warehouseTest)
	testhelper.VerifyingDestination(t, warehouseTest)
}

func TestMain(m *testing.M) {
	_, exists := os.LookupEnv(TestCredentialsKey)
	if !exists {
		log.Println("Skipping Redshift Test as the Test credentials does not exits.")
		return
	}

	handle = &TestHandle{
		WriteKey:  "JAAwdCxmM8BIabKERsUhPNmMmdf",
		EventsMap: testhelper.DefaultEventMap(),
	}
	os.Exit(testhelper.Run(m, handle))
}
