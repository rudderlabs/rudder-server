package postgres_test

import (
	"database/sql"
	"fmt"
	"os"
	"testing"

	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/postgres"
	"github.com/rudderlabs/rudder-server/warehouse/testhelper"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type TestHandle struct {
	DB        *sql.DB
	EventsMap testhelper.EventsCountMap
	WriteKey  string
}

var handle *TestHandle

func (*TestHandle) TestConnection() error {
	err := testhelper.ConnectWithBackoff(func() (err error) {
		credentials := postgres.CredentialsT{
			DBName:   "rudderdb",
			Password: "rudder-password",
			User:     "rudder",
			Host:     "postgres",
			SSLMode:  "disable",
			Port:     "5432",
		}
		if handle.DB, err = postgres.Connect(credentials); err != nil {
			err = fmt.Errorf("could not connect to warehouse postgres with error: %w", err)
			return
		}
		if err = handle.DB.Ping(); err != nil {
			err = fmt.Errorf("could not connect to warehouse postgres while pinging with error: %w", err)
			return
		}
		return
	})
	if err != nil {
		return fmt.Errorf("error while running test connection for postgres with err: %s", err.Error())
	}
	return nil
}

func TestPostgresIntegration(t *testing.T) {
	warehouseTest := &testhelper.WareHouseTest{
		Client: &client.Client{
			SQL:  handle.DB,
			Type: client.SQLClient,
		},
		WriteKey:                 handle.WriteKey,
		Schema:                   "postgres_wh_integration",
		EventsCountMap:           handle.EventsMap,
		VerifyingTablesFrequency: testhelper.DefaultQueryFrequency,
	}

	warehouseTest.Reset(warehouseutils.POSTGRES, true)
	testhelper.SendEvents(t, warehouseTest)
	testhelper.VerifyingDestination(t, warehouseTest)

	warehouseTest.Reset(warehouseutils.POSTGRES, true)
	testhelper.SendModifiedEvents(t, warehouseTest)
	testhelper.VerifyingDestination(t, warehouseTest)
}

func TestMain(m *testing.M) {
	handle = &TestHandle{
		WriteKey:  "kwzDkh9h2fhfUVuS9jZ8uVbhV3v",
		EventsMap: testhelper.DefaultEventMap(),
	}
	os.Exit(testhelper.Run(m, handle))
}
