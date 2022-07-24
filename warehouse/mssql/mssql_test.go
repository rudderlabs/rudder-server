package mssql_test

import (
	"database/sql"
	"fmt"
	"os"
	"testing"

	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/mssql"
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
		credentials := mssql.CredentialsT{
			DBName:   "master",
			Password: "reallyStrongPwd123",
			User:     "SA",
			Host:     "mssql",
			SSLMode:  "disable",
			Port:     "1433",
		}
		if handle.DB, err = mssql.Connect(credentials); err != nil {
			err = fmt.Errorf("could not connect to warehouse mssql with error: %w", err)
			return
		}
		if err = handle.DB.Ping(); err != nil {
			err = fmt.Errorf("could not connect to warehouse mssql while pinging with error: %w", err)
			return
		}
		return
	})
	if err != nil {
		return fmt.Errorf("error while running test connection for mssql with err: %s", err.Error())
	}
	return nil
}

func TestMSSQLIntegration(t *testing.T) {
	warehouseTest := &testhelper.WareHouseTest{
		Client: &client.Client{
			SQL:  handle.DB,
			Type: client.SQLClient,
		},
		WriteKey:                 handle.WriteKey,
		Schema:                   "mssql_wh_integration",
		EventsCountMap:           handle.EventsMap,
		VerifyingTablesFrequency: testhelper.DefaultQueryFrequency,
	}

	warehouseTest.SetUserId(warehouseutils.MSSQL)
	testhelper.SendEvents(t, warehouseTest)
	testhelper.VerifyingDestination(t, warehouseTest)

	warehouseTest.SetUserId(warehouseutils.MSSQL)
	testhelper.SendModifiedEvents(t, warehouseTest)
	testhelper.VerifyingDestination(t, warehouseTest)
}

func TestMain(m *testing.M) {
	handle = &TestHandle{
		WriteKey:  "YSQ3n267l1VQKGNbSuJE9fQbzON",
		EventsMap: testhelper.DefaultEventMap(),
	}
	os.Exit(testhelper.Run(m, handle))
}
