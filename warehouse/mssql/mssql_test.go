//go:build whintegration

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

type MSSQLTest struct {
	Credentials *mssql.CredentialsT
	DB          *sql.DB
	EventsMap   testhelper.EventsCountMap
	WriteKey    string
}

var MTest *MSSQLTest

func (*MSSQLTest) SetUpDestination() {
	MTest.WriteKey = "YSQ3n267l1VQKGNbSuJE9fQbzON"
	MTest.Credentials = &mssql.CredentialsT{
		DBName:   "master",
		Password: "reallyStrongPwd123",
		User:     "SA",
		Host:     "localhost",
		SSLMode:  "disable",
		Port:     "54322",
	}
	MTest.EventsMap = testhelper.DefaultEventMap()

	testhelper.ConnectWithBackoff(func() (err error) {
		if MTest.DB, err = mssql.Connect(*MTest.Credentials); err != nil {
			err = fmt.Errorf("could not connect to warehouse mssql with error: %w", err)
			return
		}
		if err = MTest.DB.Ping(); err != nil {
			err = fmt.Errorf("could not connect to warehouse mssql while pinging with error: %w", err)
			return
		}
		return
	})
}

func TestMSSQLIntegration(t *testing.T) {
	whDestTest := &testhelper.WareHouseDestinationTest{
		Client: &client.Client{
			SQL:  MTest.DB,
			Type: client.SQLClient,
		},
		WriteKey:                 MTest.WriteKey,
		Schema:                   "mssql_wh_integration",
		EventsCountMap:           MTest.EventsMap,
		VerifyingTablesFrequency: testhelper.DefaultQueryFrequency,
	}

	whDestTest.Reset(warehouseutils.MSSQL, true)
	testhelper.SendEvents(t, whDestTest)
	testhelper.VerifyingDestination(t, whDestTest)

	whDestTest.Reset(warehouseutils.MSSQL, true)
	testhelper.SendModifiedEvents(t, whDestTest)
	testhelper.VerifyingDestination(t, whDestTest)
}

func TestMain(m *testing.M) {
	MTest = &MSSQLTest{}
	os.Exit(testhelper.Run(m, MTest))
}
