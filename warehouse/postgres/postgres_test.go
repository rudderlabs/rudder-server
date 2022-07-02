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

type PostgresTest struct {
	Credentials *postgres.CredentialsT
	DB          *sql.DB
	EventsMap   testhelper.EventsCountMap
	WriteKey    string
}

var PGTest *PostgresTest

func (*PostgresTest) SetUpDestination() {
	PGTest.WriteKey = "kwzDkh9h2fhfUVuS9jZ8uVbhV3v"
	PGTest.Credentials = &postgres.CredentialsT{
		DBName:   "rudderdb",
		Password: "rudder-password",
		User:     "rudder",
		Host:     "postgres",
		SSLMode:  "disable",
		Port:     "5432",
	}
	PGTest.EventsMap = testhelper.DefaultEventMap()

	testhelper.ConnectWithBackoff(func() (err error) {
		if PGTest.DB, err = postgres.Connect(*PGTest.Credentials); err != nil {
			err = fmt.Errorf("could not connect to warehouse postgres with error: %w", err)
			return
		}
		if err = PGTest.DB.Ping(); err != nil {
			err = fmt.Errorf("could not connect to warehouse postgres while pinging with error: %w", err)
			return
		}
		return
	})
}

func TestPostgresIntegration(t *testing.T) {
	whDestTest := &testhelper.WareHouseDestinationTest{
		Client: &client.Client{
			SQL:  PGTest.DB,
			Type: client.SQLClient,
		},
		WriteKey:                 PGTest.WriteKey,
		Schema:                   "postgres_wh_integration",
		EventsCountMap:           PGTest.EventsMap,
		VerifyingTablesFrequency: testhelper.DefaultQueryFrequency,
	}

	whDestTest.Reset(warehouseutils.POSTGRES, true)
	testhelper.SendEvents(t, whDestTest)
	testhelper.VerifyingDestination(t, whDestTest)

	whDestTest.Reset(warehouseutils.POSTGRES, true)
	testhelper.SendModifiedEvents(t, whDestTest)
	testhelper.VerifyingDestination(t, whDestTest)
}

func TestMain(m *testing.M) {
	PGTest = &PostgresTest{}
	os.Exit(testhelper.Run(m, PGTest))
}
