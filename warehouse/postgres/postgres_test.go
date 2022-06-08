package postgres_test

import (
	"database/sql"
	"fmt"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/postgres"
	"github.com/rudderlabs/rudder-server/warehouse/testhelper"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"os"
	"testing"
)

type PostgresTest struct {
	Credentials *postgres.CredentialsT
	DB          *sql.DB
	EventsMap   testhelper.EventsCountMap
	WriteKey    string
}

var (
	PGTest *PostgresTest
)

func (*PostgresTest) EnhanceWorkspaceConfig(configMap map[string]string) {
	configMap["postgresWriteKey"] = PGTest.WriteKey
	configMap["postgresPort"] = PGTest.Credentials.Port
}

func (*PostgresTest) SetUpDestination() {
	PGTest.WriteKey = testhelper.RandString(27)
	PGTest.Credentials = &postgres.CredentialsT{
		DBName:   "rudderdb",
		Password: "rudder-password",
		User:     "rudder",
		Host:     "localhost",
		SSLMode:  "disable",
		Port:     "54320",
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
	os.Exit(testhelper.Setup(m, PGTest))
}
