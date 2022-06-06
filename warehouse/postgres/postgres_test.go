package postgres_test

import (
	"database/sql"
	"fmt"
	"github.com/gofrs/uuid"
	"github.com/iancoleman/strcase"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/postgres"
	"github.com/rudderlabs/rudder-server/warehouse/testhelper"
	"os"
	"strings"
	"testing"
	"time"
)

type PostgresTest struct {
	Credentials        *postgres.CredentialsT
	DB                 *sql.DB
	EventsMap          testhelper.EventsCountMap
	WriteKey           string
	TableTestQueryFreq time.Duration
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
	PGTest.EventsMap = testhelper.EventsCountMap{
		"identifies": 1,
		"users":      1,
		"tracks":     1,
		"pages":      1,
		"screens":    1,
		"aliases":    1,
		"groups":     1,
		"gateway":    6,
		"batchRT":    8,
	}
	PGTest.TableTestQueryFreq = 100 * time.Millisecond

	var err error
	if PGTest.DB, err = postgres.Connect(*PGTest.Credentials); err != nil {
		panic(fmt.Errorf("could not connect to warehouse postgres with error: %s", err.Error()))
	}
	if err = PGTest.DB.Ping(); err != nil {
		panic(fmt.Errorf("could not connect to warehouse postgres while pinging with error: %s", err.Error()))
	}
	return
}

func TestPostgres(t *testing.T) {
	t.Parallel()

	randomness := strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", "")

	whDestTest := &testhelper.WareHouseDestinationTest{
		Client: &client.Client{
			SQL:  PGTest.DB,
			Type: client.SQLClient,
		},
		EventsCountMap:           PGTest.EventsMap,
		WriteKey:                 PGTest.WriteKey,
		UserId:                   fmt.Sprintf("userId_postgres_%s", randomness),
		Event:                    fmt.Sprintf("Product Track %s", randomness),
		Schema:                   "postgres_wh_integration",
		VerifyingTablesFrequency: PGTest.TableTestQueryFreq,
	}
	whDestTest.EventsCountMap[strcase.ToSnake(whDestTest.Event)] = 1

	testhelper.SendEvents(t, whDestTest)
	testhelper.VerifyingDestination(t, whDestTest)

	randomness = strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", "")
	whDestTest.UserId = fmt.Sprintf("userId_postgres_%s", randomness)
	whDestTest.Event = fmt.Sprintf("Product Track %s", randomness)
	whDestTest.EventsCountMap[strcase.ToSnake(whDestTest.Event)] = 1
	testhelper.SendModifiedEvents(t, whDestTest)
	testhelper.VerifyingDestination(t, whDestTest)
}

func TestMain(m *testing.M) {
	PGTest = &PostgresTest{}
	os.Exit(testhelper.Setup(m, PGTest))
}
