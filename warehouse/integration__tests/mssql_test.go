package integration__tests

import (
	"fmt"
	"github.com/gofrs/uuid"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/integration__tests/testhelper"
	"strings"
	"testing"
	"time"

	"github.com/rudderlabs/rudder-server/warehouse/mssql"
)

// SetupMSSQL setup warehouse mssql destination
func SetupMSSQL() (mssqlTest *testhelper.MSSQLTest) {
	mssqlTest = &testhelper.MSSQLTest{
		WriteKey: testhelper.RandString(27),
		Credentials: &mssql.CredentialsT{
			DBName:   "master",
			Password: "reallyStrongPwd123",
			User:     "SA",
			Host:     "localhost",
			SSLMode:  "disable",
			Port:     "54322",
		},
		EventsMap: testhelper.EventsCountMap{
			"identifies":    1,
			"users":         1,
			"tracks":        1,
			"product_track": 1,
			"pages":         1,
			"screens":       1,
			"aliases":       1,
			"groups":        1,
			"gateway":       6,
			"batchRT":       8,
		},
		TableTestQueryFreq: 100 * time.Millisecond,
	}

	var err error
	if mssqlTest.DB, err = mssql.Connect(*mssqlTest.Credentials); err != nil {
		panic(fmt.Errorf("could not connect to warehouse mssql with error: %s", err.Error()))
	}
	if err = mssqlTest.DB.Ping(); err != nil {
		panic(fmt.Errorf("could not connect to warehouse mssql while pinging with error: %s", err.Error()))
	}
	return
}

func TestMSSQL(t *testing.T) {
	t.Parallel()

	MssqlTest := MSSQLTest
	randomness := strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", "")

	whDestTest := &testhelper.WareHouseDestinationTest{
		Client: &client.Client{
			SQL:  MssqlTest.DB,
			Type: client.SQLClient,
		},
		EventsCountMap:     MssqlTest.EventsMap,
		WriteKey:           MssqlTest.WriteKey,
		UserId:             fmt.Sprintf("userId_mssql_%s", randomness),
		Schema:             "mssql_wh_integration",
		TableTestQueryFreq: MssqlTest.TableTestQueryFreq,
	}
	sendEvents(whDestTest)
	destinationTest(t, whDestTest)

	randomness = strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", "")
	whDestTest.UserId = fmt.Sprintf("userId_mssql_%s", randomness)
	sendUpdatedEvents(whDestTest)
	destinationTest(t, whDestTest)
}
