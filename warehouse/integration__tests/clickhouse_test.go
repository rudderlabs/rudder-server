package integration__tests

import (
	"fmt"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/integration__tests/testhelper"
	"testing"
	"time"

	"github.com/rudderlabs/rudder-server/warehouse/clickhouse"
)

// SetupClickHouse setup warehouse clickhouse destination
func SetupClickHouse() (chTest *testhelper.ClickHouseTest) {
	chTest = &testhelper.ClickHouseTest{
		WriteKey: testhelper.RandString(27),
		Credentials: &clickhouse.CredentialsT{
			Host:          "localhost",
			User:          "rudder",
			Password:      "rudder-password",
			DBName:        "rudderdb",
			Secure:        "false",
			SkipVerify:    "true",
			TLSConfigName: "",
			Port:          "54321",
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
	if chTest.DB, err = clickhouse.Connect(*chTest.Credentials, true); err != nil {
		panic(fmt.Errorf("could not connect to warehouse clickhouse with error: %s", err.Error()))
	}
	if err = chTest.DB.Ping(); err != nil {
		panic(fmt.Errorf("could not connect to warehouse clickhouse while pinging with error: %s", err.Error()))
	}
	return
}

func TestClickHouse(t *testing.T) {
	t.Parallel()

	chTest := CHTest

	whDestTest := &testhelper.WareHouseDestinationTest{
		Client: &client.Client{
			SQL:  chTest.DB,
			Type: client.SQLClient,
		},
		EventsCountMap:     chTest.EventsMap,
		WriteKey:           chTest.WriteKey,
		UserId:             "userId_clickhouse",
		Schema:             "rudderdb",
		TableTestQueryFreq: chTest.TableTestQueryFreq,
	}
	sendEvents(whDestTest)
	destinationTest(t, whDestTest)

	whDestTest.UserId = "userId_clickhouse_1"
	sendUpdatedEvents(whDestTest)
	destinationTest(t, whDestTest)
}
