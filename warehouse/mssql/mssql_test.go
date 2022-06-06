package mssql

import (
	"database/sql"
	"fmt"
	"github.com/gofrs/uuid"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/testhelper"
	"github.com/rudderlabs/rudder-server/warehouse/testhelper/util"
	"strings"
	"testing"
	"time"
)

type MSSQLTest struct {
	Credentials        *CredentialsT
	DB                 *sql.DB
	EventsMap          testhelper.EventsCountMap
	WriteKey           string
	TableTestQueryFreq time.Duration
}

var (
	MTest *MSSQLTest
)

func (*MSSQLTest) EnhanceWorkspaceConfig(configMap map[string]string) {
	configMap["mssqlWriteKey"] = MTest.WriteKey
	configMap["mssqlPort"] = MTest.Credentials.Port
}

func (*MSSQLTest) SetUpDestination() {
	MTest.WriteKey = util.RandString(27)
	MTest.Credentials = &CredentialsT{
		DBName:   "master",
		Password: "reallyStrongPwd123",
		User:     "SA",
		Host:     "localhost",
		SSLMode:  "disable",
		Port:     "54322",
	}
	MTest.EventsMap = testhelper.EventsCountMap{
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
	}
	MTest.TableTestQueryFreq = 100 * time.Millisecond

	var err error
	if MTest.DB, err = Connect(*MTest.Credentials); err != nil {
		panic(fmt.Errorf("could not connect to warehouse mssql with error: %s", err.Error()))
	}
	if err = MTest.DB.Ping(); err != nil {
		panic(fmt.Errorf("could not connect to warehouse mssql while pinging with error: %s", err.Error()))
	}
	return
}

func TestMSSQL(t *testing.T) {
	t.Parallel()

	randomness := strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", "")

	whDestTest := &testhelper.WareHouseDestinationTest{
		Client: &client.Client{
			SQL:  MTest.DB,
			Type: client.SQLClient,
		},
		EventsCountMap:           MTest.EventsMap,
		WriteKey:                 MTest.WriteKey,
		UserId:                   fmt.Sprintf("userId_mssql_%s", randomness),
		Schema:                   "mssql_wh_integration",
		VerifyingTablesFrequency: MTest.TableTestQueryFreq,
	}
	testhelper.SendEvents(t, whDestTest)
	testhelper.VerifyingDestination(t, whDestTest)

	randomness = strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", "")
	whDestTest.UserId = fmt.Sprintf("userId_mssql_%s", randomness)
	testhelper.SendModifiedEvents(t, whDestTest)
	testhelper.VerifyingDestination(t, whDestTest)
}
