package clickhouse

import (
	"database/sql"
	"fmt"
	"github.com/gofrs/uuid"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/testhelper"
	"github.com/rudderlabs/rudder-server/warehouse/testhelper/util"
	"os"
	"strings"
	"testing"
	"time"
)

type ClickHouseTest struct {
	WriteKey           string
	Credentials        *CredentialsT
	DB                 *sql.DB
	EventsMap          testhelper.EventsCountMap
	TableTestQueryFreq time.Duration
}

var (
	CHTest *ClickHouseTest
)

func (*ClickHouseTest) EnhanceWorkspaceConfig(configMap map[string]string) {
	configMap["clickHouseWriteKey"] = CHTest.WriteKey
	configMap["clickHousePort"] = CHTest.Credentials.Port
}

func (*ClickHouseTest) SetUpDestination() {
	CHTest.WriteKey = util.RandString(27)
	CHTest.Credentials = &CredentialsT{
		Host:          "localhost",
		User:          "rudder",
		Password:      "rudder-password",
		DBName:        "rudderdb",
		Secure:        "false",
		SkipVerify:    "true",
		TLSConfigName: "",
		Port:          "54321",
	}
	CHTest.EventsMap = testhelper.EventsCountMap{
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
	CHTest.TableTestQueryFreq = 100 * time.Millisecond

	var err error
	if CHTest.DB, err = Connect(*CHTest.Credentials, true); err != nil {
		panic(fmt.Errorf("could not connect to warehouse clickhouse with error: %s", err.Error()))
	}
	if err = CHTest.DB.Ping(); err != nil {
		panic(fmt.Errorf("could not connect to warehouse clickhouse while pinging with error: %s", err.Error()))
	}
	return
}

func TestClickHouse(t *testing.T) {
	t.Parallel()

	randomness := strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", "")

	whDestTest := &testhelper.WareHouseDestinationTest{
		Client: &client.Client{
			SQL:  CHTest.DB,
			Type: client.SQLClient,
		},
		EventsCountMap:           CHTest.EventsMap,
		WriteKey:                 CHTest.WriteKey,
		UserId:                   fmt.Sprintf("userId_clickhouse_%s", randomness),
		Schema:                   "rudderdb",
		VerifyingTablesFrequency: CHTest.TableTestQueryFreq,
	}
	testhelper.SendEvents(t, whDestTest)
	testhelper.VerifyingDestination(t, whDestTest)

	randomness = strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", "")
	whDestTest.UserId = fmt.Sprintf("userId_clickhouse_%s", randomness)
	testhelper.SendModifiedEvents(t, whDestTest)
	testhelper.VerifyingDestination(t, whDestTest)
}

func TestMain(m *testing.M) {
	CHTest = &ClickHouseTest{}
	os.Exit(testhelper.Setup(m, CHTest))
}
