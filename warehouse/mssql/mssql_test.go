package mssql_test

import (
	"database/sql"
	"fmt"
	"github.com/gofrs/uuid"
	"github.com/iancoleman/strcase"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/mssql"
	"github.com/rudderlabs/rudder-server/warehouse/testhelper"
	"os"
	"strings"
	"testing"
	"time"
)

type MSSQLTest struct {
	Credentials        *mssql.CredentialsT
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
	MTest.WriteKey = testhelper.RandString(27)
	MTest.Credentials = &mssql.CredentialsT{
		DBName:   "master",
		Password: "reallyStrongPwd123",
		User:     "SA",
		Host:     "localhost",
		SSLMode:  "disable",
		Port:     "54322",
	}
	MTest.EventsMap = testhelper.EventsCountMap{
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
	MTest.TableTestQueryFreq = 100 * time.Millisecond

	var err error
	if MTest.DB, err = mssql.Connect(*MTest.Credentials); err != nil {
		panic(fmt.Errorf("could not connect to warehouse mssql with error: %s", err.Error()))
	}
	if err = MTest.DB.Ping(); err != nil {
		panic(fmt.Errorf("could not connect to warehouse mssql while pinging with error: %s", err.Error()))
	}
	return
}

func TestMSSQLIntegration(t *testing.T) {
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
		Event:                    fmt.Sprintf("Product Track %s", randomness),
		Schema:                   "mssql_wh_integration",
		VerifyingTablesFrequency: MTest.TableTestQueryFreq,
	}
	whDestTest.Tables = []string{"identifies", "users", "tracks", strcase.ToSnake(whDestTest.Event), "pages", "screens", "aliases", "groups"}
	whDestTest.PrimaryKeys = []string{"user_id", "id", "user_id", "user_id", "user_id", "user_id", "user_id", "user_id"}
	whDestTest.EventsCountMap[strcase.ToSnake(whDestTest.Event)] = 1

	testhelper.SendEvents(t, whDestTest)
	testhelper.VerifyingDestination(t, whDestTest)

	randomness = strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", "")
	whDestTest.UserId = fmt.Sprintf("userId_mssql_%s", randomness)
	whDestTest.Event = fmt.Sprintf("Product Track %s", randomness)
	whDestTest.EventsCountMap[strcase.ToSnake(whDestTest.Event)] = 1
	whDestTest.Tables = []string{"identifies", "users", "tracks", strcase.ToSnake(whDestTest.Event), "pages", "screens", "aliases", "groups"}
	whDestTest.PrimaryKeys = []string{"user_id", "id", "user_id", "user_id", "user_id", "user_id", "user_id", "user_id"}
	testhelper.SendModifiedEvents(t, whDestTest)
	testhelper.VerifyingDestination(t, whDestTest)
}

func TestMain(m *testing.M) {
	MTest = &MSSQLTest{}
	os.Exit(testhelper.Setup(m, MTest))
}
