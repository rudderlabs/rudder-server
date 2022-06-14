package snowflake_test

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/snowflake"
	"github.com/rudderlabs/rudder-server/warehouse/testhelper"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"log"
	"os"
	"testing"
)

type SnowflakeCredentials struct {
	Account     string `json:"account"`
	Warehouse   string `json:"warehouse"`
	Database    string `json:"database"`
	User        string `json:"user"`
	Password    string `json:"password"`
	BucketName  string `json:"bucketName"`
	AccessKeyID string `json:"accessKeyID"`
	AccessKey   string `json:"accessKey"`
}

type SnowflakeTest struct {
	Credentials *SnowflakeCredentials
	DB          *sql.DB
	EventsMap   testhelper.EventsCountMap
	WriteKey    string
}

var (
	SFTest *SnowflakeTest
)

func credentials() (sfCredentials *SnowflakeCredentials) {
	cred := os.Getenv("SNOWFLAKE_INTEGRATION_TEST_USER_CRED")
	if cred == "" {
		log.Panic("Error occurred while getting env variable SNOWFLAKE_INTEGRATION_TEST_USER_CRED")
	}

	var err error
	err = json.Unmarshal([]byte(cred), &sfCredentials)
	if err != nil {
		log.Panicf("Error occurred while unmarshalling snowflake integration test credentials with error: %s", err.Error())
	}
	return
}

func (*SnowflakeTest) SetUpDestination() {
	SFTest.WriteKey = "2eSJyYtqwcFiUILzXv2fcNIrWO7"
	SFTest.Credentials = credentials()
	SFTest.EventsMap = testhelper.DefaultEventMap()

	testhelper.ConnectWithBackoff(func() (err error) {
		SFTest.DB, err = snowflake.Connect(snowflake.SnowflakeCredentialsT{
			Account:  SFTest.Credentials.Account,
			WHName:   SFTest.Credentials.Warehouse,
			DBName:   SFTest.Credentials.Database,
			Username: SFTest.Credentials.User,
			Password: SFTest.Credentials.Password,
		})
		if err != nil {
			err = fmt.Errorf("could not connect to warehouse snowflake with error: %w", err)
			return
		}
		return
	})
}

func TestSnowflakeIntegration(t *testing.T) {
	whDestTest := &testhelper.WareHouseDestinationTest{
		Client: &client.Client{
			SQL:  SFTest.DB,
			Type: client.SQLClient,
		},
		WriteKey:                 SFTest.WriteKey,
		Schema:                   "SNOWFLAKE_WH_INTEGRATION",
		EventsCountMap:           SFTest.EventsMap,
		VerifyingTablesFrequency: testhelper.LongRunningQueryFrequency,
	}

	whDestTest.Reset(warehouseutils.SNOWFLAKE, true)
	testhelper.SendEvents(t, whDestTest)
	testhelper.VerifyingDestination(t, whDestTest)

	whDestTest.Reset(warehouseutils.SNOWFLAKE, true)
	testhelper.SendModifiedEvents(t, whDestTest)
	testhelper.VerifyingDestination(t, whDestTest)
}

func TestMain(m *testing.M) {
	SFTest = &SnowflakeTest{}
	os.Exit(testhelper.Run(m, SFTest))
}
