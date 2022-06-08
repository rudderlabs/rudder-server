package redshift_test

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/redshift"
	"github.com/rudderlabs/rudder-server/warehouse/testhelper"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"log"
	"os"
	"testing"
)

type RedshiftCredentials struct {
	Host        string `json:"host"`
	Port        string `json:"port"`
	Database    string `json:"database"`
	User        string `json:"user"`
	Password    string `json:"password"`
	BucketName  string `json:"bucketName"`
	AccessKeyID string `json:"accessKeyID"`
	AccessKey   string `json:"accessKey"`
}

type RedshiftTest struct {
	Credentials *RedshiftCredentials
	DB          *sql.DB
	EventsMap   testhelper.EventsCountMap
	WriteKey    string
}

var (
	RSTest *RedshiftTest
)

func credentials() (rsCredentials *RedshiftCredentials) {
	cred := os.Getenv("REDSHIFT_INTEGRATION_TEST_USER_CRED")
	if cred == "" {
		log.Panic("Error occurred while getting env variable REDSHIFT_INTEGRATION_TEST_USER_CRED")
	}

	var err error
	err = json.Unmarshal([]byte(cred), &rsCredentials)
	if err != nil {
		log.Panicf("Error occurred while unmarshalling redshift integration test credentials with error: %s", err.Error())
	}
	return
}

func (*RedshiftTest) EnhanceWorkspaceConfig(configMap map[string]string) {
	configMap["redshiftEventWriteKey"] = RSTest.WriteKey
	configMap["redshiftHost"] = RSTest.Credentials.Host
	configMap["redshiftPort"] = RSTest.Credentials.Port
	configMap["redshiftDatabase"] = RSTest.Credentials.Database
	configMap["redshiftUser"] = RSTest.Credentials.User
	configMap["redshiftPassword"] = RSTest.Credentials.Password
	configMap["redshiftBucketName"] = RSTest.Credentials.BucketName
	configMap["redshiftAccessKeyID"] = RSTest.Credentials.AccessKeyID
	configMap["redshiftAccessKey"] = RSTest.Credentials.AccessKey
}

func (*RedshiftTest) SetUpDestination() {
	RSTest.WriteKey = testhelper.RandString(27)
	RSTest.Credentials = credentials()
	RSTest.EventsMap = testhelper.DefaultEventMap()

	testhelper.ConnectWithBackoff(func() (err error) {
		RSTest.DB, err = redshift.Connect(redshift.RedshiftCredentialsT{
			Host:     RSTest.Credentials.Host,
			Port:     RSTest.Credentials.Port,
			DbName:   RSTest.Credentials.Database,
			Username: RSTest.Credentials.User,
			Password: RSTest.Credentials.Password,
		})
		if err != nil {
			err = fmt.Errorf("could not connect to warehouse redshift with error: %w", err)
			return
		}
		return
	})
}

func TestRedshiftIntegration(t *testing.T) {
	whDestTest := &testhelper.WareHouseDestinationTest{
		Client: &client.Client{
			SQL:  RSTest.DB,
			Type: client.SQLClient,
		},
		WriteKey:                 RSTest.WriteKey,
		Schema:                   "redshift_wh_integration",
		EventsCountMap:           RSTest.EventsMap,
		VerifyingTablesFrequency: testhelper.LongRunningQueryFrequency,
	}

	whDestTest.Reset(warehouseutils.RS, true)
	testhelper.SendEvents(t, whDestTest)
	testhelper.VerifyingDestination(t, whDestTest)

	whDestTest.Reset(warehouseutils.RS, true)
	testhelper.SendModifiedEvents(t, whDestTest)
	testhelper.VerifyingDestination(t, whDestTest)
}

func TestMain(m *testing.M) {
	RSTest = &RedshiftTest{}
	os.Exit(testhelper.Setup(m, RSTest))
}
