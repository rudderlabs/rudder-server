package redshift

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/cenkalti/backoff"
	"github.com/gofrs/uuid"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/testhelper"
	"github.com/rudderlabs/rudder-server/warehouse/testhelper/util"
	"log"
	"os"
	"strings"
	"testing"
	"time"
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
	WriteKey           string
	Credentials        *RedshiftCredentials
	DB                 *sql.DB
	EventsMap          testhelper.EventsCountMap
	TableTestQueryFreq time.Duration
}

var (
	runRedshiftTest bool
	RSTest          *RedshiftTest
)

func rsCredentials() (rsCredentials *RedshiftCredentials) {
	cred := os.Getenv("REDSHIFT_INTEGRATION_TEST_USER_CRED")
	if cred == "" {
		log.Panic("ERROR: ENV variable REDSHIFT_INTEGRATION_TEST_USER_CRED not found ")
	}

	var err error
	err = json.Unmarshal([]byte(cred), &rsCredentials)
	if err != nil {
		log.Panicf("Could not unmarshal REDSHIFT_INTEGRATION_TEST_USER_CRED with error: %s", err.Error())
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
	RSTest.WriteKey = util.RandString(27)
	RSTest.Credentials = rsCredentials()
	RSTest.EventsMap = testhelper.EventsCountMap{
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
	RSTest.TableTestQueryFreq = 5000 * time.Millisecond

	var err error

	operation := func() error {
		var err error
		RSTest.DB, err = Connect(RedshiftCredentialsT{
			Host:     RSTest.Credentials.Host,
			Port:     RSTest.Credentials.Port,
			DbName:   RSTest.Credentials.Database,
			Username: RSTest.Credentials.User,
			Password: RSTest.Credentials.Password,
		})
		return err
	}

	backoffWithMaxRetry := backoff.WithMaxRetries(backoff.NewConstantBackOff(1*time.Second), uint64(5))
	if err = backoff.Retry(operation, backoffWithMaxRetry); err != nil {
		log.Panicf("could not connect to warehouse redshift with error: %s", err.Error())
	}
	return
}

func TestRedshift(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test. Remove -short flag to run integration test.")
	}
	if runRedshiftTest == false {
		t.Skip("redshift integration skipped. use -redshiftintegration to add this test ")
	}

	t.Parallel()

	randomness := strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", "")

	whDestTest := &testhelper.WareHouseDestinationTest{
		Client: &client.Client{
			SQL:  RSTest.DB,
			Type: client.SQLClient,
		},
		EventsCountMap:           RSTest.EventsMap,
		WriteKey:                 RSTest.WriteKey,
		UserId:                   fmt.Sprintf("userId_redshift_%s", randomness),
		Schema:                   "redshift_wh_integration",
		VerifyingTablesFrequency: RSTest.TableTestQueryFreq,
	}

	testhelper.SendEvents(t, whDestTest)
	testhelper.VerifyingDestination(t, whDestTest)

	randomness = strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", "")
	whDestTest.UserId = fmt.Sprintf("userId_redshift_%s", randomness)
	testhelper.SendModifiedEvents(t, whDestTest)
	testhelper.VerifyingDestination(t, whDestTest)
}

func TestMain(m *testing.M) {
	flag.BoolVar(&runRedshiftTest, "redshiftintegration", false, "run redshift test")
	flag.Parse()

	RSTest = &RedshiftTest{}

	os.Exit(testhelper.Setup(m, RSTest))
}
