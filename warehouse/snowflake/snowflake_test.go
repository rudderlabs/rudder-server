package snowflake_test

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/cenkalti/backoff"
	"github.com/gofrs/uuid"
	"github.com/iancoleman/strcase"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/snowflake"
	"github.com/rudderlabs/rudder-server/warehouse/testhelper"
	"log"
	"os"
	"strings"
	"testing"
	"time"
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
	WriteKey           string
	Credentials        *SnowflakeCredentials
	DB                 *sql.DB
	EventsMap          testhelper.EventsCountMap
	TableTestQueryFreq time.Duration
}

var (
	SFTest *SnowflakeTest
)

func sfCredentials() (sfCredentials *SnowflakeCredentials) {
	cred := os.Getenv("SNOWFLAKE_INTEGRATION_TEST_USER_CRED")
	if cred == "" {
		log.Panic("ERROR: ENV variable SNOWFLAKE_INTEGRATION_TEST_USER_CRED not found ")
	}

	var err error
	err = json.Unmarshal([]byte(cred), &sfCredentials)
	if err != nil {
		log.Panicf("Could not unmarshal SNOWFLAKE_INTEGRATION_TEST_USER_CRED with error: %s", err.Error())
	}

	return
}

func (*SnowflakeTest) EnhanceWorkspaceConfig(configMap map[string]string) {
	configMap["snowflakeEventWriteKey"] = SFTest.WriteKey
	configMap["snowflakeAccount"] = SFTest.Credentials.Account
	configMap["snowflakeDatabase"] = SFTest.Credentials.Database
	configMap["snowflakeWarehouse"] = SFTest.Credentials.Warehouse
	configMap["snowflakeUser"] = SFTest.Credentials.User
	configMap["snowflakePassword"] = SFTest.Credentials.Password
	configMap["snowflakeBucketName"] = SFTest.Credentials.BucketName
	configMap["snowflakeAccesskeyID"] = SFTest.Credentials.AccessKeyID
	configMap["snowflakeAccesskey"] = SFTest.Credentials.AccessKey
}

func (*SnowflakeTest) SetUpDestination() {
	SFTest.WriteKey = testhelper.RandString(27)
	SFTest.Credentials = sfCredentials()
	SFTest.EventsMap = testhelper.EventsCountMap{
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
	SFTest.TableTestQueryFreq = 100 * time.Millisecond

	var err error

	operation := func() error {
		var err error
		SFTest.DB, err = snowflake.Connect(snowflake.SnowflakeCredentialsT{
			Account:  SFTest.Credentials.Account,
			WHName:   SFTest.Credentials.Warehouse,
			DBName:   SFTest.Credentials.Database,
			Username: SFTest.Credentials.User,
			Password: SFTest.Credentials.Password,
		})
		return err
	}

	backoffWithMaxRetry := backoff.WithMaxRetries(backoff.NewConstantBackOff(1*time.Second), uint64(5))
	if err = backoff.Retry(operation, backoffWithMaxRetry); err != nil {
		log.Panicf("could not connect to warehouse snowflake with error: %s", err.Error())
	}
	return
}

func TestSnowflakeIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test. Remove -short flag to run integration test.")
	}

	t.Parallel()

	randomness := strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", "")

	whDestTest := &testhelper.WareHouseDestinationTest{
		Client: &client.Client{
			SQL:  SFTest.DB,
			Type: client.SQLClient,
		},
		EventsCountMap:           SFTest.EventsMap,
		WriteKey:                 SFTest.WriteKey,
		UserId:                   fmt.Sprintf("userId_snowflake_%s", randomness),
		Event:                    fmt.Sprintf("Product Track %s", randomness),
		Schema:                   "SNOWFLAKE_WH_INTEGRATION",
		VerifyingTablesFrequency: SFTest.TableTestQueryFreq,
	}
	whDestTest.EventsCountMap[strcase.ToSnake(whDestTest.Event)] = 1

	testhelper.SendEvents(t, whDestTest)
	testhelper.VerifyingDestination(t, whDestTest)

	randomness = strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", "")
	whDestTest.UserId = fmt.Sprintf("userId_snowflake_%s", randomness)
	whDestTest.Event = fmt.Sprintf("Product Track %s", randomness)
	whDestTest.EventsCountMap[strcase.ToSnake(whDestTest.Event)] = 1
	testhelper.SendModifiedEvents(t, whDestTest)
	testhelper.VerifyingDestination(t, whDestTest)
}

func TestMain(m *testing.M) {
	SFTest = &SnowflakeTest{}
	os.Exit(testhelper.Setup(m, SFTest))
}
