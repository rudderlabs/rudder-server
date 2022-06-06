package deltalake_test

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/cenkalti/backoff"
	"github.com/gofrs/uuid"
	"github.com/iancoleman/strcase"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/deltalake"
	"github.com/rudderlabs/rudder-server/warehouse/deltalake/databricks"
	"github.com/rudderlabs/rudder-server/warehouse/testhelper"
	"log"
	"os"
	"strings"
	"testing"
	"time"
)

type DeltalakeTest struct {
	WriteKey           string
	Credentials        *DeltalakeCredentials
	DB                 *databricks.DBHandleT
	EventsMap          testhelper.EventsCountMap
	TableTestQueryFreq time.Duration
}

type DeltalakeCredentials struct {
	Host          string `json:"host"`
	Port          string `json:"port"`
	Path          string `json:"path"`
	Token         string `json:"token"`
	AccountName   string `json:"accountName"`
	AccountKey    string `json:"accountKey"`
	ContainerName string `json:"containerName"`
}

var (
	runDeltalakeTest bool
	DLTest           *DeltalakeTest
)

func deltalakeCredentials() (credentials *DeltalakeCredentials) {
	cred := os.Getenv("DATABRICKS_INTEGRATION_TEST_USER_CRED")
	if cred == "" {
		log.Panic("ERROR: ENV variable DATABRICKS_INTEGRATION_TEST_USER_CRED not found ")
	}

	var err error
	err = json.Unmarshal([]byte(cred), &credentials)
	if err != nil {
		log.Panicf("Could not unmarshal REDSHIFT_INTEGRATION_TEST_USER_CRED with error: %s", err.Error())
	}

	return
}

func (*DeltalakeTest) EnhanceWorkspaceConfig(configMap map[string]string) {
	configMap["deltalakeEventWriteKey"] = DLTest.WriteKey
	configMap["deltalakeHost"] = DLTest.Credentials.Host
	configMap["deltalakePort"] = DLTest.Credentials.Port
	configMap["deltalakePath"] = DLTest.Credentials.Path
	configMap["deltalakeToken"] = DLTest.Credentials.Token
	configMap["deltalakeAccountName"] = DLTest.Credentials.AccountName
	configMap["deltalakeAccountKey"] = DLTest.Credentials.AccountKey
	configMap["deltalakeContainerName"] = DLTest.Credentials.ContainerName
}

func (*DeltalakeTest) SetUpDestination() {
	DLTest.WriteKey = testhelper.RandString(27)
	DLTest.Credentials = deltalakeCredentials()
	DLTest.EventsMap = testhelper.EventsCountMap{
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
	DLTest.TableTestQueryFreq = 5000 * time.Millisecond

	var err error

	operation := func() error {
		var err error
		DLTest.DB, err = deltalake.Connect(&databricks.CredentialsT{
			Host:  DLTest.Credentials.Host,
			Port:  DLTest.Credentials.Port,
			Path:  DLTest.Credentials.Path,
			Token: DLTest.Credentials.Token,
		}, 0)
		return err
	}

	backoffWithMaxRetry := backoff.WithMaxRetries(backoff.NewConstantBackOff(1*time.Second), uint64(5))
	if err = backoff.Retry(operation, backoffWithMaxRetry); err != nil {
		log.Panicf("could not connect to warehouse deltalake with error: %s", err.Error())
	}
	return
}

func TestDeltalakeIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test. Remove -short flag to run integration test.")
	}
	if runDeltalakeTest == false {
		t.Skip("deltalake integration skipped. use -redshiftintegration to add this test ")
	}

	t.Parallel()

	verify := func() {
		randomness := strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", "")

		whDestTest := &testhelper.WareHouseDestinationTest{
			Client: &client.Client{
				DBHandleT: DLTest.DB,
				Type:      client.DBClient,
			},
			EventsCountMap:           DLTest.EventsMap,
			WriteKey:                 DLTest.WriteKey,
			UserId:                   fmt.Sprintf("userId_deltalake_%s", randomness),
			Event:                    fmt.Sprintf("Product Track %s", randomness),
			Schema:                   "deltalake_wh_integration",
			VerifyingTablesFrequency: DLTest.TableTestQueryFreq,
		}
		whDestTest.EventsCountMap[strcase.ToSnake(whDestTest.Event)] = 1

		testhelper.SendEvents(t, whDestTest)
		testhelper.VerifyingDestination(t, whDestTest)

		randomness = strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", "")
		whDestTest.UserId = fmt.Sprintf("userId_deltalake_%s", randomness)
		whDestTest.Event = fmt.Sprintf("Product Track %s", randomness)
		whDestTest.EventsCountMap[strcase.ToSnake(whDestTest.Event)] = 1
		testhelper.SendModifiedEvents(t, whDestTest)
		testhelper.VerifyingDestination(t, whDestTest)
	}

	t.Run("With Merge Mode With Partition", func(t *testing.T) {
		config.SetString("Warehouse.deltalake.loadTableStrategy", "MERGE")
		config.SetBool("Warehouse.deltalake.enablePartition", true)
		deltalake.Init()

		verify()
	})
	t.Run("With Append Mode With Partition", func(t *testing.T) {
		config.SetString("Warehouse.deltalake.loadTableStrategy", "APPEND")
		config.SetBool("Warehouse.deltalake.enablePartition", true)
		deltalake.Init()

		verify()
	})

	t.Run("With Merge Mode Without Partition", func(t *testing.T) {
		config.SetString("Warehouse.deltalake.loadTableStrategy", "MERGE")
		config.SetBool("Warehouse.deltalake.enablePartition", false)
		deltalake.Init()

		verify()
	})
	t.Run("With Append Mode Without Partition", func(t *testing.T) {
		config.SetString("Warehouse.deltalake.loadTableStrategy", "APPEND")
		config.SetBool("Warehouse.deltalake.enablePartition", false)
		deltalake.Init()

		verify()
	})
}

func TestMain(m *testing.M) {
	flag.BoolVar(&runDeltalakeTest, "deltalakeintegration", false, "run deltalake test")
	flag.Parse()

	DLTest = &DeltalakeTest{}

	os.Exit(testhelper.Setup(m, DLTest))
}
