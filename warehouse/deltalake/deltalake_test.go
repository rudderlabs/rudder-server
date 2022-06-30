//go:build whintegration
// +build whintegration

package deltalake_test

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/rudderlabs/rudder-server/warehouse/client"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/warehouse/deltalake"
	"github.com/rudderlabs/rudder-server/warehouse/deltalake/databricks"
	"github.com/rudderlabs/rudder-server/warehouse/testhelper"
)

type DeltalakeCredentials struct {
	Host          string `json:"host"`
	Port          string `json:"port"`
	Path          string `json:"path"`
	Token         string `json:"token"`
	AccountName   string `json:"accountName"`
	AccountKey    string `json:"accountKey"`
	ContainerName string `json:"containerName"`
}

type DeltalakeTest struct {
	Credentials *DeltalakeCredentials
	DB          *databricks.DBHandleT
	EventsMap   testhelper.EventsCountMap
	WriteKey    string
}

var DLTest *DeltalakeTest

func credentials() (credentials *DeltalakeCredentials) {
	cred := os.Getenv(testhelper.DatabricksIntegrationTestUserCred)
	if cred == "" {
		log.Panicf("Error occurred while getting env variable %s", testhelper.DatabricksIntegrationTestUserCred)
	}

	var err error
	err = json.Unmarshal([]byte(cred), &credentials)
	if err != nil {
		log.Panicf("Error occurred while unmarshalling deltalake integration test credentials with error: %s", err.Error())
	}
	return
}

// Do cleanup once the setup is completed.
func (*DeltalakeTest) SetUpDestination() {
	DLTest.WriteKey = "sToFgoilA0U1WxNeW1gdgUVDsEW"
	DLTest.Credentials = credentials()
	DLTest.EventsMap = testhelper.DefaultEventMap()

	testhelper.ConnectWithBackoff(func() (err error) {
		DLTest.DB, err = deltalake.Connect(&databricks.CredentialsT{
			Host:  DLTest.Credentials.Host,
			Port:  DLTest.Credentials.Port,
			Path:  DLTest.Credentials.Path,
			Token: DLTest.Credentials.Token,
		}, 0)
		if err != nil {
			err = fmt.Errorf("could not connect to warehouse deltalake with error: %w", err)
			return
		}
		return
	})
}

func TestDeltalakeIntegration(t *testing.T) {
	t.Skip()

	verify := func() {
		whDestTest := &testhelper.WareHouseDestinationTest{
			Client: &client.Client{
				DBHandleT: DLTest.DB,
				Type:      client.DBClient,
			},
			WriteKey:                 DLTest.WriteKey,
			Schema:                   "deltalake_wh_integration",
			EventsCountMap:           DLTest.EventsMap,
			VerifyingTablesFrequency: testhelper.LongRunningQueryFrequency,
		}

		whDestTest.Reset(warehouseutils.DELTALAKE, true)
		testhelper.SendEvents(t, whDestTest)
		testhelper.VerifyingDestination(t, whDestTest)

		whDestTest.Reset(warehouseutils.DELTALAKE, true)
		testhelper.SendModifiedEvents(t, whDestTest)
		testhelper.VerifyingDestination(t, whDestTest)
	}

	// Merge mode
	t.Run("With Merge Mode With Partition", func(t *testing.T) {
		config.SetString("Warehouse.deltalake.loadTableStrategy", "MERGE")
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

	// Append mode
	t.Run("With Append Mode With Partition", func(t *testing.T) {
		config.SetString("Warehouse.deltalake.loadTableStrategy", "APPEND")
		config.SetBool("Warehouse.deltalake.enablePartition", true)
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
	DLTest = &DeltalakeTest{}
	os.Exit(testhelper.Run(m, DLTest))
}
