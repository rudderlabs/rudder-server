//go:build warehouse_integration && !sources_integration

package redshift_test

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/rudderlabs/rudder-server/warehouse/integrations/redshift"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"

	"github.com/rudderlabs/rudder-server/utils/timeutil"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/testhelper"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type TestHandle struct {
	DB       *sql.DB
	WriteKey string
	Schema   string
	Tables   []string
}

var handle *TestHandle

func (*TestHandle) VerifyConnection() error {
	credentials, err := testhelper.RedshiftCredentials()
	if err != nil {
		return err
	}
	return testhelper.WithConstantBackoff(func() (err error) {
		handle.DB, err = redshift.Connect(credentials)
		if err != nil {
			err = fmt.Errorf("could not connect to warehouse redshift with error: %w", err)
			return
		}
		return
	})
}

func TestRedshiftIntegration(t *testing.T) {
	t.Cleanup(func() {
		require.NoError(t, testhelper.WithConstantBackoff(func() (err error) {
			_, err = handle.DB.Exec(fmt.Sprintf(`DROP SCHEMA %q CASCADE;`, handle.Schema))
			return
		}), fmt.Sprintf("Failed dropping schema %s for Redshift", handle.Schema))
	})

	require.NoError(t, testhelper.SetConfig([]warehouseutils.KeyValue{
		{
			Key:   "Warehouse.redshift.dedupWindow",
			Value: true,
		},
		{
			Key:   "Warehouse.redshift.dedupWindowInHours",
			Value: 5,
		},
	}))

	warehouseTest := &testhelper.WareHouseTest{
		Client: &client.Client{
			SQL:  handle.DB,
			Type: client.SQLClient,
		},
		WriteKey:      handle.WriteKey,
		Schema:        handle.Schema,
		Tables:        handle.Tables,
		Provider:      warehouseutils.RS,
		SourceID:      "279L3gEKqwruBoKGsXZtSVX7vIy",
		DestinationID: "27SthahyhhqZE74HT4NTtNPl06V",
	}

	// Scenario 1
	warehouseTest.TimestampBeforeSendingEvents = timeutil.Now()
	warehouseTest.UserId = testhelper.GetUserId(warehouseutils.RS)

	sendEventsMap := testhelper.SendEventsMap()
	testhelper.SendEvents(t, warehouseTest, sendEventsMap)
	testhelper.SendEvents(t, warehouseTest, sendEventsMap)
	testhelper.SendEvents(t, warehouseTest, sendEventsMap)
	testhelper.SendIntegratedEvents(t, warehouseTest, sendEventsMap)

	testhelper.VerifyEventsInStagingFiles(t, warehouseTest, testhelper.StagingFilesEventsMap())
	testhelper.VerifyEventsInLoadFiles(t, warehouseTest, testhelper.LoadFilesEventsMap())
	testhelper.VerifyEventsInTableUploads(t, warehouseTest, testhelper.TableUploadsEventsMap())
	testhelper.VerifyEventsInWareHouse(t, warehouseTest, testhelper.WarehouseEventsMap())

	// Scenario 2
	warehouseTest.TimestampBeforeSendingEvents = timeutil.Now()
	warehouseTest.UserId = testhelper.GetUserId(warehouseutils.RS)

	sendEventsMap = testhelper.SendEventsMap()
	testhelper.SendModifiedEvents(t, warehouseTest, sendEventsMap)
	testhelper.SendModifiedEvents(t, warehouseTest, sendEventsMap)
	testhelper.SendModifiedEvents(t, warehouseTest, sendEventsMap)
	testhelper.SendIntegratedEvents(t, warehouseTest, sendEventsMap)

	testhelper.VerifyEventsInStagingFiles(t, warehouseTest, testhelper.StagingFilesEventsMap())
	testhelper.VerifyEventsInLoadFiles(t, warehouseTest, testhelper.LoadFilesEventsMap())
	testhelper.VerifyEventsInTableUploads(t, warehouseTest, testhelper.TableUploadsEventsMap())
	testhelper.VerifyEventsInWareHouse(t, warehouseTest, testhelper.WarehouseEventsMap())

	testhelper.VerifyWorkspaceIDInStats(t)
}

func TestRedshiftConfigurationValidation(t *testing.T) {
	configurations := testhelper.PopulateTemplateConfigurations()
	destination := backendconfig.DestinationT{
		ID: "27SthahyhhqZE74HT4NTtNPl06V",
		Config: map[string]interface{}{
			"host":             configurations["redshiftHost"],
			"port":             configurations["redshiftPort"],
			"database":         configurations["redshiftDbName"],
			"user":             configurations["redshiftUsername"],
			"password":         configurations["redshiftPassword"],
			"bucketName":       configurations["redshiftBucketName"],
			"accessKeyID":      configurations["redshiftAccessKeyID"],
			"accessKey":        configurations["redshiftAccessKey"],
			"prefix":           "",
			"namespace":        configurations["redshiftNamespace"],
			"syncFrequency":    "30",
			"enableSSE":        false,
			"useRudderStorage": false,
		},
		DestinationDefinition: backendconfig.DestinationDefinitionT{
			ID:          "1UVZiJF7OgLaiIY2Jts8XOQE3M6",
			Name:        "RS",
			DisplayName: "Redshift",
		},
		Name:       "redshift-demo",
		Enabled:    true,
		RevisionID: "29HgOWobrn0RYZLpaSwPIbN2987",
	}
	testhelper.VerifyingConfigurationTest(t, destination)
}

func TestMain(m *testing.M) {
	_, exists := os.LookupEnv(testhelper.RedshiftIntegrationTestCredentials)
	if !exists {
		log.Println("Skipping Redshift Test as the Test credentials does not exists.")
		return
	}

	handle = &TestHandle{
		WriteKey: "JAAwdCxmM8BIabKERsUhPNmMmdf",
		Schema:   testhelper.Schema(warehouseutils.RS, testhelper.RedshiftIntegrationTestSchema),
		Tables:   []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"},
	}
	os.Exit(testhelper.Run(m, handle))
}
