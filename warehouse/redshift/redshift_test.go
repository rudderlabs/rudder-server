//go:build warehouse_integration && !sources_integration

package redshift_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/validations"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"

	"github.com/rudderlabs/rudder-server/utils/timeutil"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/redshift"
	"github.com/rudderlabs/rudder-server/warehouse/testhelper"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestRedshiftIntegration(t *testing.T) {
	t.Parallel()

	if _, exists := os.LookupEnv(testhelper.RedshiftIntegrationTestCredentials); !exists {
		t.Skipf("Skipping %s as %s is not set", t.Name(), testhelper.RedshiftIntegrationTestCredentials)
	}

	redshift.Init()

	credentials, err := testhelper.RedshiftCredentials()
	require.NoError(t, err)

	db, err := redshift.Connect(credentials)
	require.NoError(t, err)

	schema := testhelper.Schema(warehouseutils.RS, testhelper.RedshiftIntegrationTestSchema)

	t.Cleanup(func() {
		require.NoError(t, testhelper.WithConstantBackoff(func() (err error) {
			_, err = db.Exec(fmt.Sprintf(`DROP SCHEMA %q CASCADE;`, schema))
			return
		}), fmt.Sprintf("Failed dropping schema %s for Redshift", schema))
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

	jobsDB := testhelper.SetUpJobsDB(t)

	warehouseTest := &testhelper.WareHouseTest{
		Client: &client.Client{
			SQL:  db,
			Type: client.SQLClient,
		},
		WriteKey:      "JAAwdCxmM8BIabKERsUhPNmMmdf",
		Schema:        schema,
		Tables:        []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"},
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

	testhelper.VerifyEventsInStagingFiles(t, jobsDB, warehouseTest, testhelper.DefaultStagingFilesEventsMap())
	testhelper.VerifyEventsInLoadFiles(t, jobsDB, warehouseTest, testhelper.DefaultLoadFilesEventsMap())
	testhelper.VerifyEventsInTableUploads(t, jobsDB, warehouseTest, testhelper.DefaultTableUploadsEventsMap())
	testhelper.VerifyEventsInWareHouse(t, warehouseTest, testhelper.DefaultWarehouseEventsMap())

	// Scenario 2
	warehouseTest.TimestampBeforeSendingEvents = timeutil.Now()
	warehouseTest.UserId = testhelper.GetUserId(warehouseutils.RS)

	sendEventsMap = testhelper.SendEventsMap()
	testhelper.SendModifiedEvents(t, warehouseTest, sendEventsMap)
	testhelper.SendModifiedEvents(t, warehouseTest, sendEventsMap)
	testhelper.SendModifiedEvents(t, warehouseTest, sendEventsMap)
	testhelper.SendIntegratedEvents(t, warehouseTest, sendEventsMap)

	testhelper.VerifyEventsInStagingFiles(t, jobsDB, warehouseTest, testhelper.DefaultStagingFilesEventsMap())
	testhelper.VerifyEventsInLoadFiles(t, jobsDB, warehouseTest, testhelper.DefaultLoadFilesEventsMap())
	testhelper.VerifyEventsInTableUploads(t, jobsDB, warehouseTest, testhelper.DefaultTableUploadsEventsMap())
	testhelper.VerifyEventsInWareHouse(t, warehouseTest, testhelper.DefaultWarehouseEventsMap())

	testhelper.VerifyWorkspaceIDInStats(t)
}

func TestRedshiftConfigurationValidation(t *testing.T) {
	t.Parallel()

	if _, exists := os.LookupEnv(testhelper.RedshiftIntegrationTestCredentials); !exists {
		t.Skipf("Skipping %s as %s is not set", t.Name(), testhelper.RedshiftIntegrationTestCredentials)
	}

	misc.Init()
	validations.Init()
	warehouseutils.Init()
	redshift.Init()

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
	testhelper.VerifyConfigurationTest(t, destination)
}
