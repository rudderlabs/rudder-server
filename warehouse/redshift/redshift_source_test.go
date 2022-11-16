//go:build sources_integration && !warehouse_integration

package redshift_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/rudderlabs/rudder-server/utils/misc"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/redshift"
	"github.com/rudderlabs/rudder-server/warehouse/testhelper"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestSourcesRedshiftIntegration(t *testing.T) {
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
			_, err = db.Exec(fmt.Sprintf(`DROP SCHEMA "%s" CASCADE;`, schema))
			return
		}), fmt.Sprintf("Failed dropping schema %s for Redshift", schema))
	})

	warehouseTest := &testhelper.WareHouseTest{
		Client: &client.Client{
			SQL:  db,
			Type: client.SQLClient,
		},
		Schema:                schema,
		Tables:                []string{"tracks", "google_sheet"},
		SourceWriteKey:        "2DkCpXZcEvJK2fcpUD3LmjPI7J6",
		SourceID:              "2DkCpUr0xfiGBPJxIwqyqfyHdq4",
		DestinationID:         "27SthahyhhqZE74HT4NTtNPl06V",
		LatestSourceRunConfig: testhelper.DefaultSourceRunConfig(),
		MessageId:             misc.FastUUID().String(),
		Provider:              warehouseutils.RS,
	}

	warehouseTest.UserId = testhelper.GetUserId(warehouseutils.RS)
	sendEventsMap := testhelper.DefaultSourceEventMap()

	testhelper.SendEvents(t, warehouseTest, sendEventsMap)
	testhelper.SendEvents(t, warehouseTest, sendEventsMap)
	testhelper.SendEvents(t, warehouseTest, sendEventsMap)

	testhelper.VerifyEventsInWareHouse(t, warehouseTest, testhelper.DefaultWarehouseSourceEventsMapWithoutDedup())

	testhelper.SendEvents(t, warehouseTest, sendEventsMap)
	testhelper.SendEvents(t, warehouseTest, sendEventsMap)
	testhelper.SendEvents(t, warehouseTest, sendEventsMap)

	testhelper.SendAsyncRequest(t, warehouseTest)
	testhelper.SendAsyncStatusRequest(t, warehouseTest)

	testhelper.VerifyEventsInWareHouse(t, warehouseTest, testhelper.DefaultWarehouseSourceEventsMap())
}
