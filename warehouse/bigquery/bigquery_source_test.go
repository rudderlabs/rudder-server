//go:build sources_integration && !warehouse_integration

package bigquery_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/rudderlabs/rudder-server/utils/misc"

	"github.com/stretchr/testify/require"

	bigquery2 "github.com/rudderlabs/rudder-server/warehouse/bigquery"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/testhelper"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestSourcesBigQueryIntegration(t *testing.T) {
	t.Parallel()

	if _, exists := os.LookupEnv(testhelper.BigqueryIntegrationTestCredentials); !exists {
		t.Skipf("Skipping %s as %s is not set", t.Name(), testhelper.BigqueryIntegrationTestCredentials)
	}

	bigquery2.Init()

	credentials, err := testhelper.BigqueryCredentials()
	require.NoError(t, err)

	db, err := bigquery2.Connect(context.TODO(), &credentials)
	require.NoError(t, err)

	schema := testhelper.Schema(warehouseutils.BQ, testhelper.BigqueryIntegrationTestSchema)

	t.Cleanup(func() {
		require.NoError(t, testhelper.WithConstantBackoff(func() (err error) {
			return db.Dataset(schema).DeleteWithContents(context.TODO())
		}), fmt.Sprintf("Failed dropping dataset %s for BigQuery", schema))
	})

	t.Run("Merge Mode", func(t *testing.T) {
		require.NoError(t, testhelper.SetConfig([]warehouseutils.KeyValue{
			{
				Key:   "Warehouse.bigquery.isDedupEnabled",
				Value: true,
			},
		}))

		warehouseTest := &testhelper.WareHouseTest{
			Client: &client.Client{
				BQ:   db,
				Type: client.BQClient,
			},
			SourceWriteKey:        "2DkCpXZcUjnK2fcpUD3LmjPI7J6",
			SourceID:              "2DkCpUr0xfiPLKJxIwqyqfyHdq4",
			DestinationID:         "26Bgm9FrQDZjvadSwAlpd35atwn",
			Schema:                schema,
			Tables:                []string{"tracks", "google_sheet"},
			MessageId:             misc.FastUUID().String(),
			Provider:              warehouseutils.BQ,
			LatestSourceRunConfig: testhelper.DefaultSourceRunConfig(),
		}

		warehouseTest.UserId = testhelper.GetUserId(warehouseutils.BQ)
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
	})
}
