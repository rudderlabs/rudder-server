//go:build sources_integration && !warehouse_integration

package postgres_test

import (
	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/postgres"
	"github.com/rudderlabs/rudder-server/warehouse/testhelper"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSourcesPostgresIntegration(t *testing.T) {
	t.Parallel()

	postgres.Init()

	db, err := postgres.Connect(postgres.CredentialsT{
		DBName:   "rudderdb",
		Password: "rudder-password",
		User:     "rudder",
		Host:     "wh-postgres",
		SSLMode:  "disable",
		Port:     "5432",
	})
	require.NoError(t, err)

	err = db.Ping()
	require.NoError(t, err)

	require.NoError(t, testhelper.SetConfig([]warehouseutils.KeyValue{
		{
			Key:   "Warehouse.postgres.enableDeleteByJobs",
			Value: true,
		},
	}))

	warehouseTest := &testhelper.WareHouseTest{
		Client: &client.Client{
			SQL:  db,
			Type: client.SQLClient,
		},
		SourceWriteKey:        "2DkCpXZcEvJK2fcpUD3LmjPI7J6",
		SourceID:              "2DkCpUr0xfiGBPJxIwqyqfyHdq4",
		DestinationID:         "216ZvbavR21Um6eGKQCagZHqLGZ",
		Schema:                "postgres_wh_integration",
		Tables:                []string{"tracks", "google_sheet"},
		Provider:              warehouseutils.POSTGRES,
		LatestSourceRunConfig: testhelper.DefaultSourceRunConfig(),
	}

	warehouseTest.UserId = testhelper.GetUserId(warehouseutils.POSTGRES)
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
