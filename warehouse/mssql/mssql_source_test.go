//go:build sources_integration && !warehouse_integration

package mssql_test

import (
	"github.com/rudderlabs/rudder-server/warehouse/mssql"
	"testing"

	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/testhelper"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/stretchr/testify/require"
)

func TestSourcesMSSQLIntegration(t *testing.T) {
	t.Parallel()

	mssql.Init()

	db, err := mssql.Connect(mssql.CredentialsT{
		DBName:   "master",
		Password: "reallyStrongPwd123",
		User:     "SA",
		Host:     "wh-mssql",
		SSLMode:  "disable",
		Port:     "1433",
	})
	require.NoError(t, err)

	err = db.Ping()
	require.NoError(t, err)

	require.NoError(t, testhelper.SetConfig([]warehouseutils.KeyValue{
		{
			Key:   "Warehouse.mssql.enableDeleteByJobs",
			Value: true,
		},
	}))

	warehouseTest := &testhelper.WareHouseTest{
		Client: &client.Client{
			SQL:  db,
			Type: client.SQLClient,
		},

		SourceWriteKey:        "2DkCpXZcEvPG2fcpUD3LmjPI7J6",
		SourceID:              "2DkCpUr0xfiINRJxIwqyqfyHdq4",
		DestinationID:         "21Ezdq58khNMj07VJB0VJmxLvgu",
		Schema:                "mssql_wh_integration",
		Tables:                []string{"tracks", "google_sheet"},
		Provider:              warehouseutils.MSSQL,
		LatestSourceRunConfig: testhelper.DefaultSourceRunConfig(),
	}

	warehouseTest.UserId = testhelper.GetUserId(warehouseutils.MSSQL)
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
