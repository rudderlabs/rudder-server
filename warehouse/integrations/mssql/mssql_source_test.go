//go:build sources_integration && !warehouse_integration

package mssql_test

import (
	"database/sql"
	"fmt"
	"os"
	"testing"

	"github.com/rudderlabs/rudder-server/warehouse/integrations/mssql"

	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/testhelper"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/stretchr/testify/require"
)

type TestHandle struct {
	DB             *sql.DB
	WriteKey       string
	SourceId       string
	DestinationId  string
	SourceWriteKey string
	Schema         string
	Tables         []string
}

var handle *TestHandle

// VerifyConnection test connection for postgres
func (t *TestHandle) VerifyConnection() error {
	err := testhelper.WithConstantBackoff(func() (err error) {
		credentials := mssql.CredentialsT{
			DBName:   "master",
			Password: "reallyStrongPwd123",
			User:     "SA",
			Host:     "wh-mssql",
			SSLMode:  "disable",
			Port:     "1433",
		}
		if handle.DB, err = mssql.Connect(credentials); err != nil {
			err = fmt.Errorf("could not connect to warehouse mssql with error: %w", err)
			return
		}
		if err = handle.DB.Ping(); err != nil {
			err = fmt.Errorf("could not connect to warehouse mssql while pinging with error: %w", err)
			return
		}
		return
	})
	if err != nil {
		return fmt.Errorf("error while running test connection for mssql with err: %s", err.Error())
	}
	return nil
}

func TestSourcesMSSQLIntegration(t *testing.T) {
	// Setting up the warehouseTest
	require.NoError(t, testhelper.SetConfig([]warehouseutils.KeyValue{
		{
			Key:   "Warehouse.mssql.enableDeleteByJobs",
			Value: true,
		},
	}))
	warehouseTest := &testhelper.WareHouseTest{
		Client: &client.Client{
			SQL:  handle.DB,
			Type: client.SQLClient,
		},

		SourceWriteKey:        handle.SourceWriteKey,
		SourceID:              handle.SourceId,
		DestinationID:         handle.DestinationId,
		Schema:                handle.Schema,
		Tables:                handle.Tables,
		Provider:              warehouseutils.MSSQL,
		LatestSourceRunConfig: testhelper.DefaultSourceRunConfig(),
	}

	warehouseTest.UserId = testhelper.GetUserId(warehouseutils.MSSQL)
	sendEventsMap := testhelper.DefaultSourceEventMap()
	testhelper.SendEvents(t, warehouseTest, sendEventsMap)
	testhelper.SendEvents(t, warehouseTest, sendEventsMap)
	testhelper.SendEvents(t, warehouseTest, sendEventsMap)

	warehouseEventsWithoutDeDup := testhelper.EventsCountMap{
		"google_sheet": 3,
		"tracks":       1,
	}
	testhelper.VerifyEventsInWareHouse(t, warehouseTest, warehouseEventsWithoutDeDup)

	testhelper.SendEvents(t, warehouseTest, sendEventsMap)
	testhelper.SendEvents(t, warehouseTest, sendEventsMap)
	testhelper.SendEvents(t, warehouseTest, sendEventsMap)
	testhelper.SendAsyncRequest(t, warehouseTest)
	testhelper.SendAsyncStatusRequest(t, warehouseTest)
	testhelper.VerifyEventsInWareHouse(t, warehouseTest, testhelper.WarehouseSourceEventsMap())
}

func TestMain(m *testing.M) {
	handle = &TestHandle{
		SourceWriteKey: "2DkCpXZcEvPG2fcpUD3LmjPI7J6",
		Schema:         "mssql_wh_integration",
		SourceId:       "2DkCpUr0xfiINRJxIwqyqfyHdq4",
		DestinationId:  "21Ezdq58khNMj07VJB0VJmxLvgu",
		Tables:         []string{"tracks", "google_sheet"},
	}
	os.Exit(testhelper.Run(m, handle))
}
