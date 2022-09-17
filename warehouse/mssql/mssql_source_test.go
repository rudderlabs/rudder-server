//go:build sources_integration && !warehouse_integration

package mssql_test

import (
	"database/sql"
	"fmt"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/mssql"
	"github.com/rudderlabs/rudder-server/warehouse/testhelper"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"os"
	"testing"
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
	warehouseTest := &testhelper.WareHouseTest{
		Client: &client.Client{
			SQL:  handle.DB,
			Type: client.SQLClient,
		},

		SourceWriteKey:        handle.SourceWriteKey,
		SourceId:              handle.SourceId,
		DestinationId:         handle.DestinationId,
		Schema:                handle.Schema,
		Tables:                handle.Tables,
		EventsCountMap:        testhelper.DefaultSourceEventMap(),
		TablesQueryFrequency:  testhelper.DefaultQueryFrequency,
		UserId:                testhelper.GetUserId(warehouseutils.MSSQL),
		Provider:              warehouseutils.MSSQL,
		LatestSourceRunConfig: testhelper.DefaultSourceRunConfig(),
	}

	testhelper.SendEvents(t, warehouseTest)
	testhelper.SendEvents(t, warehouseTest)
	testhelper.SendEvents(t, warehouseTest)
	testhelper.SendAsyncRequest(t, warehouseTest)

	warehouseTest.EventsCountMap = testhelper.EventsCountMap{
		"google_sheet":    3,
		"wh_google_sheet": 1,
		"tracks":          1,
		"gateway":         3,
		"batchRT":         6,
	}
	testhelper.VerifyingGatewayEvents(t, warehouseTest)
	testhelper.VerifyingBatchRouterEvents(t, warehouseTest)
	testhelper.VerifyingTablesEventCount(t, warehouseTest)
}

func TestMain(m *testing.M) {
	handle = &TestHandle{
		SourceWriteKey: "2DkCpXZcEvKM2fcpUD3LmjPI7J6",
		SourceId:       "2DkCpXZcEvLM2fcpUD3LmjPI7J6",
		DestinationId:  "21Ezdq58khNMj07VJB0VJmxLvgu",
		Schema:         "mssql_wh_integration",
		Tables:         []string{"tracks", "google_sheet"},
	}
	os.Exit(testhelper.Run(m, handle))
}
