//go:build warehouse_integration

package mssql_test

import (
	"database/sql"
	"fmt"
	"os"
	"testing"

	"github.com/rudderlabs/rudder-server/utils/timeutil"

	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/mssql"
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

// VerifyConnection test connection for mssql
func (*TestHandle) VerifyConnection() error {
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

func TestMSSQLIntegration(t *testing.T) {
	// Setting up the warehouseTest
	warehouseTest := &testhelper.WareHouseTest{
		Client: &client.Client{
			SQL:  handle.DB,
			Type: client.SQLClient,
		},

		WriteKey:      handle.WriteKey,
		Schema:        handle.Schema,
		Tables:        handle.Tables,
		Provider:      warehouseutils.MSSQL,
		SourceID:      "1wRvLmEnMOONMbdspwaZhyCqXRE",
		DestinationID: "21Ezdq58khNMj07VJB0VJmxLvgu",
	}

	// Scenario 1
	warehouseTest.TimestampBeforeSendingEvents = timeutil.Now()
	warehouseTest.UserId = testhelper.GetUserId(warehouseutils.MSSQL)

	warehouseTest.EventsCountMap = testhelper.SendEventsMap()
	testhelper.SendEvents(t, warehouseTest)
	testhelper.SendEvents(t, warehouseTest)
	testhelper.SendEvents(t, warehouseTest)
	testhelper.SendIntegratedEvents(t, warehouseTest)

	warehouseTest.EventsCountMap = testhelper.StagingFilesEventsMap()
	testhelper.VerifyingEventsInStagingFiles(t, warehouseTest)

	warehouseTest.EventsCountMap = testhelper.LoadFilesEventsMap()
	testhelper.VerifyingEventsInLoadFiles(t, warehouseTest)

	warehouseTest.EventsCountMap = testhelper.TableUploadsEventsMap()
	testhelper.VerifyingEventsInTableUploads(t, warehouseTest)

	warehouseTest.EventsCountMap = testhelper.WarehouseEventsMap()
	testhelper.VerifyingEventsInWareHouse(t, warehouseTest)

	// Scenario 2
	warehouseTest.TimestampBeforeSendingEvents = timeutil.Now()
	warehouseTest.UserId = testhelper.GetUserId(warehouseutils.MSSQL)

	warehouseTest.EventsCountMap = testhelper.SendEventsMap()
	testhelper.SendModifiedEvents(t, warehouseTest)
	testhelper.SendModifiedEvents(t, warehouseTest)
	testhelper.SendModifiedEvents(t, warehouseTest)
	testhelper.SendIntegratedEvents(t, warehouseTest)

	warehouseTest.EventsCountMap = testhelper.StagingFilesEventsMap()
	testhelper.VerifyingEventsInStagingFiles(t, warehouseTest)

	warehouseTest.EventsCountMap = testhelper.LoadFilesEventsMap()
	testhelper.VerifyingEventsInLoadFiles(t, warehouseTest)

	warehouseTest.EventsCountMap = testhelper.TableUploadsEventsMap()
	testhelper.VerifyingEventsInTableUploads(t, warehouseTest)

	warehouseTest.EventsCountMap = testhelper.WarehouseEventsMap()
	testhelper.VerifyingEventsInWareHouse(t, warehouseTest)
}

func TestMain(m *testing.M) {
	handle = &TestHandle{
		WriteKey: "YSQ3n267l1VQKGNbSuJE9fQbzON",
		Schema:   "mssql_wh_integration",
		Tables:   []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"},
	}
	os.Exit(testhelper.Run(m, handle))
}
