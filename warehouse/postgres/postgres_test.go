//go:build warehouse_integration

package postgres_test

import (
	"database/sql"
	"fmt"
	"os"
	"testing"

	"github.com/rudderlabs/rudder-server/utils/timeutil"

	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/postgres"
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

// VerifyConnection test connection for postgres
func (*TestHandle) VerifyConnection() error {
	err := testhelper.WithConstantBackoff(func() (err error) {
		credentials := postgres.CredentialsT{
			DBName:   "rudderdb",
			Password: "rudder-password",
			User:     "rudder",
			Host:     "wh-postgres",
			SSLMode:  "disable",
			Port:     "5432",
		}
		if handle.DB, err = postgres.Connect(credentials); err != nil {
			err = fmt.Errorf("could not connect to warehouse postgres with error: %w", err)
			return
		}
		if err = handle.DB.Ping(); err != nil {
			err = fmt.Errorf("could not connect to warehouse postgres while pinging with error: %w", err)
			return
		}
		return
	})
	if err != nil {
		return fmt.Errorf("error while running test connection for postgres with err: %s", err.Error())
	}
	return nil
}

func TestPostgresIntegration(t *testing.T) {
	warehouseTest := &testhelper.WareHouseTest{
		Client: &client.Client{
			SQL:  handle.DB,
			Type: client.SQLClient,
		},
		WriteKey:      handle.WriteKey,
		Schema:        handle.Schema,
		Tables:        handle.Tables,
		Provider:      warehouseutils.POSTGRES,
		SourceID:      "1wRvLmEnMOOxSQD9pwaZhyCqXRE",
		DestinationID: "216ZvbavR21Um6eGKQCagZHqLGZ",
	}

	// Scenario 1
	warehouseTest.TimestampBeforeSendingEvents = timeutil.Now()
	warehouseTest.UserId = testhelper.GetUserId(warehouseutils.POSTGRES)

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
	warehouseTest.UserId = testhelper.GetUserId(warehouseutils.POSTGRES)

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
		WriteKey: "kwzDkh9h2fhfUVuS9jZ8uVbhV3v",
		Schema:   "postgres_wh_integration",
		Tables:   []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"},
	}
	os.Exit(testhelper.Run(m, handle))
}
