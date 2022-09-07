//go:build warehouse_integration

package postgres_test

import (
	"database/sql"
	"fmt"
	"os"
	"testing"

	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/postgres"
	"github.com/rudderlabs/rudder-server/warehouse/testhelper"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type TestHandle struct {
	DB             *sql.DB
	WriteKey       string
	SourceWriteKey string
	Schema         string
	Tables         []string
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
	// Setting up the warehouseTest
	warehouseTest := &testhelper.WareHouseTest{
		Client: &client.Client{
			SQL:  handle.DB,
			Type: client.SQLClient,
		},
		WriteKey:              handle.WriteKey,
		SourceWriteKey:        handle.SourceWriteKey,
		SourceId:              "2DkCpUr0xfiGBPJxIwqyqfyHdq4",
		DestinationId:         "216ZvbavR21Um6eGKQCagZHqLGZ",
		Schema:                handle.Schema,
		Tables:                handle.Tables,
		EventsCountMap:        testhelper.DefaultEventMap(true),
		TablesQueryFrequency:  testhelper.DefaultQueryFrequency,
		UserId:                testhelper.GetUserId(warehouseutils.POSTGRES),
		Provider:              warehouseutils.POSTGRES,
		LatestSourceRunConfig: testhelper.DefaultSourceRunConfig(),
	}

	// Scenario 1
	// Sending the first set of events.
	// Since we are sending unique message Ids.
	// These should result in events count will be equal to the number of events being sent
	testhelper.SendEvents(t, warehouseTest)
	testhelper.SendEvents(t, warehouseTest)
	testhelper.SendEvents(t, warehouseTest)
	testhelper.SendAsyncRequest(t, warehouseTest)
	testhelper.SendIntegratedEvents(t, warehouseTest)

	// Setting up the events map
	// Checking for Gateway and Batch router events
	// Checking for the events count for each table
	warehouseTest.EventsCountMap = testhelper.EventsCountMap{
		"google_sheet":    3,
		"wh_google_sheet": 1,
		"identifies":      4,
		"users":           1,
		"tracks":          5,
		"product_track":   4,
		"pages":           4,
		"screens":         4,
		"aliases":         4,
		"groups":          4,
		"gateway":         27,
		"batchRT":         38,
	}
	testhelper.VerifyingGatewayEvents(t, warehouseTest)
	testhelper.VerifyingBatchRouterEvents(t, warehouseTest)
	testhelper.VerifyingTablesEventCount(t, warehouseTest)

	// Scenario 2
	// Setting up events count map
	// Setting up the UserID
	// Sending the second set of modified events
	// Since we are sending unique message Ids
	// These should result in events count will be equal to the number of events being sent
	warehouseTest.EventsCountMap = testhelper.DefaultEventMap()
	warehouseTest.UserId = testhelper.GetUserId(warehouseutils.POSTGRES)
	testhelper.SendModifiedEvents(t, warehouseTest)
	testhelper.SendModifiedEvents(t, warehouseTest)
	testhelper.SendModifiedEvents(t, warehouseTest)
	testhelper.SendIntegratedEvents(t, warehouseTest)

	// Setting up the events map
	// Checking for Gateway and Batch router events
	// Checking for the events count for each table
	warehouseTest.EventsCountMap = testhelper.EventsCountMap{
		"google_sheet":    0,
		"wh_google_sheet": 0,
		"identifies":      4,
		"users":           1,
		"tracks":          4,
		"product_track":   4,
		"pages":           4,
		"screens":         4,
		"aliases":         4,
		"groups":          4,
		"gateway":         24,
		"batchRT":         32,
	}
	testhelper.VerifyingGatewayEvents(t, warehouseTest)
	testhelper.VerifyingBatchRouterEvents(t, warehouseTest)
	testhelper.VerifyingTablesEventCount(t, warehouseTest)
}

func TestMain(m *testing.M) {
	handle = &TestHandle{
		WriteKey:       "kwzDkh9h2fhfUVuS9jZ8uVbhV3v",
		SourceWriteKey: "2DkCpXZcEvJK2fcpUD3LmjPI7J6",
		Schema:         "postgres_wh_integration",
		Tables:         []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups", "google_sheet"},
	}
	os.Exit(testhelper.Run(m, handle))
}
