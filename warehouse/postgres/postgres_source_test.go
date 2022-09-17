//go:build sources_integration && !warehouse_integration

package postgres_test

import (
	"database/sql"
	"fmt"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/postgres"
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
	Type           string
}

var handle *TestHandle

// VerifyConnection test connection for postgres
func (t *TestHandle) VerifyConnection() error {
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

func TestSourcesPostgresIntegration(t *testing.T) {
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
		UserId:                testhelper.GetUserId(warehouseutils.POSTGRES),
		Provider:              warehouseutils.POSTGRES,
		LatestSourceRunConfig: testhelper.DefaultSourceRunConfig(),
	}

	// Sending the sheet events.
	testhelper.SendEvents(t, warehouseTest)
	testhelper.SendEvents(t, warehouseTest)
	testhelper.SendEvents(t, warehouseTest)
	testhelper.SendAsyncRequest(t, warehouseTest)

	// Setting up the events map
	// Checking for Gateway and Batch router events
	// Checking for the events count for each table
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
		Type:           "POSTGRES",
		SourceWriteKey: "2DkCpXZcEvJK2fcpUD3LmjPI7J6",
		Schema:         "postgres_wh_integration",
		SourceId:       "2DkCpUr0xfiGBPJxIwqyqfyHdq4",
		DestinationId:  "216ZvbavR21Um6eGKQCagZHqLGZ",
		Tables:         []string{"tracks", "google_sheet"},
	}
	os.Exit(testhelper.Run(m, handle))
}
