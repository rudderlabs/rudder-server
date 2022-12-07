//go:build sources_integration && !warehouse_integration

package postgres_test

import (
	"database/sql"
	"fmt"
	"os"
	"testing"

	"github.com/rudderlabs/rudder-server/warehouse/integrations/postgres"

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
	require.NoError(t, testhelper.SetConfig([]warehouseutils.KeyValue{
		{
			Key:   "Warehouse.postgres.enableDeleteByJobs",
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
		Provider:              warehouseutils.POSTGRES,
		LatestSourceRunConfig: testhelper.DefaultSourceRunConfig(),
	}

	warehouseTest.UserId = testhelper.GetUserId(warehouseutils.POSTGRES)
	sendEventsMap := testhelper.DefaultSourceEventMap()
	// Sending the sheet events.
	testhelper.SendEvents(t, warehouseTest, sendEventsMap)
	testhelper.SendEvents(t, warehouseTest, sendEventsMap)
	testhelper.SendEvents(t, warehouseTest, sendEventsMap)
	warehouseEventsWithoutDeDup := testhelper.EventsCountMap{
		"google_sheet": 3,
		"tracks":       1,
	}
	testhelper.VerifyEventsInWareHouse(t, warehouseTest, warehouseEventsWithoutDeDup)
	// Setting up the events map
	// Checking for Gateway and Batch router events
	// Checking for the events count for each table

	testhelper.SendEvents(t, warehouseTest, sendEventsMap)
	testhelper.SendEvents(t, warehouseTest, sendEventsMap)
	testhelper.SendEvents(t, warehouseTest, sendEventsMap)
	testhelper.SendAsyncRequest(t, warehouseTest)
	testhelper.SendAsyncStatusRequest(t, warehouseTest)
	testhelper.VerifyEventsInWareHouse(t, warehouseTest, testhelper.WarehouseSourceEventsMap())
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
