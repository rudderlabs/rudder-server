//go:build warehouse_integration

package postgres_test

import (
	"database/sql"
	"fmt"
	"os"
	"testing"

	"github.com/gofrs/uuid"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/postgres"
	"github.com/rudderlabs/rudder-server/warehouse/testhelper"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/stretchr/testify/require"
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
	// Setting up the test configuration
	require.NoError(t, testhelper.SetConfig([]warehouseutils.KeyValue{
		{
			Key:   "Warehouse.enableExcludedSchema",
			Value: true,
		},
		{
			Key:   "Warehouse.postgres.maxColumnCount",
			Value: 15,
		},
	}))
	// Setting up the warehouseTest
	warehouseTest := &testhelper.WareHouseTest{
		Client: &client.Client{
			SQL:  handle.DB,
			Type: client.SQLClient,
		},
		WriteKey:             handle.WriteKey,
		Schema:               handle.Schema,
		Tables:               handle.Tables,
		EventsCountMap:       testhelper.DefaultEventMap(),
		TablesQueryFrequency: testhelper.DefaultQueryFrequency,
		MessageId:            uuid.Must(uuid.NewV4()).String(),
		UserId:               testhelper.GetUserId(warehouseutils.POSTGRES),
		Provider:             warehouseutils.POSTGRES,
	}

	// Scenario 1
	// Sending a new event to handle exceeded columns, set max limit to 15
	// Since we are sending same message Id to all type of events
	// All rows in rudder discards have same row_id
	// Exceeded count: pages -> 1, groups -> 3, product_track -> 4
	testhelper.SendEvents(t, warehouseTest)

	// Setting up the events map
	// Checking for Gateway and Batch router events
	// Checking for the events count for each table
	warehouseTest.EventsCountMap = testhelper.EventsCountMap{
		"identifies":      1,
		"users":           1,
		"tracks":          1,
		"product_track":   1,
		"pages":           1,
		"screens":         1,
		"aliases":         1,
		"groups":          1,
		"rudder_discards": 7,
		"gateway":         6,
		"batchRT":         8,
	}
	testhelper.VerifyingGatewayEvents(t, warehouseTest)
	testhelper.VerifyingBatchRouterEvents(t, warehouseTest)
	testhelper.VerifyingTablesEventCount(t, warehouseTest)

	// Scenario 2
	// Setting up test configuration
	require.NoError(t, testhelper.SetConfig([]warehouseutils.KeyValue{
		{
			Key:   "Warehouse.enableExcludedSchema",
			Value: false,
		},
	}))
	// Sending the first set of events.
	// Since we are sending unique message Ids.
	// These should result in events count will be equal to the number of events being sent
	warehouseTest.UserId = testhelper.GetUserId(warehouseutils.POSTGRES)
	warehouseTest.MessageId = ""
	testhelper.SendEvents(t, warehouseTest)
	testhelper.SendEvents(t, warehouseTest)
	testhelper.SendEvents(t, warehouseTest)
	testhelper.SendIntegratedEvents(t, warehouseTest)

	// Setting up the events map
	// Checking for Gateway and Batch router events
	// Checking for the events count for each table
	warehouseTest.EventsCountMap = testhelper.EventsCountMap{
		"identifies":      4,
		"users":           1,
		"tracks":          4,
		"product_track":   4,
		"pages":           4,
		"screens":         4,
		"aliases":         4,
		"groups":          4,
		"rudder_discards": 0,
		"gateway":         24,
		"batchRT":         32,
	}
	testhelper.VerifyingGatewayEvents(t, warehouseTest)
	testhelper.VerifyingBatchRouterEvents(t, warehouseTest)
	testhelper.VerifyingTablesEventCount(t, warehouseTest)

	// Scenario 3
	// Setting up test configuration
	require.NoError(t, testhelper.SetConfig([]warehouseutils.KeyValue{
		{
			Key:   "Warehouse.enableExcludedSchema",
			Value: true,
		},
		{
			Key:   "Warehouse.postgres.maxColumnCount",
			Value: 21,
		},
	}))
	// Setting up events count map
	// Sending the second set of modified events
	// Sending a existing event to handle exceeded columns, set max limit to 21
	// Since we are sending same message Id to all type of events
	// Events will be deduped to single entry and
	// All rows in rudder discards have same row_id
	// Exceeded count: groups -> 1, product_track -> 3 for each modified event set
	warehouseTest.EventsCountMap = testhelper.DefaultEventMap()
	warehouseTest.UserId = testhelper.GetUserId(warehouseutils.POSTGRES)
	warehouseTest.MessageId = uuid.Must(uuid.NewV4()).String()
	testhelper.SendModifiedEvents(t, warehouseTest)
	testhelper.SendModifiedEvents(t, warehouseTest)
	testhelper.SendModifiedEvents(t, warehouseTest)
	testhelper.SendIntegratedEvents(t, warehouseTest)

	// Setting up the events map
	// Checking for Gateway and Batch router events
	// Checking for the events count for each table
	warehouseTest.EventsCountMap = testhelper.EventsCountMap{
		"identifies":      1,
		"users":           1,
		"tracks":          1,
		"product_track":   1,
		"pages":           1,
		"screens":         1,
		"aliases":         1,
		"groups":          1,
		"rudder_discards": 12,
		"gateway":         24,
		"batchRT":         32,
	}
	testhelper.VerifyingGatewayEvents(t, warehouseTest)
	testhelper.VerifyingBatchRouterEvents(t, warehouseTest)
	testhelper.VerifyingTablesEventCount(t, warehouseTest)
}

func TestMain(m *testing.M) {
	handle = &TestHandle{
		WriteKey: "kwzDkh9h2fhfUVuS9jZ8uVbhV3v",
		Schema:   "postgres_wh_integration",
		Tables:   []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups", "rudder_discards"},
	}
	os.Exit(testhelper.Run(m, handle))
}
