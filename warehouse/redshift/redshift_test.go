//go:build warehouse_integration

package redshift_test

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gofrs/uuid"

	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/redshift"
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

const (
	TestCredentialsKey = testhelper.RedshiftIntegrationTestCredentials
	TestSchemaKey      = testhelper.RedshiftIntegrationTestSchema
)

// redshiftCredentials extracting redshift test credentials
func redshiftCredentials() (rsCredentials redshift.RedshiftCredentialsT, err error) {
	cred, exists := os.LookupEnv(TestCredentialsKey)
	if !exists {
		err = fmt.Errorf("following %s does not exists while running the Redshift test", TestCredentialsKey)
		return
	}

	err = json.Unmarshal([]byte(cred), &rsCredentials)
	if err != nil {
		err = fmt.Errorf("error occurred while unmarshalling redshift test credentials with err: %s", err.Error())
	}
	return
}

// VerifyConnection test connection for redshift
func (*TestHandle) VerifyConnection() error {
	credentials, err := redshiftCredentials()
	if err != nil {
		return err
	}

	err = testhelper.WithConstantBackoff(func() (err error) {
		handle.DB, err = redshift.Connect(credentials)
		if err != nil {
			err = fmt.Errorf("could not connect to warehouse redshift with error: %w", err)
			return
		}
		return
	})
	if err != nil {
		return fmt.Errorf("error while running test connection for redshift with err: %s", err.Error())
	}
	return nil
}

func TestRedshiftIntegration(t *testing.T) {
	// Cleanup resources
	// Dropping temporary schema
	t.Cleanup(func() {
		require.NoError(t, testhelper.WithConstantBackoff(func() (err error) {
			_, err = handle.DB.Exec(fmt.Sprintf(`DROP SCHEMA "%s" CASCADE;`, handle.Schema))
			return
		}), fmt.Sprintf("Failed dropping schema %s for Redshift", handle.Schema))
	})

	// Setting up the warehouseTest
	warehouseTest := &testhelper.WareHouseTest{
		Client: &client.Client{
			SQL:  handle.DB,
			Type: client.SQLClient,
		},
		WriteKey:             handle.WriteKey,
		Schema:               handle.Schema,
		Tables:               handle.Tables,
		TablesQueryFrequency: testhelper.LongRunningQueryFrequency,
		EventsCountMap:       testhelper.DefaultEventMap(),
		MessageId:            uuid.Must(uuid.NewV4()).String(),
		UserId:               testhelper.GetUserId(warehouseutils.RS),
		Provider:             warehouseutils.RS,
	}

	// Scenario 1
	// Sending the first set of events.
	// Since we handle dedupe on the staging table, we need to check if the first set of events reached the destination.
	testhelper.SendEvents(t, warehouseTest)
	testhelper.SendEvents(t, warehouseTest)
	testhelper.SendEvents(t, warehouseTest)
	testhelper.SendIntegratedEvents(t, warehouseTest)

	// Setting up the events map
	// Checking for Gateway and Batch router events
	// Checking for the events count for each table
	warehouseTest.EventsCountMap = testhelper.EventsCountMap{
		"identifies":    1,
		"users":         1,
		"tracks":        1,
		"product_track": 1,
		"pages":         1,
		"screens":       1,
		"aliases":       1,
		"groups":        1,
		"gateway":       24,
		"batchRT":       32,
	}
	testhelper.VerifyingGatewayEvents(t, warehouseTest)
	testhelper.VerifyingBatchRouterEvents(t, warehouseTest)
	testhelper.VerifyingTablesEventCount(t, warehouseTest)

	// Scenario 2
	// Re-Setting up the events map
	// Sending the second set of events.
	// This time we will not be resetting the MessageID. We will be using the same one to check for the dedupe.
	warehouseTest.EventsCountMap = testhelper.DefaultEventMap()
	testhelper.SendModifiedEvents(t, warehouseTest)
	testhelper.SendModifiedEvents(t, warehouseTest)
	testhelper.SendModifiedEvents(t, warehouseTest)
	testhelper.SendIntegratedEvents(t, warehouseTest)

	// Setting up the events map
	// Checking for Gateway and Batch router events
	// Checking for the events count for each table
	// Since because of merge everything comes down to a single event in warehouse
	warehouseTest.EventsCountMap = testhelper.EventsCountMap{
		"identifies":    1,
		"users":         1,
		"tracks":        1,
		"product_track": 1,
		"pages":         1,
		"screens":       1,
		"aliases":       1,
		"groups":        1,
		"gateway":       48,
		"batchRT":       64,
	}
	testhelper.VerifyingGatewayEvents(t, warehouseTest)
	testhelper.VerifyingBatchRouterEvents(t, warehouseTest)
	testhelper.VerifyingTablesEventCount(t, warehouseTest)
}

func TestMain(m *testing.M) {
	_, exists := os.LookupEnv(TestCredentialsKey)
	if !exists {
		log.Println("Skipping Redshift Test as the Test credentials does not exits.")
		return
	}

	handle = &TestHandle{
		WriteKey: "JAAwdCxmM8BIabKERsUhPNmMmdf",
		Schema:   testhelper.GetSchema(warehouseutils.RS, TestSchemaKey),
		Tables:   []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"},
	}
	os.Exit(testhelper.Run(m, handle))
}
