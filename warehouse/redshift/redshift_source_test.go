//go:build sources_integration && !warehouse_integration

package redshift_test

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/rudderlabs/rudder-server/utils/misc"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/redshift"
	"github.com/rudderlabs/rudder-server/warehouse/testhelper"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type TestHandle struct {
	DB             *sql.DB
	WriteKey       string
	Schema         string
	Tables         []string
	SourceId       string
	DestinationId  string
	SourceWriteKey string
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

func TestSourceRedshiftIntegration(t *testing.T) {
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
		Schema:                handle.Schema,
		Tables:                handle.Tables,
		SourceWriteKey:        handle.SourceWriteKey,
		SourceID:              handle.SourceId,
		DestinationID:         handle.DestinationId,
		LatestSourceRunConfig: testhelper.DefaultSourceRunConfig(),
		MessageId:             misc.FastUUID().String(),
		Provider:              warehouseutils.RS,
	}

	warehouseTest.UserId = testhelper.GetUserId(warehouseutils.RS)
	sendEventsMap := testhelper.DefaultSourceEventMap()
	// Scenario 1
	// Sending the first set of events.
	// Since we handle dedupe on the staging table, we need to check if the first set of events reached the destination.
	testhelper.SendEvents(t, warehouseTest, sendEventsMap)
	testhelper.SendEvents(t, warehouseTest, sendEventsMap)
	testhelper.SendEvents(t, warehouseTest, sendEventsMap)
	warehouseEventsWithoutDeDup := testhelper.EventsCountMap{
		"google_sheet": 3,
		"tracks":       1,
	}
	testhelper.VerifyEventsInWareHouse(t, warehouseTest, warehouseEventsWithoutDeDup)

	// Sending deduped events
	testhelper.SendEvents(t, warehouseTest, sendEventsMap)
	testhelper.SendEvents(t, warehouseTest, sendEventsMap)
	testhelper.SendEvents(t, warehouseTest, sendEventsMap)
	testhelper.SendAsyncRequest(t, warehouseTest)
	testhelper.SendAsyncStatusRequest(t, warehouseTest)
	testhelper.VerifyEventsInWareHouse(t, warehouseTest, testhelper.DefaultWarehouseSourceEventsMap())
}

func TestMain(m *testing.M) {
	_, exists := os.LookupEnv(TestCredentialsKey)
	if !exists {
		log.Println("Skipping Redshift Test as the Test credentials does not exits.")
		return
	}

	handle = &TestHandle{
		SourceWriteKey: "2DkCpXZcEvJK2fcpUD3LmjPI7J6",
		SourceId:       "2DkCpUr0xfiGBPJxIwqyqfyHdq4",
		DestinationId:  "27SthahyhhqZE74HT4NTtNPl06V",
		Schema:         testhelper.Schema(warehouseutils.RS, testhelper.RedshiftIntegrationTestSchema),
		Tables:         []string{"tracks", "google_sheet"},
	}
	os.Exit(testhelper.Run(m, handle))
}
