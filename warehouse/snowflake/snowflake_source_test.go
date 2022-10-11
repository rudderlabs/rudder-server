//go:build sources_integration && !warehouse_integration

package snowflake_test

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"testing"

	// "github.com/stretchr/testify/require"

	"github.com/gofrs/uuid"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/snowflake"
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
	TestCredentialsKey = testhelper.SnowflakeIntegrationTestCredentials
	TestSchemaKey      = testhelper.SnowflakeIntegrationTestSchema
)

// snowflakeCredentials extracting snowflake credentials
func snowflakeCredentials() (snowflakeCredentials snowflake.SnowflakeCredentialsT, err error) {
	cred, exists := os.LookupEnv(TestCredentialsKey)
	if !exists {
		err = fmt.Errorf("following %s does not exists while running the Snowflake test", TestCredentialsKey)
		return
	}

	err = json.Unmarshal([]byte(cred), &snowflakeCredentials)
	if err != nil {
		err = fmt.Errorf("error occurred while unmarshalling snowflake test credentials with err: %s", err.Error())
		return
	}
	return
}

// VerifyConnection test connection for snowflake
func (t *TestHandle) VerifyConnection() (err error) {
	credentials, err := snowflakeCredentials()
	if err != nil {
		return err
	}

	err = testhelper.WithConstantBackoff(func() (err error) {
		handle.DB, err = snowflake.Connect(credentials)
		if err != nil {
			err = fmt.Errorf("could not connect to warehouse snowflake with error: %w", err)
			return
		}
		return
	})
	if err != nil {
		return fmt.Errorf("error while running test connection for snowflake with err: %s", err.Error())
	}
	return nil
}

func TestSnowflakeIntegration(t *testing.T) {
	// Cleanup resources
	// Dropping temporary schema
	t.Cleanup(func() {
		require.NoError(t, testhelper.WithConstantBackoff(func() (err error) {
			_, err = handle.DB.Exec(fmt.Sprintf(`DROP SCHEMA "%s" CASCADE;`, handle.Schema))
			return
		}), fmt.Sprintf("Failed dropping schema %s for Snowflake", handle.Schema))
	})

	// Setting up the test configuration
	warehouseTest := &testhelper.WareHouseTest{
		Client: &client.Client{
			SQL:  handle.DB,
			Type: client.SQLClient,
		},
		Schema:                handle.Schema,
		Tables:                handle.Tables,
		MessageId:             uuid.Must(uuid.NewV4()).String(),
		Provider:              warehouseutils.SNOWFLAKE,
		SourceID:              handle.SourceId,
		DestinationID:         handle.DestinationId,
		SourceWriteKey:        handle.SourceWriteKey,
		LatestSourceRunConfig: testhelper.DefaultSourceRunConfig(),
	}

	warehouseTest.UserId = testhelper.GetUserId(warehouseutils.SNOWFLAKE)
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
	testhelper.VerifyEventsInWareHouse(t, warehouseTest, testhelper.WarehouseSourceEventsMap())
}

func TestMain(m *testing.M) {
	_, exists := os.LookupEnv(TestCredentialsKey)
	if !exists {
		log.Println("Skipping Snowflake Test as the Test credentials does not exits.")
		return
	}

	handle = &TestHandle{
		Schema:         testhelper.Schema(warehouseutils.SNOWFLAKE, testhelper.SnowflakeIntegrationTestSchema),
		SourceWriteKey: "2DkCpXZcEvJK2fcpUD3LmjPI7J6",
		SourceId:       "2DkCpUr0xfiGBPJxIwqyqfyHdq4",
		DestinationId:  "24qeADObp6eIhjjDnEppO6P1SNc",
		Tables:         []string{"tracks", "google_sheet"},
	}
	os.Exit(testhelper.Run(m, handle))
}
