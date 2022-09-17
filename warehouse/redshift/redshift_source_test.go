//go:build sources_integration && !warehouse_integration

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
		SourceId:              handle.SourceId,
		DestinationId:         handle.DestinationId,
		LatestSourceRunConfig: testhelper.DefaultSourceRunConfig(),
		TablesQueryFrequency:  testhelper.LongRunningQueryFrequency,
		MessageId:             uuid.Must(uuid.NewV4()).String(),
		UserId:                testhelper.GetUserId(warehouseutils.RS),
		Provider:              warehouseutils.RS,
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
	_, exists := os.LookupEnv(TestCredentialsKey)
	if !exists {
		log.Println("Skipping Redshift Test as the Test credentials does not exits.")
		return
	}

	handle = &TestHandle{
		SourceWriteKey: "2DkCpJkiuyil2fcpUD3LmjPI7J6",
		SourceId:       "2DkCpXZcFJhrj2fcpUD3LmjPI7J6",
		DestinationId:  "27SthahyhhqZE74HT4NTtNPl06V",
		Schema:         testhelper.GetSchema(warehouseutils.RS, TestSchemaKey),
		Tables:         []string{"tracks", "google_sheet"},
	}
	os.Exit(testhelper.Run(m, handle))
}
