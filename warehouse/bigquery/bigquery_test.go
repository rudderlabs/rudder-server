//go:build warehouse_integration

package bigquery_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"cloud.google.com/go/bigquery"

	"github.com/gofrs/uuid"
	bigquery2 "github.com/rudderlabs/rudder-server/warehouse/bigquery"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/testhelper"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/stretchr/testify/assert"
)

type TestHandle struct {
	DB       *bigquery.Client
	WriteKey string
	Schema   string
	Tables   []string
}

var handle *TestHandle

const (
	TestCredentialsKey = testhelper.BigqueryIntegrationTestCredentials
	TestSchemaKey      = testhelper.BigqueryIntegrationTestSchema
)

// bigqueryCredentials extracting big query credentials
func bigqueryCredentials() (bqCredentials bigquery2.BQCredentialsT, err error) {
	cred, exists := os.LookupEnv(TestCredentialsKey)
	if !exists {
		err = fmt.Errorf("following %s does not exists while running the Bigquery test", TestCredentialsKey)
		return
	}

	err = json.Unmarshal([]byte(cred), &bqCredentials)
	if err != nil {
		err = fmt.Errorf("error occurred while unmarshalling bigquery test credentials with err: %s", err.Error())
		return
	}
	return
}

// VerifyConnection test connection for big query
func (*TestHandle) VerifyConnection() error {
	credentials, err := bigqueryCredentials()
	if err != nil {
		return err
	}

	err = testhelper.WithConstantBackoff(func() (err error) {
		handle.DB, err = bigquery2.Connect(context.TODO(), &credentials)
		if err != nil {
			err = fmt.Errorf("could not connect to warehouse bigquery with error: %s", err.Error())
			return
		}
		return
	})
	if err != nil {
		return fmt.Errorf("error while running test connection for bigquery with err: %s", err.Error())
	}
	return nil
}

func TestBigQueryIntegration(t *testing.T) {
	// Cleanup resources
	// Dropping temporary dataset
	t.Cleanup(func() {
		require.NoError(t, testhelper.WithConstantBackoff(func() (err error) {
			return handle.DB.Dataset(handle.Schema).DeleteWithContents(context.TODO())
		}), fmt.Sprintf("Failed dropping dataset %s for BigQuery", handle.Schema))
	})

	t.Run("Merge Mode", func(t *testing.T) {
		// Setting up the test configuration
		require.NoError(t, testhelper.SetConfig([]warehouseutils.KeyValue{
			{
				Key:   "Warehouse.bigquery.isDedupEnabled",
				Value: true,
			},
		}))

		// Setting up the warehouseTest
		warehouseTest := &testhelper.WareHouseTest{
			Client: &client.Client{
				BQ:   handle.DB,
				Type: client.BQClient,
			},
			WriteKey:             handle.WriteKey,
			Schema:               handle.Schema,
			Tables:               handle.Tables,
			TablesQueryFrequency: testhelper.LongRunningQueryFrequency,
			EventsCountMap:       testhelper.DefaultEventMap(),
			MessageId:            uuid.Must(uuid.NewV4()).String(),
			UserId:               testhelper.GetUserId(warehouseutils.BQ),
			Provider:             warehouseutils.BQ,
		}

		// Scenario 1
		// Sending the first set of events.
		// Since we handle dedupe on the staging table, we need to check if the first set of events reached the destination.
		testhelper.SendEvents(t, warehouseTest)
		testhelper.SendEvents(t, warehouseTest)
		testhelper.SendEvents(t, warehouseTest)
		// Integrated events has events set to skipReservedKeywordsEscaping to True
		// Eg: Since groups is reserved keyword in BQ, table populated will be groups if true else _groups
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
			"_groups":       1,
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
			"_groups":       1,
			"gateway":       48,
			"batchRT":       64,
		}
		testhelper.VerifyingGatewayEvents(t, warehouseTest)
		testhelper.VerifyingBatchRouterEvents(t, warehouseTest)
		testhelper.VerifyingTablesEventCount(t, warehouseTest)
	})

	t.Run("Append Mode", func(t *testing.T) {
		// Setting up the test configuration
		require.NoError(t, testhelper.SetConfig([]warehouseutils.KeyValue{
			{
				Key:   "Warehouse.bigquery.isDedupEnabled",
				Value: false,
			},
		}))

		// Setting up the warehouseTest
		warehouseTest := &testhelper.WareHouseTest{
			Client: &client.Client{
				BQ:   handle.DB,
				Type: client.BQClient,
			},
			WriteKey:             handle.WriteKey,
			Schema:               handle.Schema,
			Tables:               handle.Tables,
			TablesQueryFrequency: testhelper.LongRunningQueryFrequency,
			EventsCountMap:       testhelper.DefaultEventMap(),
			MessageId:            uuid.Must(uuid.NewV4()).String(),
			UserId:               testhelper.GetUserId(warehouseutils.BQ),
			Provider:             warehouseutils.BQ,
		}

		// Scenario 1
		// Sending the first set of events.
		// Since we don't handle dedupe on the staging table, we can just send the events.
		// And then verify the count on the warehouse.
		// Sending 1 event to groups and 3 to _groups
		testhelper.SendEvents(t, warehouseTest)
		// Integrated events has events set to skipReservedKeywordsEscaping to True
		// Eg: Since groups is reserved keyword in BQ, table populated will be groups if true else _groups
		testhelper.SendIntegratedEvents(t, warehouseTest)
		testhelper.SendModifiedEvents(t, warehouseTest)
		testhelper.SendModifiedEvents(t, warehouseTest)

		// Setting up the events map
		// Checking for Gateway and Batch router events
		// Checking for the events count for each table
		// Since because of append events count will be equal to the number of events being sent
		warehouseTest.EventsCountMap = testhelper.EventsCountMap{
			"identifies":    4,
			"users":         1,
			"tracks":        4,
			"product_track": 4,
			"pages":         4,
			"screens":       4,
			"aliases":       4,
			"groups":        1,
			"_groups":       3,
			"gateway":       24,
			"batchRT":       32,
		}
		testhelper.VerifyingGatewayEvents(t, warehouseTest)
		testhelper.VerifyingBatchRouterEvents(t, warehouseTest)
		testhelper.VerifyingTablesEventCount(t, warehouseTest)
	})
}

func TestUnsupportedCredentials(t *testing.T) {
	credentials := bigquery2.BQCredentialsT{
		ProjectID:   "projectId",
		Credentials: "{\"installed\":{\"client_id\":\"1234.apps.googleusercontent.com\",\"project_id\":\"project_id\",\"auth_uri\":\"https://accounts.google.com/o/oauth2/auth\",\"token_uri\":\"https://oauth2.googleapis.com/token\",\"auth_provider_x509_cert_url\":\"https://www.googleapis.com/oauth2/v1/certs\",\"client_secret\":\"client_secret\",\"redirect_uris\":[\"urn:ietf:wg:oauth:2.0:oob\",\"http://localhost\"]}}",
	}

	_, err := bigquery2.Connect(context.Background(), &credentials)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "client_credentials.json file is not supported")
}

func TestMain(m *testing.M) {
	_, exists := os.LookupEnv(TestCredentialsKey)
	if !exists {
		log.Println("Skipping Bigquery Test as the Test credentials does not exits.")
		return
	}

	handle = &TestHandle{
		WriteKey: "J77aX7tLFJ84qYU6UrN8ctecwZt",
		Schema:   testhelper.GetSchema(warehouseutils.BQ, TestSchemaKey),
		Tables:   []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "_groups", "groups"},
	}
	os.Exit(testhelper.Run(m, handle))
}
