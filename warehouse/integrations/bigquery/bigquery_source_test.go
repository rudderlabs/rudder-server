//go:build sources_integration && !warehouse_integration

package bigquery_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"testing"

	bigquery2 "github.com/rudderlabs/rudder-server/warehouse/integrations/bigquery"

	"github.com/rudderlabs/rudder-server/utils/misc"

	"github.com/stretchr/testify/require"

	"cloud.google.com/go/bigquery"

	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/testhelper"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/stretchr/testify/assert"
)

type TestHandle struct {
	DB             *bigquery.Client
	WriteKey       string
	Schema         string
	Tables         []string
	SourceId       string
	DestinationId  string
	SourceWriteKey string
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
			SourceWriteKey:        handle.SourceWriteKey,
			SourceID:              handle.SourceId,
			DestinationID:         handle.DestinationId,
			Schema:                handle.Schema,
			Tables:                handle.Tables,
			MessageId:             misc.FastUUID().String(),
			Provider:              warehouseutils.BQ,
			LatestSourceRunConfig: testhelper.DefaultSourceRunConfig(),
		}

		warehouseTest.UserId = testhelper.GetUserId(warehouseutils.BQ)
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
		SourceWriteKey: "2DkCpXZcUjnK2fcpUD3LmjPI7J6",
		SourceId:       "2DkCpUr0xfiPLKJxIwqyqfyHdq4",
		DestinationId:  "26Bgm9FrQDZjvadSwAlpd35atwn",
		Schema:         testhelper.Schema(warehouseutils.BQ, testhelper.BigqueryIntegrationTestSchema),
		Tables:         []string{"tracks", "google_sheet"},
	}
	os.Exit(testhelper.Run(m, handle))
}
