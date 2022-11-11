//go:build warehouse_integration && !sources_integration

package bigquery_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"

	"github.com/rudderlabs/rudder-server/utils/timeutil"

	"github.com/stretchr/testify/require"

	"cloud.google.com/go/bigquery"

	"github.com/gofrs/uuid"
	bigquery2 "github.com/rudderlabs/rudder-server/warehouse/bigquery"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/testhelper"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/stretchr/testify/assert"
)

type TestHandle struct{}

func (*TestHandle) VerifyConnection() error {
	credentials, err := testhelper.BigqueryCredentials()
	if err != nil {
		return err
	}
	return testhelper.WithConstantBackoff(func() (err error) {
		_, err = bigquery2.Connect(context.TODO(), &credentials)
		if err != nil {
			err = fmt.Errorf("could not connect to warehouse bigquery with error: %s", err.Error())
			return
		}
		return
	})
}

func TestBigQueryIntegration(t *testing.T) {
	credentials, err := testhelper.BigqueryCredentials()
	require.NoError(t, err)

	var (
		schema   = testhelper.Schema(warehouseutils.BQ, testhelper.BigqueryIntegrationTestSchema)
		writeKey = "J77aX7tLFJ84qYU6UrN8ctecwZt"
		tables   = []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "_groups", "groups"}
		db       *bigquery.Client
	)

	db, err = bigquery2.Connect(context.TODO(), &credentials)
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, testhelper.WithConstantBackoff(func() (err error) {
			return db.Dataset(schema).DeleteWithContents(context.TODO())
		}), fmt.Sprintf("Failed dropping dataset %s for BigQuery", schema))
	})

	t.Run("Merge Mode", func(t *testing.T) {
		require.NoError(t, testhelper.SetConfig([]warehouseutils.KeyValue{
			{
				Key:   "Warehouse.bigquery.isDedupEnabled",
				Value: true,
			},
		}))

		warehouseTest := &testhelper.WareHouseTest{
			Client: &client.Client{
				BQ:   db,
				Type: client.BQClient,
			},
			WriteKey:      writeKey,
			Schema:        schema,
			Tables:        tables,
			MessageId:     uuid.Must(uuid.NewV4()).String(),
			Provider:      warehouseutils.BQ,
			SourceID:      "24p1HhPk09FW25Kuzxv7GshCLKR",
			DestinationID: "26Bgm9FrQDZjvadSwAlpd35atwn",
		}

		// Scenario 1
		warehouseTest.TimestampBeforeSendingEvents = timeutil.Now()
		warehouseTest.UserId = testhelper.GetUserId(warehouseutils.BQ)

		sendEventsMap := testhelper.SendEventsMap()
		testhelper.SendEvents(t, warehouseTest, sendEventsMap)
		testhelper.SendEvents(t, warehouseTest, sendEventsMap)
		testhelper.SendEvents(t, warehouseTest, sendEventsMap)
		testhelper.SendIntegratedEvents(t, warehouseTest, sendEventsMap)

		testhelper.VerifyEventsInStagingFiles(t, warehouseTest, stagingFilesEventsMap())
		testhelper.VerifyEventsInLoadFiles(t, warehouseTest, loadFilesEventsMap())
		testhelper.VerifyEventsInTableUploads(t, warehouseTest, tableUploadsEventsMap())
		testhelper.VerifyEventsInWareHouse(t, warehouseTest, mergeEventsMap())

		// Scenario 2
		warehouseTest.TimestampBeforeSendingEvents = timeutil.Now()
		warehouseTest.UserId = testhelper.GetUserId(warehouseutils.BQ)

		sendEventsMap = testhelper.SendEventsMap()
		testhelper.SendModifiedEvents(t, warehouseTest, sendEventsMap)
		testhelper.SendModifiedEvents(t, warehouseTest, sendEventsMap)
		testhelper.SendModifiedEvents(t, warehouseTest, sendEventsMap)
		testhelper.SendIntegratedEvents(t, warehouseTest, sendEventsMap)

		testhelper.VerifyEventsInStagingFiles(t, warehouseTest, stagingFilesEventsMap())
		testhelper.VerifyEventsInLoadFiles(t, warehouseTest, loadFilesEventsMap())
		testhelper.VerifyEventsInTableUploads(t, warehouseTest, tableUploadsEventsMap())
		testhelper.VerifyEventsInWareHouse(t, warehouseTest, mergeEventsMap())

		testhelper.VerifyWorkspaceIDInStats(t)
	})

	t.Run("Append Mode", func(t *testing.T) {
		testCases := []struct {
			name                                string
			customPartitionsEnabledWorkspaceIDs []string
			prerequisite                        func(t *testing.T)
		}{
			{
				name: "Append mode without custom partitions",
			},
			{
				name:                                "Append mode with custom partitions",
				customPartitionsEnabledWorkspaceIDs: []string{"BpLnfgDsc2WD8F2qNfHK5a84jjJ"},
				prerequisite: func(t *testing.T) {
					err = db.Dataset(schema).Create(context.Background(), &bigquery.DatasetMetadata{
						Location: "US",
					})
					require.NoError(t, err)

					err = db.Dataset(schema).Table("tracks").Create(
						context.Background(),
						&bigquery.TableMetadata{
							Schema: []*bigquery.FieldSchema{{
								Name: "timestamp",
								Type: bigquery.TimestampFieldType,
							}},
							TimePartitioning: &bigquery.TimePartitioning{
								Field: "timestamp",
							},
						})
					require.NoError(t, err)
				},
			},
		}

		for _, tc := range testCases {
			tc := tc

			t.Run(tc.name, func(t *testing.T) {
				_ = db.Dataset(schema).DeleteWithContents(context.TODO())

				if tc.prerequisite != nil {
					tc.prerequisite(t)
				}

				require.NoError(t, testhelper.SetConfig([]warehouseutils.KeyValue{
					{
						Key:   "Warehouse.bigquery.isDedupEnabled",
						Value: false,
					},
					{
						Key:   "Warehouse.bigquery.customPartitionsEnabledWorkspaceIDs",
						Value: tc.customPartitionsEnabledWorkspaceIDs,
					},
				}))

				warehouseTest := &testhelper.WareHouseTest{
					Client: &client.Client{
						BQ:   db,
						Type: client.BQClient,
					},
					WriteKey:      writeKey,
					Schema:        schema,
					Tables:        tables,
					MessageId:     uuid.Must(uuid.NewV4()).String(),
					Provider:      warehouseutils.BQ,
					SourceID:      "24p1HhPk09FW25Kuzxv7GshCLKR",
					DestinationID: "26Bgm9FrQDZjvadSwAlpd35atwn",
				}

				// Scenario 1
				warehouseTest.TimestampBeforeSendingEvents = timeutil.Now()
				warehouseTest.UserId = testhelper.GetUserId(warehouseutils.BQ)

				sendEventsMap := testhelper.SendEventsMap()
				testhelper.SendEvents(t, warehouseTest, sendEventsMap)
				testhelper.SendIntegratedEvents(t, warehouseTest, sendEventsMap)
				testhelper.SendModifiedEvents(t, warehouseTest, sendEventsMap)
				testhelper.SendModifiedEvents(t, warehouseTest, sendEventsMap)

				testhelper.VerifyEventsInStagingFiles(t, warehouseTest, stagingFilesEventsMap())
				testhelper.VerifyEventsInLoadFiles(t, warehouseTest, loadFilesEventsMap())
				testhelper.VerifyEventsInTableUploads(t, warehouseTest, tableUploadsEventsMap())
				testhelper.VerifyEventsInWareHouse(t, warehouseTest, appendEventsMap())

				testhelper.VerifyWorkspaceIDInStats(t)
			})
		}
	})
}

func TestBigQueryConfigurationValidation(t *testing.T) {
	configurations := testhelper.PopulateTemplateConfigurations()
	bqCredentials, err := testhelper.BigqueryCredentials()
	require.NoError(t, err)

	destination := backendconfig.DestinationT{
		ID: "26Bgm9FrQDZjvadSwAlpd35atwn",
		Config: map[string]interface{}{
			"project":       configurations["bigqueryProjectID"],
			"location":      configurations["bigqueryLocation"],
			"bucketName":    configurations["bigqueryBucketName"],
			"credentials":   bqCredentials.Credentials,
			"prefix":        "",
			"namespace":     configurations["bigqueryNamespace"],
			"syncFrequency": "30",
		},
		DestinationDefinition: backendconfig.DestinationDefinitionT{
			ID:          "1UmeD7xhVGHsPDEHoCiSPEGytS3",
			Name:        "BQ",
			DisplayName: "BigQuery",
		},
		Name:       "bigquery-wh-integration",
		Enabled:    true,
		RevisionID: "29eejWUH80lK1abiB766fzv5Iba",
	}
	testhelper.VerifyingConfigurationTest(t, destination)
}

func loadFilesEventsMap() testhelper.EventsCountMap {
	eventsMap := testhelper.LoadFilesEventsMap()
	eventsMap["groups"] = 1
	eventsMap["_groups"] = 3
	return eventsMap
}

func tableUploadsEventsMap() testhelper.EventsCountMap {
	eventsMap := testhelper.TableUploadsEventsMap()
	eventsMap["groups"] = 1
	eventsMap["_groups"] = 3
	return eventsMap
}

func stagingFilesEventsMap() testhelper.EventsCountMap {
	return testhelper.EventsCountMap{
		"wh_staging_files": 34, // Since extra 2 merge events because of ID resolution
	}
}

func mergeEventsMap() testhelper.EventsCountMap {
	return testhelper.EventsCountMap{
		"identifies":    1,
		"users":         1,
		"tracks":        1,
		"product_track": 1,
		"pages":         1,
		"screens":       1,
		"aliases":       1,
		"groups":        1,
		"_groups":       1,
	}
}

func appendEventsMap() testhelper.EventsCountMap {
	eventsMap := testhelper.WarehouseEventsMap()
	eventsMap["groups"] = 1
	eventsMap["_groups"] = 3
	return eventsMap
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
	_, exists := os.LookupEnv(testhelper.BigqueryIntegrationTestCredentials)
	if !exists {
		log.Println("Skipping Bigquery Test as the Test credentials does not exists.")
		return
	}

	os.Exit(testhelper.Run(m, &TestHandle{}))
}
