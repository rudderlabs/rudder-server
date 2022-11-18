//go:build warehouse_integration && !sources_integration

package bigquery_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"cloud.google.com/go/bigquery"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/validations"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"

	"github.com/rudderlabs/rudder-server/utils/timeutil"

	"github.com/stretchr/testify/require"

	bigquery2 "github.com/rudderlabs/rudder-server/warehouse/bigquery"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/testhelper"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/stretchr/testify/assert"
)

func TestBigQueryIntegration(t *testing.T) {
	t.Parallel()

	if _, exists := os.LookupEnv(testhelper.BigqueryIntegrationTestCredentials); !exists {
		t.Skipf("Skipping %s as %s is not set", t.Name(), testhelper.BigqueryIntegrationTestCredentials)
	}

	bigquery2.Init()

	credentials, err := testhelper.BigqueryCredentials()
	require.NoError(t, err)

	db, err := bigquery2.Connect(context.TODO(), &credentials)
	require.NoError(t, err)

	schema := testhelper.Schema(warehouseutils.BQ, testhelper.BigqueryIntegrationTestSchema)
	sourceSchema := fmt.Sprintf("%s_%s", schema, "SOURCES")
	tables := []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "_groups", "groups"}

	t.Cleanup(func() {
		for _, dataset := range []string{schema, sourceSchema} {
			require.NoError(t,
				testhelper.WithConstantBackoff(func() (err error) {
					return db.Dataset(dataset).DeleteWithContents(context.TODO())
				}),
				fmt.Sprintf("Failed dropping dataset %s for BigQuery", dataset),
			)
		}
	})

	jobsDB := testhelper.SetUpJobsDB(t)

	testcase := []struct {
		name                  string
		schema                string
		writeKey              string
		sourceID              string
		destinationID         string
		tables                []string
		skipUserCreation      bool
		eventsMap             testhelper.EventsCountMap
		stagingFilesEventsMap testhelper.EventsCountMap
		loadFilesEventsMap    testhelper.EventsCountMap
		tableUploadsEventsMap testhelper.EventsCountMap
		warehouseEventsMap    testhelper.EventsCountMap
		asyncJob              func(t testing.TB, wareHouseTest *testhelper.WareHouseTest)
		prerequisite          func(t *testing.T)
	}{
		{
			name:                  "Upload JOB With Merge Mode",
			schema:                schema,
			tables:                []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"},
			writeKey:              "J77aX7tLFJ84qYU6UrN8ctecwZt",
			sourceID:              "24p1HhPk09FW25Kuzxv7GshCLKR",
			destinationID:         "26Bgm9FrQDZjvadSwAlpd35atwn",
			eventsMap:             testhelper.SendEventsMap(),
			stagingFilesEventsMap: stagingFilesEventsMap(),
			loadFilesEventsMap:    loadFilesEventsMap(),
			tableUploadsEventsMap: tableUploadsEventsMap(),
			warehouseEventsMap:    mergeEventsMap(),
			prerequisite: func(t *testing.T) {
				require.NoError(t, testhelper.SetConfig([]warehouseutils.KeyValue{
					{
						Key:   "Warehouse.bigquery.isDedupEnabled",
						Value: true,
					},
				}))
			},
		},
		{
			name:                  "Async JOB",
			schema:                sourceSchema,
			tables:                []string{"tracks", "google_sheet"},
			writeKey:              "J77aeABtLFJ84qYU6UrN8ctewZt",
			sourceID:              "2DkCpUr0xgjfBNasIwqyqfyHdq4",
			destinationID:         "26Bgm9FrQDZjvadBnalpd35atwn",
			eventsMap:             testhelper.SourcesSendEventMap(),
			stagingFilesEventsMap: testhelper.SourcesStagingFilesEventsMap(),
			loadFilesEventsMap:    testhelper.SourcesLoadFilesEventsMap(),
			tableUploadsEventsMap: testhelper.SourcesTableUploadsEventsMap(),
			warehouseEventsMap:    testhelper.SourcesWarehouseEventsMap(),
			asyncJob:              testhelper.VerifyAsyncJob,
			prerequisite: func(t *testing.T) {
				require.NoError(t, testhelper.SetConfig([]warehouseutils.KeyValue{
					{
						Key:   "Warehouse.bigquery.isDedupEnabled",
						Value: false,
					},
				}))
			},
		},
	}

	for _, tc := range testcase {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			_ = db.Dataset(tc.schema).DeleteWithContents(context.TODO())

			if tc.prerequisite != nil {
				tc.prerequisite(t)
			}

			warehouseTest := &testhelper.WareHouseTest{
				Client: &client.Client{
					BQ:   db,
					Type: client.BQClient,
				},
				WriteKey:      tc.writeKey,
				Schema:        tc.schema,
				SourceID:      tc.sourceID,
				DestinationID: tc.destinationID,
				Tables:        tc.tables,
				Provider:      warehouseutils.BQ,
			}

			// Scenario 1
			warehouseTest.TimestampBeforeSendingEvents = timeutil.Now()
			warehouseTest.UserId = testhelper.GetUserId(warehouseutils.BQ)
			warehouseTest.JobRunID = misc.FastUUID().String()
			warehouseTest.TaskRunID = misc.FastUUID().String()

			testhelper.SendEvents(t, warehouseTest, tc.eventsMap)
			testhelper.SendEvents(t, warehouseTest, tc.eventsMap)
			testhelper.SendEvents(t, warehouseTest, tc.eventsMap)
			testhelper.SendIntegratedEvents(t, warehouseTest, tc.eventsMap)

			testhelper.VerifyEventsInStagingFiles(t, jobsDB, warehouseTest, tc.stagingFilesEventsMap)
			testhelper.VerifyEventsInLoadFiles(t, jobsDB, warehouseTest, tc.loadFilesEventsMap)
			testhelper.VerifyEventsInTableUploads(t, jobsDB, warehouseTest, tc.tableUploadsEventsMap)
			testhelper.VerifyEventsInWareHouse(t, warehouseTest, tc.warehouseEventsMap)

			// Scenario 2
			warehouseTest.TimestampBeforeSendingEvents = timeutil.Now()
			warehouseTest.JobRunID = misc.FastUUID().String()
			warehouseTest.TaskRunID = misc.FastUUID().String()
			if !tc.skipUserCreation {
				warehouseTest.UserId = testhelper.GetUserId(warehouseutils.BQ)
			}

			testhelper.SendModifiedEvents(t, warehouseTest, tc.eventsMap)
			testhelper.SendModifiedEvents(t, warehouseTest, tc.eventsMap)
			testhelper.SendModifiedEvents(t, warehouseTest, tc.eventsMap)
			testhelper.SendIntegratedEvents(t, warehouseTest, tc.eventsMap)

			testhelper.VerifyEventsInStagingFiles(t, jobsDB, warehouseTest, tc.stagingFilesEventsMap)
			testhelper.VerifyEventsInLoadFiles(t, jobsDB, warehouseTest, tc.loadFilesEventsMap)
			testhelper.VerifyEventsInTableUploads(t, jobsDB, warehouseTest, tc.tableUploadsEventsMap)
			if tc.asyncJob != nil {
				tc.asyncJob(t, warehouseTest)
			}
			testhelper.VerifyEventsInWareHouse(t, warehouseTest, tc.warehouseEventsMap)

			testhelper.VerifyWorkspaceIDInStats(t)
		})
	}

	t.Run("Merge Mode", func(t *testing.T) {

	})

	t.Run("Append Mode", func(t *testing.T) {
		testCases := []struct {
			name                                string
			customPartitionsEnabledWorkspaceIDs []string
			prerequisite                        func(t *testing.T)
		}{
			{
				name: "Without custom partitions",
			},
			{
				name:                                "With custom partitions",
				customPartitionsEnabledWorkspaceIDs: []string{"BpLnfgDsc2WD8F2qNfHK5a84jjJ"},
				prerequisite: func(t *testing.T) {
					t.Helper()
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
					WriteKey:      "J77aX7tLFJ84qYU6UrN8ctecwZt",
					Schema:        schema,
					Tables:        tables,
					MessageID:     misc.FastUUID().String(),
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

				testhelper.VerifyEventsInStagingFiles(t, jobsDB, warehouseTest, stagingFilesEventsMap())
				testhelper.VerifyEventsInLoadFiles(t, jobsDB, warehouseTest, loadFilesEventsMap())
				testhelper.VerifyEventsInTableUploads(t, jobsDB, warehouseTest, tableUploadsEventsMap())
				testhelper.VerifyEventsInWareHouse(t, warehouseTest, appendEventsMap())

				testhelper.VerifyWorkspaceIDInStats(t)
			})
		}
	})
}

func TestBigQueryConfigurationValidation(t *testing.T) {
	t.Parallel()

	if _, exists := os.LookupEnv(testhelper.BigqueryIntegrationTestCredentials); !exists {
		t.Skipf("Skipping %s as %s is not set", t.Name(), testhelper.BigqueryIntegrationTestCredentials)
	}

	misc.Init()
	validations.Init()
	warehouseutils.Init()
	bigquery2.Init()

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
	testhelper.VerifyConfigurationTest(t, destination)
}

func loadFilesEventsMap() testhelper.EventsCountMap {
	eventsMap := testhelper.DefaultLoadFilesEventsMap()
	eventsMap["groups"] = 1
	eventsMap["_groups"] = 3
	return eventsMap
}

func tableUploadsEventsMap() testhelper.EventsCountMap {
	eventsMap := testhelper.DefaultTableUploadsEventsMap()
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
	eventsMap := testhelper.DefaultWarehouseEventsMap()
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
