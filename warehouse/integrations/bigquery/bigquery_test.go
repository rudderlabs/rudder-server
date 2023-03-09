package bigquery_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/rudderlabs/rudder-server/warehouse/encoding"

	"github.com/rudderlabs/rudder-server/warehouse/integrations/testhelper"

	bigquery2 "github.com/rudderlabs/rudder-server/warehouse/integrations/bigquery"

	"cloud.google.com/go/bigquery"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/validations"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/warehouse/client"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/stretchr/testify/assert"
)

func TestIntegrationBigQuery(t *testing.T) {
	if os.Getenv("SLOW") == "0" {
		t.Skip("Skipping tests. Remove 'SLOW=0' env var to run them.")
	}
	if _, exists := os.LookupEnv(testhelper.BigqueryIntegrationTestCredentials); !exists {
		t.Skipf("Skipping %s as %s is not set", t.Name(), testhelper.BigqueryIntegrationTestCredentials)
	}

	t.Parallel()

	bigquery2.Init()

	credentials, err := testhelper.BigqueryCredentials()
	require.NoError(t, err)

	db, err := bigquery2.Connect(context.TODO(), &credentials)
	require.NoError(t, err)

	var (
		jobsDB        = testhelper.SetUpJobsDB(t)
		provider      = warehouseutils.BQ
		schema        = testhelper.Schema(provider, testhelper.BigqueryIntegrationTestSchema)
		sourcesSchema = fmt.Sprintf("%s_%s", schema, "sources")
	)

	t.Cleanup(func() {
		for _, dataset := range []string{schema, sourcesSchema} {
			require.NoError(t,
				testhelper.WithConstantBackoff(func() (err error) {
					return db.Dataset(dataset).DeleteWithContents(context.TODO())
				}),
				fmt.Sprintf("Failed dropping dataset %s for BigQuery", dataset),
			)
		}
	})

	testcase := []struct {
		name                          string
		schema                        string
		writeKey                      string
		sourceID                      string
		destinationID                 string
		messageID                     string
		eventsMap                     testhelper.EventsCountMap
		stagingFilesEventsMap         testhelper.EventsCountMap
		stagingFilesModifiedEventsMap testhelper.EventsCountMap
		loadFilesEventsMap            testhelper.EventsCountMap
		tableUploadsEventsMap         testhelper.EventsCountMap
		warehouseEventsMap            testhelper.EventsCountMap
		asyncJob                      bool
		skipModifiedEvents            bool
		prerequisite                  func(t testing.TB)
		tables                        []string
	}{
		{
			name:                          "Merge mode",
			schema:                        schema,
			tables:                        []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"},
			writeKey:                      "J77aX7tLFJ84qYU6UrN8ctecwZt",
			sourceID:                      "24p1HhPk09FW25Kuzxv7GshCLKR",
			destinationID:                 "26Bgm9FrQDZjvadSwAlpd35atwn",
			messageID:                     misc.FastUUID().String(),
			stagingFilesEventsMap:         stagingFilesEventsMap(),
			stagingFilesModifiedEventsMap: stagingFilesEventsMap(),
			loadFilesEventsMap:            loadFilesEventsMap(),
			tableUploadsEventsMap:         tableUploadsEventsMap(),
			warehouseEventsMap:            mergeEventsMap(),
			prerequisite: func(t testing.TB) {
				t.Helper()

				_ = db.Dataset(schema).DeleteWithContents(context.TODO())

				testhelper.SetConfig(t, []warehouseutils.KeyValue{
					{
						Key:   "Warehouse.bigquery.isDedupEnabled",
						Value: true,
					},
				})
			},
		},
		{
			name:          "Async Job",
			writeKey:      "J77aeABtLFJ84qYU6UrN8ctewZt",
			sourceID:      "2DkCpUr0xgjfBNasIwqyqfyHdq4",
			destinationID: "26Bgm9FrQDZjvadBnalpd35atwn",
			schema:        sourcesSchema,
			tables:        []string{"tracks", "google_sheet"},
			eventsMap:     testhelper.SourcesSendEventsMap(),
			stagingFilesEventsMap: testhelper.EventsCountMap{
				"wh_staging_files": 9, // 8 + 1 (merge events because of ID resolution)
			},
			stagingFilesModifiedEventsMap: testhelper.EventsCountMap{
				"wh_staging_files": 8, // 8 (de-duped by encounteredMergeRuleMap)
			},
			loadFilesEventsMap:    testhelper.SourcesLoadFilesEventsMap(),
			tableUploadsEventsMap: testhelper.SourcesTableUploadsEventsMap(),
			warehouseEventsMap:    testhelper.SourcesWarehouseEventsMap(),
			asyncJob:              true,
			prerequisite: func(t testing.TB) {
				t.Helper()

				_ = db.Dataset(schema).DeleteWithContents(context.TODO())

				testhelper.SetConfig(t, []warehouseutils.KeyValue{
					{
						Key:   "Warehouse.bigquery.isDedupEnabled",
						Value: false,
					},
				})
			},
		},
		{
			name:                          "Append mode",
			schema:                        schema,
			tables:                        []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"},
			writeKey:                      "J77aX7tLFJ84qYU6UrN8ctecwZt",
			sourceID:                      "24p1HhPk09FW25Kuzxv7GshCLKR",
			destinationID:                 "26Bgm9FrQDZjvadSwAlpd35atwn",
			messageID:                     misc.FastUUID().String(),
			stagingFilesEventsMap:         stagingFilesEventsMap(),
			stagingFilesModifiedEventsMap: stagingFilesEventsMap(),
			loadFilesEventsMap:            loadFilesEventsMap(),
			tableUploadsEventsMap:         tableUploadsEventsMap(),
			warehouseEventsMap:            appendEventsMap(),
			skipModifiedEvents:            true,
			prerequisite: func(t testing.TB) {
				t.Helper()

				_ = db.Dataset(schema).DeleteWithContents(context.TODO())

				testhelper.SetConfig(t, []warehouseutils.KeyValue{
					{
						Key:   "Warehouse.bigquery.isDedupEnabled",
						Value: false,
					},
				})
			},
		},
		{
			name:                          "Append mode with custom partition",
			schema:                        schema,
			tables:                        []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"},
			writeKey:                      "J77aX7tLFJ84qYU6UrN8ctecwZt",
			sourceID:                      "24p1HhPk09FW25Kuzxv7GshCLKR",
			destinationID:                 "26Bgm9FrQDZjvadSwAlpd35atwn",
			messageID:                     misc.FastUUID().String(),
			stagingFilesEventsMap:         stagingFilesEventsMap(),
			stagingFilesModifiedEventsMap: stagingFilesEventsMap(),
			loadFilesEventsMap:            loadFilesEventsMap(),
			tableUploadsEventsMap:         tableUploadsEventsMap(),
			warehouseEventsMap:            appendEventsMap(),
			skipModifiedEvents:            true,
			prerequisite: func(t testing.TB) {
				t.Helper()

				_ = db.Dataset(schema).DeleteWithContents(context.TODO())

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
					},
				)
				require.NoError(t, err)

				testhelper.SetConfig(t, []warehouseutils.KeyValue{
					{
						Key:   "Warehouse.bigquery.isDedupEnabled",
						Value: false,
					},
					{
						Key:   "Warehouse.bigquery.customPartitionsEnabledWorkspaceIDs",
						Value: []string{"BpLnfgDsc2WD8F2qNfHK5a84jjJ"},
					},
				})
			},
		},
	}

	for _, tc := range testcase {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			ts := testhelper.WareHouseTest{
				Schema:                tc.schema,
				WriteKey:              tc.writeKey,
				SourceID:              tc.sourceID,
				DestinationID:         tc.destinationID,
				MessageID:             tc.messageID,
				Tables:                tc.tables,
				EventsMap:             tc.eventsMap,
				StagingFilesEventsMap: tc.stagingFilesEventsMap,
				LoadFilesEventsMap:    tc.loadFilesEventsMap,
				TableUploadsEventsMap: tc.tableUploadsEventsMap,
				Prerequisite:          tc.prerequisite,
				WarehouseEventsMap:    tc.warehouseEventsMap,
				AsyncJob:              tc.asyncJob,
				Provider:              provider,
				JobsDB:                jobsDB,
				TaskRunID:             misc.FastUUID().String(),
				JobRunID:              misc.FastUUID().String(),
				UserID:                testhelper.GetUserId(provider),
				Client: &client.Client{
					BQ:   db,
					Type: client.BQClient,
				},
			}
			ts.VerifyEvents(t)

			if tc.skipModifiedEvents {
				return
			}

			if !tc.asyncJob {
				ts.UserID = testhelper.GetUserId(provider)
			}
			ts.StagingFilesEventsMap = tc.stagingFilesModifiedEventsMap
			ts.JobRunID = misc.FastUUID().String()
			ts.TaskRunID = misc.FastUUID().String()
			ts.VerifyModifiedEvents(t)
		})
	}
}

func TestConfigurationValidationBigQuery(t *testing.T) {
	if os.Getenv("SLOW") == "0" {
		t.Skip("Skipping tests. Remove 'SLOW=0' env var to run them.")
	}
	if _, exists := os.LookupEnv(testhelper.BigqueryIntegrationTestCredentials); !exists {
		t.Skipf("Skipping %s as %s is not set", t.Name(), testhelper.BigqueryIntegrationTestCredentials)
	}

	t.Parallel()

	misc.Init()
	validations.Init()
	warehouseutils.Init()
	encoding.Init()
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
	return testhelper.EventsCountMap{
		"identifies":    4,
		"users":         4,
		"tracks":        4,
		"product_track": 4,
		"pages":         4,
		"screens":       4,
		"aliases":       4,
		"groups":        1,
		"_groups":       3,
	}
}

func tableUploadsEventsMap() testhelper.EventsCountMap {
	return testhelper.EventsCountMap{
		"identifies":    4,
		"users":         4,
		"tracks":        4,
		"product_track": 4,
		"pages":         4,
		"screens":       4,
		"aliases":       4,
		"groups":        1,
		"_groups":       3,
	}
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
	return testhelper.EventsCountMap{
		"identifies":    4,
		"users":         1,
		"tracks":        4,
		"product_track": 4,
		"pages":         4,
		"screens":       4,
		"aliases":       4,
		"groups":        1,
		"_groups":       3,
	}
}

func TestUnsupportedCredentials(t *testing.T) {
	credentials := bigquery2.BQCredentials{
		ProjectID:   "projectId",
		Credentials: "{\"installed\":{\"client_id\":\"1234.apps.googleusercontent.com\",\"project_id\":\"project_id\",\"auth_uri\":\"https://accounts.google.com/o/oauth2/auth\",\"token_uri\":\"https://oauth2.googleapis.com/token\",\"auth_provider_x509_cert_url\":\"https://www.googleapis.com/oauth2/v1/certs\",\"client_secret\":\"client_secret\",\"redirect_uris\":[\"urn:ietf:wg:oauth:2.0:oob\",\"http://localhost\"]}}",
	}

	_, err := bigquery2.Connect(context.Background(), &credentials)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "client_credentials.json file is not supported")
}
