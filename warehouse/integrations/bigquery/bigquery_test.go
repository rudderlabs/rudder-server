package bigquery_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"google.golang.org/api/option"

	"github.com/rudderlabs/rudder-server/testhelper/workspaceConfig"

	"github.com/rudderlabs/compose-test/testcompose"
	kitHelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	"github.com/rudderlabs/rudder-server/runner"
	"github.com/rudderlabs/rudder-server/testhelper/health"
	"github.com/rudderlabs/rudder-server/warehouse/encoding"

	"github.com/rudderlabs/rudder-server/warehouse/integrations/testhelper"

	bigquery2 "github.com/rudderlabs/rudder-server/warehouse/integrations/bigquery"
	bqHeloer "github.com/rudderlabs/rudder-server/warehouse/integrations/bigquery/testhelper"

	"cloud.google.com/go/bigquery"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/validations"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/warehouse/client"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/stretchr/testify/assert"
)

func TestIntegration(t *testing.T) {
	if os.Getenv("SLOW") != "1" {
		t.Skip("Skipping tests. Add 'SLOW=1' env var to run test.")
	}
	if !bqHeloer.IsBQTestCredentialsAvailable() {
		t.Skipf("Skipping %s as %s is not set", t.Name(), bqHeloer.TestKey)
	}

	c := testcompose.New(t, "testdata/docker-compose.yml")

	t.Cleanup(func() {
		c.Stop(context.Background())
	})
	c.Start(context.Background())

	misc.Init()
	validations.Init()
	warehouseutils.Init()
	encoding.Init()

	jobsDBPort := c.Port("jobsDb", 5432)
	transformerPort := c.Port("transformer", 9090)

	httpPort, err := kitHelper.GetFreePort()
	require.NoError(t, err)
	httpAdminPort, err := kitHelper.GetFreePort()
	require.NoError(t, err)

	workspaceID := warehouseutils.RandHex()
	sourceID := warehouseutils.RandHex()
	destinationID := warehouseutils.RandHex()
	writeKey := warehouseutils.RandHex()
	sourcesSourceID := warehouseutils.RandHex()
	sourcesDestinationID := warehouseutils.RandHex()
	sourcesWriteKey := warehouseutils.RandHex()

	destType := warehouseutils.BQ

	namespace := testhelper.RandSchema(destType)
	sourcesNamespace := testhelper.RandSchema(destType)

	bqTestCredentials, err := bqHeloer.GetBQTestCredentials()
	require.NoError(t, err)

	escapedCredentials, err := json.Marshal(bqTestCredentials.Credentials)
	require.NoError(t, err)

	escapedCredentialsTrimmedStr := strings.Trim(string(escapedCredentials), `"`)

	transformerEndpoint := fmt.Sprintf("http://localhost:%d", transformerPort)

	templateConfigurations := map[string]any{
		"workspaceID":          workspaceID,
		"sourceID":             sourceID,
		"destinationID":        destinationID,
		"writeKey":             writeKey,
		"sourcesSourceID":      sourcesSourceID,
		"sourcesDestinationID": sourcesDestinationID,
		"sourcesWriteKey":      sourcesWriteKey,
		"namespace":            namespace,
		"project":              bqTestCredentials.ProjectID,
		"location":             bqTestCredentials.Location,
		"bucketName":           bqTestCredentials.BucketName,
		"credentials":          escapedCredentialsTrimmedStr,
		"sourcesNamespace":     sourcesNamespace,
	}
	workspaceConfigPath := workspaceConfig.CreateTempFile(t, "testdata/template.json", templateConfigurations)

	t.Setenv("JOBS_DB_HOST", "localhost")
	t.Setenv("JOBS_DB_NAME", "jobsdb")
	t.Setenv("JOBS_DB_DB_NAME", "jobsdb")
	t.Setenv("JOBS_DB_USER", "rudder")
	t.Setenv("JOBS_DB_PASSWORD", "password")
	t.Setenv("JOBS_DB_SSL_MODE", "disable")
	t.Setenv("JOBS_DB_PORT", strconv.Itoa(jobsDBPort))
	t.Setenv("WAREHOUSE_JOBS_DB_HOST", "localhost")
	t.Setenv("WAREHOUSE_JOBS_DB_NAME", "jobsdb")
	t.Setenv("WAREHOUSE_JOBS_DB_DB_NAME", "jobsdb")
	t.Setenv("WAREHOUSE_JOBS_DB_USER", "rudder")
	t.Setenv("WAREHOUSE_JOBS_DB_PASSWORD", "password")
	t.Setenv("WAREHOUSE_JOBS_DB_SSL_MODE", "disable")
	t.Setenv("WAREHOUSE_JOBS_DB_PORT", strconv.Itoa(jobsDBPort))
	t.Setenv("GO_ENV", "production")
	t.Setenv("LOG_LEVEL", "INFO")
	t.Setenv("INSTANCE_ID", "1")
	t.Setenv("ALERT_PROVIDER", "pagerduty")
	t.Setenv("CONFIG_PATH", "../../../config/config.yaml")
	t.Setenv("DEST_TRANSFORM_URL", transformerEndpoint)
	t.Setenv("RSERVER_WAREHOUSE_BIGQUERY_MAX_PARALLEL_LOADS", "8")
	t.Setenv("RSERVER_WAREHOUSE_WAREHOUSE_SYNC_FREQ_IGNORE", "true")
	t.Setenv("RSERVER_WAREHOUSE_UPLOAD_FREQ_IN_S", "10")
	t.Setenv("RSERVER_WAREHOUSE_ENABLE_JITTER_FOR_SYNCS", "false")
	t.Setenv("RSERVER_WAREHOUSE_ENABLE_IDRESOLUTION", "true")
	t.Setenv("RSERVER_BACKEND_CONFIG_CONFIG_FROM_FILE", "true")
	t.Setenv("RUDDER_ADMIN_PASSWORD", "password")
	t.Setenv("RUDDER_GRACEFUL_SHUTDOWN_TIMEOUT_EXIT", "false")
	t.Setenv("RSERVER_WAREHOUSE_BIGQUERY_ENABLE_DELETE_BY_JOBS", "true")
	t.Setenv("RSERVER_GATEWAY_WEB_PORT", strconv.Itoa(httpPort))
	t.Setenv("RSERVER_GATEWAY_ADMIN_WEB_PORT", strconv.Itoa(httpAdminPort))
	t.Setenv("RSERVER_ENABLE_STATS", "false")
	t.Setenv("RSERVER_BACKEND_CONFIG_CONFIG_JSONPATH", workspaceConfigPath)
	t.Setenv("RUDDER_TMPDIR", t.TempDir())
	if testing.Verbose() {
		t.Setenv("LOG_LEVEL", "DEBUG")
	}

	svcDone := make(chan struct{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		r := runner.New(runner.ReleaseInfo{})
		_ = r.Run(ctx, []string{"bigquery-integration-test"})

		close(svcDone)
	}()
	t.Cleanup(func() { <-svcDone })

	serviceHealthEndpoint := fmt.Sprintf("http://localhost:%d/health", httpPort)
	health.WaitUntilReady(ctx, t, serviceHealthEndpoint, time.Minute, time.Second, "serviceHealthEndpoint")

	t.Run("Event flow", func(t *testing.T) {
		db, err := bigquery.NewClient(
			context.TODO(),
			bqTestCredentials.ProjectID, option.WithCredentialsJSON([]byte(bqTestCredentials.Credentials)),
		)
		require.NoError(t, err)

		jobsDB := testhelper.JobsDB(t, jobsDBPort)

		t.Cleanup(func() {
			for _, dataset := range []string{namespace, sourcesNamespace} {
				require.NoError(t, testhelper.WithConstantRetries(func() error {
					return db.Dataset(dataset).DeleteWithContents(context.TODO())
				}))
			}
		})

		testcase := []struct {
			name                                string
			writeKey                            string
			schema                              string
			sourceID                            string
			destinationID                       string
			messageID                           string
			tables                              []string
			stagingFilesEventsMap               testhelper.EventsCountMap
			stagingFilesModifiedEventsMap       testhelper.EventsCountMap
			loadFilesEventsMap                  testhelper.EventsCountMap
			tableUploadsEventsMap               testhelper.EventsCountMap
			warehouseEventsMap                  testhelper.EventsCountMap
			asyncJob                            bool
			skipModifiedEvents                  bool
			prerequisite                        func(t testing.TB)
			isDedupEnabled                      bool
			customPartitionsEnabledWorkspaceIDs string
		}{
			{
				name:                          "Merge mode",
				writeKey:                      writeKey,
				schema:                        namespace,
				tables:                        []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"},
				sourceID:                      sourceID,
				destinationID:                 destinationID,
				messageID:                     misc.FastUUID().String(),
				stagingFilesEventsMap:         stagingFilesEventsMap(),
				stagingFilesModifiedEventsMap: stagingFilesEventsMap(),
				loadFilesEventsMap:            loadFilesEventsMap(),
				tableUploadsEventsMap:         tableUploadsEventsMap(),
				warehouseEventsMap:            mergeEventsMap(),
				isDedupEnabled:                true,
				prerequisite: func(t testing.TB) {
					t.Helper()

					_ = db.Dataset(namespace).DeleteWithContents(context.TODO())
				},
			},
			{
				name:          "Async Job",
				writeKey:      sourcesWriteKey,
				sourceID:      sourcesSourceID,
				destinationID: sourcesDestinationID,
				schema:        sourcesNamespace,
				tables:        []string{"tracks", "google_sheet"},
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
				isDedupEnabled:        false,
				prerequisite: func(t testing.TB) {
					t.Helper()

					_ = db.Dataset(namespace).DeleteWithContents(context.TODO())
				},
			},
			{
				name:                          "Append mode",
				schema:                        namespace,
				tables:                        []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"},
				writeKey:                      writeKey,
				sourceID:                      sourceID,
				destinationID:                 destinationID,
				messageID:                     misc.FastUUID().String(),
				stagingFilesEventsMap:         stagingFilesEventsMap(),
				stagingFilesModifiedEventsMap: stagingFilesEventsMap(),
				loadFilesEventsMap:            loadFilesEventsMap(),
				tableUploadsEventsMap:         tableUploadsEventsMap(),
				warehouseEventsMap:            appendEventsMap(),
				skipModifiedEvents:            true,
				isDedupEnabled:                false,
				prerequisite: func(t testing.TB) {
					t.Helper()

					_ = db.Dataset(namespace).DeleteWithContents(context.TODO())
				},
			},
			{
				name:                                "Append mode with custom partition",
				schema:                              namespace,
				tables:                              []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"},
				writeKey:                            writeKey,
				sourceID:                            sourceID,
				destinationID:                       destinationID,
				messageID:                           misc.FastUUID().String(),
				stagingFilesEventsMap:               stagingFilesEventsMap(),
				stagingFilesModifiedEventsMap:       stagingFilesEventsMap(),
				loadFilesEventsMap:                  loadFilesEventsMap(),
				tableUploadsEventsMap:               tableUploadsEventsMap(),
				warehouseEventsMap:                  appendEventsMap(),
				skipModifiedEvents:                  true,
				isDedupEnabled:                      false,
				customPartitionsEnabledWorkspaceIDs: workspaceID,
				prerequisite: func(t testing.TB) {
					t.Helper()

					_ = db.Dataset(namespace).DeleteWithContents(context.TODO())

					err = db.Dataset(namespace).Create(context.Background(), &bigquery.DatasetMetadata{
						Location: "US",
					})
					require.NoError(t, err)

					err = db.Dataset(namespace).Table("tracks").Create(
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
				},
			},
		}

		for _, tc := range testcase {
			tc := tc

			t.Run(tc.name, func(t *testing.T) {
				t.Setenv("RSERVER_WAREHOUSE_BIGQUERY_IS_DEDUP_ENABLED", strconv.FormatBool(tc.isDedupEnabled))
				t.Setenv("RSERVER_WAREHOUSE_BIGQUERY_CUSTOM_PARTITIONS_ENABLED_WORKSPACE_IDS", tc.customPartitionsEnabledWorkspaceIDs)

				if tc.prerequisite != nil {
					tc.prerequisite(t)
				}

				sqlClient := &client.Client{
					BQ:   db,
					Type: client.BQClient,
				}

				conf := map[string]interface{}{
					"bucketName":  bqTestCredentials.BucketName,
					"credentials": bqTestCredentials.Credentials,
				}

				t.Log("verifying test case 1")
				ts1 := testhelper.TestConfig{
					WriteKey:              tc.writeKey,
					Schema:                tc.schema,
					Tables:                tc.tables,
					SourceID:              tc.sourceID,
					DestinationID:         tc.destinationID,
					MessageID:             tc.messageID,
					StagingFilesEventsMap: tc.stagingFilesEventsMap,
					LoadFilesEventsMap:    tc.loadFilesEventsMap,
					TableUploadsEventsMap: tc.tableUploadsEventsMap,
					WarehouseEventsMap:    tc.warehouseEventsMap,
					Config:                conf,
					WorkspaceID:           workspaceID,
					DestinationType:       destType,
					JobsDB:                jobsDB,
					HTTPPort:              httpPort,
					Client:                sqlClient,
					JobRunID:              misc.FastUUID().String(),
					TaskRunID:             misc.FastUUID().String(),
					EventTemplateCountMap: testhelper.DefaultEventsCountMap(),
					UserID:                testhelper.GetUserId(destType),
				}
				if tc.asyncJob {
					ts1.EventTemplateCountMap = testhelper.DefaultSourcesEventsCountMap()
				}
				ts1.VerifyEvents(t)

				if tc.skipModifiedEvents {
					return
				}

				t.Log("verifying test case 2")
				ts2 := testhelper.TestConfig{
					WriteKey:              tc.writeKey,
					Schema:                tc.schema,
					Tables:                tc.tables,
					SourceID:              tc.sourceID,
					DestinationID:         tc.destinationID,
					MessageID:             tc.messageID,
					StagingFilesEventsMap: tc.stagingFilesModifiedEventsMap,
					LoadFilesEventsMap:    tc.loadFilesEventsMap,
					TableUploadsEventsMap: tc.tableUploadsEventsMap,
					WarehouseEventsMap:    tc.warehouseEventsMap,
					AsyncJob:              tc.asyncJob,
					Config:                conf,
					WorkspaceID:           workspaceID,
					DestinationType:       destType,
					JobsDB:                jobsDB,
					HTTPPort:              httpPort,
					Client:                sqlClient,
					JobRunID:              misc.FastUUID().String(),
					TaskRunID:             misc.FastUUID().String(),
					EventTemplateCountMap: testhelper.ModifiedEventsCountMap(),
					UserID:                testhelper.GetUserId(destType),
				}
				if tc.asyncJob {
					ts2.UserID = ts1.UserID
					ts2.EventTemplateCountMap = testhelper.DefaultSourcesModifiedEventsCountMap()
				}
				ts2.VerifyEvents(t)
			})
		}
	})

	t.Run("Validations", func(t *testing.T) {
		dest := backendconfig.DestinationT{
			ID: destinationID,
			Config: map[string]interface{}{
				"project":       bqTestCredentials.ProjectID,
				"location":      bqTestCredentials.Location,
				"bucketName":    bqTestCredentials.BucketName,
				"credentials":   bqTestCredentials.Credentials,
				"prefix":        "",
				"namespace":     namespace,
				"syncFrequency": "30",
			},
			DestinationDefinition: backendconfig.DestinationDefinitionT{
				ID:          "1UmeD7xhVGHsPDEHoCiSPEGytS3",
				Name:        "BQ",
				DisplayName: "BigQuery",
			},
			Name:       "bigquery-integration",
			Enabled:    true,
			RevisionID: destinationID,
		}
		testhelper.VerifyConfigurationTest(t, dest)
	})
}

func makeUpperCase(eventMap testhelper.EventsCountMap) testhelper.EventsCountMap {
	newEventMap := make(testhelper.EventsCountMap)
	for k, v := range eventMap {
		newEventMap[strings.ToUpper(k)] = v
	}
	return newEventMap
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
		"groups":        4,
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
		"groups":        4,
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
		"groups":        4,
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
