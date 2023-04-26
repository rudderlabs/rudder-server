package bigquery_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/rudderlabs/compose-test/testcompose"
	kitHelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	"github.com/rudderlabs/rudder-server/runner"
	"github.com/rudderlabs/rudder-server/testhelper/health"
	"github.com/rudderlabs/rudder-server/warehouse/encoding"

	"github.com/rudderlabs/rudder-server/warehouse/integrations/testhelper"

	bigquery2 "github.com/rudderlabs/rudder-server/warehouse/integrations/bigquery"

	"cloud.google.com/go/bigquery"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/validations"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/warehouse/client"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/stretchr/testify/assert"
)

type bqTestCredentials struct {
	ProjectID   string `json:"projectID"`
	Location    string `json:"location"`
	BucketName  string `json:"bucketName"`
	Credentials string `json:"credentials"`
}

func getBQTestCredentials() (*bqTestCredentials, error) {
	cred, exists := os.LookupEnv(testhelper.BigqueryIntegrationTestCredentials)
	if !exists {
		return nil, fmt.Errorf("bq credentials not found")
	}

	var credentials bqTestCredentials
	err := json.Unmarshal([]byte(cred), &credentials)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal bq credentials: %w", err)
	}

	return &credentials, nil
}

func isBQTestCredentialsAvailable() bool {
	_, err := getBQTestCredentials()
	return err == nil
}

func TestIntegration(t *testing.T) {
	if os.Getenv("SLOW") != "1" {
		t.Skip("Skipping tests. Add 'SLOW=1' env var to run test.")
	}
	if !isBQTestCredentialsAvailable() {
		t.Skipf("Skipping %s as %s is not set", t.Name(), testhelper.BigqueryIntegrationTestCredentials)
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

	jobsDBPort := c.Port("wh-jobsDb", 5432)
	transformerPort := c.Port("wh-transformer", 9090)

	httpPort, err := kitHelper.GetFreePort()
	require.NoError(t, err)
	httpAdminPort, err := kitHelper.GetFreePort()
	require.NoError(t, err)

	schema := testhelper.RandSchema(warehouseutils.BQ)
	sourcesSchema := fmt.Sprintf("%s_%s", schema, "sources")

	bqTestCredentials, err := getBQTestCredentials()
	require.NoError(t, err)

	escapedCredentials, err := json.Marshal(bqTestCredentials.Credentials)
	require.NoError(t, err)

	escapedCredentialsTrimmedStr := strings.Trim(string(escapedCredentials), `"`)

	templateConfigurations := map[string]string{
		"workspaceId":              "BpLnfgDsc2WD8F2qNfHK5a84jjJ",
		"bigqueryWriteKey":         "J77aX7tLFJ84qYU6UrN8ctecwZt",
		"bigqueryNamespace":        schema,
		"bigqueryProjectID":        bqTestCredentials.ProjectID,
		"bigqueryLocation":         bqTestCredentials.Location,
		"bigqueryBucketName":       bqTestCredentials.BucketName,
		"bigqueryCredentials":      escapedCredentialsTrimmedStr,
		"bigquerySourcesNamespace": sourcesSchema,
		"bigquerySourcesWriteKey":  "J77aeABtLFJ84qYU6UrN8ctewZt",
	}
	workspaceConfigPath := testhelper.CreateTempFile(t, "testdata/template.json", templateConfigurations)

	t.Setenv("JOBS_DB_HOST", "localhost")
	t.Setenv("JOBS_DB_NAME", "jobsdb")
	t.Setenv("JOBS_DB_DB_NAME", "jobsdb")
	t.Setenv("JOBS_DB_USER", "rudder")
	t.Setenv("JOBS_DB_PASSWORD", "password")
	t.Setenv("JOBS_DB_SSL_MODE", "disable")
	t.Setenv("JOBS_DB_PORT", fmt.Sprint(jobsDBPort))
	t.Setenv("WAREHOUSE_JOBS_DB_HOST", "localhost")
	t.Setenv("WAREHOUSE_JOBS_DB_NAME", "jobsdb")
	t.Setenv("WAREHOUSE_JOBS_DB_DB_NAME", "jobsdb")
	t.Setenv("WAREHOUSE_JOBS_DB_USER", "rudder")
	t.Setenv("WAREHOUSE_JOBS_DB_PASSWORD", "password")
	t.Setenv("WAREHOUSE_JOBS_DB_SSL_MODE", "disable")
	t.Setenv("WAREHOUSE_JOBS_DB_PORT", fmt.Sprint(jobsDBPort))
	t.Setenv("GO_ENV", "production")
	t.Setenv("LOG_LEVEL", "INFO")
	t.Setenv("INSTANCE_ID", "1")
	t.Setenv("ALERT_PROVIDER", "pagerduty")
	t.Setenv("CONFIG_PATH", "../../../config/config.yaml")
	t.Setenv("DEST_TRANSFORM_URL", fmt.Sprintf("http://localhost:%d", transformerPort))
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

	svcDone := make(chan struct{})
	ctx, ctxCancel := context.WithCancel(context.Background())
	go func() {
		r := runner.New(runner.ReleaseInfo{EnterpriseToken: os.Getenv("ENTERPRISE_TOKEN")})
		_ = r.Run(ctx, []string{"dataLake-integration-test"})

		close(svcDone)
	}()

	serviceHealthEndpoint := fmt.Sprintf("http://localhost:%d/health", httpPort)
	health.WaitUntilReady(ctx, t, serviceHealthEndpoint, time.Minute, time.Second, "serviceHealthEndpoint")

	t.Run("Event flow", func(t *testing.T) {
		db, err := bigquery2.Connect(context.TODO(), &bigquery2.BQCredentials{
			ProjectID:   bqTestCredentials.ProjectID,
			Credentials: bqTestCredentials.Credentials,
		})
		require.NoError(t, err)

		dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
			"rudder", "rudder-password", "localhost", fmt.Sprint(jobsDBPort), "jobsdb",
		)
		jobsDB, err := sql.Open("postgres", dsn)
		require.NoError(t, err)
		require.NoError(t, jobsDB.Ping())

		provider := warehouseutils.BQ

		t.Cleanup(func() {
			for _, dataset := range []string{schema, sourcesSchema} {
				require.NoError(t, testhelper.WithConstantRetries(func() error {
					return db.Dataset(dataset).DeleteWithContents(context.TODO())
				}))
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
					HTTPPort: httpPort,
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
	})

	t.Run("Validations", func(t *testing.T) {
		destination := backendconfig.DestinationT{
			ID: "26Bgm9FrQDZjvadSwAlpd35atwn",
			Config: map[string]interface{}{
				"project":       templateConfigurations["bigqueryProjectID"],
				"location":      templateConfigurations["bigqueryLocation"],
				"bucketName":    templateConfigurations["bigqueryBucketName"],
				"credentials":   bqTestCredentials.Credentials,
				"prefix":        "",
				"namespace":     templateConfigurations["bigqueryNamespace"],
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
	})

	ctxCancel()
	<-svcDone
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
