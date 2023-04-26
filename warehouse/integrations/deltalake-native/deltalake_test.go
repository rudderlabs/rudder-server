package deltalake_native_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	dbsql "github.com/databricks/databricks-sql-go"
	"github.com/rudderlabs/compose-test/testcompose"
	kitHelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	"github.com/rudderlabs/rudder-server/runner"
	"github.com/rudderlabs/rudder-server/testhelper/health"

	"github.com/rudderlabs/rudder-server/warehouse/encoding"

	"github.com/rudderlabs/rudder-server/warehouse/validations"

	"github.com/rudderlabs/rudder-server/utils/misc"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"

	warehouseclient "github.com/rudderlabs/rudder-server/warehouse/client"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/warehouse/integrations/testhelper"
)

type deltaLakeTestCredentials struct {
	Host          string `json:"host"`
	Port          string `json:"port"`
	Path          string `json:"path"`
	Token         string `json:"token"`
	AccountName   string `json:"accountName"`
	AccountKey    string `json:"accountKey"`
	ContainerName string `json:"containerName"`
}

func getDeltaLakeTestCredentials() (*deltaLakeTestCredentials, error) {
	cred, exists := os.LookupEnv(testhelper.DeltalakeIntegrationTestCredentials)
	if !exists {
		return nil, errors.New("deltaLake test credentials not found")
	}

	var credentials deltaLakeTestCredentials
	err := json.Unmarshal([]byte(cred), &credentials)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal deltaLake test credentials: %w", err)
	}
	return &credentials, nil
}

func isDeltaLakeTestCredentialsAvailable() bool {
	_, err := getDeltaLakeTestCredentials()
	return err == nil
}

func TestIntegration(t *testing.T) {
	if os.Getenv("SLOW") != "1" {
		t.Skip("Skipping tests. Add 'SLOW=1' env var to run test.")
	}
	if !isDeltaLakeTestCredentialsAvailable() {
		t.Skipf("Skipping %s as %s is not set", t.Name(), testhelper.DeltalakeIntegrationTestCredentials)
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
	databricksConnectorPort := c.Port("wh-databricks-connector", 50051)

	httpPort, err := kitHelper.GetFreePort()
	require.NoError(t, err)
	httpAdminPort, err := kitHelper.GetFreePort()
	require.NoError(t, err)

	schema := testhelper.RandSchema(warehouseutils.DELTALAKE)
	nativeSchema := fmt.Sprintf("%s_%s", schema, "native")

	deltaLakeCredentials, err := getDeltaLakeTestCredentials()
	require.NoError(t, err)

	templateConfigurations := map[string]string{
		"workspaceId":                  "BpLnfgDsc2WD8F2qNfHK5a84jjJ",
		"deltalakeWriteKey":            "sToFgoilA0U1WxNeW1gdgUVDsEW",
		"deltalakeNativeWriteKey":      "dasFgoilA0U1WxNeW1gdgUVDfas",
		"deltalakeHost":                deltaLakeCredentials.Host,
		"deltalakePort":                deltaLakeCredentials.Port,
		"deltalakePath":                deltaLakeCredentials.Path,
		"deltalakeToken":               deltaLakeCredentials.Token,
		"deltalakeNamespace":           schema,
		"deltalakeNativeNamespace":     nativeSchema,
		"deltalakeContainerName":       deltaLakeCredentials.ContainerName,
		"deltalakeAccountName":         deltaLakeCredentials.AccountName,
		"deltalakeAccountKey":          deltaLakeCredentials.AccountKey,
		"deltalakeNativeHost":          deltaLakeCredentials.Host,
		"deltalakeNativePort":          deltaLakeCredentials.Port,
		"deltalakeNativePath":          deltaLakeCredentials.Path,
		"deltalakeNativeToken":         deltaLakeCredentials.Token,
		"deltalakeNativeContainerName": deltaLakeCredentials.ContainerName,
		"deltalakeNativeAccountName":   deltaLakeCredentials.AccountName,
		"deltalakeNativeAccountKey":    deltaLakeCredentials.AccountKey,
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
	t.Setenv("DATABRICKS_CONNECTOR_URL", fmt.Sprintf("localhost:%d", databricksConnectorPort))
	t.Setenv("DEST_TRANSFORM_URL", fmt.Sprintf("http://localhost:%d", transformerPort))
	t.Setenv("RSERVER_WAREHOUSE_DELTALAKE_MAX_PARALLEL_LOADS", "8")
	t.Setenv("RSERVER_WAREHOUSE_WAREHOUSE_SYNC_FREQ_IGNORE", "true")
	t.Setenv("RSERVER_WAREHOUSE_UPLOAD_FREQ_IN_S", "10")
	t.Setenv("RSERVER_WAREHOUSE_ENABLE_JITTER_FOR_SYNCS", "false")
	t.Setenv("RSERVER_BACKEND_CONFIG_CONFIG_FROM_FILE", "true")
	t.Setenv("RUDDER_ADMIN_PASSWORD", "password")
	t.Setenv("RUDDER_GRACEFUL_SHUTDOWN_TIMEOUT_EXIT", "false")
	t.Setenv("RSERVER_GATEWAY_WEB_PORT", strconv.Itoa(httpPort))
	t.Setenv("RSERVER_GATEWAY_ADMIN_WEB_PORT", strconv.Itoa(httpAdminPort))
	t.Setenv("RSERVER_ENABLE_STATS", "false")
	t.Setenv("RSERVER_BACKEND_CONFIG_CONFIG_JSONPATH", workspaceConfigPath)
	t.Setenv("RUDDER_TMPDIR", t.TempDir())

	svcDone := make(chan struct{})
	ctx, ctxCancel := context.WithCancel(context.Background())
	go func() {
		r := runner.New(runner.ReleaseInfo{EnterpriseToken: os.Getenv("ENTERPRISE_TOKEN")})
		_ = r.Run(ctx, []string{"deltalake-integration-test"})

		close(svcDone)
	}()

	serviceHealthEndpoint := fmt.Sprintf("http://localhost:%d/health", httpPort)
	health.WaitUntilReady(ctx, t, serviceHealthEndpoint, time.Minute, time.Second, "serviceHealthEndpoint")

	t.Run("Event flow", func(t *testing.T) {
		dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
			"rudder", "password", "localhost", fmt.Sprint(jobsDBPort), "jobsdb",
		)
		jobsDB, err := sql.Open("postgres", dsn)
		require.NoError(t, err)
		require.NoError(t, jobsDB.Ping())

		provider := warehouseutils.DELTALAKE

		port, err := strconv.Atoi(deltaLakeCredentials.Port)
		require.NoError(t, err)

		connector, err := dbsql.NewConnector(
			dbsql.WithServerHostname(deltaLakeCredentials.Host),
			dbsql.WithPort(port),
			dbsql.WithHTTPPath(deltaLakeCredentials.Path),
			dbsql.WithAccessToken(deltaLakeCredentials.Token),
			dbsql.WithSessionParams(map[string]string{
				"ansi_mode": "false",
			}),
		)
		require.NoError(t, err)

		db := sql.OpenDB(connector)
		require.NoError(t, db.Ping())

		testCases := []struct {
			name          string
			writeKey      string
			sourceID      string
			destinationID string
			schema        string
			useNative     bool
		}{
			{
				name:          "Native",
				writeKey:      "dasFgoilA0U1WxNeW1gdgUVDfas",
				sourceID:      "36H5EpYzojqQSepRSaGBrrPx3e4",
				destinationID: "36IDjdnoEus6DDNrth3SWO1FOpu",
				schema:        nativeSchema,
				useNative:     true,
			},
			{
				name:          "Legacy",
				writeKey:      "sToFgoilA0U1WxNeW1gdgUVDsEW",
				sourceID:      "25H5EpYzojqQSepRSaGBrrPx3e4",
				destinationID: "25IDjdnoEus6DDNrth3SWO1FOpu",
				schema:        schema,
				useNative:     false,
			},
		}

		for _, tc := range testCases {
			tc := tc

			t.Run(tc.name, func(t *testing.T) {
				t.Cleanup(func() {
					require.NoError(t, testhelper.WithConstantRetries(func() error {
						_, err := db.Exec(fmt.Sprintf(`DROP SCHEMA %[1]s CASCADE;`, tc.schema))
						return err
					}))
				})

				subTestCases := []struct {
					name               string
					schema             string
					writeKey           string
					sourceID           string
					destinationID      string
					messageID          string
					warehouseEventsMap testhelper.EventsCountMap
					prerequisite       func(t testing.TB)
				}{
					{
						name:               "Merge Mode",
						writeKey:           tc.writeKey,
						schema:             tc.schema,
						sourceID:           tc.sourceID,
						destinationID:      tc.destinationID,
						warehouseEventsMap: mergeEventsMap(),
						prerequisite: func(t testing.TB) {
							t.Helper()
							testhelper.SetConfig(t, []warehouseutils.KeyValue{
								{
									Key:   "Warehouse.deltalake.loadTableStrategy",
									Value: "MERGE",
								},
								{
									Key:   "Warehouse.deltalake.useParquetLoadFiles",
									Value: "false",
								},
								{
									Key:   "Warehouse.deltalake.useNative",
									Value: strconv.FormatBool(tc.useNative),
								},
							})
						},
					},
					{
						name:               "Append Mode",
						writeKey:           tc.writeKey,
						schema:             tc.schema,
						sourceID:           tc.sourceID,
						destinationID:      tc.destinationID,
						warehouseEventsMap: appendEventsMap(),
						prerequisite: func(t testing.TB) {
							t.Helper()
							testhelper.SetConfig(t, []warehouseutils.KeyValue{
								{
									Key:   "Warehouse.deltalake.loadTableStrategy",
									Value: "APPEND",
								},
								{
									Key:   "Warehouse.deltalake.useParquetLoadFiles",
									Value: "false",
								},
								{
									Key:   "Warehouse.deltalake.useNative",
									Value: strconv.FormatBool(tc.useNative),
								},
							})
						},
					},
					{
						name:               "Parquet load files",
						writeKey:           tc.writeKey,
						schema:             tc.schema,
						sourceID:           tc.sourceID,
						destinationID:      tc.destinationID,
						warehouseEventsMap: mergeEventsMap(),
						prerequisite: func(t testing.TB) {
							t.Helper()
							testhelper.SetConfig(t, []warehouseutils.KeyValue{
								{
									Key:   "Warehouse.deltalake.loadTableStrategy",
									Value: "MERGE",
								},
								{
									Key:   "Warehouse.deltalake.useParquetLoadFiles",
									Value: "true",
								},
								{
									Key:   "Warehouse.deltalake.useNative",
									Value: strconv.FormatBool(tc.useNative),
								},
							})
						},
					},
				}

				for _, stc := range subTestCases {
					stc := stc

					t.Run(tc.name+" "+stc.name, func(t *testing.T) {
						ts := testhelper.WareHouseTest{
							Schema:        stc.schema,
							WriteKey:      stc.writeKey,
							SourceID:      stc.sourceID,
							DestinationID: stc.destinationID,
							Prerequisite:  stc.prerequisite,
							JobsDB:        jobsDB,
							Provider:      provider,
							UserID:        testhelper.GetUserId(provider),
							MessageID:     misc.FastUUID().String(),
							Tables:        []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"},
							WarehouseEventsMap: testhelper.EventsCountMap{
								"identifies":    1,
								"users":         1,
								"tracks":        1,
								"product_track": 1,
								"pages":         1,
								"screens":       1,
								"aliases":       1,
								"groups":        1,
							},
							Client: &warehouseclient.Client{
								SQL:  db,
								Type: warehouseclient.SQLClient,
							},
							HTTPPort: httpPort,
						}
						ts.VerifyEvents(t)

						ts.WarehouseEventsMap = stc.warehouseEventsMap
						ts.VerifyModifiedEvents(t)
					})
				}
			})
		}
	})

	t.Run("Validation", func(t *testing.T) {
		testCases := []struct {
			name        string
			useLegacy   bool
			destination backendconfig.DestinationT
		}{
			{
				name: "Native",
				destination: backendconfig.DestinationT{
					ID: "36H5EpYzojqQSepRSaGBrrPx3e4",
					Config: map[string]interface{}{
						"host":            templateConfigurations["deltalakeNativeHost"],
						"port":            templateConfigurations["deltalakeNativePort"],
						"path":            templateConfigurations["deltalakeNativePath"],
						"token":           templateConfigurations["deltalakeNativeToken"],
						"namespace":       templateConfigurations["deltalakeNativeNamespace"],
						"bucketProvider":  "AZURE_BLOB",
						"containerName":   templateConfigurations["deltalakeNativeContainerName"],
						"prefix":          "",
						"useSTSTokens":    false,
						"enableSSE":       false,
						"accountName":     templateConfigurations["deltalakeNativeAccountName"],
						"accountKey":      templateConfigurations["deltalakeNativeAccountKey"],
						"syncFrequency":   "30",
						"eventDelivery":   false,
						"eventDeliveryTS": 1648195480174,
					},
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "23HLpnDJnIg7DsBvDWGU6DQzFEo",
						Name:        "DELTALAKE",
						DisplayName: "Databricks (Delta Lake)",
					},
					Name:       "deltalake-native-demo",
					Enabled:    true,
					RevisionID: "39eClxJQQlaWzMWyqnQctFDP5T2",
				},
				useLegacy: false,
			},
			{
				name: "Legacy",
				destination: backendconfig.DestinationT{
					ID: "25IDjdnoEus6DDNrth3SWO1FOpu",
					Config: map[string]interface{}{
						"host":            templateConfigurations["deltalakeHost"],
						"port":            templateConfigurations["deltalakePort"],
						"path":            templateConfigurations["deltalakePath"],
						"token":           templateConfigurations["deltalakeToken"],
						"namespace":       templateConfigurations["deltalakeNamespace"],
						"bucketProvider":  "AZURE_BLOB",
						"containerName":   templateConfigurations["deltalakeContainerName"],
						"prefix":          "",
						"useSTSTokens":    false,
						"enableSSE":       false,
						"accountName":     templateConfigurations["deltalakeAccountName"],
						"accountKey":      templateConfigurations["deltalakeAccountKey"],
						"syncFrequency":   "30",
						"eventDelivery":   false,
						"eventDeliveryTS": 1648195480174,
					},
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "23HLpnDJnIg7DsBvDWGU6DQzFEo",
						Name:        "DELTALAKE",
						DisplayName: "Databricks (Delta Lake)",
					},
					Name:       "deltalake-demo",
					Enabled:    true,
					RevisionID: "29eClxJQQlaWzMWyqnQctFDP5T2",
				},
				useLegacy: true,
			},
		}

		for _, tc := range testCases {
			tc := tc

			t.Run(tc.name, func(t *testing.T) {
				subTestCases := []struct {
					name                string
					useParquetLoadFiles bool
				}{
					{
						name:                "Parquet load files",
						useParquetLoadFiles: true,
					},
					{
						name:                "CSV load files",
						useParquetLoadFiles: false,
					},
				}
				for _, stc := range subTestCases {
					stc := stc

					t.Run(tc.name+" "+stc.name, func(t *testing.T) {
						testhelper.SetConfig(t, []warehouseutils.KeyValue{
							{
								Key:   "Warehouse.deltalake.useParquetLoadFiles",
								Value: strconv.FormatBool(stc.useParquetLoadFiles),
							},
							{
								Key:   "Warehouse.deltalake.useNative",
								Value: strconv.FormatBool(tc.useLegacy),
							},
						})

						testhelper.VerifyConfigurationTest(t, tc.destination)
					})
				}
			})
		}
	})

	ctxCancel()
	<-svcDone
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
		"identifies":    2,
		"users":         2,
		"tracks":        2,
		"product_track": 2,
		"pages":         2,
		"screens":       2,
		"aliases":       2,
		"groups":        2,
	}
}
