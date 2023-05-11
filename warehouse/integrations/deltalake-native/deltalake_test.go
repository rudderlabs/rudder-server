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

	"github.com/rudderlabs/rudder-server/testhelper/workspaceConfig"

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

type testCredentials struct {
	Host          string `json:"host"`
	Port          string `json:"port"`
	Path          string `json:"path"`
	Token         string `json:"token"`
	AccountName   string `json:"accountName"`
	AccountKey    string `json:"accountKey"`
	ContainerName string `json:"containerName"`
}

const testKey = "DATABRICKS_INTEGRATION_TEST_CREDENTIALS"

func deltaLakeTestCredentials() (*testCredentials, error) {
	cred, exists := os.LookupEnv(testKey)
	if !exists {
		return nil, errors.New("deltaLake test credentials not found")
	}

	var credentials testCredentials
	err := json.Unmarshal([]byte(cred), &credentials)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal deltaLake test credentials: %w", err)
	}
	return &credentials, nil
}

func testCredentialsAvailable() bool {
	_, err := deltaLakeTestCredentials()
	return err == nil
}

func TestIntegration(t *testing.T) {
	if os.Getenv("SLOW") != "1" {
		t.Skip("Skipping tests. Add 'SLOW=1' env var to run test.")
	}
	if !testCredentialsAvailable() {
		t.Skipf("Skipping %s as %s is not set", t.Name(), testKey)
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
	databricksConnectorPort := c.Port("databricks-connector", 50051)

	httpPort, err := kitHelper.GetFreePort()
	require.NoError(t, err)
	httpAdminPort, err := kitHelper.GetFreePort()
	require.NoError(t, err)

	workspaceID := warehouseutils.RandHex()
	sourceID := warehouseutils.RandHex()
	destinationID := warehouseutils.RandHex()
	writeKey := warehouseutils.RandHex()

	provider := warehouseutils.DELTALAKE

	namespace := testhelper.RandSchema(provider)

	deltaLakeCredentials, err := deltaLakeTestCredentials()
	require.NoError(t, err)

	templateConfigurations := map[string]any{
		"workspaceID":   workspaceID,
		"sourceID":      sourceID,
		"destinationID": destinationID,
		"writeKey":      writeKey,
		"host":          deltaLakeCredentials.Host,
		"port":          deltaLakeCredentials.Port,
		"path":          deltaLakeCredentials.Path,
		"token":         deltaLakeCredentials.Token,
		"namespace":     namespace,
		"containerName": deltaLakeCredentials.ContainerName,
		"accountName":   deltaLakeCredentials.AccountName,
		"accountKey":    deltaLakeCredentials.AccountKey,
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
	if testing.Verbose() {
		t.Setenv("LOG_LEVEL", "DEBUG")
	}

	svcDone := make(chan struct{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		r := runner.New(runner.ReleaseInfo{})
		_ = r.Run(ctx, []string{"deltalake-integration-test"})

		close(svcDone)
	}()
	t.Cleanup(func() { <-svcDone })

	serviceHealthEndpoint := fmt.Sprintf("http://localhost:%d/health", httpPort)
	health.WaitUntilReady(ctx, t, serviceHealthEndpoint, time.Minute, time.Second, "serviceHealthEndpoint")

	t.Run("Event flow", func(t *testing.T) {
		jobsDB := testhelper.JobsDB(t, jobsDBPort)

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

		t.Cleanup(func() {
			require.NoError(t, testhelper.WithConstantRetries(func() error {
				_, err := db.Exec(fmt.Sprintf(`DROP SCHEMA %[1]s CASCADE;`, namespace))
				return err
			}))
		})

		testCases := []struct {
			name                string
			schema              string
			sourceID            string
			destinationID       string
			messageID           string
			warehouseEventsMap  testhelper.EventsCountMap
			loadTableStrategy   string
			useParquetLoadFiles bool
		}{
			{
				name:                "Merge Mode",
				schema:              namespace,
				sourceID:            sourceID,
				destinationID:       destinationID,
				warehouseEventsMap:  mergeEventsMap(),
				loadTableStrategy:   "MERGE",
				useParquetLoadFiles: false,
			},
			{
				name:                "Append Mode",
				schema:              namespace,
				sourceID:            sourceID,
				destinationID:       destinationID,
				warehouseEventsMap:  appendEventsMap(),
				loadTableStrategy:   "APPEND",
				useParquetLoadFiles: false,
			},
			{
				name:                "Parquet load files",
				schema:              namespace,
				sourceID:            sourceID,
				destinationID:       destinationID,
				warehouseEventsMap:  mergeEventsMap(),
				loadTableStrategy:   "MERGE",
				useParquetLoadFiles: true,
			},
		}

		for _, tc := range testCases {
			tc := tc

			t.Run(tc.name, func(t *testing.T) {
				t.Setenv("RSERVER_WAREHOUSE_DELTALAKE_LOAD_TABLE_STRATEGY", tc.loadTableStrategy)
				t.Setenv("RSERVER_WAREHOUSE_DELTALAKE_USE_PARQUET_LOAD_FILES", strconv.FormatBool(tc.useParquetLoadFiles))

				ts := testhelper.TestConfig{
					Schema:          tc.schema,
					SourceID:        tc.sourceID,
					DestinationID:   tc.destinationID,
					JobsDB:          jobsDB,
					DestinationType: provider,
					UserID:          testhelper.GetUserId(provider),
					MessageID:       misc.FastUUID().String(),
					Tables:          []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"},
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
					HTTPPort:    httpPort,
					WorkspaceID: workspaceID,
				}
				ts.VerifyEvents(t)

				ts.WarehouseEventsMap = tc.warehouseEventsMap
				ts.VerifyEvents(t)
			})
		}
	})

	t.Run("Validation", func(t *testing.T) {
		dest := backendconfig.DestinationT{
			ID: destinationID,
			Config: map[string]interface{}{
				"host":            deltaLakeCredentials.Host,
				"port":            deltaLakeCredentials.Port,
				"path":            deltaLakeCredentials.Path,
				"token":           deltaLakeCredentials.Token,
				"namespace":       namespace,
				"bucketProvider":  "AZURE_BLOB",
				"containerName":   deltaLakeCredentials.ContainerName,
				"prefix":          "",
				"useSTSTokens":    false,
				"enableSSE":       false,
				"accountName":     deltaLakeCredentials.AccountName,
				"accountKey":      deltaLakeCredentials.AccountKey,
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
		}

		testCases := []struct {
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

		for _, tc := range testCases {
			tc := tc

			t.Run(tc.name, func(t *testing.T) {
				t.Setenv("RSERVER_WAREHOUSE_DELTALAKE_USE_PARQUET_LOAD_FILES", strconv.FormatBool(tc.useParquetLoadFiles))
				testhelper.VerifyConfigurationTest(t, dest)
			})
		}
	})
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
