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

	"github.com/rudderlabs/compose-test/compose"

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

	c := testcompose.New(t, compose.FilePaths([]string{"testdata/docker-compose.yml", "../testdata/docker-compose.jobsdb.yml"}))

	t.Cleanup(func() {
		c.Stop(context.Background())
	})
	c.Start(context.Background())

	misc.Init()
	validations.Init()
	warehouseutils.Init()
	encoding.Init()

	jobsDBPort := c.Port("jobsDb", 5432)
	databricksConnectorPort := c.Port("databricks-connector", 50051)

	httpPort, err := kitHelper.GetFreePort()
	require.NoError(t, err)

	workspaceID := warehouseutils.RandHex()
	sourceID := warehouseutils.RandHex()
	destinationID := warehouseutils.RandHex()
	writeKey := warehouseutils.RandHex()

	destType := warehouseutils.DELTALAKE

	namespace := testhelper.RandSchema(destType)

	deltaLakeCredentials, err := deltaLakeTestCredentials()
	require.NoError(t, err)

	databricksEndpoint := fmt.Sprintf("localhost:%d", databricksConnectorPort)

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

	testhelper.EnhanceWithDefaultEnvs(t)
	t.Setenv("JOBS_DB_PORT", strconv.Itoa(jobsDBPort))
	t.Setenv("WAREHOUSE_JOBS_DB_PORT", strconv.Itoa(jobsDBPort))
	t.Setenv("DATABRICKS_CONNECTOR_URL", databricksEndpoint)
	t.Setenv("RSERVER_WAREHOUSE_DELTALAKE_MAX_PARALLEL_LOADS", "8")
	t.Setenv("RSERVER_WAREHOUSE_WEB_PORT", strconv.Itoa(httpPort))
	t.Setenv("RSERVER_BACKEND_CONFIG_CONFIG_JSONPATH", workspaceConfigPath)
	t.Setenv("RSERVER_WAREHOUSE_DELTALAKE_SLOW_QUERY_THRESHOLD", "0s")

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
			writeKey            string
			schema              string
			sourceID            string
			destinationID       string
			messageID           string
			warehouseEventsMap  testhelper.EventsCountMap
			loadTableStrategy   string
			useParquetLoadFiles bool
			stagingFilePrefix   string
		}{
			{
				name:                "Merge Mode",
				writeKey:            writeKey,
				schema:              namespace,
				sourceID:            sourceID,
				destinationID:       destinationID,
				warehouseEventsMap:  mergeEventsMap(),
				loadTableStrategy:   "MERGE",
				useParquetLoadFiles: false,
				stagingFilePrefix:   "testdata/upload-job-merge-mode",
			},
			{
				name:                "Append Mode",
				writeKey:            writeKey,
				schema:              namespace,
				sourceID:            sourceID,
				destinationID:       destinationID,
				warehouseEventsMap:  appendEventsMap(),
				loadTableStrategy:   "APPEND",
				useParquetLoadFiles: false,
				stagingFilePrefix:   "testdata/upload-job-append-mode",
			},
			{
				name:                "Parquet load files",
				writeKey:            writeKey,
				schema:              namespace,
				sourceID:            sourceID,
				destinationID:       destinationID,
				warehouseEventsMap:  mergeEventsMap(),
				loadTableStrategy:   "MERGE",
				useParquetLoadFiles: true,
				stagingFilePrefix:   "testdata/upload-job-parquet",
			},
		}

		for _, tc := range testCases {
			tc := tc

			t.Run(tc.name, func(t *testing.T) {
				t.Setenv("RSERVER_WAREHOUSE_DELTALAKE_LOAD_TABLE_STRATEGY", tc.loadTableStrategy)
				t.Setenv("RSERVER_WAREHOUSE_DELTALAKE_USE_PARQUET_LOAD_FILES", strconv.FormatBool(tc.useParquetLoadFiles))

				sqlClient := &warehouseclient.Client{
					SQL:  db,
					Type: warehouseclient.SQLClient,
				}

				conf := map[string]interface{}{
					"bucketProvider": "AZURE_BLOB",
					"containerName":  deltaLakeCredentials.ContainerName,
					"prefix":         "",
					"useSTSTokens":   false,
					"enableSSE":      false,
					"accountName":    deltaLakeCredentials.AccountName,
					"accountKey":     deltaLakeCredentials.AccountKey,
				}
				tables := []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"}

				t.Log("verifying test case 1")
				ts1 := testhelper.TestConfig{
					WriteKey:      writeKey,
					Schema:        tc.schema,
					Tables:        tables,
					SourceID:      tc.sourceID,
					DestinationID: tc.destinationID,
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
					Config:          conf,
					WorkspaceID:     workspaceID,
					DestinationType: destType,
					JobsDB:          jobsDB,
					HTTPPort:        httpPort,
					Client:          sqlClient,
					StagingFilePath: tc.stagingFilePrefix + ".staging-1.json",
					UserID:          testhelper.GetUserId(destType),
				}
				ts1.VerifyEvents(t)

				t.Log("verifying test case 2")
				ts2 := testhelper.TestConfig{
					WriteKey:           writeKey,
					Schema:             tc.schema,
					Tables:             tables,
					SourceID:           tc.sourceID,
					DestinationID:      tc.destinationID,
					WarehouseEventsMap: tc.warehouseEventsMap,
					Config:             conf,
					WorkspaceID:        workspaceID,
					DestinationType:    destType,
					JobsDB:             jobsDB,
					HTTPPort:           httpPort,
					Client:             sqlClient,
					StagingFilePath:    tc.stagingFilePrefix + ".staging-2.json",
					UserID:             ts1.UserID,
				}
				ts2.VerifyEvents(t)
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
			conf                map[string]interface{}
		}{
			{
				name:                "Parquet load files",
				useParquetLoadFiles: true,
			},
			{
				name:                "CSV load files",
				useParquetLoadFiles: false,
			},
			{
				name:                "External location",
				useParquetLoadFiles: true,
				conf: map[string]interface{}{
					"enableExternalLocation": true,
					"externalLocation":       "/path/to/delta/table",
				},
			},
		}

		for _, tc := range testCases {
			tc := tc

			t.Run(tc.name, func(t *testing.T) {
				t.Setenv("RSERVER_WAREHOUSE_DELTALAKE_USE_PARQUET_LOAD_FILES", strconv.FormatBool(tc.useParquetLoadFiles))

				for k, v := range tc.conf {
					dest.Config[k] = v
				}

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
