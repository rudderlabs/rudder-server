package deltalake_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	dbsql "github.com/databricks/databricks-sql-go"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/compose-test/compose"
	"github.com/rudderlabs/compose-test/testcompose"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/runner"
	"github.com/rudderlabs/rudder-server/testhelper/health"
	"github.com/rudderlabs/rudder-server/testhelper/workspaceConfig"
	"github.com/rudderlabs/rudder-server/utils/misc"
	warehouseclient "github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/deltalake"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/testhelper"
	mockuploader "github.com/rudderlabs/rudder-server/warehouse/internal/mocks/utils"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/rudderlabs/rudder-server/warehouse/validations"
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

	c := testcompose.New(t, compose.FilePaths([]string{"../testdata/docker-compose.jobsdb.yml"}))
	c.Start(context.Background())

	misc.Init()
	validations.Init()
	warehouseutils.Init()

	jobsDBPort := c.Port("jobsDb", 5432)

	httpPort, err := kithelper.GetFreePort()
	require.NoError(t, err)

	workspaceID := warehouseutils.RandHex()
	sourceID := warehouseutils.RandHex()
	destinationID := warehouseutils.RandHex()
	writeKey := warehouseutils.RandHex()

	destType := warehouseutils.DELTALAKE

	namespace := testhelper.RandSchema(destType)

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

	testhelper.EnhanceWithDefaultEnvs(t)
	t.Setenv("JOBS_DB_PORT", strconv.Itoa(jobsDBPort))
	t.Setenv("WAREHOUSE_JOBS_DB_PORT", strconv.Itoa(jobsDBPort))
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

	t.Run("Event flow", func(t *testing.T) {
		jobsDB := testhelper.JobsDB(t, jobsDBPort)

		t.Cleanup(func() {
			require.Eventually(t, func() bool {
				if _, err := db.Exec(fmt.Sprintf(`DROP SCHEMA %[1]s CASCADE;`, namespace)); err != nil {
					t.Logf("error deleting schema: %v", err)
					return false
				}
				return true
			},
				time.Minute,
				time.Second,
			)
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
			jobRunID            string
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
				jobRunID:            misc.FastUUID().String(),
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
				// an empty jobRunID means that the source is not an ETL one
				// see Uploader.CanAppend()
				jobRunID: "",
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
				jobRunID:            misc.FastUUID().String(),
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
					JobRunID:      tc.jobRunID,
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
					JobRunID:           tc.jobRunID,
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
		t.Cleanup(func() {
			require.Eventually(t, func() bool {
				if _, err := db.Exec(fmt.Sprintf(`DROP SCHEMA %[1]s CASCADE;`, namespace)); err != nil {
					t.Logf("error deleting schema: %v", err)
					return false
				}
				return true
			},
				time.Minute,
				time.Second,
			)
		})

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
			Name:       "deltalake-demo",
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

func TestDeltalake_TrimErrorMessage(t *testing.T) {
	tempError := errors.New("temp error")

	testCases := []struct {
		name          string
		inputError    error
		expectedError error
	}{
		{
			name:          "error message is above max length",
			inputError:    errors.New(strings.Repeat(tempError.Error(), 100)),
			expectedError: errors.New(strings.Repeat(tempError.Error(), 25)),
		},
		{
			name:          "error message is below max length",
			inputError:    errors.New(strings.Repeat(tempError.Error(), 25)),
			expectedError: errors.New(strings.Repeat(tempError.Error(), 25)),
		},
		{
			name:          "error message is equal to max length",
			inputError:    errors.New(strings.Repeat(tempError.Error(), 10)),
			expectedError: errors.New(strings.Repeat(tempError.Error(), 10)),
		},
	}

	for _, tc := range testCases {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			c := config.New()
			c.Set("Warehouse.deltalake.maxErrorLength", len(tempError.Error())*25)

			d := deltalake.New(c, logger.NOP, stats.Default)
			require.Equal(t, d.TrimErrorMessage(tc.inputError), tc.expectedError)
		})
	}
}

func TestDeltalake_ShouldAppend(t *testing.T) {
	testCases := []struct {
		name                  string
		loadTableStrategy     string
		uploaderCanAppend     bool
		uploaderExpectedCalls int
		expected              bool
	}{
		{
			name:                  "uploader says we can append and we are in append mode",
			loadTableStrategy:     "APPEND",
			uploaderCanAppend:     true,
			uploaderExpectedCalls: 1,
			expected:              true,
		},
		{
			name:                  "uploader says we cannot append and we are in append mode",
			loadTableStrategy:     "APPEND",
			uploaderCanAppend:     false,
			uploaderExpectedCalls: 1,
			expected:              false,
		},
		{
			name:                  "uploader says we can append and we are in merge mode",
			loadTableStrategy:     "MERGE",
			uploaderCanAppend:     true,
			uploaderExpectedCalls: 0,
			expected:              false,
		},
		{
			name:                  "uploader says we cannot append and we are in merge mode",
			loadTableStrategy:     "MERGE",
			uploaderCanAppend:     false,
			uploaderExpectedCalls: 0,
			expected:              false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			c := config.New()
			c.Set("Warehouse.deltalake.loadTableStrategy", tc.loadTableStrategy)

			d := deltalake.New(c, logger.NOP, stats.Default)

			mockCtrl := gomock.NewController(t)
			uploader := mockuploader.NewMockUploader(mockCtrl)
			uploader.EXPECT().CanAppend().Times(tc.uploaderExpectedCalls).Return(tc.uploaderCanAppend)

			d.Uploader = uploader
			require.Equal(t, d.ShouldAppend(), tc.expected)
		})
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
