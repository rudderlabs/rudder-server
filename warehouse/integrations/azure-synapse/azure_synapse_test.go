package azuresynapse_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/rudderlabs/compose-test/compose"

	"github.com/rudderlabs/rudder-server/testhelper/workspaceConfig"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/compose-test/testcompose"
	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/runner"
	"github.com/rudderlabs/rudder-server/testhelper/health"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/testhelper"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/rudderlabs/rudder-server/warehouse/validations"
)

func TestIntegration(t *testing.T) {
	if os.Getenv("SLOW") != "1" {
		t.Skip("Skipping tests. Add 'SLOW=1' env var to run test.")
	}

	c := testcompose.New(t, compose.FilePaths([]string{"testdata/docker-compose.yml", "../testdata/docker-compose.jobsdb.yml", "../testdata/docker-compose.minio.yml"}))
	c.Start(context.Background())

	misc.Init()
	validations.Init()
	warehouseutils.Init()

	jobsDBPort := c.Port("jobsDb", 5432)
	minioPort := c.Port("minio", 9000)
	azureSynapsePort := c.Port("azure_synapse", 1433)

	httpPort, err := kithelper.GetFreePort()
	require.NoError(t, err)

	workspaceID := warehouseutils.RandHex()
	sourceID := warehouseutils.RandHex()
	destinationID := warehouseutils.RandHex()
	writeKey := warehouseutils.RandHex()

	destType := warehouseutils.AzureSynapse

	namespace := testhelper.RandSchema(destType)

	host := "localhost"
	database := "master"
	user := "SA"
	password := "reallyStrongPwd123"

	bucketName := "testbucket"
	accessKeyID := "MYACCESSKEY"
	secretAccessKey := "MYSECRETKEY"

	minioEndpoint := fmt.Sprintf("localhost:%d", minioPort)

	templateConfigurations := map[string]any{
		"workspaceID":     workspaceID,
		"sourceID":        sourceID,
		"destinationID":   destinationID,
		"writeKey":        writeKey,
		"host":            host,
		"database":        database,
		"user":            user,
		"password":        password,
		"port":            strconv.Itoa(azureSynapsePort),
		"namespace":       namespace,
		"bucketName":      bucketName,
		"accessKeyID":     accessKeyID,
		"secretAccessKey": secretAccessKey,
		"endPoint":        minioEndpoint,
	}
	workspaceConfigPath := workspaceConfig.CreateTempFile(t, "testdata/template.json", templateConfigurations)

	testhelper.EnhanceWithDefaultEnvs(t)
	t.Setenv("JOBS_DB_PORT", strconv.Itoa(jobsDBPort))
	t.Setenv("WAREHOUSE_JOBS_DB_PORT", strconv.Itoa(jobsDBPort))
	t.Setenv("MINIO_ACCESS_KEY_ID", accessKeyID)
	t.Setenv("MINIO_SECRET_ACCESS_KEY", secretAccessKey)
	t.Setenv("MINIO_MINIO_ENDPOINT", minioEndpoint)
	t.Setenv("MINIO_SSL", "false")
	t.Setenv("RSERVER_WAREHOUSE_AZURE_SYNAPSE_MAX_PARALLEL_LOADS", "8")
	t.Setenv("RSERVER_WAREHOUSE_WEB_PORT", strconv.Itoa(httpPort))
	t.Setenv("RSERVER_BACKEND_CONFIG_CONFIG_JSONPATH", workspaceConfigPath)
	t.Setenv("RSERVER_WAREHOUSE_AZURE_SYNAPSE_SLOW_QUERY_THRESHOLD", "0s")

	svcDone := make(chan struct{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		r := runner.New(runner.ReleaseInfo{})
		_ = r.Run(ctx, []string{"azure-synapse-integration-test"})

		close(svcDone)
	}()
	t.Cleanup(func() { <-svcDone })

	serviceHealthEndpoint := fmt.Sprintf("http://localhost:%d/health", httpPort)
	health.WaitUntilReady(ctx, t, serviceHealthEndpoint, time.Minute, time.Second, "serviceHealthEndpoint")

	t.Run("Events flow", func(t *testing.T) {
		jobsDB := testhelper.JobsDB(t, jobsDBPort)

		dsn := fmt.Sprintf("sqlserver://%s:%s@%s:%d?TrustServerCertificate=true&database=%s&encrypt=disable",
			user,
			password,
			host,
			azureSynapsePort,
			database,
		)
		db, err := sql.Open("sqlserver", dsn)
		require.NoError(t, err)
		require.NoError(t, db.Ping())

		testcase := []struct {
			name          string
			writeKey      string
			schema        string
			sourceID      string
			destinationID string
			tables        []string
		}{
			{
				name:          "Upload Job",
				writeKey:      writeKey,
				schema:        namespace,
				tables:        []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"},
				sourceID:      sourceID,
				destinationID: destinationID,
			},
		}

		for _, tc := range testcase {
			tc := tc

			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()

				sqlClient := &client.Client{
					SQL:  db,
					Type: client.SQLClient,
				}

				conf := map[string]interface{}{
					"bucketProvider":   "MINIO",
					"bucketName":       bucketName,
					"accessKeyID":      accessKeyID,
					"secretAccessKey":  secretAccessKey,
					"useSSL":           false,
					"endPoint":         minioEndpoint,
					"useRudderStorage": false,
				}

				t.Log("verifying test case 1")
				ts1 := testhelper.TestConfig{
					WriteKey:        tc.writeKey,
					Schema:          tc.schema,
					Tables:          tc.tables,
					SourceID:        tc.sourceID,
					DestinationID:   tc.destinationID,
					Config:          conf,
					WorkspaceID:     workspaceID,
					DestinationType: destType,
					JobsDB:          jobsDB,
					HTTPPort:        httpPort,
					Client:          sqlClient,
					JobRunID:        misc.FastUUID().String(),
					TaskRunID:       misc.FastUUID().String(),
					StagingFilePath: "testdata/upload-job.staging-1.json",
					UserID:          testhelper.GetUserId(destType),
				}
				ts1.VerifyEvents(t)

				t.Log("verifying test case 2")
				ts2 := testhelper.TestConfig{
					WriteKey:        tc.writeKey,
					Schema:          tc.schema,
					Tables:          tc.tables,
					SourceID:        tc.sourceID,
					DestinationID:   tc.destinationID,
					Config:          conf,
					WorkspaceID:     workspaceID,
					DestinationType: destType,
					JobsDB:          jobsDB,
					HTTPPort:        httpPort,
					Client:          sqlClient,
					JobRunID:        misc.FastUUID().String(),
					TaskRunID:       misc.FastUUID().String(),
					StagingFilePath: "testdata/upload-job.staging-2.json",
					UserID:          testhelper.GetUserId(destType),
				}
				ts2.VerifyEvents(t)
			})
		}
	})

	t.Run("Validations", func(t *testing.T) {
		dest := backendconfig.DestinationT{
			ID: destinationID,
			Config: map[string]interface{}{
				"host":             host,
				"database":         database,
				"user":             user,
				"password":         password,
				"port":             strconv.Itoa(azureSynapsePort),
				"sslMode":          "disable",
				"namespace":        "",
				"bucketProvider":   "MINIO",
				"bucketName":       bucketName,
				"accessKeyID":      accessKeyID,
				"secretAccessKey":  secretAccessKey,
				"useSSL":           false,
				"endPoint":         minioEndpoint,
				"syncFrequency":    "30",
				"useRudderStorage": false,
			},
			DestinationDefinition: backendconfig.DestinationDefinitionT{
				ID:          "1qvbUYC2xVQ7lvI9UUYkkM4IBt9",
				Name:        "AZURE_SYNAPSE",
				DisplayName: "Microsoft SQL Server",
			},
			Name:       "azure-synapse-demo",
			Enabled:    true,
			RevisionID: destinationID,
		}
		testhelper.VerifyConfigurationTest(t, dest)
	})
}

func TestAzureSynapse_ProcessColumnValue(t *testing.T) {
	testCases := []struct {
		name      string
		value     string
		valueType string
		expected  interface{}
		wantError error
	}{
		{
			name:      "valid integers",
			value:     "123",
			valueType: "int",
			expected:  123,
		},
		{
			name:      "invalid integers",
			value:     "123.456",
			valueType: "int",
			wantError: errors.New("invalid value for int: 123.456"),
		},
		{
			name:      "valid floats",
			value:     "123.456",
			valueType: "float",
			expected:  123.456,
		},
		{
			name:      "invalid floats",
			value:     "123",
			valueType: "float",
			wantError: errors.New("invalid value for float: 123"),
		},
		{
			name:      "valid booleans",
			value:     "true",
			valueType: "boolean",
			expected:  true,
		},
		{
			name:      "invalid booleans",
			value:     "123",
			valueType: "boolean",
			wantError: errors.New("invalid value for boolean: 123"),
		},
		{
			name:      "valid datetime",
			value:     "2021-01-01T00:00:00Z",
			valueType: "datetime",
			expected:  "2021-01-01T00:00:00Z",
		},
		{
			name:      "invalid datetime",
			value:     "123",
			valueType: "datetime",
			wantError: errors.New("invalid value for datetime: 123"),
		},
		{
			name:      "strings withing 512",
			value:     "123",
			valueType: "string",
			expected:  "123",
		},
		{
			name:      "strings greater than 512",
			value:     "12345678901234567890123456789012345678901234567890123456789012345678901234567890",
			valueType: "string",
			wantError: errors.New("invalid value for string: 12345678901234567890123456789012345678901234567890123456789012345678901234567890"),
		},
		{
			name: "invalid strings",
		},
	}
}
