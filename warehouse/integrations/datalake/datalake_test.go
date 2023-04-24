package datalake_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v6"
	"github.com/rudderlabs/compose-test/testcompose"
	kitHelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	"github.com/rudderlabs/rudder-server/runner"
	"github.com/rudderlabs/rudder-server/testhelper/health"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/postgres"

	"github.com/rudderlabs/rudder-server/warehouse/encoding"

	"github.com/rudderlabs/rudder-server/warehouse/integrations/testhelper"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/validations"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/stretchr/testify/require"

	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type gcsTestCredentials struct {
	BucketName  string `json:"bucketName"`
	Credentials string `json:"credentials"`
}

func getGCSTestCredentials() (*gcsTestCredentials, error) {
	cred, exists := os.LookupEnv(testhelper.BigqueryIntegrationTestCredentials)
	if !exists {
		return nil, fmt.Errorf("gcs credentials not found")
	}

	var credentials gcsTestCredentials
	err := json.Unmarshal([]byte(cred), &credentials)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal gcs credentials: %w", err)
	}

	return &credentials, nil
}

func isGCSTestCredentialsAvailable() bool {
	_, err := getGCSTestCredentials()
	return err == nil
}

func TestIntegration(t *testing.T) {
	if os.Getenv("SLOW") != "1" {
		t.Skip("Skipping tests. Add 'SLOW=1' env var to run test.")
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
	minioPort := c.Port("wh-minio", 9000)
	transformerPort := c.Port("wh-transformer", 9090)
	azurePort := c.Port("wh-azure", 10000)

	httpPort, err := kitHelper.GetFreePort()
	require.NoError(t, err)
	httpAdminPort, err := kitHelper.GetFreePort()
	require.NoError(t, err)

	templateConfigurations := map[string]string{
		"workspaceId":                "BpLnfgDsc2WD8F2qNfHK5a84jjJ",
		"azureDatalakeWriteKey":      "Hf4GTz4OiufmUqR1cq6KIeguOdC",
		"azureDatalakeContainerName": "azure-datalake-test",
		"azureDatalakeAccountName":   "MYACCESSKEY",
		"azureDatalakeAccountKey":    "TVlTRUNSRVRLRVk=",
		"azureDatalakeEndPoint":      fmt.Sprintf("localhost:%d", azurePort),
		"s3DatalakeWriteKey":         "ZapZJHfSxUN96GTIuShnz6bv0zi",
		"s3DatalakeBucketName":       "s3-datalake-test",
		"s3DatalakeRegion":           "us-east-1",
		"gcsDataLakeWriteKey":        "9zZFfcRqr2LpwerxICilhQmMybn",
		"minioBucketName":            "testbucket",
		"minioAccesskeyID":           "MYACCESSKEY",
		"minioSecretAccessKey":       "MYSECRETKEY",
		"minioEndpoint":              fmt.Sprintf("localhost:%d", minioPort),
	}
	if isGCSTestCredentialsAvailable() {
		credentials, err := getGCSTestCredentials()
		require.NoError(t, err)

		escapedCredentials, err := json.Marshal(credentials.Credentials)
		require.NoError(t, err)

		escapedCredentialsTrimmedStr := strings.Trim(string(escapedCredentials), `"`)

		templateConfigurations["gcsDataLakeBucketName"] = credentials.BucketName
		templateConfigurations["gcsDataLakeCredentials"] = escapedCredentialsTrimmedStr
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
	t.Setenv("MINIO_ACCESS_KEY_ID", "MYACCESSKEY")
	t.Setenv("MINIO_SECRET_ACCESS_KEY", "MYSECRETKEY")
	t.Setenv("MINIO_MINIO_ENDPOINT", fmt.Sprintf("localhost:%d", minioPort))
	t.Setenv("MINIO_SSL", "false")
	t.Setenv("GO_ENV", "production")
	t.Setenv("LOG_LEVEL", "INFO")
	t.Setenv("INSTANCE_ID", "1")
	t.Setenv("ALERT_PROVIDER", "pagerduty")
	t.Setenv("CONFIG_PATH", "../../../config/config.yaml")
	t.Setenv("DEST_TRANSFORM_URL", fmt.Sprintf("http://localhost:%d", transformerPort))
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
		_ = r.Run(ctx, []string{"dataLake-integration-test"})

		close(svcDone)
	}()

	serviceHealthEndpoint := fmt.Sprintf("http://localhost:%d/health", httpPort)
	health.WaitUntilReady(ctx, t, serviceHealthEndpoint, time.Minute, time.Second, "serviceHealthEndpoint")

	t.Run("Events flow", func(t *testing.T) {
		testCases := []struct {
			name          string
			writeKey      string
			sourceID      string
			destinationID string
			provider      string
			prerequisite  func(t testing.TB)
		}{
			{
				name:          "S3Datalake",
				writeKey:      "ZapZJHfSxUN96GTIuShnz6bv0zi",
				sourceID:      "279L3gEKqwruNoKGsXZtSVX7vIy",
				destinationID: "27SthahyhhqEZ7H4T4NTtNPl06V",
				provider:      warehouseutils.S3_DATALAKE,
				prerequisite: func(t testing.TB) {
					t.Helper()

					const (
						secure = false
						region = "us-east-1"
					)

					minioClient, err := minio.New(
						templateConfigurations["minioEndpoint"],
						templateConfigurations["minioAccesskeyID"],
						templateConfigurations["minioSecretAccessKey"],
						secure,
					)
					require.NoError(t, err)

					_ = minioClient.MakeBucket(templateConfigurations["s3DatalakeBucketName"], region)
				},
			},
			{
				name:          "GCSDatalake",
				writeKey:      "9zZFfcRqr2LpwerxICilhQmMybn",
				sourceID:      "279L3gEKqwruNoKGZXatSVX7vIy",
				destinationID: "27SthahyhhqEZGHaT4NTtNPl06V",
				provider:      warehouseutils.GCS_DATALAKE,
				prerequisite: func(t testing.TB) {
					t.Helper()

					if !isGCSTestCredentialsAvailable() {
						t.Skipf("Skipping %s as %s is not set", t.Name(), testhelper.BigqueryIntegrationTestCredentials)
					}
				},
			},
			{
				name:          "AzureDatalake",
				writeKey:      "Hf4GTz4OiufmUqR1cq6KIeguOdC",
				sourceID:      "279L3gEKqwruGoKGsXZtSVX7vIy",
				destinationID: "27SthahyhhqZE7H4T4NTtNPl06V",
				provider:      warehouseutils.AZURE_DATALAKE,
			},
		}

		jobsDB, err := postgres.Connect(postgres.Credentials{
			DBName:   "jobsdb",
			Password: "password",
			User:     "rudder",
			Host:     "localhost",
			SSLMode:  "disable",
			Port:     fmt.Sprint(jobsDBPort),
		})
		require.NoError(t, err)
		require.NoError(t, jobsDB.Ping())

		for _, tc := range testCases {
			tc := tc

			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()

				ts := testhelper.WareHouseTest{
					WriteKey:      tc.writeKey,
					SourceID:      tc.sourceID,
					DestinationID: tc.destinationID,
					Prerequisite:  tc.prerequisite,
					Provider:      tc.provider,
					JobsDB:        jobsDB,
					UserID:        testhelper.GetUserId(tc.provider),
					SkipWarehouse: true,
					HTTPPort:      httpPort,
				}
				ts.VerifyEvents(t)

				ts.UserID = testhelper.GetUserId(tc.provider)
				ts.VerifyModifiedEvents(t)
			})
		}
	})

	t.Run("S3 DataLake Validation", func(t *testing.T) {
		t.Parallel()

		const (
			secure = false
			region = "us-east-1"
		)

		minioClient, err := minio.New(
			templateConfigurations["minioEndpoint"],
			templateConfigurations["minioAccesskeyID"],
			templateConfigurations["minioSecretAccessKey"],
			secure,
		)
		require.NoError(t, err)

		_ = minioClient.MakeBucket(templateConfigurations["s3DatalakeBucketName"], region)

		destination := backendconfig.DestinationT{
			ID: "27SthahyhhqEZ7H4T4NTtNPl06V",
			Config: map[string]interface{}{
				"region":           templateConfigurations["s3DatalakeRegion"],
				"bucketName":       templateConfigurations["s3DatalakeBucketName"],
				"accessKeyID":      templateConfigurations["minioAccesskeyID"],
				"accessKey":        templateConfigurations["minioSecretAccessKey"],
				"endPoint":         templateConfigurations["minioEndpoint"],
				"enableSSE":        false,
				"s3ForcePathStyle": true,
				"disableSSL":       true,
				"prefix":           "some-prefix",
				"syncFrequency":    "30",
			},
			DestinationDefinition: backendconfig.DestinationDefinitionT{
				ID:          "1xAu2vuR0scUwkBivf6VhqwWgcS",
				Name:        "S3_DATALAKE",
				DisplayName: "S3 Datalake",
			},
			Name:       "s3-datalake-demo",
			Enabled:    true,
			RevisionID: "29HgOWobnr0RYZLpaSwPINb2987",
		}
		testhelper.VerifyConfigurationTest(t, destination)
	})

	t.Run("GCS DataLake Validation", func(t *testing.T) {
		t.Parallel()

		if !isGCSTestCredentialsAvailable() {
			t.Skipf("Skipping %s as %s is not set", t.Name(), testhelper.BigqueryIntegrationTestCredentials)
		}

		credentials, err := getGCSTestCredentials()
		require.NoError(t, err)

		destination := backendconfig.DestinationT{
			ID: "27SthahyhhqEZGHaT4NTtNPl06V",
			Config: map[string]interface{}{
				"bucketName":    credentials.BucketName,
				"prefix":        "",
				"credentials":   credentials.Credentials,
				"syncFrequency": "30",
			},
			DestinationDefinition: backendconfig.DestinationDefinitionT{
				ID:          "20lzWVRwzEimkq87sNQuz1or2GA",
				Name:        "GCS_DATALAKE",
				DisplayName: "Google Cloud Storage Datalake",
			},
			Name:       "gcs-datalake-demo",
			Enabled:    true,
			RevisionID: "29HgOWobnr0RYZpLASwPINb2987",
		}
		testhelper.VerifyConfigurationTest(t, destination)
	})

	t.Run("Azure DataLake Validation", func(t *testing.T) {
		t.Parallel()

		destination := backendconfig.DestinationT{
			ID: "27SthahyhhqZE7H4T4NTtNPl06V",
			Config: map[string]interface{}{
				"containerName":  templateConfigurations["azureDatalakeContainerName"],
				"prefix":         "",
				"accountName":    templateConfigurations["azureDatalakeAccountName"],
				"accountKey":     templateConfigurations["azureDatalakeAccountKey"],
				"endPoint":       templateConfigurations["azureDatalakeEndPoint"],
				"syncFrequency":  "30",
				"forcePathStyle": true,
				"disableSSL":     true,
			},
			DestinationDefinition: backendconfig.DestinationDefinitionT{
				ID:          "20lzXg0c5kCBRxGoOoKjCSyZ3AC",
				Name:        "AZURE_DATALAKE",
				DisplayName: "Azure Datalake",
			},
			Name:       "azure-datalake-demo",
			Enabled:    true,
			RevisionID: "29HgOWobnr0RYZLpaSwPIbN2987",
		}
		testhelper.VerifyConfigurationTest(t, destination)
	})

	ctxCancel()
	<-svcDone
}
