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

	"github.com/rudderlabs/rudder-server/testhelper/workspaceConfig"

	"github.com/minio/minio-go/v6"
	"github.com/rudderlabs/compose-test/testcompose"
	kitHelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	"github.com/rudderlabs/rudder-server/runner"
	"github.com/rudderlabs/rudder-server/testhelper/health"
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

const gcsTestKey = "BIGQUERY_INTEGRATION_TEST_CREDENTIALS"

func getGCSTestCredentials() (*gcsTestCredentials, error) {
	cred, exists := os.LookupEnv(gcsTestKey)
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

	jobsDBPort := c.Port("jobsDb", 5432)
	minioPort := c.Port("minio", 9000)
	transformerPort := c.Port("transformer", 9090)
	azurePort := c.Port("azure", 10000)

	httpPort, err := kitHelper.GetFreePort()
	require.NoError(t, err)
	httpAdminPort, err := kitHelper.GetFreePort()
	require.NoError(t, err)

	workspaceID := warehouseutils.RandHex()
	azWriteKey := warehouseutils.RandHex()
	azDestinationID := warehouseutils.RandHex()
	azSourceID := warehouseutils.RandHex()
	s3WriteKey := warehouseutils.RandHex()
	s3DestinationID := warehouseutils.RandHex()
	s3SourceID := warehouseutils.RandHex()
	gcsWriteKey := warehouseutils.RandHex()
	gcsDestinationID := warehouseutils.RandHex()
	gcsSourceID := warehouseutils.RandHex()

	azContainerName := "azure-datalake-test"
	s3BucketName := "s3-datalake-test"
	azAccountName := "MYACCESSKEY"
	azAccountKey := "TVlTRUNSRVRLRVk="
	azEndPoint := fmt.Sprintf("localhost:%d", azurePort)
	s3Region := "us-east-1"
	s3AccessKeyID := "MYACCESSKEY"
	s3AccessKey := "MYSECRETKEY"
	s3EndPoint := fmt.Sprintf("localhost:%d", minioPort)

	templateConfigurations := map[string]any{
		"workspaceID":      workspaceID,
		"azWriteKey":       azWriteKey,
		"azDestinationID":  azDestinationID,
		"azSourceID":       azSourceID,
		"s3WriteKey":       s3WriteKey,
		"s3DestinationID":  s3DestinationID,
		"s3SourceID":       s3SourceID,
		"gcsWriteKey":      gcsWriteKey,
		"gcsDestinationID": gcsDestinationID,
		"gcsSourceID":      gcsSourceID,
		"azContainerName":  azContainerName,
		"azAccountName":    azAccountName,
		"azAccountKey":     azAccountKey,
		"azEndpoint":       azEndPoint,
		"s3BucketName":     s3BucketName,
		"s3Region":         s3Region,
		"s3AccessKeyID":    s3AccessKeyID,
		"s3AccessKey":      s3AccessKey,
		"s3EndPoint":       s3EndPoint,
	}
	if isGCSTestCredentialsAvailable() {
		credentials, err := getGCSTestCredentials()
		require.NoError(t, err)

		escapedCredentials, err := json.Marshal(credentials.Credentials)
		require.NoError(t, err)

		escapedCredentialsTrimmedStr := strings.Trim(string(escapedCredentials), `"`)

		templateConfigurations["gcsBucketName"] = credentials.BucketName
		templateConfigurations["gcsCredentials"] = escapedCredentialsTrimmedStr
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
	if testing.Verbose() {
		t.Setenv("LOG_LEVEL", "DEBUG")
	}

	svcDone := make(chan struct{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		r := runner.New(runner.ReleaseInfo{})
		_ = r.Run(ctx, []string{"dataLake-integration-test"})

		close(svcDone)
	}()
	t.Cleanup(func() { <-svcDone })

	serviceHealthEndpoint := fmt.Sprintf("http://localhost:%d/health", httpPort)
	health.WaitUntilReady(ctx, t, serviceHealthEndpoint, time.Minute, time.Second, "serviceHealthEndpoint")

	t.Run("Events flow", func(t *testing.T) {
		testCases := []struct {
			name          string
			sourceID      string
			destinationID string
			provider      string
			prerequisite  func(t testing.TB)
		}{
			{
				name:          "S3Datalake",
				sourceID:      s3SourceID,
				destinationID: s3DestinationID,
				provider:      warehouseutils.S3_DATALAKE,
				prerequisite: func(t testing.TB) {
					t.Helper()

					const (
						secure = false
						region = "us-east-1"
					)

					minioClient, err := minio.New(s3EndPoint, s3AccessKeyID, s3AccessKey, secure)
					require.NoError(t, err)

					_ = minioClient.MakeBucket(s3BucketName, region)
				},
			},
			{
				name:          "GCSDatalake",
				sourceID:      gcsSourceID,
				destinationID: gcsDestinationID,
				provider:      warehouseutils.GCS_DATALAKE,
				prerequisite: func(t testing.TB) {
					t.Helper()

					if !isGCSTestCredentialsAvailable() {
						t.Skipf("Skipping %s as %s is not set", t.Name(), gcsTestKey)
					}
				},
			},
			{
				name:          "AzureDatalake",
				sourceID:      azSourceID,
				destinationID: azDestinationID,
				provider:      warehouseutils.AZURE_DATALAKE,
			},
		}

		jobsDB := testhelper.JobsDB(t, jobsDBPort)

		for _, tc := range testCases {
			tc := tc

			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()

				if tc.prerequisite != nil {
					tc.prerequisite(t)
				}

				ts := testhelper.TestConfig{
					SourceID:        tc.sourceID,
					DestinationID:   tc.destinationID,
					DestinationType: tc.provider,
					JobsDB:          jobsDB,
					UserID:          testhelper.GetUserId(tc.provider),
					SkipWarehouse:   true,
					HTTPPort:        httpPort,
					WorkspaceID:     workspaceID,
				}
				ts.VerifyEvents(t)

				ts.UserID = testhelper.GetUserId(tc.provider)
				ts.VerifyEvents(t)
			})
		}
	})

	t.Run("S3 DataLake Validation", func(t *testing.T) {
		t.Parallel()

		const (
			secure = false
			region = "us-east-1"
		)

		minioClient, err := minio.New(s3EndPoint, s3AccessKeyID, s3AccessKey, secure)
		require.NoError(t, err)

		_ = minioClient.MakeBucket(s3BucketName, region)

		dest := backendconfig.DestinationT{
			ID: s3DestinationID,
			Config: map[string]interface{}{
				"region":           s3Region,
				"bucketName":       s3BucketName,
				"accessKeyID":      s3AccessKeyID,
				"accessKey":        s3AccessKey,
				"endPoint":         s3EndPoint,
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
		testhelper.VerifyConfigurationTest(t, dest)
	})

	t.Run("GCS DataLake Validation", func(t *testing.T) {
		t.Parallel()

		if !isGCSTestCredentialsAvailable() {
			t.Skipf("Skipping %s as %s is not set", t.Name(), gcsTestKey)
		}

		credentials, err := getGCSTestCredentials()
		require.NoError(t, err)

		dest := backendconfig.DestinationT{
			ID: gcsDestinationID,
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
		testhelper.VerifyConfigurationTest(t, dest)
	})

	t.Run("Azure DataLake Validation", func(t *testing.T) {
		t.Parallel()

		dest := backendconfig.DestinationT{
			ID: azDestinationID,
			Config: map[string]interface{}{
				"containerName":  azContainerName,
				"prefix":         "",
				"accountName":    azAccountName,
				"accountKey":     azAccountKey,
				"endPoint":       azEndPoint,
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
		testhelper.VerifyConfigurationTest(t, dest)
	})
}
