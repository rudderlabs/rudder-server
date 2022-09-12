//go:build warehouse_integration

package datalake_test

import (
	"encoding/json"
	"fmt"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	bigquery2 "github.com/rudderlabs/rudder-server/warehouse/bigquery"
	"os"
	"testing"

	"github.com/minio/minio-go/v6"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/warehouse/testhelper"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type TestHandle struct {
	S3DatalakeWriteKey    string
	GCSDatalakeWriteKey   string
	AZUREDatalakeWriteKey string
}

var handle *TestHandle

const (
	TestCredentialsKey = testhelper.BigqueryIntegrationTestCredentials
)

func (*TestHandle) VerifyConnection() error {
	return nil
}

// bigqueryCredentials extracting big query credentials
func bigqueryCredentials() (bqCredentials bigquery2.BQCredentialsT, err error) {
	cred, exists := os.LookupEnv(TestCredentialsKey)
	if !exists {
		err = fmt.Errorf("following %s does not exists while running the Bigquery test", TestCredentialsKey)
		return
	}

	err = json.Unmarshal([]byte(cred), &bqCredentials)
	if err != nil {
		err = fmt.Errorf("error occurred while unmarshalling bigquery test credentials with err: %s", err.Error())
		return
	}
	return
}

func TestDatalakeIntegration(t *testing.T) {
	t.Run("S3Datalake", func(t *testing.T) {
		t.Parallel()

		minioClient, err := minio.New("wh-minio:9000", "MYACCESSKEY", "MYSECRETKEY", false)
		require.NoError(t, err)

		if err = minioClient.MakeBucket("s3-datalake-test", "us-east-1"); err != nil {
			exists, errBucketExists := minioClient.BucketExists("devintegrationtest")
			if !(errBucketExists == nil && exists) {
				t.Errorf("Failed to create bucket for s3-datalake integration test: %v", err)
			}
		}

		warehouseTest := &testhelper.WareHouseTest{
			WriteKey:      handle.S3DatalakeWriteKey,
			Provider:      warehouseutils.S3_DATALAKE,
			SourceID:      "279L3gEKqwruNoKGsXZtSVX7vIy",
			DestinationID: "27SthahyhhqEZ7H4T4NTtNPl06V",
		}

		// Scenario 1
		warehouseTest.TimestampBeforeSendingEvents = timeutil.Now()
		warehouseTest.UserId = testhelper.GetUserId(warehouseutils.S3_DATALAKE)

		warehouseTest.EventsCountMap = testhelper.SendEventsMap()
		testhelper.SendEvents(t, warehouseTest)
		testhelper.SendEvents(t, warehouseTest)
		testhelper.SendEvents(t, warehouseTest)
		testhelper.SendIntegratedEvents(t, warehouseTest)

		warehouseTest.EventsCountMap = testhelper.StagingFilesEventsMap()
		testhelper.VerifyEventsInStagingFiles(t, warehouseTest)

		warehouseTest.EventsCountMap = testhelper.LoadFilesEventsMap()
		testhelper.VerifyEventsInLoadFiles(t, warehouseTest)

		warehouseTest.EventsCountMap = testhelper.TableUploadsEventsMap()
		testhelper.VerifyEventsInTableUploads(t, warehouseTest)

		// Scenario 2
		warehouseTest.TimestampBeforeSendingEvents = timeutil.Now()
		warehouseTest.UserId = testhelper.GetUserId(warehouseutils.S3_DATALAKE)

		warehouseTest.EventsCountMap = testhelper.SendEventsMap()
		testhelper.SendModifiedEvents(t, warehouseTest)
		testhelper.SendModifiedEvents(t, warehouseTest)
		testhelper.SendModifiedEvents(t, warehouseTest)
		testhelper.SendIntegratedEvents(t, warehouseTest)

		warehouseTest.EventsCountMap = testhelper.StagingFilesEventsMap()
		testhelper.VerifyEventsInStagingFiles(t, warehouseTest)

		warehouseTest.EventsCountMap = testhelper.LoadFilesEventsMap()
		testhelper.VerifyEventsInLoadFiles(t, warehouseTest)

		warehouseTest.EventsCountMap = testhelper.TableUploadsEventsMap()
		testhelper.VerifyEventsInTableUploads(t, warehouseTest)
	})
	t.Run("AzureDatalake", func(t *testing.T) {
		t.Parallel()

		warehouseTest := &testhelper.WareHouseTest{
			WriteKey:      handle.AZUREDatalakeWriteKey,
			Provider:      warehouseutils.AZURE_DATALAKE,
			SourceID:      "279L3gEKqwruGoKGsXZtSVX7vIy",
			DestinationID: "27SthahyhhqZE7H4T4NTtNPl06V",
		}

		// Scenario 1
		warehouseTest.TimestampBeforeSendingEvents = timeutil.Now()
		warehouseTest.UserId = testhelper.GetUserId(warehouseutils.AZURE_DATALAKE)

		warehouseTest.EventsCountMap = testhelper.SendEventsMap()
		testhelper.SendEvents(t, warehouseTest)
		testhelper.SendEvents(t, warehouseTest)
		testhelper.SendEvents(t, warehouseTest)
		testhelper.SendIntegratedEvents(t, warehouseTest)

		warehouseTest.EventsCountMap = testhelper.StagingFilesEventsMap()
		testhelper.VerifyEventsInStagingFiles(t, warehouseTest)

		warehouseTest.EventsCountMap = testhelper.LoadFilesEventsMap()
		testhelper.VerifyEventsInLoadFiles(t, warehouseTest)

		warehouseTest.EventsCountMap = testhelper.TableUploadsEventsMap()
		testhelper.VerifyEventsInTableUploads(t, warehouseTest)

		// Scenario 2
		warehouseTest.TimestampBeforeSendingEvents = timeutil.Now()
		warehouseTest.UserId = testhelper.GetUserId(warehouseutils.AZURE_DATALAKE)

		warehouseTest.EventsCountMap = testhelper.SendEventsMap()
		testhelper.SendModifiedEvents(t, warehouseTest)
		testhelper.SendModifiedEvents(t, warehouseTest)
		testhelper.SendModifiedEvents(t, warehouseTest)
		testhelper.SendIntegratedEvents(t, warehouseTest)

		warehouseTest.EventsCountMap = testhelper.StagingFilesEventsMap()
		testhelper.VerifyEventsInStagingFiles(t, warehouseTest)

		warehouseTest.EventsCountMap = testhelper.LoadFilesEventsMap()
		testhelper.VerifyEventsInLoadFiles(t, warehouseTest)

		warehouseTest.EventsCountMap = testhelper.TableUploadsEventsMap()
		testhelper.VerifyEventsInTableUploads(t, warehouseTest)
	})
	t.Run("GCSDatalake", func(t *testing.T) {
		t.Parallel()

		if _, exists := os.LookupEnv(testhelper.BigqueryIntegrationTestCredentials); !exists {
			t.Skip("Skipping GCS Datalake Test as the Test credentials does not exists.")
		}

		warehouseTest := &testhelper.WareHouseTest{
			WriteKey:      handle.GCSDatalakeWriteKey,
			Provider:      warehouseutils.GCS_DATALAKE,
			SourceID:      "279L3gEKqwruNoKGZXatSVX7vIy",
			DestinationID: "27SthahyhhqEZGHaT4NTtNPl06V",
		}

		// Scenario 1
		warehouseTest.TimestampBeforeSendingEvents = timeutil.Now()
		warehouseTest.UserId = testhelper.GetUserId(warehouseutils.GCS_DATALAKE)

		warehouseTest.EventsCountMap = testhelper.SendEventsMap()
		testhelper.SendEvents(t, warehouseTest)
		testhelper.SendEvents(t, warehouseTest)
		testhelper.SendEvents(t, warehouseTest)
		testhelper.SendIntegratedEvents(t, warehouseTest)

		warehouseTest.EventsCountMap = testhelper.StagingFilesEventsMap()
		testhelper.VerifyEventsInStagingFiles(t, warehouseTest)

		warehouseTest.EventsCountMap = testhelper.LoadFilesEventsMap()
		testhelper.VerifyEventsInLoadFiles(t, warehouseTest)

		warehouseTest.EventsCountMap = testhelper.TableUploadsEventsMap()
		testhelper.VerifyEventsInTableUploads(t, warehouseTest)

		// Scenario 2
		warehouseTest.TimestampBeforeSendingEvents = timeutil.Now()
		warehouseTest.UserId = testhelper.GetUserId(warehouseutils.GCS_DATALAKE)

		warehouseTest.EventsCountMap = testhelper.SendEventsMap()
		testhelper.SendModifiedEvents(t, warehouseTest)
		testhelper.SendModifiedEvents(t, warehouseTest)
		testhelper.SendModifiedEvents(t, warehouseTest)
		testhelper.SendIntegratedEvents(t, warehouseTest)

		warehouseTest.EventsCountMap = testhelper.StagingFilesEventsMap()
		testhelper.VerifyEventsInStagingFiles(t, warehouseTest)

		warehouseTest.EventsCountMap = testhelper.LoadFilesEventsMap()
		testhelper.VerifyEventsInLoadFiles(t, warehouseTest)

		warehouseTest.EventsCountMap = testhelper.TableUploadsEventsMap()
		testhelper.VerifyEventsInTableUploads(t, warehouseTest)
	})
}

func TestDatalakeConfigurationValidation(t *testing.T) {
	t.Run("S3Datalake", func(t *testing.T) {
		t.Parallel()

		minioClient, err := minio.New("wh-minio:9000", "MYACCESSKEY", "MYSECRETKEY", false)
		require.NoError(t, err)

		if err = minioClient.MakeBucket("s3-datalake-test", "us-east-1"); err != nil {
			exists, errBucketExists := minioClient.BucketExists("devintegrationtest")
			if !(errBucketExists == nil && exists) {
				t.Logf("Failed to create bucket for s3-datalake integration test: %v", err)
			}
		}

		configurations := testhelper.PopulateTemplateConfigurations()

		destination := backendconfig.DestinationT{
			ID: "27SthahyhhqEZ7H4T4NTtNPl06V",
			Config: map[string]interface{}{
				"region":           configurations["s3DatalakeRegion"],
				"bucketName":       configurations["s3DatalakeBucketName"],
				"accessKeyID":      configurations["minioAccesskeyID"],
				"accessKey":        configurations["minioSecretAccessKey"],
				"endPoint":         configurations["minioEndpoint"],
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
		testhelper.VerifyingConfigurationTest(t, destination)
	})
	t.Run("GCSDatalake", func(t *testing.T) {
		t.Parallel()

		configurations := testhelper.PopulateTemplateConfigurations()
		bqCredentials, err := bigqueryCredentials()
		require.NoError(t, err)

		destination := backendconfig.DestinationT{
			ID: "27SthahyhhqEZGHaT4NTtNPl06V",
			Config: map[string]interface{}{
				"bucketName":    configurations["bigqueryBucketName"],
				"prefix":        "",
				"credentials":   bqCredentials.Credentials,
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
		testhelper.VerifyingConfigurationTest(t, destination)
	})
	t.Run("AzureDatalake", func(t *testing.T) {
		t.Parallel()

		configurations := testhelper.PopulateTemplateConfigurations()

		destination := backendconfig.DestinationT{
			ID: "27SthahyhhqZE7H4T4NTtNPl06V",
			Config: map[string]interface{}{
				"containerName":  configurations["azureDatalakeContainerName"],
				"prefix":         "",
				"accountName":    configurations["azureDatalakeAccountName"],
				"accountKey":     configurations["azureDatalakeAccountKey"],
				"endPoint":       configurations["azureDatalakeEndPoint"],
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
		testhelper.VerifyingConfigurationTest(t, destination)
	})
}

func TestMain(m *testing.M) {
	handle = &TestHandle{
		S3DatalakeWriteKey:    "ZapZJHfSxUN96GTIuShnz6bv0zi",
		GCSDatalakeWriteKey:   "9zZFfcRqr2LpwerxICilhQmMybn",
		AZUREDatalakeWriteKey: "Hf4GTz4OiufmUqR1cq6KIeguOdC",
	}
	os.Exit(testhelper.Run(m, handle))
}
