//go:build warehouse_integration

package datalake_test

import (
	"os"
	"testing"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
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

func (*TestHandle) VerifyConnection() error {
	return nil
}

func TestDatalakeIntegration(t *testing.T) {
	t.Run("S3Datalake", func(t *testing.T) {
		t.Parallel()

		warehouseTest := &testhelper.WareHouseTest{
			WriteKey:      handle.S3DatalakeWriteKey,
			Provider:      warehouseutils.S3_DATALAKE,
			SourceID:      "279L3gEKqwruNoKGsXZtSVX7vIy",
			DestinationID: "27SthahyhhqEZ7H4T4NTtNPl06V",
		}

		testhelper.CreateBucketForMinio(t, "s3-datalake-test")

		// Scenario 1
		warehouseTest.TimestampBeforeSendingEvents = timeutil.Now()
		warehouseTest.UserId = testhelper.GetUserId(warehouseutils.S3_DATALAKE)

		sendEventsMap := testhelper.SendEventsMap()
		testhelper.SendEvents(t, warehouseTest, sendEventsMap)
		testhelper.SendEvents(t, warehouseTest, sendEventsMap)
		testhelper.SendEvents(t, warehouseTest, sendEventsMap)
		testhelper.SendIntegratedEvents(t, warehouseTest, sendEventsMap)

		testhelper.VerifyEventsInStagingFiles(t, warehouseTest, testhelper.StagingFilesEventsMap())
		testhelper.VerifyEventsInLoadFiles(t, warehouseTest, testhelper.LoadFilesEventsMap())
		testhelper.VerifyEventsInTableUploads(t, warehouseTest, testhelper.TableUploadsEventsMap())

		// Scenario 2
		warehouseTest.TimestampBeforeSendingEvents = timeutil.Now()
		warehouseTest.UserId = testhelper.GetUserId(warehouseutils.S3_DATALAKE)

		sendEventsMap = testhelper.SendEventsMap()
		testhelper.SendModifiedEvents(t, warehouseTest, sendEventsMap)
		testhelper.SendModifiedEvents(t, warehouseTest, sendEventsMap)
		testhelper.SendModifiedEvents(t, warehouseTest, sendEventsMap)
		testhelper.SendIntegratedEvents(t, warehouseTest, sendEventsMap)

		testhelper.VerifyEventsInStagingFiles(t, warehouseTest, testhelper.StagingFilesEventsMap())
		testhelper.VerifyEventsInLoadFiles(t, warehouseTest, testhelper.LoadFilesEventsMap())
		testhelper.VerifyEventsInTableUploads(t, warehouseTest, testhelper.TableUploadsEventsMap())

		testhelper.VerifyWorkspaceIDInStats(t)
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

		sendEventsMap := testhelper.SendEventsMap()
		testhelper.SendEvents(t, warehouseTest, sendEventsMap)
		testhelper.SendEvents(t, warehouseTest, sendEventsMap)
		testhelper.SendEvents(t, warehouseTest, sendEventsMap)
		testhelper.SendIntegratedEvents(t, warehouseTest, sendEventsMap)

		testhelper.VerifyEventsInStagingFiles(t, warehouseTest, testhelper.StagingFilesEventsMap())
		testhelper.VerifyEventsInLoadFiles(t, warehouseTest, testhelper.LoadFilesEventsMap())
		testhelper.VerifyEventsInTableUploads(t, warehouseTest, testhelper.TableUploadsEventsMap())

		// Scenario 2
		warehouseTest.TimestampBeforeSendingEvents = timeutil.Now()
		warehouseTest.UserId = testhelper.GetUserId(warehouseutils.AZURE_DATALAKE)

		sendEventsMap = testhelper.SendEventsMap()
		testhelper.SendModifiedEvents(t, warehouseTest, sendEventsMap)
		testhelper.SendModifiedEvents(t, warehouseTest, sendEventsMap)
		testhelper.SendModifiedEvents(t, warehouseTest, sendEventsMap)
		testhelper.SendIntegratedEvents(t, warehouseTest, sendEventsMap)

		testhelper.VerifyEventsInStagingFiles(t, warehouseTest, testhelper.StagingFilesEventsMap())
		testhelper.VerifyEventsInLoadFiles(t, warehouseTest, testhelper.LoadFilesEventsMap())
		testhelper.VerifyEventsInTableUploads(t, warehouseTest, testhelper.TableUploadsEventsMap())

		testhelper.VerifyWorkspaceIDInStats(t)
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

		sendEventsMap := testhelper.SendEventsMap()
		testhelper.SendEvents(t, warehouseTest, sendEventsMap)
		testhelper.SendEvents(t, warehouseTest, sendEventsMap)
		testhelper.SendEvents(t, warehouseTest, sendEventsMap)
		testhelper.SendIntegratedEvents(t, warehouseTest, sendEventsMap)

		testhelper.VerifyEventsInStagingFiles(t, warehouseTest, testhelper.StagingFilesEventsMap())
		testhelper.VerifyEventsInLoadFiles(t, warehouseTest, testhelper.LoadFilesEventsMap())
		testhelper.VerifyEventsInTableUploads(t, warehouseTest, testhelper.TableUploadsEventsMap())

		// Scenario 2
		warehouseTest.TimestampBeforeSendingEvents = timeutil.Now()
		warehouseTest.UserId = testhelper.GetUserId(warehouseutils.GCS_DATALAKE)

		sendEventsMap = testhelper.SendEventsMap()
		testhelper.SendModifiedEvents(t, warehouseTest, sendEventsMap)
		testhelper.SendModifiedEvents(t, warehouseTest, sendEventsMap)
		testhelper.SendModifiedEvents(t, warehouseTest, sendEventsMap)
		testhelper.SendIntegratedEvents(t, warehouseTest, sendEventsMap)

		testhelper.VerifyEventsInStagingFiles(t, warehouseTest, testhelper.StagingFilesEventsMap())
		testhelper.VerifyEventsInLoadFiles(t, warehouseTest, testhelper.LoadFilesEventsMap())
		testhelper.VerifyEventsInTableUploads(t, warehouseTest, testhelper.TableUploadsEventsMap())

		testhelper.VerifyWorkspaceIDInStats(t)
	})
}

func TestDatalakeConfigurationValidation(t *testing.T) {
	configurations := testhelper.PopulateTemplateConfigurations()

	t.Run("S3Datalake", func(t *testing.T) {
		t.Parallel()

		testhelper.CreateBucketForMinio(t, "s3-datalake-test")

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

		if _, exists := os.LookupEnv(testhelper.BigqueryIntegrationTestCredentials); !exists {
			t.Skip("Skipping GCS Datalake Test as the Test credentials does not exists.")
		}

		bqCredentials, err := testhelper.BigqueryCredentials()
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
