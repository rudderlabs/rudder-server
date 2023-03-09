package datalake_test

import (
	"os"
	"testing"

	"github.com/rudderlabs/rudder-server/warehouse/encoding"

	"github.com/rudderlabs/rudder-server/warehouse/integrations/testhelper"

	"github.com/rudderlabs/rudder-server/warehouse/integrations/datalake"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/validations"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/stretchr/testify/require"

	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestIntegrationDatalake(t *testing.T) {
	if os.Getenv("SLOW") == "0" {
		t.Skip("Skipping tests. Remove 'SLOW=0' env var to run them.")
	}

	t.Parallel()

	datalake.Init()

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
				testhelper.CreateBucketForMinio(t, "s3-datalake-test")
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
				if _, exists := os.LookupEnv(testhelper.BigqueryIntegrationTestCredentials); !exists {
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

	jobsDB := testhelper.SetUpJobsDB(t)

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
			}
			ts.VerifyEvents(t)

			ts.UserID = testhelper.GetUserId(tc.provider)
			ts.VerifyModifiedEvents(t)
		})
	}
}

func TestConfigurationValidationDatalake(t *testing.T) {
	if os.Getenv("SLOW") == "0" {
		t.Skip("Skipping tests. Remove 'SLOW=0' env var to run them.")
	}

	t.Parallel()

	misc.Init()
	validations.Init()
	warehouseutils.Init()
	encoding.Init()
	datalake.Init()

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
		testhelper.VerifyConfigurationTest(t, destination)
	})
	t.Run("GCSDatalake", func(t *testing.T) {
		t.Parallel()

		if _, exists := os.LookupEnv(testhelper.BigqueryIntegrationTestCredentials); !exists {
			t.Skipf("Skipping %s as %s is not set", t.Name(), testhelper.BigqueryIntegrationTestCredentials)
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
		testhelper.VerifyConfigurationTest(t, destination)
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
		testhelper.VerifyConfigurationTest(t, destination)
	})
}
