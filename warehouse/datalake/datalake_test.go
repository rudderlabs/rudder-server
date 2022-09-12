package datalake_test

import (
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

func (*TestHandle) VerifyConnection() error {
	return nil
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
		testhelper.VerifyingEventsInStagingFiles(t, warehouseTest)

		warehouseTest.EventsCountMap = testhelper.LoadFilesEventsMap()
		testhelper.VerifyingEventsInLoadFiles(t, warehouseTest)

		warehouseTest.EventsCountMap = testhelper.TableUploadsEventsMap()
		testhelper.VerifyingEventsInTableUploads(t, warehouseTest)

		// Scenario 2
		warehouseTest.TimestampBeforeSendingEvents = timeutil.Now()
		warehouseTest.UserId = testhelper.GetUserId(warehouseutils.S3_DATALAKE)

		warehouseTest.EventsCountMap = testhelper.SendEventsMap()
		testhelper.SendModifiedEvents(t, warehouseTest)
		testhelper.SendModifiedEvents(t, warehouseTest)
		testhelper.SendModifiedEvents(t, warehouseTest)
		testhelper.SendIntegratedEvents(t, warehouseTest)

		warehouseTest.EventsCountMap = testhelper.StagingFilesEventsMap()
		testhelper.VerifyingEventsInStagingFiles(t, warehouseTest)

		warehouseTest.EventsCountMap = testhelper.LoadFilesEventsMap()
		testhelper.VerifyingEventsInLoadFiles(t, warehouseTest)

		warehouseTest.EventsCountMap = testhelper.TableUploadsEventsMap()
		testhelper.VerifyingEventsInTableUploads(t, warehouseTest)
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
		testhelper.VerifyingEventsInStagingFiles(t, warehouseTest)

		warehouseTest.EventsCountMap = testhelper.LoadFilesEventsMap()
		testhelper.VerifyingEventsInLoadFiles(t, warehouseTest)

		warehouseTest.EventsCountMap = testhelper.TableUploadsEventsMap()
		testhelper.VerifyingEventsInTableUploads(t, warehouseTest)

		// Scenario 2
		warehouseTest.TimestampBeforeSendingEvents = timeutil.Now()
		warehouseTest.UserId = testhelper.GetUserId(warehouseutils.AZURE_DATALAKE)

		warehouseTest.EventsCountMap = testhelper.SendEventsMap()
		testhelper.SendModifiedEvents(t, warehouseTest)
		testhelper.SendModifiedEvents(t, warehouseTest)
		testhelper.SendModifiedEvents(t, warehouseTest)
		testhelper.SendIntegratedEvents(t, warehouseTest)

		warehouseTest.EventsCountMap = testhelper.StagingFilesEventsMap()
		testhelper.VerifyingEventsInStagingFiles(t, warehouseTest)

		warehouseTest.EventsCountMap = testhelper.LoadFilesEventsMap()
		testhelper.VerifyingEventsInLoadFiles(t, warehouseTest)

		warehouseTest.EventsCountMap = testhelper.TableUploadsEventsMap()
		testhelper.VerifyingEventsInTableUploads(t, warehouseTest)
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
		testhelper.VerifyingEventsInStagingFiles(t, warehouseTest)

		warehouseTest.EventsCountMap = testhelper.LoadFilesEventsMap()
		testhelper.VerifyingEventsInLoadFiles(t, warehouseTest)

		warehouseTest.EventsCountMap = testhelper.TableUploadsEventsMap()
		testhelper.VerifyingEventsInTableUploads(t, warehouseTest)

		// Scenario 2
		warehouseTest.TimestampBeforeSendingEvents = timeutil.Now()
		warehouseTest.UserId = testhelper.GetUserId(warehouseutils.GCS_DATALAKE)

		warehouseTest.EventsCountMap = testhelper.SendEventsMap()
		testhelper.SendModifiedEvents(t, warehouseTest)
		testhelper.SendModifiedEvents(t, warehouseTest)
		testhelper.SendModifiedEvents(t, warehouseTest)
		testhelper.SendIntegratedEvents(t, warehouseTest)

		warehouseTest.EventsCountMap = testhelper.StagingFilesEventsMap()
		testhelper.VerifyingEventsInStagingFiles(t, warehouseTest)

		warehouseTest.EventsCountMap = testhelper.LoadFilesEventsMap()
		testhelper.VerifyingEventsInLoadFiles(t, warehouseTest)

		warehouseTest.EventsCountMap = testhelper.TableUploadsEventsMap()
		testhelper.VerifyingEventsInTableUploads(t, warehouseTest)
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
