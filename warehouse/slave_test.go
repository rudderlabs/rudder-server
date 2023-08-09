package warehouse

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/services/pgnotifier"
	"github.com/rudderlabs/rudder-server/testhelper/destination"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
)

type mockSlaveNotifier struct {
	publishCh      chan *pgnotifier.ClaimResponse
	subscriberCh   chan pgnotifier.Claim
	maintenanceErr error
}

func (m *mockSlaveNotifier) Subscribe(ctx context.Context, workerId string, jobsBufferSize int) chan pgnotifier.Claim {
	return m.subscriberCh
}

func (m *mockSlaveNotifier) UpdateClaimedEvent(claim *pgnotifier.Claim, response *pgnotifier.ClaimResponse) {
	m.publishCh <- response
}

func (m *mockSlaveNotifier) RunMaintenanceWorker(ctx context.Context) error {
	return m.maintenanceErr
}

func TestSlave(t *testing.T) {
	misc.Init()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	minioResource, err := destination.SetupMINIO(pool, t)
	require.NoError(t, err)

	ctx := context.Background()

	destConf := map[string]interface{}{
		"bucketName":       minioResource.BucketName,
		"accessKeyID":      minioResource.AccessKey,
		"accessKey":        minioResource.AccessKey,
		"secretAccessKey":  minioResource.SecretKey,
		"endPoint":         minioResource.Endpoint,
		"forcePathStyle":   true,
		"s3ForcePathStyle": true,
		"disableSSL":       true,
		"region":           minioResource.SiteRegion,
		"enableSSE":        false,
		"bucketProvider":   "MINIO",
	}

	uploadFile := func(t *testing.T, destConf map[string]interface{}, filePath string) string {
		f, err := os.Open(filePath)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, f.Close()) })

		fm, err := filemanager.New(&filemanager.Settings{
			Provider: "MINIO",
			Config: misc.GetObjectStorageConfig(misc.ObjectStorageOptsT{
				Provider: "MINIO",
				Config:   destConf,
			}),
		})

		uf, err := fm.Upload(ctx, f)
		require.NoError(t, err)

		return uf.ObjectName
	}

	jobLocation := uploadFile(t, destConf, "testdata/upload-job.staging.json.gz")

	stagingFile, err := os.Open("testdata/upload-job.staging.json.gz")
	require.NoError(t, err)

	reader, err := gzip.NewReader(stagingFile)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reader.Close()) })

	scanner := bufio.NewScanner(reader)
	schemaMap := make(model.Schema)

	type event struct {
		Metadata struct {
			Table   string            `json:"table"`
			Columns map[string]string `json:"columns"`
		}
	}

	stagingEvents := make([]event, 0)

	for scanner.Scan() {
		lineBytes := scanner.Bytes()

		var stagingEvent event
		err := json.Unmarshal(lineBytes, &stagingEvent)
		require.NoError(t, err)

		stagingEvents = append(stagingEvents, stagingEvent)
	}

	for _, event := range stagingEvents {
		tableName := event.Metadata.Table

		if _, ok := schemaMap[tableName]; !ok {
			schemaMap[tableName] = make(model.TableSchema)
		}
		for columnName, columnType := range event.Metadata.Columns {
			if _, ok := schemaMap[tableName][columnName]; !ok {
				schemaMap[tableName][columnName] = columnType
			}
		}
	}

	publishCh := make(chan *pgnotifier.ClaimResponse)
	subscriberCh := make(chan pgnotifier.Claim)

	defer close(publishCh)
	defer close(subscriberCh)

	notifier := &mockSlaveNotifier{
		publishCh:    publishCh,
		subscriberCh: subscriberCh,
	}

	workers := 4
	workerJobs := 25

	slave := newSlave(
		config.Default,
		logger.NOP,
		stats.Default,
		notifier,
		newBackendConfigManager(config.Default, nil, tenantManager, logger.NOP),
		newConstraintsManager(config.Default),
	)
	slave.config.noOfSlaveWorkerRoutines = workers

	go func() {
		require.NoError(t, slave.setupSlave(ctx))
	}()

	p := payload{
		UploadID:                     1,
		StagingFileID:                1,
		StagingFileLocation:          jobLocation,
		UploadSchema:                 schemaMap,
		WorkspaceID:                  "test_workspace_id",
		SourceID:                     "test_source_id",
		SourceName:                   "test_source_name",
		DestinationID:                "test_destination_id",
		DestinationName:              "test_destination_name",
		DestinationType:              "test_destination_type",
		DestinationNamespace:         "test_destination_namespace",
		DestinationRevisionID:        uuid.New().String(),
		StagingDestinationRevisionID: uuid.New().String(),
		DestinationConfig:            destConf,
		StagingDestinationConfig:     map[string]interface{}{},
		UseRudderStorage:             false,
		StagingUseRudderStorage:      false,
		UniqueLoadGenID:              uuid.New().String(),
		RudderStoragePrefix:          misc.GetRudderObjectStoragePrefix(),
		LoadFileType:                 "csv",
	}

	payloadJson, err := json.Marshal(p)
	require.NoError(t, err)

	claim := pgnotifier.Claim{
		ID:        1,
		BatchID:   uuid.New().String(),
		Payload:   payloadJson,
		Status:    "waiting",
		Workspace: "test_workspace",
		Attempt:   0,
		JobType:   "upload",
	}

	go func() {
		for i := 0; i < workerJobs; i++ {
			subscriberCh <- claim
		}
	}()

	for i := 0; i < workerJobs; i++ {
		response := <-publishCh
		require.NoError(t, response.Err)

		var uploadPayload payload
		err = json.Unmarshal(response.Payload, &uploadPayload)
		require.NoError(t, err)
		require.Equal(t, uploadPayload.BatchID, claim.BatchID)
		require.Equal(t, uploadPayload.UploadID, p.UploadID)
		require.Equal(t, uploadPayload.StagingFileID, p.StagingFileID)
		require.Equal(t, uploadPayload.StagingFileLocation, p.StagingFileLocation)

		require.Len(t, uploadPayload.Output, 8)
		for _, output := range uploadPayload.Output {
			require.Equal(t, output.TotalRows, 4)
			require.Equal(t, output.StagingFileID, p.StagingFileID)
			require.Equal(t, output.DestinationRevisionID, p.DestinationRevisionID)
			require.Equal(t, output.UseRudderStorage, p.StagingUseRudderStorage)
		}
	}
}
