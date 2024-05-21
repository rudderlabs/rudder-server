package slave

import (
	"bufio"
	"compress/gzip"
	"context"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/minio"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/services/notifier"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/bcm"
	"github.com/rudderlabs/rudder-server/warehouse/constraints"
	"github.com/rudderlabs/rudder-server/warehouse/encoding"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/multitenant"
)

type mockSlaveNotifier struct {
	subscribeCh    chan *notifier.ClaimJobResponse
	publishCh      chan *notifier.ClaimJob
	maintenanceErr error
}

func (m *mockSlaveNotifier) Subscribe(context.Context, string, int) <-chan *notifier.ClaimJob {
	return m.publishCh
}

func (m *mockSlaveNotifier) UpdateClaim(_ context.Context, _ *notifier.ClaimJob, response *notifier.ClaimJobResponse) {
	m.subscribeCh <- response
}

func (m *mockSlaveNotifier) RunMaintenance(context.Context) error {
	return m.maintenanceErr
}

func TestSlave(t *testing.T) {
	misc.Init()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	minioResource, err := minio.Setup(pool, t)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())

	destConf := map[string]interface{}{
		"bucketName":       minioResource.BucketName,
		"accessKeyID":      minioResource.AccessKeyID,
		"accessKey":        minioResource.AccessKeyID,
		"secretAccessKey":  minioResource.AccessKeySecret,
		"endPoint":         minioResource.Endpoint,
		"forcePathStyle":   true,
		"s3ForcePathStyle": true,
		"disableSSL":       true,
		"region":           minioResource.Region,
		"enableSSE":        false,
		"bucketProvider":   "MINIO",
	}

	jobLocation := uploadFile(t, ctx, destConf, "testdata/staging.json.gz")

	schemaMap := stagingSchema(t)

	publishCh := make(chan *notifier.ClaimJob)
	subscriberCh := make(chan *notifier.ClaimJobResponse)
	defer close(publishCh)
	defer close(subscriberCh)

	slaveNotifier := &mockSlaveNotifier{
		publishCh:   publishCh,
		subscribeCh: subscriberCh,
	}

	workers := config.SingleValueLoader(4)
	workerJobs := 25

	tenantManager := multitenant.New(
		config.New(),
		backendconfig.DefaultBackendConfig,
	)

	slave := New(
		config.New(),
		logger.NOP,
		stats.NOP,
		slaveNotifier,
		bcm.New(config.New(), nil, tenantManager, logger.NOP, stats.NOP),
		constraints.New(config.New()),
		encoding.NewFactory(config.New()),
	)
	slave.config.noOfSlaveWorkerRoutines = workers

	setupDone := make(chan struct{})
	go func() {
		defer close(setupDone)

		require.NoError(t, slave.SetupSlave(ctx))
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
		UniqueLoadGenID:              uuid.New().String(),
		RudderStoragePrefix:          misc.GetRudderObjectStoragePrefix(),
		LoadFileType:                 "csv",
	}

	payloadJson, err := json.Marshal(p)
	require.NoError(t, err)

	claim := &notifier.ClaimJob{
		Job: &notifier.Job{
			ID:                  1,
			BatchID:             uuid.New().String(),
			Payload:             payloadJson,
			Status:              model.Waiting,
			WorkspaceIdentifier: "test_workspace",
			Type:                notifier.JobTypeUpload,
		},
	}

	g, _ := errgroup.WithContext(ctx)
	g.Go(func() error {
		for i := 0; i < workerJobs; i++ {
			publishCh <- claim
		}
		return nil
	})
	g.Go(func() error {
		for i := 0; i < workerJobs; i++ {
			response := <-subscriberCh

			require.NoError(t, response.Err)

			var uploadPayload payload
			err := json.Unmarshal(response.Payload, &uploadPayload)
			require.NoError(t, err)
			require.Equal(t, uploadPayload.BatchID, claim.Job.BatchID)
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
		return nil
	})
	require.NoError(t, g.Wait())

	cancel()
	<-setupDone
}

func uploadFile(t testing.TB, ctx context.Context, destConf map[string]interface{}, filePath string) string {
	t.Helper()

	f, err := os.Open(filePath)
	require.NoError(t, err)

	defer func() {
		require.NoError(t, f.Close())
	}()

	fm, err := filemanager.New(&filemanager.Settings{
		Provider: "MINIO",
		Config: misc.GetObjectStorageConfig(misc.ObjectStorageOptsT{
			Provider: "MINIO",
			Config:   destConf,
		}),
	})
	require.NoError(t, err)

	uploadFile, err := fm.Upload(ctx, f)
	require.NoError(t, err)

	return uploadFile.ObjectName
}

func stagingSchema(t testing.TB) model.Schema {
	t.Helper()

	stagingFile, err := os.Open("testdata/staging.json.gz")
	require.NoError(t, err)

	reader, err := gzip.NewReader(stagingFile)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, reader.Close())
	}()

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

	return schemaMap
}
