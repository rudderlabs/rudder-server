package warehouse

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"

	"golang.org/x/exp/slices"

	"github.com/google/uuid"
	"github.com/ory/dockertest/v3"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
	"github.com/rudderlabs/rudder-server/testhelper/destination"
	"github.com/rudderlabs/rudder-server/warehouse/encoding"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"

	"github.com/stretchr/testify/require"
)

func TestPickupStagingFileBucket(t *testing.T) {
	inputs := []struct {
		job      *Payload
		expected bool
	}{
		{
			job:      &Payload{},
			expected: false,
		},
		{
			job: &Payload{
				StagingDestinationRevisionID: "1liYatjkkCEVkEMYUmSWOE9eZ4n",
				DestinationRevisionID:        "1liYatjkkCEVkEMYUmSWOE9eZ4n",
			},
			expected: false,
		},
		{
			job: &Payload{
				StagingDestinationRevisionID: "1liYatjkkCEVkEMYUmSWOE9eZ4n",
				DestinationRevisionID:        "2liYatjkkCEVkEMYUmSWOE9eZ4n",
			},
			expected: false,
		},
		{
			job: &Payload{
				StagingDestinationRevisionID: "1liYatjkkCEVkEMYUmSWOE9eZ4n",
				DestinationRevisionID:        "2liYatjkkCEVkEMYUmSWOE9eZ4n",
				StagingDestinationConfig:     map[string]string{},
			},
			expected: true,
		},
	}
	for _, input := range inputs {
		got := PickupStagingConfiguration(input.job)
		require.Equal(t, got, input.expected)
	}
}

type mockLoadFileWriter struct {
	file *os.File
}

func (*mockLoadFileWriter) WriteGZ(string) error {
	return nil
}

func (*mockLoadFileWriter) Write(p []byte) (int, error) {
	return 0, nil
}

func (*mockLoadFileWriter) WriteRow(r []interface{}) error {
	return nil
}

func (*mockLoadFileWriter) Close() error {
	return nil
}

func (m *mockLoadFileWriter) GetLoadFile() *os.File {
	return m.file
}

func TestUploadLoadFilesToObjectStorage(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	ctx := context.Background()
	ctxCancel, cancel := context.WithCancel(ctx)
	cancel()

	provider := "MINIO"
	workspaceID := "test-workspace-id"
	destinationID := "test-destination-id"
	destinationName := "test-destination-name"
	sourceID := "test-source-id"
	sourceName := "test-source-name"
	destType := "POSTGRES"
	worker := 7
	prefix := warehouseutils.DatalakeTimeWindowFormat
	namespace := "test-namespace"
	loadObjectFolder := "test-load-object-folder"

	testCases := []struct {
		name              string
		destType          string
		ctx               context.Context
		conf              map[string]any
		additionalWriters int
		wantError         error
	}{
		{
			name:              "Parquet file",
			additionalWriters: 9,
			destType:          warehouseutils.S3Datalake,
		},
		{
			name: "Few files",
		},
		{
			name:              "many files",
			additionalWriters: 49,
		},
		{
			name:      "Context cancelled",
			ctx:       ctxCancel,
			wantError: errors.New("uploading load file to object storage: context canceled"),
		},
		{
			name: "Unknown provider",
			conf: map[string]any{
				"bucketProvider": "UNKNOWN",
			},
			wantError: errors.New("creating uploader: service provider not supported: UNKNOWN"),
		},
		{
			name: "Invalid endpoint",
			conf: map[string]any{
				"endPoint": "http://localhost:1234",
			},
			wantError:         errors.New("uploading load file to object storage: uploading load file: Endpoint url cannot have fully qualified paths."),
			additionalWriters: 9,
		},
	}

	for _, tc := range testCases {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			minioResource, err := destination.SetupMINIO(pool, t)
			require.NoError(t, err)

			conf := map[string]any{
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
				"bucketProvider":   provider,
			}

			for k, v := range tc.conf {
				conf[k] = v
			}

			f, err := os.CreateTemp(t.TempDir(), "load.dump")
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, os.Remove(f.Name())) })

			m := &mockLoadFileWriter{
				file: f,
			}

			writerMap := map[string]encoding.LoadFileWriter{
				"test": m,
			}
			for i := 0; i < tc.additionalWriters; i++ {
				writerMap[fmt.Sprintf("test-%d", i)] = m
			}

			store := memstats.New()
			stagingFileID := int64(1001)

			destType := destType
			if tc.destType != "" {
				destType = tc.destType
			}

			job := Payload{
				StagingFileID:            stagingFileID,
				DestinationConfig:        conf,
				UseRudderStorage:         false,
				StagingDestinationConfig: conf,
				StagingUseRudderStorage:  false,
				WorkspaceID:              workspaceID,
				DestinationID:            destinationID,
				DestinationName:          destinationName,
				SourceID:                 sourceID,
				SourceName:               sourceName,
				DestinationType:          destType,
				LoadFilePrefix:           prefix,
				UniqueLoadGenID:          uuid.New().String(),
				DestinationNamespace:     namespace,
			}
			c := config.New()
			c.Set("Warehouse.numLoadFileUploadWorkers", worker)
			c.Set("Warehouse.slaveUploadTimeout", "5m")
			c.Set("WAREHOUSE_BUCKET_LOAD_OBJECTS_FOLDER_NAME", loadObjectFolder)

			jr := newJobRun(job, c, logger.NOP, store)
			jr.since = func(t time.Time) time.Duration {
				return time.Second
			}
			jr.outputFileWritersMap = writerMap

			ctx := ctx
			if tc.ctx != nil {
				ctx = tc.ctx
			}

			loadFIle, err := jr.uploadLoadFiles(ctx)
			if tc.wantError != nil {
				require.EqualError(t, err, tc.wantError.Error())
				return
			}

			require.NoError(t, err)
			require.Len(t, loadFIle, len(jr.outputFileWritersMap))
			require.EqualValues(t, time.Second*time.Duration(len(jr.outputFileWritersMap)), store.Get("load_file_total_upload_time", jr.defaultTags()).LastDuration())
			for i := 0; i < len(jr.outputFileWritersMap); i++ {
				require.EqualValues(t, time.Second, store.Get("load_file_upload_time", jr.defaultTags()).LastDuration())
			}

			outputPathRegex := fmt.Sprintf(`http://localhost:%s/testbucket/%s/test.*/%s/.*/load.dump`, minioResource.Port, loadObjectFolder, sourceID)
			if slices.Contains(warehouseutils.TimeWindowDestinations, destType) {
				outputPathRegex = fmt.Sprintf(`http://localhost:%s/testbucket/rudder-datalake/%s/test.*/2006/01/02/15/load.dump`, minioResource.Port, namespace)
			}

			for _, f := range loadFIle {
				require.Regexp(t, outputPathRegex, f.Location)
				require.Equal(t, f.StagingFileID, stagingFileID)
			}
		})
	}
}
