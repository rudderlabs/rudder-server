package slave

import (
	"compress/gzip"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"maps"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/google/uuid"
	"github.com/ory/dockertest/v3"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
	"github.com/rudderlabs/rudder-go-kit/testhelper"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/minio"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	mocksBackendConfig "github.com/rudderlabs/rudder-server/mocks/backend-config"
	"github.com/rudderlabs/rudder-server/services/notifier"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
	"github.com/rudderlabs/rudder-server/warehouse/bcm"
	"github.com/rudderlabs/rudder-server/warehouse/constraints"
	"github.com/rudderlabs/rudder-server/warehouse/encoding"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/multitenant"
	"github.com/rudderlabs/rudder-server/warehouse/source"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestSlaveWorker(t *testing.T) {
	misc.Init()

	const (
		workspaceID     = "test_workspace_id"
		sourceID        = "test_source_id"
		sourceName      = "test_source_name"
		destinationID   = "test_destination_id"
		destinationType = "test_destination_type"
		destinationName = "test_destination_name"
		namespace       = "test_namespace"
		workerIdx       = 1
	)

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	minioResource, err := minio.Setup(pool, t)
	require.NoError(t, err)

	ctx := context.Background()

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

	t.Run("Upload Job", func(t *testing.T) {
		jobLocation := uploadFile(t, ctx, destConf, "testdata/staging.json.gz")

		schemaMap := stagingSchema(t)
		ef := encoding.NewFactory(config.New())

		t.Run("success", func(t *testing.T) {
			subscribeCh := make(chan *notifier.ClaimJobResponse)
			defer close(subscribeCh)

			slaveNotifier := &mockSlaveNotifier{
				subscribeCh: subscribeCh,
			}

			tenantManager := multitenant.New(config.New(), backendconfig.DefaultBackendConfig)

			slaveWorker := newWorker(
				config.New(),
				logger.NOP,
				stats.NOP,
				slaveNotifier,
				bcm.New(config.New(), nil, tenantManager, logger.NOP, stats.NOP),
				constraints.New(config.New()),
				ef,
				workerIdx,
			)

			p := payloadV2{
				basePayload: basePayload{
					UploadID:                     1,
					UploadSchema:                 schemaMap,
					WorkspaceID:                  workspaceID,
					SourceID:                     sourceID,
					SourceName:                   sourceName,
					DestinationID:                destinationID,
					DestinationName:              destinationName,
					DestinationType:              destinationType,
					DestinationNamespace:         namespace,
					DestinationRevisionID:        uuid.New().String(),
					StagingDestinationRevisionID: uuid.New().String(),
					DestinationConfig:            destConf,
					StagingDestinationConfig:     map[string]interface{}{},
					UniqueLoadGenID:              uuid.New().String(),
					RudderStoragePrefix:          misc.GetRudderObjectStoragePrefix(),
					LoadFileType:                 "csv",
				},
				StagingFiles: []stagingFileInfo{
					{
						ID:       1,
						Location: jobLocation,
					},
				},
			}

			payloadJson, err := jsonrs.Marshal(p)
			require.NoError(t, err)

			claim := &notifier.ClaimJob{
				Job: &notifier.Job{
					ID:                  1,
					BatchID:             uuid.New().String(),
					Payload:             payloadJson,
					Status:              model.Waiting,
					WorkspaceIdentifier: "test_workspace",
					Type:                notifier.JobTypeUploadV2,
				},
			}

			claimedJobDone := make(chan struct{})
			go func() {
				defer close(claimedJobDone)

				slaveWorker.processClaimedUploadJob(ctx, claim)
			}()

			response := <-subscribeCh
			require.NoError(t, response.Err)

			var uploadPayload payloadV2
			err = jsonrs.Unmarshal(response.Payload, &uploadPayload)
			require.NoError(t, err)
			require.Equal(t, uploadPayload.BatchID, claim.Job.BatchID)
			require.Equal(t, uploadPayload.UploadID, p.UploadID)
			require.Equal(t, uploadPayload.StagingFiles[0].ID, p.StagingFiles[0].ID)
			require.Equal(t, uploadPayload.StagingFiles[0].Location, p.StagingFiles[0].Location)
			require.Equal(t, uploadPayload.UploadSchema, p.UploadSchema)
			require.Equal(t, uploadPayload.WorkspaceID, p.WorkspaceID)
			require.Equal(t, uploadPayload.SourceID, p.SourceID)
			require.Equal(t, uploadPayload.SourceName, p.SourceName)
			require.Equal(t, uploadPayload.DestinationID, p.DestinationID)
			require.Equal(t, uploadPayload.DestinationName, p.DestinationName)
			require.Equal(t, uploadPayload.DestinationType, p.DestinationType)
			require.Equal(t, uploadPayload.DestinationNamespace, p.DestinationNamespace)
			require.Equal(t, uploadPayload.DestinationRevisionID, p.DestinationRevisionID)
			require.Equal(t, uploadPayload.StagingDestinationRevisionID, p.StagingDestinationRevisionID)
			require.Equal(t, uploadPayload.DestinationConfig, p.DestinationConfig)
			require.Equal(t, uploadPayload.StagingDestinationConfig, p.StagingDestinationConfig)
			require.Equal(t, uploadPayload.UseRudderStorage, p.UseRudderStorage)
			require.Equal(t, uploadPayload.StagingUseRudderStorage, p.StagingUseRudderStorage)
			require.Equal(t, uploadPayload.UniqueLoadGenID, p.UniqueLoadGenID)
			require.Equal(t, uploadPayload.RudderStoragePrefix, p.RudderStoragePrefix)
			require.Equal(t, uploadPayload.LoadFileType, p.LoadFileType)

			require.Len(t, uploadPayload.Output, 8)
			for _, output := range uploadPayload.Output {
				require.Equal(t, output.TotalRows, 4)
				require.Equal(t, output.DestinationRevisionID, p.DestinationRevisionID)
				require.Equal(t, output.UseRudderStorage, p.StagingUseRudderStorage)
			}

			<-claimedJobDone
		})

		t.Run("clickhouse bool", func(t *testing.T) {
			subscribeCh := make(chan *notifier.ClaimJobResponse)
			defer close(subscribeCh)

			slaveNotifier := &mockSlaveNotifier{
				subscribeCh: subscribeCh,
			}

			tenantManager := multitenant.New(config.New(), backendconfig.DefaultBackendConfig)

			slaveWorker := newWorker(
				config.New(),
				logger.NOP,
				stats.NOP,
				slaveNotifier,
				bcm.New(config.New(), nil, tenantManager, logger.NOP, stats.NOP),
				constraints.New(config.New()),
				ef,
				workerIdx,
			)

			p := payloadV2{
				basePayload: basePayload{
					UploadID:                     1,
					UploadSchema:                 schemaMap,
					WorkspaceID:                  workspaceID,
					SourceID:                     sourceID,
					SourceName:                   sourceName,
					DestinationID:                destinationID,
					DestinationName:              destinationName,
					DestinationType:              warehouseutils.CLICKHOUSE,
					DestinationNamespace:         namespace,
					DestinationRevisionID:        uuid.New().String(),
					StagingDestinationRevisionID: uuid.New().String(),
					DestinationConfig:            destConf,
					StagingDestinationConfig:     map[string]interface{}{},
					UniqueLoadGenID:              uuid.New().String(),
					RudderStoragePrefix:          misc.GetRudderObjectStoragePrefix(),
					LoadFileType:                 "csv",
				},
				StagingFiles: []stagingFileInfo{
					{
						ID:       1,
						Location: jobLocation,
					},
				},
			}

			payloadJson, err := jsonrs.Marshal(p)
			require.NoError(t, err)

			claim := &notifier.ClaimJob{
				Job: &notifier.Job{
					ID:                  1,
					BatchID:             uuid.New().String(),
					Payload:             payloadJson,
					Status:              model.Waiting,
					WorkspaceIdentifier: "test_workspace",
					Type:                notifier.JobTypeUploadV2,
				},
			}

			claimedJobDone := make(chan struct{})
			go func() {
				defer close(claimedJobDone)

				slaveWorker.processClaimedUploadJob(ctx, claim)
			}()

			response := <-subscribeCh
			require.NoError(t, response.Err)

			var uploadPayload payloadV2
			err = jsonrs.Unmarshal(response.Payload, &uploadPayload)
			require.NoError(t, err)

			for _, output := range uploadPayload.Output {
				require.Equal(t, output.TotalRows, 4)
				require.Equal(t, output.DestinationRevisionID, p.DestinationRevisionID)
				require.Equal(t, output.UseRudderStorage, p.StagingUseRudderStorage)

				fm, err := filemanager.New(&filemanager.Settings{
					Provider: "MINIO",
					Config: misc.GetObjectStorageConfig(misc.ObjectStorageOptsT{
						Provider: "MINIO",
						Config:   destConf,
					}),
					Conf: config.Default,
				})
				require.NoError(t, err)

				objKey, err := fm.GetObjectNameFromLocation(output.Location)
				require.NoError(t, err)

				tmpFile, err := os.CreateTemp("", "load.csv")
				require.NoError(t, err)

				err = fm.Download(ctx, tmpFile, objKey)
				require.NoError(t, err)

				tmpFile, err = os.Open(tmpFile.Name())
				require.NoError(t, err)

				gzReader, err := gzip.NewReader(tmpFile)
				require.NoError(t, err)

				reader := csv.NewReader(gzReader)
				reader.Comma = ','

				sortedColMap := p.sortedColumnMapForAllTables()

				for i := 0; i < output.TotalRows; i++ {
					row, err := reader.Read()
					require.NoError(t, err)

					sortedCols := sortedColMap[output.TableName]

					_, boolColIdx, _ := lo.FindIndexOf(sortedCols, func(item string) bool {
						return item == "test_boolean"
					})
					_, boolArrayColIdx, _ := lo.FindIndexOf(sortedCols, func(item string) bool {
						return item == "test_boolean_array"
					})
					require.Equal(t, row[boolColIdx], "1")
					require.Equal(t, row[boolArrayColIdx], "[1,0]")
				}
				require.NoError(t, gzReader.Close())
			}

			<-claimedJobDone
		})

		t.Run("schema limit exceeded", func(t *testing.T) {
			subscribeCh := make(chan *notifier.ClaimJobResponse)
			defer close(subscribeCh)

			slaveNotifier := &mockSlaveNotifier{
				subscribeCh: subscribeCh,
			}

			c := config.New()
			c.Set("Warehouse.s3_datalake.columnCountLimit", 10)

			tenantManager := multitenant.New(config.New(), backendconfig.DefaultBackendConfig)

			slaveWorker := newWorker(
				c,
				logger.NOP,
				stats.NOP,
				slaveNotifier,
				bcm.New(config.New(), nil, tenantManager, logger.NOP, stats.NOP),
				constraints.New(config.New()),
				ef,
				workerIdx,
			)
			destConfTest := maps.Clone(destConf)
			destConfTest["endPoint"] = fmt.Sprintf("http://%s", minioResource.Endpoint)
			p := payloadV2{
				basePayload: basePayload{
					UploadID:                     1,
					UploadSchema:                 schemaMap,
					WorkspaceID:                  workspaceID,
					SourceID:                     sourceID,
					SourceName:                   sourceName,
					DestinationID:                destinationID,
					DestinationName:              destinationName,
					DestinationType:              warehouseutils.S3Datalake,
					DestinationNamespace:         namespace,
					DestinationRevisionID:        uuid.New().String(),
					StagingDestinationRevisionID: uuid.New().String(),
					DestinationConfig:            destConfTest,
					StagingDestinationConfig:     map[string]interface{}{},
					UniqueLoadGenID:              uuid.New().String(),
					RudderStoragePrefix:          misc.GetRudderObjectStoragePrefix(),
					LoadFileType:                 "csv",
				},
				StagingFiles: []stagingFileInfo{
					{
						ID:       1,
						Location: jobLocation,
					},
				},
			}

			payloadJson, err := jsonrs.Marshal(p)
			require.NoError(t, err)

			claimJob := &notifier.ClaimJob{
				Job: &notifier.Job{
					ID:                  1,
					BatchID:             uuid.New().String(),
					Payload:             payloadJson,
					Status:              model.Waiting,
					WorkspaceIdentifier: "test_workspace",
					Type:                notifier.JobTypeUploadV2,
				},
			}

			claimedJobDone := make(chan struct{})
			go func() {
				defer close(claimedJobDone)

				slaveWorker.processClaimedUploadJob(ctx, claimJob)
			}()

			response := <-subscribeCh
			require.Contains(t, response.Err.Error(), "staging file schema limit exceeded for stagingFileID: 1, actualCount: 21, maxAllowedCount: 10")

			<-claimedJobDone
		})

		t.Run("discards", func(t *testing.T) {
			subscribeCh := make(chan *notifier.ClaimJobResponse)
			defer close(subscribeCh)

			slaveNotifier := &mockSlaveNotifier{
				subscribeCh: subscribeCh,
			}

			tenantManager := multitenant.New(config.New(), backendconfig.DefaultBackendConfig)

			slaveWorker := newWorker(
				config.New(),
				logger.NOP,
				stats.NOP,
				slaveNotifier,
				bcm.New(config.New(), nil, tenantManager, logger.NOP, stats.NOP),
				constraints.New(config.New()),
				ef,
				workerIdx,
			)

			p := payloadV2{
				basePayload: basePayload{
					UploadID: 1,
					UploadSchema: map[string]model.TableSchema{
						"tracks": map[string]string{
							"id":                 "int",
							"user_id":            "int",
							"uuid_ts":            "timestamp",
							"received_at":        "timestamp",
							"original_timestamp": "timestamp",
							"timestamp":          "timestamp",
							"sent_at":            "timestamp",
							"event":              "string",
							"event_text":         "string",
						},
					},
					WorkspaceID:                  workspaceID,
					SourceID:                     sourceID,
					SourceName:                   sourceName,
					DestinationID:                destinationID,
					DestinationName:              destinationName,
					DestinationType:              destinationType,
					DestinationNamespace:         namespace,
					DestinationRevisionID:        uuid.New().String(),
					StagingDestinationRevisionID: uuid.New().String(),
					DestinationConfig:            destConf,
					StagingDestinationConfig:     map[string]interface{}{},
					UniqueLoadGenID:              uuid.New().String(),
					RudderStoragePrefix:          misc.GetRudderObjectStoragePrefix(),
					LoadFileType:                 "csv",
				},
				StagingFiles: []stagingFileInfo{
					{
						ID:       1,
						Location: jobLocation,
					},
				},
			}

			payloadJson, err := jsonrs.Marshal(p)
			require.NoError(t, err)

			claim := &notifier.ClaimJob{
				Job: &notifier.Job{
					ID:                  1,
					BatchID:             uuid.New().String(),
					Payload:             payloadJson,
					Status:              model.Waiting,
					WorkspaceIdentifier: "test_workspace",
					Type:                notifier.JobTypeUploadV2,
				},
			}

			claimedJobDone := make(chan struct{})
			go func() {
				defer close(claimedJobDone)

				slaveWorker.processClaimedUploadJob(ctx, claim)
			}()

			response := <-subscribeCh
			require.NoError(t, response.Err)

			var uploadPayload payloadV2
			err = jsonrs.Unmarshal(response.Payload, &uploadPayload)
			require.NoError(t, err)
			require.Len(t, uploadPayload.Output, 9)

			discardsOutput, ok := lo.Find(uploadPayload.Output, func(o uploadResult) bool {
				return o.TableName == warehouseutils.DiscardsTable
			})
			require.True(t, ok)
			require.Equal(t, discardsOutput.TotalRows, 24)
			require.Equal(t, discardsOutput.DestinationRevisionID, p.DestinationRevisionID)
			require.Equal(t, discardsOutput.UseRudderStorage, p.StagingUseRudderStorage)

			<-claimedJobDone
		})
	})

	t.Run("Upload V2 Job", func(t *testing.T) {
		jobLocation1 := uploadFile(t, ctx, destConf, "testdata/staging.json.gz")
		jobLocation2 := uploadFile(t, ctx, destConf, "testdata/staging2.json.gz")
		jobLocation3 := uploadFile(t, ctx, destConf, "testdata/staging3.json.gz")

		schemaMap := stagingSchema(t)
		ef := encoding.NewFactory(config.New())

		t.Run("success", func(t *testing.T) {
			subscribeCh := make(chan *notifier.ClaimJobResponse)
			defer close(subscribeCh)

			slaveNotifier := &mockSlaveNotifier{
				subscribeCh: subscribeCh,
			}

			tenantManager := multitenant.New(config.New(), backendconfig.DefaultBackendConfig)

			slaveWorker := newWorker(
				config.New(),
				logger.NOP,
				stats.NOP,
				slaveNotifier,
				bcm.New(config.New(), nil, tenantManager, logger.NOP, stats.NOP),
				constraints.New(config.New()),
				ef,
				workerIdx,
			)

			stagingFiles := []stagingFileInfo{
				{ID: 1, Location: jobLocation1},
				{ID: 2, Location: jobLocation2},
				{ID: 3, Location: jobLocation3},
			}

			p := payloadV2{
				basePayload: basePayload{
					UploadID:                     1,
					UploadSchema:                 schemaMap,
					WorkspaceID:                  workspaceID,
					SourceID:                     sourceID,
					SourceName:                   sourceName,
					DestinationID:                destinationID,
					DestinationName:              destinationName,
					DestinationType:              destinationType,
					DestinationNamespace:         namespace,
					DestinationRevisionID:        uuid.New().String(),
					StagingDestinationRevisionID: uuid.New().String(),
					DestinationConfig:            destConf,
					StagingDestinationConfig:     map[string]interface{}{},
					UniqueLoadGenID:              uuid.New().String(),
					RudderStoragePrefix:          misc.GetRudderObjectStoragePrefix(),
					LoadFileType:                 "csv",
				},
				StagingFiles: stagingFiles,
			}

			payloadJson, err := jsonrs.Marshal(p)
			require.NoError(t, err)

			claim := &notifier.ClaimJob{
				Job: &notifier.Job{
					ID:                  1,
					BatchID:             uuid.New().String(),
					Payload:             payloadJson,
					Status:              model.Waiting,
					WorkspaceIdentifier: "test_workspace",
					Type:                notifier.JobTypeUploadV2,
				},
			}

			claimedJobDone := make(chan struct{})
			go func() {
				defer close(claimedJobDone)

				slaveWorker.processClaimedUploadJob(ctx, claim)
			}()

			response := <-subscribeCh
			require.NoError(t, response.Err)

			var uploadPayload payloadV2
			err = jsonrs.Unmarshal(response.Payload, &uploadPayload)
			require.NoError(t, err)
			require.Equal(t, uploadPayload.BatchID, claim.Job.BatchID)
			require.Equal(t, uploadPayload.UploadID, p.UploadID)
			require.Equal(t, uploadPayload.StagingFiles, p.StagingFiles)
			require.Equal(t, uploadPayload.UploadSchema, p.UploadSchema)
			require.Equal(t, uploadPayload.WorkspaceID, p.WorkspaceID)
			require.Equal(t, uploadPayload.SourceID, p.SourceID)
			require.Equal(t, uploadPayload.SourceName, p.SourceName)
			require.Equal(t, uploadPayload.DestinationID, p.DestinationID)
			require.Equal(t, uploadPayload.DestinationName, p.DestinationName)
			require.Equal(t, uploadPayload.DestinationType, p.DestinationType)
			require.Equal(t, uploadPayload.DestinationNamespace, p.DestinationNamespace)
			require.Equal(t, uploadPayload.DestinationRevisionID, p.DestinationRevisionID)
			require.Equal(t, uploadPayload.StagingDestinationRevisionID, p.StagingDestinationRevisionID)
			require.Equal(t, uploadPayload.DestinationConfig, p.DestinationConfig)
			require.Equal(t, uploadPayload.StagingDestinationConfig, p.StagingDestinationConfig)
			require.Equal(t, uploadPayload.UseRudderStorage, p.UseRudderStorage)
			require.Equal(t, uploadPayload.StagingUseRudderStorage, p.StagingUseRudderStorage)
			require.Equal(t, uploadPayload.UniqueLoadGenID, p.UniqueLoadGenID)
			require.Equal(t, uploadPayload.RudderStoragePrefix, p.RudderStoragePrefix)
			require.Equal(t, uploadPayload.LoadFileType, p.LoadFileType)

			require.Len(t, uploadPayload.Output, 8)
			for _, output := range uploadPayload.Output {
				require.Equal(t, output.TotalRows, 12) // 4 rows per file * 3 files
				require.Equal(t, output.UploadID, p.UploadID)
				require.Equal(t, output.DestinationRevisionID, p.DestinationRevisionID)
				require.Equal(t, output.UseRudderStorage, p.StagingUseRudderStorage)
			}

			<-claimedJobDone
		})

		t.Run("clickhouse bool", func(t *testing.T) {
			subscribeCh := make(chan *notifier.ClaimJobResponse)
			defer close(subscribeCh)

			slaveNotifier := &mockSlaveNotifier{
				subscribeCh: subscribeCh,
			}

			tenantManager := multitenant.New(config.New(), backendconfig.DefaultBackendConfig)

			slaveWorker := newWorker(
				config.New(),
				logger.NOP,
				stats.NOP,
				slaveNotifier,
				bcm.New(config.New(), nil, tenantManager, logger.NOP, stats.NOP),
				constraints.New(config.New()),
				ef,
				workerIdx,
			)

			stagingFiles := []stagingFileInfo{
				{ID: 1, Location: jobLocation1},
				{ID: 2, Location: jobLocation2},
				{ID: 3, Location: jobLocation3},
			}

			p := payloadV2{
				basePayload: basePayload{
					UploadID:                     99,
					UploadSchema:                 schemaMap,
					WorkspaceID:                  workspaceID,
					SourceID:                     sourceID,
					SourceName:                   sourceName,
					DestinationID:                destinationID,
					DestinationName:              destinationName,
					DestinationType:              warehouseutils.CLICKHOUSE,
					DestinationNamespace:         namespace,
					DestinationRevisionID:        uuid.New().String(),
					StagingDestinationRevisionID: uuid.New().String(),
					DestinationConfig:            destConf,
					StagingDestinationConfig:     map[string]interface{}{},
					UniqueLoadGenID:              uuid.New().String(),
					RudderStoragePrefix:          misc.GetRudderObjectStoragePrefix(),
					LoadFileType:                 "csv",
				},
				StagingFiles: stagingFiles,
			}

			payloadJson, err := jsonrs.Marshal(p)
			require.NoError(t, err)

			claim := &notifier.ClaimJob{
				Job: &notifier.Job{
					ID:                  1,
					BatchID:             uuid.New().String(),
					Payload:             payloadJson,
					Status:              model.Waiting,
					WorkspaceIdentifier: "test_workspace",
					Type:                notifier.JobTypeUploadV2,
				},
			}

			claimedJobDone := make(chan struct{})
			go func() {
				defer close(claimedJobDone)

				slaveWorker.processClaimedUploadJob(ctx, claim)
			}()

			response := <-subscribeCh
			require.NoError(t, response.Err)

			var uploadPayload payloadV2
			err = jsonrs.Unmarshal(response.Payload, &uploadPayload)
			require.NoError(t, err)

			for _, output := range uploadPayload.Output {
				require.Equal(t, output.TotalRows, 12) // 4 rows per file * 3 files
				require.Equal(t, output.UploadID, p.UploadID)
				require.Equal(t, output.DestinationRevisionID, p.DestinationRevisionID)
				require.Equal(t, output.UseRudderStorage, p.StagingUseRudderStorage)

				fm, err := filemanager.New(&filemanager.Settings{
					Provider: "MINIO",
					Config: misc.GetObjectStorageConfig(misc.ObjectStorageOptsT{
						Provider: "MINIO",
						Config:   destConf,
					}),
					Conf: config.Default,
				})
				require.NoError(t, err)

				objKey, err := fm.GetObjectNameFromLocation(output.Location)
				require.NoError(t, err)

				tmpFile, err := os.CreateTemp("", "load.csv")
				require.NoError(t, err)

				err = fm.Download(ctx, tmpFile, objKey)
				require.NoError(t, err)

				tmpFile, err = os.Open(tmpFile.Name())
				require.NoError(t, err)

				gzReader, err := gzip.NewReader(tmpFile)
				require.NoError(t, err)

				reader := csv.NewReader(gzReader)
				reader.Comma = ','

				sortedColMap := p.sortedColumnMapForAllTables()

				for i := 0; i < output.TotalRows; i++ {
					row, err := reader.Read()
					require.NoError(t, err)

					sortedCols := sortedColMap[output.TableName]

					_, boolColIdx, _ := lo.FindIndexOf(sortedCols, func(item string) bool {
						return item == "test_boolean"
					})
					_, boolArrayColIdx, _ := lo.FindIndexOf(sortedCols, func(item string) bool {
						return item == "test_boolean_array"
					})
					require.Equal(t, row[boolColIdx], "1")
					require.Equal(t, row[boolArrayColIdx], "[1,0]")
				}
				require.NoError(t, gzReader.Close())
			}

			<-claimedJobDone
		})

		t.Run("schema limit exceeded", func(t *testing.T) {
			subscribeCh := make(chan *notifier.ClaimJobResponse)
			defer close(subscribeCh)

			slaveNotifier := &mockSlaveNotifier{
				subscribeCh: subscribeCh,
			}

			c := config.New()
			c.Set("Warehouse.s3_datalake.columnCountLimit", 10)

			tenantManager := multitenant.New(config.New(), backendconfig.DefaultBackendConfig)

			slaveWorker := newWorker(
				c,
				logger.NOP,
				stats.NOP,
				slaveNotifier,
				bcm.New(config.New(), nil, tenantManager, logger.NOP, stats.NOP),
				constraints.New(config.New()),
				ef,
				workerIdx,
			)

			stagingFiles := []stagingFileInfo{
				{ID: 3, Location: jobLocation1},
				{ID: 2, Location: jobLocation2},
				{ID: 1, Location: jobLocation3},
			}
			destConfTest := maps.Clone(destConf)
			destConfTest["endPoint"] = fmt.Sprintf("http://%s", minioResource.Endpoint)
			p := payloadV2{
				basePayload: basePayload{
					UploadID:                     1,
					UploadSchema:                 schemaMap,
					WorkspaceID:                  workspaceID,
					SourceID:                     sourceID,
					SourceName:                   sourceName,
					DestinationID:                destinationID,
					DestinationName:              destinationName,
					DestinationType:              warehouseutils.S3Datalake,
					DestinationNamespace:         namespace,
					DestinationRevisionID:        uuid.New().String(),
					StagingDestinationRevisionID: uuid.New().String(),
					DestinationConfig:            destConfTest,
					StagingDestinationConfig:     map[string]interface{}{},
					UniqueLoadGenID:              uuid.New().String(),
					RudderStoragePrefix:          misc.GetRudderObjectStoragePrefix(),
					LoadFileType:                 "csv",
				},
				StagingFiles: stagingFiles,
			}

			payloadJson, err := jsonrs.Marshal(p)
			require.NoError(t, err)

			claimJob := &notifier.ClaimJob{
				Job: &notifier.Job{
					ID:                  1,
					BatchID:             uuid.New().String(),
					Payload:             payloadJson,
					Status:              model.Waiting,
					WorkspaceIdentifier: "test_workspace",
					Type:                notifier.JobTypeUploadV2,
				},
			}

			claimedJobDone := make(chan struct{})
			go func() {
				defer close(claimedJobDone)

				slaveWorker.processClaimedUploadJob(ctx, claimJob)
			}()

			response := <-subscribeCh
			require.Regexp(t, `staging file schema limit exceeded for stagingFileID: \d+, actualCount: 21, maxAllowedCount: 10`, response.Err.Error())

			<-claimedJobDone
		})

		t.Run("discards", func(t *testing.T) {
			subscribeCh := make(chan *notifier.ClaimJobResponse)
			defer close(subscribeCh)

			slaveNotifier := &mockSlaveNotifier{
				subscribeCh: subscribeCh,
			}

			tenantManager := multitenant.New(config.New(), backendconfig.DefaultBackendConfig)

			slaveWorker := newWorker(
				config.New(),
				logger.NOP,
				stats.NOP,
				slaveNotifier,
				bcm.New(config.New(), nil, tenantManager, logger.NOP, stats.NOP),
				constraints.New(config.New()),
				ef,
				workerIdx,
			)

			stagingFiles := []stagingFileInfo{
				{ID: 1, Location: jobLocation1},
				{ID: 2, Location: jobLocation2},
				{ID: 3, Location: jobLocation3},
			}

			p := payloadV2{
				basePayload: basePayload{
					UploadID: 1,
					UploadSchema: map[string]model.TableSchema{
						"tracks": map[string]string{
							"id":                 "int",
							"user_id":            "int",
							"uuid_ts":            "timestamp",
							"received_at":        "timestamp",
							"original_timestamp": "timestamp",
							"timestamp":          "timestamp",
							"sent_at":            "timestamp",
							"event":              "string",
							"event_text":         "string",
						},
					},
					WorkspaceID:                  workspaceID,
					SourceID:                     sourceID,
					SourceName:                   sourceName,
					DestinationID:                destinationID,
					DestinationName:              destinationName,
					DestinationType:              destinationType,
					DestinationNamespace:         namespace,
					DestinationRevisionID:        uuid.New().String(),
					StagingDestinationRevisionID: uuid.New().String(),
					DestinationConfig:            destConf,
					StagingDestinationConfig:     map[string]interface{}{},
					UniqueLoadGenID:              uuid.New().String(),
					RudderStoragePrefix:          misc.GetRudderObjectStoragePrefix(),
					LoadFileType:                 "csv",
				},
				StagingFiles: stagingFiles,
			}

			payloadJson, err := jsonrs.Marshal(p)
			require.NoError(t, err)

			claim := &notifier.ClaimJob{
				Job: &notifier.Job{
					ID:                  1,
					BatchID:             uuid.New().String(),
					Payload:             payloadJson,
					Status:              model.Waiting,
					WorkspaceIdentifier: "test_workspace",
					Type:                notifier.JobTypeUploadV2,
				},
			}

			claimedJobDone := make(chan struct{})
			go func() {
				defer close(claimedJobDone)

				slaveWorker.processClaimedUploadJob(ctx, claim)
			}()

			response := <-subscribeCh
			require.NoError(t, response.Err)

			var uploadPayload payloadV2
			err = jsonrs.Unmarshal(response.Payload, &uploadPayload)
			require.NoError(t, err)
			require.Len(t, uploadPayload.Output, 9)

			discardsOutput, ok := lo.Find(uploadPayload.Output, func(o uploadResult) bool {
				return o.TableName == warehouseutils.DiscardsTable
			})
			require.True(t, ok)
			require.Equal(t, discardsOutput.TotalRows, 72) // 24 discards per file * 3 files
			require.Equal(t, discardsOutput.UploadID, p.UploadID)
			require.Equal(t, discardsOutput.DestinationRevisionID, p.DestinationRevisionID)
			require.Equal(t, discardsOutput.UseRudderStorage, p.StagingUseRudderStorage)

			<-claimedJobDone
		})
	})

	t.Run("async job", func(t *testing.T) {
		pgResource, err := postgres.Setup(pool, t)
		require.NoError(t, err)

		t.Log("db:", pgResource.DBDsn)

		_, err = pgResource.DB.ExecContext(ctx, "CREATE SCHEMA IF NOT EXISTS test_namespace;")
		require.NoError(t, err)
		_, err = pgResource.DB.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS test_namespace.test_table_name (id int, user_id int, uuid_ts timestamp, received_at timestamp, original_timestamp timestamp, timestamp timestamp, sent_at timestamp, event text, event_text text, context_sources_job_run_id text, context_sources_task_run_id text, context_source_id text, context_destination_id text);")
		require.NoError(t, err)

		ctrl := gomock.NewController(t)
		mockBackendConfig := mocksBackendConfig.NewMockBackendConfig(ctrl)
		mockBackendConfig.EXPECT().Subscribe(gomock.Any(), backendconfig.TopicBackendConfig).DoAndReturn(func(ctx context.Context, topic backendconfig.Topic) pubsub.DataChannel {
			ch := make(chan pubsub.DataEvent, 1)
			ch <- pubsub.DataEvent{
				Data: map[string]backendconfig.ConfigT{
					workspaceID: {
						WorkspaceID: workspaceID,
						Sources: []backendconfig.SourceT{
							{
								ID:      sourceID,
								Enabled: true,
								Destinations: []backendconfig.DestinationT{
									{
										ID:      destinationID,
										Enabled: true,
										DestinationDefinition: backendconfig.DestinationDefinitionT{
											Name: warehouseutils.POSTGRES,
										},
										Config: map[string]interface{}{
											"host":             pgResource.Host,
											"database":         pgResource.Database,
											"user":             pgResource.User,
											"password":         pgResource.Password,
											"port":             pgResource.Port,
											"sslMode":          "disable",
											"namespace":        "test_namespace",
											"useSSL":           false,
											"syncFrequency":    "30",
											"useRudderStorage": false,
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
										},
									},
								},
							},
						},
					},
				},
				Topic: string(backendconfig.TopicBackendConfig),
			}
			close(ch)
			return ch
		}).AnyTimes()

		tenantManager := multitenant.New(config.New(), mockBackendConfig)
		bcm := bcm.New(config.New(), nil, tenantManager, logger.NOP, stats.NOP)
		ef := encoding.NewFactory(config.New())

		setupCh := make(chan struct{})
		go func() {
			defer close(setupCh)

			bcm.Start(ctx)
		}()

		<-bcm.InitialConfigFetched
		<-setupCh

		t.Run("success", func(t *testing.T) {
			subscribeCh := make(chan *notifier.ClaimJobResponse)
			defer close(subscribeCh)

			slaveNotifier := &mockSlaveNotifier{
				subscribeCh: subscribeCh,
			}

			c := config.New()
			c.Set("Warehouse.postgres.enableDeleteByJobs", true)

			slaveWorker := newWorker(
				c,
				logger.NOP,
				stats.NOP,
				slaveNotifier,
				bcm,
				constraints.New(config.New()),
				ef,
				workerIdx,
			)

			p := source.NotifierRequest{
				ID:            1,
				SourceID:      sourceID,
				DestinationID: destinationID,
				TableName:     "test_table_name",
				WorkspaceID:   workspaceID,
				JobType:       model.SourceJobTypeDeleteByJobRunID.String(),
				MetaData:      []byte(`{"job_run_id": "1", "task_run_id": "1", "start_time": "2020-01-01T00:00:00Z"}`),
			}

			payloadJson, err := jsonrs.Marshal(p)
			require.NoError(t, err)

			claim := &notifier.ClaimJob{
				Job: &notifier.Job{
					ID:                  1,
					BatchID:             uuid.New().String(),
					Payload:             payloadJson,
					Status:              model.Waiting,
					WorkspaceIdentifier: "test_workspace",
					Type:                notifier.JobTypeAsync,
				},
			}

			claimedJobDone := make(chan struct{})
			go func() {
				defer close(claimedJobDone)

				slaveWorker.processClaimedSourceJob(ctx, claim)
			}()

			response := <-subscribeCh
			require.NoError(t, response.Err)

			var notifierResponse source.NotifierResponse
			err = jsonrs.Unmarshal(response.Payload, &notifierResponse)
			require.NoError(t, err)

			require.Equal(t, int64(1), notifierResponse.ID)

			<-claimedJobDone
		})

		t.Run("invalid configurations", func(t *testing.T) {
			subscribeCh := make(chan *notifier.ClaimJobResponse)
			defer close(subscribeCh)

			slaveNotifier := &mockSlaveNotifier{
				subscribeCh: subscribeCh,
			}

			c := config.New()
			c.Set("Warehouse.postgres.enableDeleteByJobs", true)

			slaveWorker := newWorker(
				c,
				logger.NOP,
				stats.NOP,
				slaveNotifier,
				bcm,
				constraints.New(config.New()),
				ef,
				workerIdx,
			)

			testCases := []struct {
				name          string
				sourceID      string
				destinationID string
				jobType       model.SourceJobType
				expectedError error
			}{
				{
					name:          "invalid parameters",
					jobType:       model.SourceJobTypeDeleteByJobRunID,
					expectedError: errors.New("getting warehouse: invalid Parameters"),
				},
				{
					name:          "invalid source id",
					sourceID:      "invalid_source_id",
					destinationID: destinationID,
					jobType:       model.SourceJobTypeDeleteByJobRunID,
					expectedError: errors.New("getting warehouse: invalid Source Id"),
				},
				{
					name:          "invalid destination id",
					sourceID:      sourceID,
					destinationID: "invalid_destination_id",
					jobType:       model.SourceJobTypeDeleteByJobRunID,
					expectedError: errors.New("getting warehouse: invalid Destination Id"),
				},
			}

			for _, tc := range testCases {
				t.Run(tc.name, func(t *testing.T) {
					p := source.NotifierRequest{
						ID:            1,
						SourceID:      tc.sourceID,
						DestinationID: tc.destinationID,
						TableName:     "test_table_name",
						WorkspaceID:   workspaceID,
						JobType:       tc.jobType.String(),
						MetaData:      []byte(`{"job_run_id": "1", "task_run_id": "1", "start_time": "2020-01-01T00:00:00Z"}`),
					}

					payloadJson, err := jsonrs.Marshal(p)
					require.NoError(t, err)

					claim := &notifier.ClaimJob{
						Job: &notifier.Job{
							ID:                  1,
							BatchID:             uuid.New().String(),
							Payload:             payloadJson,
							Status:              model.Waiting,
							WorkspaceIdentifier: "test_workspace",
							Attempt:             0,
							Type:                notifier.JobTypeAsync,
						},
					}

					claimedJobDone := make(chan struct{})
					go func() {
						defer close(claimedJobDone)

						slaveWorker.processClaimedSourceJob(ctx, claim)
					}()

					response := <-subscribeCh
					require.EqualError(t, response.Err, tc.expectedError.Error())

					<-claimedJobDone
				})
			}
		})
	})
}

func TestHandleSchemaChange(t *testing.T) {
	testCases := []struct {
		name              string
		existingDatatype  string
		currentDataType   string
		value             any
		expectedColumnVal any
		expectedError     error
	}{
		{
			name:              "should send int values if existing datatype is int, new datatype is float",
			existingDatatype:  "int",
			currentDataType:   "float",
			value:             1.501,
			expectedColumnVal: 1,
		},
		{
			name:              "should send float values if existing datatype is float, new datatype is int",
			existingDatatype:  "float",
			currentDataType:   "int",
			value:             1,
			expectedColumnVal: 1.0,
		},
		{
			name:              "should send string values if existing datatype is string, new datatype is boolean",
			existingDatatype:  "string",
			currentDataType:   "boolean",
			value:             false,
			expectedColumnVal: "false",
		},
		{
			name:              "should send string values if existing datatype is string, new datatype is int",
			existingDatatype:  "string",
			currentDataType:   "int",
			value:             1,
			expectedColumnVal: "1",
		},
		{
			name:              "should send string values if existing datatype is string, new datatype is float",
			existingDatatype:  "string",
			currentDataType:   "float",
			value:             1.501,
			expectedColumnVal: "1.501",
		},
		{
			name:              "should send string values if existing datatype is string, new datatype is datetime",
			existingDatatype:  "string",
			currentDataType:   "datetime",
			value:             "2022-05-05T00:00:00.000Z",
			expectedColumnVal: "2022-05-05T00:00:00.000Z",
		},
		{
			name:              "should send string values if existing datatype is string, new datatype is string",
			existingDatatype:  "string",
			currentDataType:   "json",
			value:             `{"json":true}`,
			expectedColumnVal: `{"json":true}`,
		},
		{
			name:              "should send json string values if existing datatype is json, new datatype is boolean",
			existingDatatype:  "json",
			currentDataType:   "boolean",
			value:             false,
			expectedColumnVal: "false",
		},
		{
			name:              "should send json string values if existing datatype is jso, new datatype is int",
			existingDatatype:  "json",
			currentDataType:   "int",
			value:             1,
			expectedColumnVal: "1",
		},
		{
			name:              "should send json string values if existing datatype is json, new datatype is float",
			existingDatatype:  "json",
			currentDataType:   "float",
			value:             1.501,
			expectedColumnVal: "1.501",
		},
		{
			name:              "should send json string values if existing datatype is json, new datatype is json",
			existingDatatype:  "json",
			currentDataType:   "datetime",
			value:             "2022-05-05T00:00:00.000Z",
			expectedColumnVal: `"2022-05-05T00:00:00.000Z"`,
		},
		{
			name:              "should send json string values if existing datatype is json, new datatype is string",
			existingDatatype:  "json",
			currentDataType:   "string",
			value:             "string value",
			expectedColumnVal: `"string value"`,
		},
		{
			name:              "should send json string values if existing datatype is json, new datatype is string with json value",
			existingDatatype:  "json",
			currentDataType:   "string",
			value:             `{"json":"value"}`,
			expectedColumnVal: `"{\"json\":\"value\"}"`,
		},
		{
			name:              "should send json string values if existing datatype is json, new datatype is array",
			existingDatatype:  "json",
			currentDataType:   "array",
			value:             []any{false, 1, "string value"},
			expectedColumnVal: []any{false, 1, "string value"},
		},
		{
			name:             "existing datatype is boolean, new datatype is int",
			existingDatatype: "boolean",
			currentDataType:  "int",
			value:            1,
			expectedError:    errors.New("incompatible schema conversion from boolean to int"),
		},
		{
			name:             "existing datatype is boolean, new datatype is float",
			existingDatatype: "boolean",
			currentDataType:  "float",
			value:            1.501,
			expectedError:    errors.New("incompatible schema conversion from boolean to float"),
		},
		{
			name:             "existing datatype is boolean, new datatype is string",
			existingDatatype: "boolean",
			currentDataType:  "string",
			value:            "string value",
			expectedError:    errors.New("incompatible schema conversion from boolean to string"),
		},
		{
			name:             "existing datatype is boolean, new datatype is datetime",
			existingDatatype: "boolean",
			currentDataType:  "datetime",
			value:            "2022-05-05T00:00:00.000Z",
			expectedError:    errors.New("incompatible schema conversion from boolean to datetime"),
		},
		{
			name:             "existing datatype is boolean, new datatype is json",
			existingDatatype: "boolean",
			currentDataType:  "json",
			value:            `{"json":true}`,
			expectedError:    errors.New("incompatible schema conversion from boolean to json"),
		},
		{
			name:             "existing datatype is int, new datatype is boolean",
			existingDatatype: "int",
			currentDataType:  "boolean",
			value:            false,
			expectedError:    errors.New("incompatible schema conversion from int to boolean"),
		},
		{
			name:             "existing datatype is int, new datatype is string",
			existingDatatype: "int",
			currentDataType:  "string",
			value:            "string value",
			expectedError:    errors.New("incompatible schema conversion from int to string"),
		},
		{
			name:             "existing datatype is int, new datatype is datetime",
			existingDatatype: "int",
			currentDataType:  "datetime",
			value:            "2022-05-05T00:00:00.000Z",
			expectedError:    errors.New("incompatible schema conversion from int to datetime"),
		},
		{
			name:             "existing datatype is int, new datatype is json",
			existingDatatype: "int",
			currentDataType:  "json",
			value:            `{"json":true}`,
			expectedError:    errors.New("incompatible schema conversion from int to json"),
		},
		{
			name:             "existing datatype is int, new datatype is float",
			existingDatatype: "int",
			currentDataType:  "float",
			value:            1,
			expectedError:    errors.New("incompatible schema conversion from int to float"),
		},
		{
			name:             "existing datatype is float, new datatype is boolean",
			existingDatatype: "float",
			currentDataType:  "boolean",
			value:            false,
			expectedError:    errors.New("incompatible schema conversion from float to boolean"),
		},
		{
			name:             "existing datatype is float, new datatype is int",
			existingDatatype: "float",
			currentDataType:  "int",
			value:            1.0,
			expectedError:    errors.New("incompatible schema conversion from float to int"),
		},
		{
			name:             "existing datatype is float, new datatype is string",
			existingDatatype: "float",
			currentDataType:  "string",
			value:            "string value",
			expectedError:    errors.New("incompatible schema conversion from float to string"),
		},
		{
			name:             "existing datatype is float, new datatype is datetime",
			existingDatatype: "float",
			currentDataType:  "datetime",
			value:            "2022-05-05T00:00:00.000Z",
			expectedError:    errors.New("incompatible schema conversion from float to datetime"),
		},
		{
			name:             "existing datatype is float, new datatype is json",
			existingDatatype: "float",
			currentDataType:  "json",
			value:            `{"json":true}`,
			expectedError:    errors.New("incompatible schema conversion from float to json"),
		},
		{
			name:             "existing datatype is datetime, new datatype is boolean",
			existingDatatype: "datetime",
			currentDataType:  "boolean",
			value:            false,
			expectedError:    errors.New("incompatible schema conversion from datetime to boolean"),
		},
		{
			name:             "existing datatype is datetime, new datatype is string",
			existingDatatype: "datetime",
			currentDataType:  "string",
			value:            "string value",
			expectedError:    errors.New("incompatible schema conversion from datetime to string"),
		},
		{
			name:             "existing datatype is datetime, new datatype is int",
			existingDatatype: "datetime",
			currentDataType:  "int",
			value:            1,
			expectedError:    errors.New("incompatible schema conversion from datetime to int"),
		},
		{
			name:             "existing datatype is datetime, new datatype is float",
			existingDatatype: "datetime",
			currentDataType:  "float",
			value:            1.501,
			expectedError:    errors.New("incompatible schema conversion from datetime to float"),
		},
		{
			name:             "existing datatype is datetime, new datatype is json",
			existingDatatype: "datetime",
			currentDataType:  "json",
			value:            `{"json":true}`,
			expectedError:    errors.New("incompatible schema conversion from datetime to json"),
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			newColumnVal, convError := HandleSchemaChange(
				tc.existingDatatype,
				tc.currentDataType,
				tc.value,
			)
			if tc.expectedError != nil {
				require.Nil(t, newColumnVal)
				require.EqualError(t, convError, tc.expectedError.Error())
				return
			}
			require.EqualValues(t, newColumnVal, tc.expectedColumnVal)
			require.NoError(t, convError)
		})
	}
}

func TestSlaveWorkerClaimRefresh(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	refreshCalls := make(chan int64, 10)
	mockNotifier := &mockSlaveNotifier{
		refreshClaim: func(ctx context.Context, jobId int64) error {
			refreshCalls <- jobId
			return nil
		},
	}

	conf := config.New()
	conf.Set("Warehouse.claimRefreshIntervalInS", "1")
	conf.Set("Warehouse.enableNotifierHeartbeat", true)

	worker := newWorker(
		conf,
		logger.NOP,
		stats.NOP,
		mockNotifier,
		bcm.New(conf, nil, multitenant.New(conf, backendconfig.DefaultBackendConfig), logger.NOP, stats.NOP),
		constraints.New(conf),
		encoding.NewFactory(conf),
		1,
	)
	worker.refreshClaimJitter = 0

	job := &notifier.ClaimJob{
		Job: &notifier.Job{
			ID:   123,
			Type: notifier.JobTypeUploadV2,
		},
	}

	// Start worker in background
	notificationChan := make(chan *notifier.ClaimJob, 1)
	go worker.start(ctx, notificationChan, "slaveID")

	// Send job to worker
	notificationChan <- job

	timeout := time.After(3 * time.Second)
	var callCount int

loop:
	for {
		select {
		case jobId := <-refreshCalls:
			callCount++
			require.Equal(t, int64(123), jobId)
		case <-timeout:
			break loop
		}
	}

	// Verify we got at least 2 refresh calls
	require.GreaterOrEqual(t, callCount, 2, "Expected at least 2 refresh calls")
	cancel()
}

func TestStagingFileDuplicateEventsMetric(t *testing.T) {
	workerId := 1
	metricName := "duplicate_events_in_staging_file"

	// Helper to create a gzip staging file with given events
	createStagingFile := func(events []string, filename string) string {
		f, err := os.CreateTemp(t.TempDir(), filename)
		require.NoError(t, err)
		gzWriter := gzip.NewWriter(f)
		for _, event := range events {
			_, err := gzWriter.Write([]byte(event + "\n"))
			require.NoError(t, err)
		}
		require.NoError(t, gzWriter.Close())
		require.NoError(t, f.Close())
		return f.Name()
	}

	eventTemplate := `{"metadata":{"table":"%s"},"data":{"id":"%s"}}`

	t.Run("increments metric for duplicates", func(t *testing.T) {
		events := []string{
			fmt.Sprintf(eventTemplate, "test_table1", "id1"),
			fmt.Sprintf(eventTemplate, "test_table", "id2"),
			fmt.Sprintf(eventTemplate, "test_table", "id1"), // duplicate
			fmt.Sprintf(eventTemplate, "test_table", "id3"),
			fmt.Sprintf(eventTemplate, "test_table", "id2"), // duplicate
		}
		statsStore, err := memstats.New()
		require.NoError(t, err)
		w := newWorker(config.New(), logger.NOP, statsStore, nil, nil, nil, nil, workerId)
		jr := newJobRun(basePayload{}, workerId, config.New(), logger.NOP, statsStore, w.encodingFactory)
		jr.downloadStagingFile = func(ctx context.Context, stagingFileInfo stagingFileInfo) error {
			stagingFilePath1 := createStagingFile(events, "staging1.json.gz")
			stagingFilePath2 := createStagingFile(append(events, fmt.Sprintf(eventTemplate, "test_table", "id1")), "staging2.json.gz")
			jr.stagingFilePaths = map[int64]string{1: stagingFilePath1, 2: stagingFilePath2}
			return nil
		}
		err = w.processSingleStagingFile(context.Background(), jr, &jr.job, stagingFileInfo{ID: 1}, "")
		require.NoError(t, err)
		m := statsStore.Get(metricName, jr.buildTags())
		require.EqualValues(t, 1, m.LastValue())
		value1 := m.LastValue()

		err = w.processSingleStagingFile(context.Background(), jr, &jr.job, stagingFileInfo{ID: 2}, "")
		require.NoError(t, err)
		m = statsStore.Get(metricName, jr.buildTags())
		require.EqualValues(t, value1+(value1+1), m.LastValue())
	})

	t.Run("does not increment metric when no duplicates", func(t *testing.T) {
		events := []string{
			fmt.Sprintf(eventTemplate, "test_table", "id1"),
			fmt.Sprintf(eventTemplate, "test_table", "id2"),
			fmt.Sprintf(eventTemplate, "test_table", "id3"),
			fmt.Sprintf(eventTemplate, "test_table2", "id1"),
			fmt.Sprintf(eventTemplate, "test_table2", "id2"),
			fmt.Sprintf(eventTemplate, "test_table2", "id3"),
		}
		stagingFilePath := createStagingFile(events, "staging3.json.gz")
		statsStore, err := memstats.New()
		require.NoError(t, err)
		w := newWorker(config.New(), logger.NOP, statsStore, nil, nil, nil, nil, workerId)
		jr := newJobRun(basePayload{}, workerId, config.New(), logger.NOP, statsStore, w.encodingFactory)
		jr.downloadStagingFile = func(ctx context.Context, stagingFileInfo stagingFileInfo) error {
			jr.stagingFilePaths = map[int64]string{2: stagingFilePath}
			return nil
		}
		err = w.processSingleStagingFile(context.Background(), jr, &jr.job, stagingFileInfo{ID: 2, Location: stagingFilePath}, "")
		require.NoError(t, err)
		m := statsStore.Get(metricName, jr.buildTags())
		require.EqualValues(t, 0, m.LastValue()) // no duplicates
	})
}

func TestLoadFileDeterministicNaming(t *testing.T) {
	misc.Init()
	const (
		workspaceID     = "test_workspace_id"
		sourceID        = "test_source_id"
		sourceName      = "test_source_name"
		destinationID   = "test_destination_id"
		destinationType = "test_destination_type"
		destinationName = "test_destination_name"
		namespace       = "test_namespace"
		workerIdx       = 1
	)

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	minioResource, err := minio.Setup(pool, t)
	require.NoError(t, err)

	ctx := context.Background()

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

	t.Run("deterministic load file naming disabled", func(t *testing.T) {
		jobLocation := uploadFile(t, ctx, destConf, "testdata/staging.json.gz")

		schemaMap := stagingSchema(t)
		ef := encoding.NewFactory(config.New())

		c := config.New()
		c.Set("Warehouse.useDeterministicLoadFileName", false)

		tenantManager := multitenant.New(config.New(), backendconfig.DefaultBackendConfig)

		slaveWorker := newWorker(
			c,
			logger.NOP,
			stats.NOP,
			nil, // notifier not needed for direct call
			bcm.New(config.New(), nil, tenantManager, logger.NOP, stats.NOP),
			constraints.New(config.New()),
			ef,
			workerIdx,
		)

		stagingFiles := []stagingFileInfo{
			{ID: 1, Location: jobLocation},
		}

		job := payloadV2{
			basePayload: basePayload{
				UploadID:                     1,
				UploadSchema:                 schemaMap,
				WorkspaceID:                  workspaceID,
				SourceID:                     sourceID,
				SourceName:                   sourceName,
				DestinationID:                destinationID,
				DestinationName:              destinationName,
				DestinationType:              destinationType,
				DestinationNamespace:         namespace,
				DestinationRevisionID:        uuid.New().String(),
				StagingDestinationRevisionID: uuid.New().String(),
				DestinationConfig:            destConf,
				StagingDestinationConfig:     map[string]interface{}{},
				UniqueLoadGenID:              uuid.New().String(),
				RudderStoragePrefix:          misc.GetRudderObjectStoragePrefix(),
				LoadFileType:                 "csv",
			},
			StagingFiles: stagingFiles,
		}

		output, err := slaveWorker.processMultiStagingFiles(ctx, &job)
		require.NoError(t, err)

		// Verify that load files have non-deterministic paths (contain UUID)
		for _, result := range output {
			location := result.Location
			fileName := path.Base(location)

			// file name should be {stagingFilePath}.{sourceID}.{UUID}.{fileFormat}
			parts := strings.Split(fileName, ".")
			// In this case csv.gz file format is used
			uuidPart := parts[len(parts)-3]
			require.True(t, misc.IsValidUUID(uuidPart), "Non-deterministic file name should contain UUID")
		}
	})

	t.Run("deterministic load file name with GCS", func(t *testing.T) {
		port, err := testhelper.GetFreePort()
		require.NoError(t, err)

		server, err := fakestorage.NewServerWithOptions(fakestorage.Options{
			Scheme: "http",
			Host:   "127.0.0.1",
			Port:   uint16(port),
			InitialObjects: []fakestorage.Object{
				{
					ObjectAttrs: fakestorage.ObjectAttrs{
						BucketName: "test-bucket",
						Name:       "test-prefix/testFile",
					},
					Content: []byte("inside the file"),
				},
			},
		})
		require.NoError(t, err)
		defer server.Stop()
		_ = os.Setenv("STORAGE_EMULATOR_HOST", server.URL())
		_ = os.Setenv("RSERVER_WORKLOAD_IDENTITY_TYPE", "GKE")

		gcsURL := fmt.Sprintf("%s/storage/v1/", server.URL())
		t.Log("GCS URL:", gcsURL)

		conf := map[string]interface{}{
			"bucketName": "test-bucket",
			"prefix":     "test-prefix",
			"endPoint":   gcsURL,
			"disableSSL": true,
			"jsonReads":  true,
		}

		fm, err := filemanager.New(&filemanager.Settings{
			Provider: warehouseutils.GCS,
			Config:   conf,
			Conf:     config.Default,
		})
		require.NoError(t, err)

		stagingFiles := []string{
			"testdata/staging.json.gz",
			"testdata/staging2.json.gz",
			"testdata/staging3.json.gz",
		}

		stagingFileLocations := []string{}

		for _, stagingFile := range stagingFiles {
			file, err := os.Open(stagingFile)
			require.NoError(t, err)
			uploadedFile, err := fm.Upload(ctx, file)
			require.NoError(t, err)
			stagingFileLocations = append(stagingFileLocations, fm.GetDownloadKeyFromFileLocation(uploadedFile.Location))
			_ = file.Close()
		}

		schemaMap := stagingSchema(t)
		ef := encoding.NewFactory(config.New())

		c := config.New()
		c.Set("Warehouse.useDeterministicLoadFileName", true)

		tenantManager := multitenant.New(config.New(), backendconfig.DefaultBackendConfig)

		slaveWorker := newWorker(
			c,
			logger.NOP,
			stats.NOP,
			nil,
			bcm.New(config.New(), nil, tenantManager, logger.NOP, stats.NOP),
			constraints.New(config.New()),
			ef,
			workerIdx,
		)

		stagingFileInfo := lo.Map(stagingFileLocations, func(location string, i int) stagingFileInfo {
			return stagingFileInfo{ID: int64(i + 1), Location: location}
		})

		job := payloadV2{
			basePayload: basePayload{
				UploadID:                     1,
				UploadSchema:                 schemaMap,
				WorkspaceID:                  workspaceID,
				SourceID:                     sourceID,
				SourceName:                   sourceName,
				DestinationID:                destinationID,
				DestinationName:              destinationName,
				DestinationType:              warehouseutils.GCSDatalake, // Use time window destination
				DestinationNamespace:         namespace,
				DestinationRevisionID:        uuid.New().String(),
				StagingDestinationRevisionID: uuid.New().String(),
				DestinationConfig:            conf,
				StagingDestinationConfig:     conf,
				UniqueLoadGenID:              uuid.New().String(),
				RudderStoragePrefix:          misc.GetRudderObjectStoragePrefix(),
				LoadFileType:                 "csv",
			},
			StagingFiles: stagingFileInfo,
		}

		output, err := slaveWorker.processMultiStagingFiles(ctx, &job)
		require.NoError(t, err)

		// Verify that all load files have the same deterministic path structure
		// The path should be: {loadFileNamePrefix}.{sourceID}.{fileFormat}
		// where loadFileNamePrefix is MD5 hash of all staging file locations
		expectedLoadFileNamePrefix := misc.GetMD5Hash(strings.Join(stagingFileLocations, ""))
		expectedFileFormat := warehouseutils.GetLoadFileFormat("csv") // csv.gz

		for _, result := range output {
			location := result.Location
			fileName := path.Base(location)

			// Verify the file name follows the deterministic pattern
			expectedFileName := fmt.Sprintf("%s.%s.%s", expectedLoadFileNamePrefix, sourceID, expectedFileFormat)
			require.Equal(t, expectedFileName, fileName, "Load file name does not match expected pattern")
		}

		// reprocess the files, Test fails if GCS upload is not idempotent
		output, err = slaveWorker.processMultiStagingFiles(ctx, &job)
		require.NoError(t, err)
		for _, result := range output {
			require.Empty(t, result.Location, "location should be empty")
		}
	})
}
