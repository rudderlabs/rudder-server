package slave

import (
	"compress/gzip"
	"context"
	"encoding/csv"
	"errors"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/ory/dockertest/v3"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/minio"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"

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

			p := payload{
				UploadID:                     1,
				StagingFileID:                1,
				StagingFileLocation:          jobLocation,
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

			claimedJobDone := make(chan struct{})
			go func() {
				defer close(claimedJobDone)

				slaveWorker.processClaimedUploadJob(ctx, claim)
			}()

			response := <-subscribeCh
			require.NoError(t, response.Err)

			var uploadPayload payload
			err = json.Unmarshal(response.Payload, &uploadPayload)
			require.NoError(t, err)
			require.Equal(t, uploadPayload.BatchID, claim.Job.BatchID)
			require.Equal(t, uploadPayload.UploadID, p.UploadID)
			require.Equal(t, uploadPayload.StagingFileID, p.StagingFileID)
			require.Equal(t, uploadPayload.StagingFileLocation, p.StagingFileLocation)
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
				require.Equal(t, output.StagingFileID, p.StagingFileID)
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

			p := payload{
				UploadID:                     1,
				StagingFileID:                1,
				StagingFileLocation:          jobLocation,
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

			claimedJobDone := make(chan struct{})
			go func() {
				defer close(claimedJobDone)

				slaveWorker.processClaimedUploadJob(ctx, claim)
			}()

			response := <-subscribeCh
			require.NoError(t, response.Err)

			var uploadPayload payload
			err = json.Unmarshal(response.Payload, &uploadPayload)
			require.NoError(t, err)

			for _, output := range uploadPayload.Output {
				require.Equal(t, output.TotalRows, 4)
				require.Equal(t, output.StagingFileID, p.StagingFileID)
				require.Equal(t, output.DestinationRevisionID, p.DestinationRevisionID)
				require.Equal(t, output.UseRudderStorage, p.StagingUseRudderStorage)

				fm, err := filemanager.New(&filemanager.Settings{
					Provider: "MINIO",
					Config: misc.GetObjectStorageConfig(misc.ObjectStorageOptsT{
						Provider: "MINIO",
						Config:   destConf,
					}),
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

			p := payload{
				UploadID:                     1,
				StagingFileID:                1,
				StagingFileLocation:          jobLocation,
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
				DestinationConfig:            destConf,
				StagingDestinationConfig:     map[string]interface{}{},
				UniqueLoadGenID:              uuid.New().String(),
				RudderStoragePrefix:          misc.GetRudderObjectStoragePrefix(),
				LoadFileType:                 "csv",
			}

			payloadJson, err := json.Marshal(p)
			require.NoError(t, err)

			claimJob := &notifier.ClaimJob{
				Job: &notifier.Job{
					ID:                  1,
					BatchID:             uuid.New().String(),
					Payload:             payloadJson,
					Status:              model.Waiting,
					WorkspaceIdentifier: "test_workspace",
					Type:                notifier.JobTypeUpload,
				},
			}

			claimedJobDone := make(chan struct{})
			go func() {
				defer close(claimedJobDone)

				slaveWorker.processClaimedUploadJob(ctx, claimJob)
			}()

			response := <-subscribeCh
			require.EqualError(t, response.Err, "staging file schema limit exceeded for stagingFileID: 1, actualCount: 21")

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

			p := payload{
				UploadID:            1,
				StagingFileID:       1,
				StagingFileLocation: jobLocation,
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

			claimedJobDone := make(chan struct{})
			go func() {
				defer close(claimedJobDone)

				slaveWorker.processClaimedUploadJob(ctx, claim)
			}()

			response := <-subscribeCh
			require.NoError(t, response.Err)

			var uploadPayload payload
			err = json.Unmarshal(response.Payload, &uploadPayload)
			require.NoError(t, err)
			require.Len(t, uploadPayload.Output, 9)

			discardsOutput, ok := lo.Find(uploadPayload.Output, func(o uploadResult) bool {
				return o.TableName == warehouseutils.DiscardsTable
			})
			require.True(t, ok)
			require.Equal(t, discardsOutput.TotalRows, 24)
			require.Equal(t, discardsOutput.StagingFileID, p.StagingFileID)
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

			payloadJson, err := json.Marshal(p)
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
			err = json.Unmarshal(response.Payload, &notifierResponse)
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
				tc := tc

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

					payloadJson, err := json.Marshal(p)
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
	inputs := []struct {
		name             string
		existingDatatype string
		currentDataType  string
		value            any

		newColumnVal any
		convError    error
	}{
		{
			name:             "should send int values if existing datatype is int, new datatype is float",
			existingDatatype: "int",
			currentDataType:  "float",
			value:            1.501,
			newColumnVal:     1,
		},
		{
			name:             "should send float values if existing datatype is float, new datatype is int",
			existingDatatype: "float",
			currentDataType:  "int",
			value:            1,
			newColumnVal:     1.0,
		},
		{
			name:             "should send string values if existing datatype is string, new datatype is boolean",
			existingDatatype: "string",
			currentDataType:  "boolean",
			value:            false,
			newColumnVal:     "false",
		},
		{
			name:             "should send string values if existing datatype is string, new datatype is int",
			existingDatatype: "string",
			currentDataType:  "int",
			value:            1,
			newColumnVal:     "1",
		},
		{
			name:             "should send string values if existing datatype is string, new datatype is float",
			existingDatatype: "string",
			currentDataType:  "float",
			value:            1.501,
			newColumnVal:     "1.501",
		},
		{
			name:             "should send string values if existing datatype is string, new datatype is datetime",
			existingDatatype: "string",
			currentDataType:  "datetime",
			value:            "2022-05-05T00:00:00.000Z",
			newColumnVal:     "2022-05-05T00:00:00.000Z",
		},
		{
			name:             "should send string values if existing datatype is string, new datatype is string",
			existingDatatype: "string",
			currentDataType:  "json",
			value:            `{"json":true}`,
			newColumnVal:     `{"json":true}`,
		},
		{
			name:             "should send json string values if existing datatype is json, new datatype is boolean",
			existingDatatype: "json",
			currentDataType:  "boolean",
			value:            false,
			newColumnVal:     "false",
		},
		{
			name:             "should send json string values if existing datatype is jso, new datatype is int",
			existingDatatype: "json",
			currentDataType:  "int",
			value:            1,
			newColumnVal:     "1",
		},
		{
			name:             "should send json string values if existing datatype is json, new datatype is float",
			existingDatatype: "json",
			currentDataType:  "float",
			value:            1.501,
			newColumnVal:     "1.501",
		},
		{
			name:             "should send json string values if existing datatype is json, new datatype is json",
			existingDatatype: "json",
			currentDataType:  "datetime",
			value:            "2022-05-05T00:00:00.000Z",
			newColumnVal:     `"2022-05-05T00:00:00.000Z"`,
		},
		{
			name:             "should send json string values if existing datatype is json, new datatype is string",
			existingDatatype: "json",
			currentDataType:  "string",
			value:            "string value",
			newColumnVal:     `"string value"`,
		},
		{
			name:             "should send json string values if existing datatype is json, new datatype is array",
			existingDatatype: "json",
			currentDataType:  "array",
			value:            []any{false, 1, "string value"},
			newColumnVal:     []any{false, 1, "string value"},
		},
		{
			name:             "existing datatype is boolean, new datatype is int",
			existingDatatype: "boolean",
			currentDataType:  "int",
			value:            1,
			convError:        errors.New("incompatible schema conversion from boolean to int"),
		},
		{
			name:             "existing datatype is boolean, new datatype is float",
			existingDatatype: "boolean",
			currentDataType:  "float",
			value:            1.501,
			convError:        errors.New("incompatible schema conversion from boolean to float"),
		},
		{
			name:             "existing datatype is boolean, new datatype is string",
			existingDatatype: "boolean",
			currentDataType:  "string",
			value:            "string value",
			convError:        errors.New("incompatible schema conversion from boolean to string"),
		},
		{
			name:             "existing datatype is boolean, new datatype is datetime",
			existingDatatype: "boolean",
			currentDataType:  "datetime",
			value:            "2022-05-05T00:00:00.000Z",
			convError:        errors.New("incompatible schema conversion from boolean to datetime"),
		},
		{
			name:             "existing datatype is boolean, new datatype is json",
			existingDatatype: "boolean",
			currentDataType:  "json",
			value:            `{"json":true}`,
			convError:        errors.New("incompatible schema conversion from boolean to json"),
		},
		{
			name:             "existing datatype is int, new datatype is boolean",
			existingDatatype: "int",
			currentDataType:  "boolean",
			value:            false,
			convError:        errors.New("incompatible schema conversion from int to boolean"),
		},
		{
			name:             "existing datatype is int, new datatype is string",
			existingDatatype: "int",
			currentDataType:  "string",
			value:            "string value",
			convError:        errors.New("incompatible schema conversion from int to string"),
		},
		{
			name:             "existing datatype is int, new datatype is datetime",
			existingDatatype: "int",
			currentDataType:  "datetime",
			value:            "2022-05-05T00:00:00.000Z",
			convError:        errors.New("incompatible schema conversion from int to datetime"),
		},
		{
			name:             "existing datatype is int, new datatype is json",
			existingDatatype: "int",
			currentDataType:  "json",
			value:            `{"json":true}`,
			convError:        errors.New("incompatible schema conversion from int to json"),
		},
		{
			name:             "existing datatype is int, new datatype is float",
			existingDatatype: "int",
			currentDataType:  "float",
			value:            1,
			convError:        errors.New("incompatible schema conversion from int to float"),
		},
		{
			name:             "existing datatype is float, new datatype is boolean",
			existingDatatype: "float",
			currentDataType:  "boolean",
			value:            false,
			convError:        errors.New("incompatible schema conversion from float to boolean"),
		},
		{
			name:             "existing datatype is float, new datatype is int",
			existingDatatype: "float",
			currentDataType:  "int",
			value:            1.0,
			convError:        errors.New("incompatible schema conversion from float to int"),
		},
		{
			name:             "existing datatype is float, new datatype is string",
			existingDatatype: "float",
			currentDataType:  "string",
			value:            "string value",
			convError:        errors.New("incompatible schema conversion from float to string"),
		},
		{
			name:             "existing datatype is float, new datatype is datetime",
			existingDatatype: "float",
			currentDataType:  "datetime",
			value:            "2022-05-05T00:00:00.000Z",
			convError:        errors.New("incompatible schema conversion from float to datetime"),
		},
		{
			name:             "existing datatype is float, new datatype is json",
			existingDatatype: "float",
			currentDataType:  "json",
			value:            `{"json":true}`,
			convError:        errors.New("incompatible schema conversion from float to json"),
		},
		{
			name:             "existing datatype is datetime, new datatype is boolean",
			existingDatatype: "datetime",
			currentDataType:  "boolean",
			value:            false,
			convError:        errors.New("incompatible schema conversion from datetime to boolean"),
		},
		{
			name:             "existing datatype is datetime, new datatype is string",
			existingDatatype: "datetime",
			currentDataType:  "string",
			value:            "string value",
			convError:        errors.New("incompatible schema conversion from datetime to string"),
		},
		{
			name:             "existing datatype is datetime, new datatype is int",
			existingDatatype: "datetime",
			currentDataType:  "int",
			value:            1,
			convError:        errors.New("incompatible schema conversion from datetime to int"),
		},
		{
			name:             "existing datatype is datetime, new datatype is float",
			existingDatatype: "datetime",
			currentDataType:  "float",
			value:            1.501,
			convError:        errors.New("incompatible schema conversion from datetime to float"),
		},
		{
			name:             "existing datatype is datetime, new datatype is json",
			existingDatatype: "datetime",
			currentDataType:  "json",
			value:            `{"json":true}`,
			convError:        errors.New("incompatible schema conversion from datetime to json"),
		},
	}
	for _, ip := range inputs {
		tc := ip

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			newColumnVal, convError := HandleSchemaChange(
				tc.existingDatatype,
				tc.currentDataType,
				tc.value,
			)
			if convError != nil {
				require.Nil(t, newColumnVal)
				require.EqualError(t, convError, tc.convError.Error())
				return
			}
			require.Equal(t, newColumnVal, tc.newColumnVal)
			require.NoError(t, convError)
		})
	}
}
