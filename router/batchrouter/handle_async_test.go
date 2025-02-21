package batchrouter

import (
	"context"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	mocksJobsDB "github.com/rudderlabs/rudder-server/mocks/jobsdb"
	mockdestinationdebugger "github.com/rudderlabs/rudder-server/mocks/services/debugger/destination"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	routerutils "github.com/rudderlabs/rudder-server/router/utils"
	destinationdebugger "github.com/rudderlabs/rudder-server/services/debugger/destination"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

type mockAsyncDestinationManager struct {
	uploadOutput common.AsyncUploadOutput
	pollOutput   common.PollStatusResponse
	statsOutput  common.GetUploadStatsResponse
}

func (m mockAsyncDestinationManager) Transform(job *jobsdb.JobT) (string, error) {
	return common.GetMarshalledData(string(job.EventPayload), job.JobID)
}

func (m mockAsyncDestinationManager) Upload(*common.AsyncDestinationStruct) common.AsyncUploadOutput {
	return m.uploadOutput
}

func (m mockAsyncDestinationManager) Poll(common.AsyncPoll) common.PollStatusResponse {
	return m.pollOutput
}

func (m mockAsyncDestinationManager) GetUploadStats(common.GetUploadStatsInput) common.GetUploadStatsResponse {
	return m.statsOutput
}

func defaultHandle(destType string) *Handle {
	batchRouter := &Handle{}
	batchRouter.destType = destType
	batchRouter.destinationsMap = make(map[string]*routerutils.DestinationWithSources)
	batchRouter.uploadIntervalMap = make(map[string]time.Duration)
	batchRouter.asyncDestinationStruct = make(map[string]*common.AsyncDestinationStruct)
	batchRouter.setupReloadableVars()
	batchRouter.logger = logger.NOP
	batchRouter.conf = config.Default
	batchRouter.adaptiveLimit = func(i int64) int64 {
		return i
	}
	batchRouter.now = func() time.Time {
		return time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	}
	return batchRouter
}

func TestAsyncDestinationManager(t *testing.T) {
	misc.Init()

	config.Reset()
	defer config.Reset()

	config.Set("BatchRouter.isolationMode", "none")
	config.Set("BatchRouter.asyncUploadWorkerTimeout", "10ms")
	config.Set("BatchRouter.pollStatusLoopSleep", "10ms")
	config.Set("BatchRouter.mainLoopFreq", "10ms")
	config.Set("BatchRouter.maxEventsInABatch", 1)
	config.Set("BatchRouter.maxPayloadSizeInBytes", 1*bytesize.KB)
	config.Set("Router.jobRetention", "175200h")

	destType := "MARKETO_BULK_UPLOAD"

	sources := []backendconfig.SourceT{
		{
			ID:      "sourceID",
			Enabled: true,
			Destinations: []backendconfig.DestinationT{
				{
					ID:   "destinationID",
					Name: "Marketo Bulk Upload",
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						Name:        destType,
						DisplayName: "Marketo Bulk Upload",
					},
					Enabled:            true,
					IsProcessorEnabled: true,
				},
			},
			WorkspaceID: "workspaceID",
		},
	}

	t.Run("Upload", func(t *testing.T) {
		t.Run("All Status", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			mockCtrl := gomock.NewController(t)
			mockBatchRouterJobsDB := mocksJobsDB.NewMockJobsDB(mockCtrl)
			mockDestinationDebugger := mockdestinationdebugger.NewMockDestinationDebugger(mockCtrl)

			batchRouter := defaultHandle(destType)
			batchRouter.jobsDB = mockBatchRouterJobsDB
			batchRouter.debugger = mockDestinationDebugger

			mockBatchRouterJobsDB.EXPECT().GetImporting(
				gomock.Any(),
				gomock.Any(),
			).DoAndReturn(
				func(ctx context.Context, params jobsdb.GetQueryParams) (jobsdb.JobsResult, error) {
					return jobsdb.JobsResult{}, nil
				},
			).AnyTimes()
			mockBatchRouterJobsDB.EXPECT().UpdateJobStatusInTx(
				gomock.Any(), gomock.Any(), gomock.Any(), []string{destType}, gomock.Any(),
			).Times(1).Do(func(ctx context.Context, _ interface{}, statuses []*jobsdb.JobStatusT, _, _ interface{}) {
				require.Len(t, statuses, 4)

				require.Equal(t, int64(1), statuses[0].JobID)
				require.Equal(t, jobsdb.Importing.State, statuses[0].JobState)
				require.Equal(t, "200", statuses[0].ErrorCode)
				require.Empty(t, gjson.GetBytes(statuses[0].ErrorResponse, "error").String())
				require.JSONEq(t, `{"importID": "importID"}`, string(statuses[0].Parameters))
				require.Nil(t, statuses[0].JobParameters)

				require.Equal(t, int64(2), statuses[1].JobID)
				require.Equal(t, jobsdb.Succeeded.State, statuses[1].JobState)
				require.Equal(t, "200", statuses[1].ErrorCode)
				require.Empty(t, gjson.GetBytes(statuses[1].ErrorResponse, "error").String())
				require.JSONEq(t, `{}`, string(statuses[1].Parameters))
				require.Nil(t, statuses[1].JobParameters)

				require.Equal(t, int64(3), statuses[2].JobID)
				require.Equal(t, jobsdb.Failed.State, statuses[2].JobState)
				require.Equal(t, "500", statuses[2].ErrorCode)
				require.Equal(t, "mocked failed reason", gjson.GetBytes(statuses[2].ErrorResponse, "error").String())
				require.JSONEq(t, `{}`, string(statuses[2].Parameters))
				require.Nil(t, statuses[2].JobParameters)

				require.Equal(t, int64(4), statuses[3].JobID)
				require.Equal(t, jobsdb.Aborted.State, statuses[3].JobState)
				require.Equal(t, "400", statuses[3].ErrorCode)
				require.Equal(t, "mocked abort reason", gjson.GetBytes(statuses[3].ErrorResponse, "error").String())
				require.JSONEq(t, `{}`, string(statuses[3].Parameters))
				require.Nil(t, statuses[3].JobParameters)

				cancel()
			}).Return(nil)
			mockBatchRouterJobsDB.EXPECT().WithUpdateSafeTx(gomock.Any(), gomock.Any()).Times(1).Do(func(ctx context.Context, f func(tx jobsdb.UpdateSafeTx) error) {
				_ = f(jobsdb.EmptyUpdateSafeTx())
			}).Return(nil)
			mockDestinationDebugger.EXPECT().RecordEventDeliveryStatus(gomock.Any(), gomock.Any()).Times(1).Do(func(destinationID string, deliveryStatus *destinationdebugger.DeliveryStatusT) {
				require.Equal(t, "3 events", deliveryStatus.EventName)
				require.Empty(t, deliveryStatus.EventType)
				require.JSONEq(t, `{"success":"1 events","failed":"2 events"}`, string(deliveryStatus.Payload))
				require.Equal(t, 1, deliveryStatus.AttemptNum)
				require.Equal(t, jobsdb.Failed.State, deliveryStatus.JobState)
				require.Equal(t, "500", deliveryStatus.ErrorCode)
				require.NotEmpty(t, deliveryStatus.ErrorResponse)
			}).Return(true)

			for _, source := range sources {
				for _, destination := range source.Destinations {
					batchRouter.destinationsMap[destination.ID] = &routerutils.DestinationWithSources{
						Destination: destination,
						Sources:     []backendconfig.SourceT{source},
					}

					batchRouter.asyncDestinationStruct[destination.ID] = &common.AsyncDestinationStruct{}
					batchRouter.asyncDestinationStruct[destination.ID].Destination = &destination
					batchRouter.asyncDestinationStruct[destination.ID].Exists = true
					batchRouter.asyncDestinationStruct[destination.ID].FileName = "testdata/uploadData.txt"
					batchRouter.asyncDestinationStruct[destination.ID].Manager = &mockAsyncDestinationManager{
						uploadOutput: common.AsyncUploadOutput{
							ImportingJobIDs:     []int64{1},
							ImportingParameters: []byte(`{"importID": "importID"}`),
							SucceededJobIDs:     []int64{2},
							SuccessResponse:     "mocked success response",
							FailedJobIDs:        []int64{3},
							FailedReason:        "mocked failed reason",
							AbortJobIDs:         []int64{4},
							AbortReason:         "mocked abort reason",
							ImportingCount:      1,
							FailedCount:         1,
							AbortCount:          1,
							DestinationID:       destination.ID,
						},
					}
				}
			}

			done := make(chan struct{})
			go func() {
				defer close(done)
				batchRouter.asyncUploadWorker(ctx)
			}()
			<-done
		})
		t.Run("Importing Status", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			mockCtrl := gomock.NewController(t)
			mockBatchRouterJobsDB := mocksJobsDB.NewMockJobsDB(mockCtrl)
			mockDestinationDebugger := mockdestinationdebugger.NewMockDestinationDebugger(mockCtrl)

			batchRouter := defaultHandle(destType)
			batchRouter.jobsDB = mockBatchRouterJobsDB
			batchRouter.debugger = mockDestinationDebugger

			mockBatchRouterJobsDB.EXPECT().GetImporting(
				gomock.Any(),
				gomock.Any(),
			).DoAndReturn(
				func(ctx context.Context, params jobsdb.GetQueryParams) (jobsdb.JobsResult, error) {
					return jobsdb.JobsResult{}, nil
				},
			).AnyTimes()
			mockBatchRouterJobsDB.EXPECT().UpdateJobStatusInTx(
				gomock.Any(), gomock.Any(), gomock.Any(), []string{destType}, gomock.Any(),
			).Times(1).Do(func(ctx context.Context, _ interface{}, statuses []*jobsdb.JobStatusT, _, _ interface{}) {
				require.Len(t, statuses, 4)

				for i := 0; i < 4; i++ {
					require.Equal(t, int64(i+1), statuses[i].JobID)
					require.Equal(t, jobsdb.Importing.State, statuses[i].JobState)
					require.Equal(t, "200", statuses[i].ErrorCode)
					require.Empty(t, gjson.GetBytes(statuses[i].ErrorResponse, "error").String())
					require.JSONEq(t, `{"importID": "importID"}`, string(statuses[i].Parameters))
					require.Nil(t, statuses[i].JobParameters)
				}

				cancel()
			}).Return(nil)
			mockBatchRouterJobsDB.EXPECT().WithUpdateSafeTx(gomock.Any(), gomock.Any()).Times(1).Do(func(ctx context.Context, f func(tx jobsdb.UpdateSafeTx) error) {
				_ = f(jobsdb.EmptyUpdateSafeTx())
			}).Return(nil)
			mockDestinationDebugger.EXPECT().RecordEventDeliveryStatus(gomock.Any(), gomock.Any()).Times(0)

			for _, source := range sources {
				for _, destination := range source.Destinations {
					batchRouter.destinationsMap[destination.ID] = &routerutils.DestinationWithSources{
						Destination: destination,
						Sources:     []backendconfig.SourceT{source},
					}

					batchRouter.asyncDestinationStruct[destination.ID] = &common.AsyncDestinationStruct{}
					batchRouter.asyncDestinationStruct[destination.ID].Destination = &destination
					batchRouter.asyncDestinationStruct[destination.ID].Exists = true
					batchRouter.asyncDestinationStruct[destination.ID].FileName = "testdata/uploadData.txt"
					batchRouter.asyncDestinationStruct[destination.ID].Manager = &mockAsyncDestinationManager{
						uploadOutput: common.AsyncUploadOutput{
							ImportingJobIDs:     []int64{1, 2, 3, 4},
							ImportingParameters: []byte(`{"importID": "importID"}`),
							ImportingCount:      4,
							DestinationID:       destination.ID,
						},
					}
				}
			}

			done := make(chan struct{})
			go func() {
				defer close(done)
				batchRouter.asyncUploadWorker(ctx)
			}()
			<-done
		})
	})
	t.Run("Poll (StatusCode=StatusBadRequest)", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Second)
		defer cancel()

		mockCtrl := gomock.NewController(t)
		mockBatchRouterJobsDB := mocksJobsDB.NewMockJobsDB(mockCtrl)
		mockDestinationDebugger := mockdestinationdebugger.NewMockDestinationDebugger(mockCtrl)

		batchRouter := defaultHandle(destType)
		batchRouter.jobsDB = mockBatchRouterJobsDB
		batchRouter.debugger = mockDestinationDebugger

		statsStore, err := memstats.New()
		require.NoError(t, err)

		batchRouter.asyncPollTimeStat = statsStore.NewStat("async_poll_time", stats.TimerType)
		batchRouter.asyncAbortedJobCount = statsStore.NewStat("async_aborted_job_count", stats.CountType)

		mockBatchRouterJobsDB.EXPECT().GetImporting(
			gomock.Any(),
			gomock.Any(),
		).DoAndReturn(
			func(ctx context.Context, params jobsdb.GetQueryParams) (jobsdb.JobsResult, error) {
				jr := jobsdb.JobsResult{}
				jr.Jobs = append(jr.Jobs, &jobsdb.JobT{
					UUID:         uuid.New(),
					UserID:       "u1",
					JobID:        1,
					CreatedAt:    time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
					ExpireAt:     time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
					CustomVal:    destType,
					EventPayload: []byte(`{"key": "value"}`),
					LastJobStatus: jobsdb.JobStatusT{
						AttemptNum:    1,
						ErrorResponse: []byte(`{"firstAttemptedAt": "2024-01-01T00:00:00.000Z"}`),
					},
					Parameters: []byte(`{"importId": "importID", "source_id": "sourceID", "destination_id": "destinationID"}`),
				})
				return jr, nil
			},
		).AnyTimes()
		mockBatchRouterJobsDB.EXPECT().UpdateJobStatusInTx(
			gomock.Any(), gomock.Any(), gomock.Any(), []string{destType}, gomock.Any(),
		).Times(1).Do(func(ctx context.Context, _ interface{}, statuses []*jobsdb.JobStatusT, _, _ interface{}) {
			require.Len(t, statuses, 1)
			require.Equal(t, int64(1), statuses[0].JobID)
			require.Equal(t, jobsdb.Aborted.State, statuses[0].JobState)
			require.Empty(t, statuses[0].ErrorCode)
			require.Equal(t, "", gjson.GetBytes(statuses[0].ErrorResponse, "error").String())
			require.JSONEq(t, `{}`, string(statuses[0].Parameters))
			require.JSONEq(t, `{"importId": "importID", "source_id": "sourceID", "destination_id": "destinationID"}`, string(statuses[0].JobParameters))
			cancel()
		}).Return(nil)
		mockBatchRouterJobsDB.EXPECT().WithUpdateSafeTx(gomock.Any(), gomock.Any()).Times(1).Do(func(ctx context.Context, f func(tx jobsdb.UpdateSafeTx) error) {
			_ = f(jobsdb.EmptyUpdateSafeTx())
		}).Return(nil)
		mockDestinationDebugger.EXPECT().RecordEventDeliveryStatus(gomock.Any(), gomock.Any()).Times(1).Do(func(destinationID string, deliveryStatus *destinationdebugger.DeliveryStatusT) {
			require.Equal(t, "1 events", deliveryStatus.EventName)
			require.Empty(t, deliveryStatus.EventType)
			require.JSONEq(t, `{"success":"0 events","failed":"1 events"}`, string(deliveryStatus.Payload))
			require.Equal(t, 1, deliveryStatus.AttemptNum)
			require.Equal(t, jobsdb.Failed.State, deliveryStatus.JobState)
			require.Equal(t, "500", deliveryStatus.ErrorCode)
			require.NotEmpty(t, deliveryStatus.ErrorResponse)
		}).Return(true)

		for _, source := range sources {
			for _, destination := range source.Destinations {
				batchRouter.destinationsMap[destination.ID] = &routerutils.DestinationWithSources{
					Destination: destination,
					Sources:     []backendconfig.SourceT{source},
				}

				batchRouter.asyncDestinationStruct[destination.ID] = &common.AsyncDestinationStruct{}
				batchRouter.asyncDestinationStruct[destination.ID].Destination = &destination
				batchRouter.asyncDestinationStruct[destination.ID].Exists = true
				batchRouter.asyncDestinationStruct[destination.ID].FileName = "testdata/uploadData.txt"
				batchRouter.asyncDestinationStruct[destination.ID].Manager = &mockAsyncDestinationManager{
					pollOutput: common.PollStatusResponse{
						InProgress: false,
						StatusCode: http.StatusBadRequest,
					},
				}
			}
		}

		done := make(chan struct{})
		go func() {
			defer close(done)
			batchRouter.pollAsyncStatus(ctx)
		}()
		<-done
		require.EqualValues(t, 1, statsStore.Get("async_aborted_job_count", stats.Tags{}).LastValue())
	})
	t.Run("Poll (StatusCode=StatusOK)", func(t *testing.T) {
		t.Run("not HasFailed and HasWarning", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Second)
			defer cancel()

			mockCtrl := gomock.NewController(t)
			mockBatchRouterJobsDB := mocksJobsDB.NewMockJobsDB(mockCtrl)
			mockDestinationDebugger := mockdestinationdebugger.NewMockDestinationDebugger(mockCtrl)

			batchRouter := defaultHandle(destType)
			batchRouter.jobsDB = mockBatchRouterJobsDB
			batchRouter.debugger = mockDestinationDebugger

			statsStore, err := memstats.New()
			require.NoError(t, err)

			batchRouter.asyncPollTimeStat = statsStore.NewStat("async_poll_time", stats.TimerType)
			batchRouter.asyncSuccessfulJobCount = statsStore.NewStat("async_successful_job_count", stats.CountType)

			mockBatchRouterJobsDB.EXPECT().GetImporting(
				gomock.Any(),
				gomock.Any(),
			).DoAndReturn(
				func(ctx context.Context, params jobsdb.GetQueryParams) (jobsdb.JobsResult, error) {
					jr := jobsdb.JobsResult{}
					jr.Jobs = append(jr.Jobs, &jobsdb.JobT{
						UUID:         uuid.New(),
						UserID:       "u1",
						JobID:        1,
						CreatedAt:    time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
						ExpireAt:     time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
						CustomVal:    destType,
						EventPayload: []byte(`{"key": "value"}`),
						LastJobStatus: jobsdb.JobStatusT{
							AttemptNum:    1,
							ErrorResponse: []byte(`{"firstAttemptedAt": "2024-01-01T00:00:00.000Z"}`),
						},
						Parameters: []byte(`{"importId": "importID", "source_id": "sourceID", "destination_id": "destinationID"}`),
					})
					return jr, nil
				},
			).AnyTimes()
			mockBatchRouterJobsDB.EXPECT().UpdateJobStatusInTx(
				gomock.Any(), gomock.Any(), gomock.Any(), []string{destType}, gomock.Any(),
			).Times(1).Do(func(ctx context.Context, _ interface{}, statuses []*jobsdb.JobStatusT, _, _ interface{}) {
				require.Len(t, statuses, 1)
				require.Equal(t, int64(1), statuses[0].JobID)
				require.Equal(t, jobsdb.Succeeded.State, statuses[0].JobState)
				require.Empty(t, statuses[0].ErrorCode)
				require.Empty(t, gjson.GetBytes(statuses[0].ErrorResponse, "error").String())
				require.JSONEq(t, `{}`, string(statuses[0].Parameters))
				require.JSONEq(t, `{"importId": "importID", "source_id": "sourceID", "destination_id": "destinationID"}`, string(statuses[0].JobParameters))
				cancel()
			}).Return(nil)
			mockBatchRouterJobsDB.EXPECT().WithUpdateSafeTx(gomock.Any(), gomock.Any()).Times(1).Do(func(ctx context.Context, f func(tx jobsdb.UpdateSafeTx) error) {
				_ = f(jobsdb.EmptyUpdateSafeTx())
			}).Return(nil)
			mockDestinationDebugger.EXPECT().RecordEventDeliveryStatus(gomock.Any(), gomock.Any()).Times(1).Do(func(destinationID string, deliveryStatus *destinationdebugger.DeliveryStatusT) {
				require.Equal(t, "1 events", deliveryStatus.EventName)
				require.Empty(t, deliveryStatus.EventType)
				require.JSONEq(t, `{"success":"1 events","failed":"0 events"}`, string(deliveryStatus.Payload))
				require.Equal(t, 1, deliveryStatus.AttemptNum)
				require.Equal(t, jobsdb.Succeeded.State, deliveryStatus.JobState)
				require.Equal(t, "200", deliveryStatus.ErrorCode)
				require.JSONEq(t, `{"success":"OK"}`, string(deliveryStatus.ErrorResponse))
			}).Return(true)

			for _, source := range sources {
				for _, destination := range source.Destinations {
					batchRouter.destinationsMap[destination.ID] = &routerutils.DestinationWithSources{
						Destination: destination,
						Sources:     []backendconfig.SourceT{source},
					}

					batchRouter.asyncDestinationStruct[destination.ID] = &common.AsyncDestinationStruct{}
					batchRouter.asyncDestinationStruct[destination.ID].Destination = &destination
					batchRouter.asyncDestinationStruct[destination.ID].Exists = true
					batchRouter.asyncDestinationStruct[destination.ID].FileName = "testdata/uploadData.txt"
					batchRouter.asyncDestinationStruct[destination.ID].Manager = &mockAsyncDestinationManager{
						pollOutput: common.PollStatusResponse{
							InProgress: false,
							StatusCode: http.StatusOK,
							Complete:   true,
							HasFailed:  false,
							HasWarning: false,
						},
					}
				}
			}

			done := make(chan struct{})
			go func() {
				defer close(done)
				batchRouter.pollAsyncStatus(ctx)
			}()
			<-done
			require.EqualValues(t, 1, statsStore.Get("async_successful_job_count", stats.Tags{}).LastValue())
		})
		t.Run("HasFailed and HasWarning", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Second)
			defer cancel()

			mockCtrl := gomock.NewController(t)
			mockBatchRouterJobsDB := mocksJobsDB.NewMockJobsDB(mockCtrl)
			mockErrJobsDB := mocksJobsDB.NewMockJobsDB(mockCtrl)
			mockDestinationDebugger := mockdestinationdebugger.NewMockDestinationDebugger(mockCtrl)

			batchRouter := defaultHandle(destType)
			batchRouter.now = func() time.Time {
				return time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
			}
			batchRouter.jobsDB = mockBatchRouterJobsDB
			batchRouter.errorDB = mockErrJobsDB
			batchRouter.debugger = mockDestinationDebugger

			mockErrJobsDB.EXPECT().Store(
				gomock.Any(), gomock.Any(),
			).Times(1).Do(func(ctx context.Context, jobList []*jobsdb.JobT) {
				require.Len(t, jobList, 1)
				require.Equal(t, int64(4), jobList[0].JobID)
				require.JSONEq(t, `{"importId": "importID", "source_id": "sourceID", "destination_id": "destinationID","reason":"aborted reason"}`, string(jobList[0].Parameters))
			})

			statsStore, err := memstats.New()
			require.NoError(t, err)

			batchRouter.asyncPollTimeStat = statsStore.NewStat("async_poll_time", stats.TimerType)
			batchRouter.asyncFailedJobsTimeStat = statsStore.NewStat("async_failed_job_poll_time", stats.TimerType)
			batchRouter.asyncSuccessfulJobCount = statsStore.NewStat("async_successful_job_count", stats.CountType)
			batchRouter.asyncFailedJobCount = statsStore.NewStat("async_failed_job_count", stats.CountType)
			batchRouter.asyncAbortedJobCount = statsStore.NewStat("async_aborted_job_count", stats.CountType)

			mockBatchRouterJobsDB.EXPECT().GetImporting(
				gomock.Any(),
				gomock.Any(),
			).DoAndReturn(
				func(ctx context.Context, params jobsdb.GetQueryParams) (jobsdb.JobsResult, error) {
					jr := jobsdb.JobsResult{}
					for i := 0; i < 4; i++ {
						jr.Jobs = append(jr.Jobs, &jobsdb.JobT{
							UUID:         uuid.New(),
							UserID:       "u" + strconv.Itoa(i+1),
							JobID:        int64(i + 1),
							CreatedAt:    time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
							ExpireAt:     time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
							CustomVal:    destType,
							EventPayload: []byte(`{"key": "value"}`),
							LastJobStatus: jobsdb.JobStatusT{
								AttemptNum:    1,
								ErrorResponse: []byte(`{"firstAttemptedAt": "2024-01-01T00:00:00.000Z"}`),
							},
							Parameters: []byte(`{"importId": "importID", "source_id": "sourceID", "destination_id": "destinationID"}`),
						})
					}
					return jr, nil
				},
			).AnyTimes()
			mockBatchRouterJobsDB.EXPECT().UpdateJobStatusInTx(
				gomock.Any(), gomock.Any(), gomock.Any(), []string{destType}, gomock.Any(),
			).Times(1).Do(func(ctx context.Context, _ interface{}, statuses []*jobsdb.JobStatusT, _, _ interface{}) {
				require.Len(t, statuses, 4)
				require.Equal(t, int64(1), statuses[0].JobID)
				require.Equal(t, jobsdb.Succeeded.State, statuses[0].JobState)
				require.Equal(t, "200", statuses[0].ErrorCode)
				require.Empty(t, gjson.GetBytes(statuses[0].ErrorResponse, "error").String())
				require.JSONEq(t, `{}`, string(statuses[0].Parameters))
				require.JSONEq(t, `{"importId": "importID", "source_id": "sourceID", "destination_id": "destinationID"}`, string(statuses[0].JobParameters))

				require.Equal(t, int64(2), statuses[1].JobID)
				require.Equal(t, jobsdb.Failed.State, statuses[1].JobState)
				require.Equal(t, "400", statuses[1].ErrorCode)
				require.Equal(t, "failed reason", gjson.GetBytes(statuses[1].ErrorResponse, "Error").String())
				require.JSONEq(t, `{}`, string(statuses[1].Parameters))
				require.JSONEq(t, `{"importId": "importID", "source_id": "sourceID", "destination_id": "destinationID"}`, string(statuses[1].JobParameters))

				require.Equal(t, int64(3), statuses[2].JobID)
				require.Equal(t, jobsdb.Succeeded.State, statuses[2].JobState)
				require.Equal(t, "200", statuses[2].ErrorCode)
				require.Equal(t, "warning reason", gjson.GetBytes(statuses[2].ErrorResponse, "Remarks").String())
				require.JSONEq(t, `{}`, string(statuses[2].Parameters))
				require.JSONEq(t, `{"importId": "importID", "source_id": "sourceID", "destination_id": "destinationID"}`, string(statuses[2].JobParameters))

				require.Equal(t, int64(4), statuses[3].JobID)
				require.Equal(t, jobsdb.Aborted.State, statuses[3].JobState)
				require.Equal(t, "400", statuses[3].ErrorCode)
				require.Equal(t, "aborted reason", gjson.GetBytes(statuses[3].ErrorResponse, "Error").String())
				require.JSONEq(t, `{}`, string(statuses[3].Parameters))
				require.JSONEq(t, `{"importId": "importID", "source_id": "sourceID", "destination_id": "destinationID"}`, string(statuses[3].JobParameters))

				cancel()
			}).Return(nil)
			mockBatchRouterJobsDB.EXPECT().WithUpdateSafeTx(gomock.Any(), gomock.Any()).Times(1).Do(func(ctx context.Context, f func(tx jobsdb.UpdateSafeTx) error) {
				_ = f(jobsdb.EmptyUpdateSafeTx())
			}).Return(nil)
			mockDestinationDebugger.EXPECT().RecordEventDeliveryStatus(gomock.Any(), gomock.Any()).Times(1).Do(func(destinationID string, deliveryStatus *destinationdebugger.DeliveryStatusT) {
				require.Equal(t, "4 events", deliveryStatus.EventName)
				require.Empty(t, deliveryStatus.EventType)
				require.JSONEq(t, `{"success":"2 events","failed":"2 events"}`, string(deliveryStatus.Payload))
				require.Equal(t, 1, deliveryStatus.AttemptNum)
				require.Equal(t, jobsdb.Failed.State, deliveryStatus.JobState)
				require.Equal(t, "500", deliveryStatus.ErrorCode)
				require.NotEmpty(t, deliveryStatus.ErrorResponse)
			}).Return(true)

			for _, source := range sources {
				for _, destination := range source.Destinations {
					batchRouter.destinationsMap[destination.ID] = &routerutils.DestinationWithSources{
						Destination: destination,
						Sources:     []backendconfig.SourceT{source},
					}

					batchRouter.asyncDestinationStruct[destination.ID] = &common.AsyncDestinationStruct{}
					batchRouter.asyncDestinationStruct[destination.ID].Destination = &destination
					batchRouter.asyncDestinationStruct[destination.ID].Exists = true
					batchRouter.asyncDestinationStruct[destination.ID].FileName = "testdata/uploadData.txt"
					batchRouter.asyncDestinationStruct[destination.ID].Manager = &mockAsyncDestinationManager{
						pollOutput: common.PollStatusResponse{
							InProgress: false,
							StatusCode: http.StatusOK,
							Complete:   true,
							HasFailed:  true,
							HasWarning: true,
						},
						statsOutput: common.GetUploadStatsResponse{
							StatusCode: http.StatusOK,
							Metadata: common.EventStatMeta{
								SucceededKeys: []int64{1},
								FailedKeys:    []int64{2},
								WarningKeys:   []int64{3},
								AbortedKeys:   []int64{4},
								FailedReasons: map[int64]string{
									2: "failed reason",
								},
								WarningReasons: map[int64]string{
									3: "warning reason",
								},
								AbortedReasons: map[int64]string{
									4: "aborted reason",
								},
							},
						},
					}
				}
			}

			done := make(chan struct{})
			go func() {
				defer close(done)
				batchRouter.pollAsyncStatus(ctx)
			}()
			<-done
			require.EqualValues(t, 2, statsStore.Get("async_successful_job_count", stats.Tags{}).LastValue())
			require.EqualValues(t, 1, statsStore.Get("async_failed_job_count", stats.Tags{}).LastValue())
			require.EqualValues(t, 1, statsStore.Get("async_aborted_job_count", stats.Tags{}).LastValue())
		})
	})
	t.Run("Poll (StatusCode not StatusOK and StatusBadRequest)", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Second)
		defer cancel()

		mockCtrl := gomock.NewController(t)
		mockBatchRouterJobsDB := mocksJobsDB.NewMockJobsDB(mockCtrl)
		mockDestinationDebugger := mockdestinationdebugger.NewMockDestinationDebugger(mockCtrl)

		batchRouter := defaultHandle(destType)
		batchRouter.now = func() time.Time {
			return time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		}
		batchRouter.jobsDB = mockBatchRouterJobsDB
		batchRouter.debugger = mockDestinationDebugger

		statsStore, err := memstats.New()
		require.NoError(t, err)

		batchRouter.asyncPollTimeStat = statsStore.NewStat("async_poll_time", stats.TimerType)
		batchRouter.asyncFailedJobCount = statsStore.NewStat("async_failed_job_count", stats.CountType)

		mockBatchRouterJobsDB.EXPECT().GetImporting(
			gomock.Any(),
			gomock.Any(),
		).DoAndReturn(
			func(ctx context.Context, params jobsdb.GetQueryParams) (jobsdb.JobsResult, error) {
				jr := jobsdb.JobsResult{}
				jr.Jobs = append(jr.Jobs, &jobsdb.JobT{
					UUID:         uuid.New(),
					UserID:       "u1",
					JobID:        1,
					CreatedAt:    time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
					ExpireAt:     time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
					CustomVal:    destType,
					EventPayload: []byte(`{"key": "value"}`),
					LastJobStatus: jobsdb.JobStatusT{
						AttemptNum:    1,
						ErrorResponse: []byte(`{"firstAttemptedAt": "2024-01-01T00:00:00.000Z"}`),
					},
					Parameters: []byte(`{"importId": "importID", "source_id": "sourceID", "destination_id": "destinationID"}`),
				})
				return jr, nil
			},
		).AnyTimes()
		mockBatchRouterJobsDB.EXPECT().UpdateJobStatusInTx(
			gomock.Any(), gomock.Any(), gomock.Any(), []string{destType}, gomock.Any(),
		).Times(1).Do(func(ctx context.Context, _ interface{}, statuses []*jobsdb.JobStatusT, _, _ interface{}) {
			require.Len(t, statuses, 1)
			require.Equal(t, int64(1), statuses[0].JobID)
			require.Equal(t, jobsdb.Failed.State, statuses[0].JobState)
			require.Equal(t, "401", statuses[0].ErrorCode)
			require.Equal(t, "failed with status code 401", gjson.GetBytes(statuses[0].ErrorResponse, "error").String())
			require.JSONEq(t, `{}`, string(statuses[0].Parameters))
			require.JSONEq(t, `{"importId": "importID", "source_id": "sourceID", "destination_id": "destinationID"}`, string(statuses[0].JobParameters))
			cancel()
		}).Return(nil)
		mockBatchRouterJobsDB.EXPECT().WithUpdateSafeTx(gomock.Any(), gomock.Any()).Times(1).Do(func(ctx context.Context, f func(tx jobsdb.UpdateSafeTx) error) {
			_ = f(jobsdb.EmptyUpdateSafeTx())
		}).Return(nil)
		mockDestinationDebugger.EXPECT().RecordEventDeliveryStatus(gomock.Any(), gomock.Any()).Times(1).Do(func(destinationID string, deliveryStatus *destinationdebugger.DeliveryStatusT) {
			require.Equal(t, "1 events", deliveryStatus.EventName)
			require.Empty(t, deliveryStatus.EventType)
			require.JSONEq(t, `{"success":"0 events","failed":"1 events"}`, string(deliveryStatus.Payload))
			require.Equal(t, 1, deliveryStatus.AttemptNum)
			require.Equal(t, jobsdb.Failed.State, deliveryStatus.JobState)
			require.Equal(t, "500", deliveryStatus.ErrorCode)
			require.NotEmpty(t, deliveryStatus.ErrorResponse)
		}).Return(true)

		for _, source := range sources {
			for _, destination := range source.Destinations {
				batchRouter.destinationsMap[destination.ID] = &routerutils.DestinationWithSources{
					Destination: destination,
					Sources:     []backendconfig.SourceT{source},
				}

				batchRouter.asyncDestinationStruct[destination.ID] = &common.AsyncDestinationStruct{}
				batchRouter.asyncDestinationStruct[destination.ID].Destination = &destination
				batchRouter.asyncDestinationStruct[destination.ID].Exists = true
				batchRouter.asyncDestinationStruct[destination.ID].FileName = "testdata/uploadData.txt"
				batchRouter.asyncDestinationStruct[destination.ID].Manager = &mockAsyncDestinationManager{
					pollOutput: common.PollStatusResponse{
						InProgress: false,
						StatusCode: http.StatusUnauthorized,
						Error:      "failed with status code 401",
					},
				}
			}
		}

		done := make(chan struct{})
		go func() {
			defer close(done)
			batchRouter.pollAsyncStatus(ctx)
		}()
		<-done
		require.EqualValues(t, 1, statsStore.Get("async_failed_job_count", stats.Tags{}).LastValue())
	})
}
