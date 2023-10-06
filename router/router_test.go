package router

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-server/enterprise/reporting"

	jsoniter "github.com/json-iterator/go"

	"github.com/tidwall/sjson"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/admin"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	mocksBackendConfig "github.com/rudderlabs/rudder-server/mocks/backend-config"
	mocksJobsDB "github.com/rudderlabs/rudder-server/mocks/jobsdb"
	mocksRouter "github.com/rudderlabs/rudder-server/mocks/router"
	mocksTransformer "github.com/rudderlabs/rudder-server/mocks/router/transformer"
	"github.com/rudderlabs/rudder-server/router/internal/eventorder"
	"github.com/rudderlabs/rudder-server/router/types"
	routerUtils "github.com/rudderlabs/rudder-server/router/utils"
	destinationdebugger "github.com/rudderlabs/rudder-server/services/debugger/destination"
	"github.com/rudderlabs/rudder-server/services/rsources"
	"github.com/rudderlabs/rudder-server/services/transientsource"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
	testutils "github.com/rudderlabs/rudder-server/utils/tests"
)

const (
	writeKeyEnabled           = "enabled-write-key"
	sourceIDEnabled           = "enabled-source"
	gaDestinationDefinitionID = "gaid1"
	gaDestinationID           = "did1"
	nonexistentDestinationID  = "non-existent-destination-id"
)

var (
	testTimeout             = 10 * time.Second
	customVal               = map[string]string{"GA": "GA"}
	workspaceID             = uuid.New().String()
	gaDestinationDefinition = backendconfig.DestinationDefinitionT{
		ID:          gaDestinationDefinitionID,
		Name:        "GA",
		DisplayName: "Google Analytics",
	}
	collectMetricsErrorMap = map[string]int{
		"Error Response 1":  1,
		"Error Response 2":  2,
		"Error Response 3":  3,
		"Error Response 4":  4,
		"Error Response 5":  1,
		"Error Response 6":  2,
		"Error Response 7":  3,
		"Error Response 8":  4,
		"Error Response 9":  1,
		"Error Response 10": 2,
		"Error Response 11": 3,
		"Error Response 12": 4,
		"Error Response 13": 1,
		"Error Response 14": 2,
		"Error Response 15": 3,
		"Error Response 16": 4,
	}
	// This configuration is assumed by all router tests and, is returned on Subscribe of mocked backend config
	sampleBackendConfig = backendconfig.ConfigT{
		WorkspaceID: workspaceID,
		Sources: []backendconfig.SourceT{
			{
				WorkspaceID: workspaceID,
				ID:          sourceIDEnabled,
				WriteKey:    writeKeyEnabled,
				Enabled:     true,
				Destinations: []backendconfig.DestinationT{
					{
						ID:                    gaDestinationID,
						Name:                  "ga dest",
						DestinationDefinition: gaDestinationDefinition,
						Enabled:               true,
						IsProcessorEnabled:    true,
					},
				},
			},
		},
	}
)

type testContext struct {
	asyncHelper     testutils.AsyncTestHelper
	dbReadBatchSize int

	mockCtrl          *gomock.Controller
	mockRouterJobsDB  *mocksJobsDB.MockJobsDB
	mockProcErrorsDB  *mocksJobsDB.MockJobsDB
	mockBackendConfig *mocksBackendConfig.MockBackendConfig
}

// Initialize mocks and common expectations
func (c *testContext) Setup() {
	c.asyncHelper.Setup()
	c.mockCtrl = gomock.NewController(GinkgoT())
	c.mockRouterJobsDB = mocksJobsDB.NewMockJobsDB(c.mockCtrl)
	c.mockProcErrorsDB = mocksJobsDB.NewMockJobsDB(c.mockCtrl)
	c.mockBackendConfig = mocksBackendConfig.NewMockBackendConfig(c.mockCtrl)

	tFunc := c.asyncHelper.ExpectAndNotifyCallbackWithName("backend_config")

	// During Setup, router subscribes to backend config
	c.mockBackendConfig.EXPECT().Subscribe(gomock.Any(), backendconfig.TopicBackendConfig).
		DoAndReturn(func(ctx context.Context, topic backendconfig.Topic) pubsub.DataChannel {
			tFunc()
			ch := make(chan pubsub.DataEvent, 1)
			ch <- pubsub.DataEvent{Data: map[string]backendconfig.ConfigT{workspaceID: sampleBackendConfig}, Topic: string(topic)}
			// on Subscribe, emulate a backend configuration event
			go func() {
				<-ctx.Done()
				close(ch)
			}()
			return ch
		})
	c.dbReadBatchSize = 10000
}

func (c *testContext) Finish() {
	c.asyncHelper.WaitWithTimeout(testTimeout)
	c.mockCtrl.Finish()
}

func initRouter() {
	config.Reset()
	admin.Init()
	logger.Reset()
	misc.Init()
}

func TestBackoff(t *testing.T) {
	t.Run("nextAttemptAfter", func(t *testing.T) {
		min := 10 * time.Second
		max := 300 * time.Second
		require.Equal(t, 10*time.Second, nextAttemptAfter(0, min, max))
		require.Equal(t, 10*time.Second, nextAttemptAfter(1, min, max))
		require.Equal(t, 20*time.Second, nextAttemptAfter(2, min, max))
		require.Equal(t, 40*time.Second, nextAttemptAfter(3, min, max))
		require.Equal(t, 80*time.Second, nextAttemptAfter(4, min, max))
		require.Equal(t, 160*time.Second, nextAttemptAfter(5, min, max))
		require.Equal(t, 300*time.Second, nextAttemptAfter(6, min, max))
	})

	t.Run("findWorker", func(t *testing.T) {
		backoffJob := &jobsdb.JobT{
			JobID:      1,
			Parameters: []byte(`{"destination_id": "destination"}`),
			LastJobStatus: jobsdb.JobStatusT{
				JobState:   jobsdb.Failed.State,
				AttemptNum: 1,
				RetryTime:  time.Now().Add(1 * time.Hour),
			},
		}
		noBackoffJob1 := &jobsdb.JobT{
			JobID:      2,
			Parameters: []byte(`{"destination_id": "destination"}`),
			LastJobStatus: jobsdb.JobStatusT{
				JobState:   jobsdb.Waiting.State,
				AttemptNum: 1,
				RetryTime:  time.Now().Add(1 * time.Hour),
			},
		}
		noBackoffJob2 := &jobsdb.JobT{
			JobID:      3,
			Parameters: []byte(`{"destination_id": "destination"}`),
			LastJobStatus: jobsdb.JobStatusT{
				JobState:   jobsdb.Failed.State,
				AttemptNum: 0,
				RetryTime:  time.Now().Add(1 * time.Hour),
			},
		}
		noBackoffJob3 := &jobsdb.JobT{
			JobID:      4,
			Parameters: []byte(`{"destination_id": "destination"}`),
			LastJobStatus: jobsdb.JobStatusT{
				JobState:   jobsdb.Failed.State,
				AttemptNum: 0,
				RetryTime:  time.Now().Add(-1 * time.Hour),
			},
		}
		noBackoffJob4 := &jobsdb.JobT{
			JobID:      5,
			Parameters: []byte(`{"destination_id": "destination"}`),
			LastJobStatus: jobsdb.JobStatusT{
				JobState:   jobsdb.Failed.State,
				AttemptNum: 0,
				RetryTime:  time.Now().Add(-1 * time.Hour),
			},
		}

		r := &Handle{
			logger:                logger.NOP,
			backgroundCtx:         context.Background(),
			noOfWorkers:           1,
			workerInputBufferSize: 3,
		}
		workers := []*worker{{
			logger:  logger.NOP,
			input:   make(chan workerJob, 3),
			barrier: eventorder.NewBarrier(),
		}}
		t.Run("eventorder disabled", func(t *testing.T) {
			r.guaranteeUserEventOrder = false
			workers[0].inputReservations = 0

			slot, err := r.findWorkerSlot(workers, backoffJob, map[string]struct{}{})
			require.Nil(t, slot)
			require.ErrorIs(t, err, types.ErrJobBackoff)

			slot, err = r.findWorkerSlot(workers, noBackoffJob1, map[string]struct{}{})
			require.NotNil(t, slot)
			require.NoError(t, err)

			slot, err = r.findWorkerSlot(workers, noBackoffJob2, map[string]struct{}{})
			require.NotNil(t, slot)
			require.NoError(t, err)

			slot, err = r.findWorkerSlot(workers, noBackoffJob3, map[string]struct{}{})
			require.NotNil(t, slot)
			require.NoError(t, err)

			slot, err = r.findWorkerSlot(workers, noBackoffJob4, map[string]struct{}{})
			require.Nil(t, slot)
			require.ErrorIs(t, err, types.ErrWorkerNoSlot)
		})

		t.Run("eventorder enabled", func(t *testing.T) {
			r.guaranteeUserEventOrder = true
			workers[0].inputReservations = 0

			slot, err := r.findWorkerSlot(workers, backoffJob, map[string]struct{}{})
			require.Nil(t, slot)
			require.ErrorIs(t, err, types.ErrJobBackoff)

			slot, err = r.findWorkerSlot(workers, noBackoffJob1, map[string]struct{}{})
			require.NotNil(t, slot)
			require.NoError(t, err)

			slot, err = r.findWorkerSlot(workers, noBackoffJob2, map[string]struct{}{})
			require.NotNil(t, slot)
			require.NoError(t, err)

			slot, err = r.findWorkerSlot(workers, noBackoffJob3, map[string]struct{}{})
			require.NotNil(t, slot)
			require.NoError(t, err)

			slot, err = r.findWorkerSlot(workers, noBackoffJob4, map[string]struct{}{})
			require.Nil(t, slot)
			require.ErrorIs(t, err, types.ErrWorkerNoSlot)
		})

		t.Run("context canceled", func(t *testing.T) {
			defer func() { r.backgroundCtx = context.Background() }()
			r.backgroundCtx, r.backgroundCancel = context.WithCancel(context.Background())
			r.backgroundCancel()
			slot, err := r.findWorkerSlot(workers, backoffJob, map[string]struct{}{})
			require.Nil(t, slot)
			require.ErrorIs(t, err, types.ErrContextCancelled)
		})

		t.Run("unmarshalling params failure", func(t *testing.T) {
			invalidJob := &jobsdb.JobT{
				JobID:      1,
				Parameters: []byte(`{"destination_id": "destination"`),
				LastJobStatus: jobsdb.JobStatusT{
					JobState:   jobsdb.Failed.State,
					AttemptNum: 1,
					RetryTime:  time.Now().Add(1 * time.Hour),
				},
			}
			slot, err := r.findWorkerSlot(workers, invalidJob, map[string]struct{}{})
			require.Nil(t, slot)
			require.ErrorIs(t, err, types.ErrParamsUnmarshal)
		})

		t.Run("blocked job", func(t *testing.T) {
			job := &jobsdb.JobT{
				JobID:      1,
				Parameters: []byte(`{"destination_id": "destination"}`),
				LastJobStatus: jobsdb.JobStatusT{
					JobState:   jobsdb.Failed.State,
					AttemptNum: 1,
					RetryTime:  time.Now().Add(1 * time.Hour),
				},
			}
			slot, err := r.findWorkerSlot(workers, backoffJob, map[string]struct{}{jobOrderKey(job.UserID, "destination"): {}})
			require.Nil(t, slot)
			require.ErrorIs(t, err, types.ErrJobOrderBlocked)
		})
	})
}

var _ = Describe("router", func() {
	initRouter()

	var c *testContext
	var conf *config.Config

	BeforeEach(func() {
		conf = config.New()
		config.Reset()
		config.Set("Router.jobRetention", "175200h") // 20 Years(20*365*24)
		c = &testContext{}
		c.Setup()
	})

	AfterEach(func() {
		config.Reset()
		c.Finish()
	})

	Context("initialization", func() {
		It("should initialize and recover after crash", func() {
			router := &Handle{
				Reporting: &reporting.NOOP{},
			}
			c.mockBackendConfig.EXPECT().AccessToken().AnyTimes()
			router.Setup(gaDestinationDefinition, logger.NOP, conf, c.mockBackendConfig, c.mockRouterJobsDB, c.mockProcErrorsDB, transientsource.NewEmptyService(), rsources.NewNoOpService(), destinationdebugger.NewNoOpService())
		})
	})

	Context("normal operation", func() {
		BeforeEach(func() {
			conf.Set("Router.maxStatusUpdateWait", "2s")
		})

		It("should send failed and unprocessed jobs to ga destination", func() {
			mockNetHandle := mocksRouter.NewMockNetHandle(c.mockCtrl)
			router := &Handle{
				Reporting: &reporting.NOOP{},
				netHandle: mockNetHandle,
			}
			c.mockBackendConfig.EXPECT().AccessToken().AnyTimes()

			router.Setup(gaDestinationDefinition, logger.NOP, conf, c.mockBackendConfig, c.mockRouterJobsDB, c.mockProcErrorsDB, transientsource.NewEmptyService(), rsources.NewNoOpService(), destinationdebugger.NewNoOpService())

			gaPayload := `{"body": {"XML": {}, "FORM": {}, "JSON": {}}, "type": "REST", "files": {}, "method": "POST", "params": {"t": "event", "v": "1", "an": "RudderAndroidClient", "av": "1.0", "ds": "android-sdk", "ea": "Demo Track", "ec": "Demo Category", "el": "Demo Label", "ni": 0, "qt": 59268380964, "ul": "en-US", "cid": "anon_id", "tid": "UA-185645846-1", "uip": "[::1]", "aiid": "com.rudderlabs.android.sdk"}, "userId": "anon_id", "headers": {}, "version": "1", "endpoint": "https://www.google-analytics.com/collect"}`
			parameters := fmt.Sprintf(`{"source_id": "1fMCVYZboDlYlauh4GFsEo2JU77", "destination_id": "%s", "message_id": "2f548e6d-60f6-44af-a1f4-62b3272445c3", "received_at": "2021-06-28T10:04:48.527+05:30", "transform_at": "processor"}`, gaDestinationID) // skipcq: GO-R4002
			toRetryJobsList := []*jobsdb.JobT{
				{
					UUID:         uuid.New(),
					UserID:       "u1",
					JobID:        2009,
					CreatedAt:    time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					ExpireAt:     time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					CustomVal:    customVal["GA"],
					EventPayload: []byte(gaPayload),
					LastJobStatus: jobsdb.JobStatusT{
						AttemptNum:    1,
						ErrorResponse: []byte(`{"firstAttemptedAt": "2021-06-28T15:57:30.742+05:30"}`),
					},
					Parameters:  []byte(parameters),
					WorkspaceId: workspaceID,
				},
			}

			unprocessedJobsList := []*jobsdb.JobT{
				{
					UUID:         uuid.New(),
					UserID:       "u1",
					JobID:        2010,
					CreatedAt:    time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					ExpireAt:     time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					CustomVal:    customVal["GA"],
					EventPayload: []byte(gaPayload),
					LastJobStatus: jobsdb.JobStatusT{
						AttemptNum: 0,
					},
					Parameters:  []byte(parameters),
					WorkspaceId: workspaceID,
				},
			}

			allJobs := append(toRetryJobsList, unprocessedJobsList...)

			payloadLimit := router.reloadableConfig.payloadLimit
			callGetAllJobs := c.mockRouterJobsDB.EXPECT().GetToProcess(gomock.Any(),
				jobsdb.GetQueryParams{
					CustomValFilters: []string{customVal["GA"]},
					ParameterFilters: []jobsdb.ParameterFilterT{{Name: "destination_id", Value: gaDestinationID}},
					PayloadSizeLimit: payloadLimit.Load(),
					JobsLimit:        10000,
				}, nil).Times(1).Return(&jobsdb.MoreJobsResult{JobsResult: jobsdb.JobsResult{Jobs: allJobs}}, nil)

			c.mockRouterJobsDB.EXPECT().UpdateJobStatus(gomock.Any(), gomock.Any(), []string{customVal["GA"]}, nil).Times(1).
				Do(func(ctx context.Context, statuses []*jobsdb.JobStatusT, _, _ interface{}) {
					assertJobStatus(toRetryJobsList[0], statuses[0], jobsdb.Executing.State, "", `{}`, 1)
					assertJobStatus(unprocessedJobsList[0], statuses[1], jobsdb.Executing.State, "", `{}`, 0)
				}).Return(nil).After(callGetAllJobs)

			mockNetHandle.EXPECT().SendPost(gomock.Any(), gomock.Any()).Times(2).Return(
				&routerUtils.SendPostResponse{StatusCode: 200, ResponseBody: []byte("")})
			done := make(chan struct{})

			c.mockRouterJobsDB.EXPECT().WithUpdateSafeTx(gomock.Any(), gomock.Any()).Times(1).Do(func(ctx context.Context, f func(tx jobsdb.UpdateSafeTx) error) {
				_ = f(jobsdb.EmptyUpdateSafeTx())
				close(done)
			}).Return(nil)
			c.mockRouterJobsDB.EXPECT().UpdateJobStatusInTx(gomock.Any(), gomock.Any(), gomock.Any(), []string{customVal["GA"]}, nil).Times(1).
				Do(func(ctx context.Context, _ interface{}, statuses []*jobsdb.JobStatusT, _, _ interface{}) {
					assertJobStatus(toRetryJobsList[0], statuses[0], jobsdb.Succeeded.State, "200", `{"content-type":"","response": "","firstAttemptedAt":"2021-06-28T15:57:30.742+05:30"}`, 2)
					assertJobStatus(unprocessedJobsList[0], statuses[1], jobsdb.Succeeded.State, "200", `{"content-type":"","response": "","firstAttemptedAt":"2021-06-28T15:57:30.742+05:30"}`, 1)
				})

			<-router.backendConfigInitialized
			worker := newPartitionWorker(context.Background(), router, gaDestinationID)
			defer worker.Stop()
			Expect(worker.Work()).To(BeTrue())
			Expect(worker.pickupCount).To(Equal(2))
			Eventually(func() bool {
				select {
				case <-done:
					return true
				default:
					return false
				}
			}, 20*time.Second, 100*time.Millisecond).Should(Equal(true))
		})

		It("should abort unprocessed jobs to ga destination because of bad payload", func() {
			router := &Handle{
				Reporting: &reporting.NOOP{},
			}
			c.mockBackendConfig.EXPECT().AccessToken().AnyTimes()

			router.Setup(gaDestinationDefinition, logger.NOP, conf, c.mockBackendConfig, c.mockRouterJobsDB, c.mockProcErrorsDB, transientsource.NewEmptyService(), rsources.NewNoOpService(), destinationdebugger.NewNoOpService())

			mockNetHandle := mocksRouter.NewMockNetHandle(c.mockCtrl)
			router.netHandle = mockNetHandle

			gaPayload := `{}`
			parameters := fmt.Sprintf(`{"source_id": "1fMCVYZboDlYlauh4GFsEo2JU77", "destination_id": "%s", "message_id": "2f548e6d-60f6-44af-a1f4-62b3272445c3", "received_at": "2021-06-28T10:04:48.527+05:30", "transform_at": "processor"}`, gaDestinationID) // skipcq: GO-R4002

			unprocessedJobsList := []*jobsdb.JobT{
				{
					UUID:         uuid.New(),
					UserID:       "u1",
					JobID:        2010,
					CreatedAt:    time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					ExpireAt:     time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					CustomVal:    customVal["GA"],
					EventPayload: []byte(gaPayload),
					LastJobStatus: jobsdb.JobStatusT{
						AttemptNum: 0,
					},
					Parameters:  []byte(parameters),
					WorkspaceId: workspaceID,
				},
			}

			payloadLimit := router.reloadableConfig.payloadLimit
			callGetAllJobs := c.mockRouterJobsDB.EXPECT().GetToProcess(gomock.Any(), jobsdb.GetQueryParams{
				CustomValFilters: []string{customVal["GA"]},
				ParameterFilters: []jobsdb.ParameterFilterT{{Name: "destination_id", Value: gaDestinationID}},
				PayloadSizeLimit: payloadLimit.Load(),
				JobsLimit:        10000,
			}, nil).Times(1).Return(&jobsdb.MoreJobsResult{JobsResult: jobsdb.JobsResult{Jobs: unprocessedJobsList}}, nil)

			c.mockRouterJobsDB.EXPECT().UpdateJobStatus(gomock.Any(), gomock.Any(), []string{customVal["GA"]}, nil).Times(1).
				Do(func(ctx context.Context, statuses []*jobsdb.JobStatusT, _, _ interface{}) {
					assertJobStatus(unprocessedJobsList[0], statuses[0], jobsdb.Executing.State, "", `{}`, 0)
				}).After(callGetAllJobs)

			mockNetHandle.EXPECT().SendPost(gomock.Any(), gomock.Any()).Times(1).Return(&routerUtils.SendPostResponse{StatusCode: 400, ResponseBody: []byte("")})

			c.mockProcErrorsDB.EXPECT().Store(gomock.Any(), gomock.Any()).Times(1).
				Do(func(ctx context.Context, jobList []*jobsdb.JobT) {
					job := jobList[0]
					var parameters map[string]interface{}
					err := json.Unmarshal(job.Parameters, &parameters)
					if err != nil {
						panic(err)
					}

					Expect(parameters["stage"]).To(Equal("router"))
					Expect(job.JobID).To(Equal(unprocessedJobsList[0].JobID))
					Expect(job.CustomVal).To(Equal(unprocessedJobsList[0].CustomVal))
					Expect(job.UserID).To(Equal(unprocessedJobsList[0].UserID))
				})
			done := make(chan struct{})
			c.mockRouterJobsDB.EXPECT().WithUpdateSafeTx(gomock.Any(), gomock.Any()).Times(1).Do(func(ctx context.Context, f func(tx jobsdb.UpdateSafeTx) error) {
				_ = f(jobsdb.EmptyUpdateSafeTx())
				close(done)
			}).Return(nil)

			c.mockRouterJobsDB.EXPECT().UpdateJobStatusInTx(gomock.Any(), gomock.Any(), gomock.Any(), []string{customVal["GA"]}, nil).Times(1).
				Do(func(ctx context.Context, _ interface{}, statuses []*jobsdb.JobStatusT, _, _ interface{}) {
					assertJobStatus(unprocessedJobsList[0], statuses[0], jobsdb.Aborted.State, "400", `{"content-type":"","response":"","firstAttemptedAt":"2021-06-28T15:57:30.742+05:30"}`, 1)
				})

			<-router.backendConfigInitialized
			worker := newPartitionWorker(context.Background(), router, gaDestinationID)
			defer worker.Stop()
			Expect(worker.Work()).To(BeTrue())
			Expect(worker.pickupCount).To(Equal(1))
			Eventually(func() bool {
				select {
				case <-done:
					return true
				default:
					return false
				}
			}, 20*time.Second, 100*time.Millisecond).Should(Equal(true))
		})

		It("aborts events that are older than a configurable duration", func() {
			config.Set("Router.jobRetention", "24h")
			router := &Handle{
				Reporting: &reporting.NOOP{},
			}
			c.mockBackendConfig.EXPECT().AccessToken().AnyTimes()

			router.Setup(gaDestinationDefinition, logger.NOP, conf, c.mockBackendConfig, c.mockRouterJobsDB, c.mockProcErrorsDB, transientsource.NewEmptyService(), rsources.NewNoOpService(), destinationdebugger.NewNoOpService())
			mockNetHandle := mocksRouter.NewMockNetHandle(c.mockCtrl)
			router.netHandle = mockNetHandle

			gaPayload := `{"body": {"XML": {}, "FORM": {}, "JSON": {}}, "type": "REST", "files": {}, "method": "POST", "params": {"t": "event", "v": "1", "an": "RudderAndroidClient", "av": "1.0", "ds": "android-sdk", "ea": "Demo Track", "ec": "Demo Category", "el": "Demo Label", "ni": 0, "qt": 59268380964, "ul": "en-US", "cid": "anon_id", "tid": "UA-185645846-1", "uip": "[::1]", "aiid": "com.rudderlabs.android.sdk"}, "userId": "anon_id", "headers": {}, "version": "1", "endpoint": "https://www.google-analytics.com/collect"}`
			parameters := fmt.Sprintf(`{"source_id": "1fMCVYZboDlYlauh4GFsEo2JU77", "destination_id": "%s", "message_id": "2f548e6d-60f6-44af-a1f4-62b3272445c3", "received_at": "2021-06-28T10:04:48.527+05:30", "transform_at": "processor"}`, gaDestinationID) // skipcq: GO-R4002

			unprocessedJobsList := []*jobsdb.JobT{
				{
					UUID:         uuid.New(),
					UserID:       "u1",
					JobID:        2010,
					CreatedAt:    time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					ExpireAt:     time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					CustomVal:    customVal["GA"],
					EventPayload: []byte(gaPayload),
					LastJobStatus: jobsdb.JobStatusT{
						AttemptNum: 0,
					},
					Parameters:  []byte(parameters),
					WorkspaceId: workspaceID,
				},
			}

			payloadLimit := router.reloadableConfig.payloadLimit
			c.mockRouterJobsDB.EXPECT().GetToProcess(gomock.Any(), jobsdb.GetQueryParams{
				CustomValFilters: []string{customVal["GA"]},
				ParameterFilters: []jobsdb.ParameterFilterT{{Name: "destination_id", Value: gaDestinationID}},
				PayloadSizeLimit: payloadLimit.Load(),
				JobsLimit:        10000,
			}, nil).Times(1).Return(&jobsdb.MoreJobsResult{JobsResult: jobsdb.JobsResult{Jobs: unprocessedJobsList}}, nil)

			var routerAborted bool
			var procErrorStored bool

			c.mockRouterJobsDB.EXPECT().UpdateJobStatus(gomock.Any(), gomock.Any(), []string{customVal["GA"]}, nil).Times(1)

			c.mockProcErrorsDB.EXPECT().Store(gomock.Any(), gomock.Any()).Times(1).
				Do(func(ctx context.Context, jobList []*jobsdb.JobT) {
					job := jobList[0]
					var parameters map[string]interface{}
					err := json.Unmarshal(job.Parameters, &parameters)
					if err != nil {
						panic(err)
					}

					Expect(job.JobID).To(Equal(unprocessedJobsList[0].JobID))
					Expect(job.CustomVal).To(Equal(unprocessedJobsList[0].CustomVal))
					Expect(job.UserID).To(Equal(unprocessedJobsList[0].UserID))
					procErrorStored = true
				})

			c.mockRouterJobsDB.EXPECT().WithUpdateSafeTx(gomock.Any(), gomock.Any()).Do(func(ctx context.Context, f func(tx jobsdb.UpdateSafeTx) error) {
				_ = f(jobsdb.EmptyUpdateSafeTx())
			}).Return(nil).Times(1)

			c.mockRouterJobsDB.EXPECT().UpdateJobStatusInTx(gomock.Any(), gomock.Any(), gomock.Any(), []string{customVal["GA"]}, nil).Times(1).
				Do(func(ctx context.Context, tx jobsdb.UpdateSafeTx, drainList []*jobsdb.JobStatusT, _, _ interface{}) {
					Expect(drainList).To(HaveLen(1))
					assertJobStatus(unprocessedJobsList[0], drainList[0], jobsdb.Aborted.State, "410", `{"reason": "job expired"}`, 0)
					routerAborted = true
				})

			<-router.backendConfigInitialized
			worker := newPartitionWorker(context.Background(), router, gaDestinationID)
			defer worker.Stop()
			Expect(worker.Work()).To(BeTrue())
			Expect(worker.pickupCount).To(Equal(len(unprocessedJobsList)))
			Eventually(func() bool { return routerAborted && procErrorStored }, 5*time.Second, 100*time.Millisecond).Should(Equal(true))
		})

		It("aborts jobs that bear a abort configured jobRunId", func() {
			config.Set("RSources.toAbortJobRunIDs", "someJobRunId")
			router := &Handle{
				Reporting: &reporting.NOOP{},
			}
			c.mockBackendConfig.EXPECT().AccessToken().AnyTimes()

			router.Setup(gaDestinationDefinition, logger.NOP, conf, c.mockBackendConfig, c.mockRouterJobsDB, c.mockProcErrorsDB, transientsource.NewEmptyService(), rsources.NewNoOpService(), destinationdebugger.NewNoOpService())
			mockNetHandle := mocksRouter.NewMockNetHandle(c.mockCtrl)
			router.netHandle = mockNetHandle

			gaPayload := `{"body": {"XML": {}, "FORM": {}, "JSON": {}}, "type": "REST", "files": {}, "method": "POST", "params": {"t": "event", "v": "1", "an": "RudderAndroidClient", "av": "1.0", "ds": "android-sdk", "ea": "Demo Track", "ec": "Demo Category", "el": "Demo Label", "ni": 0, "qt": 59268380964, "ul": "en-US", "cid": "anon_id", "tid": "UA-185645846-1", "uip": "[::1]", "aiid": "com.rudderlabs.android.sdk"}, "userId": "anon_id", "headers": {}, "version": "1", "endpoint": "https://www.google-analytics.com/collect"}`
			parameters := fmt.Sprintf(`{"source_job_run_id": "someJobRunId", "source_id": "1fMCVYZboDlYlauh4GFsEo2JU77", "destination_id": "%s", "message_id": "2f548e6d-60f6-44af-a1f4-62b3272445c3", "received_at": "2021-06-28T10:04:48.527+05:30", "transform_at": "processor"}`, gaDestinationID) // skipcq: GO-R4002

			unprocessedJobsList := []*jobsdb.JobT{
				{
					UUID:         uuid.New(),
					UserID:       "u1",
					JobID:        2010,
					CreatedAt:    time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					ExpireAt:     time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					CustomVal:    customVal["GA"],
					EventPayload: []byte(gaPayload),
					LastJobStatus: jobsdb.JobStatusT{
						AttemptNum: 0,
					},
					Parameters:  []byte(parameters),
					WorkspaceId: workspaceID,
				},
			}

			payloadLimit := router.reloadableConfig.payloadLimit
			c.mockRouterJobsDB.EXPECT().GetToProcess(gomock.Any(), jobsdb.GetQueryParams{
				CustomValFilters: []string{customVal["GA"]},
				ParameterFilters: []jobsdb.ParameterFilterT{{Name: "destination_id", Value: gaDestinationID}},
				PayloadSizeLimit: payloadLimit.Load(),
				JobsLimit:        10000,
			}, nil).Times(1).Return(&jobsdb.MoreJobsResult{JobsResult: jobsdb.JobsResult{Jobs: unprocessedJobsList}}, nil)

			var routerAborted bool
			var procErrorStored bool

			c.mockRouterJobsDB.EXPECT().UpdateJobStatus(gomock.Any(), gomock.Any(), []string{customVal["GA"]}, nil).Times(1)

			c.mockProcErrorsDB.EXPECT().Store(gomock.Any(), gomock.Any()).Times(1).
				Do(func(ctx context.Context, jobList []*jobsdb.JobT) {
					job := jobList[0]
					var parameters map[string]interface{}
					err := json.Unmarshal(job.Parameters, &parameters)
					if err != nil {
						panic(err)
					}

					Expect(job.JobID).To(Equal(unprocessedJobsList[0].JobID))
					Expect(job.CustomVal).To(Equal(unprocessedJobsList[0].CustomVal))
					Expect(job.UserID).To(Equal(unprocessedJobsList[0].UserID))
					procErrorStored = true
				})

			c.mockRouterJobsDB.EXPECT().WithUpdateSafeTx(gomock.Any(), gomock.Any()).Do(func(ctx context.Context, f func(tx jobsdb.UpdateSafeTx) error) {
				_ = f(jobsdb.EmptyUpdateSafeTx())
			}).Return(nil).Times(1)

			c.mockRouterJobsDB.EXPECT().UpdateJobStatusInTx(gomock.Any(), gomock.Any(), gomock.Any(), []string{customVal["GA"]}, nil).Times(1).
				Do(func(ctx context.Context, tx jobsdb.UpdateSafeTx, drainList []*jobsdb.JobStatusT, _, _ interface{}) {
					Expect(drainList).To(HaveLen(1))
					assertJobStatus(unprocessedJobsList[0], drainList[0], jobsdb.Aborted.State, "410", `{"reason": "job expired"}`, 0)
					routerAborted = true
				})

			<-router.backendConfigInitialized
			worker := newPartitionWorker(context.Background(), router, gaDestinationID)
			defer worker.Stop()
			Expect(worker.Work()).To(BeTrue())
			Expect(worker.pickupCount).To(Equal(len(unprocessedJobsList)))
			Eventually(func() bool { return routerAborted && procErrorStored }, 5*time.Second, 100*time.Millisecond).Should(Equal(true))
		})

		It("aborts events that have reached max retries", func() {
			config.Set("Router.jobRetention", "24h")
			mockNetHandle := mocksRouter.NewMockNetHandle(c.mockCtrl)
			c.mockBackendConfig.EXPECT().AccessToken().AnyTimes()

			router := &Handle{
				Reporting: &reporting.NOOP{},
			}
			router.Setup(gaDestinationDefinition, logger.NOP, conf, c.mockBackendConfig, c.mockRouterJobsDB, c.mockProcErrorsDB, transientsource.NewEmptyService(), rsources.NewNoOpService(), destinationdebugger.NewNoOpService())
			router.netHandle = mockNetHandle

			firstAttemptedAt := time.Now().Add(-router.reloadableConfig.retryTimeWindow.Load())
			jobs := []*jobsdb.JobT{
				{
					UUID:         uuid.New(),
					UserID:       "u1",
					JobID:        2010,
					CreatedAt:    firstAttemptedAt.Add(-time.Minute),
					ExpireAt:     firstAttemptedAt.Add(-time.Minute),
					CustomVal:    customVal["GA"],
					EventPayload: []byte(`{"body": {"XML": {}, "FORM": {}, "JSON": {}}, "type": "REST", "files": {}, "method": "POST", "params": {"t": "event", "v": "1", "an": "RudderAndroidClient", "av": "1.0", "ds": "android-sdk", "ea": "Demo Track", "ec": "Demo Category", "el": "Demo Label", "ni": 0, "qt": 59268380964, "ul": "en-US", "cid": "anon_id", "tid": "UA-185645846-1", "uip": "[::1]", "aiid": "com.rudderlabs.android.sdk"}, "userId": "anon_id", "headers": {}, "version": "1", "endpoint": "https://www.google-analytics.com/collect"}`),
					LastJobStatus: jobsdb.JobStatusT{
						AttemptNum:    router.reloadableConfig.maxFailedCountForJob.Load(),
						JobState:      jobsdb.Failed.State,
						ErrorCode:     "500",
						ErrorResponse: []byte(fmt.Sprintf(`{"firstAttemptedAt": %q}`, firstAttemptedAt.Format(misc.RFC3339Milli))),
					},
					Parameters: []byte(fmt.Sprintf(`{
						"source_id": "1fMCVYZboDlYlauh4GFsEo2JU77",
						"destination_id": "%s",
						"message_id": "2f548e6d-60f6-44af-a1f4-62b3272445c3",
						"received_at": "%s",
						"transform_at": "processor"
					}`, gaDestinationID, firstAttemptedAt.Add(-time.Minute).Format(misc.RFC3339Milli))),
					WorkspaceId: workspaceID,
				},
			}

			payloadLimit := router.reloadableConfig.payloadLimit
			c.mockRouterJobsDB.EXPECT().GetToProcess(gomock.Any(), jobsdb.GetQueryParams{
				CustomValFilters: []string{customVal["GA"]},
				ParameterFilters: []jobsdb.ParameterFilterT{{Name: "destination_id", Value: gaDestinationID}},
				PayloadSizeLimit: payloadLimit.Load(),
				JobsLimit:        10000,
			}, nil).Times(1).Return(&jobsdb.MoreJobsResult{JobsResult: jobsdb.JobsResult{Jobs: jobs}}, nil)

			var routerAborted bool
			var procErrorStored bool

			c.mockRouterJobsDB.EXPECT().UpdateJobStatus(gomock.Any(), gomock.Any(), []string{customVal["GA"]}, nil).Times(1)

			c.mockProcErrorsDB.EXPECT().Store(gomock.Any(), gomock.Any()).Times(1).
				Do(func(ctx context.Context, jobList []*jobsdb.JobT) {
					job := jobList[0]
					var parameters map[string]interface{}
					err := json.Unmarshal(job.Parameters, &parameters)
					if err != nil {
						panic(err)
					}

					Expect(job.JobID).To(Equal(jobs[0].JobID))
					Expect(job.CustomVal).To(Equal(jobs[0].CustomVal))
					Expect(job.UserID).To(Equal(jobs[0].UserID))
					procErrorStored = true
				})

			c.mockRouterJobsDB.EXPECT().WithUpdateSafeTx(gomock.Any(), gomock.Any()).Do(func(ctx context.Context, f func(tx jobsdb.UpdateSafeTx) error) {
				_ = f(jobsdb.EmptyUpdateSafeTx())
			}).Return(nil).Times(1)

			c.mockRouterJobsDB.EXPECT().UpdateJobStatusInTx(gomock.Any(), gomock.Any(), gomock.Any(), []string{customVal["GA"]}, nil).Times(1).
				Do(func(ctx context.Context, tx jobsdb.UpdateSafeTx, drainList []*jobsdb.JobStatusT, _, _ interface{}) {
					Expect(drainList).To(HaveLen(1))
					assertJobStatus(
						jobs[0],
						drainList[0],
						jobsdb.Aborted.State,
						routerUtils.DRAIN_ERROR_CODE,
						fmt.Sprintf(
							`{"reason": %s}`,
							fmt.Sprintf(
								`{"firstAttemptedAt": %q}`,
								firstAttemptedAt.Format(misc.RFC3339Milli),
							),
						),
						jobs[0].LastJobStatus.AttemptNum,
					)
					routerAborted = true
				})

			<-router.backendConfigInitialized
			worker := newPartitionWorker(context.Background(), router, gaDestinationID)
			defer worker.Stop()
			Expect(worker.Work()).To(BeTrue())
			Expect(worker.pickupCount).To(Equal(len(jobs)))
			Eventually(func() bool {
				return routerAborted && procErrorStored
			}, 60*time.Second, 10*time.Millisecond).
				Should(Equal(true), fmt.Sprintf("Router should both abort (actual: %t) and store to proc error (actual: %t)", routerAborted, procErrorStored))
		})

		It("aborts sources events that have reached max retries - different limits", func() {
			config.Set("Router.jobRetention", "24h")
			mockNetHandle := mocksRouter.NewMockNetHandle(c.mockCtrl)
			c.mockBackendConfig.EXPECT().AccessToken().AnyTimes()

			router := &Handle{
				Reporting: &reporting.NOOP{},
			}
			router.Setup(gaDestinationDefinition, logger.NOP, conf, c.mockBackendConfig, c.mockRouterJobsDB, c.mockProcErrorsDB, transientsource.NewEmptyService(), rsources.NewNoOpService(), destinationdebugger.NewNoOpService())
			router.netHandle = mockNetHandle

			firstAttemptedAt := time.Now().Add(-router.reloadableConfig.sourcesRetryTimeWindow.Load())
			jobs := []*jobsdb.JobT{
				{
					UUID:         uuid.New(),
					UserID:       "u1",
					JobID:        2010,
					CreatedAt:    firstAttemptedAt.Add(-time.Minute),
					ExpireAt:     firstAttemptedAt.Add(-time.Minute),
					CustomVal:    customVal["GA"],
					EventPayload: []byte(`{"body": {"XML": {}, "FORM": {}, "JSON": {}}, "type": "REST", "files": {}, "method": "POST", "params": {"t": "event", "v": "1", "an": "RudderAndroidClient", "av": "1.0", "ds": "android-sdk", "ea": "Demo Track", "ec": "Demo Category", "el": "Demo Label", "ni": 0, "qt": 59268380964, "ul": "en-US", "cid": "anon_id", "tid": "UA-185645846-1", "uip": "[::1]", "aiid": "com.rudderlabs.android.sdk"}, "userId": "anon_id", "headers": {}, "version": "1", "endpoint": "https://www.google-analytics.com/collect"}`),
					LastJobStatus: jobsdb.JobStatusT{
						AttemptNum:    router.reloadableConfig.maxFailedCountForSourcesJob.Load(),
						JobState:      jobsdb.Failed.State,
						ErrorCode:     "500",
						ErrorResponse: []byte(fmt.Sprintf(`{"firstAttemptedAt": %q}`, firstAttemptedAt.Format(misc.RFC3339Milli))),
						JobParameters: []byte(fmt.Sprintf(`{
							"source_id": "1fMCVYZboDlYlauh4GFsEo2JU77",
							"destination_id": "%s",
							"message_id": "2f548e6d-60f6-44af-a1f4-62b3272445c3",
							"received_at": "%s",
							"transform_at": "processor",
							"source_job_run_id": "someJobRunId"
						}`, gaDestinationID, firstAttemptedAt.Add(-time.Minute).Format(misc.RFC3339Milli))),
					},
					Parameters: []byte(fmt.Sprintf(`{
						"source_id": "1fMCVYZboDlYlauh4GFsEo2JU77",
						"destination_id": "%s",
						"message_id": "2f548e6d-60f6-44af-a1f4-62b3272445c3",
						"received_at": "%s",
						"transform_at": "processor",
						"source_job_run_id": "someJobRunId"
					}`, gaDestinationID, firstAttemptedAt.Add(-time.Minute).Format(misc.RFC3339Milli))),
					WorkspaceId: workspaceID,
				},
			}

			payloadLimit := router.reloadableConfig.payloadLimit
			c.mockRouterJobsDB.EXPECT().GetToProcess(gomock.Any(), jobsdb.GetQueryParams{
				CustomValFilters: []string{customVal["GA"]},
				ParameterFilters: []jobsdb.ParameterFilterT{{Name: "destination_id", Value: gaDestinationID}},
				PayloadSizeLimit: payloadLimit.Load(),
				JobsLimit:        10000,
			}, nil).Times(1).Return(&jobsdb.MoreJobsResult{JobsResult: jobsdb.JobsResult{Jobs: jobs}}, nil)

			var routerAborted bool
			var procErrorStored bool

			c.mockRouterJobsDB.EXPECT().UpdateJobStatus(gomock.Any(), gomock.Any(), []string{customVal["GA"]}, nil).Times(1)

			c.mockProcErrorsDB.EXPECT().Store(gomock.Any(), gomock.Any()).Times(1).
				Do(func(ctx context.Context, jobList []*jobsdb.JobT) {
					job := jobList[0]
					var parameters map[string]interface{}
					err := json.Unmarshal(job.Parameters, &parameters)
					if err != nil {
						panic(err)
					}

					Expect(job.JobID).To(Equal(jobs[0].JobID))
					Expect(job.CustomVal).To(Equal(jobs[0].CustomVal))
					Expect(job.UserID).To(Equal(jobs[0].UserID))
					procErrorStored = true
				})

			c.mockRouterJobsDB.EXPECT().WithUpdateSafeTx(gomock.Any(), gomock.Any()).Do(func(ctx context.Context, f func(tx jobsdb.UpdateSafeTx) error) {
				_ = f(jobsdb.EmptyUpdateSafeTx())
			}).Return(nil).Times(1)

			c.mockRouterJobsDB.EXPECT().UpdateJobStatusInTx(gomock.Any(), gomock.Any(), gomock.Any(), []string{customVal["GA"]}, nil).Times(1).
				Do(func(ctx context.Context, tx jobsdb.UpdateSafeTx, drainList []*jobsdb.JobStatusT, _, _ interface{}) {
					Expect(drainList).To(HaveLen(1))
					assertJobStatus(jobs[0], drainList[0], jobsdb.Aborted.State, routerUtils.DRAIN_ERROR_CODE, fmt.Sprintf(`{"reason": %s}`, fmt.Sprintf(`{"firstAttemptedAt": %q}`, firstAttemptedAt.Format(misc.RFC3339Milli))), jobs[0].LastJobStatus.AttemptNum)
					routerAborted = true
				})

			<-router.backendConfigInitialized
			worker := newPartitionWorker(context.Background(), router, gaDestinationID)
			defer worker.Stop()
			Expect(worker.Work()).To(BeTrue())
			Expect(worker.pickupCount).To(Equal(len(jobs)))
			Eventually(func() bool {
				return routerAborted && procErrorStored
			}, 60*time.Second, 10*time.Millisecond).
				Should(Equal(true), fmt.Sprintf("Router should both abort (actual: %t) and store to proc error (actual: %t)", routerAborted, procErrorStored))
		})

		It("can fail jobs if time is more than router timeout", func() {
			mockNetHandle := mocksRouter.NewMockNetHandle(c.mockCtrl)
			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)
			router := &Handle{
				Reporting: &reporting.NOOP{},
				netHandle: mockNetHandle,
			}
			c.mockBackendConfig.EXPECT().AccessToken().AnyTimes()
			router.Setup(gaDestinationDefinition, logger.NOP, conf, c.mockBackendConfig, c.mockRouterJobsDB, c.mockProcErrorsDB, transientsource.NewEmptyService(), rsources.NewNoOpService(), destinationdebugger.NewNoOpService())
			router.transformer = mockTransformer
			router.noOfWorkers = 1
			router.reloadableConfig.noOfJobsToBatchInAWorker = misc.SingleValueLoader(5)
			router.reloadableConfig.routerTimeout = misc.SingleValueLoader(time.Duration(0))

			gaPayload := `{"body": {"XML": {}, "FORM": {}, "JSON": {}}, "type": "REST", "files": {}, "method": "POST", "params": {"t": "event", "v": "1", "an": "RudderAndroidClient", "av": "1.0", "ds": "android-sdk", "ea": "Demo Track", "ec": "Demo Category", "el": "Demo Label", "ni": 0, "qt": 59268380964, "ul": "en-US", "cid": "anon_id", "tid": "UA-185645846-1", "uip": "[::1]", "aiid": "com.rudderlabs.android.sdk"}, "userId": "anon_id", "headers": {}, "version": "1", "endpoint": "https://www.google-analytics.com/collect"}`
			parameters := fmt.Sprintf(`{"source_id": "1fMCVYZboDlYlauh4GFsEo2JU77", "destination_id": "%s", "message_id": "2f548e6d-60f6-44af-a1f4-62b3272445c3", "received_at": "2021-06-28T10:04:48.527+05:30", "transform_at": "processor"}`, gaDestinationID) // skipcq: GO-R4002

			toRetryJobsList := []*jobsdb.JobT{
				{
					UUID:         uuid.New(),
					UserID:       "u1",
					JobID:        2009,
					CreatedAt:    time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					ExpireAt:     time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					CustomVal:    customVal["GA"],
					EventPayload: []byte(gaPayload),
					LastJobStatus: jobsdb.JobStatusT{
						AttemptNum:    1,
						ErrorResponse: []byte(`{"firstAttemptedAt": "2021-06-28T15:57:30.742+05:30"}`),
					},
					Parameters: []byte(parameters),
				},
			}

			unprocessedJobsList := []*jobsdb.JobT{
				{
					UUID:         uuid.New(),
					UserID:       "u1",
					JobID:        2010,
					CreatedAt:    time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					ExpireAt:     time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					CustomVal:    customVal["GA"],
					EventPayload: []byte(gaPayload),
					LastJobStatus: jobsdb.JobStatusT{
						AttemptNum: 0,
					},
					Parameters: []byte(parameters),
				},
				{
					UUID:         uuid.New(),
					UserID:       "u2",
					JobID:        2011,
					CreatedAt:    time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					ExpireAt:     time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					CustomVal:    customVal["GA"],
					EventPayload: []byte(gaPayload),
					LastJobStatus: jobsdb.JobStatusT{
						AttemptNum: 0,
					},
					Parameters: []byte(parameters),
				},
				{
					UUID:         uuid.New(),
					UserID:       "u2",
					JobID:        2012,
					CreatedAt:    time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					ExpireAt:     time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					CustomVal:    customVal["GA"],
					EventPayload: []byte(gaPayload),
					LastJobStatus: jobsdb.JobStatusT{
						AttemptNum: 0,
					},
					Parameters: []byte(parameters),
				},
				{
					UUID:         uuid.New(),
					UserID:       "u3",
					JobID:        2013,
					CreatedAt:    time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					ExpireAt:     time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					CustomVal:    customVal["GA"],
					EventPayload: []byte(gaPayload),
					LastJobStatus: jobsdb.JobStatusT{
						AttemptNum: 0,
					},
					Parameters: []byte(parameters),
				},
			}

			allJobs := append(toRetryJobsList, unprocessedJobsList...)

			payloadLimit := router.reloadableConfig.payloadLimit
			callAllJobs := c.mockRouterJobsDB.EXPECT().GetToProcess(gomock.Any(),
				jobsdb.GetQueryParams{
					CustomValFilters: []string{customVal["GA"]},
					ParameterFilters: []jobsdb.ParameterFilterT{{Name: "destination_id", Value: gaDestinationID}},
					PayloadSizeLimit: payloadLimit.Load(),
					JobsLimit:        10000,
				}, nil).Times(1).Return(&jobsdb.MoreJobsResult{JobsResult: jobsdb.JobsResult{Jobs: allJobs}}, nil)

			c.mockRouterJobsDB.EXPECT().UpdateJobStatus(gomock.Any(), gomock.Any(), []string{customVal["GA"]}, nil).Times(1).
				Do(func(ctx context.Context, statuses []*jobsdb.JobStatusT, _, _ interface{}) {
					assertJobStatus(toRetryJobsList[0], statuses[0], jobsdb.Executing.State, "", `{}`, 1)
					assertJobStatus(unprocessedJobsList[0], statuses[1], jobsdb.Executing.State, "", `{}`, 0)
					assertJobStatus(unprocessedJobsList[1], statuses[2], jobsdb.Executing.State, "", `{}`, 0)
					assertJobStatus(unprocessedJobsList[2], statuses[3], jobsdb.Executing.State, "", `{}`, 0)
					assertJobStatus(unprocessedJobsList[3], statuses[4], jobsdb.Executing.State, "", `{}`, 0)
				}).Return(nil).After(callAllJobs)
			mockNetHandle.EXPECT().SendPost(gomock.Any(), gomock.Any()).Times(0)
			done := make(chan struct{})
			c.mockRouterJobsDB.EXPECT().WithUpdateSafeTx(gomock.Any(), gomock.Any()).Times(1).Do(func(ctx context.Context, f func(tx jobsdb.UpdateSafeTx) error) {
				_ = f(jobsdb.EmptyUpdateSafeTx())
				close(done)
			}).Return(nil)
			c.mockRouterJobsDB.EXPECT().UpdateJobStatusInTx(gomock.Any(), gomock.Any(), gomock.Any(), []string{customVal["GA"]}, nil).Times(1)

			<-router.backendConfigInitialized
			worker := newPartitionWorker(context.Background(), router, gaDestinationID)
			defer worker.Stop()
			Expect(worker.Work()).To(BeTrue())
			Expect(worker.pickupCount).To(Equal(5))
			Eventually(func() bool {
				select {
				case <-done:
					return true
				default:
					return false
				}
			}, 20*time.Second, 100*time.Millisecond).Should(Equal(true))
		})

		It("fails jobs if destination is not found in config", func() {
			mockNetHandle := mocksRouter.NewMockNetHandle(c.mockCtrl)
			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)
			router := &Handle{
				Reporting: &reporting.NOOP{},
				netHandle: mockNetHandle,
			}
			c.mockBackendConfig.EXPECT().AccessToken().AnyTimes()
			router.Setup(gaDestinationDefinition, logger.NOP, conf, c.mockBackendConfig, c.mockRouterJobsDB, c.mockProcErrorsDB, transientsource.NewEmptyService(), rsources.NewNoOpService(), destinationdebugger.NewNoOpService())
			router.transformer = mockTransformer
			router.noOfWorkers = 1
			router.reloadableConfig.noOfJobsToBatchInAWorker = misc.SingleValueLoader(5)
			router.reloadableConfig.routerTimeout = misc.SingleValueLoader(60 * time.Second)
			router.reloadableConfig.jobIteratorMaxQueries = misc.SingleValueLoader(1)

			gaPayload := `{"body": {"XML": {}, "FORM": {}, "JSON": {}}, "type": "REST", "files": {}, "method": "POST", "params": {"t": "event", "v": "1", "an": "RudderAndroidClient", "av": "1.0", "ds": "android-sdk", "ea": "Demo Track", "ec": "Demo Category", "el": "Demo Label", "ni": 0, "qt": 59268380964, "ul": "en-US", "cid": "anon_id", "tid": "UA-185645846-1", "uip": "[::1]", "aiid": "com.rudderlabs.android.sdk"}, "userId": "anon_id", "headers": {}, "version": "1", "endpoint": "https://www.google-analytics.com/collect"}`
			parameters := fmt.Sprintf(`{"source_id": "1fMCVYZboDlYlauh4GFsEo2JU77", "destination_id": "%s", "message_id": "2f548e6d-60f6-44af-a1f4-62b3272445c3", "received_at": "2021-06-28T10:04:48.527+05:30", "transform_at": "processor"}`, nonexistentDestinationID) // skipcq: GO-R4002

			unprocessedJobsList := []*jobsdb.JobT{
				{
					UUID:         uuid.New(),
					UserID:       "u1",
					JobID:        2010,
					CreatedAt:    time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					ExpireAt:     time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					CustomVal:    customVal["GA"],
					EventPayload: []byte(gaPayload),
					LastJobStatus: jobsdb.JobStatusT{
						AttemptNum: 3, // done only to check the error response assertion(assertJobStatus) as well
					},
					Parameters: []byte(parameters),
				},
			}

			payloadLimit := router.reloadableConfig.payloadLimit
			callAllJobs := c.mockRouterJobsDB.EXPECT().GetToProcess(
				gomock.Any(),
				jobsdb.GetQueryParams{
					CustomValFilters: []string{customVal["GA"]},
					ParameterFilters: []jobsdb.ParameterFilterT{{Name: "destination_id", Value: gaDestinationID}},
					PayloadSizeLimit: payloadLimit.Load(),
					JobsLimit:        10000,
				},
				nil).
				Times(1).
				Return(&jobsdb.MoreJobsResult{JobsResult: jobsdb.JobsResult{Jobs: unprocessedJobsList}}, nil)

			c.mockRouterJobsDB.EXPECT().UpdateJobStatus(gomock.Any(), gomock.Any(), []string{customVal["GA"]}, nil).Times(1).
				Do(func(ctx context.Context, statuses []*jobsdb.JobStatusT, _, _ interface{}) {
					assertJobStatus(unprocessedJobsList[0], statuses[0], jobsdb.Executing.State, "", `{}`, 3)
				}).Return(nil).After(callAllJobs)

			done := make(chan struct{})
			c.mockRouterJobsDB.EXPECT().
				WithUpdateSafeTx(
					gomock.Any(),
					gomock.Any()).
				Times(1).
				Do(
					func(ctx context.Context, f func(tx jobsdb.UpdateSafeTx) error) {
						_ = f(jobsdb.EmptyUpdateSafeTx())
						close(done)
					}).Return(nil)

			c.mockRouterJobsDB.EXPECT().
				UpdateJobStatusInTx(gomock.Any(), gomock.Any(), gomock.Any(), []string{customVal["GA"]}, nil).
				Times(1).
				Do(func(ctx context.Context, _ jobsdb.UpdateSafeTx, statuses []*jobsdb.JobStatusT, _, _ interface{}) {
					assertJobStatus(
						unprocessedJobsList[0],
						statuses[0],
						jobsdb.Failed.State,
						"",
						`{"reason": "failed because destination is not available in the config" }`,
						3,
					)
				}).Return(nil)
			<-router.backendConfigInitialized
			worker := newPartitionWorker(context.Background(), router, gaDestinationID)
			defer worker.Stop()
			Expect(worker.Work()).To(BeTrue())
			Expect(worker.pickupCount).To(Equal(1))
			Eventually(func() bool {
				select {
				case <-done:
					return true
				default:
					return false
				}
			}, 20*time.Second, 100*time.Millisecond).Should(Equal(true))
		})
	})

	Context("router batching", func() {
		BeforeEach(func() {
			conf.Set("Router.maxStatusUpdateWait", "2s")
		})

		It("can batch jobs together", func() {
			mockNetHandle := mocksRouter.NewMockNetHandle(c.mockCtrl)
			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)
			router := &Handle{
				Reporting: &reporting.NOOP{},
				netHandle: mockNetHandle,
			}
			c.mockBackendConfig.EXPECT().AccessToken().AnyTimes()
			router.Setup(gaDestinationDefinition, logger.NOP, conf, c.mockBackendConfig, c.mockRouterJobsDB, c.mockProcErrorsDB, transientsource.NewEmptyService(), rsources.NewNoOpService(), destinationdebugger.NewNoOpService())

			router.transformer = mockTransformer

			router.enableBatching = true
			router.reloadableConfig.noOfJobsToBatchInAWorker = misc.SingleValueLoader(3)
			router.noOfWorkers = 1
			router.reloadableConfig.routerTimeout = misc.SingleValueLoader(time.Duration(math.MaxInt64))

			gaPayload := `{"body": {"XML": {}, "FORM": {}, "JSON": {}}, "type": "REST", "files": {}, "method": "POST", "params": {"t": "event", "v": "1", "an": "RudderAndroidClient", "av": "1.0", "ds": "android-sdk", "ea": "Demo Track", "ec": "Demo Category", "el": "Demo Label", "ni": 0, "qt": 59268380964, "ul": "en-US", "cid": "anon_id", "tid": "UA-185645846-1", "uip": "[::1]", "aiid": "com.rudderlabs.android.sdk"}, "userId": "anon_id", "headers": {}, "version": "1", "endpoint": "https://www.google-analytics.com/collect"}`
			parameters := fmt.Sprintf(`{"source_id": "1fMCVYZboDlYlauh4GFsEo2JU77", "destination_id": "%s", "message_id": "2f548e6d-60f6-44af-a1f4-62b3272445c3", "received_at": "2021-06-28T10:04:48.527+05:30", "transform_at": "processor"}`, gaDestinationID) // skipcq: GO-R4002

			toRetryJobsList := []*jobsdb.JobT{
				{
					UUID:         uuid.New(),
					UserID:       "u1",
					JobID:        2009,
					CreatedAt:    time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					ExpireAt:     time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					CustomVal:    customVal["GA"],
					EventPayload: []byte(gaPayload),
					LastJobStatus: jobsdb.JobStatusT{
						AttemptNum:    1,
						ErrorResponse: []byte(`{"firstAttemptedAt": "2021-06-28T15:57:30.742+05:30"}`),
					},
					Parameters: []byte(parameters),
				},
			}

			unprocessedJobsList := []*jobsdb.JobT{
				{
					UUID:         uuid.New(),
					UserID:       "u2",
					JobID:        2010,
					CreatedAt:    time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					ExpireAt:     time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					CustomVal:    customVal["GA"],
					EventPayload: []byte(gaPayload),
					LastJobStatus: jobsdb.JobStatusT{
						AttemptNum: 0,
					},
					Parameters: []byte(parameters),
				},
				{
					UUID:         uuid.New(),
					UserID:       "u3",
					JobID:        2011,
					CreatedAt:    time.Date(2020, 0o4, 28, 13, 27, 0o0, 0o0, time.UTC),
					ExpireAt:     time.Date(2020, 0o4, 28, 13, 27, 0o0, 0o0, time.UTC),
					CustomVal:    customVal["GA"],
					EventPayload: []byte(gaPayload),
					LastJobStatus: jobsdb.JobStatusT{
						AttemptNum: 0,
					},
					Parameters: []byte(parameters),
				},
			}

			jobsList := append(toRetryJobsList, unprocessedJobsList...)
			c.mockBackendConfig.EXPECT().AccessToken().AnyTimes()

			payloadLimit := router.reloadableConfig.payloadLimit
			callAllJobs := c.mockRouterJobsDB.EXPECT().GetToProcess(gomock.Any(), jobsdb.GetQueryParams{
				CustomValFilters: []string{customVal["GA"]},
				ParameterFilters: []jobsdb.ParameterFilterT{{Name: "destination_id", Value: gaDestinationID}},
				PayloadSizeLimit: payloadLimit.Load(),
				JobsLimit:        10000,
			}, nil).Times(1).Return(&jobsdb.MoreJobsResult{JobsResult: jobsdb.JobsResult{Jobs: jobsList}}, nil)

			c.mockRouterJobsDB.EXPECT().UpdateJobStatus(gomock.Any(), gomock.Any(), []string{customVal["GA"]}, nil).Times(1).
				Do(func(ctx context.Context, statuses []*jobsdb.JobStatusT, _, _ interface{}) {
					assertJobStatus(toRetryJobsList[0], statuses[0], jobsdb.Executing.State, "", `{}`, 1)
					assertJobStatus(unprocessedJobsList[0], statuses[1], jobsdb.Executing.State, "", `{}`, 0)
					assertJobStatus(unprocessedJobsList[1], statuses[2], jobsdb.Executing.State, "", `{}`, 0)
				}).Return(nil)

			mockTransformer.EXPECT().Transform("BATCH", gomock.Any()).After(callAllJobs).Times(1).
				DoAndReturn(
					func(_ string, transformMessage *types.TransformMessageT) []types.DestinationJobT {
						assertRouterJobs(&transformMessage.Data[0], toRetryJobsList[0])
						assertRouterJobs(&transformMessage.Data[1], unprocessedJobsList[0])
						assertRouterJobs(&transformMessage.Data[2], unprocessedJobsList[1])
						return []types.DestinationJobT{
							{
								Message: []byte(`{"message": "some transformed message"}`),
								JobMetadataArray: []types.JobMetadataT{
									{
										UserID: "u1",
										JobID:  2009,
										JobT:   toRetryJobsList[0],
									},
									{
										UserID: "u2",
										JobID:  2010,
										JobT:   unprocessedJobsList[0],
									},
									{
										UserID: "u3",
										JobID:  2011,
										JobT:   unprocessedJobsList[1],
									},
								},
								Batched:    true,
								Error:      `{"firstAttemptedAt": "2021-06-28T15:57:30.742+05:30"}`,
								StatusCode: 200,
							},
						}
					})

			mockNetHandle.EXPECT().SendPost(gomock.Any(), gomock.Any()).Times(1).Return(&routerUtils.SendPostResponse{StatusCode: 200, ResponseBody: []byte("")})
			done := make(chan struct{})
			c.mockRouterJobsDB.EXPECT().WithUpdateSafeTx(gomock.Any(), gomock.Any()).Times(1).Do(func(ctx context.Context, f func(tx jobsdb.UpdateSafeTx) error) {
				_ = f(jobsdb.EmptyUpdateSafeTx())
				close(done)
			}).Return(nil)
			c.mockRouterJobsDB.EXPECT().UpdateJobStatusInTx(gomock.Any(), gomock.Any(), gomock.Any(), []string{customVal["GA"]}, nil).Times(1).
				Do(func(ctx context.Context, _ interface{}, statuses []*jobsdb.JobStatusT, _, _ interface{}) {
					assertTransformJobStatuses(toRetryJobsList[0], statuses[0], jobsdb.Succeeded.State, "200", 1)
					assertTransformJobStatuses(unprocessedJobsList[0], statuses[1], jobsdb.Succeeded.State, "200", 1)
					assertTransformJobStatuses(unprocessedJobsList[1], statuses[2], jobsdb.Succeeded.State, "200", 1)
				})

			<-router.backendConfigInitialized
			worker := newPartitionWorker(context.Background(), router, gaDestinationID)
			defer worker.Stop()
			Expect(worker.Work()).To(BeTrue())
			Expect(worker.pickupCount).To(Equal(3))
			Eventually(func() bool {
				select {
				case <-done:
					return true
				default:
					return false
				}
			}, 20*time.Second, 100*time.Millisecond).Should(Equal(true))
		})

		It("aborts jobs if batching fails for few of the jobs", func() {
			mockNetHandle := mocksRouter.NewMockNetHandle(c.mockCtrl)
			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)
			router := &Handle{
				Reporting: &reporting.NOOP{},
				netHandle: mockNetHandle,
			}
			c.mockBackendConfig.EXPECT().AccessToken().AnyTimes()
			router.Setup(gaDestinationDefinition, logger.NOP, conf, c.mockBackendConfig, c.mockRouterJobsDB, c.mockProcErrorsDB, transientsource.NewEmptyService(), rsources.NewNoOpService(), destinationdebugger.NewNoOpService())

			// we have a job that has failed once(toRetryJobsList), it should abort when picked up next
			// Because we only allow one failure per job with this
			router.transformer = mockTransformer
			router.reloadableConfig.noOfJobsToBatchInAWorker = misc.SingleValueLoader(3)
			router.reloadableConfig.maxFailedCountForJob = misc.SingleValueLoader(5)
			router.enableBatching = true

			gaPayload := `{"body": {"XML": {}, "FORM": {}, "JSON": {}}, "type": "REST", "files": {}, "method": "POST", "params": {"t": "event", "v": "1", "an": "RudderAndroidClient", "av": "1.0", "ds": "android-sdk", "ea": "Demo Track", "ec": "Demo Category", "el": "Demo Label", "ni": 0, "qt": 59268380964, "ul": "en-US", "cid": "anon_id", "tid": "UA-185645846-1", "uip": "[::1]", "aiid": "com.rudderlabs.android.sdk"}, "userId": "anon_id", "headers": {}, "version": "1", "endpoint": "https://www.google-analytics.com/collect"}`
			parameters := fmt.Sprintf(`{"source_id": "1fMCVYZboDlYlauh4GFsEo2JU77", "destination_id": "%s", "message_id": "2f548e6d-60f6-44af-a1f4-62b3272445c3", "received_at": "2021-06-28T10:04:48.527+05:30", "transform_at": "processor"}`, gaDestinationID) // skipcq: GO-R4002

			toRetryJobsList := []*jobsdb.JobT{
				{
					UUID:         uuid.New(),
					UserID:       "u1",
					JobID:        2009,
					CreatedAt:    time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					ExpireAt:     time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					CustomVal:    customVal["GA"],
					EventPayload: []byte(gaPayload),
					LastJobStatus: jobsdb.JobStatusT{
						AttemptNum:    1,
						ErrorResponse: []byte(`{"firstAttemptedAt": "2021-06-28T15:57:30.742+05:30"}`),
					},
					Parameters: []byte(parameters),
				},
			}
			unprocessedJobsList := []*jobsdb.JobT{
				{
					UUID:         uuid.New(),
					UserID:       "u1",
					JobID:        2010,
					CreatedAt:    time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					ExpireAt:     time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					CustomVal:    customVal["GA"],
					EventPayload: []byte(gaPayload),
					LastJobStatus: jobsdb.JobStatusT{
						AttemptNum: 0,
					},
					Parameters: []byte(parameters),
				},
				{
					UUID:         uuid.New(),
					UserID:       "u1",
					JobID:        2011,
					CreatedAt:    time.Date(2020, 0o4, 28, 13, 27, 0o0, 0o0, time.UTC),
					ExpireAt:     time.Date(2020, 0o4, 28, 13, 27, 0o0, 0o0, time.UTC),
					CustomVal:    customVal["GA"],
					EventPayload: []byte(gaPayload),
					LastJobStatus: jobsdb.JobStatusT{
						AttemptNum: 0,
					},
					Parameters: []byte(parameters),
				},
			}
			allJobs := append(toRetryJobsList, unprocessedJobsList...)

			payloadLimit := router.reloadableConfig.payloadLimit
			callAllJobs := c.mockRouterJobsDB.EXPECT().GetToProcess(gomock.Any(),
				jobsdb.GetQueryParams{
					CustomValFilters: []string{customVal["GA"]},
					ParameterFilters: []jobsdb.ParameterFilterT{{Name: "destination_id", Value: gaDestinationID}},
					PayloadSizeLimit: payloadLimit.Load(),
					JobsLimit:        10000,
				}, nil).Return(&jobsdb.MoreJobsResult{JobsResult: jobsdb.JobsResult{Jobs: toRetryJobsList}}, nil).Times(
				1).Return(&jobsdb.MoreJobsResult{JobsResult: jobsdb.JobsResult{Jobs: allJobs}}, nil)

			c.mockRouterJobsDB.EXPECT().UpdateJobStatus(gomock.Any(), gomock.Any(), []string{customVal["GA"]}, nil).Times(1).
				Do(func(ctx context.Context, statuses []*jobsdb.JobStatusT, _, _ interface{}) {
					assertJobStatus(toRetryJobsList[0], statuses[0], jobsdb.Executing.State, "", `{}`, 1)
					assertJobStatus(unprocessedJobsList[0], statuses[1], jobsdb.Executing.State, "", `{}`, 0)
					assertJobStatus(unprocessedJobsList[1], statuses[2], jobsdb.Executing.State, "", `{}`, 0)
				}).Return(nil).After(callAllJobs)

			mockTransformer.EXPECT().Transform("BATCH", gomock.Any()).After(callAllJobs).Times(1).DoAndReturn(
				func(_ string, transformMessage *types.TransformMessageT) []types.DestinationJobT {
					assertRouterJobs(&transformMessage.Data[0], toRetryJobsList[0])
					assertRouterJobs(&transformMessage.Data[1], unprocessedJobsList[0])
					assertRouterJobs(&transformMessage.Data[2], unprocessedJobsList[1])

					return []types.DestinationJobT{
						{
							Message: []byte(`{"message": "some transformed message"}`),
							JobMetadataArray: []types.JobMetadataT{
								{
									UserID:           "u1",
									JobID:            2009,
									JobT:             toRetryJobsList[0],
									FirstAttemptedAt: "2021-06-28T15:57:30.742+05:30",
									AttemptNum:       1,
								},
								{
									UserID:           "u1",
									JobID:            2010,
									JobT:             unprocessedJobsList[0],
									FirstAttemptedAt: "2021-06-28T15:57:30.742+05:30",
								},
							},
							Batched:    true,
							Error:      `{"firstAttemptedAt": "2021-06-28T15:57:30.742+05:30"}`,
							StatusCode: 500,
						},
						{
							Message: []byte(`{"message": "some transformed message"}`),
							JobMetadataArray: []types.JobMetadataT{
								{
									UserID: "u1",
									JobID:  2011,
									JobT:   unprocessedJobsList[1],
								},
							},
							Batched:    true,
							Error:      ``,
							StatusCode: 200,
						},
					}
				})

			mockNetHandle.EXPECT().SendPost(gomock.Any(), gomock.Any()).Times(0).Return(&routerUtils.SendPostResponse{StatusCode: 200, ResponseBody: []byte("")})
			done := make(chan struct{})

			c.mockRouterJobsDB.EXPECT().WithUpdateSafeTx(gomock.Any(), gomock.Any()).Times(1).Do(func(ctx context.Context, f func(tx jobsdb.UpdateSafeTx) error) {
				_ = f(jobsdb.EmptyUpdateSafeTx())
				close(done)
			}).Return(nil)
			c.mockRouterJobsDB.EXPECT().UpdateJobStatusInTx(gomock.Any(), gomock.Any(), gomock.Any(), []string{customVal["GA"]}, nil).Times(1).
				Do(func(ctx context.Context, _ interface{}, statuses []*jobsdb.JobStatusT, _, _ interface{}) {
					assertTransformJobStatuses(toRetryJobsList[0], statuses[0], jobsdb.Failed.State, "500", 2)
					assertTransformJobStatuses(unprocessedJobsList[0], statuses[1], jobsdb.Waiting.State, "", 0)
					assertTransformJobStatuses(unprocessedJobsList[1], statuses[2], jobsdb.Waiting.State, "", 0)
				})

			<-router.backendConfigInitialized
			worker := newPartitionWorker(context.Background(), router, gaDestinationID)
			defer worker.Stop()
			Expect(worker.Work()).To(BeTrue())
			Expect(worker.pickupCount).To(Equal(3))
			Eventually(func() bool {
				select {
				case <-done:
					return true
				default:
					return false
				}
			}, 20*time.Second, 100*time.Millisecond).Should(Equal(true))
		})
	})

	Context("router transform", func() {
		BeforeEach(func() {
			conf.Set("Router.maxStatusUpdateWait", "2s")
			conf.Set("Router.jobsBatchTimeout", "10s")
		})
		/*
			Router transform
				Job1 u1
				Job2 u1
				Job3 u2
				Job4 u2
				Job5 u3
			{[1]: T200 [2]: T500, [3]: T500, [4]: T200, [5]: T200}

			[1] should be sent
			[2] shouldn't be sent
			[3] shouldn't be sent
			[4] should be dropped
			[5] should be sent
		*/
		It("can transform jobs at router", func() {
			mockNetHandle := mocksRouter.NewMockNetHandle(c.mockCtrl)
			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)
			router := &Handle{
				Reporting: &reporting.NOOP{},
				netHandle: mockNetHandle,
			}
			c.mockBackendConfig.EXPECT().AccessToken().AnyTimes()
			router.Setup(gaDestinationDefinition, logger.NOP, conf, c.mockBackendConfig, c.mockRouterJobsDB, c.mockProcErrorsDB, transientsource.NewEmptyService(), rsources.NewNoOpService(), destinationdebugger.NewNoOpService())
			router.transformer = mockTransformer
			router.noOfWorkers = 1
			router.reloadableConfig.noOfJobsToBatchInAWorker = misc.SingleValueLoader(5)
			router.reloadableConfig.routerTimeout = misc.SingleValueLoader(time.Duration(math.MaxInt64))

			gaPayload := `{"body": {"XML": {}, "FORM": {}, "JSON": {}}, "type": "REST", "files": {}, "method": "POST", "params": {"t": "event", "v": "1", "an": "RudderAndroidClient", "av": "1.0", "ds": "android-sdk", "ea": "Demo Track", "ec": "Demo Category", "el": "Demo Label", "ni": 0, "qt": 59268380964, "ul": "en-US", "cid": "anon_id", "tid": "UA-185645846-1", "uip": "[::1]", "aiid": "com.rudderlabs.android.sdk"}, "userId": "anon_id", "headers": {}, "version": "1", "endpoint": "https://www.google-analytics.com/collect"}`
			parameters := fmt.Sprintf(`{"source_id": "1fMCVYZboDlYlauh4GFsEo2JU77", "destination_id": "%s", "message_id": "2f548e6d-60f6-44af-a1f4-62b3272445c3", "received_at": "2021-06-28T10:04:48.527+05:30", "transform_at": "router"}`, gaDestinationID) // skipcq: GO-R4002

			toRetryJobsList := []*jobsdb.JobT{
				{
					UUID:         uuid.New(),
					UserID:       "u1",
					JobID:        2009,
					CreatedAt:    time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					ExpireAt:     time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					CustomVal:    customVal["GA"],
					EventPayload: []byte(gaPayload),
					LastJobStatus: jobsdb.JobStatusT{
						AttemptNum:    1,
						ErrorResponse: []byte(`{"firstAttemptedAt": "2021-06-28T15:57:30.742+05:30"}`),
					},
					Parameters: []byte(parameters),
				},
			}

			unprocessedJobsList := []*jobsdb.JobT{
				{
					UUID:         uuid.New(),
					UserID:       "u1",
					JobID:        2010,
					CreatedAt:    time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					ExpireAt:     time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					CustomVal:    customVal["GA"],
					EventPayload: []byte(gaPayload),
					LastJobStatus: jobsdb.JobStatusT{
						AttemptNum: 0,
					},
					Parameters: []byte(parameters),
				},
				{
					UUID:         uuid.New(),
					UserID:       "u2",
					JobID:        2011,
					CreatedAt:    time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					ExpireAt:     time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					CustomVal:    customVal["GA"],
					EventPayload: []byte(gaPayload),
					LastJobStatus: jobsdb.JobStatusT{
						AttemptNum: 0,
					},
					Parameters: []byte(parameters),
				},
				{
					UUID:         uuid.New(),
					UserID:       "u2",
					JobID:        2012,
					CreatedAt:    time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					ExpireAt:     time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					CustomVal:    customVal["GA"],
					EventPayload: []byte(gaPayload),
					LastJobStatus: jobsdb.JobStatusT{
						AttemptNum: 0,
					},
					Parameters: []byte(parameters),
				},
				{
					UUID:         uuid.New(),
					UserID:       "u3",
					JobID:        2013,
					CreatedAt:    time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					ExpireAt:     time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					CustomVal:    customVal["GA"],
					EventPayload: []byte(gaPayload),
					LastJobStatus: jobsdb.JobStatusT{
						AttemptNum: 0,
					},
					Parameters: []byte(parameters),
				},
			}

			allJobs := append(toRetryJobsList, unprocessedJobsList...)

			payloadLimit := router.reloadableConfig.payloadLimit
			callAllJobs := c.mockRouterJobsDB.EXPECT().GetToProcess(gomock.Any(),
				jobsdb.GetQueryParams{
					CustomValFilters: []string{customVal["GA"]},
					ParameterFilters: []jobsdb.ParameterFilterT{{Name: "destination_id", Value: gaDestinationID}},
					PayloadSizeLimit: payloadLimit.Load(),
					JobsLimit:        10000,
				}, nil).Times(1).Return(&jobsdb.MoreJobsResult{JobsResult: jobsdb.JobsResult{Jobs: allJobs}}, nil)

			c.mockRouterJobsDB.EXPECT().UpdateJobStatus(gomock.Any(), gomock.Any(), []string{customVal["GA"]}, nil).Times(1).
				Do(func(ctx context.Context, statuses []*jobsdb.JobStatusT, _, _ interface{}) {
					assertJobStatus(toRetryJobsList[0], statuses[0], jobsdb.Executing.State, "", `{}`, 1)
					assertJobStatus(unprocessedJobsList[0], statuses[1], jobsdb.Executing.State, "", `{}`, 0)
					assertJobStatus(unprocessedJobsList[1], statuses[2], jobsdb.Executing.State, "", `{}`, 0)
					assertJobStatus(unprocessedJobsList[2], statuses[3], jobsdb.Executing.State, "", `{}`, 0)
					assertJobStatus(unprocessedJobsList[3], statuses[4], jobsdb.Executing.State, "", `{}`, 0)
				}).Return(nil).After(callAllJobs)

			mockTransformer.EXPECT().Transform("ROUTER_TRANSFORM", gomock.Any()).After(callAllJobs).Times(1).DoAndReturn(
				func(_ string, transformMessage *types.TransformMessageT) []types.DestinationJobT {
					assertRouterJobs(&transformMessage.Data[0], toRetryJobsList[0])
					assertRouterJobs(&transformMessage.Data[1], unprocessedJobsList[0])
					assertRouterJobs(&transformMessage.Data[2], unprocessedJobsList[1])
					assertRouterJobs(&transformMessage.Data[3], unprocessedJobsList[2])
					assertRouterJobs(&transformMessage.Data[4], unprocessedJobsList[3])

					return []types.DestinationJobT{
						{
							Message: []byte(`{"message": "some transformed message"}`),
							JobMetadataArray: []types.JobMetadataT{
								{
									UserID:     "u1",
									JobID:      2009,
									AttemptNum: 1,
									JobT:       toRetryJobsList[0],
								},
							},
							Batched:    false,
							Error:      `{"firstAttemptedAt": "2021-06-28T15:57:30.742+05:30"}`,
							StatusCode: 200,
						},
						{
							Message: []byte(`{"message": "some transformed message"}`),
							JobMetadataArray: []types.JobMetadataT{
								{
									UserID: "u1",
									JobID:  2010,
									JobT:   unprocessedJobsList[0],
								},
							},
							Batched:    false,
							Error:      `{"firstAttemptedAt": "2021-06-28T15:57:30.742+05:30"}`,
							StatusCode: 500,
						},
						{
							Message: []byte(`{"message": "some transformed message"}`),
							JobMetadataArray: []types.JobMetadataT{
								{
									UserID: "u2",
									JobID:  2011,
									JobT:   unprocessedJobsList[1],
								},
							},
							Batched:    false,
							Error:      `{"firstAttemptedAt": "2021-06-28T15:57:30.742+05:30"}`,
							StatusCode: 500,
						},
						{
							Message: []byte(`{"message": "some transformed message"}`),
							JobMetadataArray: []types.JobMetadataT{
								{
									UserID: "u2",
									JobID:  2012,
									JobT:   unprocessedJobsList[2],
								},
							},
							Batched:    false,
							Error:      `{"firstAttemptedAt": "2021-06-28T15:57:30.742+05:30"}`,
							StatusCode: 200,
						},
						{
							Message: []byte(`{"message": "some transformed message"}`),
							JobMetadataArray: []types.JobMetadataT{
								{
									UserID: "u3",
									JobID:  2013,
									JobT:   unprocessedJobsList[3],
								},
							},
							Batched:    false,
							Error:      `{"firstAttemptedAt": "2021-06-28T15:57:30.742+05:30"}`,
							StatusCode: 200,
						},
					}
				})

			mockNetHandle.EXPECT().SendPost(gomock.Any(), gomock.Any()).Times(2).Return(&routerUtils.SendPostResponse{StatusCode: 200, ResponseBody: []byte("")})
			done := make(chan struct{})
			c.mockRouterJobsDB.EXPECT().WithUpdateSafeTx(gomock.Any(), gomock.Any()).Times(1).Do(func(ctx context.Context, f func(tx jobsdb.UpdateSafeTx) error) {
				_ = f(jobsdb.EmptyUpdateSafeTx())
				close(done)
			}).Return(nil)
			c.mockRouterJobsDB.EXPECT().UpdateJobStatusInTx(gomock.Any(), gomock.Any(), gomock.Any(), []string{customVal["GA"]}, nil).Times(1)

			<-router.backendConfigInitialized
			worker := newPartitionWorker(context.Background(), router, gaDestinationID)
			defer worker.Stop()
			Expect(worker.Work()).To(BeTrue())
			Expect(worker.pickupCount).To(Equal(5))
			Eventually(func() bool {
				select {
				case <-done:
					return true
				default:
					return false
				}
			}, 20*time.Second, 100*time.Millisecond).Should(Equal(true))
		})

		It("skip sendpost && (if statusCode returned is 298 then mark as filtered & if statusCode returned is 299 then mark as succeeded)", func() {
			mockNetHandle := mocksRouter.NewMockNetHandle(c.mockCtrl)
			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)
			router := &Handle{
				Reporting: &reporting.NOOP{},
				netHandle: mockNetHandle,
			}
			c.mockBackendConfig.EXPECT().AccessToken().AnyTimes()
			router.Setup(gaDestinationDefinition, logger.NOP, conf, c.mockBackendConfig, c.mockRouterJobsDB, c.mockProcErrorsDB, transientsource.NewEmptyService(), rsources.NewNoOpService(), destinationdebugger.NewNoOpService())
			router.transformer = mockTransformer
			router.noOfWorkers = 1
			router.reloadableConfig.noOfJobsToBatchInAWorker = misc.SingleValueLoader(3)
			router.reloadableConfig.routerTimeout = misc.SingleValueLoader(time.Duration(math.MaxInt64))

			gaPayload := `{"body": {"XML": {}, "FORM": {}, "JSON": {}}, "type": "REST", "files": {}, "method": "POST", "params": {"t": "event", "v": "1", "an": "RudderAndroidClient", "av": "1.0", "ds": "android-sdk", "ea": "Demo Track", "ec": "Demo Category", "el": "Demo Label", "ni": 0, "qt": 59268380964, "ul": "en-US", "cid": "anon_id", "tid": "UA-185645846-1", "uip": "[::1]", "aiid": "com.rudderlabs.android.sdk"}, "userId": "anon_id", "headers": {}, "version": "1", "endpoint": "https://www.google-analytics.com/collect"}`
			parameters := fmt.Sprintf(`{"source_id": "1fMCVYZboDlYlauh4GFsEo2JU77", "destination_id": "%s", "message_id": "2f548e6d-60f6-44af-a1f4-62b3272445c3", "received_at": "2021-06-28T10:04:48.527+05:30", "transform_at": "router"}`, gaDestinationID) // skipcq: GO-R4002

			toRetryJobsList := []*jobsdb.JobT{
				{
					UUID:         uuid.New(),
					UserID:       "u1",
					JobID:        2009,
					CreatedAt:    time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					ExpireAt:     time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					CustomVal:    customVal["GA"],
					EventPayload: []byte(gaPayload),
					LastJobStatus: jobsdb.JobStatusT{
						AttemptNum:    1,
						ErrorResponse: []byte(`{"firstAttemptedAt": "2021-06-28T15:57:30.742+05:30"}`),
					},
					Parameters: []byte(parameters),
				},
			}

			unprocessedJobsList := []*jobsdb.JobT{
				{
					UUID:         uuid.New(),
					UserID:       "u1",
					JobID:        2010,
					CreatedAt:    time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					ExpireAt:     time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					CustomVal:    customVal["GA"],
					EventPayload: []byte(gaPayload),
					LastJobStatus: jobsdb.JobStatusT{
						AttemptNum: 0,
					},
					Parameters: []byte(parameters),
				},
				{
					UUID:         uuid.New(),
					UserID:       "u2",
					JobID:        2011,
					CreatedAt:    time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					ExpireAt:     time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					CustomVal:    customVal["GA"],
					EventPayload: []byte(gaPayload),
					LastJobStatus: jobsdb.JobStatusT{
						AttemptNum: 0,
					},
					Parameters: []byte(parameters),
				},
			}

			allJobs := append(toRetryJobsList, unprocessedJobsList...)

			payloadLimit := router.reloadableConfig.payloadLimit
			callAllJobs := c.mockRouterJobsDB.EXPECT().GetToProcess(gomock.Any(),
				jobsdb.GetQueryParams{
					CustomValFilters: []string{customVal["GA"]},
					ParameterFilters: []jobsdb.ParameterFilterT{{Name: "destination_id", Value: gaDestinationID}},
					PayloadSizeLimit: payloadLimit.Load(),
					JobsLimit:        10000,
				}, nil).Times(1).Return(&jobsdb.MoreJobsResult{JobsResult: jobsdb.JobsResult{Jobs: allJobs}}, nil)

			c.mockRouterJobsDB.EXPECT().UpdateJobStatus(gomock.Any(), gomock.Any(), []string{customVal["GA"]}, nil).Times(1).
				Do(func(ctx context.Context, statuses []*jobsdb.JobStatusT, _, _ interface{}) {
					assertJobStatus(toRetryJobsList[0], statuses[0], jobsdb.Executing.State, "", `{}`, 1)
					assertJobStatus(unprocessedJobsList[0], statuses[1], jobsdb.Executing.State, "", `{}`, 0)
					assertJobStatus(unprocessedJobsList[1], statuses[2], jobsdb.Executing.State, "", `{}`, 0)
				}).Return(nil).After(callAllJobs)

			mockTransformer.EXPECT().Transform("ROUTER_TRANSFORM", gomock.Any()).After(callAllJobs).Times(1).DoAndReturn(
				func(_ string, transformMessage *types.TransformMessageT) []types.DestinationJobT {
					assertRouterJobs(&transformMessage.Data[0], toRetryJobsList[0])
					assertRouterJobs(&transformMessage.Data[1], unprocessedJobsList[0])
					assertRouterJobs(&transformMessage.Data[2], unprocessedJobsList[1])

					return []types.DestinationJobT{
						{
							Message: []byte(`{"message": "some transformed message"}`),
							JobMetadataArray: []types.JobMetadataT{
								{
									UserID:     "u1",
									JobID:      2009,
									AttemptNum: 1,
									JobT:       toRetryJobsList[0],
								},
							},
							Batched:    false,
							Error:      `{}`,
							StatusCode: 200,
						},
						{
							Message: []byte(`{"message": "some transformed message"}`),
							JobMetadataArray: []types.JobMetadataT{
								{
									UserID: "u1",
									JobID:  2010,
									JobT:   unprocessedJobsList[0],
								},
							},
							Batched:    false,
							Error:      `{}`,
							StatusCode: 298,
						},
						{
							Message: []byte(`{"message": "some transformed message"}`),
							JobMetadataArray: []types.JobMetadataT{
								{
									UserID: "u2",
									JobID:  2011,
									JobT:   unprocessedJobsList[1],
								},
							},
							Batched:    false,
							Error:      `{}`,
							StatusCode: 299,
						},
					}
				})

			mockNetHandle.EXPECT().SendPost(gomock.Any(), gomock.Any()).Times(1).Return(&routerUtils.SendPostResponse{StatusCode: 200, ResponseBody: []byte("")})
			done := make(chan struct{})
			c.mockRouterJobsDB.EXPECT().WithUpdateSafeTx(gomock.Any(), gomock.Any()).Times(1).Do(func(ctx context.Context, f func(tx jobsdb.UpdateSafeTx) error) {
				_ = f(jobsdb.EmptyUpdateSafeTx())
				close(done)
			}).Return(nil)
			c.mockRouterJobsDB.EXPECT().UpdateJobStatusInTx(gomock.Any(), gomock.Any(), gomock.Len(len(toRetryJobsList)+len(unprocessedJobsList)), []string{customVal["GA"]}, nil).Times(1).
				Do(func(ctx context.Context, txn jobsdb.UpdateSafeTx, statuses []*jobsdb.JobStatusT, _, _ interface{}) {
					assertTransformJobStatuses(toRetryJobsList[0], statuses[0], jobsdb.Succeeded.State, "200", 2)
					assertTransformJobStatuses(unprocessedJobsList[0], statuses[1], jobsdb.Filtered.State, "298", 1)
					assertTransformJobStatuses(unprocessedJobsList[1], statuses[2], jobsdb.Succeeded.State, "299", 1)
				})

			<-router.backendConfigInitialized
			worker := newPartitionWorker(context.Background(), router, gaDestinationID)
			defer worker.Stop()
			Expect(worker.Work()).To(BeTrue())
			Expect(worker.pickupCount).To(Equal(3))
			Eventually(func() bool {
				select {
				case <-done:
					return true
				default:
					return false
				}
			}, 20*time.Second, 100*time.Millisecond).Should(Equal(true))
		})

		/*
				Job1 u1
				Job2 u1
				Job3 u1
			{[1]: T500, [T2]: T200, [3]: T200}

				[1] shouldn't be sent
				[2] should be dropped
				[3] should be dropped
		*/
		It("marks all jobs of a user failed if a preceding job fails due to transformation failure", func() {
			mockNetHandle := mocksRouter.NewMockNetHandle(c.mockCtrl)
			router := &Handle{
				Reporting: &reporting.NOOP{},
				netHandle: mockNetHandle,
			}
			c.mockBackendConfig.EXPECT().AccessToken().AnyTimes()
			router.Setup(gaDestinationDefinition, logger.NOP, conf, c.mockBackendConfig, c.mockRouterJobsDB, c.mockProcErrorsDB, transientsource.NewEmptyService(), rsources.NewNoOpService(), destinationdebugger.NewNoOpService())
			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)
			router.transformer = mockTransformer

			router.reloadableConfig.noOfJobsToBatchInAWorker = misc.SingleValueLoader(3)
			router.noOfWorkers = 1

			gaPayload := `{"body": {"XML": {}, "FORM": {}, "JSON": {}}, "type": "REST", "files": {}, "method": "POST", "params": {"t": "event", "v": "1", "an": "RudderAndroidClient", "av": "1.0", "ds": "android-sdk", "ea": "Demo Track", "ec": "Demo Category", "el": "Demo Label", "ni": 0, "qt": 59268380964, "ul": "en-US", "cid": "anon_id", "tid": "UA-185645846-1", "uip": "[::1]", "aiid": "com.rudderlabs.android.sdk"}, "userId": "anon_id", "headers": {}, "version": "1", "endpoint": "https://www.google-analytics.com/collect"}`
			parameters := fmt.Sprintf(`{"source_id": "1fMCVYZboDlYlauh4GFsEo2JU77", "destination_id": "%s", "message_id": "2f548e6d-60f6-44af-a1f4-62b3272445c3", "received_at": "2021-06-28T10:04:48.527+05:30", "transform_at": "router"}`, gaDestinationID) // skipcq: GO-R4002

			toRetryJobsList := []*jobsdb.JobT{
				{
					UUID:         uuid.New(),
					UserID:       "u1",
					JobID:        2009,
					CreatedAt:    time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					ExpireAt:     time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					CustomVal:    customVal["GA"],
					EventPayload: []byte(gaPayload),
					LastJobStatus: jobsdb.JobStatusT{
						AttemptNum:    1,
						ErrorResponse: []byte(`{"firstAttemptedAt": "2021-06-28T15:57:30.742+05:30"}`),
					},
					Parameters: []byte(parameters),
				},
			}

			unprocessedJobsList := []*jobsdb.JobT{
				{
					UUID:         uuid.New(),
					UserID:       "u1",
					JobID:        2010,
					CreatedAt:    time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					ExpireAt:     time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					CustomVal:    customVal["GA"],
					EventPayload: []byte(gaPayload),
					LastJobStatus: jobsdb.JobStatusT{
						AttemptNum: 0,
					},
					Parameters: []byte(parameters),
				},
				{
					UUID:         uuid.New(),
					UserID:       "u2",
					JobID:        2011,
					CreatedAt:    time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					ExpireAt:     time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					CustomVal:    customVal["GA"],
					EventPayload: []byte(gaPayload),
					LastJobStatus: jobsdb.JobStatusT{
						AttemptNum: 0,
					},
					Parameters: []byte(parameters),
				},
			}

			allJobs := append(toRetryJobsList, unprocessedJobsList...)

			payloadLimit := router.reloadableConfig.payloadLimit
			callAllJobs := c.mockRouterJobsDB.EXPECT().GetToProcess(gomock.Any(),
				jobsdb.GetQueryParams{
					CustomValFilters: []string{customVal["GA"]},
					ParameterFilters: []jobsdb.ParameterFilterT{{Name: "destination_id", Value: gaDestinationID}},
					PayloadSizeLimit: payloadLimit.Load(),
					JobsLimit:        10000,
				}, nil).Times(1).Return(&jobsdb.MoreJobsResult{JobsResult: jobsdb.JobsResult{Jobs: allJobs}}, nil)

			c.mockRouterJobsDB.EXPECT().UpdateJobStatus(gomock.Any(), gomock.Any(), []string{customVal["GA"]}, nil).Times(1).
				Do(func(ctx context.Context, statuses []*jobsdb.JobStatusT, _, _ interface{}) {
					assertJobStatus(toRetryJobsList[0], statuses[0], jobsdb.Executing.State, "", `{}`, 1)
					assertJobStatus(unprocessedJobsList[0], statuses[1], jobsdb.Executing.State, "", `{}`, 0)
					assertJobStatus(unprocessedJobsList[1], statuses[2], jobsdb.Executing.State, "", `{}`, 0)
				}).Return(nil).After(callAllJobs)

			mockTransformer.EXPECT().Transform("ROUTER_TRANSFORM", gomock.Any()).After(callAllJobs).Times(1).DoAndReturn(
				func(_ string, transformMessage *types.TransformMessageT) []types.DestinationJobT {
					assertRouterJobs(&transformMessage.Data[0], toRetryJobsList[0])
					assertRouterJobs(&transformMessage.Data[1], unprocessedJobsList[0])
					assertRouterJobs(&transformMessage.Data[2], unprocessedJobsList[1])

					return []types.DestinationJobT{
						{
							Message: []byte(`{"message": "some transformed message"}`),
							JobMetadataArray: []types.JobMetadataT{
								{
									UserID:     "u1",
									JobID:      2009,
									AttemptNum: 1,
									JobT:       toRetryJobsList[0],
								},
							},
							Batched:    false,
							Error:      `{"firstAttemptedAt": "2021-06-28T15:57:30.742+05:30"}`,
							StatusCode: 500,
						},
						{
							Message: []byte(`{"message": "some transformed message"}`),
							JobMetadataArray: []types.JobMetadataT{
								{
									UserID: "u1",
									JobID:  2010,
									JobT:   unprocessedJobsList[0],
								},
							},
							Batched:    false,
							Error:      `{"firstAttemptedAt": "2021-06-28T15:57:30.742+05:30"}`,
							StatusCode: 200,
						},
						{
							Message: []byte(`{"message": "some transformed message"}`),
							JobMetadataArray: []types.JobMetadataT{
								{
									UserID: "u1",
									JobID:  2010,
									JobT:   unprocessedJobsList[0],
								},
							},
							Batched:    false,
							Error:      `{"firstAttemptedAt": "2021-06-28T15:57:30.742+05:30"}`,
							StatusCode: 200,
						},
					}
				})
			mockNetHandle.EXPECT().SendPost(gomock.Any(), gomock.Any()).Times(0).Return(&routerUtils.SendPostResponse{StatusCode: 200, ResponseBody: []byte("")})
			done := make(chan struct{})

			c.mockRouterJobsDB.EXPECT().WithUpdateSafeTx(gomock.Any(), gomock.Any()).Times(1).Do(func(ctx context.Context, f func(tx jobsdb.UpdateSafeTx) error) {
				_ = f(jobsdb.EmptyUpdateSafeTx())
				close(done)
			}).Return(nil)
			c.mockRouterJobsDB.EXPECT().UpdateJobStatusInTx(gomock.Any(), gomock.Any(), gomock.Any(), []string{customVal["GA"]}, nil).Times(1)

			<-router.backendConfigInitialized
			worker := newPartitionWorker(context.Background(), router, gaDestinationID)
			defer worker.Stop()
			Expect(worker.Work()).To(BeTrue())
			Expect(worker.pickupCount).To(Equal(3))
			Eventually(func() bool {
				select {
				case <-done:
					return true
				default:
					return false
				}
			}, 20*time.Second, 100*time.Millisecond).Should(Equal(true))
		})
	})
})

func assertRouterJobs(routerJob *types.RouterJobT, job *jobsdb.JobT) {
	Expect(routerJob.JobMetadata.JobID).To(Equal(job.JobID))
	Expect(routerJob.JobMetadata.UserID).To(Equal(job.UserID))
}

func assertJobStatus(job *jobsdb.JobT, status *jobsdb.JobStatusT, expectedState, errorCode, errorResponse string, attemptNum int) {
	Expect(status.JobID).To(Equal(job.JobID))
	Expect(status.JobState).To(Equal(expectedState))
	Expect(status.ErrorCode).To(Equal(errorCode))
	if attemptNum >= 1 {
		Expect(gjson.GetBytes(status.ErrorResponse, "content-type").String()).To(Equal(gjson.Get(errorResponse, "content-type").String()))
		Expect(gjson.GetBytes(status.ErrorResponse, "response").String()).To(Equal(gjson.Get(errorResponse, "response").String()))
		Expect(gjson.Get(string(status.ErrorResponse), "reason").String()).To(Equal(gjson.Get(errorResponse, "reason").String()))
	}
	Expect(status.ExecTime).To(BeTemporally("~", time.Now(), 10*time.Second))
	Expect(status.RetryTime).To(BeTemporally(">=", status.ExecTime, 10*time.Second))
	Expect(status.AttemptNum).To(Equal(attemptNum))
}

func assertTransformJobStatuses(job *jobsdb.JobT, status *jobsdb.JobStatusT, expectedState, errorCode string, attemptNum int) {
	Expect(status.JobID).To(Equal(job.JobID))
	Expect(status.JobState).To(Equal(expectedState))
	Expect(status.ErrorCode).To(Equal(errorCode))
	Expect(status.ExecTime).To(BeTemporally("~", time.Now(), 10*time.Second))
	Expect(status.RetryTime).To(BeTemporally(">=", status.ExecTime, 10*time.Second))
	Expect(status.AttemptNum).To(Equal(attemptNum))
}

func Benchmark_SJSON_SET(b *testing.B) {
	var stringValue string
	var err error
	for i := 0; i < b.N; i++ {
		for k, v := range collectMetricsErrorMap {
			stringValue, err = sjson.Set(stringValue, k, v)
			if err != nil {
				stringValue = ""
			}
		}
	}
}

func Benchmark_JSON_MARSHAL(b *testing.B) {
	for i := 0; i < b.N; i++ {
		val, _ := json.Marshal(collectMetricsErrorMap)
		_ = string(val)
	}
}

func Benchmark_FASTJSON_MARSHAL(b *testing.B) {
	jsonfast := jsoniter.ConfigCompatibleWithStandardLibrary

	for i := 0; i < b.N; i++ {
		val, _ := jsonfast.Marshal(collectMetricsErrorMap)
		_ = string(val)
	}
}

func TestAllowRouterAbortAlert(t *testing.T) {
	type skipT struct {
		deliveryAlert       bool
		transformationAlert bool
	}
	cases := []struct {
		skip                   skipT
		transformerProxy       bool
		expectedAlertFlagValue bool
		errorAt                string
		caseName               string
	}{
		// normal destinations' delivery cases
		{
			caseName:               "[delivery] when deliveryAlert is to be skipped, proxy is enabled the alert should be false",
			skip:                   skipT{deliveryAlert: true},
			transformerProxy:       true,
			expectedAlertFlagValue: false,
			errorAt:                routerUtils.ERROR_AT_DEL,
		},
		{
			caseName:               "[delivery] when deliveryAlert is to be skipped, proxy is disabled the alert should be false",
			skip:                   skipT{deliveryAlert: true},
			transformerProxy:       false,
			expectedAlertFlagValue: false,
			errorAt:                routerUtils.ERROR_AT_DEL,
		},
		{
			caseName:               "[delivery] when deliveryAlert is not to be skipped, proxy is disabled the alert should be true",
			skip:                   skipT{},
			transformerProxy:       false,
			expectedAlertFlagValue: true,
			errorAt:                routerUtils.ERROR_AT_DEL,
		},
		{
			caseName:               "[delivery] when deliveryAlert is to be skipped, proxy is enabled the alert should be false",
			skip:                   skipT{},
			transformerProxy:       true,
			expectedAlertFlagValue: false,
			errorAt:                routerUtils.ERROR_AT_DEL,
		},
		// transformation cases
		{
			caseName:               "[transformation] when transformationAlert is to be skipped, the alert should be false",
			skip:                   skipT{transformationAlert: true},
			expectedAlertFlagValue: false,
			errorAt:                routerUtils.ERROR_AT_TF,
		},
		{
			caseName:               "[transformation]when transformationAlert is not to be skipped, the alert should be true",
			skip:                   skipT{},
			expectedAlertFlagValue: true,
			errorAt:                routerUtils.ERROR_AT_TF,
		},
		// Custom destination's delivery cases
		{
			caseName:               "[custom] when transformerProxy is enabled, the alert should be true",
			skip:                   skipT{},
			transformerProxy:       true,
			expectedAlertFlagValue: true,
			errorAt:                routerUtils.ERROR_AT_CUST,
		},
		{
			caseName:               "[custom] when transformerProxy is disabled, the alert should be true",
			skip:                   skipT{},
			transformerProxy:       false,
			expectedAlertFlagValue: true,
			errorAt:                routerUtils.ERROR_AT_CUST,
		},
		{
			caseName:               "[custom] when transformerProxy is enabled & deliveryAlert is to be skipped, the alert should be false",
			skip:                   skipT{deliveryAlert: true},
			transformerProxy:       true,
			expectedAlertFlagValue: true,
			errorAt:                routerUtils.ERROR_AT_CUST,
		},
		// empty errorAt
		{
			caseName:               "[emptyErrorAt] when transformerProxy is disabled & deliveryAlert is not to be skipped, the alert should be true",
			skip:                   skipT{},
			expectedAlertFlagValue: true,
		},
		{
			caseName:               "[emptyErrorAt] when transformerProxy is disabled & deliveryAlert is to be skipped, the alert should be true",
			skip:                   skipT{deliveryAlert: true},
			expectedAlertFlagValue: true,
		},
	}
	for _, tc := range cases {
		wrk := &worker{
			logger: logger.NOP,
			rt: &Handle{
				reloadableConfig: &reloadableConfig{
					transformerProxy:                  misc.SingleValueLoader(tc.transformerProxy),
					skipRtAbortAlertForDelivery:       misc.SingleValueLoader(tc.skip.deliveryAlert),
					skipRtAbortAlertForTransformation: misc.SingleValueLoader(tc.skip.transformationAlert),
				},
			},
		}
		t.Run(tc.caseName, func(testT *testing.T) {
			output := wrk.allowRouterAbortedAlert(tc.errorAt)
			assert.Equal(testT, tc.expectedAlertFlagValue, output)
		})
	}
}
