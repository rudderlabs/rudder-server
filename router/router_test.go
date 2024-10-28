package router

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	jsoniter "github.com/json-iterator/go"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/admin"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/enterprise/reporting"
	"github.com/rudderlabs/rudder-server/jobsdb"
	mocksBackendConfig "github.com/rudderlabs/rudder-server/mocks/backend-config"
	mocksJobsDB "github.com/rudderlabs/rudder-server/mocks/jobsdb"
	mocksRouter "github.com/rudderlabs/rudder-server/mocks/router"
	mocksTransformer "github.com/rudderlabs/rudder-server/mocks/router/transformer"
	mockutils "github.com/rudderlabs/rudder-server/mocks/utils/types"
	"github.com/rudderlabs/rudder-server/router/internal/eventorder"
	"github.com/rudderlabs/rudder-server/router/throttler"
	"github.com/rudderlabs/rudder-server/router/transformer"
	"github.com/rudderlabs/rudder-server/router/types"
	routerutils "github.com/rudderlabs/rudder-server/router/utils"
	destinationdebugger "github.com/rudderlabs/rudder-server/services/debugger/destination"
	"github.com/rudderlabs/rudder-server/services/rsources"
	transformerFeaturesService "github.com/rudderlabs/rudder-server/services/transformer"
	"github.com/rudderlabs/rudder-server/services/transientsource"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
	testutils "github.com/rudderlabs/rudder-server/utils/tests"
	txutils "github.com/rudderlabs/rudder-server/utils/tx"
	utilTypes "github.com/rudderlabs/rudder-server/utils/types"
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
		Connections: map[string]backendconfig.Connection{
			"connection1": {
				Enabled:          true,
				SourceID:         sourceIDEnabled,
				DestinationID:    gaDestinationID,
				ProcessorEnabled: true,
				Config:           map[string]interface{}{"key": "value"},
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
	mockReporting     *mockutils.MockReporting
}

// Initialize mocks and common expectations
func (c *testContext) Setup() {
	c.asyncHelper.Setup()
	c.mockCtrl = gomock.NewController(GinkgoT())
	c.mockRouterJobsDB = mocksJobsDB.NewMockJobsDB(c.mockCtrl)
	c.mockProcErrorsDB = mocksJobsDB.NewMockJobsDB(c.mockCtrl)
	c.mockBackendConfig = mocksBackendConfig.NewMockBackendConfig(c.mockCtrl)
	c.mockReporting = mockutils.NewMockReporting(c.mockCtrl)

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

type drainer struct {
	drain  bool
	reason string
}

func (d *drainer) Drain(_ *jobsdb.JobT) (bool, string) {
	return d.drain, d.reason
}

type mockThrottlerFactory struct {
	count *atomic.Int64
}

func (m *mockThrottlerFactory) Get(destName, destID string) throttler.Throttler {
	m.count.Add(1)
	return throttler.NewNoOpThrottlerFactory().Get(destName, destID)
}

func (m *mockThrottlerFactory) Shutdown() {}

func TestBackoff(t *testing.T) {
	t.Run("nextAttemptAfter", func(t *testing.T) {
		minBackoff := 10 * time.Second
		maxBackoff := 300 * time.Second
		require.Equal(t, 10*time.Second, nextAttemptAfter(0, minBackoff, maxBackoff))
		require.Equal(t, 10*time.Second, nextAttemptAfter(1, minBackoff, maxBackoff))
		require.Equal(t, 20*time.Second, nextAttemptAfter(2, minBackoff, maxBackoff))
		require.Equal(t, 40*time.Second, nextAttemptAfter(3, minBackoff, maxBackoff))
		require.Equal(t, 80*time.Second, nextAttemptAfter(4, minBackoff, maxBackoff))
		require.Equal(t, 160*time.Second, nextAttemptAfter(5, minBackoff, maxBackoff))
		require.Equal(t, 300*time.Second, nextAttemptAfter(6, minBackoff, maxBackoff))
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
		conf := config.New()
		barrier := eventorder.NewBarrier(eventorder.WithOrderingDisabledCheckForBarrierKey(func(key eventorder.BarrierKey) bool {
			return slices.Contains(conf.GetStringSlice("Router.orderingDisabledDestinationIDs", nil), "destination")
		}))
		r := &Handle{
			logger:                logger.NOP,
			backgroundCtx:         context.Background(),
			noOfWorkers:           1,
			workerInputBufferSize: 3,
			barrier:               barrier,
			reloadableConfig: &reloadableConfig{
				maxFailedCountForJob: config.SingleValueLoader(3),
				retryTimeWindow:      config.SingleValueLoader(180 * time.Minute),
			},
			drainer:          &drainer{},
			throttlerFactory: &mockThrottlerFactory{count: new(atomic.Int64)},
			eventOrderingDisabledForWorkspace: func(workspaceID string) bool {
				return slices.Contains(conf.GetStringSlice("Router.orderingDisabledWorkspaceIDs", nil), workspaceID)
			},
			eventOrderingDisabledForDestination: func(destinationID string) bool {
				return slices.Contains(conf.GetStringSlice("Router.orderingDisabledDestinationIDs", nil), destinationID)
			},
		}
		workers := []*worker{{
			logger:  logger.NOP,
			input:   make(chan workerJob, 3),
			barrier: barrier,
		}}
		t.Run("eventorder disabled", func(t *testing.T) {
			r.guaranteeUserEventOrder = false
			workers[0].inputReservations = 0

			slot, err := r.findWorkerSlot(context.Background(), workers, backoffJob, map[eventorder.BarrierKey]struct{}{})
			require.Nil(t, slot)
			require.ErrorIs(t, err, types.ErrJobBackoff)

			slot, err = r.findWorkerSlot(context.Background(), workers, noBackoffJob1, map[eventorder.BarrierKey]struct{}{})
			require.NotNil(t, slot)
			require.NoError(t, err)
			require.Equal(t, int64(1), r.throttlerFactory.(*mockThrottlerFactory).count.Load())

			slot, err = r.findWorkerSlot(context.Background(), workers, noBackoffJob2, map[eventorder.BarrierKey]struct{}{})
			require.NotNil(t, slot)
			require.NoError(t, err)
			require.Equal(t, int64(2), r.throttlerFactory.(*mockThrottlerFactory).count.Load())

			slot, err = r.findWorkerSlot(context.Background(), workers, noBackoffJob3, map[eventorder.BarrierKey]struct{}{})
			require.NotNil(t, slot)
			require.NoError(t, err)
			require.NotNil(t, slot)
			require.Equal(t, int64(3), r.throttlerFactory.(*mockThrottlerFactory).count.Load())

			slot, err = r.findWorkerSlot(context.Background(), workers, noBackoffJob4, map[eventorder.BarrierKey]struct{}{})
			require.Nil(t, slot)
			require.ErrorIs(t, err, types.ErrWorkerNoSlot)
			require.Equal(t, int64(3), r.throttlerFactory.(*mockThrottlerFactory).count.Load())

			// reset throttler counter
			r.throttlerFactory.(*mockThrottlerFactory).count.Store(0)
		})

		t.Run("eventorder enabled", func(t *testing.T) {
			r.guaranteeUserEventOrder = true
			workers[0].inputReservations = 0

			slot, err := r.findWorkerSlot(context.Background(), workers, backoffJob, map[eventorder.BarrierKey]struct{}{})
			require.Nil(t, slot)
			require.ErrorIs(t, err, types.ErrJobBackoff)
			require.Equal(t, int64(0), r.throttlerFactory.(*mockThrottlerFactory).count.Load())

			slot, err = r.findWorkerSlot(context.Background(), workers, noBackoffJob1, map[eventorder.BarrierKey]struct{}{})
			require.NotNil(t, slot)
			require.NoError(t, err)
			require.Equal(t, int64(1), r.throttlerFactory.(*mockThrottlerFactory).count.Load())

			slot, err = r.findWorkerSlot(context.Background(), workers, noBackoffJob2, map[eventorder.BarrierKey]struct{}{})
			require.NotNil(t, slot)
			require.NoError(t, err)
			require.Equal(t, int64(2), r.throttlerFactory.(*mockThrottlerFactory).count.Load())

			slot, err = r.findWorkerSlot(context.Background(), workers, noBackoffJob3, map[eventorder.BarrierKey]struct{}{})
			require.NotNil(t, slot)
			require.NoError(t, err)
			require.Equal(t, int64(3), r.throttlerFactory.(*mockThrottlerFactory).count.Load())
			slotToRelease := slot
			defer func() { slotToRelease.slot.Release() }()

			slot, err = r.findWorkerSlot(context.Background(), workers, noBackoffJob4, map[eventorder.BarrierKey]struct{}{})
			require.Nil(t, slot)
			require.ErrorIs(t, err, types.ErrWorkerNoSlot)
			require.Equal(t, int64(3), r.throttlerFactory.(*mockThrottlerFactory).count.Load())

			// reset throttler counter
			r.throttlerFactory.(*mockThrottlerFactory).count.Store(0)
		})

		t.Run("eventorder enabled with drain job", func(t *testing.T) {
			r.drainer = &drainer{drain: true, reason: "drain job due to some reason"}
			r.guaranteeUserEventOrder = true
			workers[0].inputReservations = 0

			slot, err := r.findWorkerSlot(context.Background(), workers, backoffJob, map[eventorder.BarrierKey]struct{}{})
			require.NotNil(t, slot)
			require.NoError(t, err, "drain job should be accepted even if it's to be backed off")
			require.Equal(
				t,
				int64(0),
				r.throttlerFactory.(*mockThrottlerFactory).count.Load(),
				"throttle check shouldn't even happen for drain job",
			)

			slot, err = r.findWorkerSlot(context.Background(), workers, noBackoffJob1, map[eventorder.BarrierKey]struct{}{})
			require.NotNil(t, slot)
			require.NoError(t, err)
			require.Equal(
				t,
				int64(0),
				r.throttlerFactory.(*mockThrottlerFactory).count.Load(),
				"throttle check shouldn't even happen for drain job",
			)

			slot, err = r.findWorkerSlot(context.Background(), workers, noBackoffJob1, map[eventorder.BarrierKey]struct{}{})
			require.NotNil(t, slot)
			require.NoError(t, err)
			require.Equal(
				t,
				int64(0),
				r.throttlerFactory.(*mockThrottlerFactory).count.Load(),
				"throttle check shouldn't even happen for drain job",
			)

			slot, err = r.findWorkerSlot(context.Background(), workers, noBackoffJob1, map[eventorder.BarrierKey]struct{}{})
			require.Nil(t, slot)
			require.ErrorIs(t, err, types.ErrWorkerNoSlot)
			require.Equal(
				t,
				int64(0),
				r.throttlerFactory.(*mockThrottlerFactory).count.Load(),
				"throttle check shouldn't even happen for drain job",
			)
		})

		t.Run("context canceled", func(t *testing.T) {
			defer func() { r.backgroundCtx = context.Background() }()
			r.backgroundCtx, r.backgroundCancel = context.WithCancel(context.Background())
			r.backgroundCancel()
			slot, err := r.findWorkerSlot(context.Background(), workers, backoffJob, map[eventorder.BarrierKey]struct{}{})
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
			slot, err := r.findWorkerSlot(context.Background(), workers, invalidJob, map[eventorder.BarrierKey]struct{}{})
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
			slot, err := r.findWorkerSlot(context.Background(), workers, backoffJob, map[eventorder.BarrierKey]struct{}{{UserID: job.UserID, DestinationID: "destination"}: {}})
			require.Nil(t, slot)
			require.ErrorIs(t, err, types.ErrJobOrderBlocked)
		})

		t.Run("job not blocked after event ordering is disabled(destinationID level)", func(t *testing.T) {
			r.guaranteeUserEventOrder = true
			workers[0].inputReservations = 0
			job := &jobsdb.JobT{
				JobID:      1,
				Parameters: []byte(`{"destination_id": "destination"}`),
				LastJobStatus: jobsdb.JobStatusT{
					JobState:   jobsdb.Failed.State,
					AttemptNum: 1,
					RetryTime:  time.Now().Add(-1 * time.Hour),
				},
				WorkspaceId: "someWorkspace",
			}
			conf.Set("Router.orderingDisabledDestinationIDs", []string{"destination"})
			slot, err := r.findWorkerSlot(
				context.Background(),
				workers,
				job,
				map[eventorder.BarrierKey]struct{}{{UserID: job.UserID, DestinationID: "destination", WorkspaceID: job.WorkspaceId}: {}},
			)
			require.NoError(t, err)
			require.NotNil(t, slot)

			conf.Set("Router.orderingDisabledDestinationIDs", nil)
			slot, err = r.findWorkerSlot(
				context.Background(),
				workers,
				job,
				map[eventorder.BarrierKey]struct{}{{UserID: job.UserID, DestinationID: "destination", WorkspaceID: job.WorkspaceId}: {}})
			require.Nil(t, slot)
			require.ErrorIs(t, err, types.ErrJobOrderBlocked)
		})

		t.Run("job not blocked after event ordering is disabled(workspaceID level)", func(t *testing.T) {
			r.guaranteeUserEventOrder = true
			workers[0].inputReservations = 0
			job := &jobsdb.JobT{
				JobID:      1,
				Parameters: []byte(`{"destination_id": "destination"}`),
				LastJobStatus: jobsdb.JobStatusT{
					JobState:   jobsdb.Failed.State,
					AttemptNum: 1,
					RetryTime:  time.Now().Add(-1 * time.Hour),
				},
				WorkspaceId: "someWorkspace",
			}
			conf.Set("Router.orderingDisabledWorkspaceIDs", []string{"someWorkspace"})
			slot, err := r.findWorkerSlot(
				context.Background(),
				workers,
				job,
				map[eventorder.BarrierKey]struct{}{{UserID: job.UserID, DestinationID: "destination", WorkspaceID: job.WorkspaceId}: {}},
			)
			require.NoError(t, err)
			require.NotNil(t, slot)

			conf.Set("Router.orderingDisabledWorkspaceIDs", nil)
			slot, err = r.findWorkerSlot(
				context.Background(),
				workers,
				job,
				map[eventorder.BarrierKey]struct{}{{UserID: job.UserID, DestinationID: "destination", WorkspaceID: job.WorkspaceId}: {}},
			)
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
			router.Setup(gaDestinationDefinition, logger.NOP, conf, c.mockBackendConfig, c.mockRouterJobsDB, c.mockProcErrorsDB, transientsource.NewEmptyService(), rsources.NewNoOpService(), transformerFeaturesService.NewNoOpService(), destinationdebugger.NewNoOpService(), throttler.NewNoOpThrottlerFactory())
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

			router.Setup(gaDestinationDefinition, logger.NOP, conf, c.mockBackendConfig, c.mockRouterJobsDB, c.mockProcErrorsDB, transientsource.NewEmptyService(), rsources.NewNoOpService(), transformerFeaturesService.NewNoOpService(), destinationdebugger.NewNoOpService(), throttler.NewNoOpThrottlerFactory())

			gaPayload := `{"body": {"XML": {}, "FORM": {}, "JSON": {}}, "type": "REST", "files": {}, "method": "POST", "params": {"t": "event", "v": "1", "an": "RudderAndroidClient", "av": "1.0", "ds": "android-sdk", "ea": "Demo Track", "ec": "Demo Category", "el": "Demo Label", "ni": 0, "qt": 59268380964, "ul": "en-US", "cid": "anon_id", "tid": "UA-185645846-1", "uip": "[::1]", "aiid": "com.rudderlabs.android.sdk"}, "userId": "anon_id", "headers": {}, "version": "1", "endpoint": "https://www.google-analytics.com/collect"}`
			parameters := fmt.Sprintf(`{"source_id": "%s", "destination_id": "%s", "message_id": "2f548e6d-60f6-44af-a1f4-62b3272445c3", "received_at": "2021-06-28T10:04:48.527+05:30", "transform_at": "processor"}`, sourceIDEnabled, gaDestinationID) // skipcq: GO-R4002
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
				&routerutils.SendPostResponse{StatusCode: 200, ResponseBody: []byte("")})
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

			router.Setup(gaDestinationDefinition, logger.NOP, conf, c.mockBackendConfig, c.mockRouterJobsDB, c.mockProcErrorsDB, transientsource.NewEmptyService(), rsources.NewNoOpService(), transformerFeaturesService.NewNoOpService(), destinationdebugger.NewNoOpService(), throttler.NewNoOpThrottlerFactory())

			mockNetHandle := mocksRouter.NewMockNetHandle(c.mockCtrl)
			router.netHandle = mockNetHandle

			gaPayload := `{}`
			parameters := fmt.Sprintf(`{"source_id": "%s", "destination_id": "%s", "message_id": "2f548e6d-60f6-44af-a1f4-62b3272445c3", "received_at": "2021-06-28T10:04:48.527+05:30", "transform_at": "processor"}`, sourceIDEnabled, gaDestinationID) // skipcq: GO-R4002

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

			mockNetHandle.EXPECT().SendPost(gomock.Any(), gomock.Any()).Times(1).Return(&routerutils.SendPostResponse{StatusCode: 400, ResponseBody: []byte("")})

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

			router.Setup(gaDestinationDefinition, logger.NOP, conf, c.mockBackendConfig, c.mockRouterJobsDB, c.mockProcErrorsDB, transientsource.NewEmptyService(), rsources.NewNoOpService(), transformerFeaturesService.NewNoOpService(), destinationdebugger.NewNoOpService(), throttler.NewNoOpThrottlerFactory())
			mockNetHandle := mocksRouter.NewMockNetHandle(c.mockCtrl)
			router.netHandle = mockNetHandle

			gaPayload := `{"body": {"XML": {}, "FORM": {}, "JSON": {}}, "type": "REST", "files": {}, "method": "POST", "params": {"t": "event", "v": "1", "an": "RudderAndroidClient", "av": "1.0", "ds": "android-sdk", "ea": "Demo Track", "ec": "Demo Category", "el": "Demo Label", "ni": 0, "qt": 59268380964, "ul": "en-US", "cid": "anon_id", "tid": "UA-185645846-1", "uip": "[::1]", "aiid": "com.rudderlabs.android.sdk"}, "userId": "anon_id", "headers": {}, "version": "1", "endpoint": "https://www.google-analytics.com/collect"}`
			parameters := fmt.Sprintf(`{"source_id": "%s", "destination_id": "%s", "message_id": "2f548e6d-60f6-44af-a1f4-62b3272445c3", "received_at": "2021-06-28T10:04:48.527+05:30", "transform_at": "processor"}`, sourceIDEnabled, gaDestinationID) // skipcq: GO-R4002

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
			conf.Set("drain.jobRunIDs", "someJobRunId")
			router := &Handle{
				Reporting: &reporting.NOOP{},
			}
			c.mockBackendConfig.EXPECT().AccessToken().AnyTimes()

			router.Setup(gaDestinationDefinition, logger.NOP, conf, c.mockBackendConfig, c.mockRouterJobsDB, c.mockProcErrorsDB, transientsource.NewEmptyService(), rsources.NewNoOpService(), transformerFeaturesService.NewNoOpService(), destinationdebugger.NewNoOpService(), throttler.NewNoOpThrottlerFactory())
			mockNetHandle := mocksRouter.NewMockNetHandle(c.mockCtrl)
			router.netHandle = mockNetHandle

			gaPayload := `{"body": {"XML": {}, "FORM": {}, "JSON": {}}, "type": "REST", "files": {}, "method": "POST", "params": {"t": "event", "v": "1", "an": "RudderAndroidClient", "av": "1.0", "ds": "android-sdk", "ea": "Demo Track", "ec": "Demo Category", "el": "Demo Label", "ni": 0, "qt": 59268380964, "ul": "en-US", "cid": "anon_id", "tid": "UA-185645846-1", "uip": "[::1]", "aiid": "com.rudderlabs.android.sdk"}, "userId": "anon_id", "headers": {}, "version": "1", "endpoint": "https://www.google-analytics.com/collect"}`
			parameters := fmt.Sprintf(`{"source_job_run_id": "someJobRunId", "source_id": "%s", "destination_id": "%s", "message_id": "2f548e6d-60f6-44af-a1f4-62b3272445c3", "received_at": "2021-06-28T10:04:48.527+05:30", "transform_at": "processor"}`, sourceIDEnabled, gaDestinationID) // skipcq: GO-R4002

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
			router.Setup(gaDestinationDefinition, logger.NOP, conf, c.mockBackendConfig, c.mockRouterJobsDB, c.mockProcErrorsDB, transientsource.NewEmptyService(), rsources.NewNoOpService(), transformerFeaturesService.NewNoOpService(), destinationdebugger.NewNoOpService(), throttler.NewNoOpThrottlerFactory())
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
						"source_id": "%s",
						"destination_id": "%s",
						"message_id": "2f548e6d-60f6-44af-a1f4-62b3272445c3",
						"received_at": "%s",
						"transform_at": "processor"
					}`, sourceIDEnabled, gaDestinationID, firstAttemptedAt.Add(-time.Minute).Format(misc.RFC3339Milli))),
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
						routerutils.DRAIN_ERROR_CODE,
						fmt.Sprintf(
							`{"reason": "%[1]s", "firstAttemptedAt": %[2]q}`,
							"retry limit reached",
							firstAttemptedAt.Format(misc.RFC3339Milli),
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
			router.Setup(gaDestinationDefinition, logger.NOP, conf, c.mockBackendConfig, c.mockRouterJobsDB, c.mockProcErrorsDB, transientsource.NewEmptyService(), rsources.NewNoOpService(), transformerFeaturesService.NewNoOpService(), destinationdebugger.NewNoOpService(), throttler.NewNoOpThrottlerFactory())
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
							"source_id": "%s",
							"destination_id": "%s",
							"message_id": "2f548e6d-60f6-44af-a1f4-62b3272445c3",
							"received_at": "%s",
							"transform_at": "processor",
							"source_job_run_id": "someJobRunId"
						}`, sourceIDEnabled, gaDestinationID, firstAttemptedAt.Add(-time.Minute).Format(misc.RFC3339Milli))),
					},
					Parameters: []byte(fmt.Sprintf(`{
						"source_id": "%s",
						"destination_id": "%s",
						"message_id": "2f548e6d-60f6-44af-a1f4-62b3272445c3",
						"received_at": "%s",
						"transform_at": "processor",
						"source_job_run_id": "someJobRunId"
					}`, sourceIDEnabled, gaDestinationID, firstAttemptedAt.Add(-time.Minute).Format(misc.RFC3339Milli))),
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
						routerutils.DRAIN_ERROR_CODE,
						fmt.Sprintf(
							`{"reason": "%[1]s", "firstAttemptedAt": %[2]q}`,
							"retry limit reached",
							firstAttemptedAt.Format(misc.RFC3339Milli),
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
		It("aborts jobs if destination is not found in config", func() {
			mockNetHandle := mocksRouter.NewMockNetHandle(c.mockCtrl)
			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)
			router := &Handle{
				Reporting: &reporting.NOOP{},
				netHandle: mockNetHandle,
			}
			c.mockBackendConfig.EXPECT().AccessToken().AnyTimes()
			router.Setup(gaDestinationDefinition, logger.NOP, conf, c.mockBackendConfig, c.mockRouterJobsDB, c.mockProcErrorsDB, transientsource.NewEmptyService(), rsources.NewNoOpService(), transformerFeaturesService.NewNoOpService(), destinationdebugger.NewNoOpService(), throttler.NewNoOpThrottlerFactory())
			router.transformer = mockTransformer
			router.noOfWorkers = 1
			router.reloadableConfig.noOfJobsToBatchInAWorker = config.SingleValueLoader(5)

			gaPayload := `{"body": {"XML": {}, "FORM": {}, "JSON": {}}, "type": "REST", "files": {}, "method": "POST", "params": {"t": "event", "v": "1", "an": "RudderAndroidClient", "av": "1.0", "ds": "android-sdk", "ea": "Demo Track", "ec": "Demo Category", "el": "Demo Label", "ni": 0, "qt": 59268380964, "ul": "en-US", "cid": "anon_id", "tid": "UA-185645846-1", "uip": "[::1]", "aiid": "com.rudderlabs.android.sdk"}, "userId": "anon_id", "headers": {}, "version": "1", "endpoint": "https://www.google-analytics.com/collect"}`
			parameters := fmt.Sprintf(`{"source_id": "%s", "destination_id": "%s", "message_id": "2f548e6d-60f6-44af-a1f4-62b3272445c3", "received_at": "2021-06-28T10:04:48.527+05:30", "transform_at": "processor"}`, sourceIDEnabled, nonexistentDestinationID) // skipcq: GO-R4002

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

			c.mockProcErrorsDB.EXPECT().Store(gomock.Any(), gomock.Len(1)).Times(1)

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
						jobsdb.Aborted.State,
						routerutils.DRAIN_ERROR_CODE,
						`{"reason": "`+routerutils.DrainReasonDestNotFound+`"}`,
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
				Reporting: c.mockReporting,
				netHandle: mockNetHandle,
			}
			c.mockBackendConfig.EXPECT().AccessToken().AnyTimes()
			router.Setup(gaDestinationDefinition, logger.NOP, conf, c.mockBackendConfig, c.mockRouterJobsDB, c.mockProcErrorsDB, transientsource.NewEmptyService(), rsources.NewNoOpService(), transformerFeaturesService.NewNoOpService(), destinationdebugger.NewNoOpService(), throttler.NewNoOpThrottlerFactory())

			router.transformer = mockTransformer

			router.enableBatching = true
			router.reloadableConfig.noOfJobsToBatchInAWorker = config.SingleValueLoader(3)
			router.noOfWorkers = 1

			gaPayload := `{"body": {"XML": {}, "FORM": {}, "JSON": {}}, "type": "REST", "files": {}, "method": "POST", "params": {"t": "event", "v": "1", "an": "RudderAndroidClient", "av": "1.0", "ds": "android-sdk", "ea": "Demo Track", "ec": "Demo Category", "el": "Demo Label", "ni": 0, "qt": 59268380964, "ul": "en-US", "cid": "anon_id", "tid": "UA-185645846-1", "uip": "[::1]", "aiid": "com.rudderlabs.android.sdk"}, "userId": "anon_id", "headers": {}, "version": "1", "endpoint": "https://www.google-analytics.com/collect"}`
			parameters := fmt.Sprintf(`{"source_id": "%s", "destination_id": "%s", "message_id": "2f548e6d-60f6-44af-a1f4-62b3272445c3", "received_at": "2021-06-28T10:04:48.527+05:30", "transform_at": "processor"}`, sourceIDEnabled, gaDestinationID) // skipcq: GO-R4002

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

			mockNetHandle.EXPECT().SendPost(gomock.Any(), gomock.Any()).Times(1).Return(&routerutils.SendPostResponse{StatusCode: 200, ResponseBody: []byte("")})
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
			c.mockReporting.EXPECT().Report(gomock.Any(), gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
				func(_ context.Context, metrics []*utilTypes.PUReportedMetric, _ *txutils.Tx) error {
					Expect(metrics).To(HaveLen(1))
					Expect(metrics[0].StatusDetail.StatusCode).To(Equal(200))
					Expect(metrics[0].StatusDetail.Status).To(Equal(jobsdb.Succeeded.State))
					Expect(metrics[0].StatusDetail.SampleEvent).To(Equal(toRetryJobsList[0].EventPayload))
					return nil
				},
			)

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

		It("fails jobs if batching fails for few of the jobs", func() {
			mockNetHandle := mocksRouter.NewMockNetHandle(c.mockCtrl)
			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)
			router := &Handle{
				Reporting: c.mockReporting,
				netHandle: mockNetHandle,
			}
			c.mockBackendConfig.EXPECT().AccessToken().AnyTimes()
			router.Setup(gaDestinationDefinition, logger.NOP, conf, c.mockBackendConfig, c.mockRouterJobsDB, c.mockProcErrorsDB, transientsource.NewEmptyService(), rsources.NewNoOpService(), transformerFeaturesService.NewNoOpService(), destinationdebugger.NewNoOpService(), throttler.NewNoOpThrottlerFactory())

			router.transformer = mockTransformer
			router.reloadableConfig.noOfJobsToBatchInAWorker = config.SingleValueLoader(3)
			router.reloadableConfig.maxFailedCountForJob = config.SingleValueLoader(5)
			router.enableBatching = true

			gaPayload := `{"body": {"XML": {}, "FORM": {}, "JSON": {}}, "type": "REST", "files": {}, "method": "POST", "params": {"t": "event", "v": "1", "an": "RudderAndroidClient", "av": "1.0", "ds": "android-sdk", "ea": "Demo Track", "ec": "Demo Category", "el": "Demo Label", "ni": 0, "qt": 59268380964, "ul": "en-US", "cid": "anon_id", "tid": "UA-185645846-1", "uip": "[::1]", "aiid": "com.rudderlabs.android.sdk"}, "userId": "anon_id", "headers": {}, "version": "1", "endpoint": "https://www.google-analytics.com/collect"}`
			parameters := fmt.Sprintf(`{"source_id": "%s", "destination_id": "%s", "message_id": "2f548e6d-60f6-44af-a1f4-62b3272445c3", "received_at": "2021-06-28T10:04:48.527+05:30", "transform_at": "processor"}`, sourceIDEnabled, gaDestinationID) // skipcq: GO-R4002

			unprocessedJobsList := []*jobsdb.JobT{
				{
					UUID:          uuid.New(),
					UserID:        "u1",
					JobID:         2009,
					CreatedAt:     time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					ExpireAt:      time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					CustomVal:     customVal["GA"],
					EventPayload:  []byte(gaPayload),
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    []byte(parameters),
				},
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
			allJobs := unprocessedJobsList

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
					assertJobStatus(unprocessedJobsList[0], statuses[0], jobsdb.Executing.State, "", `{}`, 0)
					assertJobStatus(unprocessedJobsList[1], statuses[1], jobsdb.Executing.State, "", `{}`, 0)
					assertJobStatus(unprocessedJobsList[2], statuses[2], jobsdb.Executing.State, "", `{}`, 0)
				}).Return(nil).After(callAllJobs)

			mockTransformer.EXPECT().Transform("BATCH", gomock.Any()).After(callAllJobs).Times(1).DoAndReturn(
				func(_ string, transformMessage *types.TransformMessageT) []types.DestinationJobT {
					assertRouterJobs(&transformMessage.Data[0], unprocessedJobsList[0])
					assertRouterJobs(&transformMessage.Data[1], unprocessedJobsList[1])
					assertRouterJobs(&transformMessage.Data[2], unprocessedJobsList[2])

					return []types.DestinationJobT{
						{
							Message: []byte(`{"message": "some transformed message"}`),
							JobMetadataArray: []types.JobMetadataT{
								{
									UserID: "u1",
									JobID:  2009,
									JobT:   unprocessedJobsList[0],
								},
								{
									UserID: "u1",
									JobID:  2010,
									JobT:   unprocessedJobsList[1],
								},
							},
							Batched:    true,
							Error:      `{"firstAttemptedAt": "2021-06-28T15:57:30.742+05:30"}`,
							StatusCode: 500,
						},
						{
							Message: []byte(`{"message": "some other transformed message"}`),
							JobMetadataArray: []types.JobMetadataT{
								{
									UserID: "u1",
									JobID:  2011,
									JobT:   unprocessedJobsList[2],
								},
							},
							Batched:    true,
							Error:      ``,
							StatusCode: 200,
						},
					}
				})

			mockNetHandle.EXPECT().SendPost(gomock.Any(), gomock.Any()).Times(0).Return(&routerutils.SendPostResponse{StatusCode: 200, ResponseBody: []byte("")})
			done := make(chan struct{})

			c.mockRouterJobsDB.EXPECT().WithUpdateSafeTx(gomock.Any(), gomock.Any()).Times(1).Do(func(ctx context.Context, f func(tx jobsdb.UpdateSafeTx) error) {
				_ = f(jobsdb.EmptyUpdateSafeTx())
				close(done)
			}).Return(nil)
			c.mockRouterJobsDB.EXPECT().UpdateJobStatusInTx(gomock.Any(), gomock.Any(), gomock.Any(), []string{customVal["GA"]}, nil).Times(1).
				Do(func(ctx context.Context, _ interface{}, statuses []*jobsdb.JobStatusT, _, _ interface{}) {
					assertTransformJobStatuses(unprocessedJobsList[0], statuses[0], jobsdb.Failed.State, "500", 1)
					assertTransformJobStatuses(unprocessedJobsList[1], statuses[1], jobsdb.Waiting.State, "", 0)
					assertTransformJobStatuses(unprocessedJobsList[2], statuses[2], jobsdb.Waiting.State, "", 0)
				})
			c.mockReporting.EXPECT().Report(gomock.Any(), gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
				func(_ context.Context, metrics []*utilTypes.PUReportedMetric, _ *txutils.Tx) error {
					Expect(metrics).To(HaveLen(1))
					Expect(metrics[0].StatusDetail.StatusCode).To(Equal(500))
					Expect(metrics[0].StatusDetail.Status).To(Equal(jobsdb.Failed.State))
					Expect(metrics[0].StatusDetail.SampleEvent).To(Equal(json.RawMessage(gaPayload)))
					Expect(metrics[0].StatusDetail.SampleResponse).To(ContainSubstring(`"routerSubStage":"router_dest_transformer"`))

					return nil
				},
			)

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
			router.Setup(gaDestinationDefinition, logger.NOP, conf, c.mockBackendConfig, c.mockRouterJobsDB, c.mockProcErrorsDB, transientsource.NewEmptyService(), rsources.NewNoOpService(), transformerFeaturesService.NewNoOpService(), destinationdebugger.NewNoOpService(), throttler.NewNoOpThrottlerFactory())
			router.transformer = mockTransformer
			router.noOfWorkers = 1
			router.reloadableConfig.noOfJobsToBatchInAWorker = config.SingleValueLoader(5)

			gaPayload := `{"body": {"XML": {}, "FORM": {}, "JSON": {}}, "type": "REST", "files": {}, "method": "POST", "params": {"t": "event", "v": "1", "an": "RudderAndroidClient", "av": "1.0", "ds": "android-sdk", "ea": "Demo Track", "ec": "Demo Category", "el": "Demo Label", "ni": 0, "qt": 59268380964, "ul": "en-US", "cid": "anon_id", "tid": "UA-185645846-1", "uip": "[::1]", "aiid": "com.rudderlabs.android.sdk"}, "userId": "anon_id", "headers": {}, "version": "1", "endpoint": "https://www.google-analytics.com/collect"}`
			parameters := fmt.Sprintf(`{"source_id": "%s", "destination_id": "%s", "message_id": "2f548e6d-60f6-44af-a1f4-62b3272445c3", "received_at": "2021-06-28T10:04:48.527+05:30", "transform_at": "router"}`, sourceIDEnabled, gaDestinationID) // skipcq: GO-R4002

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

			mockNetHandle.EXPECT().SendPost(gomock.Any(), gomock.Any()).Times(2).Return(&routerutils.SendPostResponse{StatusCode: 200, ResponseBody: []byte("")})
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
			router.Setup(gaDestinationDefinition, logger.NOP, conf, c.mockBackendConfig, c.mockRouterJobsDB, c.mockProcErrorsDB, transientsource.NewEmptyService(), rsources.NewNoOpService(), transformerFeaturesService.NewNoOpService(), destinationdebugger.NewNoOpService(), throttler.NewNoOpThrottlerFactory())
			router.transformer = mockTransformer
			router.noOfWorkers = 1
			router.reloadableConfig.noOfJobsToBatchInAWorker = config.SingleValueLoader(3)

			gaPayload := `{"body": {"XML": {}, "FORM": {}, "JSON": {}}, "type": "REST", "files": {}, "method": "POST", "params": {"t": "event", "v": "1", "an": "RudderAndroidClient", "av": "1.0", "ds": "android-sdk", "ea": "Demo Track", "ec": "Demo Category", "el": "Demo Label", "ni": 0, "qt": 59268380964, "ul": "en-US", "cid": "anon_id", "tid": "UA-185645846-1", "uip": "[::1]", "aiid": "com.rudderlabs.android.sdk"}, "userId": "anon_id", "headers": {}, "version": "1", "endpoint": "https://www.google-analytics.com/collect"}`
			parameters := fmt.Sprintf(`{"source_id": "%s", "destination_id": "%s", "message_id": "2f548e6d-60f6-44af-a1f4-62b3272445c3", "received_at": "2021-06-28T10:04:48.527+05:30", "transform_at": "router"}`, sourceIDEnabled, gaDestinationID) // skipcq: GO-R4002

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

			mockNetHandle.EXPECT().SendPost(gomock.Any(), gomock.Any()).Times(1).Return(&routerutils.SendPostResponse{StatusCode: 200, ResponseBody: []byte("")})
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
			router.Setup(gaDestinationDefinition, logger.NOP, conf, c.mockBackendConfig, c.mockRouterJobsDB, c.mockProcErrorsDB, transientsource.NewEmptyService(), rsources.NewNoOpService(), transformerFeaturesService.NewNoOpService(), destinationdebugger.NewNoOpService(), throttler.NewNoOpThrottlerFactory())
			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)
			router.transformer = mockTransformer

			router.reloadableConfig.noOfJobsToBatchInAWorker = config.SingleValueLoader(3)
			router.noOfWorkers = 1

			gaPayload := `{"body": {"XML": {}, "FORM": {}, "JSON": {}}, "type": "REST", "files": {}, "method": "POST", "params": {"t": "event", "v": "1", "an": "RudderAndroidClient", "av": "1.0", "ds": "android-sdk", "ea": "Demo Track", "ec": "Demo Category", "el": "Demo Label", "ni": 0, "qt": 59268380964, "ul": "en-US", "cid": "anon_id", "tid": "UA-185645846-1", "uip": "[::1]", "aiid": "com.rudderlabs.android.sdk"}, "userId": "anon_id", "headers": {}, "version": "1", "endpoint": "https://www.google-analytics.com/collect"}`
			parameters := fmt.Sprintf(`{"source_id": "%s", "destination_id": "%s", "message_id": "2f548e6d-60f6-44af-a1f4-62b3272445c3", "received_at": "2021-06-28T10:04:48.527+05:30", "transform_at": "router"}`, sourceIDEnabled, gaDestinationID) // skipcq: GO-R4002

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
			mockNetHandle.EXPECT().SendPost(gomock.Any(), gomock.Any()).Times(0).Return(&routerutils.SendPostResponse{StatusCode: 200, ResponseBody: []byte("")})
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

	Context("transformer proxy", func() {
		BeforeEach(func() {
			conf.Set("Router.maxStatusUpdateWait", "2s")
		})
		It("all jobs should go through transformer proxy and succeed", func() {
			mockNetHandle := mocksRouter.NewMockNetHandle(c.mockCtrl)
			router := &Handle{
				Reporting: &reporting.NOOP{},
				netHandle: mockNetHandle,
			}
			c.mockBackendConfig.EXPECT().AccessToken().AnyTimes()
			router.Setup(gaDestinationDefinition, logger.NOP, conf, c.mockBackendConfig, c.mockRouterJobsDB, c.mockProcErrorsDB, transientsource.NewEmptyService(), rsources.NewNoOpService(), transformerFeaturesService.NewNoOpService(), destinationdebugger.NewNoOpService(), throttler.NewNoOpThrottlerFactory())
			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)
			router.transformer = mockTransformer

			router.reloadableConfig.noOfJobsToBatchInAWorker = config.SingleValueLoader(3)
			router.reloadableConfig.transformerProxy = config.SingleValueLoader(true)
			router.noOfWorkers = 1

			gaPayload := `{"body": {"XML": {}, "FORM": {}, "JSON": {}}, "type": "REST", "files": {}, "method": "POST", "params": {"t": "event", "v": "1", "an": "RudderAndroidClient", "av": "1.0", "ds": "android-sdk", "ea": "Demo Track", "ec": "Demo Category", "el": "Demo Label", "ni": 0, "qt": 59268380964, "ul": "en-US", "cid": "anon_id", "tid": "UA-185645846-1", "uip": "[::1]", "aiid": "com.rudderlabs.android.sdk"}, "userId": "anon_id", "headers": {}, "version": "1", "endpoint": "https://www.google-analytics.com/collect"}`
			parameters := fmt.Sprintf(`{"source_id": "%s", "destination_id": "%s", "message_id": "2f548e6d-60f6-44af-a1f4-62b3272445c3", "received_at": "2021-06-28T10:04:48.527+05:30", "transform_at": "router"}`, sourceIDEnabled, gaDestinationID) // skipcq: GO-R4002

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
				}).Return(nil).After(callAllJobs)

			mockTransformer.EXPECT().Transform("ROUTER_TRANSFORM", gomock.Any()).After(callAllJobs).Times(1).DoAndReturn(
				func(_ string, transformMessage *types.TransformMessageT) []types.DestinationJobT {
					assertRouterJobs(&transformMessage.Data[0], toRetryJobsList[0])
					assertRouterJobs(&transformMessage.Data[1], unprocessedJobsList[0])

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
							StatusCode: 200,
						},
					}
				})

			mockTransformer.EXPECT().ProxyRequest(gomock.Any(), gomock.Any()).Times(2).DoAndReturn(
				func(_ context.Context, proxyReqparams *transformer.ProxyRequestParams) transformer.ProxyRequestResponse {
					codes := make(map[int64]int)
					bodys := make(map[int64]string)
					dontBatchDirectives := make(map[int64]bool)
					for _, m := range proxyReqparams.ResponseData.Metadata {
						codes[m.JobID] = 200
						bodys[m.JobID] = `{"message": "some message"}`
						dontBatchDirectives[m.JobID] = false
					}
					return transformer.ProxyRequestResponse{
						ProxyRequestStatusCode:   200,
						ProxyRequestResponseBody: "OK",
						RespContentType:          "application/json",
						OAuthErrorCategory:       "",
						RespStatusCodes:          codes,
						RespBodys:                bodys,
						DontBatchDirectives:      dontBatchDirectives,
					}
				})
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
		It("all jobs should be marked failed when partial 5xx failure occurred", func() {
			mockNetHandle := mocksRouter.NewMockNetHandle(c.mockCtrl)
			router := &Handle{
				Reporting: &reporting.NOOP{},
				netHandle: mockNetHandle,
			}
			c.mockBackendConfig.EXPECT().AccessToken().AnyTimes()
			router.Setup(gaDestinationDefinition, logger.NOP, conf, c.mockBackendConfig, c.mockRouterJobsDB, c.mockProcErrorsDB, transientsource.NewEmptyService(), rsources.NewNoOpService(), transformerFeaturesService.NewNoOpService(), destinationdebugger.NewNoOpService(), throttler.NewNoOpThrottlerFactory())
			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)
			router.transformer = mockTransformer

			router.reloadableConfig.noOfJobsToBatchInAWorker = config.SingleValueLoader(3)
			router.reloadableConfig.transformerProxy = config.SingleValueLoader(true)
			router.noOfWorkers = 1

			gaPayload := `{"body": {"XML": {}, "FORM": {}, "JSON": {}}, "type": "REST", "files": {}, "method": "POST", "params": {"t": "event", "v": "1", "an": "RudderAndroidClient", "av": "1.0", "ds": "android-sdk", "ea": "Demo Track", "ec": "Demo Category", "el": "Demo Label", "ni": 0, "qt": 59268380964, "ul": "en-US", "cid": "anon_id", "tid": "UA-185645846-1", "uip": "[::1]", "aiid": "com.rudderlabs.android.sdk"}, "userId": "anon_id", "headers": {}, "version": "1", "endpoint": "https://www.google-analytics.com/collect"}`
			parameters := fmt.Sprintf(`{"source_id": "%s", "destination_id": "%s", "message_id": "2f548e6d-60f6-44af-a1f4-62b3272445c3", "received_at": "2021-06-28T10:04:48.527+05:30", "transform_at": "router"}`, sourceIDEnabled, gaDestinationID) // skipcq: GO-R4002

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
				}).Return(nil).After(callAllJobs)

			mockTransformer.EXPECT().Transform("ROUTER_TRANSFORM", gomock.Any()).After(callAllJobs).Times(1).DoAndReturn(
				func(_ string, transformMessage *types.TransformMessageT) []types.DestinationJobT {
					assertRouterJobs(&transformMessage.Data[0], toRetryJobsList[0])
					assertRouterJobs(&transformMessage.Data[1], unprocessedJobsList[0])

					return []types.DestinationJobT{
						{
							Message: []byte(`[{"message": "some transformed message1"},{"message": "some transformed message2"}]`),
							JobMetadataArray: []types.JobMetadataT{
								{
									UserID:      "u1",
									JobID:       2009,
									AttemptNum:  1,
									JobT:        toRetryJobsList[0],
									TransformAt: "router",
								},
								{
									UserID:      "u1",
									JobID:       2010,
									JobT:        unprocessedJobsList[0],
									TransformAt: "router",
								},
							},
							Batched:    true,
							Error:      `{"firstAttemptedAt": "2021-06-28T15:57:30.742+05:30"}`,
							StatusCode: 200,
						},
					}
				})

			mockTransformer.EXPECT().ProxyRequest(gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
				func(_ context.Context, proxyReqparams *transformer.ProxyRequestParams) transformer.ProxyRequestResponse {
					codes := make(map[int64]int)
					bodys := make(map[int64]string)
					dontBatchDirectives := make(map[int64]bool)
					codes[proxyReqparams.ResponseData.Metadata[0].JobID] = 200
					bodys[proxyReqparams.ResponseData.Metadata[0].JobID] = `{"message": "some message1"}`
					dontBatchDirectives[proxyReqparams.ResponseData.Metadata[0].JobID] = false
					codes[proxyReqparams.ResponseData.Metadata[1].JobID] = 500
					bodys[proxyReqparams.ResponseData.Metadata[1].JobID] = `{"message": "some message2"}`
					dontBatchDirectives[proxyReqparams.ResponseData.Metadata[1].JobID] = false

					return transformer.ProxyRequestResponse{
						ProxyRequestStatusCode:   200,
						ProxyRequestResponseBody: "OK",
						RespContentType:          "application/json",
						OAuthErrorCategory:       "",
						RespStatusCodes:          codes,
						RespBodys:                bodys,
						DontBatchDirectives:      dontBatchDirectives,
					}
				})
			done := make(chan struct{})

			c.mockRouterJobsDB.EXPECT().WithUpdateSafeTx(gomock.Any(), gomock.Any()).Times(1).Do(func(ctx context.Context, f func(tx jobsdb.UpdateSafeTx) error) {
				_ = f(jobsdb.EmptyUpdateSafeTx())
				close(done)
			}).Return(nil)
			c.mockRouterJobsDB.EXPECT().UpdateJobStatusInTx(gomock.Any(), gomock.Any(), gomock.Any(), []string{customVal["GA"]}, nil).Times(1).
				Do(func(ctx context.Context, _ interface{}, statuses []*jobsdb.JobStatusT, _, _ interface{}) {
					assertJobStatus(toRetryJobsList[0], statuses[0], jobsdb.Failed.State, "500", `{"content-type":"application/json","response": "{\"message\": \"some message1\"}","firstAttemptedAt": "2021-06-28T15:57:30.742+05:30"}`, 2)
					assertJobStatus(unprocessedJobsList[0], statuses[1], jobsdb.Waiting.State, "", `{"content-type":"application/json","response": "{\"message\": \"some message2\"}","firstAttemptedAt": "2021-06-28T15:57:30.742+05:30"}`, 0)
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
	})
})

func assertRouterJobs(routerJob *types.RouterJobT, job *jobsdb.JobT) {
	Expect(routerJob.JobMetadata.JobID).To(Equal(job.JobID))
	Expect(routerJob.JobMetadata.UserID).To(Equal(job.UserID))
	Expect(routerJob.Connection).To(Equal(backendconfig.Connection{
		Enabled:          true,
		SourceID:         sourceIDEnabled,
		DestinationID:    gaDestinationID,
		ProcessorEnabled: true,
		Config:           map[string]interface{}{"key": "value"},
	}))
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
			errorAt:                routerutils.ERROR_AT_DEL,
		},
		{
			caseName:               "[delivery] when deliveryAlert is to be skipped, proxy is disabled the alert should be false",
			skip:                   skipT{deliveryAlert: true},
			transformerProxy:       false,
			expectedAlertFlagValue: false,
			errorAt:                routerutils.ERROR_AT_DEL,
		},
		{
			caseName:               "[delivery] when deliveryAlert is not to be skipped, proxy is disabled the alert should be true",
			skip:                   skipT{},
			transformerProxy:       false,
			expectedAlertFlagValue: true,
			errorAt:                routerutils.ERROR_AT_DEL,
		},
		{
			caseName:               "[delivery] when deliveryAlert is to be skipped, proxy is enabled the alert should be false",
			skip:                   skipT{},
			transformerProxy:       true,
			expectedAlertFlagValue: false,
			errorAt:                routerutils.ERROR_AT_DEL,
		},
		// transformation cases
		{
			caseName:               "[transformation] when transformationAlert is to be skipped, the alert should be false",
			skip:                   skipT{transformationAlert: true},
			expectedAlertFlagValue: false,
			errorAt:                routerutils.ERROR_AT_TF,
		},
		{
			caseName:               "[transformation]when transformationAlert is not to be skipped, the alert should be true",
			skip:                   skipT{},
			expectedAlertFlagValue: true,
			errorAt:                routerutils.ERROR_AT_TF,
		},
		// Custom destination's delivery cases
		{
			caseName:               "[custom] when transformerProxy is enabled, the alert should be true",
			skip:                   skipT{},
			transformerProxy:       true,
			expectedAlertFlagValue: true,
			errorAt:                routerutils.ERROR_AT_CUST,
		},
		{
			caseName:               "[custom] when transformerProxy is disabled, the alert should be true",
			skip:                   skipT{},
			transformerProxy:       false,
			expectedAlertFlagValue: true,
			errorAt:                routerutils.ERROR_AT_CUST,
		},
		{
			caseName:               "[custom] when transformerProxy is enabled & deliveryAlert is to be skipped, the alert should be false",
			skip:                   skipT{deliveryAlert: true},
			transformerProxy:       true,
			expectedAlertFlagValue: true,
			errorAt:                routerutils.ERROR_AT_CUST,
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
					transformerProxy:                  config.SingleValueLoader(tc.transformerProxy),
					skipRtAbortAlertForDelivery:       config.SingleValueLoader(tc.skip.deliveryAlert),
					skipRtAbortAlertForTransformation: config.SingleValueLoader(tc.skip.transformationAlert),
				},
			},
		}
		t.Run(tc.caseName, func(testT *testing.T) {
			output := wrk.allowRouterAbortedAlert(tc.errorAt)
			assert.Equal(testT, tc.expectedAlertFlagValue, output)
		})
	}
}
