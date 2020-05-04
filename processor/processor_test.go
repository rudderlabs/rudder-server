package processor

import (
	"time"

	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	uuid "github.com/satori/go.uuid"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils"
	"github.com/rudderlabs/rudder-server/utils/logger"

	mocksBackendConfig "github.com/rudderlabs/rudder-server/mocks/config/backend-config"
	mocksJobsDB "github.com/rudderlabs/rudder-server/mocks/jobsdb"
	mocksStats "github.com/rudderlabs/rudder-server/mocks/stats"

	testutils "github.com/rudderlabs/rudder-server/utils/tests"
)

var testTimeout = 5 * time.Second

type context struct {
	asyncHelper testutils.AsyncTestHelper

	mockCtrl              *gomock.Controller
	mockBackendConfig     *mocksBackendConfig.MockBackendConfig
	mockGatewayJobsDB     *mocksJobsDB.MockJobsDB
	mockRouterJobsDB      *mocksJobsDB.MockJobsDB
	mockBatchRouterJobsDB *mocksJobsDB.MockJobsDB
	mockStats             *mocksStats.MockStats

	mockStatGatewayDBRead         *mocksStats.MockRudderStats
	mockStatGatewayDBWrite        *mocksStats.MockRudderStats
	mockStatRouterDBWrite         *mocksStats.MockRudderStats
	mockStatBatchRouterDBWrite    *mocksStats.MockRudderStats
	mockStatActiveUsers           *mocksStats.MockRudderStats
	mockStatGatewayDBReadTime     *mocksStats.MockRudderStats
	mockStatGatewayDBWriteTime    *mocksStats.MockRudderStats
	mockStatLoopTime              *mocksStats.MockRudderStats
	mockStatSessionTransformTime  *mocksStats.MockRudderStats
	mockStatUserTransformTime     *mocksStats.MockRudderStats
	mockStatDestTransformTime     *mocksStats.MockRudderStats
	mockStatJobListSort           *mocksStats.MockRudderStats
	mockStatMarshalSingularEvents *mocksStats.MockRudderStats
	mockStatDestProcessing        *mocksStats.MockRudderStats
}

func (c *context) Setup() {
	c.mockCtrl = gomock.NewController(GinkgoT())
	c.mockBackendConfig = mocksBackendConfig.NewMockBackendConfig(c.mockCtrl)
	c.mockGatewayJobsDB = mocksJobsDB.NewMockJobsDB(c.mockCtrl)
	c.mockRouterJobsDB = mocksJobsDB.NewMockJobsDB(c.mockCtrl)
	c.mockBatchRouterJobsDB = mocksJobsDB.NewMockJobsDB(c.mockCtrl)
	c.mockStats = mocksStats.NewMockStats(c.mockCtrl)

	c.mockBackendConfig.EXPECT().Subscribe(gomock.Any(), backendconfig.TopicProcessConfig).
		Do(func(channel chan utils.DataEvent, topic backendconfig.Topic) {
			// on Subscribe, emulate a backend configuration event
			go func() {
				channel <- utils.DataEvent{Data: sampleBackendConfig, Topic: string(topic)}
			}()
		}).
		Do(c.asyncHelper.ExpectAndNotifyCallback()).
		Return().Times(1)

	registerStatMocks := func(name string, statType string) *mocksStats.MockRudderStats {
		stat := mocksStats.NewMockRudderStats(c.mockCtrl)
		c.mockStats.EXPECT().NewStat(name, statType).Return(stat).Times(1)
		return stat
	}

	registerDestStatMocks := func(destID string) *DestStatT {
		numEvents := mocksStats.NewMockRudderStats(c.mockCtrl)
		numOutputEvents := mocksStats.NewMockRudderStats(c.mockCtrl)
		sessionTransform := mocksStats.NewMockRudderStats(c.mockCtrl)
		userTransform := mocksStats.NewMockRudderStats(c.mockCtrl)
		destTransform := mocksStats.NewMockRudderStats(c.mockCtrl)

		// These are registered asynchronously when a new backend config is received
		c.asyncHelper.RegisterCalls(
			c.mockStats.EXPECT().NewDestStat("proc_num_events", stats.CountType, destID).Return(numEvents).Times(1),
			c.mockStats.EXPECT().NewDestStat("proc_num_output_events", stats.CountType, destID).Return(numOutputEvents).Times(1),
			c.mockStats.EXPECT().NewDestStat("proc_session_transform", stats.TimerType, destID).Return(sessionTransform).Times(1),
			c.mockStats.EXPECT().NewDestStat("proc_user_transform", stats.TimerType, destID).Return(userTransform).Times(1),
			c.mockStats.EXPECT().NewDestStat("proc_dest_transform", stats.TimerType, destID).Return(destTransform).Times(1),
		)

		return &DestStatT{
			id:               destID,
			numEvents:        numEvents,
			numOutputEvents:  numOutputEvents,
			sessionTransform: sessionTransform,
			userTransform:    userTransform,
			destTransform:    destTransform,
		}
	}

	c.mockStatGatewayDBRead = registerStatMocks("processor.gateway_db_read", stats.CountType)
	c.mockStatGatewayDBWrite = registerStatMocks("processor.gateway_db_write", stats.CountType)
	c.mockStatRouterDBWrite = registerStatMocks("processor.router_db_write", stats.CountType)
	c.mockStatBatchRouterDBWrite = registerStatMocks("processor.batch_router_db_write", stats.CountType)
	c.mockStatActiveUsers = registerStatMocks("processor.active_users", stats.GaugeType)
	c.mockStatGatewayDBReadTime = registerStatMocks("processor.gateway_db_read_time", stats.TimerType)
	c.mockStatGatewayDBWriteTime = registerStatMocks("processor.gateway_db_write_time", stats.TimerType)
	c.mockStatLoopTime = registerStatMocks("processor.loop_time", stats.TimerType)
	c.mockStatSessionTransformTime = registerStatMocks("processor.session_transform_time", stats.TimerType)
	c.mockStatUserTransformTime = registerStatMocks("processor.user_transform_time", stats.TimerType)
	c.mockStatDestTransformTime = registerStatMocks("processor.dest_transform_time", stats.TimerType)
	c.mockStatJobListSort = registerStatMocks("processor.job_list_sort", stats.TimerType)
	c.mockStatMarshalSingularEvents = registerStatMocks("processor.marshal_singular_events", stats.TimerType)
	c.mockStatDestProcessing = registerStatMocks("processor.dest_processing", stats.TimerType)

	registerDestStatMocks(DestinationIDEnabled)
	registerDestStatMocks(DestinationIDDisabled)
	registerDestStatMocks(DestinationIDDisabledProcessor)
}

func (c *context) Finish() {
	c.asyncHelper.WaitWithTimeout(testTimeout)
	c.mockCtrl.Finish()
}

const (
	WriteKeyEnabled                = "enabled-write-key"
	WriteKeyDisabled               = "disabled-write-key"
	SourceIDEnabled                = "enabled-source"
	SourceIDDisabled               = "disabled-source"
	DestinationIDEnabled           = "enabled-destination"
	DestinationIDDisabled          = "disabled-destination"
	DestinationIDDisabledProcessor = "disabled-processor-destination"
)

var (
	gatewayCustomVal []string = []string{"GW"}
	emptyJobsList    []*jobsdb.JobT
)

// This configuration is assumed by all processor tests and, is returned on Subscribe of mocked backend config
var sampleBackendConfig = backendconfig.SourcesT{
	Sources: []backendconfig.SourceT{
		{
			ID:       SourceIDDisabled,
			WriteKey: WriteKeyDisabled,
			Enabled:  false,
		},
		{
			ID:       SourceIDEnabled,
			WriteKey: WriteKeyEnabled,
			Enabled:  true,
			Destinations: []backendconfig.DestinationT{
				{
					ID: DestinationIDEnabled,
				},
				{
					ID: DestinationIDDisabled,
				},
				{
					ID: DestinationIDDisabledProcessor,
				},
			},
		},
	},
}

var _ = Describe("Processor", func() {
	var c *context

	BeforeEach(func() {
		c = &context{}
		c.Setup()

		// setup static requirements of dependencies
		logger.Setup()
		stats.Setup()
	})

	AfterEach(func() {
		c.Finish()
	})

	Context("Initialization", func() {
		It("should initialize (no jobs to recover)", func() {
			var processor *HandleT = &HandleT{}

			// crash recover returns empty list
			c.mockGatewayJobsDB.EXPECT().GetExecuting(gatewayCustomVal, 10000, nil).Return(emptyJobsList).Times(1)

			processor.Setup(c.mockBackendConfig, c.mockGatewayJobsDB, c.mockRouterJobsDB, c.mockBatchRouterJobsDB, c.mockStats)
		})

		It("should recover after crash", func() {
			var processor *HandleT = &HandleT{}

			assertJobStatus := func(job *jobsdb.JobT, status *jobsdb.JobStatusT) {
				Expect(status.JobID).To(Equal(job.JobID))
				Expect(status.JobState).To(Equal(jobsdb.FailedState))
				Expect(status.ErrorCode).To(Equal(""))
				Expect(status.ErrorResponse).To(MatchJSON(`{}`))
				Expect(status.RetryTime).To(BeTemporally("~", time.Now(), 10*time.Millisecond))
				Expect(status.ExecTime).To(BeTemporally("~", time.Now(), 10*time.Millisecond))
				Expect(status.AttemptNum).To(Equal(job.LastJobStatus.AttemptNum + 1))
			}

			var executingJobsList []*jobsdb.JobT = []*jobsdb.JobT{
				{
					UUID:         uuid.NewV4(),
					JobID:        1010,
					CreatedAt:    time.Date(2020, 04, 28, 13, 26, 00, 00, time.UTC),
					ExpireAt:     time.Date(2020, 04, 28, 13, 26, 00, 00, time.UTC),
					CustomVal:    gatewayCustomVal[0],
					EventPayload: nil,
					LastJobStatus: jobsdb.JobStatusT{
						AttemptNum: 1,
					},
					Parameters: nil,
				},
				{
					UUID:          uuid.NewV4(),
					JobID:         1002,
					CreatedAt:     time.Date(2020, 04, 28, 13, 27, 00, 00, time.UTC),
					ExpireAt:      time.Date(2020, 04, 28, 13, 27, 00, 00, time.UTC),
					CustomVal:     gatewayCustomVal[0],
					EventPayload:  nil,
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    nil,
				},
				{
					UUID:          uuid.NewV4(),
					JobID:         1003,
					CreatedAt:     time.Date(2020, 04, 28, 13, 28, 00, 00, time.UTC),
					ExpireAt:      time.Date(2020, 04, 28, 13, 28, 00, 00, time.UTC),
					CustomVal:     gatewayCustomVal[0],
					EventPayload:  nil,
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    nil,
				},
			}

			// GetExecuting is called in a loop until all executing jobs have been updated. Each executing job should be updated to failed status
			var executingCall1 = c.mockGatewayJobsDB.EXPECT().GetExecuting(gatewayCustomVal, 10000, nil).Return(executingJobsList[0:2]).Times(1)
			var updateCall1 = c.mockGatewayJobsDB.EXPECT().UpdateJobStatus(gomock.Len(2), gatewayCustomVal, nil).After(executingCall1).Times(1).
				Do(func(statuses []*jobsdb.JobStatusT, _ interface{}, _ interface{}) {
					assertJobStatus(executingJobsList[0], statuses[0])
					assertJobStatus(executingJobsList[1], statuses[1])
				})

			// second loop iteration
			var executingCall2 = c.mockGatewayJobsDB.EXPECT().GetExecuting(gatewayCustomVal, 10000, nil).Return(emptyJobsList).After(updateCall1).Return(executingJobsList[2:]).Times(1)
			var updateCall2 = c.mockGatewayJobsDB.EXPECT().UpdateJobStatus(gomock.Len(1), gatewayCustomVal, nil).After(executingCall2).Times(1).
				Do(func(statuses []*jobsdb.JobStatusT, _ interface{}, _ interface{}) {
					assertJobStatus(executingJobsList[2], statuses[0])
				})

			c.mockGatewayJobsDB.EXPECT().GetExecuting(gatewayCustomVal, 10000, nil).After(updateCall2).Return(emptyJobsList).Times(1) // returning empty job list should end crash recover loop

			processor.Setup(c.mockBackendConfig, c.mockGatewayJobsDB, c.mockRouterJobsDB, c.mockBatchRouterJobsDB, c.mockStats)
		})
	})

	Context("normal operation", func() {
		BeforeEach(func() {
			// crash recovery check
			c.mockGatewayJobsDB.EXPECT().GetExecuting(gatewayCustomVal, 10000, nil).Return(emptyJobsList).Times(1)
		})

		It("should only send proper stats, if not pending jobs are returned", func() {
			var processor *HandleT = &HandleT{}
			processor.Setup(c.mockBackendConfig, c.mockGatewayJobsDB, c.mockRouterJobsDB, c.mockBatchRouterJobsDB, c.mockStats)

			callLoopTime := c.mockStatLoopTime.EXPECT().Start().Times(1)
			callDBRTime := c.mockStatGatewayDBReadTime.EXPECT().Start().Times(1).After(callLoopTime)

			callRetry := c.mockGatewayJobsDB.EXPECT().GetToRetry(gatewayCustomVal, 10000, nil).Return(emptyJobsList).Times(1).After(callDBRTime)
			callUnprocessed := c.mockGatewayJobsDB.EXPECT().GetUnprocessed(gatewayCustomVal, 10000, nil).Return(emptyJobsList).Times(1).After(callRetry)

			c.mockStatGatewayDBReadTime.EXPECT().End().Times(1).After(callUnprocessed)

			var didWork = processor.handlePendingGatewayJobs()
			Expect(didWork).To(Equal(false))
		})

		It("should process ToRetry and Unprocessed jobs", func() {
			var processor *HandleT = &HandleT{}
			processor.Setup(c.mockBackendConfig, c.mockGatewayJobsDB, c.mockRouterJobsDB, c.mockBatchRouterJobsDB, c.mockStats)

			var toRetryJobsList []*jobsdb.JobT = []*jobsdb.JobT{
				{
					UUID:         uuid.NewV4(),
					JobID:        2010,
					CreatedAt:    time.Date(2020, 04, 28, 13, 26, 00, 00, time.UTC),
					ExpireAt:     time.Date(2020, 04, 28, 13, 26, 00, 00, time.UTC),
					CustomVal:    gatewayCustomVal[0],
					EventPayload: nil,
					LastJobStatus: jobsdb.JobStatusT{
						AttemptNum: 1,
					},
					Parameters: nil,
				},
				{
					UUID:          uuid.NewV4(),
					JobID:         2002,
					CreatedAt:     time.Date(2020, 04, 28, 13, 27, 00, 00, time.UTC),
					ExpireAt:      time.Date(2020, 04, 28, 13, 27, 00, 00, time.UTC),
					CustomVal:     gatewayCustomVal[0],
					EventPayload:  nil,
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    nil,
				},
				{
					UUID:          uuid.NewV4(),
					JobID:         2003,
					CreatedAt:     time.Date(2020, 04, 28, 13, 28, 00, 00, time.UTC),
					ExpireAt:      time.Date(2020, 04, 28, 13, 28, 00, 00, time.UTC),
					CustomVal:     gatewayCustomVal[0],
					EventPayload:  nil,
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    nil,
				},
			}

			var unprocessedJobsList []*jobsdb.JobT = []*jobsdb.JobT{
				{
					UUID:          uuid.NewV4(),
					JobID:         1010,
					CreatedAt:     time.Date(2020, 04, 28, 23, 26, 00, 00, time.UTC),
					ExpireAt:      time.Date(2020, 04, 28, 23, 26, 00, 00, time.UTC),
					CustomVal:     gatewayCustomVal[0],
					EventPayload:  nil,
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    nil,
				},
				{
					UUID:          uuid.NewV4(),
					JobID:         1002,
					CreatedAt:     time.Date(2020, 04, 28, 23, 27, 00, 00, time.UTC),
					ExpireAt:      time.Date(2020, 04, 28, 23, 27, 00, 00, time.UTC),
					CustomVal:     gatewayCustomVal[0],
					EventPayload:  nil,
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    nil,
				},
			}

			callLoopTime := c.mockStatLoopTime.EXPECT().Start().Times(1)
			callDBRTime := c.mockStatGatewayDBReadTime.EXPECT().Start().Times(1).After(callLoopTime)

			callRetry := c.mockGatewayJobsDB.EXPECT().GetToRetry(gatewayCustomVal, 10000, nil).Return(toRetryJobsList).Times(1).After(callDBRTime)
			callUnprocessed := c.mockGatewayJobsDB.EXPECT().GetUnprocessed(gatewayCustomVal, 10000-len(toRetryJobsList), nil).Return(unprocessedJobsList).Times(1).After(callRetry)

			callDBRTimeEnd := c.mockStatGatewayDBReadTime.EXPECT().End().Times(1).After(callUnprocessed)

			// process jobs
			assertJobStatus := func(job *jobsdb.JobT, status *jobsdb.JobStatusT) {
				Expect(status.JobID).To(Equal(job.JobID))
				Expect(status.JobState).To(Equal(jobsdb.SucceededState))
				Expect(status.ErrorCode).To(Equal("200"))
				Expect(status.ErrorResponse).To(MatchJSON(`{"success":"OK"}`))
				Expect(status.RetryTime).To(BeTemporally("~", time.Now(), 10*time.Millisecond))
				Expect(status.ExecTime).To(BeTemporally("~", time.Now(), 10*time.Millisecond))

				// Attempt num will be overwritten
				Expect(status.AttemptNum).To(Equal(1))
			}

			callListSort := c.mockStatJobListSort.EXPECT().Start().Times(1).After(callDBRTimeEnd)
			c.mockStatGatewayDBRead.EXPECT().Count(len(toRetryJobsList) + len(unprocessedJobsList)).Times(1).After(callListSort)

			callListSortEnd := c.mockStatJobListSort.EXPECT().End().Times(1).After(callListSort)

			callMarshallSingularEvents := c.mockStatMarshalSingularEvents.EXPECT().Start().Times(1).After(callListSortEnd)
			callMarshallSingularEventsEnd := c.mockStatMarshalSingularEvents.EXPECT().End().Times(1).After(callMarshallSingularEvents)
			callDestProcessing := c.mockStatDestProcessing.EXPECT().Start().Times(1).After(callMarshallSingularEventsEnd)
			callDestProcessingEnd := c.mockStatDestProcessing.EXPECT().End().Times(1).After(callDestProcessing)
			callDBWrite := c.mockStatGatewayDBWriteTime.EXPECT().Start().Times(1).After(callDestProcessingEnd)

			callUpdateJobs := c.mockGatewayJobsDB.EXPECT().UpdateJobStatus(gomock.Len(len(toRetryJobsList)+len(unprocessedJobsList)), gatewayCustomVal, nil).Times(1).After(callDBWrite).
				Do(func(statuses []*jobsdb.JobStatusT, _ interface{}, _ interface{}) {
					// jobs should be sorted by jobid, so order of statuses is different than order of jobs
					assertJobStatus(unprocessedJobsList[1], statuses[0]) // id 1002
					assertJobStatus(unprocessedJobsList[0], statuses[1]) // id 1010
					assertJobStatus(toRetryJobsList[1], statuses[2])     // id 2002
					assertJobStatus(toRetryJobsList[2], statuses[3])     // id 2003
					assertJobStatus(toRetryJobsList[0], statuses[4])     // id 2010
				})

			c.mockStatGatewayDBWriteTime.EXPECT().End().Times(1).After(callUpdateJobs)
			c.mockStatGatewayDBWrite.EXPECT().Count(len(toRetryJobsList) + len(unprocessedJobsList)).Times(1)
			c.mockStatRouterDBWrite.EXPECT().Count(0).Times(1)
			c.mockStatBatchRouterDBWrite.EXPECT().Count(0).Times(1)

			c.mockStatLoopTime.EXPECT().End().Times(1)

			didWork := processor.handlePendingGatewayJobs()
			Expect(didWork).To(Equal(true))
		})
	})
})
