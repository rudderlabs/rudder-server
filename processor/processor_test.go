package processor_test

import (
	"time"

	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	uuid "github.com/satori/go.uuid"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	. "github.com/rudderlabs/rudder-server/processor"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils"
	"github.com/rudderlabs/rudder-server/utils/logger"

	mocksBackendConfig "github.com/rudderlabs/rudder-server/mocks/config/backend-config"
	mocksJobsDB "github.com/rudderlabs/rudder-server/mocks/jobsdb"

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
}

const (
	WriteKeyEnabled   = "enabled-write-key"
	WriteKeyDisabled  = "disabled-write-key"
	WriteKeyInvalid   = "invalid-write-key"
	WriteKeyEmpty     = ""
	SourceIDEnabled   = "enabled-source"
	SourceIDDisabled  = "disabled-source"
	TestRemoteAddress = "test.com"
)

var (
	gatewayCustomVal []string = []string{"GW"}
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
		},
	},
}

func (c *context) Setup() {
	c.mockCtrl = gomock.NewController(GinkgoT())
	c.mockBackendConfig = mocksBackendConfig.NewMockBackendConfig(c.mockCtrl)
	c.mockGatewayJobsDB = mocksJobsDB.NewMockJobsDB(c.mockCtrl)
	c.mockRouterJobsDB = mocksJobsDB.NewMockJobsDB(c.mockCtrl)
	c.mockBatchRouterJobsDB = mocksJobsDB.NewMockJobsDB(c.mockCtrl)

	c.mockBackendConfig.EXPECT().Subscribe(gomock.Any(), backendconfig.TopicProcessConfig).
		Do(func(channel chan utils.DataEvent, topic string) {
			// on Subscribe, emulate a backend configuration event
			go func() { channel <- utils.DataEvent{Data: sampleBackendConfig, Topic: topic} }()
		}).
		Do(c.asyncHelper.ExpectAndNotifyCallback()).
		Return().Times(1)
}

func (c *context) Finish() {
	c.asyncHelper.WaitWithTimeout(testTimeout)
	c.mockCtrl.Finish()
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
			var emptyJobsList []*jobsdb.JobT
			c.mockGatewayJobsDB.EXPECT().GetExecuting(gatewayCustomVal, 10000, nil).Return(emptyJobsList).Times(1)
			c.mockGatewayJobsDB.EXPECT().GetToRetry(gatewayCustomVal, 10000, nil).Return(emptyJobsList).MinTimes(1).
				Do(c.asyncHelper.ExpectAndNotifyCallback())
			c.mockGatewayJobsDB.EXPECT().GetUnprocessed(gatewayCustomVal, 10000, nil).Return(emptyJobsList).MinTimes(1).
				Do(c.asyncHelper.ExpectAndNotifyCallback())

			processor.Setup(c.mockBackendConfig, c.mockGatewayJobsDB, c.mockRouterJobsDB, c.mockBatchRouterJobsDB)
			Expect(true).To(BeTrue())
		})

		It("should recover after crash", func() {
			var processor *HandleT = &HandleT{}
			var emptyJobsList []*jobsdb.JobT

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
					JobID:        1001,
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

			// ignore the rest of the calls, as they are tested later
			c.mockGatewayJobsDB.EXPECT().GetToRetry(gatewayCustomVal, 10000, nil).Return(emptyJobsList).MinTimes(1).
				Do(c.asyncHelper.ExpectAndNotifyCallback())
			c.mockGatewayJobsDB.EXPECT().GetUnprocessed(gatewayCustomVal, 10000, nil).Return(emptyJobsList).MinTimes(1).
				Do(c.asyncHelper.ExpectAndNotifyCallback())

			processor.Setup(c.mockBackendConfig, c.mockGatewayJobsDB, c.mockRouterJobsDB, c.mockBatchRouterJobsDB)
		})
	})
})
