package router

import (
	"time"

	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	uuid "github.com/satori/go.uuid"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	mocksBackendConfig "github.com/rudderlabs/rudder-server/mocks/config/backend-config"
	mocksJobsDB "github.com/rudderlabs/rudder-server/mocks/jobsdb"
	mocksRouter "github.com/rudderlabs/rudder-server/mocks/router"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils"
	testutils "github.com/rudderlabs/rudder-server/utils/tests"
)

const (
	WriteKeyEnabled           = "enabled-write-key"
	WriteKeyDisabled          = "disabled-write-key"
	WriteKeyInvalid           = "invalid-write-key"
	WriteKeyEmpty             = ""
	SourceIDEnabled           = "enabled-source"
	SourceIDDisabled          = "disabled-source"
	TestRemoteAddressWithPort = "test.com:80"
	TestRemoteAddress         = "test.com"
	GADestinationID           = "gad1"
)

var testTimeout = 10 * time.Second

// This configuration is assumed by all router tests and, is returned on Subscribe of mocked backend config
var sampleBackendConfig = backendconfig.ConfigT{
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

var gaDestinationDefinition = backendconfig.DestinationDefinitionT{ID: GADestinationID, Name: "GA", DisplayName: "Google Analytics", Config: nil, ResponseRules: nil}

type context struct {
	asyncHelper     testutils.AsyncTestHelper
	dbReadBatchSize int

	mockCtrl          *gomock.Controller
	mockRouterJobsDB  *mocksJobsDB.MockJobsDB
	mockProcErrorsDB  *mocksJobsDB.MockJobsDB
	mockBackendConfig *mocksBackendConfig.MockBackendConfig
}

// Initiaze mocks and common expectations
func (c *context) Setup() {
	c.asyncHelper.Setup()
	c.mockCtrl = gomock.NewController(GinkgoT())
	c.mockRouterJobsDB = mocksJobsDB.NewMockJobsDB(c.mockCtrl)
	c.mockProcErrorsDB = mocksJobsDB.NewMockJobsDB(c.mockCtrl)
	c.mockBackendConfig = mocksBackendConfig.NewMockBackendConfig(c.mockCtrl)

	// During Setup, router subscribes to backend config
	c.mockBackendConfig.EXPECT().Subscribe(gomock.Any(), backendconfig.TopicBackendConfig).
		Do(func(channel chan utils.DataEvent, topic backendconfig.Topic) {
			// on Subscribe, emulate a backend configuration event
			go func() { channel <- utils.DataEvent{Data: sampleBackendConfig, Topic: string(topic)} }()
		}).
		Do(c.asyncHelper.ExpectAndNotifyCallbackWithName("process_config")).
		Return().Times(1)

	c.dbReadBatchSize = 10000
}

func (c *context) Finish() {
	c.asyncHelper.WaitWithTimeout(testTimeout)
	c.mockCtrl.Finish()
}

var (
	CustomVal     map[string]string = map[string]string{"GA": "GA"}
	emptyJobsList []*jobsdb.JobT
)

var _ = Describe("Router", func() {
	var c *context

	BeforeEach(func() {
		c = &context{}
		c.Setup()

		// setup static requirements of dependencies
		stats.Setup()
		RoutersManagerSetup()
	})

	AfterEach(func() {
		c.Finish()
	})

	Context("Initialization", func() {

		It("should initialize and recover after crash", func() {
			router := &HandleT{}

			c.mockRouterJobsDB.EXPECT().DeleteExecuting(jobsdb.GetQueryParamsT{CustomValFilters: []string{gaDestinationDefinition.Name}, Count: -1}).Times(1)

			router.Setup(c.mockBackendConfig, c.mockRouterJobsDB, c.mockProcErrorsDB, gaDestinationDefinition, nil)
		})
	})

	Context("normal operation - ga", func() {
		BeforeEach(func() {
			// crash recovery check
			c.mockRouterJobsDB.EXPECT().DeleteExecuting(jobsdb.GetQueryParamsT{CustomValFilters: []string{gaDestinationDefinition.Name}, Count: -1}).Times(1)
		})

		It("should send failed, unprocessed jobs to ga destination", func() {
			router := &HandleT{}

			router.Setup(c.mockBackendConfig, c.mockRouterJobsDB, c.mockProcErrorsDB, gaDestinationDefinition, nil)
			mockNetHandle := mocksRouter.NewMockNetHandleI(c.mockCtrl)
			router.netHandle = mockNetHandle

			gaPayload := `{"body": {"XML": {}, "FORM": {}, "JSON": {}}, "type": "REST", "files": {}, "method": "POST", "params": {"t": "event", "v": "1", "an": "RudderAndroidClient", "av": "1.0", "ds": "android-sdk", "ea": "Demo Track", "ec": "Demo Category", "el": "Demo Label", "ni": 0, "qt": 59268380964, "ul": "en-US", "cid": "anon_id", "tid": "UA-185645846-1", "uip": "[::1]", "aiid": "com.rudderlabs.android.sdk"}, "userId": "anon_id", "headers": {}, "version": "1", "endpoint": "https://www.google-analytics.com/collect"}`
			parameters := `{"source_id": "1fMCVYZboDlYlauh4GFsEo2JU77", "destination_id": "1pFUq4xLe1XKJ62UHRwZbLBH9Dd", "message_id": "2f548e6d-60f6-44af-a1f4-62b3272445c3", "received_at": "2021-06-28T10:04:48.527+05:30", "transform_at": "processor"}`

			var toRetryJobsList []*jobsdb.JobT = []*jobsdb.JobT{
				{
					UUID:         uuid.NewV4(),
					UserID:       "u1",
					JobID:        2009,
					CreatedAt:    time.Date(2020, 04, 28, 13, 26, 00, 00, time.UTC),
					ExpireAt:     time.Date(2020, 04, 28, 13, 26, 00, 00, time.UTC),
					CustomVal:    CustomVal["GA"],
					EventPayload: []byte(gaPayload),
					LastJobStatus: jobsdb.JobStatusT{
						AttemptNum: 1,
					},
					Parameters: []byte(parameters),
				},
			}

			var unprocessedJobsList []*jobsdb.JobT = []*jobsdb.JobT{
				{
					UUID:         uuid.NewV4(),
					UserID:       "u1",
					JobID:        2010,
					CreatedAt:    time.Date(2020, 04, 28, 13, 26, 00, 00, time.UTC),
					ExpireAt:     time.Date(2020, 04, 28, 13, 26, 00, 00, time.UTC),
					CustomVal:    CustomVal["GA"],
					EventPayload: []byte(gaPayload),
					LastJobStatus: jobsdb.JobStatusT{
						AttemptNum: 0,
					},
					Parameters: []byte(parameters),
				},
			}

			callRetry := c.mockRouterJobsDB.EXPECT().GetToRetry(jobsdb.GetQueryParamsT{CustomValFilters: []string{CustomVal["GA"]}, Count: c.dbReadBatchSize}).Return(toRetryJobsList).Times(1)
			callThrottled := c.mockRouterJobsDB.EXPECT().GetThrottled(jobsdb.GetQueryParamsT{CustomValFilters: []string{CustomVal["GA"]}, Count: c.dbReadBatchSize - len(toRetryJobsList)}).Return(emptyJobsList).Times(1).After(callRetry)
			callWaiting := c.mockRouterJobsDB.EXPECT().GetWaiting(jobsdb.GetQueryParamsT{CustomValFilters: []string{CustomVal["GA"]}, Count: c.dbReadBatchSize - len(toRetryJobsList)}).Return(emptyJobsList).Times(1).After(callThrottled)
			c.mockRouterJobsDB.EXPECT().GetUnprocessed(jobsdb.GetQueryParamsT{CustomValFilters: []string{CustomVal["GA"]}, Count: c.dbReadBatchSize - len(toRetryJobsList)}).Return(unprocessedJobsList).Times(1).After(callWaiting)

			c.mockRouterJobsDB.EXPECT().UpdateJobStatus(gomock.Any(), []string{CustomVal["GA"]}, nil).Times(1).
				Do(func(statuses []*jobsdb.JobStatusT, _ interface{}, _ interface{}) {
					// jobs should be sorted by jobid, so order of statuses is different than order of jobs
					assertJobStatus(toRetryJobsList[0], statuses[0], jobsdb.Executing.State, "", `{}`, 1)
					assertJobStatus(unprocessedJobsList[0], statuses[1], jobsdb.Executing.State, "", `{}`, 0)
				})

			count := router.readAndProcess()
			Expect(count).To(Equal(2))

			testutils.RunTestWithTimeout(func() {
				mockNetHandle.EXPECT().SendPost(gomock.Any).Times(1).Return(200, "")

				c.mockRouterJobsDB.EXPECT().UpdateJobStatus(gomock.Any(), []string{CustomVal["GA"]}, nil).Times(1).
					Do(func(statuses []*jobsdb.JobStatusT, _ interface{}, _ interface{}) {
						// jobs should be sorted by jobid, so order of statuses is different than order of jobs
						assertJobStatus(toRetryJobsList[0], statuses[0], jobsdb.Executing.State, "200", `{}`, 2)
						assertJobStatus(unprocessedJobsList[0], statuses[1], jobsdb.Executing.State, "200", `{}`, 1)
					})
			}, testTimeout)
		})
	})
})

func assertJobStatus(job *jobsdb.JobT, status *jobsdb.JobStatusT, expectedState string, errorCode string, errorResponse string, attemptNum int) {
	Expect(status.JobID).To(Equal(job.JobID))
	Expect(status.JobState).To(Equal(expectedState))
	Expect(status.ErrorCode).To(Equal(errorCode))
	Expect(status.ErrorResponse).To(MatchJSON(errorResponse))
	Expect(status.RetryTime).To(BeTemporally("~", time.Now(), 200*time.Millisecond))
	Expect(status.ExecTime).To(BeTemporally("~", time.Now(), 200*time.Millisecond))
	Expect(status.AttemptNum).To(Equal(attemptNum))
}
