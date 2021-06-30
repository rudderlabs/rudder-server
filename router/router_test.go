package router

import (
	"encoding/json"
	"fmt"
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
	GADestinationDefinitionID = "gaid1"
	GADestinationID           = "did1"
)

var testTimeout = 10 * time.Second

var gaDestinationDefinition = backendconfig.DestinationDefinitionT{ID: GADestinationDefinitionID, Name: "GA", DisplayName: "Google Analytics", Config: nil, ResponseRules: nil}

// This configuration is assumed by all router tests and, is returned on Subscribe of mocked backend config
var sampleBackendConfig = backendconfig.ConfigT{
	Sources: []backendconfig.SourceT{
		{
			ID:           SourceIDEnabled,
			WriteKey:     WriteKeyEnabled,
			Enabled:      true,
			Destinations: []backendconfig.DestinationT{backendconfig.DestinationT{ID: GADestinationID, Name: "ga dest", DestinationDefinition: gaDestinationDefinition, Enabled: true, IsProcessorEnabled: true}},
		},
	},
}

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
		Do(c.asyncHelper.ExpectAndNotifyCallbackWithName("backend_config")).
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
			maxStatusUpdateWait = 2 * time.Second

			// crash recovery check
			c.mockRouterJobsDB.EXPECT().DeleteExecuting(jobsdb.GetQueryParamsT{CustomValFilters: []string{gaDestinationDefinition.Name}, Count: -1}).Times(1)
		})

		It("should send failed, unprocessed jobs to ga destination", func() {
			router := &HandleT{}

			router.Setup(c.mockBackendConfig, c.mockRouterJobsDB, c.mockProcErrorsDB, gaDestinationDefinition, nil)
			mockNetHandle := mocksRouter.NewMockNetHandleI(c.mockCtrl)
			router.netHandle = mockNetHandle

			gaPayload := `{"body": {"XML": {}, "FORM": {}, "JSON": {}}, "type": "REST", "files": {}, "method": "POST", "params": {"t": "event", "v": "1", "an": "RudderAndroidClient", "av": "1.0", "ds": "android-sdk", "ea": "Demo Track", "ec": "Demo Category", "el": "Demo Label", "ni": 0, "qt": 59268380964, "ul": "en-US", "cid": "anon_id", "tid": "UA-185645846-1", "uip": "[::1]", "aiid": "com.rudderlabs.android.sdk"}, "userId": "anon_id", "headers": {}, "version": "1", "endpoint": "https://www.google-analytics.com/collect"}`
			parameters := fmt.Sprintf(`{"source_id": "1fMCVYZboDlYlauh4GFsEo2JU77", "destination_id": "%s", "message_id": "2f548e6d-60f6-44af-a1f4-62b3272445c3", "received_at": "2021-06-28T10:04:48.527+05:30", "transform_at": "processor"}`, GADestinationID)

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
						AttemptNum:    1,
						ErrorResponse: []byte(`{"firstAttemptedAt": "2021-06-28T15:57:30.742+05:30"}`),
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
					assertJobStatus(toRetryJobsList[0], statuses[0], jobsdb.Executing.State, "", `{}`, 1)
					assertJobStatus(unprocessedJobsList[0], statuses[1], jobsdb.Executing.State, "", `{}`, 0)
				}).Return(nil)

			mockNetHandle.EXPECT().SendPost(gomock.Any()).Times(2).Return(200, "")

			callBeginTransaction := c.mockRouterJobsDB.EXPECT().BeginGlobalTransaction().Times(1).Return(nil)
			callAcquireLocks := c.mockRouterJobsDB.EXPECT().AcquireUpdateJobStatusLocks().Times(1).After(callBeginTransaction)
			callUpdateStatus := c.mockRouterJobsDB.EXPECT().UpdateJobStatusInTxn(gomock.Any(), gomock.Any(), []string{CustomVal["GA"]}, nil).Times(1).After(callAcquireLocks).
				Do(func(_ interface{}, statuses []*jobsdb.JobStatusT, _ interface{}, _ interface{}) {
					assertJobStatus(toRetryJobsList[0], statuses[0], jobsdb.Succeeded.State, "200", `{"firstAttemptedAt": "2021-06-28T15:57:30.742+05:30"}`, 2)
					assertJobStatus(unprocessedJobsList[0], statuses[1], jobsdb.Succeeded.State, "200", `{"firstAttemptedAt": "2021-06-28T15:57:30.742+05:30"}`, 1)
				})
			callCommitTransaction := c.mockRouterJobsDB.EXPECT().CommitTransaction(gomock.Any()).Times(1).After(callUpdateStatus)
			c.mockRouterJobsDB.EXPECT().ReleaseUpdateJobStatusLocks().Times(1).After(callCommitTransaction)

			<-router.backendConfigInitialized
			count := router.readAndProcess()
			Expect(count).To(Equal(2))

			time.Sleep(3 * time.Second)
		})

		It("should abort unprocessed jobs to ga destination because of bad payload", func() {
			router := &HandleT{}

			router.Setup(c.mockBackendConfig, c.mockRouterJobsDB, c.mockProcErrorsDB, gaDestinationDefinition, nil)
			mockNetHandle := mocksRouter.NewMockNetHandleI(c.mockCtrl)
			router.netHandle = mockNetHandle

			gaPayload := `{}`
			parameters := fmt.Sprintf(`{"source_id": "1fMCVYZboDlYlauh4GFsEo2JU77", "destination_id": "%s", "message_id": "2f548e6d-60f6-44af-a1f4-62b3272445c3", "received_at": "2021-06-28T10:04:48.527+05:30", "transform_at": "processor"}`, GADestinationID)

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

			callRetry := c.mockRouterJobsDB.EXPECT().GetToRetry(jobsdb.GetQueryParamsT{CustomValFilters: []string{CustomVal["GA"]}, Count: c.dbReadBatchSize}).Return(emptyJobsList).Times(1)
			callThrottled := c.mockRouterJobsDB.EXPECT().GetThrottled(jobsdb.GetQueryParamsT{CustomValFilters: []string{CustomVal["GA"]}, Count: c.dbReadBatchSize}).Return(emptyJobsList).Times(1).After(callRetry)
			callWaiting := c.mockRouterJobsDB.EXPECT().GetWaiting(jobsdb.GetQueryParamsT{CustomValFilters: []string{CustomVal["GA"]}, Count: c.dbReadBatchSize}).Return(emptyJobsList).Times(1).After(callThrottled)
			c.mockRouterJobsDB.EXPECT().GetUnprocessed(jobsdb.GetQueryParamsT{CustomValFilters: []string{CustomVal["GA"]}, Count: c.dbReadBatchSize}).Return(unprocessedJobsList).Times(1).After(callWaiting)

			c.mockRouterJobsDB.EXPECT().UpdateJobStatus(gomock.Any(), []string{CustomVal["GA"]}, nil).Times(1).
				Do(func(statuses []*jobsdb.JobStatusT, _ interface{}, _ interface{}) {
					assertJobStatus(unprocessedJobsList[0], statuses[0], jobsdb.Executing.State, "", `{}`, 0)
				})

			mockNetHandle.EXPECT().SendPost(gomock.Any()).Times(1).Return(400, "")

			c.mockProcErrorsDB.EXPECT().Store(gomock.Any()).Times(1).
				Do(func(jobList []*jobsdb.JobT) {
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

			callBeginTransaction := c.mockRouterJobsDB.EXPECT().BeginGlobalTransaction().Times(1).Return(nil)
			callAcquireLocks := c.mockRouterJobsDB.EXPECT().AcquireUpdateJobStatusLocks().Times(1).After(callBeginTransaction)
			callUpdateStatus := c.mockRouterJobsDB.EXPECT().UpdateJobStatusInTxn(gomock.Any(), gomock.Any(), []string{CustomVal["GA"]}, nil).Times(1).After(callAcquireLocks).
				Do(func(_ interface{}, statuses []*jobsdb.JobStatusT, _ interface{}, _ interface{}) {
					assertJobStatus(unprocessedJobsList[0], statuses[0], jobsdb.Aborted.State, "400", `{"firstAttemptedAt": "2021-06-28T15:57:30.742+05:30"}`, 1)
				})
			callCommitTransaction := c.mockRouterJobsDB.EXPECT().CommitTransaction(gomock.Any()).Times(1).After(callUpdateStatus)
			c.mockRouterJobsDB.EXPECT().ReleaseUpdateJobStatusLocks().Times(1).After(callCommitTransaction)

			<-router.backendConfigInitialized
			count := router.readAndProcess()
			Expect(count).To(Equal(1))

			time.Sleep(3 * time.Second)
		})
	})
})

func assertJobStatus(job *jobsdb.JobT, status *jobsdb.JobStatusT, expectedState string, errorCode string, errorResponse string, attemptNum int) {
	Expect(status.JobID).To(Equal(job.JobID))
	Expect(status.JobState).To(Equal(expectedState))
	Expect(status.ErrorCode).To(Equal(errorCode))
	if attemptNum > 1 {
		Expect(status.ErrorResponse).To(MatchJSON(errorResponse))
	}
	Expect(status.RetryTime).To(BeTemporally("~", time.Now(), 10*time.Second))
	Expect(status.ExecTime).To(BeTemporally("~", time.Now(), 10*time.Second))
	Expect(status.AttemptNum).To(Equal(attemptNum))
}
