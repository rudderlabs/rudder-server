package batchrouter

import (
	"fmt"
	"time"

	uuid "github.com/gofrs/uuid"
	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/rudderlabs/rudder-server/admin"
	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	mocksBackendConfig "github.com/rudderlabs/rudder-server/mocks/config/backend-config"
	mocksJobsDB "github.com/rudderlabs/rudder-server/mocks/jobsdb"
	mocksFileManager "github.com/rudderlabs/rudder-server/mocks/services/filemanager"
	mocksMultitenant "github.com/rudderlabs/rudder-server/mocks/services/multitenant"
	router_utils "github.com/rudderlabs/rudder-server/router/utils"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/services/transientsource"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
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
	S3DestinationDefinitionID = "s3id1"
	S3DestinationID           = "did1"
)

var testTimeout = 10 * time.Second

var s3DestinationDefinition = backendconfig.DestinationDefinitionT{ID: S3DestinationDefinitionID, Name: "S3", DisplayName: "S3", Config: nil, ResponseRules: nil}

// This configuration is assumed by all router tests and, is returned on Subscribe of mocked backend config
var sampleBackendConfig = backendconfig.ConfigT{
	Sources: []backendconfig.SourceT{
		{
			ID:           SourceIDEnabled,
			WriteKey:     WriteKeyEnabled,
			Enabled:      true,
			Destinations: []backendconfig.DestinationT{{ID: S3DestinationID, Name: "s3 dest", DestinationDefinition: s3DestinationDefinition, Enabled: true, IsProcessorEnabled: true}},
		},
	},
}

var sampleConfigPrefix = "config_prefix"
var sampleFileObjects = []*filemanager.FileObject{
	{
		Key:          fmt.Sprintf("%s/%s/%s/%s/%s", sampleConfigPrefix, SourceIDEnabled, WriteKeyEnabled, "01-02-2006", "tmp1.log"),
		LastModified: time.Now(),
	},
	{
		Key:          fmt.Sprintf("%s/%s/%s/%s/%s", sampleConfigPrefix, SourceIDEnabled, WriteKeyEnabled, "2006-01-02", "tmp2.log"),
		LastModified: time.Now(),
	},
}

type testContext struct {
	asyncHelper       testutils.AsyncTestHelper
	jobQueryBatchSize int

	mockCtrl               *gomock.Controller
	mockBatchRouterJobsDB  *mocksJobsDB.MockJobsDB
	mockProcErrorsDB       *mocksJobsDB.MockJobsDB
	mockBackendConfig      *mocksBackendConfig.MockBackendConfig
	mockFileManagerFactory *mocksFileManager.MockFileManagerFactory
	mockFileManager        *mocksFileManager.MockFileManager
	mockConfigPrefix       string
	mockFileObjects        []*filemanager.FileObject
	mockMultitenantI       *mocksMultitenant.MockMultiTenantI
}

// Initiaze mocks and common expectations
func (c *testContext) Setup() {
	c.asyncHelper.Setup()
	c.mockCtrl = gomock.NewController(GinkgoT())
	c.mockBatchRouterJobsDB = mocksJobsDB.NewMockJobsDB(c.mockCtrl)
	c.mockProcErrorsDB = mocksJobsDB.NewMockJobsDB(c.mockCtrl)
	c.mockBackendConfig = mocksBackendConfig.NewMockBackendConfig(c.mockCtrl)
	c.mockFileManagerFactory = mocksFileManager.NewMockFileManagerFactory(c.mockCtrl)
	c.mockFileManager = mocksFileManager.NewMockFileManager(c.mockCtrl)
	c.mockMultitenantI = mocksMultitenant.NewMockMultiTenantI(c.mockCtrl)

	// During Setup, router subscribes to backend config
	mockCall := c.mockBackendConfig.EXPECT().Subscribe(gomock.Any(), backendconfig.TopicBackendConfig).
		Do(func(channel chan pubsub.DataEvent, topic backendconfig.Topic) {
			// on Subscribe, emulate a backend configuration event
			go func() { channel <- pubsub.DataEvent{Data: sampleBackendConfig, Topic: string(topic)} }()
		})
	tFunc := c.asyncHelper.ExpectAndNotifyCallbackWithName("backend_config")
	mockCall.Do(func(channel chan pubsub.DataEvent, topic backendconfig.Topic) { tFunc() }).
		Return().Times(1)

	c.jobQueryBatchSize = 100000
	c.mockConfigPrefix = sampleConfigPrefix
	c.mockFileObjects = sampleFileObjects
}

func (c *testContext) Finish() {
	c.asyncHelper.WaitWithTimeout(testTimeout)
	c.mockCtrl.Finish()
}

var (
	CustomVal           map[string]string = map[string]string{"S3": "S3"}
	emptyJournalEntries []jobsdb.JournalEntryT
)

func initBatchRouter() {
	config.Load()
	admin.Init()
	logger.Init()
	misc.Init()
	Init()
	Init2()
}

var _ = Describe("BatchRouter", func() {
	initBatchRouter()

	var c *testContext

	BeforeEach(func() {
		router_utils.JobRetention = time.Duration(175200) * time.Hour //20 Years(20*365*24)
		c = &testContext{}
		c.Setup()

		// setup static requirements of dependencies
		stats.Setup()
	})

	AfterEach(func() {
		c.Finish()
	})

	Context("Initialization", func() {

		It("should initialize and recover after crash", func() {
			batchrouter := &HandleT{}

			c.mockBatchRouterJobsDB.EXPECT().GetJournalEntries(gomock.Any()).Times(1).Return(emptyJournalEntries)

			batchrouter.Setup(c.mockBackendConfig, c.mockBatchRouterJobsDB, c.mockProcErrorsDB, s3DestinationDefinition.Name, nil, c.mockMultitenantI, transientsource.NewEmptyService())
		})
	})

	Context("normal operation - s3 - do not readPerDestination", func() {
		BeforeEach(func() {
			// crash recovery check
			c.mockBatchRouterJobsDB.EXPECT().GetJournalEntries(gomock.Any()).Times(1).Return(emptyJournalEntries)
		})

		It("should send failed, unprocessed jobs to s3 destination", func() {
			batchrouter := &HandleT{}

			batchrouter.Setup(c.mockBackendConfig, c.mockBatchRouterJobsDB, c.mockProcErrorsDB, s3DestinationDefinition.Name, nil, c.mockMultitenantI, transientsource.NewEmptyService())
			readPerDestination = false
			setQueryFilters()
			batchrouter.fileManagerFactory = c.mockFileManagerFactory

			c.mockFileManagerFactory.EXPECT().New(gomock.Any()).Times(1).Return(c.mockFileManager, nil)
			c.mockFileManager.EXPECT().Upload(gomock.Any(), gomock.Any(), gomock.Any()).Return(filemanager.UploadOutput{Location: "local", ObjectName: "file"}, nil)
			c.mockFileManager.EXPECT().GetConfiguredPrefix().Return(c.mockConfigPrefix)
			c.mockFileManager.EXPECT().ListFilesWithPrefix(gomock.Any(), gomock.Any(), gomock.Any()).Return(c.mockFileObjects, nil)

			s3Payload := `{
				"userId": "identified user id",
				"anonymousId":"anon-id-new",
				"context": {
				  "traits": {
					 "trait1": "new-val"
				  },
				  "ip": "14.5.67.21",
				  "library": {
					  "name": "http"
				  }
				},
				"timestamp": "2020-02-02T00:23:09.544Z"
			  }`
			parameters := fmt.Sprintf(`{"source_id": "%s", "destination_id": "%s", "message_id": "2f548e6d-60f6-44af-a1f4-62b3272445c3", "received_at": "2021-06-28T10:04:48.527+05:30", "transform_at": "none"}`, SourceIDEnabled, S3DestinationID)

			var toRetryJobsList []*jobsdb.JobT = []*jobsdb.JobT{
				{
					UUID:         uuid.Must(uuid.NewV4()),
					UserID:       "u1",
					JobID:        2009,
					CreatedAt:    time.Date(2020, 04, 28, 13, 26, 00, 00, time.UTC),
					ExpireAt:     time.Date(2020, 04, 28, 13, 26, 00, 00, time.UTC),
					CustomVal:    CustomVal["S3"],
					EventPayload: []byte(s3Payload),
					LastJobStatus: jobsdb.JobStatusT{
						AttemptNum:    1,
						ErrorResponse: []byte(`{"firstAttemptedAt": "2021-06-28T15:57:30.742+05:30"}`),
					},
					Parameters: []byte(parameters),
				},
			}

			var unprocessedJobsList []*jobsdb.JobT = []*jobsdb.JobT{
				{
					UUID:         uuid.Must(uuid.NewV4()),
					UserID:       "u1",
					JobID:        2010,
					CreatedAt:    time.Date(2020, 04, 28, 13, 26, 00, 00, time.UTC),
					ExpireAt:     time.Date(2020, 04, 28, 13, 26, 00, 00, time.UTC),
					CustomVal:    CustomVal["S3"],
					EventPayload: []byte(s3Payload),
					LastJobStatus: jobsdb.JobStatusT{
						AttemptNum: 0,
					},
					Parameters: []byte(parameters),
				},
			}

			payloadLimit := batchrouter.payloadLimit
			callRetry := c.mockBatchRouterJobsDB.EXPECT().GetToRetry(jobsdb.GetQueryParamsT{CustomValFilters: []string{CustomVal["S3"]}, JobsLimit: c.jobQueryBatchSize, PayloadSizeLimit: payloadLimit}).Return(toRetryJobsList).Times(1)
			c.mockBatchRouterJobsDB.EXPECT().GetUnprocessed(jobsdb.GetQueryParamsT{CustomValFilters: []string{CustomVal["S3"]}, JobsLimit: c.jobQueryBatchSize - len(toRetryJobsList), PayloadSizeLimit: payloadLimit}).Return(unprocessedJobsList).Times(1).After(callRetry)

			c.mockBatchRouterJobsDB.EXPECT().UpdateJobStatus(gomock.Any(), []string{CustomVal["S3"]}, gomock.Any()).Times(1).
				Do(func(statuses []*jobsdb.JobStatusT, _ interface{}, _ interface{}) {
					assertJobStatus(toRetryJobsList[0], statuses[0], jobsdb.Executing.State, "", `{}`, 2)
					assertJobStatus(unprocessedJobsList[0], statuses[1], jobsdb.Executing.State, "", `{}`, 1)
				}).Return(nil)

			c.mockBatchRouterJobsDB.EXPECT().JournalMarkStart(gomock.Any(), gomock.Any()).Times(1).Return(int64(1))

			callBeginTransaction := c.mockBatchRouterJobsDB.EXPECT().BeginGlobalTransaction().Times(1).Return(nil)
			callAcquireLocks := c.mockBatchRouterJobsDB.EXPECT().AcquireUpdateJobStatusLocks().Times(1).After(callBeginTransaction)
			callUpdateStatus := c.mockBatchRouterJobsDB.EXPECT().UpdateJobStatusInTxn(nil, gomock.Any(), []string{CustomVal["S3"]}, nil).Times(1).After(callAcquireLocks).
				Do(func(_ interface{}, statuses []*jobsdb.JobStatusT, _ interface{}, _ interface{}) {
					assertJobStatus(toRetryJobsList[0], statuses[0], jobsdb.Succeeded.State, "", `{"firstAttemptedAt": "2021-06-28T15:57:30.742+05:30", "success": "OK"}`, 2)
					assertJobStatus(unprocessedJobsList[0], statuses[1], jobsdb.Succeeded.State, "", `{"firstAttemptedAt": "2021-06-28T15:57:30.742+05:30, "success": "OK""}`, 1)
				}).Return(nil)
			callCommitTransaction := c.mockBatchRouterJobsDB.EXPECT().CommitTransaction(gomock.Any()).Times(1).After(callUpdateStatus)
			c.mockBatchRouterJobsDB.EXPECT().ReleaseUpdateJobStatusLocks().Times(1).After(callCommitTransaction)

			c.mockBatchRouterJobsDB.EXPECT().JournalDeleteEntry(gomock.Any()).Times(1)

			<-batchrouter.backendConfigInitialized
			batchrouter.readAndProcess()
		})

		// It("should split batchJobs based on timeWindow for s3 datalake destination", func() {

		// 	batchJobs := BatchJobsT{
		// 		Jobs: []*jobsdb.JobT{
		// 			{
		// 				EventPayload: json.RawMessage(`{"receivedAt": "2019-10-12T07:20:50.52Z"}`),
		// 			},
		// 			{
		// 				EventPayload: json.RawMessage(`{"receivedAt": "2019-10-12T07:20:59.52Z"}`),
		// 			},
		// 			{
		// 				EventPayload: json.RawMessage(`{"receivedAt": "2019-10-12T07:30:50.52Z"}`),
		// 			},
		// 			{
		// 				EventPayload: json.RawMessage(`{"receivedAt": "2019-10-12T07:30:59.52Z"}`),
		// 			},
		// 			{
		// 				EventPayload: json.RawMessage(`{"receivedAt": "2019-10-12T08:00:01.52Z"}`),
		// 			},
		// 		},
		// 	}

		// 	brt := &HandleT{destType: "S3_DATALAKE"}
		// 	splitBatchJobs := brt.splitBatchJobsOnTimeWindow(batchJobs)
		// 	for timeWindow, batchJob := range splitBatchJobs {
		// 		fmt.Println(timeWindow, len(batchJob.Jobs))
		// 	}
		// })

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
