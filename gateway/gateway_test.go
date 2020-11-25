package gateway

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	uuid "github.com/satori/go.uuid"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"

	"github.com/rudderlabs/rudder-server/app"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/gateway/response"
	"github.com/rudderlabs/rudder-server/jobsdb"
	mocksApp "github.com/rudderlabs/rudder-server/mocks/app"
	mocksBackendConfig "github.com/rudderlabs/rudder-server/mocks/config/backend-config"
	mocksJobsDB "github.com/rudderlabs/rudder-server/mocks/jobsdb"
	mocksRateLimiter "github.com/rudderlabs/rudder-server/mocks/rate-limiter"
	mocksStats "github.com/rudderlabs/rudder-server/mocks/stats"
	mocksTypes "github.com/rudderlabs/rudder-server/mocks/utils/types"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils"
	"github.com/rudderlabs/rudder-server/utils/misc"
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

	// CurrentWorkspaceID        = "workspace-id"
	// WorkspaceSuppressedUserID = "suppressed-user-1"
	SuppressedUserID = "suppressed-user-2"
	NormalUserID     = "normal-user-1"
	// SecondEnabledSourceID     = "enabled-source-2"
	// SecondEnabledWriteKey     = "enabled-write-key-2"
)

var testTimeout = 10 * time.Second

// This configuration is assumed by all gateway tests and, is returned on Subscribe of mocked backend config
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
		// {
		// 	ID:       SecondEnabledSourceID,
		// 	WriteKey: SecondEnabledWriteKey,
		// 	Enabled:  true,
		// },
	},
}

// var sampleRegulationsConfig = backendconfig.RegulationsT{
// 	WorkspaceRegulations: []backendconfig.WorkspaceRegulationT{
// 		ID: "1",
// 		RegulationType: "Suppress",
// 		WorkspaceID: CurrentWorkspaceID,
// 		UserID: WorkspaceSuppressedUserID
// 	},
// 	SourceRegulations: []backendconfig.SourceRegulationT{
// 		ID: "2",
// 		RegulationType: "Suppress",
// 		WorkspaceID: CurrentWorkspaceID,
// 		SourceID: SourceIDEnabled,
// 		UserID: SourceSuppressedUserID
// 	},
// }

type context struct {
	asyncHelper testutils.AsyncTestHelper

	mockCtrl          *gomock.Controller
	mockJobsDB        *mocksJobsDB.MockJobsDB
	mockBackendConfig *mocksBackendConfig.MockBackendConfig
	mockApp           *mocksApp.MockInterface
	mockRateLimiter   *mocksRateLimiter.MockRateLimiter
	mockStats         *mocksStats.MockStats

	mockStatGatewayResponseTime     *mocksStats.MockRudderStats
	mockStatGatewayBatchSize        *mocksStats.MockRudderStats
	mockStatDBWritesStat            *mocksStats.MockRudderStats
	mockStatDBWorkersBufferFullStat *mocksStats.MockRudderStats
	mockStatDBWorkersTimeOutStat    *mocksStats.MockRudderStats
	mockStatGatewayBatchTime        []*mocksStats.MockRudderStats
	mockStatGatewayBufferFullStat   []*mocksStats.MockRudderStats
	mockStatGatewayTimeOutStat      []*mocksStats.MockRudderStats
	mockVersionHandler              func(w http.ResponseWriter, r *http.Request)

	//Enterprise mocks
	mockSuppressUser        *mocksTypes.MockSuppressUserI
	mockSuppressUserFeature *mocksApp.MockSuppressUserFeature
}

func (c *context) initializeSetupStats() {
	registerStatMocks := func(name string, statType string) *mocksStats.MockRudderStats {
		stat := mocksStats.NewMockRudderStats(c.mockCtrl)
		c.mockStats.EXPECT().NewStat(name, statType).Return(stat).Times(1)
		return stat
	}
	c.mockStatGatewayBatchSize = mocksStats.NewMockRudderStats(c.mockCtrl)
	c.mockStatDBWritesStat = mocksStats.NewMockRudderStats(c.mockCtrl)
	c.mockStatDBWorkersBufferFullStat = mocksStats.NewMockRudderStats(c.mockCtrl)
	c.mockStatDBWorkersTimeOutStat = mocksStats.NewMockRudderStats(c.mockCtrl)
	c.mockStatGatewayBatchTime = make([]*mocksStats.MockRudderStats, maxUserWebRequestWorkerProcess)
	c.mockStatGatewayBufferFullStat = make([]*mocksStats.MockRudderStats, maxUserWebRequestWorkerProcess)
	c.mockStatGatewayTimeOutStat = make([]*mocksStats.MockRudderStats, maxUserWebRequestWorkerProcess)
	for i := 0; i < maxUserWebRequestWorkerProcess; i++ {
		c.mockStatGatewayBatchTime[i] = registerStatMocks("gateway.batch_time",stats.TimerType)
		c.mockStatGatewayBufferFullStat[i] = registerStatMocks(fmt.Sprintf("gateway.user_request_worker_%d_buffer_full", i),stats.CountType)
		c.mockStatGatewayTimeOutStat[i] = registerStatMocks(fmt.Sprintf("gateway.user_request_worker_%d_time_out", i),stats.CountType)
		//Since we have 64 User request batch workers, not all workers might get
		//Start and End calls while running tests. So using AnyTimes() instead of Times(1)
		c.mockStatGatewayBatchTime[i].EXPECT().Start().AnyTimes()
		c.mockStatGatewayBatchTime[i].EXPECT().End().AnyTimes()
		c.mockStatGatewayBufferFullStat[i].EXPECT().Count(1).AnyTimes()
		c.mockStatGatewayTimeOutStat[i].EXPECT().Count(1).AnyTimes()
	}

	// During Setup, gateway always creates the following stats
	c.mockStats.EXPECT().NewStat("gateway.batch_size", stats.CountType).Return(c.mockStatGatewayBatchSize).Times(1).Do(c.asyncHelper.ExpectAndNotifyCallbackWithName("gateway.batch_size.new"))
	c.mockStats.EXPECT().NewStat("gateway.db_writes", stats.CountType).Return(c.mockStatDBWritesStat).Times(1).Do(c.asyncHelper.ExpectAndNotifyCallbackWithName("gateway.db_writes.new"))
	c.mockStats.EXPECT().NewStat("gateway.db_workers_buffer_full", stats.CountType).Return(c.mockStatDBWorkersBufferFullStat).Times(1).Do(c.asyncHelper.ExpectAndNotifyCallbackWithName("gateway.db_workers_buffer_full.new"))
	c.mockStats.EXPECT().NewStat("gateway.db_workers_time_out", stats.CountType).Return(c.mockStatDBWorkersTimeOutStat).Times(1).Do(c.asyncHelper.ExpectAndNotifyCallbackWithName("gateway.db_workers_time_out.new"))

}

func (c *context) initializeAppFeatures() {
	c.mockApp.EXPECT().Features().Return(&app.Features{}).AnyTimes()
}

func (c *context) initializeEnterprizeAppFeatures() {
	enterpriseFeatures := &app.Features{
		SuppressUser: c.mockSuppressUserFeature,
	}
	c.mockApp.EXPECT().Features().Return(enterpriseFeatures).AnyTimes()
}

// Initiaze mocks and common expectations
func (c *context) Setup() {
	c.asyncHelper.Setup()
	c.mockCtrl = gomock.NewController(GinkgoT())
	c.mockJobsDB = mocksJobsDB.NewMockJobsDB(c.mockCtrl)
	c.mockBackendConfig = mocksBackendConfig.NewMockBackendConfig(c.mockCtrl)
	c.mockApp = mocksApp.NewMockInterface(c.mockCtrl)
	c.mockRateLimiter = mocksRateLimiter.NewMockRateLimiter(c.mockCtrl)
	c.mockStats = mocksStats.NewMockStats(c.mockCtrl)

	c.initializeSetupStats()

	// During Setup, gateway subscribes to backend config and waits until it is received.
	c.mockBackendConfig.EXPECT().WaitForConfig().Return().Times(1).Do(c.asyncHelper.ExpectAndNotifyCallbackWithName("wait_for_config"))
	c.mockBackendConfig.EXPECT().Subscribe(gomock.Any(), backendconfig.TopicProcessConfig).
		Do(func(channel chan utils.DataEvent, topic backendconfig.Topic) {
			// on Subscribe, emulate a backend configuration event
			go func() { channel <- utils.DataEvent{Data: sampleBackendConfig, Topic: string(topic)} }()
		}).
		Do(c.asyncHelper.ExpectAndNotifyCallbackWithName("process_config")).
		Return().Times(1)
	c.mockVersionHandler = func(w http.ResponseWriter, r *http.Request) {}
}

func (c *context) Finish() {
	c.asyncHelper.WaitWithTimeout(testTimeout)
	c.mockCtrl.Finish()
}

// helper function to add expectations about a specific writeKey stat. Returns gomock.Call of RudderStats Count()
func (c *context) expectWriteKeyStat(name string, writeKey string, count int) *gomock.Call {
	mockStat := mocksStats.NewMockRudderStats(c.mockCtrl)

	c.mockStats.EXPECT().NewTaggedStat(name, stats.CountType, stats.Tags{"writekey" : writeKey,}).
		Return(mockStat).Times(1).
		Do(c.asyncHelper.ExpectAndNotifyCallbackWithName(fmt.Sprintf("write_key.new.%s.%s", name, writeKey)))

	return mockStat.EXPECT().Count(count).
		Times(1).
		Do(c.asyncHelper.ExpectAndNotifyCallbackWithName(fmt.Sprintf("write_key.count.%s.%s", name, writeKey)))
}

var _ = Describe("Gateway Enterprise", func() {
	var c *context

	BeforeEach(func() {
		c = &context{}
		c.Setup()

		c.mockSuppressUser = mocksTypes.NewMockSuppressUserI(c.mockCtrl)
		c.mockSuppressUserFeature = mocksApp.NewMockSuppressUserFeature(c.mockCtrl)
		c.initializeEnterprizeAppFeatures()

		c.mockSuppressUserFeature.EXPECT().Setup(gomock.Any()).AnyTimes().Return(c.mockSuppressUser)
		c.mockSuppressUser.EXPECT().IsSuppressedUser(NormalUserID, SourceIDEnabled, WriteKeyEnabled).Return(false).AnyTimes()
		c.mockSuppressUser.EXPECT().IsSuppressedUser(SuppressedUserID, SourceIDEnabled, WriteKeyEnabled).Return(true).AnyTimes()

		// setup static requirements of dependencies
		stats.Setup()

		// setup common environment, override in BeforeEach when required
		SetEnableRateLimit(false)
		SetEnableDedup(false)
		SetEnableSuppressUserFeature(true)
		SetEnableEventSchemasFeature(false)
	})

	AfterEach(func() {
		c.Finish()
	})

	Context("Suppress users", func() {
		var clearDB = false
		gateway := &HandleT{}

		BeforeEach(func() {
			gateway.Setup(c.mockApp, c.mockBackendConfig, c.mockJobsDB, nil, c.mockStats, &clearDB, c.mockVersionHandler)
		})

		It("should not accept events from suppress users", func() {
			suppressedUserEventData := fmt.Sprintf("{\"batch\":[{\"userId\": \"%s\"}]}", SuppressedUserID)

			c.mockStatGatewayBatchSize.EXPECT().Count(1).Times(1).Do(c.asyncHelper.ExpectAndNotifyCallbackWithName("batch-size"))

			c.expectWriteKeyStat("gateway.write_key_requests", WriteKeyEnabled, 1)
			c.expectWriteKeyStat("gateway.write_key_events", WriteKeyEnabled, 1)
			// Why GET
			expectHandlerResponse(gateway.webBatchHandler, authorizedRequest(WriteKeyEnabled, bytes.NewBufferString(suppressedUserEventData)), 200, "OK")
		})

		It("should accept events from normal users", func() {
			allowedUserEventData := fmt.Sprintf("{\"batch\":[{\"userId\": \"%s\"}]}", NormalUserID)

			c.mockJobsDB.EXPECT().StoreWithRetryEach(gomock.Any()).DoAndReturn(jobsToEmptyErrors).Times(1).Do(c.asyncHelper.ExpectAndNotifyCallbackWithName("store-job"))
			c.mockStatGatewayBatchSize.EXPECT().Count(1).Times(1).Do(c.asyncHelper.ExpectAndNotifyCallbackWithName("batch-size"))
			c.mockStatDBWritesStat.EXPECT().Count(1).Times(1).Do(c.asyncHelper.ExpectAndNotifyCallbackWithName("db_writes"))
			c.mockStatDBWorkersTimeOutStat.EXPECT().Count(1).Times(1).Do(c.asyncHelper.ExpectAndNotifyCallbackWithName("db_workers_time_out"))

			c.expectWriteKeyStat("gateway.write_key_requests", WriteKeyEnabled, 1)
			c.expectWriteKeyStat("gateway.write_key_successful_requests", WriteKeyEnabled, 1)
			c.expectWriteKeyStat("gateway.write_key_events", WriteKeyEnabled, 1)
			c.expectWriteKeyStat("gateway.write_key_successful_events", WriteKeyEnabled, 1)
			// Why GET
			expectHandlerResponse(gateway.webBatchHandler, authorizedRequest(WriteKeyEnabled, bytes.NewBufferString(allowedUserEventData)), 200, "OK")
		})
	})

})

var _ = Describe("Gateway", func() {
	var c *context

	BeforeEach(func() {
		c = &context{}
		c.Setup()
		c.initializeAppFeatures()

		// setup static requirements of dependencies
		stats.Setup()

		// setup common environment, override in BeforeEach when required
		SetEnableRateLimit(false)
		SetEnableDedup(false)
		SetEnableEventSchemasFeature(false)
	})

	AfterEach(func() {
		c.Finish()
	})

	Context("Initialization", func() {
		gateway := &HandleT{}
		var clearDB = false

		It("should wait for backend config", func() {
			gateway.Setup(c.mockApp, c.mockBackendConfig, c.mockJobsDB, nil, c.mockStats, &clearDB, c.mockVersionHandler)
		})
	})

	Context("Valid requests", func() {
		var (
			gateway                = &HandleT{}
			clearDB           bool = false
			gatewayBatchCalls int  = 1
		)

		// tracks expected batch_id
		nextBatchID := func() (batchID int) {
			batchID = gatewayBatchCalls
			gatewayBatchCalls++
			return
		}

		BeforeEach(func() {
			gateway.Setup(c.mockApp, c.mockBackendConfig, c.mockJobsDB, nil, c.mockStats, &clearDB, c.mockVersionHandler)
		})

		assertJobMetadata := func(job *jobsdb.JobT, batchLength int, batchId int) {
			Expect(misc.IsValidUUID(job.UUID.String())).To(Equal(true))
			Expect(job.Parameters).To(Equal(json.RawMessage(fmt.Sprintf(`{"source_id": "%v", "batch_id": %d}`, SourceIDEnabled, batchId))))
			Expect(job.CustomVal).To(Equal(CustomVal))

			responseData := []byte(job.EventPayload)
			receivedAt := gjson.GetBytes(responseData, "receivedAt")
			writeKey := gjson.GetBytes(responseData, "writeKey")
			requestIP := gjson.GetBytes(responseData, "requestIP")
			batch := gjson.GetBytes(responseData, "batch")

			Expect(time.Parse(misc.RFC3339Milli, receivedAt.String())).To(BeTemporally("~", time.Now(), 100*time.Millisecond))
			Expect(writeKey.String()).To(Equal(WriteKeyEnabled))
			Expect(requestIP.String()).To(Equal(TestRemoteAddress))
			Expect(batch.Array()).To(HaveLen(batchLength))
		}

		createValidBody := func(customProperty string, customValue string) []byte {
			validData := `{"userId":"dummyId","data":{"string":"valid-json","nested":{"child":1}}}`
			validDataWithProperty, _ := sjson.SetBytes([]byte(validData), customProperty, customValue)

			return validDataWithProperty
		}

		assertJobBatchItem := func(payload gjson.Result) {
			messageID := payload.Get("messageId")
			messageType := payload.Get("type")

			// Assertions regarding batch message
			Expect(messageID.Exists()).To(BeTrue())
			Expect(messageID.String()).To(testutils.BeValidUUID())
			Expect(messageType.Exists()).To(BeTrue())
		}

		stripJobPayload := func(payload gjson.Result) string {
			strippedPayload, _ := sjson.Delete(payload.String(), "messageId")
			strippedPayload, _ = sjson.Delete(strippedPayload, "rudderId")
			strippedPayload, _ = sjson.Delete(strippedPayload, "type")

			return strippedPayload
		}

		// common tests for all web handlers
		assertSingleMessageHandler := func(handlerType string, handler http.HandlerFunc) {
			It("should accept valid requests on a single endpoint (except batch), and store to jobsdb", func() {
				validBody := createValidBody("custom-property", "custom-value")

				c.mockStatGatewayBatchSize.EXPECT().Count(1).
					Times(1).Do(c.asyncHelper.ExpectAndNotifyCallbackWithName("gateway.batch_size.count"))
				c.mockStatDBWritesStat.EXPECT().Count(1).Times(1).Do(c.asyncHelper.ExpectAndNotifyCallbackWithName("db_writes"))
				c.mockStatDBWorkersTimeOutStat.EXPECT().Count(1).Times(1).Do(c.asyncHelper.ExpectAndNotifyCallbackWithName("db_workers_time_out"))
				c.expectWriteKeyStat("gateway.write_key_requests", WriteKeyEnabled, 1)
				c.expectWriteKeyStat("gateway.write_key_successful_requests", WriteKeyEnabled, 1)
				c.expectWriteKeyStat("gateway.write_key_events", WriteKeyEnabled, 1)
				c.expectWriteKeyStat("gateway.write_key_successful_events", WriteKeyEnabled, 1)
				c.mockJobsDB.
					EXPECT().StoreWithRetryEach(gomock.Any()).
					DoAndReturn(func(jobs []*jobsdb.JobT) map[uuid.UUID]string {
						for _, job := range jobs {
							// each call should be included in a separate batch, with a separate batch_id
							expectedBatchID := nextBatchID()
							assertJobMetadata(job, 1, expectedBatchID)

							responseData := []byte(job.EventPayload)
							payload := gjson.GetBytes(responseData, "batch.0")

							assertJobBatchItem(payload)

							messageType := payload.Get("type")
							Expect(messageType.String()).To(Equal(handlerType))
							Expect(stripJobPayload(payload)).To(MatchJSON(validBody))
						}
						c.asyncHelper.ExpectAndNotifyCallbackWithName("jobsdb_store")()

						return jobsToEmptyErrors(jobs)
					}).
					Times(1)

				expectHandlerResponse(handler, authorizedRequest(WriteKeyEnabled, bytes.NewBuffer(validBody)), 200, "OK")
			})
		}

		for handlerType, handler := range allHandlers(gateway) {
			if handlerType != "batch" {
				assertSingleMessageHandler(handlerType, handler)
			}
		}

		//Commenting the following test
		//This will be fixed and uncommented in user based request routing tests
		// It("should process multiple requests to all endpoints (except batch) in a batch", func() {
		// 	handlers := map[string]http.HandlerFunc{
		// 		"alias":    gateway.webAliasHandler,
		// 		"group":    gateway.webGroupHandler,
		// 		"identify": gateway.webIdentifyHandler,
		// 		"page":     gateway.webPageHandler,
		// 	}

		// 	handlerExpectation := func(handlerType string, handler http.HandlerFunc) *RequestExpectation {
		// 		// we add the handler type in custom property of request's body, to check that the type field is set correctly while batching
		// 		validBody := createValidBody("custom-property-type", handlerType)
		// 		validRequest := authorizedRequest(WriteKeyEnabled, bytes.NewBuffer(validBody))

		// 		return &RequestExpectation{
		// 			request:        validRequest,
		// 			handler:        handler,
		// 			responseStatus: 200,
		// 			responseBody:   "OK",
		// 		}
		// 	}

		// 	c.mockStatGatewayBatchSize.EXPECT().Count(4).
		// 		Times(1).Do(c.asyncHelper.ExpectAndNotifyCallbackWithName(""))
		//  c.mockStatDBWritesStat.EXPECT().Count(1).Times(1).Do(c.asyncHelper.ExpectAndNotifyCallbackWithName("db_writes"))
		//	c.mockStatDBWorkersTimeOutStat.EXPECT().Count(1).Times(1).Do(c.asyncHelper.ExpectAndNotifyCallbackWithName("db_workers_time_out"))

		// 	callStart := c.mockStatGatewayBatchTime.EXPECT().Start().Times(1).Do(c.asyncHelper.ExpectAndNotifyCallbackWithName(""))

		// 	callStore := c.mockJobsDB.
		// 		EXPECT().StoreWithRetryEach(gomock.Any()).
		// 		DoAndReturn(func(jobs []*jobsdb.JobT) map[uuid.UUID]string {
		// 			// will collect all message handler types, found in jobs send to Store function
		// 			typesFound := make(map[string]bool, 4)

		// 			// All jobs should belong to the same batchId
		// 			expectedBatchID := nextBatchID()

		// 			for _, job := range jobs {
		// 				assertJobMetadata(job, 1, expectedBatchID)

		// 				responseData := []byte(job.EventPayload)
		// 				payload := gjson.GetBytes(responseData, "batch.0")

		// 				assertJobBatchItem(payload)

		// 				messageType := payload.Get("type").String()
		// 				Expect(stripJobPayload(payload)).To(MatchJSON(createValidBody("custom-property-type", messageType)))

		// 				typesFound[messageType] = true
		// 			}

		// 			// ensure all message handler types appear in jobs
		// 			for t := range handlers {
		// 				if t != "batch" {
		// 					_, found := typesFound[t]
		// 					Expect(found).To(BeTrue())
		// 				}
		// 			}

		// 			c.asyncHelper.ExpectAndNotifyCallbackWithName("")()

		// 			return jobsToEmptyErrors(jobs)
		// 		}).
		// 		Times(1)

		// 	c.mockStatGatewayBatchTime.EXPECT().End().After(callStart).After(callStore).Times(1).Do(c.asyncHelper.ExpectAndNotifyCallbackWithName(""))

		// 	expectations := []*RequestExpectation{}
		// 	for t, h := range handlers {
		// 		if t != "batch" {
		// 			expectations = append(expectations, handlerExpectation(t, h))
		// 		}
		// 	}

		// 	c.expectWriteKeyStat("gateway.write_key_requests", WriteKeyEnabled, 4)
		// 	c.expectWriteKeyStat("gateway.write_key_successful_requests", WriteKeyEnabled, 4)
		// 	c.expectWriteKeyStat("gateway.write_key_events", WriteKeyEnabled, 0)
		// 	c.expectWriteKeyStat("gateway.write_key_successful_events", WriteKeyEnabled, 0)

		// 	expectBatch(expectations)
		// })
	})

	Context("Rate limits", func() {
		var (
			gateway      = &HandleT{}
			clearDB bool = false
		)

		BeforeEach(func() {
			SetEnableRateLimit(true)
			gateway.Setup(c.mockApp, c.mockBackendConfig, c.mockJobsDB, c.mockRateLimiter, c.mockStats, &clearDB, c.mockVersionHandler)
		})

		It("should store messages successfuly if rate limit is not reached for workspace", func() {
			workspaceID := "some-workspace-id"

			c.mockBackendConfig.EXPECT().GetWorkspaceIDForWriteKey(WriteKeyEnabled).Return(workspaceID).AnyTimes().Do(c.asyncHelper.ExpectAndNotifyCallbackWithName(""))
			c.mockRateLimiter.EXPECT().LimitReached(workspaceID).Return(false).Times(1).Do(c.asyncHelper.ExpectAndNotifyCallbackWithName(""))
			c.mockJobsDB.EXPECT().StoreWithRetryEach(gomock.Any()).DoAndReturn(jobsToEmptyErrors).Times(1).Do(c.asyncHelper.ExpectAndNotifyCallbackWithName(""))
			c.mockStatGatewayBatchSize.EXPECT().Count(1).Times(1).Do(c.asyncHelper.ExpectAndNotifyCallbackWithName(""))
			c.mockStatDBWritesStat.EXPECT().Count(1).Times(1).Do(c.asyncHelper.ExpectAndNotifyCallbackWithName("db_writes"))
			c.mockStatDBWorkersTimeOutStat.EXPECT().Count(1).Times(1).Do(c.asyncHelper.ExpectAndNotifyCallbackWithName("db_workers_time_out"))

			c.expectWriteKeyStat("gateway.write_key_requests", WriteKeyEnabled, 1)
			c.expectWriteKeyStat("gateway.write_key_successful_requests", WriteKeyEnabled, 1)
			c.expectWriteKeyStat("gateway.write_key_events", WriteKeyEnabled, 1)
			c.expectWriteKeyStat("gateway.write_key_successful_events", WriteKeyEnabled, 1)

			expectHandlerResponse(gateway.webAliasHandler, authorizedRequest(WriteKeyEnabled, bytes.NewBufferString(`{"userId":"dummyId"}`)), 200, "OK")
		})

		It("should reject messages if rate limit is reached for workspace", func() {
			workspaceID := "some-workspace-id"

			c.mockBackendConfig.EXPECT().GetWorkspaceIDForWriteKey(WriteKeyEnabled).Return(workspaceID).AnyTimes().Do(c.asyncHelper.ExpectAndNotifyCallbackWithName(""))
			c.mockRateLimiter.EXPECT().LimitReached(workspaceID).Return(true).Times(1).Do(c.asyncHelper.ExpectAndNotifyCallbackWithName(""))
			c.mockStatGatewayBatchSize.EXPECT().Count(1).Times(1).Do(c.asyncHelper.ExpectAndNotifyCallbackWithName(""))

			c.expectWriteKeyStat("gateway.write_key_requests", WriteKeyEnabled, 1)
			c.expectWriteKeyStat("gateway.work_space_dropped_requests", workspaceID, 1)

			expectHandlerResponse(gateway.webAliasHandler, authorizedRequest(WriteKeyEnabled, bytes.NewBufferString("{}")), 400, response.TooManyRequests+"\n")
		})
	})

	Context("Invalid requests", func() {
		var (
			gateway      = &HandleT{}
			clearDB bool = false
		)

		BeforeEach(func() {
			gateway.Setup(c.mockApp, c.mockBackendConfig, c.mockJobsDB, nil, c.mockStats, &clearDB, c.mockVersionHandler)
		})

		// common tests for all web handlers
		assertHandler := func(handlerType string, handler http.HandlerFunc) {
			It("should reject requests without Authorization header", func() {
				c.mockStatGatewayBatchSize.EXPECT().Count(1).
					Times(1).Do(c.asyncHelper.ExpectAndNotifyCallbackWithName(""))

				c.expectWriteKeyStat("gateway.write_key_requests", "", 1)
				c.expectWriteKeyStat("gateway.write_key_failed_requests", "noWriteKey", 1)

				expectHandlerResponse(handler, unauthorizedRequest(nil), 400, response.NoWriteKeyInBasicAuth+"\n")
			})

			It("should reject requests without username in Authorization header", func() {
				c.mockStatGatewayBatchSize.EXPECT().Count(1).
					Times(1).Do(c.asyncHelper.ExpectAndNotifyCallbackWithName(""))

				c.expectWriteKeyStat("gateway.write_key_requests", "", 1)
				c.expectWriteKeyStat("gateway.write_key_failed_requests", "noWriteKey", 1)

				expectHandlerResponse(handler, authorizedRequest(WriteKeyEmpty, nil), 400, response.NoWriteKeyInBasicAuth+"\n")
			})

			It("should reject requests with both userId and anonymousId not present", func() {
				validBody := `{"data": "valid-json"}`
				if handlerType == "batch" {
					validBody = `{"batch": [{"data": "valid-json"}]}`
				}
				c.mockStatGatewayBatchSize.EXPECT().Count(1).
					Times(1).Do(c.asyncHelper.ExpectAndNotifyCallbackWithName(""))

				c.expectWriteKeyStat("gateway.write_key_requests", WriteKeyEnabled, 1)
				c.expectWriteKeyStat("gateway.write_key_failed_requests", "notIdentifiable", 1)
				c.expectWriteKeyStat("gateway.write_key_events", WriteKeyEnabled, 1)
				expectHandlerResponse(handler, authorizedRequest(WriteKeyEnabled, bytes.NewBufferString(validBody)), 400, response.NonIdentifiableRequest+"\n")
			})

			It("should reject requests without request body", func() {
				c.mockStatGatewayBatchSize.EXPECT().Count(1).
					Times(1).Do(c.asyncHelper.ExpectAndNotifyCallbackWithName(""))

				c.expectWriteKeyStat("gateway.write_key_requests", WriteKeyInvalid, 1)

				expectHandlerResponse(handler, authorizedRequest(WriteKeyInvalid, nil), 400, response.RequestBodyNil+"\n")
			})

			It("should reject requests without valid json in request body", func() {
				invalidBody := "not-a-valid-json"

				c.mockStatGatewayBatchSize.EXPECT().Count(1).
					Times(1).Do(c.asyncHelper.ExpectAndNotifyCallbackWithName(""))

				c.expectWriteKeyStat("gateway.write_key_requests", WriteKeyInvalid, 1)
				c.expectWriteKeyStat("gateway.write_key_failed_requests", WriteKeyInvalid, 1)

				expectHandlerResponse(handler, authorizedRequest(WriteKeyInvalid, bytes.NewBufferString(invalidBody)), 400, response.InvalidJSON+"\n")
			})

			It("should reject requests with request bodies larger than configured limit", func() {
				data := make([]byte, gateway.MaxReqSize())
				for i := range data {
					data[i] = 'a'
				}
				body := fmt.Sprintf(`{"data":"%s"}`, string(data))
				if handlerType == "batch" {
					body = fmt.Sprintf(`{"batch":[%s]}`, body)
				}
				c.mockStatGatewayBatchSize.EXPECT().Count(1).
					Times(1).Do(c.asyncHelper.ExpectAndNotifyCallbackWithName(""))
				c.expectWriteKeyStat("gateway.write_key_requests", WriteKeyInvalid, 1)
				c.expectWriteKeyStat("gateway.write_key_failed_requests", WriteKeyInvalid, 1)
				c.expectWriteKeyStat("gateway.write_key_events", WriteKeyInvalid, 1)
				c.expectWriteKeyStat("gateway.write_key_failed_events", WriteKeyInvalid, 1)

				expectHandlerResponse(handler, authorizedRequest(WriteKeyInvalid, bytes.NewBufferString(body)), 400, response.RequestBodyTooLarge+"\n")
			})

			It("should reject requests with invalid write keys", func() {
				validBody := `{"data":"valid-json"}`
				if handlerType == "batch" {
					validBody = `{"batch":[{"data":"valid-json"}]}`
				}
				c.mockStatGatewayBatchSize.EXPECT().Count(1).
					Times(1).Do(c.asyncHelper.ExpectAndNotifyCallbackWithName(""))
				c.expectWriteKeyStat("gateway.write_key_requests", WriteKeyInvalid, 1)
				c.expectWriteKeyStat("gateway.write_key_failed_requests", WriteKeyInvalid, 1)
				c.expectWriteKeyStat("gateway.write_key_events", WriteKeyInvalid, 1)
				c.expectWriteKeyStat("gateway.write_key_failed_events", WriteKeyInvalid, 1)

				expectHandlerResponse(handler, authorizedRequest(WriteKeyInvalid, bytes.NewBufferString(validBody)), 400, response.InvalidWriteKey+"\n")
			})

			It("should reject requests with disabled write keys", func() {
				validBody := `{"data":"valid-json"}`
				if handlerType == "batch" {
					validBody = `{"batch":[{"data":"valid-json"}]}`
				}
				c.mockStatGatewayBatchSize.EXPECT().Count(1).
					Times(1).Do(c.asyncHelper.ExpectAndNotifyCallbackWithName(""))

				c.expectWriteKeyStat("gateway.write_key_requests", WriteKeyDisabled, 1)
				c.expectWriteKeyStat("gateway.write_key_failed_requests", WriteKeyDisabled, 1)
				c.expectWriteKeyStat("gateway.write_key_events", WriteKeyDisabled, 1)
				c.expectWriteKeyStat("gateway.write_key_failed_events", WriteKeyDisabled, 1)

				expectHandlerResponse(handler, authorizedRequest(WriteKeyDisabled, bytes.NewBufferString(validBody)), 400, response.InvalidWriteKey+"\n")
			})
		}

		for handlerType, handler := range allHandlers(gateway) {
			assertHandler(handlerType, handler)
		}
	})
})

func unauthorizedRequest(body io.Reader) *http.Request {
	req, err := http.NewRequest("GET", "", body)
	if err != nil {
		panic(err)
	}

	return req
}

func authorizedRequest(username string, body io.Reader) *http.Request {
	req := unauthorizedRequest(body)

	basicAuth := base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%s:password-should-be-ignored", username)))

	req.Header.Set("Authorization", fmt.Sprintf("Basic %s", basicAuth))
	// set anonymousId header to ensure everything goes into same batch
	req.Header.Set("AnonymousId", "094985f8-b4eb-43c3-bc8a-e8b75aae9c7c")
	req.RemoteAddr = TestRemoteAddressWithPort
	return req
}

func expectHandlerResponse(handler http.HandlerFunc, req *http.Request, responseStatus int, responseBody string) {
	testutils.RunTestWithTimeout(func() {
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)

		bodyBytes, _ := ioutil.ReadAll(rr.Body)
		body := string(bodyBytes)

		Expect(rr.Result().StatusCode).To(Equal(responseStatus))
		Expect(body).To(Equal(responseBody))
	}, testTimeout)
}

type RequestExpectation struct {
	request        *http.Request
	handler        http.HandlerFunc
	responseStatus int
	responseBody   string
}

func expectBatch(expectations []*RequestExpectation) {
	c := make(chan struct{})

	for _, x := range expectations {
		go func(e *RequestExpectation) {
			defer GinkgoRecover()
			expectHandlerResponse(e.handler, e.request, e.responseStatus, e.responseBody)
			c <- struct{}{}
		}(x)
	}

	misc.RunWithTimeout(func() {
		for range expectations {
			<-c
		}
	}, func() {
		Fail("Not all batch requests responded on time")
	}, testTimeout)
}

func allHandlers(gateway *HandleT) map[string]http.HandlerFunc {
	return map[string]http.HandlerFunc{
		"alias":    gateway.webAliasHandler,
		"batch":    gateway.webBatchHandler,
		"group":    gateway.webGroupHandler,
		"identify": gateway.webIdentifyHandler,
		"page":     gateway.webPageHandler,
		"screen":   gateway.webScreenHandler,
		"track":    gateway.webTrackHandler,
	}
}

// converts a job list to a map of empty errors, to emulate a successful jobsdb.Store response
func jobsToEmptyErrors(jobs []*jobsdb.JobT) map[uuid.UUID]string {
	return make(map[uuid.UUID]string)
}
