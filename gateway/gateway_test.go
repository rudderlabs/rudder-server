package gateway

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/gofrs/uuid"
	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"

	"github.com/rudderlabs/rudder-server/admin"
	"github.com/rudderlabs/rudder-server/app"
	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/gateway/response"
	"github.com/rudderlabs/rudder-server/jobsdb"
	mocksApp "github.com/rudderlabs/rudder-server/mocks/app"
	mocksBackendConfig "github.com/rudderlabs/rudder-server/mocks/config/backend-config"
	mocksJobsDB "github.com/rudderlabs/rudder-server/mocks/jobsdb"
	mocksRateLimiter "github.com/rudderlabs/rudder-server/mocks/rate-limiter"
	mocksTypes "github.com/rudderlabs/rudder-server/mocks/utils/types"
	"github.com/rudderlabs/rudder-server/services/rsources"
	"github.com/rudderlabs/rudder-server/services/stats"
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

	SuppressedUserID = "suppressed-user-2"
	NormalUserID     = "normal-user-1"
)

var testTimeout = 15 * time.Second

// This configuration is assumed by all gateway tests and, is returned on Subscribe of mocked backend config
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

type testContext struct {
	asyncHelper testutils.AsyncTestHelper

	mockCtrl          *gomock.Controller
	mockJobsDB        *mocksJobsDB.MockJobsDB
	mockBackendConfig *mocksBackendConfig.MockBackendConfig
	mockApp           *mocksApp.MockInterface
	mockRateLimiter   *mocksRateLimiter.MockRateLimiter

	mockVersionHandler func(w http.ResponseWriter, r *http.Request)

	// Enterprise mocks
	mockSuppressUser        *mocksTypes.MockSuppressUserI
	mockSuppressUserFeature *mocksApp.MockSuppressUserFeature
}

func (c *testContext) initializeAppFeatures() {
	c.mockApp.EXPECT().Features().Return(&app.Features{}).AnyTimes()
}

func (c *testContext) initializeEnterpriseAppFeatures() {
	enterpriseFeatures := &app.Features{
		SuppressUser: c.mockSuppressUserFeature,
	}
	c.mockApp.EXPECT().Features().Return(enterpriseFeatures).AnyTimes()
}

func setAllowReqsWithoutUserIDAndAnonymousID(allow bool) {
	allowReqsWithoutUserIDAndAnonymousID = allow
}

// Initialise mocks and common expectations
func (c *testContext) Setup() {
	c.asyncHelper.Setup()
	c.mockCtrl = gomock.NewController(GinkgoT())
	c.mockJobsDB = mocksJobsDB.NewMockJobsDB(c.mockCtrl)
	c.mockBackendConfig = mocksBackendConfig.NewMockBackendConfig(c.mockCtrl)
	c.mockApp = mocksApp.NewMockInterface(c.mockCtrl)
	c.mockRateLimiter = mocksRateLimiter.NewMockRateLimiter(c.mockCtrl)

	tFunc := c.asyncHelper.ExpectAndNotifyCallbackWithName("process_config")

	c.mockBackendConfig.EXPECT().Subscribe(gomock.Any(), backendconfig.TopicProcessConfig).
		DoAndReturn(func(ctx context.Context, topic backendconfig.Topic) pubsub.DataChannel {
			tFunc()

			ch := make(chan pubsub.DataEvent, 1)
			ch <- pubsub.DataEvent{Data: sampleBackendConfig, Topic: string(topic)}
			// on Subscribe, emulate a backend configuration event
			go func() {
				<-ctx.Done()
				close(ch)
			}()
			return ch
		})
	c.mockVersionHandler = func(w http.ResponseWriter, r *http.Request) {}
}

func (c *testContext) Finish() {
	c.asyncHelper.WaitWithTimeout(testTimeout)
	c.mockCtrl.Finish()
}

var _ = Describe("Reconstructing JSON for ServerSide SDK", func() {
	gateway := &HandleT{}
	_ = DescribeTable("newDSIdx tests",
		func(inputKey, value string) {
			testValidBody := `{"batch":[
                {"anonymousId":"anon_id_1","event":"event_1_1"},
                {"anonymousId":"anon_id_2","event":"event_2_1"},
                {"anonymousId":"anon_id_3","event":"event_3_1"},
                {"anonymousId":"anon_id_1","event":"event_1_2"},
                {"anonymousId":"anon_id_2","event":"event_2_2"},
                {"anonymousId":"anon_id_1","event":"event_1_3"}
            ]}`
			response, payloadError := gateway.getUsersPayload([]byte(testValidBody))
			key, err := misc.GetMD5UUID(inputKey)
			Expect(string(response[key.String()])).To(Equal(value))
			Expect(err).To(BeNil())
			Expect(payloadError).To(BeNil())
		},
		Entry("Expected JSON for Key 1 Test 1 : ", ":anon_id_1", `{"batch":[{"anonymousId":"anon_id_1","event":"event_1_1"},{"anonymousId":"anon_id_1","event":"event_1_2"},{"anonymousId":"anon_id_1","event":"event_1_3"}]}`),
		Entry("Expected JSON for Key 2 Test 1 : ", ":anon_id_2", `{"batch":[{"anonymousId":"anon_id_2","event":"event_2_1"},{"anonymousId":"anon_id_2","event":"event_2_2"}]}`),
		Entry("Expected JSON for Key 3 Test 1 : ", ":anon_id_3", `{"batch":[{"anonymousId":"anon_id_3","event":"event_3_1"}]}`),
	)
})

func initGW() {
	config.Load()
	admin.Init()
	logger.Init()
	misc.Init()
	Init()
}

var _ = Describe("Gateway Enterprise", func() {
	initGW()

	var c *testContext

	BeforeEach(func() {
		c = &testContext{}
		c.Setup()

		c.mockSuppressUser = mocksTypes.NewMockSuppressUserI(c.mockCtrl)
		c.mockSuppressUserFeature = mocksApp.NewMockSuppressUserFeature(c.mockCtrl)
		c.initializeEnterpriseAppFeatures()

		c.mockSuppressUserFeature.EXPECT().Setup(gomock.Any()).AnyTimes().Return(c.mockSuppressUser, nil)
		c.mockSuppressUser.EXPECT().IsSuppressedUser(NormalUserID, SourceIDEnabled, WriteKeyEnabled).Return(false).AnyTimes()
		c.mockSuppressUser.EXPECT().IsSuppressedUser(SuppressedUserID, SourceIDEnabled, WriteKeyEnabled).Return(true).AnyTimes()

		// setup static requirements of dependencies
		stats.Setup()

		// setup common environment, override in BeforeEach when required
		SetEnableRateLimit(false)
		SetEnableSuppressUserFeature(true)
		SetEnableEventSchemasFeature(false)
		// SetUserWebRequestBatchTimeout(time.Second)
		// SetMaxDBBatchSize(1)
	})

	AfterEach(func() {
		c.Finish()
	})

	Context("Suppress users", func() {
		gateway := &HandleT{}

		BeforeEach(func() {
			err := gateway.Setup(c.mockApp, c.mockBackendConfig, c.mockJobsDB, nil, c.mockVersionHandler, rsources.NewNoOpService())
			Expect(err).To(BeNil())
		})

		It("should not accept events from suppress users", func() {
			suppressedUserEventData := fmt.Sprintf(`{"batch":[{"userId":%q}]}`, SuppressedUserID)
			// Why GET
			expectHandlerResponse(gateway.webBatchHandler, authorizedRequest(WriteKeyEnabled, bytes.NewBufferString(suppressedUserEventData)), 200, "OK")
		})

		It("should accept events from normal users", func() {
			allowedUserEventData := fmt.Sprintf(`{"batch":[{"userId":%q}]}`, NormalUserID)

			c.mockJobsDB.EXPECT().WithStoreSafeTx(gomock.Any()).Times(1).Do(func(f func(tx jobsdb.StoreSafeTx) error) {
				_ = f(jobsdb.EmptyStoreSafeTx())
			}).Return(nil)
			mockCall := c.mockJobsDB.EXPECT().StoreWithRetryEachInTx(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(jobsToEmptyErrors).Times(1)
			tFunc := c.asyncHelper.ExpectAndNotifyCallbackWithName("store-job")
			mockCall.Do(func(context.Context, interface{}, interface{}) { tFunc() })

			// Why GET
			expectHandlerResponse(gateway.webBatchHandler, authorizedRequest(WriteKeyEnabled, bytes.NewBufferString(allowedUserEventData)), 200, "OK")
		})
	})
})

var _ = Describe("Gateway", func() {
	initGW()

	var c *testContext

	BeforeEach(func() {
		c = &testContext{}
		c.Setup()
		c.initializeAppFeatures()

		// setup static requirements of dependencies
		stats.Setup()

		// setup common environment, override in BeforeEach when required
		SetEnableRateLimit(false)
		SetEnableEventSchemasFeature(false)
	})

	AfterEach(func() {
		c.Finish()
	})

	Context("Initialization", func() {
		It("should wait for backend config", func() {
			gateway := &HandleT{}
			err := gateway.Setup(c.mockApp, c.mockBackendConfig, c.mockJobsDB, nil, c.mockVersionHandler, rsources.NewNoOpService())
			Expect(err).To(BeNil())
			err = gateway.Shutdown()
			Expect(err).To(BeNil())
		})
	})

	Context("Valid requests", func() {
		var (
			gateway           *HandleT
			gatewayBatchCalls = 1
		)

		// tracks expected batch_id
		nextBatchID := func() (batchID int) {
			batchID = gatewayBatchCalls
			gatewayBatchCalls++
			return
		}

		BeforeEach(func() {
			gateway = &HandleT{}
			err := gateway.Setup(c.mockApp, c.mockBackendConfig, c.mockJobsDB, nil, c.mockVersionHandler, rsources.NewNoOpService())
			Expect(err).To(BeNil())
		})

		AfterEach(func() {
			err := gateway.Shutdown()
			Expect(err).To(BeNil())
		})

		assertJobMetadata := func(job *jobsdb.JobT, batchLength, batchId int) {
			Expect(misc.IsValidUUID(job.UUID.String())).To(Equal(true))

			var paramsMap, expectedParamsMap map[string]interface{}
			_ = json.Unmarshal(job.Parameters, &paramsMap)
			expectedStr := []byte(fmt.Sprintf(`{"source_id": "%v", "batch_id": %d, "source_job_run_id": "", "source_task_run_id": ""}`, SourceIDEnabled, batchId))
			_ = json.Unmarshal(expectedStr, &expectedParamsMap)
			equals := reflect.DeepEqual(paramsMap, expectedParamsMap)
			Expect(equals).To(Equal(true))

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

		createValidBody := func(customProperty, customValue string) []byte {
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
		It("should accept valid requests on a single endpoint (except batch), and store to jobsdb", func() {
			for handlerType, handler := range allHandlers(gateway) {
				if !(handlerType == "batch" || handlerType == "import") {

					validBody := createValidBody("custom-property", "custom-value")

					c.mockJobsDB.EXPECT().WithStoreSafeTx(gomock.Any()).Times(1).Do(func(f func(tx jobsdb.StoreSafeTx) error) {
						_ = f(jobsdb.EmptyStoreSafeTx())
					}).Return(nil)
					c.mockJobsDB.
						EXPECT().StoreWithRetryEachInTx(gomock.Any(), gomock.Any(), gomock.Any()).
						DoAndReturn(func(ctx context.Context, tx jobsdb.StoreSafeTx, jobs []*jobsdb.JobT) (map[uuid.UUID]string, error) {
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

							return jobsToEmptyErrors(ctx, tx, jobs)
						}).
						Times(1)

					expectHandlerResponse(handler, authorizedRequest(WriteKeyEnabled, bytes.NewBuffer(validBody)), 200, "OK")
				}
			}
		})
	})

	Context("Rate limits", func() {
		var gateway *HandleT

		BeforeEach(func() {
			gateway = &HandleT{}
			SetEnableRateLimit(true)
			err := gateway.Setup(c.mockApp, c.mockBackendConfig, c.mockJobsDB, c.mockRateLimiter, c.mockVersionHandler, rsources.NewNoOpService())
			Expect(err).To(BeNil())
		})

		AfterEach(func() {
			err := gateway.Shutdown()
			Expect(err).To(BeNil())
		})

		It("should store messages successfully if rate limit is not reached for workspace", func() {
			workspaceID := "some-workspace-id"

			mockCall := c.mockBackendConfig.EXPECT().GetWorkspaceIDForWriteKey(WriteKeyEnabled).Return(workspaceID).AnyTimes()
			tFunc := c.asyncHelper.ExpectAndNotifyCallbackWithName("")
			mockCall.Do(func(interface{}) { tFunc() })

			mockCall = c.mockRateLimiter.EXPECT().LimitReached(workspaceID).Return(false).Times(1)
			tFunc = c.asyncHelper.ExpectAndNotifyCallbackWithName("")
			mockCall.Do(func(interface{}) { tFunc() })

			c.mockJobsDB.EXPECT().WithStoreSafeTx(gomock.Any()).Times(1).Do(func(f func(tx jobsdb.StoreSafeTx) error) {
				_ = f(jobsdb.EmptyStoreSafeTx())
			}).Return(nil)
			mockCall = c.mockJobsDB.EXPECT().StoreWithRetryEachInTx(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(jobsToEmptyErrors).Times(1)
			tFunc = c.asyncHelper.ExpectAndNotifyCallbackWithName("")
			mockCall.Do(func(context.Context, interface{}, interface{}) { tFunc() })

			expectHandlerResponse(gateway.webAliasHandler, authorizedRequest(WriteKeyEnabled, bytes.NewBufferString(`{"userId":"dummyId"}`)), 200, "OK")
		})

		It("should reject messages if rate limit is reached for workspace", func() {
			workspaceID := "some-workspace-id"

			mockCall := c.mockBackendConfig.EXPECT().GetWorkspaceIDForWriteKey(WriteKeyEnabled).Return(workspaceID).AnyTimes()
			tFunc := c.asyncHelper.ExpectAndNotifyCallbackWithName("")
			mockCall.Do(func(interface{}) { tFunc() })

			c.mockRateLimiter.EXPECT().LimitReached(workspaceID).Return(true).Times(1)
			tFunc = c.asyncHelper.ExpectAndNotifyCallbackWithName("")
			mockCall.Do(func(interface{}) { tFunc() })

			expectHandlerResponse(gateway.webAliasHandler, authorizedRequest(WriteKeyEnabled, bytes.NewBufferString("{}")), 429, response.TooManyRequests+"\n")
		})
	})

	Context("Invalid requests", func() {
		var gateway *HandleT

		BeforeEach(func() {
			gateway = &HandleT{}
			err := gateway.Setup(c.mockApp, c.mockBackendConfig, c.mockJobsDB, nil, c.mockVersionHandler, rsources.NewNoOpService())
			Expect(err).To(BeNil())
		})

		AfterEach(func() {
			err := gateway.Shutdown()
			Expect(err).To(BeNil())
		})

		createJSONBody := func(customProperty, customValue string) []byte {
			validData := `{"userId":"dummyId","data":{"string":"valid-json","nested":{"child":1}}}`
			validDataWithProperty, _ := sjson.SetBytes([]byte(validData), customProperty, customValue)

			return validDataWithProperty
		}

		// common tests for all web handlers
		It("should reject requests without Authorization header", func() {
			for _, handler := range allHandlers(gateway) {
				expectHandlerResponse(handler, unauthorizedRequest(nil), 401, response.NoWriteKeyInBasicAuth+"\n")
			}
		})

		It("should reject requests without username in Authorization header", func() {
			for _, handler := range allHandlers(gateway) {
				expectHandlerResponse(handler, authorizedRequest(WriteKeyEmpty, nil), 401, response.NoWriteKeyInBasicAuth+"\n")
			}
		})

		It("should reject requests without valid rudder event in request body", func() {
			for handlerType, handler := range allHandlers(gateway) {
				notRudderEvent := `[{"data": "valid-json","foo":"bar"}]`
				if handlerType == "batch" || handlerType == "import" {
					notRudderEvent = `{"batch": [[{"data": "valid-json","foo":"bar"}]]}`
				}
				setAllowReqsWithoutUserIDAndAnonymousID(true)
				expectHandlerResponse(handler, authorizedRequest(WriteKeyEnabled, bytes.NewBufferString(notRudderEvent)), 400, response.NotRudderEvent+"\n")
				setAllowReqsWithoutUserIDAndAnonymousID(false)
			}
		})

		It("should reject requests with both userId and anonymousId not present", func() {
			for handlerType, handler := range allHandlers(gateway) {
				validBody := `{"data": "valid-json"}`
				if handlerType == "batch" || handlerType == "import" {
					validBody = `{"batch": [{"data": "valid-json"}]}`
				}
				expectHandlerResponse(handler, authorizedRequest(WriteKeyEnabled, bytes.NewBufferString(validBody)), 400, response.NonIdentifiableRequest+"\n")
			}
		})

		It("should reject requests without request body", func() {
			for _, handler := range allHandlers(gateway) {
				expectHandlerResponse(handler, authorizedRequest(WriteKeyInvalid, nil), 400, response.RequestBodyNil+"\n")
			}
		})

		It("should reject requests without valid json in request body", func() {
			for _, handler := range allHandlers(gateway) {
				invalidBody := "not-a-valid-json"
				expectHandlerResponse(handler, authorizedRequest(WriteKeyInvalid, bytes.NewBufferString(invalidBody)), 400, response.InvalidJSON+"\n")
			}
		})

		It("should reject requests with request bodies larger than configured limit", func() {
			for handlerType, handler := range allHandlers(gateway) {
				data := make([]byte, gateway.MaxReqSize())
				for i := range data {
					data[i] = 'a'
				}
				body := `{
                    "anonymousId": "anon_id"
                  }`
				body, _ = sjson.Set(body, "properties", data)
				if handlerType == "batch" || handlerType == "import" {
					body = fmt.Sprintf(`{"batch":[%s]}`, body)
				}
				if handlerType != "audiencelist" {
					expectHandlerResponse(handler, authorizedRequest(WriteKeyEnabled, bytes.NewBufferString(body)), 413, response.RequestBodyTooLarge+"\n")
				}
			}
		})

		It("should reject requests with invalid write keys", func() {
			for handlerType, handler := range allHandlers(gateway) {
				validBody := `{"data":"valid-json"}`
				if handlerType == "batch" || handlerType == "import" {
					validBody = `{"batch":[{"data":"valid-json"}]}`
				}
				expectHandlerResponse(handler, authorizedRequest(WriteKeyInvalid, bytes.NewBufferString(validBody)), 401, response.InvalidWriteKey+"\n")
			}
		})

		It("should reject requests with disabled write keys (source)", func() {
			for handlerType, handler := range allHandlers(gateway) {
				validBody := `{"data":"valid-json"}`
				if handlerType == "batch" || handlerType == "import" {
					validBody = `{"batch":[{"data":"valid-json"}]}`
				}
				expectHandlerResponse(handler, authorizedRequest(WriteKeyDisabled, bytes.NewBufferString(validBody)), 404, response.SourceDisabled+"\n")
			}
		})

		It("should reject requests with 500 if jobsdb store returns an error", func() {
			for handlerType, handler := range allHandlers(gateway) {
				if handlerType != "import" {
					validBody := createJSONBody("custom-property", "custom-value")

					c.mockJobsDB.EXPECT().WithStoreSafeTx(gomock.Any()).Times(1).Do(func(f func(tx jobsdb.StoreSafeTx) error) {
						_ = f(jobsdb.EmptyStoreSafeTx())
					}).Return(nil)
					c.mockJobsDB.
						EXPECT().StoreWithRetryEachInTx(gomock.Any(), gomock.Any(), gomock.Any()).
						DoAndReturn(jobsToJobsdbErrors).
						Times(1)

					expectHandlerResponse(handler, authorizedRequest(WriteKeyEnabled, bytes.NewBuffer(validBody)), 500, "tx error"+"\n")
				}
			}
		})
	})

	Context("Robots", func() {
		var gateway *HandleT

		BeforeEach(func() {
			gateway = &HandleT{}
			err := gateway.Setup(c.mockApp, c.mockBackendConfig, c.mockJobsDB, nil, c.mockVersionHandler, rsources.NewNoOpService())
			Expect(err).To(BeNil())
		})

		AfterEach(func() {
			err := gateway.Shutdown()
			Expect(err).To(BeNil())
		})

		It("should return a robots.txt", func() {
			expectHandlerResponse(gateway.robots, nil, 200, "User-agent: * \nDisallow: / \n")
		})
	})

	Context("Warehouse proxy", func() {
		DescribeTable("forwarding requests to warehouse with different response codes",
			func(url string, code int, payload string) {
				gateway := &HandleT{}
				whMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					Expect(r.URL.String()).To(Equal(url))
					Expect(r.Body)
					Expect(r.Body).To(Not(BeNil()))
					defer r.Body.Close()
					reqBody, err := io.ReadAll(r.Body)
					Expect(err).To(BeNil())
					Expect(string(reqBody)).To(Equal(payload))
					w.WriteHeader(code)
				}))
				err := os.Setenv("WAREHOUSE_URL", whMock.URL)
				Expect(err).To(BeNil())
				err = os.Setenv("RSERVER_WAREHOUSE_MODE", config.OffMode)
				Expect(err).To(BeNil())
				err = gateway.Setup(c.mockApp, c.mockBackendConfig, c.mockJobsDB, nil, c.mockVersionHandler, rsources.NewNoOpService())
				Expect(err).To(BeNil())

				defer func() {
					err := gateway.Shutdown()
					Expect(err).To(BeNil())
					whMock.Close()
				}()

				req := httptest.NewRequest("POST", "http://rudder-server"+url, bytes.NewBufferString(payload))
				w := httptest.NewRecorder()
				gateway.whProxy.ServeHTTP(w, req)
				resp := w.Result()
				Expect(resp.StatusCode).To(Equal(code))
			},
			Entry("successful request", "/v1/warehouse/pending-events", http.StatusOK, `{"source_id": "1", "task_run_id":"2"}`),
			Entry("failed request", "/v1/warehouse/pending-events", http.StatusBadRequest, `{"source_id": "3", "task_run_id":"4"}`),
			Entry("request with query parameters", "/v1/warehouse/pending-events?triggerUpload=true", http.StatusOK, `{"source_id": "5", "task_run_id":"6"}`),
		)
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

		bodyBytes, _ := io.ReadAll(rr.Body)
		body := string(bodyBytes)

		Expect(rr.Result().StatusCode).To(Equal(responseStatus))
		Expect(body).To(Equal(responseBody))
	}, testTimeout)
}

func allHandlers(gateway *HandleT) map[string]http.HandlerFunc {
	return map[string]http.HandlerFunc{
		"alias":        gateway.webAliasHandler,
		"batch":        gateway.webBatchHandler,
		"group":        gateway.webGroupHandler,
		"identify":     gateway.webIdentifyHandler,
		"page":         gateway.webPageHandler,
		"screen":       gateway.webScreenHandler,
		"track":        gateway.webTrackHandler,
		"import":       gateway.webImportHandler,
		"audiencelist": gateway.webAudienceListHandler,
	}
}

// converts a job list to a map of empty errors, to emulate a successful jobsdb.Store response
func jobsToEmptyErrors(_ context.Context, _ jobsdb.StoreSafeTx, _ []*jobsdb.JobT) (map[uuid.UUID]string, error) {
	return make(map[uuid.UUID]string), nil
}

// converts a job list to a map of empty errors, to emulate a successful jobsdb.Store response
func jobsToJobsdbErrors(_ context.Context, _ jobsdb.StoreSafeTx, jobs []*jobsdb.JobT) (map[uuid.UUID]string, error) {
	errorsMap := make(map[uuid.UUID]string, len(jobs))
	for _, job := range jobs {
		errorsMap[job.UUID] = "tx error"
	}

	return errorsMap, nil
}

func TestContentTypeFunction(t *testing.T) {
	expectedContentType := "application/json; charset=utf-8"
	expectedStatus := http.StatusOK
	nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// setting it custom to verify that next handler is called
		w.WriteHeader(expectedStatus)
	})
	handlerToTest := WithContentType(expectedContentType, nextHandler)
	respRecorder := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "http://testing", nil)

	handlerToTest.ServeHTTP(respRecorder, req)
	receivedContentType := respRecorder.Header()["Content-Type"][0]
	require.Equal(t, expectedContentType, receivedContentType, "actual content type different than expected.")
	require.Equal(t, expectedStatus, respRecorder.Code, "actual response code different than expected.")
}
