package gateway

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strconv"
	"testing"
	"time"

	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
	"github.com/rudderlabs/rudder-server/admin"
	"github.com/rudderlabs/rudder-server/app"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/gateway/response"
	"github.com/rudderlabs/rudder-server/jobsdb"
	mocksApp "github.com/rudderlabs/rudder-server/mocks/app"
	mocksBackendConfig "github.com/rudderlabs/rudder-server/mocks/backend-config"
	mockGateway "github.com/rudderlabs/rudder-server/mocks/gateway"
	mocksJobsDB "github.com/rudderlabs/rudder-server/mocks/jobsdb"
	mocksTypes "github.com/rudderlabs/rudder-server/mocks/utils/types"
	sourcedebugger "github.com/rudderlabs/rudder-server/services/debugger/source"
	"github.com/rudderlabs/rudder-server/services/rsources"
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
	WorkspaceID      = "workspace"
	sourceType1      = "sourceType1"
	sourceType2      = "sourceType2"
	sdkLibrary       = "sdkLibrary"
	sdkVersion       = "v1.2.3"
)

var (
	testTimeout = 15 * time.Second
	sdkContext  = fmt.Sprintf(
		`"context":{"library":{"name": %[1]q, "version":%[2]q}}`,
		sdkLibrary,
		sdkVersion,
	)
	sdkStatTag = fmt.Sprintf(
		`%[1]s/%[2]s`,
		sdkLibrary,
		sdkVersion,
	)
)

// This configuration is assumed by all gateway tests and, is returned on Subscribe of mocked backend config
var sampleBackendConfig = backendconfig.ConfigT{
	WorkspaceID: WorkspaceID,
	Sources: []backendconfig.SourceT{
		{
			ID:       SourceIDDisabled,
			WriteKey: WriteKeyDisabled,
			Enabled:  false,
			SourceDefinition: backendconfig.SourceDefinitionT{
				Category: sourceType1,
			},
		},
		{
			ID:       SourceIDEnabled,
			WriteKey: WriteKeyEnabled,
			Enabled:  true,
			SourceDefinition: backendconfig.SourceDefinitionT{
				Category: sourceType2,
			},
			WorkspaceID: WorkspaceID,
		},
	},
}

type testContext struct {
	asyncHelper testutils.AsyncTestHelper

	mockCtrl           *gomock.Controller
	mockJobsDB         *mocksJobsDB.MockJobsDB
	mockBackendConfig  *mocksBackendConfig.MockBackendConfig
	mockRateLimiter    *mockGateway.MockThrottler
	mockApp            *mocksApp.MockApp
	mockWebhook        *mockGateway.MockWebhook
	mockVersionHandler func(w http.ResponseWriter, r *http.Request)

	// Enterprise mocks
	mockSuppressUser        *mocksTypes.MockUserSuppression
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
	c.mockApp = mocksApp.NewMockApp(c.mockCtrl)
	c.mockRateLimiter = mockGateway.NewMockThrottler(c.mockCtrl)
	c.mockWebhook = mockGateway.NewMockWebhook(c.mockCtrl)
	c.mockWebhook.EXPECT().Shutdown().AnyTimes()
	c.mockBackendConfig.EXPECT().Subscribe(gomock.Any(), backendconfig.TopicProcessConfig).
		DoAndReturn(func(ctx context.Context, topic backendconfig.Topic) pubsub.DataChannel {
			ch := make(chan pubsub.DataEvent, 1)
			ch <- pubsub.DataEvent{Data: map[string]backendconfig.ConfigT{WorkspaceID: sampleBackendConfig}, Topic: string(topic)}
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

func initGW() {
	config.Reset()
	admin.Init()
	logger.Reset()
	misc.Init()
	Init()
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

var _ = Describe("Gateway Enterprise", func() {
	initGW()

	var (
		c          *testContext
		gateway    *HandleT
		statsStore *memstats.Store
	)

	BeforeEach(func() {
		c = &testContext{}
		c.Setup()

		c.mockSuppressUser = mocksTypes.NewMockUserSuppression(c.mockCtrl)
		c.mockSuppressUserFeature = mocksApp.NewMockSuppressUserFeature(c.mockCtrl)
		c.initializeEnterpriseAppFeatures()

		c.mockSuppressUserFeature.EXPECT().Setup(gomock.Any(), gomock.Any()).AnyTimes().Return(c.mockSuppressUser, nil)
		c.mockSuppressUser.EXPECT().IsSuppressedUser(WorkspaceID, NormalUserID, SourceIDEnabled).Return(false).AnyTimes()
		c.mockSuppressUser.EXPECT().IsSuppressedUser(WorkspaceID, SuppressedUserID, SourceIDEnabled).Return(true).AnyTimes()
		// setup common environment, override in BeforeEach when required
		SetEnableRateLimit(false)
		SetEnableSuppressUserFeature(true)
		SetEnableEventSchemasFeature(false)
	})

	AfterEach(func() {
		c.Finish()
	})

	Context("Suppress users", func() {
		gateway = &HandleT{}

		BeforeEach(func() {
			err := gateway.Setup(context.Background(), c.mockApp, c.mockBackendConfig, c.mockJobsDB, nil, c.mockVersionHandler, rsources.NewNoOpService(), sourcedebugger.NewNoOpService())
			Expect(err).To(BeNil())
			statsStore = memstats.New()
			gateway.stats = statsStore
		})

		It("should not accept events from suppress users", func() {
			suppressedUserEventData := fmt.Sprintf(`{"batch":[{"userId":%q}]}`, SuppressedUserID)
			// Why GET
			expectHandlerResponse(gateway.webBatchHandler, authorizedRequest(WriteKeyEnabled, bytes.NewBufferString(suppressedUserEventData)), http.StatusOK, "OK")
			Eventually(
				func() bool {
					stat := statsStore.Get(
						"gateway.write_key_suppressed_requests",
						map[string]string{
							"source":      gateway.getSourceTagFromWriteKey(WriteKeyEnabled),
							"sourceID":    gateway.getSourceIDForWriteKey(WriteKeyEnabled),
							"workspaceId": getWorkspaceID(WriteKeyEnabled),
							"writeKey":    WriteKeyEnabled,
							"reqType":     "batch",
							"sourceType":  sourceType2,
							"sdkVersion":  "",
						},
					)
					return stat != nil && stat.LastValue() == float64(1)
				},
				1*time.Second,
			).Should(BeTrue())
		})

		It("should accept events from normal users", func() {
			allowedUserEventData := fmt.Sprintf(
				`{"batch":[{"userId":%[1]q,%[2]s}]}`,
				NormalUserID,
				sdkContext,
			)
			c.mockJobsDB.EXPECT().WithStoreSafeTx(
				gomock.Any(),
				gomock.Any(),
			).Times(1).Do(func(ctx context.Context, f func(tx jobsdb.StoreSafeTx) error) {
				_ = f(jobsdb.EmptyStoreSafeTx())
			}).Return(nil)
			mockCall := c.mockJobsDB.EXPECT().StoreEachBatchRetryInTx(
				gomock.Any(),
				gomock.Any(),
				gomock.Any(),
			).DoAndReturn(jobsToEmptyErrors).Times(1)
			tFunc := c.asyncHelper.ExpectAndNotifyCallbackWithName("store-job")
			mockCall.Do(func(context.Context, interface{}, interface{}) { tFunc() })

			// Why GET
			expectHandlerResponse(
				gateway.webBatchHandler,
				authorizedRequest(
					WriteKeyEnabled,
					bytes.NewBufferString(allowedUserEventData),
				),
				http.StatusOK,
				"OK",
			)
			Eventually(
				func() bool {
					stat := statsStore.Get(
						"gateway.write_key_successful_requests",
						map[string]string{
							"source":      gateway.getSourceTagFromWriteKey(WriteKeyEnabled),
							"sourceID":    gateway.getSourceIDForWriteKey(WriteKeyEnabled),
							"workspaceId": getWorkspaceID(WriteKeyEnabled),
							"writeKey":    WriteKeyEnabled,
							"reqType":     "batch",
							"sourceType":  sourceType2,
							"sdkVersion":  sdkStatTag,
						},
					)
					return stat != nil && stat.LastValue() == float64(1)
				},
				1*time.Second,
			).Should(BeTrue())
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
			err := gateway.Setup(context.Background(), c.mockApp, c.mockBackendConfig, c.mockJobsDB, nil, c.mockVersionHandler, rsources.NewNoOpService(), sourcedebugger.NewNoOpService())
			Expect(err).To(BeNil())
			err = gateway.Shutdown()
			Expect(err).To(BeNil())
		})
	})

	Context("Test All endpoints", func() {
		var (
			gateway    *HandleT
			statsStore *memstats.Store
			whServer   *httptest.Server
			serverURL  string
		)

		BeforeEach(func() {
			whServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				_, _ = io.WriteString(w, "OK")
			}))
			WHURL := whServer.URL
			parsedURL, err := url.Parse(WHURL)
			Expect(err).To(BeNil())
			whPort := parsedURL.Port()
			GinkgoT().Setenv("RSERVER_WAREHOUSE_WEB_PORT", whPort)

			serverPort, err := kithelper.GetFreePort()
			Expect(err).To(BeNil())
			serverURL = fmt.Sprintf("http://localhost:%d", serverPort)
			GinkgoT().Setenv("RSERVER_GATEWAY_WEB_PORT", strconv.Itoa(serverPort))

			loadConfig()

			gateway = &HandleT{}
			err = gateway.Setup(context.Background(), c.mockApp, c.mockBackendConfig, c.mockJobsDB, nil, c.mockVersionHandler, rsources.NewNoOpService(), sourcedebugger.NewNoOpService())
			Expect(err).To(BeNil())
			gateway.irh = mockRequestHandler{}
			gateway.rrh = mockRequestHandler{}
			gateway.webhookHandler = c.mockWebhook
			statsStore = memstats.New()
			gateway.stats = statsStore
		})
		AfterEach(func() {
			err := gateway.Shutdown()
			Expect(err).To(BeNil())
			whServer.Close()
		})

		createValidBody := func(customProperty, customValue string) []byte {
			validData := fmt.Sprintf(
				`{"userId":"dummyId","data":{"string":"valid-json","nested":{"child":1}},%s}`,
				sdkContext,
			)
			validDataWithProperty, _ := sjson.SetBytes([]byte(validData), customProperty, customValue)

			return validDataWithProperty
		}
		verifyEndpoint := func(endpoints []string, method string) {
			client := &http.Client{}
			for _, ep := range endpoints {
				url := fmt.Sprintf("%s%s", serverURL, ep)
				var req *http.Request
				var err error
				if ep == "/beacon/v1/batch" {
					url = url + "?writeKey=WriteKeyEnabled"
				}

				if method == http.MethodGet {
					req, err = http.NewRequest(method, url, nil)
				} else {
					req, err = http.NewRequest(method, url, bytes.NewBuffer(createValidBody("custom-property", "custom-value")))
				}
				Expect(err).To(BeNil())
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(WriteKeyEnabled+":")))
				resp, err := client.Do(req)
				Expect(err).To(BeNil())

				Expect(resp.StatusCode).To(SatisfyAny(Equal(http.StatusOK), Equal(http.StatusNoContent)))

			}
		}
		It("should be able to hit all the handlers", func() {
			c.mockWebhook.EXPECT().RequestHandler(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				_, _ = io.WriteString(w, "OK")
			})
			c.mockBackendConfig.EXPECT().WaitForConfig(gomock.Any()).AnyTimes()
			var err error
			wait := make(chan struct{})
			ctx, cancel := context.WithCancel(context.Background())
			go func() {
				err = gateway.StartWebHandler(ctx)
				Expect(err).To(BeNil())
				close(wait)
			}()
			Eventually(func() bool {
				resp, err := http.Get(fmt.Sprintf("%s/version", serverURL))
				if err != nil {
					return false
				}
				return resp.StatusCode == http.StatusOK
			}, time.Second*10, time.Second).Should(BeTrue())

			getEndpoint, postEndpoints, deleteEndpoints := endpointsToVerify()
			verifyEndpoint(getEndpoint, http.MethodGet)
			verifyEndpoint(postEndpoints, http.MethodPost)
			verifyEndpoint(deleteEndpoints, http.MethodDelete)
			cancel()
			<-wait
		})
	})

	Context("Valid requests", func() {
		var (
			gateway    *HandleT
			statsStore *memstats.Store
		)

		BeforeEach(func() {
			gateway = &HandleT{}
			err := gateway.Setup(context.Background(), c.mockApp, c.mockBackendConfig, c.mockJobsDB, nil, c.mockVersionHandler, rsources.NewNoOpService(), sourcedebugger.NewNoOpService())
			Expect(err).To(BeNil())
			statsStore = memstats.New()
			gateway.stats = statsStore
		})

		AfterEach(func() {
			err := gateway.Shutdown()
			Expect(err).To(BeNil())
		})

		assertJobMetadata := func(job *jobsdb.JobT) {
			Expect(misc.IsValidUUID(job.UUID.String())).To(Equal(true))

			var paramsMap, expectedParamsMap map[string]interface{}
			_ = json.Unmarshal(job.Parameters, &paramsMap)
			expectedStr := []byte(fmt.Sprintf(`{"source_id": "%v", "source_job_run_id": "", "source_task_run_id": ""}`, SourceIDEnabled))
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
			Expect(batch.Array()).To(HaveLen(1)) // each batch split into multiple batches of 1 event
		}

		createValidBody := func(customProperty, customValue string) []byte {
			validData := fmt.Sprintf(
				`{"userId":"dummyId","data":{"string":"valid-json","nested":{"child":1}},%s}`,
				sdkContext,
			)
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

					c.mockJobsDB.EXPECT().WithStoreSafeTx(
						gomock.Any(),
						gomock.Any()).Times(1).Do(func(
						ctx context.Context,
						f func(tx jobsdb.StoreSafeTx) error,
					) {
						_ = f(jobsdb.EmptyStoreSafeTx())
					}).Return(nil)
					c.mockJobsDB.
						EXPECT().StoreEachBatchRetryInTx(
						gomock.Any(),
						gomock.Any(),
						gomock.Any(),
					).
						DoAndReturn(
							func(
								ctx context.Context,
								tx jobsdb.StoreSafeTx,
								jobBatches [][]*jobsdb.JobT,
							) (map[uuid.UUID]string, error) {
								for _, batch := range jobBatches {
									for _, job := range batch {
										// each call should be included in a separate batch, with a separate batch_id
										assertJobMetadata(job)

										responseData := []byte(job.EventPayload)
										payload := gjson.GetBytes(responseData, "batch.0")

										assertJobBatchItem(payload)

										messageType := payload.Get("type")
										Expect(messageType.String()).To(Equal(handlerType))
										Expect(stripJobPayload(payload)).To(MatchJSON(validBody))
									}
								}
								c.asyncHelper.ExpectAndNotifyCallbackWithName("jobsdb_store")()

								return jobsToEmptyErrors(ctx, tx, jobBatches)
							}).
						Times(1)

					expectHandlerResponse(
						handler,
						authorizedRequest(
							WriteKeyEnabled,
							bytes.NewBuffer(validBody)),
						http.StatusOK,
						"OK",
					)
					Eventually(
						func() bool {
							stat := statsStore.Get(
								"gateway.write_key_successful_requests",
								map[string]string{
									"source":      gateway.getSourceTagFromWriteKey(WriteKeyEnabled),
									"sourceID":    gateway.getSourceIDForWriteKey(WriteKeyEnabled),
									"workspaceId": gateway.getWorkspaceForWriteKey(WriteKeyEnabled),
									"writeKey":    WriteKeyEnabled,
									"reqType":     handlerType,
									"sourceType":  sourceType2,
									"sdkVersion":  sdkStatTag,
								},
							)
							return stat != nil && stat.LastValue() == float64(1)
						},
						1*time.Second,
					).Should(BeTrue())
				}
			}
		})
	})

	Context("Bots", func() {
		var (
			gateway    *HandleT
			statsStore *memstats.Store
		)

		BeforeEach(func() {
			gateway = &HandleT{}

			err := gateway.Setup(context.Background(), c.mockApp, c.mockBackendConfig, c.mockJobsDB, c.mockRateLimiter, c.mockVersionHandler, rsources.NewNoOpService(), sourcedebugger.NewNoOpService())
			Expect(err).To(BeNil())

			statsStore = memstats.New()
			gateway.stats = statsStore
		})

		AfterEach(func() {
			Expect(gateway.Shutdown()).To(BeNil())
		})

		It("should send bots information", func() {
			c.mockJobsDB.EXPECT().WithStoreSafeTx(gomock.Any(), gomock.Any()).Times(1).Do(func(ctx context.Context, f func(tx jobsdb.StoreSafeTx) error) {
				_ = f(jobsdb.EmptyStoreSafeTx())
			}).Return(nil)

			mockCall := c.mockJobsDB.EXPECT().StoreEachBatchRetryInTx(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(jobsToEmptyErrors).Times(1)
			mockCall.Do(func(context.Context, interface{}, interface{}) {
				c.asyncHelper.ExpectAndNotifyCallbackWithName("")()
			})

			expectHandlerResponse(
				gateway.webBatchHandler,
				authorizedRequest(
					WriteKeyEnabled,
					bytes.NewBufferString(
						fmt.Sprintf(`{
						  "batch": [
							{
							  "userId": "dummyId",
							  "context": {
								"userAgent": "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
								"library": {
								  "name": %[1]q,
								  "version": %[2]q
								}
							  }
							},
							{
							  "userId": "dummyId",
							  "context": {
								"userAgent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36",
								"library": {
								  "name": %[1]q,
								  "version": %[2]q
								}
							  }
							}
						  ]
						}
					`,
							sdkLibrary,
							sdkVersion,
						),
					),
				),
				http.StatusOK,
				"OK",
			)

			tags := stats.Tags{
				"source":      gateway.getSourceTagFromWriteKey(WriteKeyEnabled),
				"sourceID":    gateway.getSourceIDForWriteKey(WriteKeyEnabled),
				"workspaceId": getWorkspaceID(WriteKeyEnabled),
				"writeKey":    WriteKeyEnabled,
				"reqType":     "batch",
				"sourceType":  sourceType2,
				"sdkVersion":  sdkStatTag,
			}
			GinkgoWriter.Println("tags: %v", tags)

			Eventually(
				func() bool {
					stat := statsStore.Get(
						"gateway.write_key_events",
						tags,
					)
					GinkgoWriter.Println("stat: %v", stat)
					return stat != nil && stat.LastValue() == float64(2)
				},
			).Should(BeTrue())

			Eventually(
				func() bool {
					stat := statsStore.Get(
						"gateway.write_key_bot_events",
						tags,
					)
					return stat != nil && stat.LastValue() == float64(1)
				},
			).Should(BeTrue())
		})
	})

	Context("Rate limits", func() {
		var (
			gateway    *HandleT
			statsStore *memstats.Store
		)

		BeforeEach(func() {
			gateway = &HandleT{}
			SetEnableRateLimit(true)
			err := gateway.Setup(context.Background(), c.mockApp, c.mockBackendConfig, c.mockJobsDB, c.mockRateLimiter, c.mockVersionHandler, rsources.NewNoOpService(), sourcedebugger.NewNoOpService())
			Expect(err).To(BeNil())
			statsStore = memstats.New()
			gateway.stats = statsStore
		})

		AfterEach(func() {
			err := gateway.Shutdown()
			Expect(err).To(BeNil())
		})

		It("should store messages successfully if rate limit is not reached for workspace", func() {
			c.mockRateLimiter.EXPECT().CheckLimitReached(gomock.Any(), gomock.Any()).Return(false, nil).Times(1)
			c.mockJobsDB.EXPECT().WithStoreSafeTx(gomock.Any(), gomock.Any()).Times(1).Do(func(ctx context.Context, f func(tx jobsdb.StoreSafeTx) error) {
				_ = f(jobsdb.EmptyStoreSafeTx())
			}).Return(nil)
			mockCall := c.mockJobsDB.EXPECT().StoreEachBatchRetryInTx(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(jobsToEmptyErrors).Times(1)
			tFunc := c.asyncHelper.ExpectAndNotifyCallbackWithName("")
			mockCall.Do(func(context.Context, interface{}, interface{}) { tFunc() })

			expectHandlerResponse(
				gateway.webAliasHandler,
				authorizedRequest(
					WriteKeyEnabled,
					bytes.NewBufferString(
						fmt.Sprintf(`{"userId":"dummyId",%s}`, sdkContext),
					),
				),
				http.StatusOK,
				"OK",
			)
			Eventually(
				func() bool {
					stat := statsStore.Get(
						"gateway.write_key_successful_requests",
						map[string]string{
							"source":      gateway.getSourceTagFromWriteKey(WriteKeyEnabled),
							"sourceID":    gateway.getSourceIDForWriteKey(WriteKeyEnabled),
							"workspaceId": getWorkspaceID(WriteKeyEnabled),
							"writeKey":    WriteKeyEnabled,
							"reqType":     "alias",
							"sourceType":  sourceType2,
							"sdkVersion":  sdkStatTag,
						},
					)
					return stat != nil && stat.LastValue() == float64(1)
				},
			).Should(BeTrue())
		})

		It("should reject messages if rate limit is reached for workspace", func() {
			c.mockRateLimiter.EXPECT().CheckLimitReached(gomock.Any(), gomock.Any()).Return(true, nil).Times(1)
			expectHandlerResponse(
				gateway.webAliasHandler,
				authorizedRequest(WriteKeyEnabled, bytes.NewBufferString("{}")),
				http.StatusTooManyRequests,
				response.TooManyRequests+"\n",
			)
			Eventually(
				func() bool {
					stat := statsStore.Get(
						"gateway.write_key_dropped_requests",
						map[string]string{
							"source":      gateway.getSourceTagFromWriteKey(WriteKeyEnabled),
							"sourceID":    gateway.getSourceIDForWriteKey(WriteKeyEnabled),
							"workspaceId": getWorkspaceID(WriteKeyEnabled),
							"writeKey":    WriteKeyEnabled,
							"reqType":     "alias",
							"sourceType":  sourceType2,
							"sdkVersion":  "",
						},
					)
					return stat != nil && stat.LastValue() == float64(1)
				},
				1*time.Second,
			).Should(BeTrue())
		})
	})

	Context("Invalid requests", func() {
		var (
			gateway    *HandleT
			statsStore *memstats.Store
		)

		BeforeEach(func() {
			gateway = &HandleT{}
			err := gateway.Setup(context.Background(), c.mockApp, c.mockBackendConfig, c.mockJobsDB, nil, c.mockVersionHandler, rsources.NewNoOpService(), sourcedebugger.NewNoOpService())
			Expect(err).To(BeNil())
			statsStore = memstats.New()
			gateway.stats = statsStore
		})

		AfterEach(func() {
			err := gateway.Shutdown()
			Expect(err).To(BeNil())
		})

		createJSONBody := func(customProperty, customValue string) []byte {
			validData := fmt.Sprintf(
				`{"userId":"dummyId","data":{"string":"valid-json","nested":{"child":1}},%s}`,
				sdkContext,
			)
			validDataWithProperty, _ := sjson.SetBytes([]byte(validData), customProperty, customValue)

			return validDataWithProperty
		}

		// common tests for all web handlers
		It("should reject requests without Authorization header", func() {
			for reqType, handler := range allHandlers(gateway) {
				expectHandlerResponse(
					handler,
					unauthorizedRequest(nil),
					http.StatusUnauthorized,
					response.NoWriteKeyInBasicAuth+"\n",
				)
				Eventually(
					func() bool {
						stat := statsStore.Get(
							"gateway.write_key_failed_requests",
							map[string]string{
								"source":      "noWriteKey",
								"sourceID":    gateway.getSourceIDForWriteKey(""),
								"workspaceId": "",
								"writeKey":    "noWriteKey",
								"reqType":     reqType,
								"reason":      "noWriteKeyInBasicAuth",
								"sourceType":  "",
								"sdkVersion":  "",
							},
						)
						return stat != nil && stat.LastValue() == float64(1)
					},
				).Should(BeTrue())
			}
			gateway.stats = stats.Default
		})

		It("should reject requests without username in Authorization header", func() {
			for reqType, handler := range allHandlers(gateway) {
				expectHandlerResponse(
					handler,
					authorizedRequest(WriteKeyEmpty, nil),
					http.StatusUnauthorized,
					response.NoWriteKeyInBasicAuth+"\n",
				)
				Eventually(
					func() bool {
						stat := statsStore.Get(
							"gateway.write_key_failed_requests",
							map[string]string{
								"source":      "noWriteKey",
								"sourceID":    gateway.getSourceIDForWriteKey(""),
								"workspaceId": getWorkspaceID(""),
								"writeKey":    "noWriteKey",
								"reqType":     reqType,
								"reason":      "noWriteKeyInBasicAuth",
								"sourceType":  "",
								"sdkVersion":  "",
							},
						)
						return stat != nil && stat.LastValue() == float64(1)
					},
				).Should(BeTrue())
			}
		})

		It("should reject requests without valid rudder event in request body", func() {
			for handlerType, handler := range allHandlers(gateway) {
				reqType := handlerType
				notRudderEvent := `[{"data": "valid-json","foo":"bar"}]`
				if handlerType == "batch" || handlerType == "import" {
					notRudderEvent = `{"batch": [[{"data": "valid-json","foo":"bar"}]]}`
					reqType = "batch"
				}
				setAllowReqsWithoutUserIDAndAnonymousID(true)
				expectHandlerResponse(
					handler,
					authorizedRequest(
						WriteKeyEnabled,
						bytes.NewBufferString(notRudderEvent),
					),
					http.StatusBadRequest,
					response.NotRudderEvent+"\n",
				)
				Eventually(
					func() bool {
						stat := statsStore.Get(
							"gateway.write_key_failed_requests",
							map[string]string{
								"source":      gateway.getSourceTagFromWriteKey(WriteKeyEnabled),
								"sourceID":    gateway.getSourceIDForWriteKey(WriteKeyEnabled),
								"workspaceId": getWorkspaceID(WriteKeyEnabled),
								"writeKey":    WriteKeyEnabled,
								"reqType":     reqType,
								"reason":      response.NotRudderEvent,
								"sourceType":  sourceType2,
								"sdkVersion":  "",
							},
						)
						return stat != nil && stat.LastValue() == float64(1)
					},
				).Should(BeTrue())
				setAllowReqsWithoutUserIDAndAnonymousID(false)
			}
		})

		It("should reject requests with both userId and anonymousId not present", func() {
			for handlerType, handler := range allHandlers(gateway) {
				reqType := handlerType
				validBody := `{"data": "valid-json"}`
				if handlerType == "batch" || handlerType == "import" {
					validBody = `{"batch": [{"data": "valid-json"}]}`
					reqType = "batch"
				}
				if handlerType != "extract" {
					expectHandlerResponse(
						handler,
						authorizedRequest(
							WriteKeyEnabled,
							bytes.NewBufferString(validBody),
						),
						http.StatusBadRequest,
						response.NonIdentifiableRequest+"\n",
					)
					Eventually(
						func() bool {
							stat := statsStore.Get(
								"gateway.write_key_failed_requests",
								map[string]string{
									"source":      gateway.getSourceTagFromWriteKey(WriteKeyEnabled),
									"sourceID":    gateway.getSourceIDForWriteKey(WriteKeyEnabled),
									"workspaceId": getWorkspaceID(WriteKeyEnabled),
									"writeKey":    WriteKeyEnabled,
									"reqType":     reqType,
									"reason":      response.NonIdentifiableRequest,
									"sourceType":  sourceType2,
									"sdkVersion":  "",
								},
							)
							return stat != nil && stat.LastValue() == float64(1)
						},
					).Should(BeTrue())
				}
			}
		})

		It("should allow requests with both userId and anonymousId absent in case of extract events", func() {
			extractHandlers := map[string]http.HandlerFunc{
				"batch":   gateway.webBatchHandler,
				"import":  gateway.webImportHandler,
				"extract": gateway.webExtractHandler,
			}
			c.mockJobsDB.EXPECT().WithStoreSafeTx(gomock.Any(), gomock.Any()).AnyTimes().Do(func(ctx context.Context, f func(tx jobsdb.StoreSafeTx) error) {
				_ = f(jobsdb.EmptyStoreSafeTx())
			}).Return(nil)
			c.mockJobsDB.EXPECT().StoreEachBatchRetryInTx(gomock.Any(), gomock.Any(), gomock.Any()).Times(3)

			for handlerType, handler := range extractHandlers {
				var body string
				switch handlerType {
				case "extract":
					body = `{"data": "valid-json", "type": "extract"}`
				default:
					body = `{"batch": [{"data": "valid-json", "type": "extract"}]}`
				}
				expectHandlerResponse(
					handler,
					authorizedRequest(
						WriteKeyEnabled,
						bytes.NewBufferString(body),
					),
					http.StatusOK,
					"OK",
				)
			}
		})

		It("should reject requests without request body", func() {
			for _, handler := range allHandlers(gateway) {
				expectHandlerResponse(handler, authorizedRequest(WriteKeyInvalid, nil), http.StatusBadRequest, response.RequestBodyNil+"\n")
			}
		})

		It("should reject requests without valid json in request body", func() {
			for _, handler := range allHandlers(gateway) {
				invalidBody := "not-a-valid-json"
				expectHandlerResponse(handler, authorizedRequest(WriteKeyInvalid, bytes.NewBufferString(invalidBody)), http.StatusBadRequest, response.InvalidJSON+"\n")
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
					expectHandlerResponse(handler, authorizedRequest(WriteKeyEnabled, bytes.NewBufferString(body)), http.StatusRequestEntityTooLarge, response.RequestBodyTooLarge+"\n")
				}
			}
		})

		It("should reject requests with invalid write keys", func() {
			for handlerType, handler := range allHandlers(gateway) {
				validBody := `{"data":"valid-json"}`
				if handlerType == "batch" || handlerType == "import" {
					validBody = `{"batch":[{"data":"valid-json"}]}`
				}
				expectHandlerResponse(handler, authorizedRequest(WriteKeyInvalid, bytes.NewBufferString(validBody)), http.StatusUnauthorized, response.InvalidWriteKey+"\n")
			}
		})

		It("should reject requests with disabled write keys (source)", func() {
			for handlerType, handler := range allHandlers(gateway) {
				validBody := `{"data":"valid-json"}`
				if handlerType == "batch" || handlerType == "import" {
					validBody = `{"batch":[{"data":"valid-json"}]}`
				}
				expectHandlerResponse(handler, authorizedRequest(WriteKeyDisabled, bytes.NewBufferString(validBody)), http.StatusNotFound, response.SourceDisabled+"\n")
			}
		})

		It("should reject requests with 500 if jobsdb store returns an error", func() {
			for handlerType, handler := range allHandlers(gateway) {
				if !(handlerType == "batch" || handlerType == "import") {
					validBody := createJSONBody("custom-property", "custom-value")

					c.mockJobsDB.EXPECT().WithStoreSafeTx(gomock.Any(), gomock.Any()).Times(1).Do(func(ctx context.Context, f func(tx jobsdb.StoreSafeTx) error) {
						_ = f(jobsdb.EmptyStoreSafeTx())
					}).Return(nil)
					c.mockJobsDB.
						EXPECT().StoreEachBatchRetryInTx(gomock.Any(), gomock.Any(), gomock.Any()).
						DoAndReturn(jobsToJobsdbErrors).
						Times(1)

					expectHandlerResponse(
						handler,
						authorizedRequest(
							WriteKeyEnabled,
							bytes.NewBuffer(validBody),
						),
						http.StatusInternalServerError,
						"tx error"+"\n",
					)
					Eventually(
						func() bool {
							stat := statsStore.Get(
								"gateway.write_key_failed_requests",
								map[string]string{
									"source":      gateway.getSourceTagFromWriteKey(WriteKeyEnabled),
									"sourceID":    gateway.getSourceIDForWriteKey(WriteKeyEnabled),
									"workspaceId": getWorkspaceID(WriteKeyEnabled),
									"writeKey":    WriteKeyEnabled,
									"reqType":     handlerType,
									"reason":      "storeFailed",
									"sourceType":  sourceType2,
									"sdkVersion":  sdkStatTag,
								},
							)
							return stat != nil && stat.LastValue() == float64(1)
						},
					).Should(BeTrue())
				}
			}
		})
	})

	Context("Robots", func() {
		var gateway *HandleT

		BeforeEach(func() {
			gateway = &HandleT{}
			err := gateway.Setup(context.Background(), c.mockApp, c.mockBackendConfig, c.mockJobsDB, nil, c.mockVersionHandler, rsources.NewNoOpService(), sourcedebugger.NewNoOpService())
			Expect(err).To(BeNil())
		})

		AfterEach(func() {
			err := gateway.Shutdown()
			Expect(err).To(BeNil())
		})

		It("should return a robots.txt", func() {
			expectHandlerResponse(gateway.robots, nil, http.StatusOK, "User-agent: * \nDisallow: / \n")
		})
	})

	Context("jobDataFromRequest", func() {
		var (
			gateway      *HandleT
			someWriteKey = "someWriteKey"
			userIDHeader = "userIDHeader"
		)
		BeforeEach(func() {
			gateway = &HandleT{}
			err := gateway.Setup(context.Background(), c.mockApp, c.mockBackendConfig, c.mockJobsDB, nil, c.mockVersionHandler, rsources.NewNoOpService(), sourcedebugger.NewNoOpService())
			Expect(err).To(BeNil())
		})

		AfterEach(func() {
			err := gateway.Shutdown()
			Expect(err).To(BeNil())
		})

		It("returns invalid JSON in case of invalid JSON payload", func() {
			req := &webRequestT{
				reqType:        "batch",
				writeKey:       someWriteKey,
				done:           make(chan<- string),
				userIDHeader:   userIDHeader,
				requestPayload: []byte(``),
			}
			jobData, err := gateway.getJobDataFromRequest(req)
			Expect(errors.New(response.InvalidJSON)).To(Equal(err))
			Expect(jobData.jobs).To(BeNil())
		})

		It("drops non-identifiable requests if userID and anonID are not present in the request payload", func() {
			req := &webRequestT{
				reqType:        "batch",
				writeKey:       WriteKeyEnabled,
				done:           make(chan<- string),
				userIDHeader:   userIDHeader,
				requestPayload: []byte(`{"batch": [{"type": "track"}]}`),
			}
			jobData, err := gateway.getJobDataFromRequest(req)
			Expect(err).To(Equal(errors.New(response.NonIdentifiableRequest)))
			Expect(jobData.jobs).To(BeNil())
		})

		It("accepts events with non-string type anonymousId and/or userId", func() {
			// map type usreId
			payloadMap := map[string]interface{}{
				"batch": []interface{}{
					map[string]interface{}{
						"type":   "track",
						"userId": map[string]interface{}{"id": 456},
					},
				},
			}
			payload, err := json.Marshal(payloadMap)
			Expect(err).To(BeNil())
			req := &webRequestT{
				reqType:        "batch",
				writeKey:       WriteKeyEnabled,
				done:           make(chan<- string),
				userIDHeader:   userIDHeader,
				requestPayload: payload,
			}
			_, err = gateway.getJobDataFromRequest(req)
			Expect(err).To(BeNil())

			// int type anonymousId
			payloadMap = map[string]interface{}{
				"batch": []interface{}{
					map[string]interface{}{
						"type":   "track",
						"userId": 456,
					},
				},
			}
			payload, err = json.Marshal(payloadMap)
			Expect(err).To(BeNil())
			Expect(err).To(BeNil())
			req = &webRequestT{
				reqType:        "batch",
				writeKey:       WriteKeyEnabled,
				done:           make(chan<- string),
				userIDHeader:   userIDHeader,
				requestPayload: payload,
			}
			_, err = gateway.getJobDataFromRequest(req)
			Expect(err).To(BeNil())
		})

		It("allows extract events even if userID and anonID are not present in the request payload", func() {
			req := &webRequestT{
				reqType:        "batch",
				writeKey:       WriteKeyEnabled,
				done:           make(chan<- string),
				userIDHeader:   userIDHeader,
				requestPayload: []byte(`{"batch": [{"type": "extract"}]}`),
			}
			_, err := gateway.getJobDataFromRequest(req)
			Expect(err).To(BeNil())
		})
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

// return all endpoints as key and method as value
func endpointsToVerify() ([]string, []string, []string) {
	getEndpoints := []string{
		"/version",
		"/robots.txt",
		"/pixel/v1/track",
		"/pixel/v1/page",
		"/v1/webhook",
		"/v1/job-status/123",
		"/v1/job-status/123/failed-records",
		"/v1/warehouse/jobs/status",
		"/v1/warehouse/fetch-tables",
	}

	postEndpoints := []string{
		"/v1/batch",
		"/v1/identify",
		"/v1/track",
		"/v1/page",
		"/v1/screen",
		"/v1/alias",
		"/v1/merge",
		"/v1/group",
		"/v1/import",
		"/v1/audiencelist",
		"/v1/webhook",
		"/beacon/v1/batch",
		"/internal/v1/extract",
		"/v1/warehouse/pending-events",
		"/v1/warehouse/trigger-upload",
		"/v1/warehouse/jobs",
	}

	deleteEndpoints := []string{
		"/v1/job-status/1234",
	}
	return getEndpoints, postEndpoints, deleteEndpoints
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
		"extract":      gateway.webExtractHandler,
	}
}

// converts a job list to a map of empty errors, to emulate a successful jobsdb.Store response
func jobsToEmptyErrors(_ context.Context, _ jobsdb.StoreSafeTx, _ [][]*jobsdb.JobT) (map[uuid.UUID]string, error) {
	return make(map[uuid.UUID]string), nil
}

// converts a job list to a map of empty errors, to emulate a successful jobsdb.Store response
func jobsToJobsdbErrors(_ context.Context, _ jobsdb.StoreSafeTx, jobs [][]*jobsdb.JobT) (map[uuid.UUID]string, error) {
	errorsMap := make(map[uuid.UUID]string, len(jobs))
	for _, batch := range jobs {
		errorsMap[batch[0].UUID] = "tx error"
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

func getWorkspaceID(writeKey string) string {
	configSubscriberLock.RLock()
	defer configSubscriberLock.RUnlock()
	return enabledWriteKeyWorkspaceMap[writeKey]
}

type mockRequestHandler struct{}

func (mockRequestHandler) ProcessRequest(gateway *HandleT, w *http.ResponseWriter, r *http.Request, reqType string, payload []byte, writeKey string) string {
	// deepsource ignore: Unused method arguments
	_ = gateway
	_ = w
	_ = r
	_ = reqType
	_ = payload
	_ = writeKey
	return ""
}
