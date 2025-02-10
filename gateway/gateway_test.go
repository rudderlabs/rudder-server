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

	"github.com/rudderlabs/rudder-schemas/go/stream"
	"github.com/rudderlabs/rudder-server/utils/httputil"

	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	kituuid "github.com/rudderlabs/rudder-go-kit/uuid"

	"go.uber.org/mock/gomock"

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
	"github.com/rudderlabs/rudder-server/enterprise/suppress-user/model"
	gwstats "github.com/rudderlabs/rudder-server/gateway/internal/stats"
	gwtypes "github.com/rudderlabs/rudder-server/gateway/internal/types"
	"github.com/rudderlabs/rudder-server/gateway/response"
	webhookModel "github.com/rudderlabs/rudder-server/gateway/webhook/model"
	"github.com/rudderlabs/rudder-server/jobsdb"
	mocksApp "github.com/rudderlabs/rudder-server/mocks/app"
	mocksBackendConfig "github.com/rudderlabs/rudder-server/mocks/backend-config"
	mockGateway "github.com/rudderlabs/rudder-server/mocks/gateway"
	mocksJobsDB "github.com/rudderlabs/rudder-server/mocks/jobsdb"
	mocksTypes "github.com/rudderlabs/rudder-server/mocks/utils/types"
	sourcedebugger "github.com/rudderlabs/rudder-server/services/debugger/source"
	mocksrcdebugger "github.com/rudderlabs/rudder-server/services/debugger/source/mocks"
	"github.com/rudderlabs/rudder-server/services/rsources"
	"github.com/rudderlabs/rudder-server/services/transformer"
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
	ReplaySourceID            = "replay-source"
	ReplayWriteKey            = "replay-source"
	RETLSourceID              = "retl-source"
	RETLWriteKey              = "retl-source"
	SourceIDDisabled          = "disabled-source"
	TestRemoteAddressWithPort = "test.com:80"
	TestRemoteAddress         = "test.com"

	SuppressedUserID = "suppressed-user-2"
	NormalUserID     = "normal-user-1"
	WorkspaceID      = "workspace"
	sourceType1      = "sourceType1"
	sourceType2      = "webhook"
	RETLSourceType   = "retl"
	sdkLibrary       = "sdkLibrary"
	sdkVersion       = "v1.2.3"

	writeKeyNotPresentInSource = "write-key-not-present-for-source"
)

var (
	rCtxEnabled = &gwtypes.AuthRequestContext{
		WriteKey:       WriteKeyEnabled,
		SourceID:       SourceIDEnabled,
		WorkspaceID:    WorkspaceID,
		SourceCategory: sourceType2,
	}
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
				Name:     SourceIDDisabled,
				Category: sourceType1,
			},
		},
		{
			ID:       SourceIDEnabled,
			WriteKey: WriteKeyEnabled,
			Enabled:  true,
			SourceDefinition: backendconfig.SourceDefinitionT{
				Name:     SourceIDEnabled,
				Category: sourceType2,
			},
			WorkspaceID: WorkspaceID,
		},
		{
			ID:         ReplaySourceID,
			WriteKey:   ReplayWriteKey,
			Enabled:    true,
			OriginalID: ReplaySourceID,
			SourceDefinition: backendconfig.SourceDefinitionT{
				Name: SourceIDEnabled,
			},
			WorkspaceID: WorkspaceID,
		},
		{
			ID:         RETLSourceID,
			WriteKey:   RETLWriteKey,
			Enabled:    true,
			OriginalID: RETLSourceID,
			SourceDefinition: backendconfig.SourceDefinitionT{
				Name:     SourceIDEnabled,
				Category: RETLSourceType,
			},
			WorkspaceID: WorkspaceID,
		},
	},
}

type testContext struct {
	asyncHelper testutils.AsyncTestHelper

	mockCtrl           *gomock.Controller
	mockJobsDB         *mocksJobsDB.MockJobsDB
	mockErrJobsDB      *mocksJobsDB.MockJobsDB
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

// Initialise mocks and common expectations
func (c *testContext) Setup() {
	c.asyncHelper.Setup()
	c.mockCtrl = gomock.NewController(GinkgoT())
	c.mockJobsDB = mocksJobsDB.NewMockJobsDB(c.mockCtrl)
	c.mockErrJobsDB = mocksJobsDB.NewMockJobsDB(c.mockCtrl)
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
}

var _ = Describe("Reconstructing JSON for ServerSide SDK", func() {
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
			response, payloadError := getUsersPayload([]byte(testValidBody))
			key, err := kituuid.GetMD5UUID(inputKey)
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
		gateway    *Handle
		statsStore *memstats.Store
		conf       *config.Config
	)

	BeforeEach(func() {
		c = &testContext{}
		c.Setup()

		c.mockSuppressUser = mocksTypes.NewMockUserSuppression(c.mockCtrl)
		c.mockSuppressUserFeature = mocksApp.NewMockSuppressUserFeature(c.mockCtrl)
		c.initializeEnterpriseAppFeatures()

		c.mockSuppressUserFeature.EXPECT().Setup(gomock.Any(), gomock.Any()).AnyTimes().Return(c.mockSuppressUser, nil)
		c.mockSuppressUser.EXPECT().GetSuppressedUser(WorkspaceID, NormalUserID, SourceIDEnabled).Return(nil).AnyTimes()
		c.mockSuppressUser.EXPECT().GetSuppressedUser(WorkspaceID, SuppressedUserID, SourceIDEnabled).Return(&model.Metadata{
			CreatedAt: time.Now(),
		}).AnyTimes()

		conf = config.New()
		conf.Set("Gateway.enableRateLimit", false)
		conf.Set("Gateway.enableSuppressUserFeature", true)
		conf.Set("Gateway.enableEventSchemasFeature", false)
	})

	AfterEach(func() {
		c.Finish()
	})

	Context("Suppress users", func() {
		BeforeEach(func() {
			var err error
			statsStore, err = memstats.New()
			Expect(err).To(BeNil())

			gateway = &Handle{}
			err = gateway.Setup(context.Background(), conf, logger.NOP, statsStore, c.mockApp, c.mockBackendConfig, c.mockJobsDB, c.mockErrJobsDB, nil, c.mockVersionHandler, rsources.NewNoOpService(), transformer.NewNoOpService(), sourcedebugger.NewNoOpService(), nil)
			Expect(err).To(BeNil())

			waitForBackendConfigInit(gateway)
		})

		AfterEach(func() {
			err := gateway.Shutdown()
			Expect(err).To(BeNil())
		})

		It("should not accept events from suppress users", func() {
			suppressedUserEventData := fmt.Sprintf(`{"batch":[{"userId":%q}]}`, SuppressedUserID)
			// Why GET
			expectHandlerResponse(gateway.webBatchHandler(), authorizedRequest(WriteKeyEnabled, bytes.NewBufferString(suppressedUserEventData)), http.StatusOK, "OK", "batch")
			Eventually(
				func() bool {
					stat := statsStore.Get(
						"gateway.write_key_suppressed_requests",
						map[string]string{
							"source":      rCtxEnabled.SourceTag(),
							"sourceID":    rCtxEnabled.SourceID,
							"workspaceId": rCtxEnabled.WorkspaceID,
							"writeKey":    rCtxEnabled.WriteKey,
							"reqType":     "batch",
							"sourceType":  rCtxEnabled.SourceCategory,
							"sdkVersion":  "",
						},
					)
					return stat != nil && stat.LastValue() == float64(1)
				},
				1*time.Second,
			).Should(BeTrue())
			// stat should be present for user suppression
			Eventually(
				func() bool {
					stat := statsStore.Get(
						"gateway.user_suppression_age",
						map[string]string{
							"sourceID":    rCtxEnabled.SourceID,
							"workspaceId": rCtxEnabled.WorkspaceID,
						},
					)
					return stat != nil
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
				gateway.webBatchHandler(),
				authorizedRequest(
					WriteKeyEnabled,
					bytes.NewBufferString(allowedUserEventData),
				),
				http.StatusOK,
				"OK",
				"batch",
			)
			Eventually(
				func() bool {
					stat := statsStore.Get(
						"gateway.write_key_successful_requests",
						map[string]string{
							"source":      rCtxEnabled.SourceTag(),
							"sourceID":    rCtxEnabled.SourceID,
							"workspaceId": rCtxEnabled.WorkspaceID,
							"writeKey":    rCtxEnabled.WriteKey,
							"reqType":     "batch",
							"sourceType":  rCtxEnabled.SourceCategory,
							"sdkVersion":  sdkStatTag,
						},
					)
					return stat != nil && stat.LastValue() == float64(1)
				},
				1*time.Second,
			).Should(BeTrue())
			// stat should not be present for normal user
			stat := statsStore.Get(
				"gateway.user_suppression_age",
				map[string]string{
					"sourceID":    rCtxEnabled.SourceID,
					"workspaceId": rCtxEnabled.WorkspaceID,
				},
			)
			Expect(stat).To(BeNil())
		})
	})
})

var _ = Describe("Gateway", func() {
	initGW()

	var (
		conf *config.Config
		c    *testContext
	)

	BeforeEach(func() {
		conf = config.New()
		conf.Set("Gateway.enableRateLimit", false)
		conf.Set("Gateway.enableEventSchemasFeature", false)
		c = &testContext{}
		c.Setup()
	})

	AfterEach(func() {
		c.Finish()
	})

	Context("Initialization", func() {
		It("should wait for backend config", func() {
			c.initializeAppFeatures()
			gateway := &Handle{}
			err := gateway.Setup(context.Background(), conf, logger.NOP, stats.NOP, c.mockApp, c.mockBackendConfig, c.mockJobsDB, c.mockErrJobsDB, nil, c.mockVersionHandler, rsources.NewNoOpService(), transformer.NewNoOpService(), sourcedebugger.NewNoOpService(), nil)
			Expect(err).To(BeNil())
			waitForBackendConfigInit(gateway)
			err = gateway.Shutdown()
			Expect(err).To(BeNil())
		})
	})

	Context("Test All endpoints", func() {
		var (
			gateway   *Handle
			whServer  *httptest.Server
			serverURL string
		)

		BeforeEach(func() {
			c.initializeAppFeatures()
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

			gateway = &Handle{}
			err = gateway.Setup(context.Background(), conf, logger.NOP, stats.NOP, c.mockApp, c.mockBackendConfig, c.mockJobsDB, c.mockErrJobsDB, nil, c.mockVersionHandler, rsources.NewNoOpService(), transformer.NewNoOpService(), sourcedebugger.NewNoOpService(), nil)
			Expect(err).To(BeNil())
			waitForBackendConfigInit(gateway)
			gateway.irh = mockRequestHandler{}
			gateway.rrh = mockRequestHandler{}
			gateway.webhook = c.mockWebhook
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
					url = url + "?writeKey=" + WriteKeyEnabled
				}

				if method == http.MethodGet {
					req, err = http.NewRequest(method, url, nil)
				} else {
					req, err = http.NewRequest(method, url, bytes.NewBuffer(createValidBody("custom-property", "custom-value")))
				}
				Expect(err).To(BeNil())
				req.Header.Set("Content-Type", "application/json")
				switch ep {
				case "/internal/v1/replay", "/internal/v1/retl":
					req.Header.Set("X-Rudder-Source-Id", ReplaySourceID)
				default:
					req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(WriteKeyEnabled+":")))
				}
				resp, err := client.Do(req)
				Expect(err).To(BeNil())
				Expect(resp.StatusCode).To(SatisfyAny(Equal(http.StatusOK), Equal(http.StatusNoContent)), "endpoint: "+ep)

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
			err        error
			gateway    *Handle
			statsStore *memstats.Store
		)

		BeforeEach(func() {
			c.initializeAppFeatures()
			statsStore, err = memstats.New()
			Expect(err).To(BeNil())

			gateway = &Handle{}
			err := gateway.Setup(context.Background(), conf, logger.NOP, statsStore, c.mockApp, c.mockBackendConfig, c.mockJobsDB, c.mockErrJobsDB, nil, c.mockVersionHandler, rsources.NewNoOpService(), transformer.NewNoOpService(), sourcedebugger.NewNoOpService(), nil)
			Expect(err).To(BeNil())
			waitForBackendConfigInit(gateway)
		})

		AfterEach(func() {
			err := gateway.Shutdown()
			Expect(err).To(BeNil())
		})

		assertJobMetadata := func(job *jobsdb.JobT) {
			Expect(misc.IsValidUUID(job.UUID.String())).To(Equal(true))

			var paramsMap, expectedParamsMap map[string]interface{}
			_ = json.Unmarshal(job.Parameters, &paramsMap)
			expectedStr := []byte(fmt.Sprintf(
				`{"source_id": "%v", "source_job_run_id": "", "source_task_run_id": "","source_category": "webhook", "traceparent": ""}`,
				SourceIDEnabled,
			))
			_ = json.Unmarshal(expectedStr, &expectedParamsMap)
			equals := reflect.DeepEqual(paramsMap, expectedParamsMap)
			Expect(equals).To(BeTrue())

			Expect(job.CustomVal).To(Equal(customVal))

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
			strippedPayload, _ = sjson.Delete(strippedPayload, "receivedAt")
			strippedPayload, _ = sjson.Delete(strippedPayload, "request_ip")

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
						handlerType,
					)
					Eventually(
						func() bool {
							stat := statsStore.Get(
								"gateway.write_key_successful_requests",
								map[string]string{
									"source":      rCtxEnabled.SourceTag(),
									"sourceID":    rCtxEnabled.SourceID,
									"workspaceId": rCtxEnabled.WorkspaceID,
									"writeKey":    rCtxEnabled.WriteKey,
									"reqType":     handlerType,
									"sourceType":  rCtxEnabled.SourceCategory,
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

		It("should accept valid replay request and store to jobsdb", func() {
			handler := gateway.webReplayHandler()

			validBody := []byte(fmt.Sprintf(`{"batch":[%s]}`, string(createValidBody("custom-property", "custom-value"))))

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
						c.asyncHelper.ExpectAndNotifyCallbackWithName("jobsdb_store")()
						return jobsToEmptyErrors(ctx, tx, jobBatches)
					}).
				Times(1)

			expectHandlerResponse(
				handler,
				authorizedReplayRequest(
					ReplaySourceID,
					bytes.NewBuffer(validBody)),
				http.StatusOK,
				"OK",
				"replay",
			)

			Eventually(
				func() bool {
					stat := statsStore.Get(
						"gateway.write_key_successful_requests",
						map[string]string{
							"source":      "_source",
							"sourceID":    ReplaySourceID,
							"workspaceId": WorkspaceID,
							"writeKey":    ReplayWriteKey,
							"reqType":     "replay",
							"sourceType":  "eventStream",
							"sdkVersion":  sdkStatTag,
						},
					)
					return stat != nil && stat.LastValue() == float64(1)
				},
				1*time.Second,
			).Should(BeTrue())
		})

		It("should accept valid (with and without userId and anonymousId) retl request and store to jobsdb", func() {
			handler := gateway.webRetlHandler()

			payloads := [2][]byte{
				[]byte(`{"batch": [{"data": "valid-json", "type": "record", "userId": "dummyId"}]}`),
				[]byte(`{"batch": [{"data": "valid-json", "type": "record"}]}`),
			}
			c.mockJobsDB.EXPECT().WithStoreSafeTx(
				gomock.Any(),
				gomock.Any()).AnyTimes().Do(func(
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
			).AnyTimes()

			// With userId
			expectHandlerResponse(
				handler,
				authorizedRETLRequest(
					RETLSourceID,
					bytes.NewBuffer(payloads[0])),
				http.StatusOK,
				"OK",
				"retl",
			)

			Eventually(
				func() bool {
					stat := statsStore.Get(
						"gateway.write_key_successful_events",
						map[string]string{
							"source":      "_source",
							"sourceID":    RETLSourceID,
							"workspaceId": WorkspaceID,
							"writeKey":    RETLWriteKey,
							"reqType":     "retl",
							"sourceType":  "retl",
							"sdkVersion":  "",
						},
					)
					return stat != nil && stat.LastValue() == float64(1)
				},
				1*time.Second,
			).Should(BeTrue())

			// Without userId
			expectHandlerResponse(
				handler,
				authorizedRETLRequest(
					RETLSourceID,
					bytes.NewBuffer(payloads[1])),
				http.StatusOK,
				"OK",
				"retl",
			)

			Eventually(
				func() bool {
					stat := statsStore.Get(
						"gateway.write_key_successful_events",
						map[string]string{
							"source":      "_source",
							"sourceID":    RETLSourceID,
							"workspaceId": WorkspaceID,
							"writeKey":    RETLWriteKey,
							"reqType":     "retl",
							"sourceType":  "retl",
							"sdkVersion":  "",
						},
					)
					return stat != nil && stat.LastValue() == float64(2)
				},
				1*time.Second,
			).Should(BeTrue())
		})

		It("should accept requests with both userId and anonymousId absent in case of extract events", func() {
			extractHandlers := map[string]http.HandlerFunc{
				"batch":   gateway.webBatchHandler(),
				"import":  gateway.webImportHandler(),
				"extract": gateway.webExtractHandler(),
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
					handlerType,
				)
			}
		})
	})

	Context("Bots", func() {
		var (
			err        error
			gateway    *Handle
			statsStore *memstats.Store
		)

		BeforeEach(func() {
			c.initializeAppFeatures()
			statsStore, err = memstats.New()
			Expect(err).To(BeNil())

			gateway = &Handle{}

			err := gateway.Setup(context.Background(), conf, logger.NOP, statsStore, c.mockApp, c.mockBackendConfig, c.mockJobsDB, c.mockErrJobsDB, c.mockRateLimiter, c.mockVersionHandler, rsources.NewNoOpService(), transformer.NewNoOpService(), sourcedebugger.NewNoOpService(), nil)
			Expect(err).To(BeNil())
			waitForBackendConfigInit(gateway)
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
				gateway.webBatchHandler(),
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
				"batch",
			)

			tags := stats.Tags{
				"source":      rCtxEnabled.SourceTag(),
				"sourceID":    rCtxEnabled.SourceID,
				"workspaceId": rCtxEnabled.WorkspaceID,
				"writeKey":    rCtxEnabled.WriteKey,
				"reqType":     "batch",
				"sourceType":  rCtxEnabled.SourceCategory,
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
			err        error
			gateway    *Handle
			statsStore *memstats.Store
		)

		BeforeEach(func() {
			c.initializeAppFeatures()
			statsStore, err = memstats.New()
			Expect(err).To(BeNil())

			gateway = &Handle{}
			conf.Set("Gateway.enableRateLimit", true)
			err := gateway.Setup(context.Background(), conf, logger.NOP, statsStore, c.mockApp, c.mockBackendConfig, c.mockJobsDB, c.mockErrJobsDB, c.mockRateLimiter, c.mockVersionHandler, rsources.NewNoOpService(), transformer.NewNoOpService(), sourcedebugger.NewNoOpService(), nil)
			Expect(err).To(BeNil())
			waitForBackendConfigInit(gateway)
		})

		AfterEach(func() {
			err := gateway.Shutdown()
			Expect(err).To(BeNil())
		})

		It("should store messages successfully if rate limit is not reached for workspace", func() {
			c.mockRateLimiter.EXPECT().CheckLimitReached(gomock.Any(), gomock.Any(), gomock.Any()).Return(false, nil).Times(1)
			c.mockJobsDB.EXPECT().WithStoreSafeTx(gomock.Any(), gomock.Any()).Times(1).Do(func(ctx context.Context, f func(tx jobsdb.StoreSafeTx) error) {
				_ = f(jobsdb.EmptyStoreSafeTx())
			}).Return(nil)
			mockCall := c.mockJobsDB.EXPECT().StoreEachBatchRetryInTx(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(jobsToEmptyErrors).Times(1)
			tFunc := c.asyncHelper.ExpectAndNotifyCallbackWithName("")
			mockCall.Do(func(context.Context, interface{}, interface{}) { tFunc() })

			expectHandlerResponse(
				gateway.webAliasHandler(),
				authorizedRequest(
					WriteKeyEnabled,
					bytes.NewBufferString(
						fmt.Sprintf(`{"userId":"dummyId",%s}`, sdkContext),
					),
				),
				http.StatusOK,
				"OK",
				"alias",
			)
			Eventually(
				func() bool {
					stat := statsStore.Get(
						"gateway.write_key_successful_requests",
						map[string]string{
							"source":      rCtxEnabled.SourceTag(),
							"sourceID":    rCtxEnabled.SourceID,
							"workspaceId": rCtxEnabled.WorkspaceID,
							"writeKey":    rCtxEnabled.WriteKey,
							"reqType":     "alias",
							"sourceType":  rCtxEnabled.SourceCategory,
							"sdkVersion":  sdkStatTag,
						},
					)
					return stat != nil && stat.LastValue() == float64(1)
				},
			).Should(BeTrue())
		})

		It("should reject messages if rate limit is reached for workspace", func() {
			conf.Set("Gateway.allowReqsWithoutUserIDAndAnonymousID", true)
			c.mockRateLimiter.EXPECT().CheckLimitReached(gomock.Any(), gomock.Any(), gomock.Any()).Return(true, nil).Times(1)
			expectHandlerResponse(
				gateway.webAliasHandler(),
				authorizedRequest(WriteKeyEnabled, bytes.NewBufferString(`{"data": "valid-json"}`)),
				http.StatusTooManyRequests,
				response.TooManyRequests+"\n",
				"alias",
			)
			Eventually(
				func() bool {
					stat := statsStore.Get(
						"gateway.write_key_dropped_requests",
						map[string]string{
							"source":      rCtxEnabled.SourceTag(),
							"sourceID":    rCtxEnabled.SourceID,
							"workspaceId": rCtxEnabled.WorkspaceID,
							"writeKey":    rCtxEnabled.WriteKey,
							"reqType":     "alias",
							"sourceType":  rCtxEnabled.SourceCategory,
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
			err        error
			gateway    *Handle
			statsStore *memstats.Store
		)

		BeforeEach(func() {
			c.initializeAppFeatures()
			statsStore, err = memstats.New()
			Expect(err).To(BeNil())

			gateway = &Handle{}
			err := gateway.Setup(context.Background(), conf, logger.NOP, statsStore, c.mockApp, c.mockBackendConfig, c.mockJobsDB, c.mockErrJobsDB, nil, c.mockVersionHandler, rsources.NewNoOpService(), transformer.NewNoOpService(), sourcedebugger.NewNoOpService(), nil)
			Expect(err).To(BeNil())
			waitForBackendConfigInit(gateway)
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
					reqType,
				)
				Eventually(
					func() bool {
						stat := statsStore.Get(
							"gateway.write_key_failed_requests",
							map[string]string{
								"source":      "noWriteKey",
								"sourceID":    "noWriteKey",
								"workspaceId": "",
								"writeKey":    "noWriteKey",
								"reqType":     reqType,
								"reason":      "Failed to read writeKey from header",
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
					reqType,
				)
				Eventually(
					func() bool {
						stat := statsStore.Get(
							"gateway.write_key_failed_requests",
							map[string]string{
								"source":      "noWriteKey",
								"sourceID":    "noWriteKey",
								"workspaceId": "",
								"writeKey":    "noWriteKey",
								"reqType":     reqType,
								"reason":      "Failed to read writeKey from header",
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
			conf.Set("Gateway.allowReqsWithoutUserIDAndAnonymousID", true)
			Eventually(func() bool { return gateway.conf.allowReqsWithoutUserIDAndAnonymousID.Load() }).Should(BeTrue())
			batchCount := 0
			for handlerType, handler := range allHandlers(gateway) {
				reqType := handlerType
				notRudderEvent := `[{"data": "valid-json","foo":"bar"}]`
				if handlerType == "batch" || handlerType == "import" {
					notRudderEvent = `{"batch": [[{"data": "valid-json","foo":"bar"}]]}`
					reqType = "batch"
					batchCount++
				}
				expectHandlerResponse(
					handler,
					authorizedRequest(
						WriteKeyEnabled,
						bytes.NewBufferString(notRudderEvent),
					),
					http.StatusBadRequest,
					response.NotRudderEvent+"\n",
					handlerType,
				)
				Eventually(
					func() bool {
						stat := statsStore.Get(
							"gateway.write_key_failed_requests",
							map[string]string{
								"source":      rCtxEnabled.SourceTag(),
								"sourceID":    rCtxEnabled.SourceID,
								"workspaceId": rCtxEnabled.WorkspaceID,
								"writeKey":    rCtxEnabled.WriteKey,
								"reqType":     reqType,
								"reason":      response.NotRudderEvent,
								"sourceType":  rCtxEnabled.SourceCategory,
								"sdkVersion":  "",
							},
						)
						// multiple `handlerType` are mapped to batch `reqType`
						count := 1
						if reqType == "batch" {
							count = batchCount
						}

						return stat != nil && stat.LastValue() == float64(count)
					},
				).Should(BeTrue())
			}
		})

		It("should reject requests with both userId and anonymousId not present", func() {
			batchCount := 0
			for handlerType, handler := range allHandlers(gateway) {
				reqType := handlerType
				validBody := `{"data": "valid-json"}`
				if handlerType == "batch" || handlerType == "import" {
					validBody = `{"batch": [{"data": "valid-json"}]}`
					reqType = "batch"
					batchCount++
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
						handlerType,
					)
					Eventually(
						func() bool {
							stat := statsStore.Get(
								"gateway.write_key_failed_requests",
								map[string]string{
									"source":      rCtxEnabled.SourceTag(),
									"sourceID":    rCtxEnabled.SourceID,
									"workspaceId": rCtxEnabled.WorkspaceID,
									"writeKey":    rCtxEnabled.WriteKey,
									"reqType":     reqType,
									"reason":      response.NonIdentifiableRequest,
									"sourceType":  rCtxEnabled.SourceCategory,
									"sdkVersion":  "",
								},
							)
							// multiple `handlerType` are mapped to batch `reqType`
							count := 1
							if reqType == "batch" {
								count = batchCount
							}
							return stat != nil && stat.LastValue() == float64(count)
						},
					).Should(BeTrue())
				}
			}
		})

		It("should reject requests without request body", func() {
			for handlerType, handler := range allHandlers(gateway) {
				expectHandlerResponse(handler, authorizedRequest(WriteKeyEnabled, nil), http.StatusBadRequest, response.RequestBodyNil+"\n", handlerType)
			}
		})

		It("should reject requests without valid json in request body", func() {
			for handlerType, handler := range allHandlers(gateway) {
				invalidBody := "not-a-valid-json"
				expectHandlerResponse(handler, authorizedRequest(WriteKeyEnabled, bytes.NewBufferString(invalidBody)), http.StatusBadRequest, response.InvalidJSON+"\n", handlerType)
			}
		})

		It("should reject requests with request bodies larger than configured limit", func() {
			for handlerType, handler := range allHandlers(gateway) {
				data := make([]byte, gateway.conf.maxReqSize.Load())
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
					expectHandlerResponse(handler, authorizedRequest(WriteKeyEnabled, bytes.NewBufferString(body)), http.StatusRequestEntityTooLarge, response.RequestBodyTooLarge+"\n", handlerType)
				}
			}
		})

		It("should reject requests with invalid write keys", func() {
			for handlerType, handler := range allHandlers(gateway) {
				validBody := `{"data":"valid-json"}`
				if handlerType == "batch" || handlerType == "import" {
					validBody = `{"batch":[{"data":"valid-json"}]}`
				}
				expectHandlerResponse(handler, authorizedRequest(WriteKeyInvalid, bytes.NewBufferString(validBody)), http.StatusUnauthorized, response.InvalidWriteKey+"\n", handlerType)
			}
		})

		It("should reject requests with disabled write keys (source)", func() {
			for handlerType, handler := range allHandlers(gateway) {
				validBody := `{"data":"valid-json"}`
				if handlerType == "batch" || handlerType == "import" {
					validBody = `{"batch":[{"data":"valid-json"}]}`
				}
				expectHandlerResponse(handler, authorizedRequest(WriteKeyDisabled, bytes.NewBufferString(validBody)), http.StatusNotFound, response.SourceDisabled+"\n", handlerType)
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
						handlerType,
					)
					Eventually(
						func() bool {
							stat := statsStore.Get(
								"gateway.write_key_failed_requests",
								map[string]string{
									"source":      rCtxEnabled.SourceTag(),
									"sourceID":    rCtxEnabled.SourceID,
									"workspaceId": rCtxEnabled.WorkspaceID,
									"writeKey":    rCtxEnabled.WriteKey,
									"reqType":     handlerType,
									"reason":      "storeFailed",
									"sourceType":  rCtxEnabled.SourceCategory,
									"sdkVersion":  sdkStatTag,
								},
							)
							return stat != nil && stat.LastValue() == float64(1)
						},
					).Should(BeTrue())
				}
			}
		})

		It("should return empty batch payload error", func() {
			expectHandlerResponse(
				gateway.webBatchHandler(),
				authorizedRequest(
					WriteKeyEnabled,
					bytes.NewBufferString(`{"batch":[]}`),
				),
				http.StatusBadRequest,
				"Empty batch payload\n",
				"batch",
			)
		})
	})

	Context("Robots", func() {
		var gateway *Handle

		BeforeEach(func() {
			c.initializeAppFeatures()
			gateway = &Handle{}
			err := gateway.Setup(context.Background(), conf, logger.NOP, stats.NOP, c.mockApp, c.mockBackendConfig, c.mockJobsDB, c.mockErrJobsDB, nil, c.mockVersionHandler, rsources.NewNoOpService(), transformer.NewNoOpService(), sourcedebugger.NewNoOpService(), nil)
			Expect(err).To(BeNil())
			waitForBackendConfigInit(gateway)
		})

		AfterEach(func() {
			err := gateway.Shutdown()
			Expect(err).To(BeNil())
		})

		It("should return a robots.txt", func() {
			req := httptest.NewRequest("GET", "/robots.txt", nil)
			expectHandlerResponse(gateway.robotsHandler, req, http.StatusOK, "User-agent: * \nDisallow: / \n", "robots")
		})
	})

	Context("jobDataFromRequest", func() {
		var (
			gateway *Handle
			arctx   = &gwtypes.AuthRequestContext{
				WriteKey: "someWriteKey",
			}
			userIDHeader = "userIDHeader"
		)
		BeforeEach(func() {
			c.initializeAppFeatures()
			gateway = &Handle{}
			err := gateway.Setup(context.Background(), conf, logger.NOP, stats.NOP, c.mockApp, c.mockBackendConfig, c.mockJobsDB, c.mockErrJobsDB, nil, c.mockVersionHandler, rsources.NewNoOpService(), transformer.NewNoOpService(), sourcedebugger.NewNoOpService(), nil)
			Expect(err).To(BeNil())
			waitForBackendConfigInit(gateway)
		})

		AfterEach(func() {
			err := gateway.Shutdown()
			Expect(err).To(BeNil())
		})

		It("returns invalid JSON in case of invalid JSON payload", func() {
			req := &webRequestT{
				reqType:        "batch",
				authContext:    arctx,
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
				authContext:    rCtxEnabled,
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
				authContext:    rCtxEnabled,
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
				authContext:    rCtxEnabled,
				done:           make(chan<- string),
				userIDHeader:   userIDHeader,
				requestPayload: payload,
			}
			_, err = gateway.getJobDataFromRequest(req)
			Expect(err).To(BeNil())
		})

		It("sanitizes messageID, trim space and replace with new uuid", func() {
			// passing a messageID full of invisible characters
			payloadMap := map[string]interface{}{
				"batch": []interface{}{
					map[string]interface{}{
						"type":      "track",
						"userId":    map[string]interface{}{"id": 456},
						"messageId": " \u0000\u00A0\t\n\r\u034F ",
					},
				},
			}
			payload, err := json.Marshal(payloadMap)
			Expect(err).To(BeNil())
			req := &webRequestT{
				reqType:        "batch",
				authContext:    rCtxEnabled,
				done:           make(chan<- string),
				userIDHeader:   userIDHeader,
				requestPayload: payload,
			}
			jobForm, err := gateway.getJobDataFromRequest(req)
			Expect(err).To(BeNil())

			var job struct {
				Batch []struct {
					MessageID string `json:"messageID"`
				} `json:"batch"`
			}

			err = json.Unmarshal(jobForm.jobs[0].EventPayload, &job)
			Expect(err).To(BeNil())

			u, err := uuid.Parse(job.Batch[0].MessageID)
			Expect(err).To(BeNil())
			Expect(u.Version()).To(Equal(uuid.Version(4)))
		})

		It("sanitizes messageID, remove bad runes and trim space", func() {
			// passing a messageID full of invisible characters
			payloadMap := map[string]interface{}{
				"batch": []interface{}{
					map[string]interface{}{
						"type":      "track",
						"userId":    map[string]interface{}{"id": 456},
						"messageId": "\u0000 -a-random-string \u00A0\t\n\r\u034F",
					},
				},
			}
			payload, err := json.Marshal(payloadMap)
			Expect(err).To(BeNil())
			req := &webRequestT{
				reqType:        "batch",
				authContext:    rCtxEnabled,
				done:           make(chan<- string),
				userIDHeader:   userIDHeader,
				requestPayload: payload,
			}
			jobForm, err := gateway.getJobDataFromRequest(req)
			Expect(err).To(BeNil())

			var job struct {
				Batch []struct {
					MessageID string `json:"messageID"`
				} `json:"batch"`
			}

			err = json.Unmarshal(jobForm.jobs[0].EventPayload, &job)
			Expect(err).To(BeNil())
			Expect(job.Batch[0].MessageID).To(Equal("-a-random-string"))
		})

		It("doesn't override if receivedAt or request_ip already exists in payload", func() {
			req := &webRequestT{
				reqType:        "batch",
				authContext:    rCtxEnabled,
				done:           make(chan<- string),
				userIDHeader:   userIDHeader,
				requestPayload: []byte(`{"batch": [{"type": "extract", "receivedAt": "2024-01-01T01:01:01.000000001Z", "request_ip": "dummyIPFromPayload"}]}`),
			}
			jobForm, err := gateway.getJobDataFromRequest(req)
			Expect(err).To(BeNil())

			var job struct {
				Batch []struct {
					ReceivedAt string `json:"receivedAt"`
					RequestIP  string `json:"request_ip"`
				} `json:"batch"`
			}
			err = json.Unmarshal(jobForm.jobs[0].EventPayload, &job)
			Expect(err).To(BeNil())
			Expect(job.Batch[0].ReceivedAt).To(ContainSubstring("2024-01-01T01:01:01.000000001Z"))
			Expect(job.Batch[0].RequestIP).To(ContainSubstring("dummyIPFromPayload"))
		})

		It("adds receivedAt and request_ip in the request payload if it's not already present", func() {
			req := &webRequestT{
				reqType:        "batch",
				authContext:    rCtxEnabled,
				done:           make(chan<- string),
				userIDHeader:   userIDHeader,
				requestPayload: []byte(`{"batch": [{"type": "extract"}]}`),
			}
			req.ipAddr = "dummyIP"
			jobForm, err := gateway.getJobDataFromRequest(req)
			Expect(err).To(BeNil())

			var job struct {
				Batch []struct {
					ReceivedAt string `json:"receivedAt"`
					RequestIP  string `json:"request_ip"`
				} `json:"batch"`
			}
			err = json.Unmarshal(jobForm.jobs[0].EventPayload, &job)
			Expect(err).To(BeNil())
			Expect(job.Batch[0].ReceivedAt).To(Not(BeEmpty()))
			Expect(job.Batch[0].RequestIP).To(ContainSubstring("dummyIP"))
		})

		It("allows extract events even if userID and anonID are not present in the request payload", func() {
			req := &webRequestT{
				reqType:        "batch",
				authContext:    rCtxEnabled,
				done:           make(chan<- string),
				userIDHeader:   userIDHeader,
				requestPayload: []byte(`{"batch": [{"type": "extract"}]}`),
			}
			_, err := gateway.getJobDataFromRequest(req)
			Expect(err).To(BeNil())
		})

		It("allows retl events even if userID and anonID are not present in the request payload", func() {
			req := &webRequestT{
				reqType:        "retl",
				authContext:    rCtxEnabled,
				done:           make(chan<- string),
				userIDHeader:   userIDHeader,
				requestPayload: []byte(`{"batch": [{"type": "record"}]}`),
			}
			_, err := gateway.getJobDataFromRequest(req)
			Expect(err).To(BeNil())
		})
	})

	Context("SaveWebhookFailures", func() {
		var gateway *Handle
		BeforeEach(func() {
			c.initializeAppFeatures()
			gateway = &Handle{}
			err := gateway.Setup(context.Background(), conf, logger.NOP, stats.NOP, c.mockApp, c.mockBackendConfig, c.mockJobsDB, c.mockErrJobsDB, nil, c.mockVersionHandler, rsources.NewNoOpService(), transformer.NewNoOpService(), sourcedebugger.NewNoOpService(), nil)
			Expect(err).To(BeNil())
			waitForBackendConfigInit(gateway)
		})

		AfterEach(func() {
			err := gateway.Shutdown()
			Expect(err).To(BeNil())
		})

		It("should save failures to error db", func() {
			c.mockErrJobsDB.
				EXPECT().Store(
				gomock.Any(),
				gomock.Any(),
			).
				DoAndReturn(
					func(
						ctx context.Context,
						jobs []*jobsdb.JobT,
					) error {
						for idx, job := range jobs {
							Expect(misc.IsValidUUID(job.UUID.String())).To(BeTrue())
							Expect(job.CustomVal).To(Equal("WEBHOOK"))

							var paramsMap, expectedParamsMap map[string]interface{}
							var expectedStr []byte

							switch idx {
							case 0:
								Expect(job.EventPayload).To(Equal(json.RawMessage(`{"a1": "b1"}`)))
								expectedStr = []byte(fmt.Sprintf(`{"source_id": "%v", "stage": "webhook", "source_type": "cio", "reason": "err1"}`, SourceIDEnabled))
							case 1:
								Expect(job.EventPayload).To(Equal(json.RawMessage(`{"a2": "b2"}`)))
								expectedStr = []byte(fmt.Sprintf(`{"source_id": "%v", "stage": "webhook", "source_type": "af", "reason": "err2"}`, SourceIDEnabled))
							}

							_ = json.Unmarshal(job.Parameters, &paramsMap)
							_ = json.Unmarshal(expectedStr, &expectedParamsMap)
							equals := reflect.DeepEqual(paramsMap, expectedParamsMap)
							Expect(equals).To(BeTrue())
						}
						return nil
					}).
				Times(1)

			reqs := make([]*webhookModel.FailedWebhookPayload, 2)
			reqs[0] = &webhookModel.FailedWebhookPayload{
				RequestContext: rCtxEnabled,
				Payload:        []byte(`{"a1": "b1"}`),
				SourceType:     "cio",
				Reason:         "err1",
			}
			reqs[1] = &webhookModel.FailedWebhookPayload{
				RequestContext: rCtxEnabled,
				Payload:        []byte(`{"a2": "b2"}`),
				SourceType:     "af",
				Reason:         "err2",
			}
			err := gateway.SaveWebhookFailures(reqs)
			Expect(err).To(BeNil())
		})
	})

	Context("Internal Batch", func() {
		client := http.Client{}
		var (
			gateway               *Handle
			internalBatchEndpoint string
			ctx                   context.Context
			cancel                func()
			wait                  chan struct{}
			srcDebugger           *mocksrcdebugger.MockSourceDebugger
		)

		createInternalBatchPayload := func(userID, sourceID string) []byte {
			validData := fmt.Sprintf(
				`{"userId":%q,"data":{"string":"valid-json","nested":{"child":1}},%s}`,
				userID, sdkContext,
			)
			internalBatchPayload := fmt.Sprintf(`[{
			"properties": {
				"requestType": "dummyRequestType",
				"routingKey": "anonymousId_header<<>>anonymousId_1<<>>identified_user_id",
				"workspaceID": %q,
				"userID": %q,
				"sourceID": %q,
				"sourceJobRunID": "sourceJobRunID",
				"sourceTaskRunID": "sourceTaskRunID",
				"receivedAt": "2024-01-01T01:01:01.000000001Z",
				"requestIP": "1.1.1.1",
				"traceID": "traceID"
			},
			"payload": %s
			}]`, WorkspaceID, userID, sourceID, validData)
			return []byte(internalBatchPayload)
		}

		createInternalBatchPayloadWithMultipleEvents := func(userID, sourceID, workspaceID string) []byte {
			validData := fmt.Sprintf(
				`{"userId":%q,"data":{"string":"valid-json","nested":{"child":1}},%s}`,
				userID, sdkContext,
			)
			internalBatchPayload := func() string {
				return fmt.Sprintf(`{
			"properties": {
				"requestType": "dummyRequestType",
				"routingKey": "anonymousId_header<<>>anonymousId_1<<>>identified_user_id",
				"workspaceID": %q,
				"userID": %q,
				"sourceID": %q,
				"sourceJobRunID": "sourceJobRunID",
				"sourceTaskRunID": "sourceTaskRunID",
				"receivedAt": "2024-01-01T01:01:01.000000001Z",
				"requestIP": "1.1.1.1",
				"traceID": "traceID"
			},
			"payload": %s
			}`, workspaceID, userID, sourceID, validData)
			}
			return []byte(fmt.Sprintf(`[%s,%s]`, internalBatchPayload(), internalBatchPayload()))
		}

		var statStore *memstats.Store
		// a second after receivedAt
		now, err := time.Parse(time.RFC3339Nano, "2024-01-01T01:01:02.000000001Z")
		Expect(err).To(BeNil())

		BeforeEach(func() {
			c.mockSuppressUser = mocksTypes.NewMockUserSuppression(c.mockCtrl)
			c.mockSuppressUserFeature = mocksApp.NewMockSuppressUserFeature(c.mockCtrl)
			c.initializeEnterpriseAppFeatures()

			c.mockSuppressUserFeature.EXPECT().Setup(gomock.Any(), gomock.Any()).AnyTimes().Return(c.mockSuppressUser, nil)
			c.mockSuppressUser.EXPECT().GetSuppressedUser(WorkspaceID, NormalUserID, SourceIDEnabled).Return(nil).AnyTimes()
			c.mockSuppressUser.EXPECT().GetSuppressedUser(WorkspaceID, SuppressedUserID, SourceIDEnabled).Return(&model.Metadata{
				CreatedAt: time.Now(),
			}).AnyTimes()
			c.mockSuppressUser.EXPECT().GetSuppressedUser(WorkspaceID, NormalUserID, writeKeyNotPresentInSource).Return(nil).AnyTimes()

			conf = config.New()
			conf.Set("Gateway.enableRateLimit", false)
			conf.Set("Gateway.enableSuppressUserFeature", true)
			conf.Set("Gateway.enableEventSchemasFeature", false)

			serverPort, err := kithelper.GetFreePort()
			Expect(err).To(BeNil())
			internalBatchEndpoint = fmt.Sprintf("http://localhost:%d/internal/v1/batch", serverPort)
			GinkgoT().Setenv("RSERVER_GATEWAY_WEB_PORT", strconv.Itoa(serverPort))

			statStore, err = memstats.New(
				memstats.WithNow(func() time.Time {
					return now
				}),
			)
			Expect(err).To(BeNil())

			gateway = &Handle{}
			srcDebugger = mocksrcdebugger.NewMockSourceDebugger(c.mockCtrl)
			err = gateway.Setup(context.Background(), conf, logger.NOP, statStore, c.mockApp, c.mockBackendConfig, c.mockJobsDB, c.mockErrJobsDB, nil, c.mockVersionHandler, rsources.NewNoOpService(), transformer.NewNoOpService(), srcDebugger, nil)
			Expect(err).To(BeNil())
			waitForBackendConfigInit(gateway)
			c.mockBackendConfig.EXPECT().WaitForConfig(gomock.Any()).AnyTimes()
			ctx, cancel = context.WithCancel(context.Background())
			wait = make(chan struct{})
			go func() {
				err = gateway.StartWebHandler(ctx)
				Expect(err).To(BeNil())
				close(wait)
			}()
			Eventually(func() bool {
				resp, err := http.Get(fmt.Sprintf("http://localhost:%d/version", serverPort))
				if err != nil {
					return false
				}
				return resp.StatusCode == http.StatusOK
			}, time.Second*10, time.Second).Should(BeTrue())
		})

		AfterEach(func() {
			cancel()
			<-wait
			c.Finish()
			err := gateway.Shutdown()
			Expect(err).To(BeNil())
		})

		It("Successful request, with debugger", func() {
			srcDebugger.EXPECT().RecordEvent(WriteKeyEnabled, gomock.Any()).Times(1)
			c.mockJobsDB.EXPECT().WithStoreSafeTx(gomock.Any(), gomock.Any()).Times(1)
			payload := createInternalBatchPayload(NormalUserID, SourceIDEnabled)
			req, err := http.NewRequest(http.MethodPost, internalBatchEndpoint, bytes.NewBuffer(payload))
			Expect(err).To(BeNil())
			resp, err := client.Do(req)
			Expect(err).To(BeNil())
			Expect(http.StatusOK, resp.StatusCode)

			Expect(statStore.GetByName("gateway.event_pickup_lag_seconds")).To(Equal([]memstats.Metric{
				{
					Name: "gateway.event_pickup_lag_seconds",
					Tags: map[string]string{"workspaceId": WorkspaceID},
					Durations: []time.Duration{
						time.Second,
					},
				},
			}))
			Expect(statStore.GetByName("gateway.write_key_events")).To(Equal([]memstats.Metric{
				{
					Name: "gateway.write_key_events",
					Tags: map[string]string{
						"source":      "",
						"writeKey":    WriteKeyEnabled,
						"reqType":     "internalBatch",
						"workspaceId": WorkspaceID,
						"sourceID":    SourceIDEnabled,
						"sourceType":  "",
						"sdkVersion":  "",
					},
					Value: 1,
				},
			}))
			Expect(statStore.GetByName("gateway.write_key_successful_events")).To(Equal([]memstats.Metric{
				{
					Name: "gateway.write_key_successful_events",
					Tags: map[string]string{
						"source":      "",
						"writeKey":    WriteKeyEnabled,
						"reqType":     "internalBatch",
						"workspaceId": WorkspaceID,
						"sourceID":    SourceIDEnabled,
						"sourceType":  "",
						"sdkVersion":  "",
					},
					Value: 1,
				},
			}))
			Expect(statStore.GetByName("gateway.write_key_requests")).To(Equal([]memstats.Metric{
				{
					Name: "gateway.write_key_requests",
					Tags: map[string]string{
						"workspaceId": "",
						"sourceID":    "",
						"sourceType":  "",
						"sdkVersion":  "",
						"source":      "",
						"writeKey":    "",
						"reqType":     "internalBatch",
					},
					Value: 1,
				},
			}))
			Expect(statStore.GetByName("gateway.write_key_successful_requests")).To(Equal([]memstats.Metric{
				{
					Name: "gateway.write_key_successful_requests",
					Tags: map[string]string{
						"source":      "",
						"writeKey":    "",
						"reqType":     "internalBatch",
						"workspaceId": "",
						"sourceID":    "",
						"sourceType":  "",
						"sdkVersion":  "",
					},
					Value: 1,
				},
			}))
			Expect(statStore.GetByName("gateway.write_key_failed_requests")).To(Equal([]memstats.Metric{
				{
					Name: "gateway.write_key_failed_requests",
					Tags: map[string]string{
						"source":      "",
						"writeKey":    "",
						"reqType":     "internalBatch",
						"workspaceId": "",
						"sourceID":    "",
						"sourceType":  "",
						"sdkVersion":  "",
					},
					Value: 0,
				},
			}))
			Expect(statStore.GetByName("gateway.write_key_failed_events")).To(Equal([]memstats.Metric{
				{
					Name: "gateway.write_key_failed_events",
					Tags: map[string]string{
						"source":      "",
						"writeKey":    WriteKeyEnabled,
						"reqType":     "internalBatch",
						"workspaceId": WorkspaceID,
						"sourceID":    SourceIDEnabled,
						"sourceType":  "",
						"sdkVersion":  "",
					},
					Value: 0,
				},
			}))
		})

		It("Successful request, without debugger", func() {
			c.mockJobsDB.EXPECT().WithStoreSafeTx(gomock.Any(), gomock.Any()).Times(1)
			payload := createInternalBatchPayload(NormalUserID, writeKeyNotPresentInSource)
			req, err := http.NewRequest(http.MethodPost, internalBatchEndpoint, bytes.NewBuffer(payload))
			Expect(err).To(BeNil())
			resp, err := client.Do(req)
			Expect(err).To(BeNil())
			Expect(http.StatusOK, resp.StatusCode)
		})

		It("request failed 0 messages", func() {
			req, err := http.NewRequest(http.MethodPost, internalBatchEndpoint, bytes.NewBuffer([]byte(`[]`)))
			Expect(err).To(BeNil())
			resp, err := client.Do(req)
			Expect(err).To(BeNil())
			Expect(http.StatusOK, resp.StatusCode)
			respData, err := io.ReadAll(resp.Body)
			defer httputil.CloseResponse(resp)
			Expect(err).To(BeNil())
			Expect(string(respData)).Should(ContainSubstring(response.NotRudderEvent))
			failedRequestStat := statStore.Get("gateway.write_key_failed_requests", map[string]string{
				"writeKey":    "",
				"reqType":     "internalBatch",
				"reason":      response.NotRudderEvent,
				"workspaceId": "",
				"sourceID":    "",
				"sourceType":  "",
				"sdkVersion":  "",
				"source":      "",
			})
			Expect(failedRequestStat).To(Not(BeNil()))
			Expect(failedRequestStat.Values()).To(Equal([]float64{1}))
		})

		It("request failed unmarshall error", func() {
			req, err := http.NewRequest(http.MethodPost, internalBatchEndpoint, bytes.NewBuffer([]byte(`[`)))
			Expect(err).To(BeNil())
			resp, err := client.Do(req)
			Expect(err).To(BeNil())
			Expect(http.StatusBadRequest, resp.StatusCode)
			respData, err := io.ReadAll(resp.Body)
			defer httputil.CloseResponse(resp)
			Expect(err).To(BeNil())
			Expect(string(respData)).Should(ContainSubstring(response.InvalidJSON))
			failedRequestStat := statStore.Get("gateway.write_key_failed_requests", map[string]string{
				"writeKey":    "",
				"reqType":     "internalBatch",
				"reason":      response.InvalidJSON,
				"workspaceId": "",
				"sourceID":    "",
				"sourceType":  "",
				"sdkVersion":  "",
				"source":      "",
			})
			Expect(failedRequestStat).To(Not(BeNil()))
			Expect(failedRequestStat.Values()).To(Equal([]float64{1}))
			failedEventStat := statStore.Get("gateway.write_key_failed_events", map[string]string{
				"writeKey":    "",
				"reqType":     "internalBatch",
				"reason":      response.InvalidJSON,
				"workspaceId": "",
				"sourceID":    "",
				"sourceType":  "",
				"sdkVersion":  "",
				"source":      "",
			})
			Expect(failedEventStat).To(BeNil())
		})

		It("request success - suppressed user", func() {
			payload := createInternalBatchPayload(SuppressedUserID, SourceIDEnabled)
			req, err := http.NewRequest(http.MethodPost, internalBatchEndpoint, bytes.NewBuffer(payload))
			Expect(err).To(BeNil())
			resp, err := client.Do(req)
			Expect(err).To(BeNil())
			Expect(http.StatusOK, resp.StatusCode)
			successfulReqStat := statStore.Get("gateway.write_key_successful_requests", map[string]string{
				"writeKey":    "",
				"reqType":     "internalBatch",
				"workspaceId": "",
				"sourceID":    "",
				"sourceType":  "",
				"sdkVersion":  "",
				"source":      "",
			})
			Expect(successfulReqStat).To(Not(BeNil()))
			Expect(successfulReqStat.Values()).To(Equal([]float64{1}))
		})

		It("request success - multiple messages", func() {
			srcDebugger.EXPECT().RecordEvent(WriteKeyEnabled, gomock.Any()).Times(2)
			c.mockJobsDB.EXPECT().WithStoreSafeTx(gomock.Any(), gomock.Any()).Times(1)
			payload := createInternalBatchPayloadWithMultipleEvents(NormalUserID, SourceIDEnabled, WorkspaceID)
			req, err := http.NewRequest(http.MethodPost, internalBatchEndpoint, bytes.NewBuffer(payload))
			Expect(err).To(BeNil())
			resp, err := client.Do(req)
			Expect(err).To(BeNil())
			Expect(http.StatusOK, resp.StatusCode)
			successfulReqStat := statStore.Get("gateway.write_key_successful_requests", map[string]string{
				"writeKey":    "",
				"reqType":     "internalBatch",
				"workspaceId": "",
				"sourceID":    "",
				"sourceType":  "",
				"sdkVersion":  "",
				"source":      "",
			})
			Expect(successfulReqStat).To(Not(BeNil()))
			Expect(successfulReqStat.LastValue()).To(Equal(float64(1)))
			successfulEventStat := statStore.Get("gateway.write_key_successful_events", map[string]string{
				"writeKey":    WriteKeyEnabled,
				"reqType":     "internalBatch",
				"workspaceId": WorkspaceID,
				"sourceID":    SourceIDEnabled,
				"sourceType":  "",
				"sdkVersion":  "",
				"source":      "",
			})
			Expect(successfulEventStat).To(Not(BeNil()))
			Expect(successfulEventStat.LastValue()).To(Equal(float64(2)))
			eventsStat := statStore.Get("gateway.write_key_events", map[string]string{
				"writeKey":    WriteKeyEnabled,
				"reqType":     "internalBatch",
				"workspaceId": WorkspaceID,
				"sourceID":    SourceIDEnabled,
				"sourceType":  "",
				"sdkVersion":  "",
				"source":      "",
			})
			Expect(eventsStat).To(Not(BeNil()))
			Expect(eventsStat.Values()).To(Equal([]float64{1, 2}))
		})

		It("request failed db error", func() {
			c.mockJobsDB.EXPECT().WithStoreSafeTx(gomock.Any(), gomock.Any()).Times(1).Return(fmt.Errorf("db error"))
			payload := createInternalBatchPayload(NormalUserID, SourceIDEnabled)
			req, err := http.NewRequest(http.MethodPost, internalBatchEndpoint, bytes.NewBuffer(payload))
			Expect(err).To(BeNil())
			resp, err := client.Do(req)
			Expect(err).To(BeNil())
			Expect(http.StatusInternalServerError, resp.StatusCode)
			failedReqStat := statStore.Get("gateway.write_key_failed_requests", map[string]string{
				"writeKey":    "",
				"reqType":     "internalBatch",
				"workspaceId": "",
				"sourceID":    "",
				"sourceType":  "",
				"sdkVersion":  "",
				"source":      "",
				"reason":      "storeFailed",
			})
			Expect(failedReqStat).To(Not(BeNil()))
			Expect(failedReqStat.Values()).To(Equal([]float64{1}))
			failedEventStat := statStore.Get("gateway.write_key_failed_events", map[string]string{
				"writeKey":    WriteKeyEnabled,
				"reqType":     "internalBatch",
				"workspaceId": WorkspaceID,
				"sourceID":    SourceIDEnabled,
				"sourceType":  "",
				"sdkVersion":  "",
				"source":      "",
				"reason":      "storeFailed",
			})
			Expect(failedEventStat).To(Not(BeNil()))
			Expect(failedEventStat.Values()).To(Equal([]float64{1}))
			eventsStat := statStore.Get("gateway.write_key_events", map[string]string{
				"writeKey":    WriteKeyEnabled,
				"reqType":     "internalBatch",
				"workspaceId": WorkspaceID,
				"sourceID":    SourceIDEnabled,
				"sourceType":  "",
				"sdkVersion":  "",
				"source":      "",
			})
			Expect(eventsStat).To(Not(BeNil()))
			Expect(eventsStat.Values()).To(Equal([]float64{1}))
		})
	})

	Context("extractJobsFromInternalBatchPayload", func() {
		var gateway *Handle
		BeforeEach(func() {
			c.initializeAppFeatures()
			gateway = &Handle{}
			err := gateway.Setup(context.Background(), conf, logger.NOP, stats.NOP, c.mockApp, c.mockBackendConfig, c.mockJobsDB, c.mockErrJobsDB, nil, c.mockVersionHandler, rsources.NewNoOpService(), transformer.NewNoOpService(), sourcedebugger.NewNoOpService(), nil)
			Expect(err).To(BeNil())
			waitForBackendConfigInit(gateway)
		})

		AfterEach(func() {
			err := gateway.Shutdown()
			Expect(err).To(BeNil())
		})

		It("doesn't override if receivedAt or request_ip already exists in payload", func() {
			properties := stream.MessageProperties{
				RequestType:   "dummyRequestType",
				RoutingKey:    "anonymousId_header<<>>anonymousId_1<<>>identified_user_id",
				WorkspaceID:   "workspaceID",
				SourceID:      "sourceID",
				ReceivedAt:    time.Date(2024, 1, 1, 1, 1, 1, 1, time.UTC),
				RequestIP:     "dummyIP",
				DestinationID: "destinationID",
			}
			msg := stream.Message{
				Properties: properties,
				Payload:    []byte(`{"receivedAt": "dummyReceivedAtFromPayload", "request_ip": "dummyIPFromPayload"}`),
			}
			messages := []stream.Message{msg}
			payload, err := json.Marshal(messages)
			Expect(err).To(BeNil())

			req := &webRequestT{
				reqType:        "batch",
				authContext:    rCtxEnabled,
				done:           make(chan<- string),
				requestPayload: payload,
			}
			jobsWithStats, err := gateway.extractJobsFromInternalBatchPayload("batch", req.requestPayload)
			Expect(err).To(BeNil())
			Expect(jobsWithStats).To(HaveLen(1))
			Expect(jobsWithStats[0].stat).To(Equal(
				gwstats.SourceStat{
					SourceID:    "sourceID",
					WorkspaceID: "workspaceID",
					ReqType:     "batch",
				},
			))

			var job struct {
				Batch []struct {
					ReceivedAt string `json:"receivedAt"`
					RequestIP  string `json:"request_ip"`
				} `json:"batch"`
			}
			jobForm := jobsWithStats[0].job
			err = json.Unmarshal(jobForm.EventPayload, &job)
			Expect(err).To(BeNil())
			Expect(job.Batch).To(HaveLen(1))
			Expect(job.Batch[0].ReceivedAt).To(ContainSubstring("dummyReceivedAtFromPayload"))
			Expect(job.Batch[0].RequestIP).To(ContainSubstring("dummyIPFromPayload"))
		})

		It("adds receivedAt and request_ip in the request payload if it's not already present", func() {
			properties := stream.MessageProperties{
				RequestType:   "dummyRequestType",
				RoutingKey:    "anonymousId_header<<>>anonymousId_1<<>>identified_user_id",
				WorkspaceID:   "workspaceID",
				SourceID:      "sourceID",
				ReceivedAt:    time.Date(2024, 1, 1, 1, 1, 1, 1, time.UTC),
				RequestIP:     "dummyIP",
				DestinationID: "destinationID",
			}
			msg := stream.Message{
				Properties: properties,
				Payload:    []byte(`{}`),
			}
			messages := []stream.Message{msg}
			payload, err := json.Marshal(messages)
			Expect(err).To(BeNil())
			req := &webRequestT{
				reqType:        "batch",
				authContext:    rCtxEnabled,
				done:           make(chan<- string),
				requestPayload: payload,
			}
			jobsWithStats, err := gateway.extractJobsFromInternalBatchPayload("batch", req.requestPayload)
			Expect(err).To(BeNil())
			Expect(jobsWithStats).To(HaveLen(1))
			Expect(jobsWithStats[0].stat).To(Equal(gwstats.SourceStat{
				SourceID:    "sourceID",
				WorkspaceID: "workspaceID",
				ReqType:     "batch",
			}))

			var job struct {
				Batch []struct {
					ReceivedAt string `json:"receivedAt"`
					RequestIP  string `json:"request_ip"`
				} `json:"batch"`
			}
			err = json.Unmarshal(jobsWithStats[0].job.EventPayload, &job)
			Expect(err).To(BeNil())
			Expect(job.Batch).To(HaveLen(1))
			Expect(job.Batch[0].ReceivedAt).To(ContainSubstring("2024-01-01T01:01:01.000Z"))
			Expect(job.Batch[0].RequestIP).To(ContainSubstring("dummyIP"))
		})

		It("adds messageID, rudderId in the request payload if it's not already present", func() {
			properties := stream.MessageProperties{
				RequestType:   "dummyRequestType",
				RoutingKey:    "anonymousId_header<<>>anonymousId_1<<>>identified_user_id",
				WorkspaceID:   "workspaceID",
				SourceID:      "sourceID",
				ReceivedAt:    time.Date(2024, 1, 1, 1, 1, 1, 1, time.UTC),
				RequestIP:     "dummyIP",
				DestinationID: "destinationID",
			}
			msg := stream.Message{
				Properties: properties,
				Payload:    []byte(`{}`),
			}
			messages := []stream.Message{msg}
			payload, err := json.Marshal(messages)
			Expect(err).To(BeNil())
			req := &webRequestT{
				reqType:        "batch",
				authContext:    rCtxEnabled,
				done:           make(chan<- string),
				requestPayload: payload,
			}
			jobsWithStats, err := gateway.extractJobsFromInternalBatchPayload("batch", req.requestPayload)
			Expect(err).To(BeNil())
			Expect(jobsWithStats).To(HaveLen(1))
			Expect(jobsWithStats[0].stat).To(Equal(gwstats.SourceStat{
				SourceID:    "sourceID",
				WorkspaceID: "workspaceID",
				ReqType:     "batch",
			}))

			var job struct {
				Batch []struct {
					MessageID string `json:"messageID"`
					RudderID  string `json:"rudderId"`
				} `json:"batch"`
			}
			err = json.Unmarshal(jobsWithStats[0].job.EventPayload, &job)
			Expect(err).To(BeNil())
			Expect(job.Batch).To(HaveLen(1))
			Expect(job.Batch[0].MessageID).To(Not(BeEmpty()))
			Expect(job.Batch[0].RudderID).To(Not(BeEmpty()))
		})

		It("doesn't override if messageID already exists in payload", func() {
			properties := stream.MessageProperties{
				RequestType:   "dummyRequestType",
				RoutingKey:    "anonymousId_header<<>>anonymousId_1<<>>identified_user_id",
				WorkspaceID:   "workspaceID",
				SourceID:      "sourceID",
				ReceivedAt:    time.Date(2024, 1, 1, 1, 1, 1, 1, time.UTC),
				RequestIP:     "dummyIP",
				DestinationID: "destinationID",
			}
			msg := stream.Message{
				Properties: properties,
				Payload:    []byte(`{"messageId": "dummyMessageID"}`),
			}
			messages := []stream.Message{msg}
			payload, err := json.Marshal(messages)
			Expect(err).To(BeNil())

			req := &webRequestT{
				reqType:        "batch",
				authContext:    rCtxEnabled,
				done:           make(chan<- string),
				requestPayload: payload,
			}
			jobsWithStats, err := gateway.extractJobsFromInternalBatchPayload("batch", req.requestPayload)
			Expect(err).To(BeNil())
			Expect(jobsWithStats).To(HaveLen(1))
			Expect(jobsWithStats[0].stat).To(Equal(
				gwstats.SourceStat{
					SourceID:    "sourceID",
					WorkspaceID: "workspaceID",
					ReqType:     "batch",
				},
			))

			var job struct {
				Batch []struct {
					MessageID string `json:"messageId"`
				} `json:"batch"`
			}
			jobForm := jobsWithStats[0].job
			err = json.Unmarshal(jobForm.EventPayload, &job)
			Expect(err).To(BeNil())
			Expect(job.Batch).To(HaveLen(1))
			Expect(job.Batch[0].MessageID).To(ContainSubstring("dummyMessageID"))
		})

		It("adds type in the request payload if RequestType Property is not one of batch, replay, retl, import", func() {
			properties := stream.MessageProperties{
				RequestType:   "dummyRequestType",
				RoutingKey:    "anonymousId_header<<>>anonymousId_1<<>>identified_user_id",
				WorkspaceID:   "workspaceID",
				SourceID:      "sourceID",
				ReceivedAt:    time.Date(2024, 1, 1, 1, 1, 1, 1, time.UTC),
				RequestIP:     "dummyIP",
				DestinationID: "destinationID",
			}
			msg := stream.Message{
				Properties: properties,
				Payload:    []byte(`{}`),
			}
			messages := []stream.Message{msg}
			payload, err := json.Marshal(messages)
			Expect(err).To(BeNil())
			req := &webRequestT{
				reqType:        "batch",
				authContext:    rCtxEnabled,
				done:           make(chan<- string),
				requestPayload: payload,
			}
			jobsWithStats, err := gateway.extractJobsFromInternalBatchPayload("batch", req.requestPayload)
			Expect(err).To(BeNil())
			Expect(jobsWithStats).To(HaveLen(1))
			Expect(jobsWithStats[0].stat).To(Equal(gwstats.SourceStat{
				SourceID:    "sourceID",
				WorkspaceID: "workspaceID",
				ReqType:     "batch",
			}))

			var job struct {
				Batch []struct {
					Type string `json:"type"`
				} `json:"batch"`
			}
			err = json.Unmarshal(jobsWithStats[0].job.EventPayload, &job)
			Expect(err).To(BeNil())
			Expect(job.Batch).To(HaveLen(1))
			Expect(job.Batch[0].Type).To(ContainSubstring("dummyRequestType"))
		})

		It("does not change type if  RequestType Property is batch, replay, retl, import", func() {
			requestTypes := []string{"batch", "replay", "retl", "import"}
			for _, reqType := range requestTypes {
				properties := stream.MessageProperties{
					RequestType:   reqType,
					RoutingKey:    "anonymousId_header<<>>anonymousId_1<<>>identified_user_id",
					WorkspaceID:   "workspaceID",
					SourceID:      "sourceID",
					ReceivedAt:    time.Date(2024, 1, 1, 1, 1, 1, 1, time.UTC),
					RequestIP:     "dummyIP",
					DestinationID: "destinationID",
				}
				msg := stream.Message{
					Properties: properties,
					Payload:    []byte(`{"type": "dummyType"}`),
				}
				messages := []stream.Message{msg}
				payload, err := json.Marshal(messages)
				Expect(err).To(BeNil())
				req := &webRequestT{
					reqType:        "batch",
					authContext:    rCtxEnabled,
					done:           make(chan<- string),
					requestPayload: payload,
				}
				jobsWithStats, err := gateway.extractJobsFromInternalBatchPayload("batch", req.requestPayload)
				Expect(err).To(BeNil())
				Expect(jobsWithStats).To(HaveLen(1))
				Expect(jobsWithStats[0].stat).To(Equal(gwstats.SourceStat{
					SourceID:    "sourceID",
					WorkspaceID: "workspaceID",
					ReqType:     "batch",
				}))

				var job struct {
					Batch []struct {
						Type string `json:"type"`
					} `json:"batch"`
				}
				err = json.Unmarshal(jobsWithStats[0].job.EventPayload, &job)
				Expect(err).To(BeNil())
				Expect(job.Batch).To(HaveLen(1))
				Expect(job.Batch[0].Type).To(ContainSubstring("dummyType"))
			}
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

func authorizedReplayRequest(sourceID string, body io.Reader) *http.Request {
	req := unauthorizedRequest(body)
	req.Header.Set("X-Rudder-Source-Id", sourceID)
	// set anonymousId header to ensure everything goes into same batch
	req.Header.Set("AnonymousId", "094985f8-b4eb-43c3-bc8a-e8b75aae9c7c")
	req.RemoteAddr = TestRemoteAddressWithPort
	return req
}

func authorizedRETLRequest(sourceID string, body io.Reader) *http.Request {
	req := unauthorizedRequest(body)
	req.Header.Set("X-Rudder-Source-Id", sourceID)
	// set anonymousId header to ensure everything goes into same batch
	req.Header.Set("AnonymousId", "094985f8-b4eb-43c3-bc8a-e8b75aae9c7c")
	req.RemoteAddr = TestRemoteAddressWithPort
	return req
}

func expectHandlerResponse(handler http.HandlerFunc, req *http.Request, responseStatus int, responseBody, reqType string) {
	var body string
	Eventually(func() int {
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req.Clone(context.Background()))
		if rr.Body != nil {
			bodyBytes, _ := io.ReadAll(rr.Body)
			body = string(bodyBytes)
		} else {
			body = ""
		}
		return rr.Result().StatusCode
	}).Should(Equal(responseStatus), fmt.Sprintf("reqType: %s", reqType))
	Expect(body).To(Equal(responseBody), fmt.Sprintf("reqType: %s", reqType))
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
		// TODO: Remove this endpoint once sources change is released
		"/v1/warehouse/fetch-tables",
		"/internal/v1/warehouse/fetch-tables",
		"/internal/v1/job-status/123",
		"/internal/v1/job-status/123/failed-records",
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
		"/v1/audiencelist", // Get rid of this over time and use the /internal endpoint
		"/v1/webhook",
		"/beacon/v1/batch",
		"/internal/v1/extract",
		"/internal/v1/retl",
		"/internal/v1/replay",
		"/internal/v1/audiencelist",
		"/v1/warehouse/pending-events",
		"/v1/warehouse/trigger-upload",
		"/v1/warehouse/jobs",
		// "/internal/v1/batch", will be tested in new unit test
	}

	deleteEndpoints := []string{
		"/v1/job-status/1234",
	}
	return getEndpoints, postEndpoints, deleteEndpoints
}

func allHandlers(gw *Handle) map[string]http.HandlerFunc {
	return map[string]http.HandlerFunc{
		"alias":        gw.webAliasHandler(),
		"batch":        gw.webBatchHandler(),
		"group":        gw.webGroupHandler(),
		"identify":     gw.webIdentifyHandler(),
		"page":         gw.webPageHandler(),
		"screen":       gw.webScreenHandler(),
		"track":        gw.webTrackHandler(),
		"import":       gw.webImportHandler(),
		"audiencelist": gw.webAudienceListHandler(),
		"extract":      gw.webExtractHandler(),
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
	handlerToTest := withContentType(expectedContentType, nextHandler)
	respRecorder := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "http://testing", nil)

	handlerToTest.ServeHTTP(respRecorder, req)
	receivedContentType := respRecorder.Header()["Content-Type"][0]
	require.Equal(t, expectedContentType, receivedContentType, "actual content type different than expected.")
	require.Equal(t, expectedStatus, respRecorder.Code, "actual response code different than expected.")
}

type mockRequestHandler struct{}

func (mockRequestHandler) ProcessRequest(_ *http.ResponseWriter, _ *http.Request, _ string, _ []byte, _ *gwtypes.AuthRequestContext) string {
	return ""
}

func waitForBackendConfigInit(gw *Handle) {
	Eventually(
		func() bool {
			select {
			case <-gw.backendConfigInitialisedChan:
				return true
			default:
				return false
			}
		},
		2*time.Second,
	).Should(BeTrue())
}
