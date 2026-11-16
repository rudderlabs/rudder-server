package gateway

import (
	"bytes"
	"context"
	"encoding/base64"
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

	"github.com/google/uuid"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/minio"
	kituuid "github.com/rudderlabs/rudder-go-kit/uuid"
	"github.com/rudderlabs/rudder-schemas/go/stream"

	"github.com/rudderlabs/rudder-server/admin"
	"github.com/rudderlabs/rudder-server/app"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/enterprise/suppress-user/model"
	gwstats "github.com/rudderlabs/rudder-server/gateway/internal/stats"
	"github.com/rudderlabs/rudder-server/gateway/response"
	"github.com/rudderlabs/rudder-server/gateway/throttler"
	gwtypes "github.com/rudderlabs/rudder-server/gateway/types"
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
		SourceDefName:  SourceIDEnabled,
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

type gatewayFixture struct {
	conf *config.Config

	mockCtrl           *gomock.Controller
	mockJobsDB         *mocksJobsDB.MockJobsDB
	mockBackendConfig  *mocksBackendConfig.MockBackendConfig
	mockRateLimiter    *mockGateway.MockThrottler
	mockApp            *mocksApp.MockApp
	mockWebhook        *mockGateway.MockWebhookRequestHandler
	mockVersionHandler func(w http.ResponseWriter, r *http.Request)

	// Enterprise mocks
	mockSuppressUser        *mocksTypes.MockUserSuppression
	mockSuppressUserFeature *mocksApp.MockSuppressUserFeature
}

// newGatewayFixture initialises mocks and common expectations.
func newGatewayFixture(t *testing.T) *gatewayFixture {
	t.Helper()
	f := &gatewayFixture{}
	f.conf = config.New()
	f.conf.Set("Gateway.enableRateLimit", false)
	f.mockCtrl = gomock.NewController(t)
	f.mockJobsDB = mocksJobsDB.NewMockJobsDB(f.mockCtrl)
	f.mockBackendConfig = mocksBackendConfig.NewMockBackendConfig(f.mockCtrl)
	f.mockApp = mocksApp.NewMockApp(f.mockCtrl)
	f.mockRateLimiter = mockGateway.NewMockThrottler(f.mockCtrl)
	f.mockWebhook = mockGateway.NewMockWebhookRequestHandler(f.mockCtrl)
	f.mockWebhook.EXPECT().Shutdown().AnyTimes()
	f.mockBackendConfig.EXPECT().Subscribe(gomock.Any(), backendconfig.TopicProcessConfig).
		DoAndReturn(func(ctx context.Context, topic backendconfig.Topic) pubsub.DataChannel {
			ch := make(chan pubsub.DataEvent, 1)
			ch <- pubsub.DataEvent{Data: map[string]backendconfig.ConfigT{WorkspaceID: sampleBackendConfig}, Topic: string(topic)}
			// on Subscribe, emulate a backend configuration event
			go func() {
				<-ctx.Done()
				close(ch)
			}()
			return ch
		}).AnyTimes()
	f.mockVersionHandler = func(w http.ResponseWriter, r *http.Request) {}
	return f
}

func (f *gatewayFixture) initializeAppFeatures() {
	f.mockApp.EXPECT().Features().Return(&app.Features{}).AnyTimes()
}

// withSuppressUser enables the suppress user feature, suppressing SuppressedUserID for SourceIDEnabled.
func (f *gatewayFixture) withSuppressUser() {
	f.mockSuppressUser = mocksTypes.NewMockUserSuppression(f.mockCtrl)
	f.mockSuppressUserFeature = mocksApp.NewMockSuppressUserFeature(f.mockCtrl)
	f.mockApp.EXPECT().Features().Return(&app.Features{SuppressUser: f.mockSuppressUserFeature}).AnyTimes()

	f.mockSuppressUserFeature.EXPECT().Setup(gomock.Any(), gomock.Any()).AnyTimes().Return(f.mockSuppressUser, nil)
	f.mockSuppressUser.EXPECT().GetSuppressedUser(WorkspaceID, NormalUserID, SourceIDEnabled).Return(nil).AnyTimes()
	f.mockSuppressUser.EXPECT().GetSuppressedUser(WorkspaceID, SuppressedUserID, SourceIDEnabled).Return(&model.Metadata{
		CreatedAt: time.Now(),
	}).AnyTimes()

	f.conf.Set("Gateway.enableSuppressUserFeature", true)
}

// waitOnCleanup waits during teardown for ch to be closed by a mocked call made from a gateway goroutine.
func waitOnCleanup(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	t.Cleanup(func() {
		select {
		case <-ch:
		case <-time.After(testTimeout):
			t.Fatalf("timed out waiting for %s", what)
		}
	})
}

// setupGateway sets up a gateway using the fixture's mocks and shuts it down on cleanup.
func (f *gatewayFixture) setupGateway(t *testing.T, stat stats.Stats, rateLimiter throttler.Throttler, srcDebugger sourcedebugger.SourceDebugger, opts ...OptFunc) *Handle {
	t.Helper()
	gw := &Handle{}
	err := gw.Setup(t.Context(), f.conf, logger.NOP, stat, f.mockApp, f.mockBackendConfig, f.mockJobsDB, rateLimiter, f.mockVersionHandler, rsources.NewNoOpService(), transformer.NewNoOpService(), srcDebugger, nil, opts...)
	require.NoError(t, err)
	waitForBackendConfigInit(t, gw)
	t.Cleanup(func() {
		require.NoError(t, gw.Shutdown())
	})
	return gw
}

func initGW() {
	config.Reset()
	admin.Init()
	logger.Reset()
	misc.Init()
}

func TestReconstructingJSONForServerSideSDK(t *testing.T) {
	t.Run("newDSIdx tests", func(t *testing.T) {
		testCases := []struct {
			name     string
			inputKey string
			value    string
		}{
			{name: "Expected JSON for Key 1 Test 1 : ", inputKey: ":anon_id_1", value: `{"batch":[{"anonymousId":"anon_id_1","event":"event_1_1"},{"anonymousId":"anon_id_1","event":"event_1_2"},{"anonymousId":"anon_id_1","event":"event_1_3"}]}`},
			{name: "Expected JSON for Key 2 Test 1 : ", inputKey: ":anon_id_2", value: `{"batch":[{"anonymousId":"anon_id_2","event":"event_2_1"},{"anonymousId":"anon_id_2","event":"event_2_2"}]}`},
			{name: "Expected JSON for Key 3 Test 1 : ", inputKey: ":anon_id_3", value: `{"batch":[{"anonymousId":"anon_id_3","event":"event_3_1"}]}`},
		}
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				testValidBody := `{"batch":[
		                {"anonymousId":"anon_id_1","event":"event_1_1"},
		                {"anonymousId":"anon_id_2","event":"event_2_1"},
		                {"anonymousId":"anon_id_3","event":"event_3_1"},
		                {"anonymousId":"anon_id_1","event":"event_1_2"},
		                {"anonymousId":"anon_id_2","event":"event_2_2"},
		                {"anonymousId":"anon_id_1","event":"event_1_3"}
		            ]}`
				response, payloadError := getUsersPayload([]byte(testValidBody))
				key, err := kituuid.GetMD5UUID(tc.inputKey)
				require.Equal(t, tc.value, string(response[key.String()]))
				require.NoError(t, err)
				require.NoError(t, payloadError)
			})
		}
	})
}

func TestGatewayEnterprise(t *testing.T) {
	initGW()

	t.Run("Suppress users", func(t *testing.T) {
		type suppressUsersFixture struct {
			*gatewayFixture
			gateway    *Handle
			statsStore *memstats.Store
		}
		newSuppressUsersFixture := func(t *testing.T) *suppressUsersFixture {
			t.Helper()
			f := &suppressUsersFixture{gatewayFixture: newGatewayFixture(t)}
			f.withSuppressUser()
			var err error
			f.statsStore, err = memstats.New()
			require.NoError(t, err)
			f.gateway = f.setupGateway(t, f.statsStore, nil, sourcedebugger.NewNoOpService())
			return f
		}

		t.Run("should not accept events from suppress users", func(t *testing.T) {
			f := newSuppressUsersFixture(t)
			suppressedUserEventData := fmt.Sprintf(`{"batch":[{"userId":%q}]}`, SuppressedUserID)
			// Why GET
			expectHandlerResponse(t, f.gateway.webBatchHandler(), authorizedRequest(WriteKeyEnabled, bytes.NewBufferString(suppressedUserEventData)), http.StatusOK, "ok", "batch")
			require.Eventually(t,
				func() bool {
					stat := f.statsStore.Get(
						"gateway.write_key_suppressed_requests",
						map[string]string{
							"source":        rCtxEnabled.SourceTag(),
							"sourceID":      rCtxEnabled.SourceID,
							"workspaceId":   rCtxEnabled.WorkspaceID,
							"writeKey":      rCtxEnabled.WriteKey,
							"reqType":       "batch",
							"sourceType":    rCtxEnabled.SourceCategory,
							"sdkVersion":    "",
							"sourceDefName": rCtxEnabled.SourceDefName,
						},
					)
					return stat != nil && stat.LastValue() == float64(1)
				},
				1*time.Second, 10*time.Millisecond,
			)
			// stat should be present for user suppression
			require.Eventually(t,
				func() bool {
					stat := f.statsStore.Get(
						"gateway.user_suppression_age",
						map[string]string{
							"sourceID":    rCtxEnabled.SourceID,
							"workspaceId": rCtxEnabled.WorkspaceID,
						},
					)
					return stat != nil
				},
				1*time.Second, 10*time.Millisecond,
			)
		})

		t.Run("should accept events from normal users", func(t *testing.T) {
			f := newSuppressUsersFixture(t)
			allowedUserEventData := fmt.Sprintf(
				`{"batch":[{"userId":%[1]q,%[2]s}]}`,
				NormalUserID,
				sdkContext,
			)
			f.mockJobsDB.EXPECT().WithStoreSafeTx(
				gomock.Any(),
				gomock.Any(),
			).Times(1).Do(func(ctx context.Context, fn func(jobsdb.StoreSafeTx) error) {
				_ = fn(jobsdb.EmptyStoreSafeTx())
			}).Return(nil)
			stored := make(chan struct{})
			f.mockJobsDB.EXPECT().StoreInTx(
				gomock.Any(),
				gomock.Any(),
				gomock.Any(),
			).DoAndReturn(func(context.Context, jobsdb.StoreSafeTx, []*jobsdb.JobT) error {
				close(stored)
				return nil
			}).Times(1)
			waitOnCleanup(t, stored, "jobsdb store")

			// Why GET
			expectHandlerResponse(
				t,
				f.gateway.webBatchHandler(),
				authorizedRequest(
					WriteKeyEnabled,
					bytes.NewBufferString(allowedUserEventData),
				),
				http.StatusOK,
				"ok",
				"batch",
			)
			require.Eventually(t,
				func() bool {
					stat := f.statsStore.Get(
						"gateway.write_key_successful_requests",
						map[string]string{
							"source":        rCtxEnabled.SourceTag(),
							"sourceID":      rCtxEnabled.SourceID,
							"workspaceId":   rCtxEnabled.WorkspaceID,
							"writeKey":      rCtxEnabled.WriteKey,
							"reqType":       "batch",
							"sourceType":    rCtxEnabled.SourceCategory,
							"sdkVersion":    sdkStatTag,
							"sourceDefName": rCtxEnabled.SourceDefName,
						},
					)
					return stat != nil && stat.LastValue() == float64(1)
				},
				1*time.Second, 10*time.Millisecond,
			)
			// stat should not be present for normal user
			stat := f.statsStore.Get(
				"gateway.user_suppression_age",
				map[string]string{
					"sourceID":    rCtxEnabled.SourceID,
					"workspaceId": rCtxEnabled.WorkspaceID,
				},
			)
			require.Nil(t, stat)
		})

		// t.Run("Gateway.storeUserSuppressedEvents") pins the (user-decided) final contract: the
		// public path never stored dummy jobs for suppressed events and, after the flag was added
		// and then walked back, it still doesn't — regardless of the flag's value. The dummy-job
		// behaviour now lives exclusively on the internal batch path
		// (extractJobsFromInternalBatchPayload, covered in handle_test.go).
		t.Run("Gateway.storeUserSuppressedEvents", func(t *testing.T) {
			t.Run("flag off: a fully suppressed request still returns via errRequestSuppressed and stores nothing", func(t *testing.T) {
				f := newSuppressUsersFixture(t)
				f.mockJobsDB.EXPECT().WithStoreSafeTx(gomock.Any(), gomock.Any()).Times(0)
				f.mockJobsDB.EXPECT().StoreInTx(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

				suppressedUserEventData := fmt.Sprintf(`{"batch":[{"userId":%q}]}`, SuppressedUserID)
				expectHandlerResponse(
					t,
					f.gateway.webBatchHandler(),
					authorizedRequest(WriteKeyEnabled, bytes.NewBufferString(suppressedUserEventData)),
					http.StatusOK, "ok", "batch",
				)
			})

			t.Run("flag on: the public path still drops via errRequestSuppressed and stores nothing", func(t *testing.T) {
				f := newSuppressUsersFixture(t)
				// Guards against the flag ever leaking back into the public path: even with it
				// explicitly enabled, a fully suppressed request must behave exactly as if the
				// flag didn't exist here.
				f.conf.Set("Gateway.storeUserSuppressedEvents", true)

				f.mockJobsDB.EXPECT().WithStoreSafeTx(gomock.Any(), gomock.Any()).Times(0)
				f.mockJobsDB.EXPECT().StoreInTx(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

				suppressedUserEventData := fmt.Sprintf(`{"batch":[{"userId":%q}]}`, SuppressedUserID)
				expectHandlerResponse(
					t,
					f.gateway.webBatchHandler(),
					authorizedRequest(WriteKeyEnabled, bytes.NewBufferString(suppressedUserEventData)),
					http.StatusOK, "ok", "batch",
				)
			})
		})
	})
}

func TestGateway(t *testing.T) {
	initGW()

	t.Run("Initialization", func(t *testing.T) {
		t.Run("should wait for backend config", func(t *testing.T) {
			f := newGatewayFixture(t)
			f.initializeAppFeatures()
			gateway := &Handle{}
			err := gateway.Setup(t.Context(), f.conf, logger.NOP, stats.NOP, f.mockApp, f.mockBackendConfig, f.mockJobsDB, nil, f.mockVersionHandler, rsources.NewNoOpService(), transformer.NewNoOpService(), sourcedebugger.NewNoOpService(), nil, WithInternalEndpointsEnabled(true))
			require.NoError(t, err)
			waitForBackendConfigInit(t, gateway)
			err = gateway.Shutdown()
			require.NoError(t, err)
		})
	})

	t.Run("Test All endpoints", func(t *testing.T) {
		type allEndpointsFixture struct {
			*gatewayFixture
			gateway   *Handle
			serverURL string
		}
		newAllEndpointsFixture := func(t *testing.T) *allEndpointsFixture {
			t.Helper()
			f := &allEndpointsFixture{gatewayFixture: newGatewayFixture(t)}
			f.initializeAppFeatures()
			whServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				_, _ = io.WriteString(w, "ok")
			}))
			t.Cleanup(whServer.Close)
			WHURL := whServer.URL
			parsedURL, err := url.Parse(WHURL)
			require.NoError(t, err)
			whPort := parsedURL.Port()
			t.Setenv("RSERVER_WAREHOUSE_WEB_PORT", whPort)

			serverPort, err := kithelper.GetFreePort()
			require.NoError(t, err)
			f.serverURL = fmt.Sprintf("http://localhost:%d", serverPort)
			t.Setenv("RSERVER_GATEWAY_WEB_PORT", strconv.Itoa(serverPort))

			f.gateway = f.setupGateway(t, stats.NOP, nil, sourcedebugger.NewNoOpService(), WithInternalEndpointsEnabled(true))
			f.gateway.irh = mockRequestHandler{}
			f.gateway.rrh = mockRequestHandler{}
			// gw.Shutdown only stops the webhook it holds, so the one created by Setup is stopped before it gets replaced.
			require.NoError(t, f.gateway.webhook.Shutdown())
			f.gateway.webhook = f.mockWebhook
			return f
		}

		createValidBody := func(customProperty, customValue string) []byte {
			validData := fmt.Sprintf(
				`{"userId":"dummyId","data":{"string":"valid-json","nested":{"child":1}},%s}`,
				sdkContext,
			)
			validDataWithProperty, _ := sjson.SetBytes([]byte(validData), customProperty, customValue)

			return validDataWithProperty
		}
		verifyEndpoint := func(t *testing.T, serverURL string, endpoints []string, method string) {
			t.Helper()
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
				require.NoError(t, err)
				req.Header.Set("Content-Type", "application/json")
				switch ep {
				case "/internal/v1/replay", "/internal/v1/retl":
					req.Header.Set("X-Rudder-Source-Id", ReplaySourceID)
				default:
					req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(WriteKeyEnabled+":")))
				}
				resp, err := client.Do(req)
				require.NoError(t, err)
				_ = resp.Body.Close()
				require.Contains(t, []int{http.StatusOK, http.StatusNoContent}, resp.StatusCode, "endpoint: %s, method: %s", ep, method)

			}
		}
		t.Run("should be able to hit all the handlers", func(t *testing.T) {
			f := newAllEndpointsFixture(t)
			f.mockWebhook.EXPECT().RequestHandler(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				_, _ = io.WriteString(w, "ok")
			})
			f.mockBackendConfig.EXPECT().WaitForConfig(gomock.Any()).AnyTimes()
			wait := make(chan error, 1)
			ctx, cancel := context.WithCancel(t.Context())
			go func() {
				wait <- f.gateway.StartWebHandler(ctx)
			}()
			require.Eventually(t, func() bool {
				resp, err := http.Get(fmt.Sprintf("%s/version", f.serverURL))
				if err != nil {
					return false
				}
				defer func() { _ = resp.Body.Close() }()
				return resp.StatusCode == http.StatusOK
			}, time.Second*10, time.Second)

			getEndpoint, postEndpoints, deleteEndpoints := endpointsToVerify()
			verifyEndpoint(t, f.serverURL, getEndpoint, http.MethodGet)
			verifyEndpoint(t, f.serverURL, postEndpoints, http.MethodPost)
			verifyEndpoint(t, f.serverURL, deleteEndpoints, http.MethodDelete)
			cancel()
			require.NoError(t, <-wait)
		})
	})

	t.Run("Valid requests", func(t *testing.T) {
		type validRequestsFixture struct {
			*gatewayFixture
			gateway    *Handle
			statsStore *memstats.Store
		}
		newValidRequestsFixture := func(t *testing.T) *validRequestsFixture {
			t.Helper()
			f := &validRequestsFixture{gatewayFixture: newGatewayFixture(t)}
			f.initializeAppFeatures()
			var err error
			f.statsStore, err = memstats.New()
			require.NoError(t, err)

			f.gateway = f.setupGateway(t, f.statsStore, nil, sourcedebugger.NewNoOpService(), WithInternalEndpointsEnabled(true))
			return f
		}

		// assertJobMetadata runs inside jobsdb mock calls, on a gateway goroutine
		assertJobMetadata := func(t *testing.T, job *jobsdb.JobT) {
			assert.True(t, misc.IsValidUUID(job.UUID.String()))

			var paramsMap, expectedParamsMap map[string]any
			_ = jsonrs.Unmarshal(job.Parameters, &paramsMap)
			expectedStr := fmt.Appendf(nil,
				`{"source_id": "%v", "source_job_run_id": "", "source_task_run_id": "","source_category": "webhook", "traceparent": ""}`,
				SourceIDEnabled,
			)
			_ = jsonrs.Unmarshal(expectedStr, &expectedParamsMap)
			equals := reflect.DeepEqual(paramsMap, expectedParamsMap)
			assert.True(t, equals)

			assert.Equal(t, customVal, job.CustomVal)

			responseData := []byte(job.EventPayload)
			receivedAt := gjson.GetBytes(responseData, "receivedAt")
			writeKey := gjson.GetBytes(responseData, "writeKey")
			requestIP := gjson.GetBytes(responseData, "requestIP")
			batch := gjson.GetBytes(responseData, "batch")

			receivedAtTime, err := time.Parse(misc.RFC3339Milli, receivedAt.String())
			assert.NoError(t, err)
			assert.WithinDuration(t, time.Now(), receivedAtTime, 100*time.Millisecond)
			assert.Equal(t, WriteKeyEnabled, writeKey.String())
			assert.Equal(t, TestRemoteAddress, requestIP.String())
			assert.Len(t, batch.Array(), 1) // each batch split into multiple batches of 1 event
		}

		createValidBody := func(customProperty, customValue string) []byte {
			validData := fmt.Sprintf(
				`{"userId":"dummyId","data":{"string":"valid-json","nested":{"child":1}},%s}`,
				sdkContext,
			)
			validDataWithProperty, _ := sjson.SetBytes([]byte(validData), customProperty, customValue)

			return validDataWithProperty
		}

		// assertJobBatchItem runs inside jobsdb mock calls, on a gateway goroutine
		assertJobBatchItem := func(t *testing.T, payload gjson.Result) {
			messageID := payload.Get("messageId")
			messageType := payload.Get("type")

			// Assertions regarding batch message
			assert.True(t, messageID.Exists())
			assert.True(t, misc.IsValidUUID(messageID.String()))
			assert.True(t, messageType.Exists())
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
		t.Run("should accept valid requests on a single endpoint (except batch), and store to jobsdb", func(t *testing.T) {
			f := newValidRequestsFixture(t)
			for handlerType, handler := range allHandlers(f.gateway) {
				if handlerType != "batch" && handlerType != "import" {

					validBody := createValidBody("custom-property", "custom-value")

					f.mockJobsDB.EXPECT().WithStoreSafeTx(
						gomock.Any(),
						gomock.Any()).Times(1).Do(func(
						ctx context.Context,
						fn func(jobsdb.StoreSafeTx) error,
					) {
						_ = fn(jobsdb.EmptyStoreSafeTx())
					}).Return(nil)
					f.mockJobsDB.
						EXPECT().StoreInTx(
						gomock.Any(),
						gomock.Any(),
						gomock.Any(),
					).
						DoAndReturn(
							func(
								ctx context.Context,
								tx jobsdb.StoreSafeTx,
								jobs []*jobsdb.JobT,
							) error {
								for _, job := range jobs {
									assertJobMetadata(t, job)

									responseData := []byte(job.EventPayload)
									payload := gjson.GetBytes(responseData, "batch.0")

									assertJobBatchItem(t, payload)

									messageType := payload.Get("type")
									assert.Equal(t, handlerType, messageType.String())
									assert.JSONEq(t, string(validBody), stripJobPayload(payload))
								}

								return nil
							}).
						Times(1)

					expectHandlerResponse(
						t,
						handler,
						authorizedRequest(
							WriteKeyEnabled,
							bytes.NewBuffer(validBody)),
						http.StatusOK,
						"ok",
						handlerType,
					)
					require.Eventually(t,
						func() bool {
							stat := f.statsStore.Get(
								"gateway.write_key_successful_requests",
								map[string]string{
									"source":        rCtxEnabled.SourceTag(),
									"sourceID":      rCtxEnabled.SourceID,
									"workspaceId":   rCtxEnabled.WorkspaceID,
									"writeKey":      rCtxEnabled.WriteKey,
									"reqType":       handlerType,
									"sourceType":    rCtxEnabled.SourceCategory,
									"sdkVersion":    sdkStatTag,
									"sourceDefName": rCtxEnabled.SourceDefName,
								},
							)
							return stat != nil && stat.LastValue() == float64(1)
						},
						1*time.Second, 10*time.Millisecond,
					)
				}
			}
		})

		t.Run("should accept valid replay request and store to jobsdb", func(t *testing.T) {
			f := newValidRequestsFixture(t)
			handler := f.gateway.webReplayHandler()

			validBody := fmt.Appendf(nil, `{"batch":[%s]}`, string(createValidBody("custom-property", "custom-value")))

			f.mockJobsDB.EXPECT().WithStoreSafeTx(
				gomock.Any(),
				gomock.Any()).Times(1).Do(func(
				ctx context.Context,
				fn func(jobsdb.StoreSafeTx) error,
			) {
				_ = fn(jobsdb.EmptyStoreSafeTx())
			}).Return(nil)
			f.mockJobsDB.
				EXPECT().StoreInTx(
				gomock.Any(),
				gomock.Any(),
				gomock.Any(),
			).
				Return(nil).
				Times(1)

			expectHandlerResponse(
				t,
				handler,
				authorizedReplayRequest(
					ReplaySourceID,
					bytes.NewBuffer(validBody)),
				http.StatusOK,
				"ok",
				"replay",
			)

			require.Eventually(t,
				func() bool {
					stat := f.statsStore.Get(
						"gateway.write_key_successful_requests",
						map[string]string{
							"source":        "_source",
							"sourceID":      ReplaySourceID,
							"workspaceId":   WorkspaceID,
							"writeKey":      ReplayWriteKey,
							"reqType":       "replay",
							"sourceType":    "eventStream",
							"sdkVersion":    sdkStatTag,
							"sourceDefName": SourceIDEnabled,
						},
					)
					return stat != nil && stat.LastValue() == float64(1)
				},
				1*time.Second, 10*time.Millisecond,
			)
		})

		t.Run("should accept valid (with and without userId and anonymousId) retl request and store to jobsdb", func(t *testing.T) {
			f := newValidRequestsFixture(t)
			handler := f.gateway.webRetlHandler()

			payloads := [2][]byte{
				[]byte(`{"batch": [{"data": "valid-json", "type": "record", "userId": "dummyId"}]}`),
				[]byte(`{"batch": [{"data": "valid-json", "type": "record"}]}`),
			}
			f.mockJobsDB.EXPECT().WithStoreSafeTx(
				gomock.Any(),
				gomock.Any()).AnyTimes().Do(func(
				ctx context.Context,
				fn func(jobsdb.StoreSafeTx) error,
			) {
				_ = fn(jobsdb.EmptyStoreSafeTx())
			}).Return(nil)
			f.mockJobsDB.
				EXPECT().StoreInTx(
				gomock.Any(),
				gomock.Any(),
				gomock.Any(),
			).Return(nil).AnyTimes()

			// With userId
			expectHandlerResponse(
				t,
				handler,
				authorizedRETLRequest(
					RETLSourceID,
					bytes.NewBuffer(payloads[0])),
				http.StatusOK,
				"ok",
				"retl",
			)

			require.Eventually(t,
				func() bool {
					stat := f.statsStore.Get(
						"gateway.write_key_successful_events",
						map[string]string{
							"source":        "_source",
							"sourceID":      RETLSourceID,
							"workspaceId":   WorkspaceID,
							"writeKey":      RETLWriteKey,
							"reqType":       "retl",
							"sourceType":    "retl",
							"sdkVersion":    "",
							"sourceDefName": SourceIDEnabled,
						},
					)
					return stat != nil && stat.LastValue() == float64(1)
				},
				1*time.Second, 10*time.Millisecond,
			)

			// Without userId
			expectHandlerResponse(
				t,
				handler,
				authorizedRETLRequest(
					RETLSourceID,
					bytes.NewBuffer(payloads[1])),
				http.StatusOK,
				"ok",
				"retl",
			)

			require.Eventually(t,
				func() bool {
					stat := f.statsStore.Get(
						"gateway.write_key_successful_events",
						map[string]string{
							"source":        "_source",
							"sourceID":      RETLSourceID,
							"workspaceId":   WorkspaceID,
							"writeKey":      RETLWriteKey,
							"reqType":       "retl",
							"sourceType":    "retl",
							"sdkVersion":    "",
							"sourceDefName": SourceIDEnabled,
						},
					)
					return stat != nil && stat.LastValue() == float64(2)
				},
				1*time.Second, 10*time.Millisecond,
			)
		})

		t.Run("should accept requests with both userId and anonymousId absent in case of extract events", func(t *testing.T) {
			f := newValidRequestsFixture(t)
			extractHandlers := map[string]http.HandlerFunc{
				"batch":   f.gateway.webBatchHandler(),
				"import":  f.gateway.webImportHandler(),
				"extract": f.gateway.webExtractHandler(),
			}
			f.mockJobsDB.EXPECT().WithStoreSafeTx(gomock.Any(), gomock.Any()).AnyTimes().Do(func(ctx context.Context, fn func(jobsdb.StoreSafeTx) error) {
				_ = fn(jobsdb.EmptyStoreSafeTx())
			}).Return(nil)
			f.mockJobsDB.EXPECT().StoreInTx(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(3)

			for handlerType, handler := range extractHandlers {
				var body string
				switch handlerType {
				case "extract":
					body = `{"data": "valid-json", "type": "extract"}`
				default:
					body = `{"batch": [{"data": "valid-json", "type": "extract"}]}`
				}
				expectHandlerResponse(
					t,
					handler,
					authorizedRequest(
						WriteKeyEnabled,
						bytes.NewBufferString(body),
					),
					http.StatusOK,
					"ok",
					handlerType,
				)
			}
		})
	})

	t.Run("Bots", func(t *testing.T) {
		t.Run("should send bots information", func(t *testing.T) {
			f := newGatewayFixture(t)
			f.initializeAppFeatures()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			gateway := f.setupGateway(t, statsStore, f.mockRateLimiter, sourcedebugger.NewNoOpService())

			f.mockJobsDB.EXPECT().WithStoreSafeTx(gomock.Any(), gomock.Any()).Times(1).Do(func(ctx context.Context, fn func(jobsdb.StoreSafeTx) error) {
				_ = fn(jobsdb.EmptyStoreSafeTx())
			}).Return(nil)

			f.mockJobsDB.EXPECT().StoreInTx(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(1)

			expectHandlerResponse(
				t,
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
				"ok",
				"batch",
			)

			tags := stats.Tags{
				"source":        rCtxEnabled.SourceTag(),
				"sourceID":      rCtxEnabled.SourceID,
				"workspaceId":   rCtxEnabled.WorkspaceID,
				"writeKey":      rCtxEnabled.WriteKey,
				"reqType":       "batch",
				"sourceType":    rCtxEnabled.SourceCategory,
				"sdkVersion":    sdkStatTag,
				"sourceDefName": rCtxEnabled.SourceDefName,
			}
			t.Logf("tags: %v", tags)

			require.Eventually(t,
				func() bool {
					stat := statsStore.Get(
						"gateway.write_key_events",
						tags,
					)
					return stat != nil && stat.LastValue() == float64(2)
				},
				time.Second, 10*time.Millisecond,
			)

			require.Eventually(t,
				func() bool {
					stat := statsStore.Get(
						"gateway.write_key_bot_events",
						tags,
					)
					return stat != nil && stat.LastValue() == float64(1)
				},
				time.Second, 10*time.Millisecond,
			)
		})
	})

	t.Run("Rate limits", func(t *testing.T) {
		type rateLimitsFixture struct {
			*gatewayFixture
			gateway    *Handle
			statsStore *memstats.Store
		}
		newRateLimitsFixture := func(t *testing.T) *rateLimitsFixture {
			t.Helper()
			f := &rateLimitsFixture{gatewayFixture: newGatewayFixture(t)}
			f.initializeAppFeatures()
			var err error
			f.statsStore, err = memstats.New()
			require.NoError(t, err)

			f.conf.Set("Gateway.enableRateLimit", true)
			f.gateway = f.setupGateway(t, f.statsStore, f.mockRateLimiter, sourcedebugger.NewNoOpService())
			return f
		}

		t.Run("should store messages successfully if rate limit is not reached for workspace", func(t *testing.T) {
			f := newRateLimitsFixture(t)
			f.mockRateLimiter.EXPECT().CheckLimitReached(gomock.Any(), gomock.Any(), gomock.Any()).Return(false, nil).Times(1)
			f.mockJobsDB.EXPECT().WithStoreSafeTx(gomock.Any(), gomock.Any()).Times(1).Do(func(ctx context.Context, fn func(jobsdb.StoreSafeTx) error) {
				_ = fn(jobsdb.EmptyStoreSafeTx())
			}).Return(nil)
			stored := make(chan struct{})
			f.mockJobsDB.EXPECT().StoreInTx(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(context.Context, jobsdb.StoreSafeTx, []*jobsdb.JobT) error {
				close(stored)
				return nil
			}).Times(1)
			waitOnCleanup(t, stored, "jobsdb store")

			expectHandlerResponse(
				t,
				f.gateway.webAliasHandler(),
				authorizedRequest(
					WriteKeyEnabled,
					bytes.NewBufferString(
						fmt.Sprintf(`{"userId":"dummyId",%s}`, sdkContext),
					),
				),
				http.StatusOK,
				"ok",
				"alias",
			)
			require.Eventually(t,
				func() bool {
					stat := f.statsStore.Get(
						"gateway.write_key_successful_requests",
						map[string]string{
							"source":        rCtxEnabled.SourceTag(),
							"sourceID":      rCtxEnabled.SourceID,
							"workspaceId":   rCtxEnabled.WorkspaceID,
							"writeKey":      rCtxEnabled.WriteKey,
							"reqType":       "alias",
							"sourceType":    rCtxEnabled.SourceCategory,
							"sdkVersion":    sdkStatTag,
							"sourceDefName": rCtxEnabled.SourceDefName,
						},
					)
					return stat != nil && stat.LastValue() == float64(1)
				},
				time.Second, 10*time.Millisecond,
			)
		})

		t.Run("should reject messages if rate limit is reached for workspace", func(t *testing.T) {
			f := newRateLimitsFixture(t)
			f.conf.Set("Gateway.allowReqsWithoutUserIDAndAnonymousID", true)
			f.mockRateLimiter.EXPECT().CheckLimitReached(gomock.Any(), gomock.Any(), gomock.Any()).Return(true, nil).Times(1)
			expectHandlerResponse(
				t,
				f.gateway.webAliasHandler(),
				authorizedRequest(WriteKeyEnabled, bytes.NewBufferString(`{"data": "valid-json"}`)),
				http.StatusTooManyRequests,
				response.TooManyRequests+"\n",
				"alias",
			)
			require.Eventually(t,
				func() bool {
					stat := f.statsStore.Get(
						"gateway.write_key_dropped_requests",
						map[string]string{
							"source":      rCtxEnabled.SourceTag(),
							"sourceID":    rCtxEnabled.SourceID,
							"workspaceId": rCtxEnabled.WorkspaceID,
							"writeKey":    rCtxEnabled.WriteKey,
							"reqType":     "alias",
							// a drop now says why: this is what lets a consumer tell a rate limit from any other
							// reason a request was turned away
							"reason":        gwtypes.ReasonRateLimit.Value(),
							"sourceType":    rCtxEnabled.SourceCategory,
							"sdkVersion":    "",
							"sourceDefName": rCtxEnabled.SourceDefName,
						},
					)
					return stat != nil && stat.LastValue() == float64(1)
				},
				1*time.Second, 10*time.Millisecond,
			)
		})
	})

	t.Run("Invalid requests", func(t *testing.T) {
		type invalidRequestsFixture struct {
			*gatewayFixture
			gateway    *Handle
			statsStore *memstats.Store
		}
		newInvalidRequestsFixture := func(t *testing.T) *invalidRequestsFixture {
			t.Helper()
			f := &invalidRequestsFixture{gatewayFixture: newGatewayFixture(t)}
			f.initializeAppFeatures()
			var err error
			f.statsStore, err = memstats.New()
			require.NoError(t, err)

			f.gateway = f.setupGateway(t, f.statsStore, nil, sourcedebugger.NewNoOpService())
			return f
		}

		createJSONBody := func(customProperty, customValue string) []byte {
			validData := fmt.Sprintf(
				`{"userId":"dummyId","data":{"string":"valid-json","nested":{"child":1}},%s}`,
				sdkContext,
			)
			validDataWithProperty, _ := sjson.SetBytes([]byte(validData), customProperty, customValue)

			return validDataWithProperty
		}

		// common tests for all web handlers
		t.Run("should reject requests without Authorization header", func(t *testing.T) {
			f := newInvalidRequestsFixture(t)
			for reqType, handler := range allHandlers(f.gateway) {
				expectHandlerResponse(
					t,
					handler,
					unauthorizedRequest(nil),
					http.StatusUnauthorized,
					response.NoWriteKeyInBasicAuth+"\n",
					reqType,
				)
				require.Eventually(t,
					func() bool {
						stat := f.statsStore.Get(
							"gateway.write_key_failed_requests",
							map[string]string{
								"source":        "noWriteKey",
								"sourceID":      "noWriteKey",
								"workspaceId":   "",
								"writeKey":      "noWriteKey",
								"reqType":       reqType,
								"reason":        "failed to read writekey from header",
								"sourceType":    "",
								"sdkVersion":    "",
								"sourceDefName": "",
							},
						)
						return stat != nil && stat.LastValue() == float64(1)
					},
					time.Second, 10*time.Millisecond,
				)
			}
			f.gateway.stats = stats.Default
		})

		t.Run("should reject requests without username in Authorization header", func(t *testing.T) {
			f := newInvalidRequestsFixture(t)
			for reqType, handler := range allHandlers(f.gateway) {
				expectHandlerResponse(
					t,
					handler,
					authorizedRequest(WriteKeyEmpty, nil),
					http.StatusUnauthorized,
					response.NoWriteKeyInBasicAuth+"\n",
					reqType,
				)
				require.Eventually(t,
					func() bool {
						stat := f.statsStore.Get(
							"gateway.write_key_failed_requests",
							map[string]string{
								"source":        "noWriteKey",
								"sourceID":      "noWriteKey",
								"workspaceId":   "",
								"writeKey":      "noWriteKey",
								"reqType":       reqType,
								"reason":        "failed to read writekey from header",
								"sourceType":    "",
								"sdkVersion":    "",
								"sourceDefName": "",
							},
						)
						return stat != nil && stat.LastValue() == float64(1)
					},
					time.Second, 10*time.Millisecond,
				)
			}
		})

		t.Run("should reject requests without valid rudder event in request body", func(t *testing.T) {
			f := newInvalidRequestsFixture(t)
			f.conf.Set("Gateway.allowReqsWithoutUserIDAndAnonymousID", true)
			require.Eventually(t, func() bool { return f.gateway.conf.allowReqsWithoutUserIDAndAnonymousID.Load() }, time.Second, 10*time.Millisecond)
			batchCount := 0
			for handlerType, handler := range allHandlers(f.gateway) {
				reqType := handlerType
				notRudderEvent := `[{"data": "valid-json","foo":"bar"}]`
				if handlerType == "batch" || handlerType == "import" {
					notRudderEvent = `{"batch": [[{"data": "valid-json","foo":"bar"}]]}`
					reqType = "batch"
					batchCount++
				}
				expectHandlerResponse(
					t,
					handler,
					authorizedRequest(
						WriteKeyEnabled,
						bytes.NewBufferString(notRudderEvent),
					),
					http.StatusBadRequest,
					response.NotRudderEvent+"\n",
					handlerType,
				)
				require.Eventually(t,
					func() bool {
						stat := f.statsStore.Get(
							"gateway.write_key_failed_requests",
							map[string]string{
								"source":        rCtxEnabled.SourceTag(),
								"sourceID":      rCtxEnabled.SourceID,
								"workspaceId":   rCtxEnabled.WorkspaceID,
								"writeKey":      rCtxEnabled.WriteKey,
								"reqType":       reqType,
								"reason":        response.NotRudderEvent,
								"sourceType":    rCtxEnabled.SourceCategory,
								"sdkVersion":    "",
								"sourceDefName": rCtxEnabled.SourceDefName,
							},
						)
						// multiple `handlerType` are mapped to batch `reqType`
						count := 1
						if reqType == "batch" {
							count = batchCount
						}

						return stat != nil && stat.LastValue() == float64(count)
					},
					time.Second, 10*time.Millisecond,
				)
			}
		})

		t.Run("should reject requests with both userId and anonymousId not present", func(t *testing.T) {
			f := newInvalidRequestsFixture(t)
			batchCount := 0
			for handlerType, handler := range allHandlers(f.gateway) {
				reqType := handlerType
				validBody := `{"data": "valid-json"}`
				if handlerType == "batch" || handlerType == "import" {
					validBody = `{"batch": [{"data": "valid-json"}]}`
					reqType = "batch"
					batchCount++
				}
				if handlerType != "extract" {
					expectHandlerResponse(
						t,
						handler,
						authorizedRequest(
							WriteKeyEnabled,
							bytes.NewBufferString(validBody),
						),
						http.StatusBadRequest,
						response.NonIdentifiableRequest+"\n",
						handlerType,
					)
					require.Eventually(t,
						func() bool {
							stat := f.statsStore.Get(
								"gateway.write_key_failed_requests",
								map[string]string{
									"source":        rCtxEnabled.SourceTag(),
									"sourceID":      rCtxEnabled.SourceID,
									"workspaceId":   rCtxEnabled.WorkspaceID,
									"writeKey":      rCtxEnabled.WriteKey,
									"reqType":       reqType,
									"reason":        response.NonIdentifiableRequest,
									"sourceType":    rCtxEnabled.SourceCategory,
									"sdkVersion":    "",
									"sourceDefName": rCtxEnabled.SourceDefName,
								},
							)
							// multiple `handlerType` are mapped to batch `reqType`
							count := 1
							if reqType == "batch" {
								count = batchCount
							}
							return stat != nil && stat.LastValue() == float64(count)
						},
						time.Second, 10*time.Millisecond,
					)
				}
			}
		})

		t.Run("should reject requests without request body", func(t *testing.T) {
			f := newInvalidRequestsFixture(t)
			for handlerType, handler := range allHandlers(f.gateway) {
				expectHandlerResponse(t, handler, authorizedRequest(WriteKeyEnabled, nil), http.StatusBadRequest, response.RequestBodyNil+"\n", handlerType)
			}
		})

		t.Run("should reject requests without valid json in request body", func(t *testing.T) {
			f := newInvalidRequestsFixture(t)
			for handlerType, handler := range allHandlers(f.gateway) {
				invalidBody := "not-a-valid-json"
				expectHandlerResponse(t, handler, authorizedRequest(WriteKeyEnabled, bytes.NewBufferString(invalidBody)), http.StatusBadRequest, response.InvalidJSON+"\n", handlerType)
			}
		})

		t.Run("should reject requests with request bodies larger than configured limit", func(t *testing.T) {
			f := newInvalidRequestsFixture(t)
			for handlerType, handler := range allHandlers(f.gateway) {
				data := make([]byte, f.gateway.conf.maxReqSize.Load())
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
					expectHandlerResponse(t, handler, authorizedRequest(WriteKeyEnabled, bytes.NewBufferString(body)), http.StatusRequestEntityTooLarge, response.RequestBodyTooLarge+"\n", handlerType)
				}
			}
		})

		t.Run("should reject requests with invalid write keys", func(t *testing.T) {
			f := newInvalidRequestsFixture(t)
			for handlerType, handler := range allHandlers(f.gateway) {
				validBody := `{"data":"valid-json"}`
				if handlerType == "batch" || handlerType == "import" {
					validBody = `{"batch":[{"data":"valid-json"}]}`
				}
				expectHandlerResponse(t, handler, authorizedRequest(WriteKeyInvalid, bytes.NewBufferString(validBody)), http.StatusUnauthorized, response.InvalidWriteKey+"\n", handlerType)
			}
		})

		t.Run("should reject requests with disabled write keys (source)", func(t *testing.T) {
			f := newInvalidRequestsFixture(t)
			for handlerType, handler := range allHandlers(f.gateway) {
				validBody := `{"data":"valid-json"}`
				if handlerType == "batch" || handlerType == "import" {
					validBody = `{"batch":[{"data":"valid-json"}]}`
				}
				expectHandlerResponse(t, handler, authorizedRequest(WriteKeyDisabled, bytes.NewBufferString(validBody)), http.StatusNotFound, response.SourceDisabled+"\n", handlerType)
			}
		})

		t.Run("should reject requests with 500 if jobsdb store returns an error", func(t *testing.T) {
			f := newInvalidRequestsFixture(t)
			for handlerType, handler := range allHandlers(f.gateway) {
				if handlerType != "batch" && handlerType != "import" {
					validBody := createJSONBody("custom-property", "custom-value")

					f.mockJobsDB.EXPECT().WithStoreSafeTx(gomock.Any(), gomock.Any()).Times(1).DoAndReturn(func(ctx context.Context, fn func(jobsdb.StoreSafeTx) error) error {
						return fn(jobsdb.EmptyStoreSafeTx())
					})
					f.mockJobsDB.
						EXPECT().StoreInTx(gomock.Any(), gomock.Any(), gomock.Any()).
						Return(errors.New("tx error")).
						Times(1)

					expectHandlerResponse(
						t,
						handler,
						authorizedRequest(
							WriteKeyEnabled,
							bytes.NewBuffer(validBody),
						),
						http.StatusInternalServerError,
						"tx error"+"\n",
						handlerType,
					)
					require.Eventually(t,
						func() bool {
							stat := f.statsStore.Get(
								"gateway.write_key_failed_requests",
								map[string]string{
									"source":        rCtxEnabled.SourceTag(),
									"sourceID":      rCtxEnabled.SourceID,
									"workspaceId":   rCtxEnabled.WorkspaceID,
									"writeKey":      rCtxEnabled.WriteKey,
									"reqType":       handlerType,
									"reason":        "storeFailed",
									"sourceType":    rCtxEnabled.SourceCategory,
									"sdkVersion":    sdkStatTag,
									"sourceDefName": rCtxEnabled.SourceDefName,
								},
							)
							return stat != nil && stat.LastValue() == float64(1)
						},
						time.Second, 10*time.Millisecond,
					)
				}
			}
		})

		t.Run("should return empty batch payload error", func(t *testing.T) {
			f := newInvalidRequestsFixture(t)
			expectHandlerResponse(
				t,
				f.gateway.webBatchHandler(),
				authorizedRequest(
					WriteKeyEnabled,
					bytes.NewBufferString(`{"batch":[]}`),
				),
				http.StatusBadRequest,
				"empty batch payload\n",
				"batch",
			)
		})
	})

	t.Run("Robots", func(t *testing.T) {
		t.Run("should return a robots.txt", func(t *testing.T) {
			f := newGatewayFixture(t)
			f.initializeAppFeatures()
			gateway := f.setupGateway(t, stats.NOP, nil, sourcedebugger.NewNoOpService())

			req := httptest.NewRequest("GET", "/robots.txt", nil)
			expectHandlerResponse(t, gateway.robotsHandler, req, http.StatusOK, "User-agent: * \nDisallow: / \n", "robots")
		})
	})

	t.Run("jobDataFromRequest", func(t *testing.T) {
		var (
			arctx = &gwtypes.AuthRequestContext{
				WriteKey: "someWriteKey",
			}
			userIDHeader = "userIDHeader"
		)
		type jobDataFromRequestFixture struct {
			*gatewayFixture
			gateway *Handle
		}
		newJobDataFromRequestFixture := func(t *testing.T) *jobDataFromRequestFixture {
			t.Helper()
			f := &jobDataFromRequestFixture{gatewayFixture: newGatewayFixture(t)}
			f.initializeAppFeatures()
			f.gateway = f.setupGateway(t, stats.NOP, nil, sourcedebugger.NewNoOpService())
			return f
		}

		t.Run("returns invalid JSON in case of invalid JSON payload", func(t *testing.T) {
			f := newJobDataFromRequestFixture(t)
			req := &webRequestT{
				reqType:        "batch",
				authContext:    arctx,
				done:           make(chan<- string),
				userIDHeader:   userIDHeader,
				requestPayload: []byte(``),
			}
			jobData, err := f.gateway.getJobDataFromRequest(req)
			require.Equal(t, errors.New(response.InvalidJSON), err)
			require.Nil(t, jobData.jobs)
		})

		t.Run("drops non-identifiable requests if userID and anonID are not present in the request payload", func(t *testing.T) {
			f := newJobDataFromRequestFixture(t)
			req := &webRequestT{
				reqType:        "batch",
				authContext:    rCtxEnabled,
				done:           make(chan<- string),
				userIDHeader:   userIDHeader,
				requestPayload: []byte(`{"batch": [{"type": "track"}]}`),
			}
			jobData, err := f.gateway.getJobDataFromRequest(req)
			require.Equal(t, errors.New(response.NonIdentifiableRequest), err)
			require.Nil(t, jobData.jobs)
		})

		t.Run("accepts events with non-string type anonymousId and/or userId", func(t *testing.T) {
			f := newJobDataFromRequestFixture(t)
			// map type usreId
			payloadMap := map[string]any{
				"batch": []any{
					map[string]any{
						"type":   "track",
						"userId": map[string]any{"id": 456},
					},
				},
			}
			payload, err := jsonrs.Marshal(payloadMap)
			require.NoError(t, err)
			req := &webRequestT{
				reqType:        "batch",
				authContext:    rCtxEnabled,
				done:           make(chan<- string),
				userIDHeader:   userIDHeader,
				requestPayload: payload,
			}
			_, err = f.gateway.getJobDataFromRequest(req)
			require.NoError(t, err)

			// int type anonymousId
			payloadMap = map[string]any{
				"batch": []any{
					map[string]any{
						"type":   "track",
						"userId": 456,
					},
				},
			}
			payload, err = jsonrs.Marshal(payloadMap)
			require.NoError(t, err)
			require.NoError(t, err)
			req = &webRequestT{
				reqType:        "batch",
				authContext:    rCtxEnabled,
				done:           make(chan<- string),
				userIDHeader:   userIDHeader,
				requestPayload: payload,
			}
			_, err = f.gateway.getJobDataFromRequest(req)
			require.NoError(t, err)
		})

		t.Run("sanitizes messageID, trim space and replace with new uuid", func(t *testing.T) {
			f := newJobDataFromRequestFixture(t)
			// passing a messageID full of invisible characters
			payloadMap := map[string]any{
				"batch": []any{
					map[string]any{
						"type":      "track",
						"userId":    map[string]any{"id": 456},
						"messageId": " \u0000\u00A0\t\n\r\u034F ",
					},
				},
			}
			payload, err := jsonrs.Marshal(payloadMap)
			require.NoError(t, err)
			req := &webRequestT{
				reqType:        "batch",
				authContext:    rCtxEnabled,
				done:           make(chan<- string),
				userIDHeader:   userIDHeader,
				requestPayload: payload,
			}
			jobForm, err := f.gateway.getJobDataFromRequest(req)
			require.NoError(t, err)

			var job struct {
				Batch []struct {
					MessageID string `json:"messageID"`
				} `json:"batch"`
			}

			err = jsonrs.Unmarshal(jobForm.jobs[0].EventPayload, &job)
			require.NoError(t, err)

			u, err := uuid.Parse(job.Batch[0].MessageID)
			require.NoError(t, err)
			require.Equal(t, uuid.Version(4), u.Version())
		})

		t.Run("sanitizes messageID, remove bad runes and trim space", func(t *testing.T) {
			f := newJobDataFromRequestFixture(t)
			// passing a messageID full of invisible characters
			payloadMap := map[string]any{
				"batch": []any{
					map[string]any{
						"type":      "track",
						"userId":    map[string]any{"id": 456},
						"messageId": "\u0000 -a-random-string \u00A0\t\n\r\u034F",
					},
				},
			}
			payload, err := jsonrs.Marshal(payloadMap)
			require.NoError(t, err)
			req := &webRequestT{
				reqType:        "batch",
				authContext:    rCtxEnabled,
				done:           make(chan<- string),
				userIDHeader:   userIDHeader,
				requestPayload: payload,
			}
			jobForm, err := f.gateway.getJobDataFromRequest(req)
			require.NoError(t, err)

			var job struct {
				Batch []struct {
					MessageID string `json:"messageID"`
				} `json:"batch"`
			}

			err = jsonrs.Unmarshal(jobForm.jobs[0].EventPayload, &job)
			require.NoError(t, err)
			require.Equal(t, "-a-random-string", job.Batch[0].MessageID)
		})

		t.Run("doesn't override if receivedAt or request_ip already exists in payload", func(t *testing.T) {
			f := newJobDataFromRequestFixture(t)
			req := &webRequestT{
				reqType:        "batch",
				authContext:    rCtxEnabled,
				done:           make(chan<- string),
				userIDHeader:   userIDHeader,
				requestPayload: []byte(`{"batch": [{"type": "extract", "receivedAt": "2024-01-01T01:01:01.000000001Z", "request_ip": "dummyIPFromPayload"}]}`),
			}
			jobForm, err := f.gateway.getJobDataFromRequest(req)
			require.NoError(t, err)

			var job struct {
				Batch []struct {
					ReceivedAt string `json:"receivedAt"`
					RequestIP  string `json:"request_ip"`
				} `json:"batch"`
			}
			err = jsonrs.Unmarshal(jobForm.jobs[0].EventPayload, &job)
			require.NoError(t, err)
			require.Contains(t, job.Batch[0].ReceivedAt, "2024-01-01T01:01:01.000000001Z")
			require.Contains(t, job.Batch[0].RequestIP, "dummyIPFromPayload")
		})

		t.Run("adds receivedAt and request_ip in the request payload if it's not already present", func(t *testing.T) {
			f := newJobDataFromRequestFixture(t)
			req := &webRequestT{
				reqType:        "batch",
				authContext:    rCtxEnabled,
				done:           make(chan<- string),
				userIDHeader:   userIDHeader,
				requestPayload: []byte(`{"batch": [{"type": "extract"}]}`),
			}
			req.ipAddr = "dummyIP"
			jobForm, err := f.gateway.getJobDataFromRequest(req)
			require.NoError(t, err)

			var job struct {
				Batch []struct {
					ReceivedAt string `json:"receivedAt"`
					RequestIP  string `json:"request_ip"`
				} `json:"batch"`
			}
			err = jsonrs.Unmarshal(jobForm.jobs[0].EventPayload, &job)
			require.NoError(t, err)
			require.NotEmpty(t, job.Batch[0].ReceivedAt)
			require.Contains(t, job.Batch[0].RequestIP, "dummyIP")
		})

		t.Run("allows extract events even if userID and anonID are not present in the request payload", func(t *testing.T) {
			f := newJobDataFromRequestFixture(t)
			req := &webRequestT{
				reqType:        "batch",
				authContext:    rCtxEnabled,
				done:           make(chan<- string),
				userIDHeader:   userIDHeader,
				requestPayload: []byte(`{"batch": [{"type": "extract"}]}`),
			}
			_, err := f.gateway.getJobDataFromRequest(req)
			require.NoError(t, err)
		})

		t.Run("allows retl events even if userID and anonID are not present in the request payload", func(t *testing.T) {
			f := newJobDataFromRequestFixture(t)
			req := &webRequestT{
				reqType:        "retl",
				authContext:    rCtxEnabled,
				done:           make(chan<- string),
				userIDHeader:   userIDHeader,
				requestPayload: []byte(`{"batch": [{"type": "record"}]}`),
			}
			_, err := f.gateway.getJobDataFromRequest(req)
			require.NoError(t, err)
		})

		t.Run("rate limit checks", func(t *testing.T) {
			type rateLimitChecksFixture struct {
				*jobDataFromRequestFixture
				req     *webRequestT
				gateway *Handle
			}
			newRateLimitChecksFixture := func(t *testing.T) *rateLimitChecksFixture {
				t.Helper()
				f := &rateLimitChecksFixture{jobDataFromRequestFixture: newJobDataFromRequestFixture(t)}
				f.conf.Set("Gateway.enableRateLimit", true)

				// Create a new fresh logger and stats to avoid nils
				f.gateway = f.setupGateway(t, stats.NOP, f.mockRateLimiter, sourcedebugger.NewNoOpService())

				f.req = &webRequestT{
					reqType:        "batch",
					authContext:    rCtxEnabled,
					done:           make(chan<- string),
					userIDHeader:   userIDHeader,
					requestPayload: []byte(`{"batch": [{"type": "track", "userId": "123"}]}`),
				}
				return f
			}

			t.Run("returns errRequestDropped when rate limit is reached", func(t *testing.T) {
				f := newRateLimitChecksFixture(t)
				ctx := t.Context()
				f.req.ctx = ctx
				f.mockRateLimiter.EXPECT().CheckLimitReached(ctx, rCtxEnabled.WorkspaceID, int64(1)).Return(true, nil).Times(1)

				jobData, err := f.gateway.getJobDataFromRequest(f.req)
				require.Equal(t, errRequestDropped, err)
				require.NotNil(t, jobData)
			})

			t.Run("returns context error when context is cancelled and rate limit returns error", func(t *testing.T) {
				f := newRateLimitChecksFixture(t)
				ctx, cancel := context.WithCancel(t.Context())
				f.req.ctx = ctx
				cancel() // This line cancels the context immediately

				expectedErr := errors.New("some rate limit error")
				f.mockRateLimiter.EXPECT().CheckLimitReached(ctx, rCtxEnabled.WorkspaceID, int64(1)).Return(false, expectedErr).Times(1)

				jobData, err := f.gateway.getJobDataFromRequest(f.req)
				require.Equal(t, context.Canceled, err)
				require.NotNil(t, jobData)
			})

			t.Run("logs error and allows request when rate limit returns error but context is not cancelled", func(t *testing.T) {
				f := newRateLimitChecksFixture(t)
				ctx := t.Context()
				f.req.ctx = ctx
				expectedErr := errors.New("some rate limit error")
				f.mockRateLimiter.EXPECT().CheckLimitReached(ctx, rCtxEnabled.WorkspaceID, int64(1)).Return(false, expectedErr).Times(1)

				jobData, err := f.gateway.getJobDataFromRequest(f.req)
				require.NoError(t, err)
				require.NotNil(t, jobData)
				require.Len(t, jobData.jobs, 1)
			})
		})
	})

	t.Run("Internal Batch", func(t *testing.T) {
		client := http.Client{}

		createInternalBatchPayload := func(userID, sourceID string) []byte {
			validData := fmt.Sprintf(
				`{"type":"track","userId":%q, "messageId":"msgid","request_ip":"1.1.1.1","rudderId":"rid","receivedAt":"2024-01-01T01:01:01.000000001Z","data":{"string":"valid-json","nested":{"child":1}},%s}`,
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
				`{"type":"track","userId":%q, "messageId":"msgid","request_ip":"1.1.1.1","rudderId":"rid","receivedAt":"2024-01-01T01:01:01.000000001Z","data":{"string":"valid-json","nested":{"child":1}},%s}`,
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
			return fmt.Appendf(nil, `[%s,%s]`, internalBatchPayload(), internalBatchPayload())
		}

		// a second after receivedAt
		now, err := time.Parse(time.RFC3339Nano, "2024-01-01T01:01:02.000000001Z")
		require.NoError(t, err)

		type internalBatchFixture struct {
			*gatewayFixture
			gateway               *Handle
			internalBatchEndpoint string
			statStore             *memstats.Store
			srcDebugger           *mocksrcdebugger.MockSourceDebugger
		}
		newInternalBatchFixture := func(t *testing.T) *internalBatchFixture {
			t.Helper()
			f := &internalBatchFixture{gatewayFixture: newGatewayFixture(t)}
			f.withSuppressUser()
			f.mockSuppressUser.EXPECT().GetSuppressedUser(WorkspaceID, NormalUserID, writeKeyNotPresentInSource).Return(nil).AnyTimes()

			serverPort, err := kithelper.GetFreePort()
			require.NoError(t, err)
			f.internalBatchEndpoint = fmt.Sprintf("http://localhost:%d/internal/v1/batch", serverPort)
			t.Setenv("RSERVER_GATEWAY_WEB_PORT", strconv.Itoa(serverPort))

			f.statStore, err = memstats.New(
				memstats.WithNow(func() time.Time {
					return now
				}),
			)
			require.NoError(t, err)

			f.srcDebugger = mocksrcdebugger.NewMockSourceDebugger(f.mockCtrl)
			f.gateway = f.setupGateway(t, f.statStore, nil, f.srcDebugger, WithInternalEndpointsEnabled(true))
			f.mockBackendConfig.EXPECT().WaitForConfig(gomock.Any()).AnyTimes()
			ctx, cancel := context.WithCancel(t.Context())
			wait := make(chan error, 1)
			go func() {
				wait <- f.gateway.StartWebHandler(ctx)
			}()
			t.Cleanup(func() {
				cancel()
				require.NoError(t, <-wait)
			})
			require.Eventually(t, func() bool {
				resp, err := http.Get(fmt.Sprintf("http://localhost:%d/version", serverPort))
				if err != nil {
					return false
				}
				defer func() { _ = resp.Body.Close() }()
				return resp.StatusCode == http.StatusOK
			}, time.Second*10, time.Second)
			return f
		}

		t.Run("Successful request, with debugger", func(t *testing.T) {
			f := newInternalBatchFixture(t)
			f.srcDebugger.EXPECT().RecordEvent(WriteKeyEnabled, gomock.Any()).Times(1)
			f.mockJobsDB.EXPECT().WithStoreSafeTx(gomock.Any(), gomock.Any()).Times(1)
			payload := createInternalBatchPayload(NormalUserID, SourceIDEnabled)
			req, err := http.NewRequest(http.MethodPost, f.internalBatchEndpoint, bytes.NewBuffer(payload))
			require.NoError(t, err)
			resp, err := client.Do(req)
			require.NoError(t, err)
			defer func() { _ = resp.Body.Close() }()
			require.Equal(t, http.StatusOK, resp.StatusCode)

			require.Equal(t, []memstats.Metric{
				{
					Name: "gateway.event_pickup_lag_seconds",
					Tags: map[string]string{"workspaceId": WorkspaceID},
					Durations: []time.Duration{
						time.Second,
					},
				},
			}, f.statStore.GetByName("gateway.event_pickup_lag_seconds"))
			require.Equal(t, []memstats.Metric{
				{
					Name: "gateway.write_key_events",
					Tags: map[string]string{
						"source":        "",
						"writeKey":      WriteKeyEnabled,
						"reqType":       "internalBatch",
						"workspaceId":   WorkspaceID,
						"sourceID":      SourceIDEnabled,
						"sourceType":    sourceType2,
						"sdkVersion":    "",
						"sourceDefName": SourceIDEnabled,
					},
					Value: 1,
				},
			}, f.statStore.GetByName("gateway.write_key_events"))
			require.Equal(t, []memstats.Metric{
				{
					Name: "gateway.write_key_successful_events",
					Tags: map[string]string{
						"source":        "",
						"writeKey":      WriteKeyEnabled,
						"reqType":       "internalBatch",
						"workspaceId":   WorkspaceID,
						"sourceID":      SourceIDEnabled,
						"sourceType":    sourceType2,
						"sdkVersion":    "",
						"sourceDefName": SourceIDEnabled,
					},
					Value: 1,
				},
			}, f.statStore.GetByName("gateway.write_key_successful_events"))
			require.Equal(t, []memstats.Metric{
				{
					Name: "gateway.write_key_requests",
					Tags: map[string]string{
						"workspaceId":   "",
						"sourceID":      "",
						"sourceType":    "",
						"sdkVersion":    "",
						"source":        "",
						"writeKey":      "",
						"reqType":       "internalBatch",
						"sourceDefName": "",
					},
					Value: 1,
				},
			}, f.statStore.GetByName("gateway.write_key_requests"))
			require.Equal(t, []memstats.Metric{
				{
					Name: "gateway.write_key_successful_requests",
					Tags: map[string]string{
						"source":        "",
						"writeKey":      "",
						"reqType":       "internalBatch",
						"workspaceId":   "",
						"sourceID":      "",
						"sourceType":    "",
						"sdkVersion":    "",
						"sourceDefName": "",
					},
					Value: 1,
				},
			}, f.statStore.GetByName("gateway.write_key_successful_requests"))
			require.Equal(t, []memstats.Metric{
				{
					Name: "gateway.write_key_failed_requests",
					Tags: map[string]string{
						"source":        "",
						"writeKey":      "",
						"reqType":       "internalBatch",
						"workspaceId":   "",
						"sourceID":      "",
						"sourceType":    "",
						"sdkVersion":    "",
						"sourceDefName": "",
					},
					Value: 0,
				},
			}, f.statStore.GetByName("gateway.write_key_failed_requests"))
			require.Equal(t, []memstats.Metric{
				{
					Name: "gateway.write_key_failed_events",
					Tags: map[string]string{
						"source":        "",
						"writeKey":      WriteKeyEnabled,
						"reqType":       "internalBatch",
						"workspaceId":   WorkspaceID,
						"sourceID":      SourceIDEnabled,
						"sourceType":    sourceType2,
						"sdkVersion":    "",
						"sourceDefName": SourceIDEnabled,
					},
					Value: 0,
				},
			}, f.statStore.GetByName("gateway.write_key_failed_events"))
		})

		t.Run("Successful request, without debugger", func(t *testing.T) {
			f := newInternalBatchFixture(t)
			f.mockJobsDB.EXPECT().WithStoreSafeTx(gomock.Any(), gomock.Any()).Times(1)
			payload := createInternalBatchPayload(NormalUserID, writeKeyNotPresentInSource)
			req, err := http.NewRequest(http.MethodPost, f.internalBatchEndpoint, bytes.NewBuffer(payload))
			require.NoError(t, err)
			resp, err := client.Do(req)
			require.NoError(t, err)
			defer func() { _ = resp.Body.Close() }()
			require.Equal(t, http.StatusOK, resp.StatusCode)
		})

		t.Run("request failed 0 messages", func(t *testing.T) {
			f := newInternalBatchFixture(t)
			req, err := http.NewRequest(http.MethodPost, f.internalBatchEndpoint, bytes.NewBuffer([]byte(`[]`)))
			require.NoError(t, err)
			resp, err := client.Do(req)
			require.NoError(t, err)
			defer func() { _ = resp.Body.Close() }()
			require.Equal(t, http.StatusBadRequest, resp.StatusCode)
			respData, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Contains(t, string(respData), response.NotRudderEvent)
			failedRequestStat := f.statStore.Get("gateway.write_key_failed_requests", map[string]string{
				"writeKey":      "",
				"reqType":       "internalBatch",
				"reason":        response.NotRudderEvent,
				"workspaceId":   "",
				"sourceID":      "",
				"sourceType":    "",
				"sdkVersion":    "",
				"source":        "",
				"sourceDefName": "",
			})
			require.NotNil(t, failedRequestStat)
			require.Equal(t, []float64{1}, failedRequestStat.Values())
		})

		t.Run("request failed unmarshall error", func(t *testing.T) {
			f := newInternalBatchFixture(t)
			req, err := http.NewRequest(http.MethodPost, f.internalBatchEndpoint, bytes.NewBuffer([]byte(`[`)))
			require.NoError(t, err)
			resp, err := client.Do(req)
			require.NoError(t, err)
			defer func() { _ = resp.Body.Close() }()
			require.Equal(t, http.StatusBadRequest, resp.StatusCode)
			respData, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Contains(t, string(respData), response.InvalidJSON)
			failedRequestStat := f.statStore.Get("gateway.write_key_failed_requests", map[string]string{
				"writeKey":      "",
				"reqType":       "internalBatch",
				"reason":        response.InvalidJSON,
				"workspaceId":   "",
				"sourceID":      "",
				"sourceType":    "",
				"sdkVersion":    "",
				"source":        "",
				"sourceDefName": "",
			})
			require.NotNil(t, failedRequestStat)
			require.Equal(t, []float64{1}, failedRequestStat.Values())
			failedEventStat := f.statStore.Get("gateway.write_key_failed_events", map[string]string{
				"writeKey":      "",
				"reqType":       "internalBatch",
				"reason":        response.InvalidJSON,
				"workspaceId":   "",
				"sourceID":      "",
				"sourceType":    "",
				"sdkVersion":    "",
				"source":        "",
				"sourceDefName": "",
			})
			require.Nil(t, failedEventStat)
		})

		t.Run("request success - suppressed user", func(t *testing.T) {
			f := newInternalBatchFixture(t)
			payload := createInternalBatchPayload(SuppressedUserID, SourceIDEnabled)
			req, err := http.NewRequest(http.MethodPost, f.internalBatchEndpoint, bytes.NewBuffer(payload))
			require.NoError(t, err)
			resp, err := client.Do(req)
			require.NoError(t, err)
			defer func() { _ = resp.Body.Close() }()
			require.Equal(t, http.StatusOK, resp.StatusCode)
			successfulReqStat := f.statStore.Get("gateway.write_key_successful_requests", map[string]string{
				"writeKey":      "",
				"reqType":       "internalBatch",
				"workspaceId":   "",
				"sourceID":      "",
				"sourceType":    "",
				"sdkVersion":    "",
				"source":        "",
				"sourceDefName": "",
			})
			require.NotNil(t, successfulReqStat)
			require.Equal(t, []float64{1}, successfulReqStat.Values())
		})

		t.Run("request success - multiple messages", func(t *testing.T) {
			f := newInternalBatchFixture(t)
			f.srcDebugger.EXPECT().RecordEvent(WriteKeyEnabled, gomock.Any()).Times(2)
			f.mockJobsDB.EXPECT().WithStoreSafeTx(gomock.Any(), gomock.Any()).Times(1)
			payload := createInternalBatchPayloadWithMultipleEvents(NormalUserID, SourceIDEnabled, WorkspaceID)
			req, err := http.NewRequest(http.MethodPost, f.internalBatchEndpoint, bytes.NewBuffer(payload))
			require.NoError(t, err)
			resp, err := client.Do(req)
			require.NoError(t, err)
			defer func() { _ = resp.Body.Close() }()
			require.Equal(t, http.StatusOK, resp.StatusCode)
			successfulReqStat := f.statStore.Get("gateway.write_key_successful_requests", map[string]string{
				"writeKey":      "",
				"reqType":       "internalBatch",
				"workspaceId":   "",
				"sourceID":      "",
				"sourceType":    "",
				"sdkVersion":    "",
				"source":        "",
				"sourceDefName": "",
			})
			require.NotNil(t, successfulReqStat)
			require.Equal(t, float64(1), successfulReqStat.LastValue())
			successfulEventStat := f.statStore.Get("gateway.write_key_successful_events", map[string]string{
				"writeKey":      WriteKeyEnabled,
				"reqType":       "internalBatch",
				"workspaceId":   WorkspaceID,
				"sourceID":      SourceIDEnabled,
				"sourceType":    sourceType2,
				"sdkVersion":    "",
				"source":        "",
				"sourceDefName": SourceIDEnabled,
			})
			require.NotNil(t, successfulEventStat)
			require.Equal(t, float64(2), successfulEventStat.LastValue())
			eventsStat := f.statStore.Get("gateway.write_key_events", map[string]string{
				"writeKey":      WriteKeyEnabled,
				"reqType":       "internalBatch",
				"workspaceId":   WorkspaceID,
				"sourceID":      SourceIDEnabled,
				"sourceType":    sourceType2,
				"sdkVersion":    "",
				"source":        "",
				"sourceDefName": SourceIDEnabled,
			})
			require.NotNil(t, eventsStat)
			require.Equal(t, []float64{1, 2}, eventsStat.Values())
		})

		t.Run("request failed db error", func(t *testing.T) {
			f := newInternalBatchFixture(t)
			f.mockJobsDB.EXPECT().WithStoreSafeTx(gomock.Any(), gomock.Any()).Times(1).Return(fmt.Errorf("db error"))
			payload := createInternalBatchPayload(NormalUserID, SourceIDEnabled)
			req, err := http.NewRequest(http.MethodPost, f.internalBatchEndpoint, bytes.NewBuffer(payload))
			require.NoError(t, err)
			resp, err := client.Do(req)
			require.NoError(t, err)
			defer func() { _ = resp.Body.Close() }()
			require.Equal(t, http.StatusInternalServerError, resp.StatusCode)
			failedReqStat := f.statStore.Get("gateway.write_key_failed_requests", map[string]string{
				"writeKey":      "",
				"reqType":       "internalBatch",
				"workspaceId":   "",
				"sourceID":      "",
				"sourceType":    "",
				"sdkVersion":    "",
				"source":        "",
				"reason":        "storeFailed",
				"sourceDefName": "",
			})
			require.NotNil(t, failedReqStat)
			require.Equal(t, []float64{1}, failedReqStat.Values())
			failedEventStat := f.statStore.Get("gateway.write_key_failed_events", map[string]string{
				"writeKey":      WriteKeyEnabled,
				"reqType":       "internalBatch",
				"workspaceId":   WorkspaceID,
				"sourceID":      SourceIDEnabled,
				"sourceType":    sourceType2,
				"sdkVersion":    "",
				"source":        "",
				"reason":        "storeFailed",
				"sourceDefName": SourceIDEnabled,
			})
			require.NotNil(t, failedEventStat)
			require.Equal(t, []float64{1}, failedEventStat.Values())
			eventsStat := f.statStore.Get("gateway.write_key_events", map[string]string{
				"writeKey":      WriteKeyEnabled,
				"reqType":       "internalBatch",
				"workspaceId":   WorkspaceID,
				"sourceID":      SourceIDEnabled,
				"sourceType":    sourceType2,
				"sdkVersion":    "",
				"source":        "",
				"sourceDefName": SourceIDEnabled,
			})
			require.NotNil(t, eventsStat)
			require.Equal(t, []float64{1}, eventsStat.Values())
		})
	})

	t.Run("extractJobsFromInternalBatchPayload", func(t *testing.T) {
		newExtractJobsFixture := func(t *testing.T) *Handle {
			t.Helper()
			f := newGatewayFixture(t)
			f.initializeAppFeatures()
			return f.setupGateway(t, stats.NOP, nil, sourcedebugger.NewNoOpService(), WithInternalEndpointsEnabled(true))
		}

		t.Run("doesn't override if receivedAt or request_ip already exists in payload", func(t *testing.T) {
			gateway := newExtractJobsFixture(t)
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
				Payload: []byte(`{
								"receivedAt": "dummyReceivedAtFromPayload",
								"request_ip": "dummyIPFromPayload",
								"messageId": "dummyMessageID",
								"rudderId": "dummyRudderID",
								"type": "track",
								"userId": "userID"
							}`),
			}
			messages := []stream.Message{msg}
			payload, err := jsonrs.Marshal(messages)
			require.NoError(t, err)

			req := &webRequestT{
				reqType:        "batch",
				authContext:    rCtxEnabled,
				done:           make(chan<- string),
				requestPayload: payload,
			}
			jobsWithStats, err := gateway.extractJobsFromInternalBatchPayload("batch", req.requestPayload)
			require.NoError(t, err)
			require.Len(t, jobsWithStats, 1)
			require.Equal(t,
				gwstats.SourceStat{
					SourceID:    "sourceID",
					WorkspaceID: "workspaceID",
					ReqType:     "batch",
				},
				jobsWithStats[0].stat,
			)

			var job struct {
				Batch []struct {
					ReceivedAt string `json:"receivedAt"`
					RequestIP  string `json:"request_ip"`
				} `json:"batch"`
			}
			jobForm := jobsWithStats[0].job
			err = jsonrs.Unmarshal(jobForm.EventPayload, &job)
			require.NoError(t, err)
			require.Len(t, job.Batch, 1)
			require.Contains(t, job.Batch[0].ReceivedAt, "dummyReceivedAtFromPayload")
			require.Contains(t, job.Batch[0].RequestIP, "dummyIPFromPayload")
		})

		t.Run("validations", func(t *testing.T) {
			gateway := newExtractJobsFixture(t)
			properties := stream.MessageProperties{
				RequestType:   "track",
				RoutingKey:    "anonymousId_header<<>>anonymousId_1<<>>identified_user_id",
				WorkspaceID:   "workspaceID",
				SourceID:      "sourceID",
				ReceivedAt:    time.Date(2024, 1, 1, 1, 1, 1, 1, time.UTC),
				RequestIP:     "dummyIP",
				DestinationID: "destinationID",
			}

			// Test case: valid payload
			msg := stream.Message{
				Properties: properties,
				Payload:    []byte(`{"type": "track", "event": "test-1", "userId": "user1", "request_ip": "dummyIP", "messageId": "dummyMessageID", "rudderId": "dummyRudderID", "receivedAt": "2024-01-01T01:01:01.001Z"}`),
			}
			messages := []stream.Message{msg}
			payload, err := jsonrs.Marshal(messages)
			require.NoError(t, err)
			req := &webRequestT{
				reqType:        "batch",
				authContext:    rCtxEnabled,
				done:           make(chan<- string),
				requestPayload: payload,
			}
			jobsWithStats, err := gateway.extractJobsFromInternalBatchPayload("batch", req.requestPayload)
			require.NoError(t, err)
			require.Len(t, jobsWithStats, 1)

			// Test case: missing messageID
			msg.Payload = []byte(`{"type": "track", "event": "test-1", "userId": "user1", "request_ip": "dummyIP", "rudderId": "dummyRudderID", "receivedAt": "2024-01-01T01:01:01.001Z"}`)
			messages = []stream.Message{msg}
			payload, err = jsonrs.Marshal(messages)
			require.NoError(t, err)
			req.requestPayload = payload
			_, err = gateway.extractJobsFromInternalBatchPayload("batch", req.requestPayload)
			require.Error(t, err)
			require.Equal(t, response.NotRudderEvent, err.Error())

			// Test case: missing required properties
			msg.Properties = stream.MessageProperties{}
			messages = []stream.Message{msg}
			payload, err = jsonrs.Marshal(messages)
			require.NoError(t, err)
			req.requestPayload = payload
			_, err = gateway.extractJobsFromInternalBatchPayload("batch", req.requestPayload)
			require.Error(t, err)
			require.Equal(t, response.NotRudderEvent, err.Error())

			// Test case: missing receivedAt in payload
			msg.Properties = properties
			msg.Payload = []byte(`{"type": "track", "event": "test-1", "userId": "user1", "request_ip": "dummyIP", "messageId": "dummyMessageID", "rudderId": "dummyRudderID"}`)
			messages = []stream.Message{msg}
			payload, err = jsonrs.Marshal(messages)
			require.NoError(t, err)
			req.requestPayload = payload
			_, err = gateway.extractJobsFromInternalBatchPayload("batch", req.requestPayload)
			require.Error(t, err)
			require.Equal(t, response.NotRudderEvent, err.Error())

			// Test case: missing type in payload
			msg.Payload = []byte(`{"event": "test-1", "userId": "user1", "request_ip": "dummyIP", "messageId": "dummyMessageID", "rudderId": "dummyRudderID", "receivedAt": "2024-01-01T01:01:01.001Z"}`)
			messages = []stream.Message{msg}
			payload, err = jsonrs.Marshal(messages)
			require.NoError(t, err)
			req.requestPayload = payload
			_, err = gateway.extractJobsFromInternalBatchPayload("batch", req.requestPayload)
			require.Error(t, err)
			require.Equal(t, response.NotRudderEvent, err.Error())

			// Test case: missing request_ip in payload
			msg.Payload = []byte(`{"type": "track", "event": "test-1", "userId": "user1", "messageId": "dummyMessageID", "rudderId": "dummyRudderID", "receivedAt": "2024-01-01T01:01:01.001Z"}`)
			messages = []stream.Message{msg}
			payload, err = jsonrs.Marshal(messages)
			require.NoError(t, err)
			req.requestPayload = payload
			_, err = gateway.extractJobsFromInternalBatchPayload("batch", req.requestPayload)
			require.Error(t, err)
			require.Equal(t, response.NotRudderEvent, err.Error())

			// Test case: missing rudderId in payload
			msg.Payload = []byte(`{"type": "track", "event": "test-1", "userId": "user1", "request_ip": "dummyIP", "messageId": "dummyMessageID", "receivedAt": "2024-01-01T01:01:01.001Z"}`)
			messages = []stream.Message{msg}
			payload, err = jsonrs.Marshal(messages)
			require.NoError(t, err)
			req.requestPayload = payload
			_, err = gateway.extractJobsFromInternalBatchPayload("batch", req.requestPayload)
			require.Error(t, err)
			require.Equal(t, response.NotRudderEvent, err.Error())
		})

		t.Run("adds isBot, botName, botURL, botIsInvalidBrowser, botAction in the request parameters", func(t *testing.T) {
			gateway := newExtractJobsFixture(t)
			properties := stream.MessageProperties{
				RequestType:         "dummyRequestType",
				RoutingKey:          "anonymousId_header<<>>anonymousId_1<<>>identified_user_id",
				WorkspaceID:         "workspaceID",
				SourceID:            "sourceID",
				ReceivedAt:          time.Date(2024, 1, 1, 1, 1, 1, 1, time.UTC),
				RequestIP:           "dummyIP",
				DestinationID:       "destinationID",
				IsBot:               true,
				BotName:             "dummyBotName",
				BotURL:              "dummyBotURL",
				BotIsInvalidBrowser: true,
				BotAction:           "flag",
			}
			msg := stream.Message{
				Properties: properties,
				Payload:    []byte(`{"type": "track", "event": "test-1", "userId": "user1", "request_ip": "dummyIP", "messageId": "dummyMessageID", "rudderId": "dummyRudderID", "receivedAt": "2024-01-01T01:01:01.001Z"}`),
			}
			messages := []stream.Message{msg}
			payload, err := jsonrs.Marshal(messages)
			require.NoError(t, err)
			req := &webRequestT{
				reqType:        "batch",
				authContext:    rCtxEnabled,
				done:           make(chan<- string),
				requestPayload: payload,
			}
			jobsWithStats, err := gateway.extractJobsFromInternalBatchPayload("batch", req.requestPayload)
			require.NoError(t, err)
			require.Len(t, jobsWithStats, 1)

			// check if the parameters are present in the request
			type params struct {
				IsBot               bool   `json:"is_bot"`
				BotName             string `json:"bot_name"`
				BotURL              string `json:"bot_url"`
				BotIsInvalidBrowser bool   `json:"bot_is_invalid_browser"`
				BotAction           string `json:"bot_action"`
			}
			var reqParams params
			err = jsonrs.Unmarshal(jobsWithStats[0].job.Parameters, &reqParams)
			require.NoError(t, err)
			require.True(t, reqParams.IsBot)
			require.Equal(t, "dummyBotName", reqParams.BotName)
			require.Equal(t, "dummyBotURL", reqParams.BotURL)
			require.True(t, reqParams.BotIsInvalidBrowser)
			require.Equal(t, "flag", reqParams.BotAction)
		})
	})
}

func unauthorizedRequest(body io.Reader) *http.Request {
	req, err := http.NewRequest("GET", "", body)
	if err != nil {
		panic(err)
	}

	return req
}

func authorizedRequest(username string, body io.Reader) *http.Request {
	req := unauthorizedRequest(body)

	basicAuth := base64.StdEncoding.EncodeToString(fmt.Appendf(nil, "%s:password-should-be-ignored", username))

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

func expectHandlerResponse(t *testing.T, handler http.HandlerFunc, req *http.Request, responseStatus int, responseBody, reqType string) {
	t.Helper()
	var body string
	require.Eventually(t, func() bool {
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req.Clone(t.Context()))
		if rr.Body != nil {
			bodyBytes, _ := io.ReadAll(rr.Body)
			body = string(bodyBytes)
		} else {
			body = ""
		}
		return rr.Result().StatusCode == responseStatus
	}, time.Second, 10*time.Millisecond, "reqType: %s", reqType)
	require.Equal(t, responseBody, body, "reqType: %s", reqType)
}

// return all endpoints as key and method as value
func endpointsToVerify() ([]string, []string, []string) {
	getEndpoints := []string{
		"/version",
		"/robots.txt",
		"/pixel/v1/track",
		"/pixel/v1/page",
		"/v1/webhook",
		"/internal/v1/warehouse/jobs/status",
		"/internal/v1/warehouse/fetch-tables",
		"/internal/v2/job-status/123",
		"/internal/v2/job-status/123/failed-records",
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
		"/internal/v1/warehouse/pending-events",
		"/internal/v1/warehouse/trigger-upload",
		"/internal/v1/warehouse/jobs",
	}

	deleteEndpoints := []string{
		"/internal/v2/job-status/1234",
		"/internal/v2/job-status/123/failed-records",
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

func waitForBackendConfigInit(t *testing.T, gw *Handle) {
	t.Helper()
	require.Eventually(t,
		func() bool {
			select {
			case <-gw.backendConfigInitialisedChan:
				return true
			default:
				return false
			}
		},
		2*time.Second, 10*time.Millisecond,
	)
}

func TestLeakyUploader(t *testing.T) {
	// Setup Minio test container
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = time.Minute

	t.Run("invalid JSON payload should be uploaded to Minio", func(t *testing.T) {
		t.Parallel()
		minioContainer, err := minio.Setup(pool, t)
		require.NoError(t, err)

		// create a test gateway with leaky uploader enabled
		gw, cleanupFn := createTestGatewayWithLeakyUploader(t, fmt.Sprintf("http://%s", minioContainer.Endpoint), minioContainer.AccessKeyID, minioContainer.AccessKeySecret)
		t.Cleanup(cleanupFn)

		// Prepare invalid JSON payload
		invalidPayload := []byte(`{"invalid": "json`)

		// Call internal batch handler
		jobs, err := gw.extractJobsFromInternalBatchPayload("batch", invalidPayload)

		// Verify error response
		require.Error(t, err)
		require.Equal(t, response.InvalidJSON, err.Error())
		require.Nil(t, jobs)

		// verify file was uploaded to Minio
		require.Eventually(t, func() bool {
			contents, err := minioContainer.Contents(context.Background(), "gw-failed-events")
			return err == nil && len(contents) != 0
		}, 10*time.Second, time.Second, "Minio bucket not updated within timeout")
	})

	t.Run("valid JSON payload should be processed normally", func(t *testing.T) {
		t.Parallel()
		// Prepare valid payload
		validPayload := []byte(`[
			{
				"properties": {
					"requestType": "track",
					"routingKey": "test-key",
					"workspaceId": "workspace1",
					"sourceId": "source-id-1",
					"receivedAt": "2023-01-01T00:00:00Z",
					"requestIP": "127.0.0.1"
				},
				"payload": {"type":"track","userId":"userid","messageId":"msgid","request_ip":"1.1.1.1","rudderId":"rid","receivedAt":"2024-01-01T01:01:01.000000001Z"}
			}
		]`)

		minioContainer, err := minio.Setup(pool, t)
		require.NoError(t, err)

		// create test gateway with leaky uploader enabled
		gw, cleanupFn := createTestGatewayWithLeakyUploader(t, minioContainer.Endpoint, minioContainer.AccessKeyID, minioContainer.AccessKeySecret)
		t.Cleanup(cleanupFn)

		// Call internal batch handler
		jobs, err := gw.extractJobsFromInternalBatchPayload("batch", validPayload)

		// Verify successful processing
		require.NoError(t, err)
		require.NotNil(t, jobs)
		require.Len(t, jobs, 1)

		// Verify no files uploaded to Minio
		require.Never(t, func() bool {
			contents, err := minioContainer.Contents(context.Background(), "gw-failed-events")
			require.NoError(t, err)
			return len(contents) != 0
		}, 5*time.Second, time.Second, "Minio bucket not updated within timeout")
	})
}

// Helper function to create test gateway with leaky uploader
func createTestGatewayWithLeakyUploader(t *testing.T, endpoint, accessKeyID, secretKey string) (gw *Handle, cleanup func()) {
	mockCtrl := gomock.NewController(t)
	mockJobsDB := mocksJobsDB.NewMockJobsDB(mockCtrl)
	mockBackendConfig := mocksBackendConfig.NewMockBackendConfig(mockCtrl)
	mockApp := mocksApp.NewMockApp(mockCtrl)
	mockRateLimiter := mockGateway.NewMockThrottler(mockCtrl)
	mockWebhook := mockGateway.NewMockWebhookRequestHandler(mockCtrl)
	mockWebhook.EXPECT().Shutdown().AnyTimes()
	mockBackendConfig.EXPECT().Subscribe(gomock.Any(), backendconfig.TopicProcessConfig).
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
	mockVersionHandler := func(w http.ResponseWriter, r *http.Request) {}
	enterpriseFeatures := &app.Features{}
	mockApp.EXPECT().Features().Return(enterpriseFeatures).AnyTimes()
	gw = &Handle{}
	conf := config.New()
	conf.Set("Gateway.leakyUploader.enabled", true)
	conf.Set("Gateway.leakyUploader.Storage.Endpoint", endpoint)
	conf.Set("Gateway.leakyUploader.Storage.AccessKeyId", accessKeyID)
	conf.Set("Gateway.leakyUploader.Storage.AccessKey", secretKey)
	conf.Set("Gateway.leakyUploader.Storage.Bucket", "rudder-saas")
	conf.Set("Gateway.leakyUploader.Timeout", "2s")
	conf.Set("Gateway.leakyUploader.Storage.DisableSsl", true)
	conf.Set("Gateway.leakyUploader.Storage.UseGlue", true)
	conf.Set("Gateway.leakyUploader.Storage.S3ForcePathStyle", true)
	err := gw.Setup(context.Background(), conf, logger.NewLogger().Withn(logger.NewStringField("component", "test")), stats.NOP, mockApp, mockBackendConfig, mockJobsDB, mockRateLimiter, mockVersionHandler, rsources.NewNoOpService(), transformer.NewNoOpService(), sourcedebugger.NewNoOpService(), nil, WithInternalEndpointsEnabled(true))
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		select {
		case <-gw.backendConfigInitialisedChan:
			return true
		default:
			return false
		}
	}, 2*time.Second, time.Second)
	return gw, func() {
		_ = gw.Shutdown()
		mockCtrl.Finish()
	}
}
