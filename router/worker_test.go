package router

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/enterprise/reporting"
	"github.com/rudderlabs/rudder-server/processor/integrations"
	"github.com/rudderlabs/rudder-server/router/throttler"
	"github.com/rudderlabs/rudder-server/router/transformer"
	"github.com/rudderlabs/rudder-server/router/types"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	mocksRouter "github.com/rudderlabs/rudder-server/mocks/router"
	mocksTransformer "github.com/rudderlabs/rudder-server/mocks/router/transformer"
	destinationdebugger "github.com/rudderlabs/rudder-server/services/debugger/destination"
	"github.com/rudderlabs/rudder-server/services/rsources"
	transformerFeaturesService "github.com/rudderlabs/rudder-server/services/transformer"
	"github.com/rudderlabs/rudder-server/services/transientsource"
)

func TestConsolidateRespBodys(t *testing.T) {
	tcs := []struct {
		in       []map[int64]string
		expected map[int64]string
	}{
		{
			in: []map[int64]string{{
				1: "1",
				2: "2",
				3: "3",
			}, {
				1: "1",
				2: "2",
				3: "3",
			}},
			expected: map[int64]string{
				1: "1 1",
				2: "2 2",
				3: "3 3",
			},
		},
		{
			in: []map[int64]string{{
				1: "1",
				2: "2",
				3: "3",
			}, {
				1: "1",
				2: "2",
			}},
			expected: map[int64]string{
				1: "1 1",
				2: "2 2",
				3: "3 ",
			},
		},
		{
			in: []map[int64]string{{
				1: "1",
				2: "2",
			}, {
				1: "1",
				2: "2",
				3: "3",
			}},
			expected: map[int64]string{
				1: "1 1",
				2: "2 2",
			},
		},
		{
			in:       nil,
			expected: nil,
		},
	}

	for i, tc := range tcs {
		testCaseName := fmt.Sprintf("test case index: %d", i)
		t.Run(testCaseName, func(t *testing.T) {
			out := consolidateRespBodys(tc.in)
			require.Equal(t, tc.expected, out)
		})
	}
}

func TestAnyNonTerminalCode(t *testing.T) {
	tcs := []struct {
		in       map[int64]int
		expected bool
	}{
		{
			in: map[int64]int{
				1: 201,
				2: 404,
				3: 504,
			},
			expected: true,
		},
		{
			in: map[int64]int{
				1: 201,
				2: 204,
				3: 404,
			},
			expected: false,
		},
		{
			in: map[int64]int{
				1: 503,
				2: 404,
			},
			expected: true,
		},
		{
			in: map[int64]int{
				1: 201,
				3: 599,
			},
			expected: true,
		},
		{
			in: map[int64]int{
				1: 201,
				3: 429,
			},
			expected: true,
		},
		{
			in:       nil,
			expected: false,
		},
	}

	for i, tc := range tcs {
		testCaseName := fmt.Sprintf("test case index: %d", i)
		t.Run(testCaseName, func(t *testing.T) {
			out := anyNonTerminalCode(tc.in)
			require.Equal(t, tc.expected, out)
		})
	}
}

var _ = Describe("Proxy Request", func() {
	initRouter()

	var c *testContext
	var conf *config.Config

	BeforeEach(func() {
		conf = config.New()
		config.Reset()
		c = &testContext{}
		c.Setup()
	})

	AfterEach(func() {
		config.Reset()
		c.Finish()
	})

	Context("proxyRequest", func() {
		It("should return responses transformer.ProxyRequest returned for every job on transformer.ProxyRequest's 200", func() {
			mockNetHandle := mocksRouter.NewMockNetHandle(c.mockCtrl)
			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)
			router := &Handle{
				Reporting: &reporting.NOOP{},
				netHandle: mockNetHandle,
			}
			c.mockBackendConfig.EXPECT().AccessToken().AnyTimes()

			mockTransformer.EXPECT().ProxyRequest(gomock.Any(), gomock.Any()).
				Times(1).
				DoAndReturn(func(ctx context.Context, proxyReqParams *transformer.ProxyRequestParams) transformer.ProxyRequestResponse {
					Expect(len(proxyReqParams.ResponseData.Metadata)).To(Equal(2))
					Expect(proxyReqParams.ResponseData.Metadata[0].JobID).To(Equal(int64(1)))
					Expect(proxyReqParams.ResponseData.Metadata[1].JobID).To(Equal(int64(2)))
					Expect(proxyReqParams.ResponseData.DestinationConfig).To(Equal(map[string]interface{}{
						"x": map[string]interface{}{
							"y": "z",
						},
					}))

					return transformer.ProxyRequestResponse{
						ProxyRequestStatusCode:   200,
						ProxyRequestResponseBody: "OK",
						RespContentType:          "application/json",
						RespStatusCodes: map[int64]int{
							1: 200,
							2: 201,
						},
						RespBodys: map[int64]string{
							1: "ok1",
							2: "ok2",
						},
					}
				})

			router.Setup(gaDestinationDefinition, logger.NOP, conf, c.mockBackendConfig, c.mockRouterJobsDB, c.mockProcErrorsDB, transientsource.NewEmptyService(), rsources.NewNoOpService(), transformerFeaturesService.NewNoOpService(), destinationdebugger.NewNoOpService(), throttler.NewNoOpThrottlerFactory())
			router.transformer = mockTransformer

			<-router.backendConfigInitialized
			worker := &worker{
				logger:          router.logger.Child("w-0"),
				partition:       "partition",
				id:              1,
				input:           make(chan workerJob, 1),
				rt:              router,
				routerProxyStat: stats.NOP.NewTaggedStat("router_proxy_latency", stats.TimerType, stats.Tags{"destType": "ga"}),
			}

			destinationJob := types.DestinationJobT{
				Message: json.RawMessage(`{}`),
				JobMetadataArray: []types.JobMetadataT{
					{
						JobID: 1,
					},
					{
						JobID: 2,
					},
				},
				Destination: backendconfig.DestinationT{
					ID: gaDestinationID,
					Config: map[string]interface{}{
						"x": map[string]interface{}{
							"y": "z",
						},
					},
				},
				Batched:    false,
				StatusCode: 200,
				Error:      "",
			}

			postParameters := integrations.PostParametersT{
				URL: "https://www.test.com",
			}

			expectedRespCodes := map[int64]int{
				1: 200,
				2: 201,
			}
			expectedRespBodys := map[int64]string{
				1: "ok1",
				2: "ok2",
			}

			resp := worker.proxyRequest(context.TODO(), destinationJob, postParameters)
			respCodes, respBodys, contentType := resp.RespStatusCodes, resp.RespBodys, resp.RespContentType
			require.Equal(GinkgoT(), expectedRespCodes, respCodes)
			require.Equal(GinkgoT(), expectedRespBodys, respBodys)
			require.Equal(GinkgoT(), "application/json", contentType)
		})
		It("should return responses transformer.ProxyRequest returned for every job on transformer.ProxyRequest's non 200 and authType is not OAuth", func() {
			mockNetHandle := mocksRouter.NewMockNetHandle(c.mockCtrl)
			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)
			router := &Handle{
				Reporting: &reporting.NOOP{},
				netHandle: mockNetHandle,
			}
			c.mockBackendConfig.EXPECT().AccessToken().AnyTimes()

			mockTransformer.EXPECT().ProxyRequest(gomock.Any(), gomock.Any()).
				Times(1).
				DoAndReturn(func(ctx context.Context, proxyReqParams *transformer.ProxyRequestParams) transformer.ProxyRequestResponse {
					Expect(len(proxyReqParams.ResponseData.Metadata)).To(Equal(2))
					Expect(proxyReqParams.ResponseData.Metadata[0].JobID).To(Equal(int64(1)))
					Expect(proxyReqParams.ResponseData.Metadata[1].JobID).To(Equal(int64(2)))
					Expect(proxyReqParams.ResponseData.DestinationConfig).To(Equal(map[string]interface{}{
						"x": map[string]interface{}{
							"y": "z",
						},
					}))

					return transformer.ProxyRequestResponse{
						ProxyRequestStatusCode:   400,
						ProxyRequestResponseBody: "Err",
						RespContentType:          "application/json",
						RespStatusCodes: map[int64]int{
							1: 400,
							2: 401,
						},
						RespBodys: map[int64]string{
							1: "err1",
							2: "err2",
						},
					}
				})

			router.Setup(gaDestinationDefinition, logger.NOP, conf, c.mockBackendConfig, c.mockRouterJobsDB, c.mockProcErrorsDB, transientsource.NewEmptyService(), rsources.NewNoOpService(), transformerFeaturesService.NewNoOpService(), destinationdebugger.NewNoOpService(), throttler.NewNoOpThrottlerFactory())
			router.transformer = mockTransformer

			<-router.backendConfigInitialized
			worker := &worker{
				logger:          router.logger.Child("w-0"),
				partition:       "partition",
				id:              1,
				input:           make(chan workerJob, 1),
				rt:              router,
				routerProxyStat: stats.NOP.NewTaggedStat("router_proxy_latency", stats.TimerType, stats.Tags{"destType": "ga"}),
			}

			destinationJob := types.DestinationJobT{
				Message: json.RawMessage(`{}`),
				JobMetadataArray: []types.JobMetadataT{
					{
						JobID: 1,
					},
					{
						JobID: 2,
					},
				},
				Destination: backendconfig.DestinationT{
					ID: gaDestinationID,
					Config: map[string]interface{}{
						"x": map[string]interface{}{
							"y": "z",
						},
					},
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						Config: map[string]interface{}{
							"auth": map[string]interface{}{
								"type": "basic",
							},
						},
					},
				},
				Batched:    false,
				StatusCode: 200,
				Error:      "",
			}

			postParameters := integrations.PostParametersT{
				URL: "https://www.test.com",
			}

			expectedRespCodes := map[int64]int{
				1: 400,
				2: 401,
			}
			expectedRespBodys := map[int64]string{
				1: "err1",
				2: "err2",
			}

			resp := worker.proxyRequest(context.TODO(), destinationJob, postParameters)
			respCodes, respBodys, contentType := resp.RespStatusCodes, resp.RespBodys, resp.RespContentType
			require.Equal(GinkgoT(), expectedRespCodes, respCodes)
			require.Equal(GinkgoT(), expectedRespBodys, respBodys)
			require.Equal(GinkgoT(), "application/json", contentType)
		})
		It("should return responses after going through OAuth handling for every job on transformer.ProxyRequest's non 200 and authType is OAuth", func() {
			mockNetHandle := mocksRouter.NewMockNetHandle(c.mockCtrl)
			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)
			router := &Handle{
				Reporting: &reporting.NOOP{},
				netHandle: mockNetHandle,
			}
			c.mockBackendConfig.EXPECT().AccessToken().AnyTimes()

			mockTransformer.EXPECT().ProxyRequest(gomock.Any(), gomock.Any()).
				Times(1).
				DoAndReturn(func(ctx context.Context, proxyReqParams *transformer.ProxyRequestParams) transformer.ProxyRequestResponse {
					Expect(len(proxyReqParams.ResponseData.Metadata)).To(Equal(2))
					Expect(proxyReqParams.ResponseData.Metadata[0].JobID).To(Equal(int64(1)))
					Expect(proxyReqParams.ResponseData.Metadata[1].JobID).To(Equal(int64(2)))
					Expect(proxyReqParams.ResponseData.DestinationConfig).To(Equal(map[string]interface{}{
						"x": map[string]interface{}{
							"y": "z",
						},
					}))

					return transformer.ProxyRequestResponse{
						ProxyRequestStatusCode:   400,
						ProxyRequestResponseBody: "Err",
						RespContentType:          "application/json",
						RespStatusCodes: map[int64]int{
							1: 500,
							2: 501,
						},
						RespBodys: map[int64]string{
							1: "err1",
							2: "err2",
						},
					}
				})

			router.Setup(gaDestinationDefinition, logger.NOP, conf, c.mockBackendConfig, c.mockRouterJobsDB, c.mockProcErrorsDB, transientsource.NewEmptyService(), rsources.NewNoOpService(), transformerFeaturesService.NewNoOpService(), destinationdebugger.NewNoOpService(), throttler.NewNoOpThrottlerFactory())
			router.transformer = mockTransformer

			<-router.backendConfigInitialized
			worker := &worker{
				logger:          router.logger.Child("w-0"),
				partition:       "partition",
				id:              1,
				input:           make(chan workerJob, 1),
				rt:              router,
				routerProxyStat: stats.NOP.NewTaggedStat("router_proxy_latency", stats.TimerType, stats.Tags{"destType": "ga"}),
			}

			destinationJob := types.DestinationJobT{
				Message: json.RawMessage(`{}`),
				JobMetadataArray: []types.JobMetadataT{
					{
						JobID: 1,
					},
					{
						JobID: 2,
					},
				},
				Destination: backendconfig.DestinationT{
					ID: gaDestinationID,
					Config: map[string]interface{}{
						"x": map[string]interface{}{
							"y": "z",
						},
					},
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						Config: map[string]interface{}{
							"auth": map[string]interface{}{
								"type": "OAuth",
							},
						},
					},
				},
				Batched:    false,
				StatusCode: 200,
				Error:      "",
			}

			postParameters := integrations.PostParametersT{
				URL: "https://www.test.com",
			}

			// Note: Taking advantage of empty rudderAccountID check in handleOAuthDestResponse function
			expectedRespCodes := map[int64]int{
				1: 400,
				2: 400,
			}

			resp := worker.proxyRequest(context.TODO(), destinationJob, postParameters)
			respCodes, _, contentType := resp.RespStatusCodes, resp.RespBodys, resp.RespContentType
			require.Equal(GinkgoT(), expectedRespCodes, respCodes)
			require.Equal(GinkgoT(), "application/json", contentType)
		})
	})
})
