package router

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sync"
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/enterprise/reporting"
	"github.com/rudderlabs/rudder-server/processor/integrations"
	"github.com/rudderlabs/rudder-server/router/internal/partition"
	"github.com/rudderlabs/rudder-server/router/throttler"
	"github.com/rudderlabs/rudder-server/router/transformer"
	"github.com/rudderlabs/rudder-server/router/types"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	kitsync "github.com/rudderlabs/rudder-go-kit/sync"
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

func TestTransformForOAuthV2Destination(t *testing.T) {
	initRouter()
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	mockTransformer := mocksTransformer.NewMockTransformer(mockCtrl)

	worker := &worker{
		rt: &Handle{
			transformer:                    mockTransformer,
			destType:                       "some_dest_type",
			logger:                         logger.NOP,
			routerTransformInputCountStat:  stats.NOP.NewTaggedStat("router_transform_input_count", stats.CountType, stats.Tags{"destType": "some_dest_type"}),
			routerTransformOutputCountStat: stats.NOP.NewTaggedStat("router_transform_output_count", stats.CountType, stats.Tags{"destType": "some_dest_type"}),
			isOAuthDestination:             true,
			reloadableConfig: &reloadableConfig{
				oauthV2Enabled: config.GetReloadableBoolVar(true),
			},
		},
	}
	var limiterWg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	defer limiterWg.Wait()
	defer cancel()
	worker.rt.limiter.transform = kitsync.NewLimiter(ctx, &limiterWg, "transform", math.MaxInt, stats.Default)
	worker.rt.limiter.stats.transform = partition.NewStats()

	routerJobs := []types.RouterJobT{
		{
			Destination: backendconfig.DestinationT{
				ID: "d1",
			},
			Message: json.RawMessage(`{"event": "d1-test1"}`),
			JobMetadata: types.JobMetadataT{
				JobID: 1,
			},
		},
		{
			Destination: backendconfig.DestinationT{
				ID: "d2",
			},
			Message: json.RawMessage(`{"event": "d2-test2"}`),
			JobMetadata: types.JobMetadataT{
				JobID: 2,
			},
		},
		{
			Destination: backendconfig.DestinationT{
				ID: "d1",
			},
			Message: json.RawMessage(`{"event": "d1-test3"}`),
			JobMetadata: types.JobMetadataT{
				JobID: 3,
			},
		},
		{
			Destination: backendconfig.DestinationT{
				ID: "d3",
			},
			Message: json.RawMessage(`{"event": "d3-test4"}`),
			JobMetadata: types.JobMetadataT{
				JobID: 4,
			},
		},
		{
			Destination: backendconfig.DestinationT{
				ID: "d1",
			},
			Message: json.RawMessage(`{"event": "d1-test5"}`),
			JobMetadata: types.JobMetadataT{
				JobID: 5,
			},
		},
		{
			Destination: backendconfig.DestinationT{
				ID: "d2",
			},
			Message: json.RawMessage(`{"event": "d2-test6"}`),
			JobMetadata: types.JobMetadataT{
				JobID: 6,
			},
		},
	}
	mockTransformer.EXPECT().Transform(transformer.ROUTER_TRANSFORM, &types.TransformMessageT{
		Data:     []types.RouterJobT{routerJobs[0], routerJobs[2], routerJobs[4]},
		DestType: worker.rt.destType,
	}).Return([]types.DestinationJobT{
		{
			Destination: backendconfig.DestinationT{
				ID: "d1",
			},
			Message: json.RawMessage(`{"event": ["d1-test1", "d1-test3"]}`),
			JobMetadataArray: []types.JobMetadataT{
				{
					JobID: 1,
				},
				{
					JobID: 3,
				},
			},
		},
		{
			Destination: backendconfig.DestinationT{
				ID: "d1",
			},
			Message: json.RawMessage(`{"event": [ "d1-test5"]}`),
			JobMetadataArray: []types.JobMetadataT{
				{
					JobID: 5,
				},
			},
		},
	})
	mockTransformer.EXPECT().Transform(transformer.ROUTER_TRANSFORM, &types.TransformMessageT{
		Data:     []types.RouterJobT{routerJobs[1], routerJobs[5]},
		DestType: worker.rt.destType,
	}).Return([]types.DestinationJobT{
		{
			Destination: backendconfig.DestinationT{
				ID: "d2",
			},
			Message: json.RawMessage(`{"event": ["d2-test2", "d2-test6"]}`),
			JobMetadataArray: []types.JobMetadataT{
				{
					JobID: 2,
				},
				{
					JobID: 6,
				},
			},
		},
	})
	mockTransformer.EXPECT().Transform(transformer.ROUTER_TRANSFORM, &types.TransformMessageT{
		Data:     []types.RouterJobT{routerJobs[3]},
		DestType: worker.rt.destType,
	}).Return([]types.DestinationJobT{
		{
			Destination: backendconfig.DestinationT{
				ID: "d3",
			},
			Message: json.RawMessage(`{"event": ["d3-test4"]}`),
			JobMetadataArray: []types.JobMetadataT{
				{
					JobID: 4,
				},
			},
		},
	})
	destinationJobs := worker.transform(routerJobs)
	require.Equal(t, 4, len(destinationJobs))
	destinationIDJobsMap := lo.GroupBy(destinationJobs, func(job types.DestinationJobT) string {
		return job.Destination.ID
	})
	require.Equal(t, 3, len(destinationIDJobsMap))
	require.Equal(t, 2, len(destinationIDJobsMap["d1"]))
	require.Equal(t, 1, len(destinationIDJobsMap["d2"]))
	require.Equal(t, 1, len(destinationIDJobsMap["d3"]))
}

func TestTransformForNonOAuthDestination(t *testing.T) {
	initRouter()
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	mockTransformer := mocksTransformer.NewMockTransformer(mockCtrl)

	worker := &worker{
		rt: &Handle{
			transformer:                    mockTransformer,
			destType:                       "some_dest_type",
			logger:                         logger.NOP,
			routerTransformInputCountStat:  stats.NOP.NewTaggedStat("router_transform_input_count", stats.CountType, stats.Tags{"destType": "some_dest_type"}),
			routerTransformOutputCountStat: stats.NOP.NewTaggedStat("router_transform_output_count", stats.CountType, stats.Tags{"destType": "some_dest_type"}),
			isOAuthDestination:             false,
			reloadableConfig: &reloadableConfig{
				oauthV2Enabled: config.GetReloadableBoolVar(true),
			},
		},
	}
	var limiterWg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	defer limiterWg.Wait()
	defer cancel()
	worker.rt.limiter.transform = kitsync.NewLimiter(ctx, &limiterWg, "transform", math.MaxInt, stats.Default)
	worker.rt.limiter.stats.transform = partition.NewStats()

	routerJobs := []types.RouterJobT{
		{
			Destination: backendconfig.DestinationT{
				ID: "d1",
			},
			Message: json.RawMessage(`{"event": "d1-test1"}`),
			JobMetadata: types.JobMetadataT{
				JobID: 1,
			},
		},
		{
			Destination: backendconfig.DestinationT{
				ID: "d2",
			},
			Message: json.RawMessage(`{"event": "d2-test2"}`),
			JobMetadata: types.JobMetadataT{
				JobID: 2,
			},
		},
	}
	mockTransformer.EXPECT().Transform(transformer.ROUTER_TRANSFORM, &types.TransformMessageT{
		Data:     routerJobs,
		DestType: worker.rt.destType,
	}).Return([]types.DestinationJobT{
		{
			Destination: backendconfig.DestinationT{
				ID: "d1",
			},
			Message: json.RawMessage(`{"event": ["d1-test1"]}`),
			JobMetadataArray: []types.JobMetadataT{
				{
					JobID: 1,
				},
			},
		},
		{
			Destination: backendconfig.DestinationT{
				ID: "d2",
			},
			Message: json.RawMessage(`{"event": [ "d2-test2"]}`),
			JobMetadataArray: []types.JobMetadataT{
				{
					JobID: 2,
				},
			},
		},
	})

	worker.transform(routerJobs)
}
