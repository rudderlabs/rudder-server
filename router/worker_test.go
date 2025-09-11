package router

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

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
	routerutils "github.com/rudderlabs/rudder-server/router/utils"
	"github.com/rudderlabs/rudder-server/utils/cache"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
	kitsync "github.com/rudderlabs/rudder-go-kit/sync"
	mocksRouter "github.com/rudderlabs/rudder-server/mocks/router"
	mocksTransformer "github.com/rudderlabs/rudder-server/mocks/router/transformer"
	destinationdebugger "github.com/rudderlabs/rudder-server/services/debugger/destination"
	"github.com/rudderlabs/rudder-server/services/rmetrics"
	"github.com/rudderlabs/rudder-server/services/rsources"
	transformerFeaturesService "github.com/rudderlabs/rudder-server/services/transformer"
	"github.com/rudderlabs/rudder-server/services/transientsource"
)

// createTestWorker creates a worker instance for testing with properly initialized StatsCache instances
func createTestWorker(destType string, stat stats.Stats) *worker {
	return &worker{
		rt: &Handle{
			destType: destType,
		},
		deliveryLatencyStatsCache: cache.NewStatsCache(func(labels deliveryMetricLabels) stats.Measurement {
			return stat.NewTaggedStat("transformer_outgoing_request_latency", stats.TimerType, labels.ToStatTags())
		}),
		deliveryCountStatsCache: cache.NewStatsCache(func(labels deliveryMetricLabels) stats.Measurement {
			return stat.NewTaggedStat("transformer_outgoing_request_count", stats.CountType, labels.ToStatTags())
		}),
	}
}

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

			router.Setup(
				gaDestinationDefinition,
				logger.NOP,
				conf,
				c.mockBackendConfig,
				c.mockRouterJobsDB,
				transientsource.NewEmptyService(),
				rsources.NewNoOpService(),
				transformerFeaturesService.NewNoOpService(),
				destinationdebugger.NewNoOpService(),
				throttler.NewNoOpThrottlerFactory(),
				rmetrics.NewPendingEventsRegistry(),
			)
			router.transformer = mockTransformer

			<-router.backendConfigInitialized
			worker := &worker{
				logger:          router.logger.Child("w-0"),
				partition:       "partition",
				id:              1,
				inputCh:         make(chan workerJob, 1),
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

			router.Setup(
				gaDestinationDefinition,
				logger.NOP,
				conf,
				c.mockBackendConfig,
				c.mockRouterJobsDB,
				transientsource.NewEmptyService(),
				rsources.NewNoOpService(),
				transformerFeaturesService.NewNoOpService(),
				destinationdebugger.NewNoOpService(),
				throttler.NewNoOpThrottlerFactory(),
				rmetrics.NewPendingEventsRegistry(),
			)
			router.transformer = mockTransformer

			<-router.backendConfigInitialized
			worker := &worker{
				logger:          router.logger.Child("w-0"),
				partition:       "partition",
				id:              1,
				inputCh:         make(chan workerJob, 1),
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
			batchSizeHistogramStat:         stats.NOP.NewTaggedStat("router_batch_size_histogram", stats.HistogramType, stats.Tags{"destType": "some_dest_type"}),
			routerTransformInputCountStat:  stats.NOP.NewTaggedStat("router_transform_input_count", stats.CountType, stats.Tags{"destType": "some_dest_type"}),
			routerTransformOutputCountStat: stats.NOP.NewTaggedStat("router_transform_output_count", stats.CountType, stats.Tags{"destType": "some_dest_type"}),
			isOAuthDestination:             true,
			reloadableConfig:               &reloadableConfig{},
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
			batchSizeHistogramStat:         stats.NOP.NewTaggedStat("router_batch_size_histogram", stats.HistogramType, stats.Tags{"destType": "some_dest_type"}),
			routerTransformInputCountStat:  stats.NOP.NewTaggedStat("router_transform_input_count", stats.CountType, stats.Tags{"destType": "some_dest_type"}),
			routerTransformOutputCountStat: stats.NOP.NewTaggedStat("router_transform_output_count", stats.CountType, stats.Tags{"destType": "some_dest_type"}),
			isOAuthDestination:             false,
			reloadableConfig:               &reloadableConfig{},
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

func TestWorker_recordTransformerOutgoingRequestMetrics(t *testing.T) {
	testCases := []struct {
		name           string
		postParams     integrations.PostParametersT
		destinationJob types.DestinationJobT
		resp           *routerutils.SendPostResponse
		duration       time.Duration
		expectedLabels stats.Tags
		shouldEmit     bool
	}{
		{
			name: "complete data with endpoint path",
			postParams: integrations.PostParametersT{
				EndpointPath:  "/api/track",
				RequestMethod: "POST",
			},
			destinationJob: types.DestinationJobT{
				Destination: backendconfig.DestinationT{
					ID:          "dest-123",
					WorkspaceID: "ws-456",
				},
				JobMetadataArray: []types.JobMetadataT{
					{
						WorkspaceID: "ws-456",
					},
				},
			},
			resp: &routerutils.SendPostResponse{
				StatusCode: 200,
			},
			duration: 150 * time.Millisecond,
			expectedLabels: stats.Tags{
				"destType":      "TEST_DEST",
				"endpointPath":  "/api/track",
				"statusCode":    "200",
				"requestMethod": "POST",
				"module":        "router",
				"workspaceId":   "ws-456",
				"destinationId": "dest-123",
			},
			shouldEmit: true,
		},
		{
			name: "empty endpoint path should not emit metric",
			postParams: integrations.PostParametersT{
				EndpointPath:  "",
				RequestMethod: "GET",
			},
			destinationJob: types.DestinationJobT{
				Destination: backendconfig.DestinationT{
					ID:          "dest-456",
					WorkspaceID: "ws-789",
				},
				JobMetadataArray: []types.JobMetadataT{
					{
						WorkspaceID: "ws-789",
					},
				},
			},
			resp: &routerutils.SendPostResponse{
				StatusCode: 400,
			},
			duration:   200 * time.Millisecond,
			shouldEmit: false,
		},
		{
			name: "empty job metadata array with endpoint path",
			postParams: integrations.PostParametersT{
				EndpointPath:  "/api/identify",
				RequestMethod: "PUT",
			},
			destinationJob: types.DestinationJobT{
				Destination: backendconfig.DestinationT{
					ID:          "dest-789",
					WorkspaceID: "",
				},
				JobMetadataArray: []types.JobMetadataT{},
			},
			resp: &routerutils.SendPostResponse{
				StatusCode: 500,
			},
			duration: 100 * time.Millisecond,
			expectedLabels: stats.Tags{
				"destType":      "TEST_DEST",
				"endpointPath":  "/api/identify",
				"statusCode":    "500",
				"requestMethod": "PUT",
				"module":        "router",
				"workspaceId":   "",
				"destinationId": "dest-789",
			},
			shouldEmit: true,
		},
	}

	// Test the convertDeliveryMetricLabelToStatTags method
	t.Run("convertDeliveryMetricLabelToStatTags", func(t *testing.T) {
		labels := deliveryMetricLabels{
			DestType:      "TEST_DEST",
			EndpointPath:  "/api/test",
			StatusCode:    "200",
			RequestMethod: "POST",
			Module:        "router",
			WorkspaceID:   "ws-123",
			DestinationID: "dest-456",
		}

		expectedTags := stats.Tags{
			"destType":      "TEST_DEST",
			"endpointPath":  "/api/test",
			"statusCode":    "200",
			"requestMethod": "POST",
			"module":        "router",
			"workspaceId":   "ws-123",
			"destinationId": "dest-456",
		}

		result := labels.ToStatTags()
		require.Equal(t, expectedTags, result)
	})

	// Test caching behavior
	t.Run("caching behavior", func(t *testing.T) {
		stat, err := memstats.New()
		require.NoError(t, err)
		worker := createTestWorker("TEST_DEST", stat)

		labels := deliveryMetricLabels{
			DestType:      "TEST_DEST",
			EndpointPath:  "/api/cache",
			StatusCode:    "200",
			RequestMethod: "GET",
			Module:        "router",
			WorkspaceID:   "ws-cache",
			DestinationID: "dest-cache",
		}

		// First call should create new stats
		latencyStat1 := worker.deliveryLatencyStatsCache.Get(labels)
		countStat1 := worker.deliveryCountStatsCache.Get(labels)

		// Second call with same labels should return cached stats
		latencyStat2 := worker.deliveryLatencyStatsCache.Get(labels)
		countStat2 := worker.deliveryCountStatsCache.Get(labels)

		// Should be the same objects (cached)
		require.Equal(t, latencyStat1, latencyStat2)
		require.Equal(t, countStat1, countStat2)

		// Cache should have one entry for each type (StatsCache doesn't expose length)
		// We can verify caching by checking that the same object is returned

		// Test that different labels create different cache entries
		differentLabels := deliveryMetricLabels{
			DestType:      "TEST_DEST",
			EndpointPath:  "/api/different",
			StatusCode:    "404",
			RequestMethod: "PUT",
			Module:        "router",
			WorkspaceID:   "ws-diff",
			DestinationID: "dest-diff",
		}

		worker.deliveryLatencyStatsCache.Get(differentLabels)
		worker.deliveryCountStatsCache.Get(differentLabels)

		// Cache should now have two entries for each type (StatsCache doesn't expose length)
		// We can verify by getting the stats again and ensuring they're different objects
		latencyStat3 := worker.deliveryLatencyStatsCache.Get(differentLabels)
		countStat3 := worker.deliveryCountStatsCache.Get(differentLabels)

		// Should be different objects from the first set
		require.NotEqual(t, latencyStat1, latencyStat3)
		require.NotEqual(t, countStat1, countStat3)
	})

	// Test ToStatTags method with edge cases
	t.Run("ToStatTags edge cases", func(t *testing.T) {
		// Test with empty strings
		emptyLabels := deliveryMetricLabels{
			DestType:      "",
			EndpointPath:  "",
			StatusCode:    "",
			RequestMethod: "",
			Module:        "",
			WorkspaceID:   "",
			DestinationID: "",
		}

		expectedEmptyTags := stats.Tags{
			"destType":      "",
			"endpointPath":  "",
			"statusCode":    "",
			"requestMethod": "",
			"module":        "",
			"workspaceId":   "",
			"destinationId": "",
		}

		result := emptyLabels.ToStatTags()
		require.Equal(t, expectedEmptyTags, result)

		// Test with special characters in strings
		specialLabels := deliveryMetricLabels{
			DestType:      "test-dest_with.special:chars",
			EndpointPath:  "/api/test?param=value&other=123",
			StatusCode:    "200",
			RequestMethod: "POST",
			Module:        "router",
			WorkspaceID:   "ws-123_456",
			DestinationID: "dest-789",
		}

		expectedSpecialTags := stats.Tags{
			"destType":      "test-dest_with.special:chars",
			"endpointPath":  "/api/test?param=value&other=123",
			"statusCode":    "200",
			"requestMethod": "POST",
			"module":        "router",
			"workspaceId":   "ws-123_456",
			"destinationId": "dest-789",
		}

		result = specialLabels.ToStatTags()
		require.Equal(t, expectedSpecialTags, result)
	})

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create a memstats store for testing
			stat, err := memstats.New()
			require.NoError(t, err)

			// Create worker with mock router
			worker := createTestWorker("TEST_DEST", stat)

			// Call the method under test
			worker.recordTransformerOutgoingRequestMetrics(tc.postParams, tc.destinationJob, tc.resp, tc.duration)

			// Get all recorded metrics
			allMetrics := stat.GetAll()

			if tc.shouldEmit {
				// Verify both metrics were recorded by checking the memstats store
				// Find latency metric
				var foundLatencyMetric memstats.Metric
				var foundCountMetric memstats.Metric

				for _, metric := range allMetrics {
					if metric.Name == "transformer_outgoing_request_latency" {
						// Check if tags match
						tagsMatch := true
						for key, expectedValue := range tc.expectedLabels {
							if metric.Tags[key] != expectedValue {
								tagsMatch = false
								break
							}
						}
						if tagsMatch {
							foundLatencyMetric = metric
						}
					}
					if metric.Name == "transformer_outgoing_request_count" {
						// Check if tags match
						tagsMatch := true
						for key, expectedValue := range tc.expectedLabels {
							if metric.Tags[key] != expectedValue {
								tagsMatch = false
								break
							}
						}
						if tagsMatch {
							foundCountMetric = metric
						}
					}
				}

				require.NotEmpty(t, foundLatencyMetric.Name, "Expected metric 'transformer_outgoing_request_latency' with matching tags to be recorded. Available metrics: %+v", allMetrics)
				require.Equal(t, "transformer_outgoing_request_latency", foundLatencyMetric.Name)
				require.Equal(t, tc.expectedLabels, foundLatencyMetric.Tags)

				require.NotEmpty(t, foundCountMetric.Name, "Expected metric 'transformer_outgoing_request_count' with matching tags to be recorded. Available metrics: %+v", allMetrics)
				require.Equal(t, "transformer_outgoing_request_count", foundCountMetric.Name)
				require.Equal(t, tc.expectedLabels, foundCountMetric.Tags)
			} else {
				// Verify no metrics were recorded
				var foundLatencyMetric bool
				var foundCountMetric bool
				for _, metric := range allMetrics {
					if metric.Name == "transformer_outgoing_request_latency" {
						foundLatencyMetric = true
					}
					if metric.Name == "transformer_outgoing_request_count" {
						foundCountMetric = true
					}
				}
				require.False(t, foundLatencyMetric, "Expected no 'transformer_outgoing_request_latency' metric to be recorded when EndpointPath is empty")
				require.False(t, foundCountMetric, "Expected no 'transformer_outgoing_request_count' metric to be recorded when EndpointPath is empty")
			}
		})
	}
}
