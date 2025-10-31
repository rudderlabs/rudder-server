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
func createTestWorker(destType string, transformProxy bool, stat stats.Stats) *worker {
	return &worker{
		rt: &Handle{
			destType: destType,
			reloadableConfig: &reloadableConfig{
				transformerProxy: config.SingleValueLoader(transformProxy),
			},
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
				workerBuffer:    newSimpleWorkerBuffer(1),
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
				workerBuffer:    newSimpleWorkerBuffer(1),
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

// Helper function to create test destination job
func createTestDestinationJob(destID, workspaceID string, jobMetadata []types.JobMetadataT) types.DestinationJobT {
	return types.DestinationJobT{
		Destination: backendconfig.DestinationT{
			ID:          destID,
			WorkspaceID: workspaceID,
		},
		JobMetadataArray: jobMetadata,
	}
}

// Helper function to create test post parameters
func createTestPostParams(endpointPath, requestMethod string) integrations.PostParametersT {
	return integrations.PostParametersT{
		EndpointPath:  endpointPath,
		RequestMethod: requestMethod,
	}
}

// Helper function to find metric by name and tags
func findMetricByNameAndTags(metrics []memstats.Metric, name string, expectedTags stats.Tags) (memstats.Metric, bool) {
	for _, metric := range metrics {
		if metric.Name == name {
			tagsMatch := true
			for key, expectedValue := range expectedTags {
				if metric.Tags[key] != expectedValue {
					tagsMatch = false
					break
				}
			}
			if tagsMatch {
				return metric, true
			}
		}
	}
	return memstats.Metric{}, false
}

// Helper function to verify metrics are recorded
func verifyMetricsRecorded(t *testing.T, allMetrics []memstats.Metric, expectedLabels stats.Tags) {
	t.Helper()

	latencyMetric, foundLatency := findMetricByNameAndTags(allMetrics, "transformer_outgoing_request_latency", expectedLabels)
	countMetric, foundCount := findMetricByNameAndTags(allMetrics, "transformer_outgoing_request_count", expectedLabels)

	require.True(t, foundLatency, "Expected metric 'transformer_outgoing_request_latency' with matching tags to be recorded. Available metrics: %+v", allMetrics)
	require.True(t, foundCount, "Expected metric 'transformer_outgoing_request_count' with matching tags to be recorded. Available metrics: %+v", allMetrics)

	require.Equal(t, "transformer_outgoing_request_latency", latencyMetric.Name)
	require.Equal(t, expectedLabels, latencyMetric.Tags)
	require.Equal(t, "transformer_outgoing_request_count", countMetric.Name)
	require.Equal(t, expectedLabels, countMetric.Tags)
}

// Helper function to verify no metrics are recorded
func verifyNoMetricsRecorded(t *testing.T, allMetrics []memstats.Metric) {
	t.Helper()

	var foundLatencyMetric, foundCountMetric bool
	for _, metric := range allMetrics {
		if metric.Name == "transformer_outgoing_request_latency" {
			foundLatencyMetric = true
		}
		if metric.Name == "transformer_outgoing_request_count" {
			foundCountMetric = true
		}
	}
	require.False(t, foundLatencyMetric, "Expected no 'transformer_outgoing_request_latency' metric to be recorded")
	require.False(t, foundCountMetric, "Expected no 'transformer_outgoing_request_count' metric to be recorded")
}

func TestWorker_recordTransformerOutgoingRequestMetrics(t *testing.T) {
	testCases := []struct {
		name             string
		postParams       integrations.PostParametersT
		destinationJob   types.DestinationJobT
		statusCode       int
		duration         time.Duration
		transformerProxy bool
		expectedLabels   stats.Tags
		shouldEmit       bool
	}{
		{
			name:       "complete data with endpoint path and transformer proxy disabled",
			postParams: createTestPostParams("/api/track", "POST"),
			destinationJob: createTestDestinationJob("dest-123", "ws-456", []types.JobMetadataT{
				{WorkspaceID: "ws-456"},
			}),
			statusCode:       200,
			duration:         150 * time.Millisecond,
			transformerProxy: false,
			expectedLabels: stats.Tags{
				"destType":         "TEST_DEST",
				"endpointPath":     "/api/track",
				"statusCode":       "200",
				"transformerProxy": "false",
				"requestMethod":    "POST",
				"module":           "router",
				"workspaceId":      "ws-456",
				"destinationId":    "dest-123",
			},
			shouldEmit: true,
		},
		{
			name:       "complete data with endpoint path and transformer proxy enabled",
			postParams: createTestPostParams("/api/track", "POST"),
			destinationJob: createTestDestinationJob("dest-123", "ws-456", []types.JobMetadataT{
				{WorkspaceID: "ws-456"},
			}),
			statusCode:       201,
			duration:         200 * time.Millisecond,
			transformerProxy: true,
			expectedLabels: stats.Tags{
				"destType":         "TEST_DEST",
				"endpointPath":     "/api/track",
				"statusCode":       "201",
				"transformerProxy": "true",
				"requestMethod":    "POST",
				"module":           "router",
				"workspaceId":      "ws-456",
				"destinationId":    "dest-123",
			},
			shouldEmit: true,
		},
		{
			name:       "empty endpoint path",
			postParams: createTestPostParams("", "PATCH"),
			destinationJob: createTestDestinationJob("dest-patch", "ws-patch", []types.JobMetadataT{
				{WorkspaceID: "ws-patch"},
			}),
			statusCode:       422,
			duration:         75 * time.Millisecond,
			transformerProxy: true,
			expectedLabels: stats.Tags{
				"destType":         "TEST_DEST",
				"endpointPath":     "default",
				"statusCode":       "422",
				"transformerProxy": "true",
				"requestMethod":    "PATCH",
				"module":           "router",
				"workspaceId":      "ws-patch",
				"destinationId":    "dest-patch",
			},
			shouldEmit: true,
		},
		{
			name:             "empty job metadata array with endpoint path",
			postParams:       createTestPostParams("/api/identify", "PUT"),
			destinationJob:   createTestDestinationJob("dest-789", "", []types.JobMetadataT{}),
			statusCode:       500,
			duration:         100 * time.Millisecond,
			transformerProxy: false,
			expectedLabels: stats.Tags{
				"destType":         "TEST_DEST",
				"endpointPath":     "/api/identify",
				"statusCode":       "500",
				"transformerProxy": "false",
				"requestMethod":    "PUT",
				"module":           "router",
				"workspaceId":      "",
				"destinationId":    "dest-789",
			},
			shouldEmit: true,
		},
		{
			name:       "different HTTP methods and status codes",
			postParams: createTestPostParams("/api/page", "PATCH"),
			destinationJob: createTestDestinationJob("dest-patch", "ws-patch", []types.JobMetadataT{
				{WorkspaceID: "ws-patch"},
			}),
			statusCode:       422,
			duration:         75 * time.Millisecond,
			transformerProxy: true,
			expectedLabels: stats.Tags{
				"destType":         "TEST_DEST",
				"endpointPath":     "/api/page",
				"statusCode":       "422",
				"transformerProxy": "true",
				"requestMethod":    "PATCH",
				"module":           "router",
				"workspaceId":      "ws-patch",
				"destinationId":    "dest-patch",
			},
			shouldEmit: true,
		},
	}

	// Test the convertDeliveryMetricLabelToStatTags method
	t.Run("ToStatTags conversion", func(t *testing.T) {
		t.Run("basic conversion", func(t *testing.T) {
			labels := deliveryMetricLabels{
				DestType:         "TEST_DEST",
				EndpointPath:     "/api/test",
				StatusCode:       200,
				TransformerProxy: false,
				RequestMethod:    "POST",
				Module:           "router",
				WorkspaceID:      "ws-123",
				DestinationID:    "dest-456",
			}

			expectedTags := stats.Tags{
				"destType":         "TEST_DEST",
				"endpointPath":     "/api/test",
				"statusCode":       "200",
				"transformerProxy": "false",
				"requestMethod":    "POST",
				"module":           "router",
				"workspaceId":      "ws-123",
				"destinationId":    "dest-456",
			}

			result := labels.ToStatTags()
			require.Equal(t, expectedTags, result)
		})

		t.Run("with transformer proxy enabled", func(t *testing.T) {
			labels := deliveryMetricLabels{
				DestType:         "TEST_DEST",
				EndpointPath:     "/api/proxy",
				StatusCode:       201,
				TransformerProxy: true,
				RequestMethod:    "PUT",
				Module:           "router",
				WorkspaceID:      "ws-proxy",
				DestinationID:    "dest-proxy",
			}

			expectedTags := stats.Tags{
				"destType":         "TEST_DEST",
				"endpointPath":     "/api/proxy",
				"statusCode":       "201",
				"transformerProxy": "true",
				"requestMethod":    "PUT",
				"module":           "router",
				"workspaceId":      "ws-proxy",
				"destinationId":    "dest-proxy",
			}

			result := labels.ToStatTags()
			require.Equal(t, expectedTags, result)
		})
	})

	// Test caching behavior
	t.Run("caching behavior", func(t *testing.T) {
		stat, err := memstats.New()
		require.NoError(t, err)
		worker := createTestWorker("TEST_DEST", true, stat)

		labels := deliveryMetricLabels{
			DestType:         "TEST_DEST",
			EndpointPath:     "/api/cache",
			TransformerProxy: true,
			StatusCode:       200,
			RequestMethod:    "GET",
			Module:           "router",
			WorkspaceID:      "ws-cache",
			DestinationID:    "dest-cache",
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
			DestType:         "TEST_DEST",
			EndpointPath:     "/api/different",
			TransformerProxy: true,
			StatusCode:       404,
			RequestMethod:    "PUT",
			Module:           "router",
			WorkspaceID:      "ws-diff",
			DestinationID:    "dest-diff",
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
		emptyLabels := deliveryMetricLabels{}

		expectedEmptyTags := stats.Tags{
			"destType":         "",
			"endpointPath":     "",
			"transformerProxy": "false",
			"statusCode":       "0",
			"requestMethod":    "",
			"module":           "",
			"workspaceId":      "",
			"destinationId":    "",
		}

		result := emptyLabels.ToStatTags()
		require.Equal(t, expectedEmptyTags, result)

		// Test with special characters in strings
		specialLabels := deliveryMetricLabels{
			DestType:         "test-dest_with.special:chars",
			EndpointPath:     "/api/test?param=value&other=123",
			StatusCode:       200,
			TransformerProxy: false,
			RequestMethod:    "POST",
			Module:           "router",
			WorkspaceID:      "ws-123_456",
			DestinationID:    "dest-789",
		}

		expectedSpecialTags := stats.Tags{
			"destType":         "test-dest_with.special:chars",
			"endpointPath":     "/api/test?param=value&other=123",
			"statusCode":       "200",
			"transformerProxy": "false",
			"requestMethod":    "POST",
			"module":           "router",
			"workspaceId":      "ws-123_456",
			"destinationId":    "dest-789",
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
			worker := createTestWorker("TEST_DEST", tc.transformerProxy, stat)

			// Call the method under test
			worker.recordTransformerOutgoingRequestMetrics(tc.postParams, tc.destinationJob, tc.statusCode, tc.duration)

			// Get all recorded metrics
			allMetrics := stat.GetAll()

			if tc.shouldEmit {
				verifyMetricsRecorded(t, allMetrics, tc.expectedLabels)
			} else {
				verifyNoMetricsRecorded(t, allMetrics)
			}
		})
	}
}
