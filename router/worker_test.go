package router

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
	kitsync "github.com/rudderlabs/rudder-go-kit/sync"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/enterprise/reporting"
	"github.com/rudderlabs/rudder-server/jobsdb"
	mocksJobsDB "github.com/rudderlabs/rudder-server/mocks/jobsdb"
	mocksRouter "github.com/rudderlabs/rudder-server/mocks/router"
	mocksTransformer "github.com/rudderlabs/rudder-server/mocks/router/transformer"
	"github.com/rudderlabs/rudder-server/processor/integrations"
	"github.com/rudderlabs/rudder-server/router/internal/partition"
	"github.com/rudderlabs/rudder-server/router/throttler"
	"github.com/rudderlabs/rudder-server/router/transformer"
	"github.com/rudderlabs/rudder-server/router/types"
	routerutils "github.com/rudderlabs/rudder-server/router/utils"
	destinationdebugger "github.com/rudderlabs/rudder-server/services/debugger/destination"
	"github.com/rudderlabs/rudder-server/services/rsources"
	transformerFeaturesService "github.com/rudderlabs/rudder-server/services/transformer"
	"github.com/rudderlabs/rudder-server/services/transientsource"
	"github.com/rudderlabs/rudder-server/utils/cache"
	txutils "github.com/rudderlabs/rudder-server/utils/tx"
	utilTypes "github.com/rudderlabs/rudder-server/utils/types"
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

type recordingReporter struct {
	metrics []*utilTypes.PUReportedMetric
}

func (r *recordingReporter) Report(_ context.Context, metrics []*utilTypes.PUReportedMetric, _ *txutils.Tx) error {
	r.metrics = append(r.metrics, metrics...)
	return nil
}

type v1TransformerProxyFeaturesService struct{}

func (v1TransformerProxyFeaturesService) Regulations() []string { return nil }
func (v1TransformerProxyFeaturesService) SourceTransformerVersion() string {
	return transformerFeaturesService.V2
}
func (v1TransformerProxyFeaturesService) RouterTransform(string) bool { return false }
func (v1TransformerProxyFeaturesService) TransformerProxyVersion() string {
	return transformerFeaturesService.V1
}
func (v1TransformerProxyFeaturesService) SupportDestTransformCompactedPayloadV1() bool {
	return false
}
func (v1TransformerProxyFeaturesService) Wait() chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}

// TestDeliveredWithWarningsEnabled covers the OR between the destination-definition capability
// (the GA switch) and the per-workspace controlled-rollout allow-list.
func TestDeliveredWithWarningsEnabled(t *testing.T) {
	const (
		enabledWorkspace = "workspace-enabled"
		otherWorkspace   = "workspace-other"
	)

	newHandle := func(destDefSupports bool, allowList ...string) *Handle {
		rt := &Handle{}
		rt.supportsDeliveredWithWarnings.Store(destDefSupports)
		rt.deliveredWithWarningsEnabledForWorkspace = func(workspaceID string) bool {
			return slices.Contains(allowList, workspaceID)
		}
		return rt
	}

	t.Run("destination definition supports it, workspace not allow-listed", func(t *testing.T) {
		require.True(t, newHandle(true).deliveredWithWarningsEnabled(otherWorkspace))
	})

	t.Run("destination definition does not support it, workspace allow-listed", func(t *testing.T) {
		require.True(t, newHandle(false, enabledWorkspace).deliveredWithWarningsEnabled(enabledWorkspace))
	})

	t.Run("both enabled", func(t *testing.T) {
		require.True(t, newHandle(true, enabledWorkspace).deliveredWithWarningsEnabled(enabledWorkspace))
	})

	t.Run("neither enabled", func(t *testing.T) {
		require.False(t, newHandle(false, enabledWorkspace).deliveredWithWarningsEnabled(otherWorkspace))
	})
}

// TestGateDeliveredWithWarning covers the 296 -> 200 downgrade applied per job, per workspace.
func TestGateDeliveredWithWarning(t *testing.T) {
	const (
		gateDestType     = "BRAZE"
		downgradeMetric  = "router_status_downgraded_count"
		enabledWorkspace = "workspace-enabled"
		otherWorkspace   = "workspace-other"
	)
	downgradeTags := stats.Tags{
		"destType": gateDestType,
		"from":     strconv.Itoa(utilTypes.DeliveredWithWarningCode),
		"to":       strconv.Itoa(utilTypes.SuccessEventCode),
	}

	newGateWorker := func(t *testing.T, destDefSupports bool, allowList ...string) (*worker, *memstats.Store) {
		t.Helper()
		statsStore, err := memstats.New()
		require.NoError(t, err)
		rt := &Handle{
			destType: gateDestType,
			statusDowngradedStat: func(from, to int) stats.Counter {
				return statsStore.NewTaggedStat(downgradeMetric, stats.CountType, stats.Tags{
					"destType": gateDestType,
					"from":     strconv.Itoa(from),
					"to":       strconv.Itoa(to),
				})
			},
		}
		rt.supportsDeliveredWithWarnings.Store(destDefSupports)
		rt.deliveredWithWarningsEnabledForWorkspace = func(workspaceID string) bool {
			return slices.Contains(allowList, workspaceID)
		}
		return &worker{rt: rt, logger: logger.NOP}, statsStore
	}

	downgrades := func(statsStore *memstats.Store) float64 {
		if m := statsStore.Get(downgradeMetric, downgradeTags); m != nil {
			return m.LastValue()
		}
		return 0
	}

	jobsOf := func(metadata ...types.JobMetadataT) types.DestinationJobT {
		return types.DestinationJobT{JobMetadataArray: metadata}
	}
	jobMeta := func(jobID int64, workspaceID string) types.JobMetadataT {
		return types.JobMetadataT{JobID: jobID, WorkspaceID: workspaceID}
	}

	t.Run("allow-listed workspace keeps 296", func(t *testing.T) {
		w, statsStore := newGateWorker(t, false, enabledWorkspace)
		codes := map[int64]int{1: utilTypes.DeliveredWithWarningCode}
		w.gateDeliveredWithWarning(jobsOf(jobMeta(1, enabledWorkspace)), codes)
		require.Equal(t, utilTypes.DeliveredWithWarningCode, codes[1])
		require.Zero(t, downgrades(statsStore))
	})

	t.Run("destination definition support keeps 296 for any workspace", func(t *testing.T) {
		w, statsStore := newGateWorker(t, true)
		codes := map[int64]int{1: utilTypes.DeliveredWithWarningCode}
		w.gateDeliveredWithWarning(jobsOf(jobMeta(1, otherWorkspace)), codes)
		require.Equal(t, utilTypes.DeliveredWithWarningCode, codes[1])
		require.Zero(t, downgrades(statsStore))
	})

	t.Run("non allow-listed workspace downgrades to 200", func(t *testing.T) {
		w, statsStore := newGateWorker(t, false, enabledWorkspace)
		codes := map[int64]int{1: utilTypes.DeliveredWithWarningCode}
		w.gateDeliveredWithWarning(jobsOf(jobMeta(1, otherWorkspace)), codes)
		require.Equal(t, http.StatusOK, codes[1])
		require.EqualValues(t, 1, downgrades(statsStore))
	})

	t.Run("batch spanning workspaces downgrades only the non allow-listed job", func(t *testing.T) {
		w, statsStore := newGateWorker(t, false, enabledWorkspace)
		codes := map[int64]int{
			1: utilTypes.DeliveredWithWarningCode,
			2: utilTypes.DeliveredWithWarningCode,
		}
		w.gateDeliveredWithWarning(jobsOf(jobMeta(1, enabledWorkspace), jobMeta(2, otherWorkspace)), codes)
		require.Equal(t, utilTypes.DeliveredWithWarningCode, codes[1])
		require.Equal(t, http.StatusOK, codes[2])
		require.EqualValues(t, 1, downgrades(statsStore))
	})

	t.Run("only 296 is rewritten in a mixed-code batch", func(t *testing.T) {
		w, statsStore := newGateWorker(t, false)
		codes := map[int64]int{
			1: http.StatusOK,
			2: utilTypes.DeliveredWithWarningCode,
			3: http.StatusBadRequest,
			4: http.StatusInternalServerError,
		}
		w.gateDeliveredWithWarning(jobsOf(
			jobMeta(1, otherWorkspace), jobMeta(2, otherWorkspace),
			jobMeta(3, otherWorkspace), jobMeta(4, otherWorkspace),
		), codes)
		require.Equal(t, map[int64]int{
			1: http.StatusOK,
			2: http.StatusOK,
			3: http.StatusBadRequest,
			4: http.StatusInternalServerError,
		}, codes)
		require.EqualValues(t, 1, downgrades(statsStore))
	})

	t.Run("duplicate job metadata downgrades once", func(t *testing.T) {
		w, statsStore := newGateWorker(t, false)
		codes := map[int64]int{1: utilTypes.DeliveredWithWarningCode}
		w.gateDeliveredWithWarning(jobsOf(jobMeta(1, otherWorkspace), jobMeta(1, otherWorkspace)), codes)
		require.Equal(t, http.StatusOK, codes[1])
		require.EqualValues(t, 1, downgrades(statsStore))
	})
}

// TestPostStatusOnResponseQStoreDeliveredWithWarningPayload verifies that, on the success path,
// only a Delivered-with-Warning (296) status reports the transformed delivery body instead of the
// router input payload, gated by the storeDeliveredWithWarningPayload flag.
func TestPostStatusOnResponseQStoreDeliveredWithWarningPayload(t *testing.T) {
	const (
		destType     = "BRAZE"
		inputPayload = `{"input":"event"}`
		deliveryBody = `{"delivery":"transformed batch body"}`
	)

	newTestWorker := func(storePayload bool) *worker {
		return &worker{
			logger: logger.NOP,
			rt: &Handle{
				destType:                         destType,
				responseQ:                        make(chan workerJobStatus, 1),
				reportJobsdbPayload:              config.SingleValueLoader(true),
				storeDeliveredWithWarningPayload: config.SingleValueLoader(storePayload),
			},
		}
	}

	report := func(w *worker, statusCode int, message string) workerJobStatus {
		destinationJob := &types.DestinationJobT{Message: json.RawMessage(message)}
		metadata := &types.JobMetadataT{JobT: &jobsdb.JobT{EventPayload: json.RawMessage(inputPayload)}}
		status := &jobsdb.JobStatusT{ErrorResponse: json.RawMessage(`{}`)}
		w.postStatusOnResponseQ(statusCode, destinationJob, "application/json", metadata, status, "")
		return <-w.rt.responseQ
	}

	t.Run("296 reports the transformed delivery body and marks payloadStage=delivery", func(t *testing.T) {
		got := report(newTestWorker(true), utilTypes.DeliveredWithWarningCode, deliveryBody)
		require.JSONEq(t, deliveryBody, string(got.payload))
		require.Equal(t, jobsdb.Succeeded.State, got.status.JobState)
		require.Contains(t, string(got.status.ErrorResponse), `"payloadStage":"delivery"`)
	})

	t.Run("200 keeps the router input payload with no delivery marker", func(t *testing.T) {
		got := report(newTestWorker(true), utilTypes.SuccessEventCode, deliveryBody)
		require.JSONEq(t, inputPayload, string(got.payload))
		require.Equal(t, jobsdb.Succeeded.State, got.status.JobState)
		require.NotContains(t, string(got.status.ErrorResponse), "payloadStage")
	})

	t.Run("296 with storeDeliveredWithWarningPayload off falls back to the input payload", func(t *testing.T) {
		got := report(newTestWorker(false), utilTypes.DeliveredWithWarningCode, deliveryBody)
		require.JSONEq(t, inputPayload, string(got.payload))
		require.NotContains(t, string(got.status.ErrorResponse), "payloadStage")
	})

	t.Run("filter event code stays Filtered with the input payload", func(t *testing.T) {
		got := report(newTestWorker(true), utilTypes.FilterEventCode, deliveryBody)
		require.JSONEq(t, inputPayload, string(got.payload))
		require.Equal(t, jobsdb.Filtered.State, got.status.JobState)
	})
}

func TestBrazeTransformerProxyStatusContract(t *testing.T) {
	const (
		destType   = "BRAZE"
		destID     = "braze-destination"
		workspace  = "workspace-braze"
		sourceID   = "source-braze"
		delivery   = `{"braze":"delivery-payload"}`
		inputEvent = `{"braze":"input-event"}`
	)

	newJob := func(jobID int64, eventName string) *jobsdb.JobT {
		return &jobsdb.JobT{
			JobID:        jobID,
			UserID:       fmt.Sprintf("user-%d", jobID),
			WorkspaceId:  workspace,
			EventPayload: json.RawMessage(inputEvent),
			Parameters:   json.RawMessage(`{"event_name":"braze-contract","event_type":"track"}`),
		}
	}
	newMetadata := func(job *jobsdb.JobT, eventName string) types.JobMetadataT {
		return types.JobMetadataT{
			UserID:        job.UserID,
			JobID:         job.JobID,
			SourceID:      sourceID,
			DestinationID: destID,
			WorkspaceID:   workspace,
			JobT:          job,
			Parameters: routerutils.JobParameters{
				SourceID:                sourceID,
				DestinationID:           destID,
				WorkspaceID:             workspace,
				TransformAt:             "processor",
				SourceDefinitionID:      "source-def-braze",
				DestinationDefinitionID: "braze-def",
				SourceCategory:          "webhook",
				MessageID:               fmt.Sprintf("message-%d", job.JobID),
				EventName:               eventName,
				EventType:               "track",
			},
		}
	}
	newWorker := func(t *testing.T, storeWarningPayload bool) (*worker, *memstats.Store) {
		t.Helper()
		statsStore := replaceDefaultStatsWithMemStats(t)
		rt := &Handle{
			destType:                         destType,
			logger:                           logger.NOP,
			responseQ:                        make(chan workerJobStatus, 16),
			reportJobsdbPayload:              config.SingleValueLoader(true),
			storeDeliveredWithWarningPayload: config.SingleValueLoader(storeWarningPayload),
			saveDestinationResponseOverride:  config.SingleValueLoader(true),
			reloadableConfig: &reloadableConfig{
				transformerProxy:                  config.SingleValueLoader(true),
				skipRtAbortAlertForTransformation: config.SingleValueLoader(false),
				skipRtAbortAlertForDelivery:       config.SingleValueLoader(false),
				minRetryBackoff:                   config.SingleValueLoader(time.Second),
				maxRetryBackoff:                   config.SingleValueLoader(time.Minute),
				maxFailedCountForJob:              config.SingleValueLoader(10),
				maxFailedCountForSourcesJob:       config.SingleValueLoader(10),
				retryTimeWindow:                   config.SingleValueLoader(time.Hour),
				sourcesRetryTimeWindow:            config.SingleValueLoader(time.Hour),
			},
		}
		rt.supportsDeliveredWithWarnings.Store(true)
		rt.deliveredWithWarningsEnabledForWorkspace = func(string) bool { return false }
		return &worker{
			rt:              rt,
			logger:          logger.NOP,
			routerProxyStat: stats.NOP.NewTaggedStat("router_proxy_latency", stats.TimerType, stats.Tags{"destType": destType}),
		}, statsStore
	}
	proxyThroughTransformer := func(t *testing.T, w *worker, destinationJob types.DestinationJobT, response any) transformer.ProxyRequestResponse {
		t.Helper()
		svr := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.URL.Path != "/v1/destinations/braze/proxy" {
				http.Error(rw, "unexpected transformer proxy path", http.StatusNotFound)
				return
			}
			rw.Header().Set("Content-Type", "application/json")
			rw.Header().Set("apiVersion", strconv.Itoa(utilTypes.SupportedTransformerApiVersion))
			require.NoError(t, json.NewEncoder(rw).Encode(response))
		}))
		t.Cleanup(svr.Close)
		t.Setenv("DELIVERY_TRANSFORMER_URL", "")
		t.Setenv("DEST_TRANSFORM_URL", svr.URL)
		w.rt.transformer = transformer.NewTransformer(destType, time.Minute, time.Minute, nil, config.SingleValueLoader(time.Minute), nil, config.New())
		w.rt.transformerFeaturesService = v1TransformerProxyFeaturesService{}
		return w.proxyRequest(context.Background(), destinationJob, integrations.PostParametersT{
			URL:           "https://rest.iad-01.braze.com/users/track",
			RequestMethod: http.MethodPost,
		})
	}

	t.Run("per-job proxy statuses map to jobsdb state, metrics, and reporting", func(t *testing.T) {
		mockCtrl := gomock.NewController(t)
		defer mockCtrl.Finish()

		w, statsStore := newWorker(t, true)
		reporter := &recordingReporter{}
		mockJobsDB := mocksJobsDB.NewMockJobsDB(mockCtrl)
		w.rt.Reporting = reporter
		w.rt.jobsDB = mockJobsDB
		w.rt.transientSources = transientsource.NewEmptyService()
		w.rt.rsourcesService = rsources.NewNoOpService()
		w.rt.throttlerFactory = throttler.NewNoOpThrottlerFactory()
		w.rt.reloadableConfig.updateStatusBatchSize = config.SingleValueLoader(100)
		w.rt.reloadableConfig.jobsDBCommandTimeout = config.SingleValueLoader(time.Second)
		w.rt.reloadableConfig.jobdDBMaxRetries = config.SingleValueLoader(1)

		jobsByID := map[int64]*jobsdb.JobT{
			101: newJob(101, "warning"),
			102: newJob(102, "bad-request"),
			103: newJob(103, "rate-limited"),
			104: newJob(104, "server-error"),
			105: newJob(105, "missing-output"),
			106: newJob(106, "duplicate-job-id"),
		}
		destinationJob := types.DestinationJobT{
			Message: json.RawMessage(delivery),
			Destination: backendconfig.DestinationT{
				ID:          destID,
				Name:        "Braze Golden",
				WorkspaceID: workspace,
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					ID:   "braze-def",
					Name: destType,
				},
			},
			JobMetadataArray: []types.JobMetadataT{
				newMetadata(jobsByID[101], "warning"),
				newMetadata(jobsByID[102], "bad-request"),
				newMetadata(jobsByID[103], "rate-limited"),
				newMetadata(jobsByID[104], "server-error"),
				newMetadata(jobsByID[105], "missing-output"),
				newMetadata(jobsByID[106], "duplicate-job-id"),
				newMetadata(jobsByID[106], "duplicate-job-id"),
			},
		}
		proxyResp := proxyThroughTransformer(t, w, destinationJob, map[string]any{
			"output": map[string]any{
				"message": "Request processed successfully",
				"response": []map[string]any{
					{
						"statusCode": utilTypes.DeliveredWithWarningCode,
						"error":      "delivered with warning",
						"metadata":   map[string]any{"jobId": 101, "sourceId": sourceID, "destinationId": destID, "workspaceId": workspace},
					},
					{
						"statusCode": http.StatusBadRequest,
						"error":      "bad request from Braze",
						"metadata":   map[string]any{"jobId": 102, "sourceId": sourceID, "destinationId": destID, "workspaceId": workspace},
					},
					{
						"statusCode": http.StatusTooManyRequests,
						"error":      "rate limited by Braze",
						"metadata":   map[string]any{"jobId": 103, "sourceId": sourceID, "destinationId": destID, "workspaceId": workspace},
					},
					{
						"statusCode": http.StatusInternalServerError,
						"error":      "Braze server error",
						"metadata":   map[string]any{"jobId": 104, "sourceId": sourceID, "destinationId": destID, "workspaceId": workspace},
					},
					{
						"statusCode": http.StatusBadRequest,
						"error":      "bad request from duplicate metadata",
						"metadata":   map[string]any{"jobId": 106, "sourceId": sourceID, "destinationId": destID, "workspaceId": workspace},
					},
					{
						"statusCode": http.StatusBadRequest,
						"error":      "duplicate Braze response entry",
						"metadata":   map[string]any{"jobId": 106, "sourceId": sourceID, "destinationId": destID, "workspaceId": workspace},
					},
				},
			},
		})
		require.Equal(t, http.StatusOK, proxyResp.ProxyRequestStatusCode)
		require.Equal(t, "application/json", proxyResp.RespContentType)
		require.Equal(t, map[int64]int{
			101: utilTypes.DeliveredWithWarningCode,
			102: http.StatusBadRequest,
			103: http.StatusTooManyRequests,
			104: http.StatusInternalServerError,
			106: http.StatusBadRequest,
		}, proxyResp.RespStatusCodes)
		require.NotContains(t, proxyResp.RespStatusCodes, int64(105), "missing per-job proxy output should be detected before router hydration")

		var workerStatuses []workerJobStatus
		for _, response := range w.prepareRouterJobResponses(destinationJob, proxyResp.RespStatusCodes, proxyResp.RespBodys, routerutils.ERROR_AT_DEL) {
			status := &jobsdb.JobStatusT{
				JobID:         response.destinationJobMetadata.JobID,
				AttemptNum:    response.destinationJobMetadata.AttemptNum,
				ExecTime:      time.Now(),
				RetryTime:     time.Now(),
				Parameters:    routerutils.EmptyPayload,
				JobParameters: response.destinationJobMetadata.JobT.Parameters,
				WorkspaceId:   response.destinationJobMetadata.WorkspaceID,
			}
			status.AttemptNum++
			status.ErrorResponse = routerutils.EnhanceJSON(routerutils.EmptyPayload, "response", response.respBody)
			status.ErrorCode = strconv.Itoa(response.respStatusCode)
			w.postStatusOnResponseQ(response.respStatusCode, response.destinationJob, proxyResp.RespContentType, response.destinationJobMetadata, status, response.errorAt)
			got := <-w.rt.responseQ
			workerStatuses = append(workerStatuses, got)
			w.sendEventDeliveryStat(response.destinationJobMetadata, got.status, &response.destinationJob.Destination)
			w.sendRouterResponseCountStat(got.status, &response.destinationJob.Destination, response.errorAt)
		}

		statusByJobID := make(map[int64]*jobsdb.JobStatusT)
		payloadByJobID := make(map[int64]json.RawMessage)
		for _, got := range workerStatuses {
			statusByJobID[got.status.JobID] = got.status
			payloadByJobID[got.status.JobID] = got.payload
		}
		require.Len(t, statusByJobID, 6, "duplicate job metadata should produce one final jobsdb status")
		require.Equal(t, jobsdb.Succeeded.State, statusByJobID[101].JobState)
		require.Equal(t, strconv.Itoa(utilTypes.DeliveredWithWarningCode), statusByJobID[101].ErrorCode)
		require.JSONEq(t, delivery, string(payloadByJobID[101]))
		require.Contains(t, string(statusByJobID[101].ErrorResponse), `"payloadStage":"delivery"`)
		require.Equal(t, jobsdb.Aborted.State, statusByJobID[102].JobState)
		require.Equal(t, jobsdb.Failed.State, statusByJobID[103].JobState)
		require.Equal(t, jobsdb.Failed.State, statusByJobID[104].JobState)
		require.Equal(t, jobsdb.Failed.State, statusByJobID[105].JobState)
		require.Equal(t, "500", statusByJobID[105].ErrorCode)
		require.Contains(t, string(statusByJobID[105].ErrorResponse), "Response for this job is expected but not found")
		require.Equal(t, jobsdb.Aborted.State, statusByJobID[106].JobState)

		allMetrics := statsStore.GetAll()
		_, inOutMismatchMetric := findMetricByNameAndTags(allMetrics, "router_transformerproxy_invalid_response", stats.Tags{
			"reason":        "in out mismatch",
			"destType":      destType,
			"destinationId": destID,
		})
		require.True(t, inOutMismatchMetric, "missing and duplicate per-job proxy entries should be caught at the transformer-proxy boundary")
		_, warningAbortMetric := findMetricByNameAndTags(allMetrics, "router_aborted_events", stats.Tags{
			"destType":       destType,
			"destId":         destID,
			"workspaceId":    workspace,
			"respStatusCode": strconv.Itoa(utilTypes.DeliveredWithWarningCode),
		})
		require.False(t, warningAbortMetric, "296 delivered-with-warning must not increment abort metrics")
		_, badRequestAbortMetric := findMetricByNameAndTags(allMetrics, "router_aborted_events", stats.Tags{
			"destType":       destType,
			"destId":         destID,
			"workspaceId":    workspace,
			"respStatusCode": strconv.Itoa(http.StatusBadRequest),
			"errorAt":        routerutils.ERROR_AT_DEL,
			"alert":          "false",
		})
		require.True(t, badRequestAbortMetric, "terminal 400s should keep abort metrics with transformer-proxy delivery alerts suppressed")
		_, warningResponseMetric := findMetricByNameAndTags(allMetrics, "router_response_counts", stats.Tags{
			"destType":       destType,
			"destId":         destID,
			"workspaceId":    workspace,
			"respStatusCode": strconv.Itoa(utilTypes.DeliveredWithWarningCode),
			"errorAt":        "",
		})
		require.True(t, warningResponseMetric, "296 should still be visible in router response counts")

		var persisted []*jobsdb.JobStatusT
		mockJobsDB.EXPECT().WithUpdateSafeTx(gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
			func(ctx context.Context, f func(tx jobsdb.UpdateSafeTx) error) error {
				return f(jobsdb.EmptyUpdateSafeTx())
			},
		)
		mockJobsDB.EXPECT().UpdateJobStatusInTx(gomock.Any(), gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
			func(_ context.Context, _ jobsdb.UpdateSafeTx, statuses []*jobsdb.JobStatusT) error {
				persisted = append(persisted, statuses...)
				return nil
			},
		)
		w.rt.commitStatusList(&workerStatuses)

		require.Len(t, persisted, 6)
		require.Len(t, reporter.metrics, 4)
		reportByCode := make(map[int]*utilTypes.PUReportedMetric)
		for _, metric := range reporter.metrics {
			reportByCode[metric.StatusDetail.StatusCode] = metric
			require.Equal(t, utilTypes.ROUTER, metric.PUDetails.PU)
			require.Equal(t, utilTypes.DEST_TRANSFORMER, metric.PUDetails.InPU)
			require.True(t, metric.PUDetails.TerminalPU)
			require.False(t, metric.PUDetails.InitialPU)
			require.Equal(t, sourceID, metric.ConnectionDetails.SourceID)
			require.Equal(t, destID, metric.ConnectionDetails.DestinationID)
			require.Equal(t, "braze-def", metric.ConnectionDetails.DestinationDefinitionID)
			require.Equal(t, "source-def-braze", metric.ConnectionDetails.SourceDefinitionID)
			require.Equal(t, "webhook", metric.ConnectionDetails.SourceCategory)
		}
		require.Equal(t, jobsdb.Succeeded.State, reportByCode[utilTypes.DeliveredWithWarningCode].StatusDetail.Status)
		require.Equal(t, int64(1), reportByCode[utilTypes.DeliveredWithWarningCode].StatusDetail.Count)
		require.Equal(t, jobsdb.Aborted.State, reportByCode[http.StatusBadRequest].StatusDetail.Status)
		require.Equal(t, int64(2), reportByCode[http.StatusBadRequest].StatusDetail.Count)
		require.Equal(t, jobsdb.Failed.State, reportByCode[http.StatusTooManyRequests].StatusDetail.Status)
		require.Equal(t, int64(1), reportByCode[http.StatusTooManyRequests].StatusDetail.Count)
		require.Equal(t, jobsdb.Failed.State, reportByCode[http.StatusInternalServerError].StatusDetail.Status)
		require.Equal(t, int64(2), reportByCode[http.StatusInternalServerError].StatusDetail.Count)
		require.Contains(t, reportByCode[http.StatusBadRequest].StatusDetail.SampleResponse, `"routerSubStage":"router_dest_delivery"`)
		require.Contains(t, reportByCode[http.StatusTooManyRequests].StatusDetail.SampleResponse, `"payloadStage":"router_input"`)
	})

	t.Run("malformed proxy output without per-job results stays retryable", func(t *testing.T) {
		w, statsStore := newWorker(t, false)
		job := newJob(201, "malformed-output")
		destinationJob := types.DestinationJobT{
			Message: json.RawMessage(delivery),
			Destination: backendconfig.DestinationT{
				ID:          destID,
				Name:        "Braze Golden",
				WorkspaceID: workspace,
			},
			JobMetadataArray: []types.JobMetadataT{newMetadata(job, "malformed-output")},
		}
		proxyResp := proxyThroughTransformer(t, w, destinationJob, map[string]any{
			"message": "missing output envelope from transformer",
		})
		require.Equal(t, http.StatusOK, proxyResp.ProxyRequestStatusCode)
		require.Empty(t, proxyResp.RespStatusCodes)
		responses := w.prepareRouterJobResponses(destinationJob, proxyResp.RespStatusCodes, proxyResp.RespBodys, routerutils.ERROR_AT_DEL)
		require.Len(t, responses, 1)
		status := &jobsdb.JobStatusT{
			JobID:         responses[0].destinationJobMetadata.JobID,
			ExecTime:      time.Now(),
			RetryTime:     time.Now(),
			Parameters:    routerutils.EmptyPayload,
			JobParameters: job.Parameters,
			WorkspaceId:   workspace,
			ErrorResponse: routerutils.EnhanceJSON(routerutils.EmptyPayload, "response", responses[0].respBody),
			ErrorCode:     strconv.Itoa(responses[0].respStatusCode),
		}
		w.postStatusOnResponseQ(responses[0].respStatusCode, responses[0].destinationJob, "text/plain; charset=utf-8", responses[0].destinationJobMetadata, status, responses[0].errorAt)
		got := <-w.rt.responseQ
		require.Equal(t, jobsdb.Failed.State, got.status.JobState)
		require.Equal(t, strconv.Itoa(http.StatusInternalServerError), got.status.ErrorCode)
		require.Contains(t, string(got.status.ErrorResponse), "Response for this job is expected but not found")
		w.sendRouterResponseCountStat(got.status, &responses[0].destinationJob.Destination, responses[0].errorAt)
		_, missingOutputMetric := findMetricByNameAndTags(statsStore.GetAll(), "router_transformerproxy_invalid_response", stats.Tags{
			"reason":        "missing output",
			"destType":      destType,
			"destinationId": destID,
		})
		require.True(t, missingOutputMetric, "malformed transformer output should be caught before router hydration")
		_, abortMetric := findMetricByNameAndTags(statsStore.GetAll(), "router_aborted_events", stats.Tags{"destType": destType})
		require.False(t, abortMetric, "malformed proxy output must not be terminal-aborted")
	})

	t.Run("delivered-with-warning keeps router input payload when payload storage is disabled", func(t *testing.T) {
		w, _ := newWorker(t, false)
		job := newJob(301, "warning-no-payload-store")
		destinationJob := types.DestinationJobT{
			Message:          json.RawMessage(delivery),
			JobMetadataArray: []types.JobMetadataT{newMetadata(job, "warning-no-payload-store")},
		}
		responses := w.prepareRouterJobResponses(destinationJob, map[int64]int{301: utilTypes.DeliveredWithWarningCode}, map[int64]string{301: "warning"}, routerutils.ERROR_AT_DEL)
		status := &jobsdb.JobStatusT{
			JobID:         301,
			ExecTime:      time.Now(),
			RetryTime:     time.Now(),
			Parameters:    routerutils.EmptyPayload,
			JobParameters: job.Parameters,
			WorkspaceId:   workspace,
			ErrorResponse: routerutils.EnhanceJSON(routerutils.EmptyPayload, "response", responses[0].respBody),
			ErrorCode:     strconv.Itoa(responses[0].respStatusCode),
		}
		w.postStatusOnResponseQ(responses[0].respStatusCode, responses[0].destinationJob, "application/json", responses[0].destinationJobMetadata, status, responses[0].errorAt)
		got := <-w.rt.responseQ
		require.Equal(t, jobsdb.Succeeded.State, got.status.JobState)
		require.JSONEq(t, inputEvent, string(got.payload))
		require.NotContains(t, string(got.status.ErrorResponse), "payloadStage")
	})
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
					Expect(proxyReqParams.ResponseData.DestinationConfig).To(Equal(map[string]any{
						"x": map[string]any{
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
					Config: map[string]any{
						"x": map[string]any{
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
					Expect(proxyReqParams.ResponseData.DestinationConfig).To(Equal(map[string]any{
						"x": map[string]any{
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
					Config: map[string]any{
						"x": map[string]any{
							"y": "z",
						},
					},
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						Config: map[string]any{
							"auth": map[string]any{
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

func TestTransformForDestination(t *testing.T) {
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
			reloadableConfig:               &reloadableConfig{},
		},
	}
	var limiterWg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	defer limiterWg.Wait()
	defer cancel()
	worker.rt.limiter.transform = kitsync.NewLimiter(ctx, &limiterWg, "transform", math.MaxInt, stats.Default)
	worker.rt.limiter.stats.transform = partition.NewStats()

	// OAuth destination definition - jobs will be grouped by destination ID
	oauthDestDef := backendconfig.DestinationDefinitionT{
		Config: map[string]any{
			"auth": map[string]any{
				"type": "OAuth",
			},
		},
	}
	// Non-OAuth destination definition - jobs will be grouped together
	nonOauthDestDef := backendconfig.DestinationDefinitionT{
		Config: map[string]any{},
	}

	routerJobs := []types.RouterJobT{
		{
			Destination: backendconfig.DestinationT{
				ID:                    "d1",
				DestinationDefinition: oauthDestDef, // OAuth
			},
			Message: json.RawMessage(`{"event": "d1-test1"}`),
			JobMetadata: types.JobMetadataT{
				JobID: 1,
			},
		},
		{
			Destination: backendconfig.DestinationT{
				ID:                    "d2",
				DestinationDefinition: nonOauthDestDef, // non-OAuth
			},
			Message: json.RawMessage(`{"event": "d2-test2"}`),
			JobMetadata: types.JobMetadataT{
				JobID: 2,
			},
		},
		{
			Destination: backendconfig.DestinationT{
				ID:                    "d1",
				DestinationDefinition: oauthDestDef, // OAuth
			},
			Message: json.RawMessage(`{"event": "d1-test3"}`),
			JobMetadata: types.JobMetadataT{
				JobID: 3,
			},
		},
		{
			Destination: backendconfig.DestinationT{
				ID:                    "d3",
				DestinationDefinition: nonOauthDestDef, // non-OAuth
			},
			Message: json.RawMessage(`{"event": "d3-test4"}`),
			JobMetadata: types.JobMetadataT{
				JobID: 4,
			},
		},
		{
			Destination: backendconfig.DestinationT{
				ID:                    "d1",
				DestinationDefinition: oauthDestDef, // OAuth
			},
			Message: json.RawMessage(`{"event": "d1-test5"}`),
			JobMetadata: types.JobMetadataT{
				JobID: 5,
			},
		},
		{
			Destination: backendconfig.DestinationT{
				ID:                    "d2",
				DestinationDefinition: nonOauthDestDef, // non-OAuth
			},
			Message: json.RawMessage(`{"event": "d2-test6"}`),
			JobMetadata: types.JobMetadataT{
				JobID: 6,
			},
		},
	}
	// Expect call for OAuth destination d1 (jobs grouped by destination ID)
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
	// Expect call for non-OAuth destinations d2 and d3 (jobs grouped together)
	mockTransformer.EXPECT().Transform(transformer.ROUTER_TRANSFORM, &types.TransformMessageT{
		Data:     []types.RouterJobT{routerJobs[1], routerJobs[3], routerJobs[5]},
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
