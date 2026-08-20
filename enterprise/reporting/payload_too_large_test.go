package reporting

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-server/enterprise/reporting/client"
	"github.com/rudderlabs/rudder-server/utils/types"
)

func TestSendMetricWithPayloadTooLargeSplit(t *testing.T) {
	t.Run("splits batch and sends individual metrics", func(t *testing.T) {
		var mu sync.Mutex
		var payloads []types.Metric
		requestCount := 0
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			defer mu.Unlock()
			requestCount++
			body, err := io.ReadAll(r.Body)
			require.NoError(t, err)
			var payload types.Metric
			require.NoError(t, jsonrs.Unmarshal(body, &payload))
			payloads = append(payloads, payload)

			if requestCount == 1 {
				w.WriteHeader(http.StatusRequestEntityTooLarge)
				return
			}
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		metricClient := newPayloadTooLargeTestClient(t, server.URL, 0)
		err := sendMetricWithPayloadTooLargeSplit(context.Background(), metricClient, testPayloadTooLargeMetric())
		require.NoError(t, err)

		mu.Lock()
		defer mu.Unlock()
		require.Equal(t, 3, requestCount)
		require.Len(t, payloads[0].StatusDetails, 2)
		require.Len(t, payloads[1].StatusDetails, 1)
		require.Equal(t, "event-1", payloads[1].StatusDetails[0].EventName)
		require.Len(t, payloads[2].StatusDetails, 1)
		require.Equal(t, "event-2", payloads[2].StatusDetails[0].EventName)
	})

	t.Run("replaces individual oversized sample event and retries with fallback", func(t *testing.T) {
		var mu sync.Mutex
		var payloads []types.Metric
		requestCount := 0
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			defer mu.Unlock()
			requestCount++
			body, err := io.ReadAll(r.Body)
			require.NoError(t, err)
			var payload types.Metric
			require.NoError(t, jsonrs.Unmarshal(body, &payload))
			payloads = append(payloads, payload)

			if requestCount <= 2 {
				w.WriteHeader(http.StatusRequestEntityTooLarge)
				return
			}
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		metricClient := newPayloadTooLargeTestClient(t, server.URL, 0)
		err := sendMetricWithPayloadTooLargeSplit(context.Background(), metricClient, testPayloadTooLargeMetricWithOneStatusDetail())
		require.NoError(t, err)

		mu.Lock()
		defer mu.Unlock()
		require.Equal(t, 3, requestCount)
		require.JSONEq(t, sampleEventNotAvailableEntityTooLarge, string(payloads[2].StatusDetails[0].SampleEvent))
		require.Equal(t, "sample-response-1", payloads[2].StatusDetails[0].SampleResponse)
		require.JSONEq(t, `{"event":"sample-1"}`, string(payloads[1].StatusDetails[0].SampleEvent))
	})

	t.Run("stripped oversized metric retries through normal fallback path", func(t *testing.T) {
		var mu sync.Mutex
		requestCount := 0
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			requestCount++
			mu.Unlock()
			w.WriteHeader(http.StatusRequestEntityTooLarge)
		}))
		defer server.Close()

		metricClient := newPayloadTooLargeTestClient(t, server.URL, 1)
		err := sendMetricWithPayloadTooLargeSplit(context.Background(), metricClient, testPayloadTooLargeMetricWithOneStatusDetail())
		require.Error(t, err)
		require.False(t, errors.Is(err, client.ErrPayloadTooLarge))
		require.Contains(t, err.Error(), "statusCode: 413")

		mu.Lock()
		defer mu.Unlock()
		require.Equal(t, 4, requestCount)
	})

	t.Run("individual non-413 error retries through normal path", func(t *testing.T) {
		var mu sync.Mutex
		requestCount := 0
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			requestCount++
			currentRequest := requestCount
			mu.Unlock()

			if currentRequest == 1 {
				w.WriteHeader(http.StatusRequestEntityTooLarge)
				return
			}
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer server.Close()

		metricClient := newPayloadTooLargeTestClient(t, server.URL, 1)
		err := sendMetricWithPayloadTooLargeSplit(context.Background(), metricClient, testPayloadTooLargeMetricWithOneStatusDetail())
		require.Error(t, err)
		require.False(t, errors.Is(err, client.ErrPayloadTooLarge))
		require.Contains(t, err.Error(), "statusCode: 500")

		mu.Lock()
		defer mu.Unlock()
		require.Equal(t, 3, requestCount)
	})
}

func TestSendEDMetricWithPayloadTooLargeSplit(t *testing.T) {
	t.Run("splits batch and sends individual error metrics", func(t *testing.T) {
		var mu sync.Mutex
		var payloads []types.EDMetric
		requestCount := 0
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			defer mu.Unlock()
			requestCount++
			body, err := io.ReadAll(r.Body)
			require.NoError(t, err)
			var payload types.EDMetric
			require.NoError(t, jsonrs.Unmarshal(body, &payload))
			payloads = append(payloads, payload)

			if requestCount == 1 {
				w.WriteHeader(http.StatusRequestEntityTooLarge)
				return
			}
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		edClient := newPayloadTooLargeTestClient(t, server.URL, 0)
		err := sendEDMetricWithPayloadTooLargeSplit(context.Background(), edClient, testPayloadTooLargeEDMetric())
		require.NoError(t, err)

		mu.Lock()
		defer mu.Unlock()
		require.Equal(t, 3, requestCount)
		require.Len(t, payloads[0].Errors, 2)
		require.Len(t, payloads[1].Errors, 1)
		require.Equal(t, "event-1", payloads[1].Errors[0].EventName)
		require.Len(t, payloads[2].Errors, 1)
		require.Equal(t, "event-2", payloads[2].Errors[0].EventName)
	})

	t.Run("replaces individual oversized error sample event and retries with fallback", func(t *testing.T) {
		var mu sync.Mutex
		var payloads []types.EDMetric
		requestCount := 0
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			defer mu.Unlock()
			requestCount++
			body, err := io.ReadAll(r.Body)
			require.NoError(t, err)
			var payload types.EDMetric
			require.NoError(t, jsonrs.Unmarshal(body, &payload))
			payloads = append(payloads, payload)

			if requestCount <= 2 {
				w.WriteHeader(http.StatusRequestEntityTooLarge)
				return
			}
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		edClient := newPayloadTooLargeTestClient(t, server.URL, 0)
		err := sendEDMetricWithPayloadTooLargeSplit(context.Background(), edClient, testPayloadTooLargeEDMetricWithOneError())
		require.NoError(t, err)

		mu.Lock()
		defer mu.Unlock()
		require.Equal(t, 3, requestCount)
		require.JSONEq(t, sampleEventNotAvailableEntityTooLarge, string(payloads[2].Errors[0].SampleEvent))
		require.Equal(t, "sample-response-1", payloads[2].Errors[0].SampleResponse)
		require.JSONEq(t, `{"event":"sample-1"}`, string(payloads[1].Errors[0].SampleEvent))
	})

	t.Run("stripped oversized error metric retries through normal fallback path", func(t *testing.T) {
		var mu sync.Mutex
		requestCount := 0
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			requestCount++
			mu.Unlock()
			w.WriteHeader(http.StatusRequestEntityTooLarge)
		}))
		defer server.Close()

		edClient := newPayloadTooLargeTestClient(t, server.URL, 1)
		err := sendEDMetricWithPayloadTooLargeSplit(context.Background(), edClient, testPayloadTooLargeEDMetricWithOneError())
		require.Error(t, err)
		require.False(t, errors.Is(err, client.ErrPayloadTooLarge))
		require.Contains(t, err.Error(), "statusCode: 413")

		mu.Lock()
		defer mu.Unlock()
		require.Equal(t, 4, requestCount)
	})
}

func newPayloadTooLargeTestClient(t *testing.T, serverURL string, maxRetries int) *client.Client {
	t.Helper()
	conf := config.New()
	conf.Set("REPORTING_URL", serverURL)
	conf.Set("Reporting.splitOnPayloadTooLarge.enabled", true)
	conf.Set("Reporting.httpClient.backoff.maxRetries", maxRetries)
	return client.New(client.RouteMetrics, conf, logger.NOP, stats.NOP)
}

func testPayloadTooLargeMetric() *types.Metric {
	metric := testPayloadTooLargeMetricWithOneStatusDetail()
	metric.StatusDetails = append(metric.StatusDetails, &types.StatusDetail{
		Status:         "failed",
		Count:          2,
		StatusCode:     http.StatusBadRequest,
		SampleResponse: "sample-response-2",
		SampleEvent:    []byte(`{"event":"sample-2"}`),
		EventName:      "event-2",
		EventType:      "track",
	})
	return metric
}

func testPayloadTooLargeMetricWithOneStatusDetail() *types.Metric {
	return &types.Metric{
		InstanceDetails: types.InstanceDetails{
			WorkspaceID: "workspace-1",
			InstanceID:  "instance-1",
		},
		ConnectionDetails: types.ConnectionDetails{
			SourceID:      "source-1",
			DestinationID: "destination-1",
		},
		PUDetails: types.PUDetails{
			InPU: "gateway",
			PU:   "router",
		},
		ReportMetadata: types.ReportMetadata{
			ReportedAt:        1000,
			SampleEventBucket: 900,
		},
		StatusDetails: []*types.StatusDetail{
			{
				Status:         "failed",
				Count:          1,
				StatusCode:     http.StatusBadRequest,
				SampleResponse: "sample-response-1",
				SampleEvent:    []byte(`{"event":"sample-1"}`),
				EventName:      "event-1",
				EventType:      "track",
			},
		},
	}
}

func testPayloadTooLargeEDMetric() *types.EDMetric {
	metric := testPayloadTooLargeEDMetricWithOneError()
	metric.Errors = append(metric.Errors, types.EDErrorDetails{
		EDErrorDetailsKey: types.EDErrorDetailsKey{
			StatusCode:   http.StatusBadRequest,
			ErrorCode:    "ERR_2",
			ErrorMessage: "error 2",
			EventType:    "track",
			EventName:    "event-2",
		},
		SampleResponse: "sample-response-2",
		SampleEvent:    []byte(`{"event":"sample-2"}`),
		ErrorCount:     2,
	})
	return metric
}

func testPayloadTooLargeEDMetricWithOneError() *types.EDMetric {
	return &types.EDMetric{
		EDInstanceDetails: types.EDInstanceDetails{
			WorkspaceID: "workspace-1",
			InstanceID:  "instance-1",
		},
		EDConnectionDetails: types.EDConnectionDetails{
			SourceID:      "source-1",
			DestinationID: "destination-1",
		},
		PU: "router",
		ReportMetadata: types.ReportMetadata{
			ReportedAt:        1000,
			SampleEventBucket: 900,
		},
		Errors: []types.EDErrorDetails{
			{
				EDErrorDetailsKey: types.EDErrorDetailsKey{
					StatusCode:   http.StatusBadRequest,
					ErrorCode:    "ERR_1",
					ErrorMessage: "error 1",
					EventType:    "track",
					EventName:    "event-1",
				},
				SampleResponse: "sample-response-1",
				SampleEvent:    []byte(`{"event":"sample-1"}`),
				ErrorCount:     1,
			},
		},
	}
}
