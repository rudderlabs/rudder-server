package reporting

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"

	"github.com/rudderlabs/rudder-server/enterprise/reporting/client"
	"github.com/rudderlabs/rudder-server/utils/types"
)

func TestShouldReport(t *testing.T) {
	RegisterTestingT(t)

	testCases := []struct {
		name     string
		metric   types.PUReportedMetric
		expected bool
	}{
		{
			name: "event failure case",
			metric: types.PUReportedMetric{
				StatusDetail: &types.StatusDetail{
					StatusCode: http.StatusBadRequest,
				},
			},
			expected: true,
		},
		{
			name: "filter event case",
			metric: types.PUReportedMetric{
				StatusDetail: &types.StatusDetail{
					StatusCode: types.FilterEventCode,
				},
			},
			expected: true,
		},
		{
			name: "suppress event case",
			metric: types.PUReportedMetric{
				StatusDetail: &types.StatusDetail{
					StatusCode: types.SuppressEventCode,
				},
			},
			expected: true,
		},
		{
			name: "success case",
			metric: types.PUReportedMetric{
				StatusDetail: &types.StatusDetail{
					StatusCode: http.StatusOK,
				},
			},
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := shouldReport(tc.metric)
			Expect(result).To(Equal(tc.expected))
		})
	}
}

func TestCleanUpErrorMessage(t *testing.T) {
	ext := NewErrorDetailExtractor(logger.NOP, config.New())

	testCases := []struct {
		name     string
		inputStr string
		expected string
	}{
		{
			name:     "object ID cleanup",
			inputStr: "Object with ID '123983489734' is not a valid object",
			expected: "Object with ID is not a valid object",
		},
		{
			name:     "URL cleanup with context deadline",
			inputStr: "http://xyz-rudder.com/v1/endpoint not reachable: context deadline exceeded",
			expected: "not reachable context deadline exceeded",
		},
		{
			name:     "URL cleanup with EOF",
			inputStr: "http://xyz-rudder.com/v1/endpoint not reachable 172.22.22.10: EOF",
			expected: "not reachable EOF",
		},
		{
			name:     "timestamp cleanup",
			inputStr: "Request failed to process from 16-12-2022:19:30:23T+05:30 due to internal server error",
			expected: "Request failed to process from due to internal server error",
		},
		{
			name:     "email cleanup",
			inputStr: "User with email 'vagor12@bing.com' is not valid",
			expected: "User with email is not valid",
		},
		{
			name:     "time duration cleanup",
			inputStr: "Allowed timestamp is [15 minutes] into the future",
			expected: "Allowed timestamp is minutes into the future",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			actual := ext.CleanUpErrorMessage(tc.inputStr)
			require.Equal(t, tc.expected, actual)
		})
	}
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
		metric := testPayloadTooLargeEDMetric()
		err := sendEDMetricWithPayloadTooLargeSplit(context.Background(), edClient, metric)
		require.NoError(t, err)

		mu.Lock()
		defer mu.Unlock()
		// batch (413) + event-1 (413) + event-1 stripped (200) + event-2 (200)
		require.Equal(t, 4, requestCount)
		require.Equal(t, "event-1", payloads[1].Errors[0].EventName)
		require.JSONEq(t, `{"event":"sample-1"}`, string(payloads[1].Errors[0].SampleEvent))
		require.Equal(t, "event-1", payloads[2].Errors[0].EventName)
		require.JSONEq(t, string(sampleEventNotAvailableEntityTooLarge), string(payloads[2].Errors[0].SampleEvent))
		require.Equal(t, "sample-response-1", payloads[2].Errors[0].SampleResponse)
		require.Equal(t, "event-2", payloads[3].Errors[0].EventName)
		require.JSONEq(t, `{"event":"sample-2"}`, string(payloads[3].Errors[0].SampleEvent))
		// caller's metric must not be mutated by the split/strip
		require.Len(t, metric.Errors, 2)
		require.JSONEq(t, `{"event":"sample-1"}`, string(metric.Errors[0].SampleEvent))
	})

	t.Run("single error detail skips redundant resend and goes straight to stripped fallback", func(t *testing.T) {
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
		metric := testPayloadTooLargeEDMetricWithOneError()
		err := sendEDMetricWithPayloadTooLargeSplit(context.Background(), edClient, metric)
		require.NoError(t, err)

		mu.Lock()
		defer mu.Unlock()
		// batch (413) + stripped (200); no identical individual resend in between
		require.Equal(t, 2, requestCount)
		require.JSONEq(t, `{"event":"sample-1"}`, string(payloads[0].Errors[0].SampleEvent))
		require.JSONEq(t, string(sampleEventNotAvailableEntityTooLarge), string(payloads[1].Errors[0].SampleEvent))
		require.JSONEq(t, `{"event":"sample-1"}`, string(metric.Errors[0].SampleEvent))
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
		// batch (413) + stripped fallback (413, retried once)
		require.Equal(t, 3, requestCount)
	})

	t.Run("partial failure returns error after delivering earlier error metrics", func(t *testing.T) {
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

			switch requestCount {
			case 1:
				w.WriteHeader(http.StatusRequestEntityTooLarge)
			case 2:
				w.WriteHeader(http.StatusOK)
			default:
				w.WriteHeader(http.StatusInternalServerError)
			}
		}))
		defer server.Close()

		edClient := newPayloadTooLargeTestClient(t, server.URL, 1)
		err := sendEDMetricWithPayloadTooLargeSplit(context.Background(), edClient, testPayloadTooLargeEDMetric())
		require.Error(t, err)
		require.False(t, errors.Is(err, client.ErrPayloadTooLarge))
		require.Contains(t, err.Error(), "statusCode: 500")

		mu.Lock()
		defer mu.Unlock()
		// 1 batch (413) + 1 for event-1 (200) + 2 for event-2 (500, retried once)
		require.Equal(t, 4, requestCount)
		require.Len(t, payloads[1].Errors, 1)
		require.Equal(t, "event-1", payloads[1].Errors[0].EventName)
		require.Len(t, payloads[2].Errors, 1)
		require.Equal(t, "event-2", payloads[2].Errors[0].EventName)
		require.Equal(t, "event-2", payloads[3].Errors[0].EventName)
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

		edClient := newPayloadTooLargeTestClient(t, server.URL, 1)
		err := sendEDMetricWithPayloadTooLargeSplit(context.Background(), edClient, testPayloadTooLargeEDMetricWithOneError())
		require.Error(t, err)
		require.False(t, errors.Is(err, client.ErrPayloadTooLarge))
		require.Contains(t, err.Error(), "statusCode: 500")

		mu.Lock()
		defer mu.Unlock()
		require.Equal(t, 3, requestCount)
	})
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
