package flusher

import (
	"context"
	"encoding/json"
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
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"

	"github.com/rudderlabs/rudder-server/enterprise/reporting/client"
)

func TestFlusherSendPayloadTooLarge(t *testing.T) {
	t.Run("splits batch and sends individual items", func(t *testing.T) {
		var mu sync.Mutex
		requestCount := 0
		var payloads [][]map[string]any
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			defer mu.Unlock()
			requestCount++
			body, err := io.ReadAll(r.Body)
			require.NoError(t, err)
			var payload []map[string]any
			require.NoError(t, jsonrs.Unmarshal(body, &payload))
			payloads = append(payloads, payload)
			if requestCount == 1 {
				w.WriteHeader(http.StatusRequestEntityTooLarge)
				return
			}
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		f, statsStore := newPayloadTooLargeTestFlusher(t, server.URL, 0, 2)
		err := f.send(context.Background(), []json.RawMessage{[]byte(`{"id":1}`), []byte(`{"id":2}`)})
		require.NoError(t, err)

		mu.Lock()
		defer mu.Unlock()
		require.Equal(t, 3, requestCount)
		requirePayloadTooLargeSplits(t, statsStore, 1)
		require.Len(t, payloads[0], 2)
		require.Len(t, payloads[1], 1)
		require.Equal(t, float64(1), payloads[1][0]["id"])
		require.Len(t, payloads[2], 1)
		require.Equal(t, float64(2), payloads[2][0]["id"])
	})

	t.Run("individual item still too large retries through normal fallback path", func(t *testing.T) {
		var mu sync.Mutex
		requestCount := 0
		var payloads [][]map[string]any
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			defer mu.Unlock()
			requestCount++
			body, err := io.ReadAll(r.Body)
			require.NoError(t, err)
			var payload []map[string]any
			require.NoError(t, jsonrs.Unmarshal(body, &payload))
			payloads = append(payloads, payload)
			w.WriteHeader(http.StatusRequestEntityTooLarge)
		}))
		defer server.Close()

		f, statsStore := newPayloadTooLargeTestFlusher(t, server.URL, 1, 1)
		err := f.send(context.Background(), []json.RawMessage{[]byte(`{"id":1}`)})
		require.Error(t, err)
		require.False(t, errors.Is(err, client.ErrPayloadTooLarge))
		require.Contains(t, err.Error(), "statusCode: 413")

		mu.Lock()
		defer mu.Unlock()
		// batch (413) + fallback (413, retried once); no identical individual resend in between
		require.Equal(t, 3, requestCount)
		requirePayloadTooLargeSplits(t, statsStore, 1)
		for _, payload := range payloads {
			require.Len(t, payload, 1)
			require.Equal(t, float64(1), payload[0]["id"])
		}
	})

	t.Run("single item skips redundant resend and goes straight to fallback", func(t *testing.T) {
		var mu sync.Mutex
		requestCount := 0
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			defer mu.Unlock()
			requestCount++
			if requestCount == 1 {
				w.WriteHeader(http.StatusRequestEntityTooLarge)
				return
			}
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		f, statsStore := newPayloadTooLargeTestFlusher(t, server.URL, 0, 1)
		err := f.send(context.Background(), []json.RawMessage{[]byte(`{"id":1}`)})
		require.NoError(t, err)

		mu.Lock()
		defer mu.Unlock()
		// batch (413) + fallback (200)
		require.Equal(t, 2, requestCount)
		requirePayloadTooLargeSplits(t, statsStore, 1)
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

		f, statsStore := newPayloadTooLargeTestFlusher(t, server.URL, 1, 1)
		err := f.send(context.Background(), []json.RawMessage{[]byte(`{"id":1}`)})
		require.Error(t, err)
		require.False(t, errors.Is(err, client.ErrPayloadTooLarge))
		require.Contains(t, err.Error(), "statusCode: 500")

		mu.Lock()
		defer mu.Unlock()
		require.Equal(t, 3, requestCount)
		requirePayloadTooLargeSplits(t, statsStore, 1)
	})
}

func newPayloadTooLargeTestFlusher(t *testing.T, serverURL string, maxRetries, batchSize int) (*Flusher, *memstats.Store) {
	t.Helper()
	conf := config.New()
	conf.Set("REPORTING_URL", serverURL)
	conf.Set("Reporting.payloadTooLargeHandling.enabled", true)
	conf.Set("Reporting.httpClient.backoff.maxRetries", maxRetries)
	conf.Set("Reporting.flusher.batchSizeToReporting", batchSize)
	conf.Set("Reporting.flusher.minConcurrentRequests", 1)
	conf.Set("Reporting.flusher.maxConcurrentRequests", 1)
	statsStore, err := memstats.New()
	require.NoError(t, err)
	commonClient := client.New(client.RouteTrackedUsers, conf, logger.NOP, stats.NOP)
	f, err := NewFlusher(nil, logger.NOP, statsStore, conf, "tracked_users_reports", commonClient, nil, "test")
	require.NoError(t, err)
	return f, statsStore
}

func requirePayloadTooLargeSplits(t *testing.T, statsStore *memstats.Store, want float64) {
	t.Helper()
	metrics := statsStore.GetByName("reporting_flusher_payload_too_large_split")
	require.Len(t, metrics, 1)
	require.Equal(t, want, metrics[0].Value, "payload too large split count")
}
