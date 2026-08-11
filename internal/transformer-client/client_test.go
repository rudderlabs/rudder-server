package transformerclient

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/httputil"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"
)

func TestClient_RetryBehavior(t *testing.T) {
	t.Run("retries on 503 with X-Rudder-Should-Retry header", func(t *testing.T) {
		var requestCount int
		retryableResponses := 3
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requestCount++

			if requestCount <= retryableResponses {
				// Return retriable error
				w.Header().Set(HeaderShouldRetry, "true")
				w.Header().Set(HeaderErrorReason, "temporary-overload")
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte("Service temporarily unavailable"))
			} else {
				// Return success
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte("OK"))
			}
		}))
		defer server.Close()

		clientConfig := &ClientConfig{
			ClientTimeout: 10 * time.Second,
			RetryRudderErrors: struct {
				Enabled         bool
				MaxRetry        int
				InitialInterval time.Duration
				MaxInterval     time.Duration
				MaxElapsedTime  time.Duration
				Multiplier      float64
			}{
				Enabled:         true,
				MaxRetry:        -1, // Unlimited retries
				InitialInterval: 50 * time.Millisecond,
				MaxInterval:     200 * time.Millisecond,
				MaxElapsedTime:  5 * time.Second,
				Multiplier:      2.0,
			},
		}
		client := NewClient("testClient", clientConfig)

		req, err := http.NewRequest("POST", server.URL, strings.NewReader("test data"))
		require.NoError(t, err)

		start := time.Now()
		resp, err := client.Do(req)
		elapsed := time.Since(start)

		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		require.Equal(t, retryableResponses+1, requestCount, "Should make exactly %d requests", retryableResponses+1)

		require.True(t, elapsed > 100*time.Millisecond, "Should take some time due to retries")
		require.True(t, elapsed < 5*time.Second, "Should complete before max elapsed time")

		resp.Body.Close()
	})

	t.Run("stops retrying after max elapsed time", func(t *testing.T) {
		var requestCount int
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requestCount++
			w.Header().Set(HeaderShouldRetry, "true")
			w.Header().Set(HeaderErrorReason, "persistent-overload")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("Service permanently unavailable"))
		}))
		defer server.Close()

		clientConfig := &ClientConfig{
			ClientTimeout: 10 * time.Second,
			RetryRudderErrors: struct {
				Enabled         bool
				MaxRetry        int
				InitialInterval time.Duration
				MaxInterval     time.Duration
				MaxElapsedTime  time.Duration
				Multiplier      float64
			}{
				Enabled:         true,
				MaxRetry:        -1, // Unlimited retries
				InitialInterval: 50 * time.Millisecond,
				MaxInterval:     200 * time.Millisecond,
				MaxElapsedTime:  1 * time.Second, // Short elapsed time to test timeout
				Multiplier:      2.0,
			},
		}
		client := NewClient("testClient", clientConfig)

		req, err := http.NewRequest("POST", server.URL, strings.NewReader("test data"))
		require.NoError(t, err)

		start := time.Now()
		resp, err := client.Do(req)
		elapsed := time.Since(start)

		require.NoError(t, err, "Retryable client returns last response, not error")
		require.NotNil(t, resp)
		require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)

		require.True(t, elapsed >= 500*time.Millisecond, "Should have retried for substantial time")
		require.True(t, elapsed <= 1500*time.Millisecond, "Should not retry much beyond max elapsed time")
		require.True(t, requestCount > 1, "Should have made multiple requests")

		resp.Body.Close()
	})

	t.Run("switches from retriable to non-retriable response", func(t *testing.T) {
		var requestCount int
		switchAfter := 2
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requestCount++

			if requestCount <= switchAfter {
				// Return retriable error
				w.Header().Set(HeaderShouldRetry, "true")
				w.Header().Set(HeaderErrorReason, "temporary-overload")
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte("Service temporarily unavailable"))
			} else {
				// Return non-retriable error (503 without retry header)
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte("Service unavailable - do not retry"))
			}
		}))
		defer server.Close()

		clientConfig := &ClientConfig{
			ClientTimeout: 10 * time.Second,
			RetryRudderErrors: struct {
				Enabled         bool
				MaxRetry        int
				InitialInterval time.Duration
				MaxInterval     time.Duration
				MaxElapsedTime  time.Duration
				Multiplier      float64
			}{
				Enabled:         true,
				MaxRetry:        -1, // Unlimited retries
				InitialInterval: 50 * time.Millisecond,
				MaxInterval:     200 * time.Millisecond,
				MaxElapsedTime:  5 * time.Second,
				Multiplier:      2.0,
			},
		}
		client := NewClient("testClient", clientConfig)

		req, err := http.NewRequest("POST", server.URL, strings.NewReader("test data"))
		require.NoError(t, err)

		start := time.Now()
		resp, err := client.Do(req)
		elapsed := time.Since(start)

		require.NoError(t, err)
		require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
		require.Equal(t, switchAfter+1, requestCount, "Should make exactly %d requests", switchAfter+1)

		require.True(t, elapsed < 1*time.Second, "Should complete quickly after non-retriable response")

		resp.Body.Close()
	})

	t.Run("does not retry non-503 status codes", func(t *testing.T) {
		testCases := []struct {
			name       string
			statusCode int
		}{
			{"400 Bad Request", http.StatusBadRequest},
			{"401 Unauthorized", http.StatusUnauthorized},
			{"404 Not Found", http.StatusNotFound},
			{"500 Internal Server Error", http.StatusInternalServerError},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				var requestCount int
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					requestCount++
					w.WriteHeader(tc.statusCode)
					_, _ = w.Write([]byte("Error"))
				}))
				defer server.Close()

				clientConfig := &ClientConfig{
					ClientTimeout: 10 * time.Second,
					RetryRudderErrors: struct {
						Enabled         bool
						MaxRetry        int
						InitialInterval time.Duration
						MaxInterval     time.Duration
						MaxElapsedTime  time.Duration
						Multiplier      float64
					}{
						Enabled:         true,
						MaxRetry:        -1, // Unlimited retries
						InitialInterval: 50 * time.Millisecond,
						MaxInterval:     200 * time.Millisecond,
						MaxElapsedTime:  5 * time.Second,
						Multiplier:      2.0,
					},
				}
				client := NewClient("testClient", clientConfig)

				req, err := http.NewRequest("POST", server.URL, strings.NewReader("test data"))
				require.NoError(t, err)

				start := time.Now()
				resp, err := client.Do(req)
				elapsed := time.Since(start)

				require.NoError(t, err)
				require.Equal(t, tc.statusCode, resp.StatusCode)
				require.Equal(t, 1, requestCount, "Should make exactly 1 request")
				require.True(t, elapsed < 100*time.Millisecond, "Should complete quickly without retries")

				resp.Body.Close()
			})
		}
	})

	t.Run("does not retry 503 without X-Rudder-Should-Retry header", func(t *testing.T) {
		var requestCount int
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requestCount++
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("Service unavailable"))
		}))
		defer server.Close()

		clientConfig := &ClientConfig{
			ClientTimeout: 10 * time.Second,
			RetryRudderErrors: struct {
				Enabled         bool
				MaxRetry        int
				InitialInterval time.Duration
				MaxInterval     time.Duration
				MaxElapsedTime  time.Duration
				Multiplier      float64
			}{
				Enabled:         true,
				MaxRetry:        -1, // Unlimited retries
				InitialInterval: 50 * time.Millisecond,
				MaxInterval:     200 * time.Millisecond,
				MaxElapsedTime:  5 * time.Second,
				Multiplier:      2.0,
			},
		}
		client := NewClient("testClient", clientConfig)

		req, err := http.NewRequest("POST", server.URL, strings.NewReader("test data"))
		require.NoError(t, err)

		start := time.Now()
		resp, err := client.Do(req)
		elapsed := time.Since(start)

		require.NoError(t, err)
		require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
		require.Equal(t, 1, requestCount, "Should make exactly 1 request")
		require.True(t, elapsed < 100*time.Millisecond, "Should complete quickly without retries")

		resp.Body.Close()
	})
}

func TestClient_ErrorsNotRetried(t *testing.T) {
	t.Run("connection errors are not retried", func(t *testing.T) {
		unusedPort, err := kithelper.GetFreePort()
		require.NoError(t, err)
		url := fmt.Sprintf("http://localhost:%d", unusedPort)

		clientConfig := &ClientConfig{
			ClientTimeout: 1 * time.Second,
			RetryRudderErrors: struct {
				Enabled         bool
				MaxRetry        int
				InitialInterval time.Duration
				MaxInterval     time.Duration
				MaxElapsedTime  time.Duration
				Multiplier      float64
			}{
				Enabled:         true,
				MaxRetry:        0, // zero value gets overridden with default unlimited retries
				InitialInterval: 10 * time.Millisecond,
				MaxInterval:     50 * time.Millisecond,
				MaxElapsedTime:  500 * time.Millisecond,
				Multiplier:      2.0,
			},
		}
		client := NewClient("testClient", clientConfig)

		req, err := http.NewRequest("POST", url, strings.NewReader("test data"))
		require.NoError(t, err)

		start := time.Now()
		resp, err := client.Do(req)
		if resp != nil {
			defer resp.Body.Close()
		}
		elapsed := time.Since(start)

		require.Error(t, err)
		require.Nil(t, resp)

		require.True(t, elapsed < 1*time.Second, "Should fail quickly without retries")

		require.Contains(t, strings.ToLower(err.Error()), "connection")
	})

	t.Run("DNS resolution errors are not retried", func(t *testing.T) {
		url := "http://thisdoesnotexist.invalid.domain.com"

		clientConfig := &ClientConfig{
			ClientTimeout: 1 * time.Second,
			RetryRudderErrors: struct {
				Enabled         bool
				MaxRetry        int
				InitialInterval time.Duration
				MaxInterval     time.Duration
				MaxElapsedTime  time.Duration
				Multiplier      float64
			}{
				Enabled:         true,
				MaxRetry:        0, // zero value gets overridden with default unlimited retries
				InitialInterval: 10 * time.Millisecond,
				MaxInterval:     50 * time.Millisecond,
				MaxElapsedTime:  500 * time.Millisecond,
				Multiplier:      2.0,
			},
		}
		client := NewClient("testClient", clientConfig)

		req, err := http.NewRequest("POST", url, strings.NewReader("test data"))
		require.NoError(t, err)

		start := time.Now()
		resp, err := client.Do(req)
		if resp != nil {
			defer resp.Body.Close()
		}
		elapsed := time.Since(start)

		require.Error(t, err)
		require.Nil(t, resp)

		require.True(t, elapsed < 1*time.Second, "Should fail quickly without retries")
	})
}

func TestClient_ConfigurableRetrySettings(t *testing.T) {
	t.Run("respects custom retry configuration", func(t *testing.T) {
		var requestCount int
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requestCount++
			// Always return retriable error
			w.Header().Set(HeaderShouldRetry, "true")
			w.Header().Set(HeaderErrorReason, "test-overload")
			w.WriteHeader(http.StatusServiceUnavailable)
		}))
		defer server.Close()

		clientConfig := &ClientConfig{
			ClientTimeout: 10 * time.Second,
			RetryRudderErrors: struct {
				Enabled         bool
				MaxRetry        int
				InitialInterval time.Duration
				MaxInterval     time.Duration
				MaxElapsedTime  time.Duration
				Multiplier      float64
			}{
				Enabled:         true,
				MaxRetry:        1, // Only 1 retry
				InitialInterval: 10 * time.Millisecond,
				MaxInterval:     50 * time.Millisecond,
				MaxElapsedTime:  500 * time.Millisecond,
				Multiplier:      2.0,
			},
		}
		client := NewClient("testClient", clientConfig)

		req, err := http.NewRequest("POST", server.URL, strings.NewReader("test data"))
		require.NoError(t, err)

		start := time.Now()
		resp, err := client.Do(req)
		elapsed := time.Since(start)

		require.NoError(t, err, "Retryable client returns last response after max retries")
		require.NotNil(t, resp)
		require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)

		require.Equal(t, 2, requestCount, "Should make exactly 2 requests (initial + 1 retry)")
		require.True(t, elapsed < 1*time.Second, "Should complete quickly after max retries")

		resp.Body.Close()
	})

	t.Run("demonstrates when client returns error with memory-fenced", func(t *testing.T) {
		var requestCount int
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requestCount++
			w.Header().Set(HeaderShouldRetry, "true")
			w.Header().Set(HeaderErrorReason, "test-overload")
			w.WriteHeader(http.StatusServiceUnavailable)
		}))
		defer server.Close()

		clientConfig := &ClientConfig{
			ClientTimeout: 10 * time.Second,
			RetryRudderErrors: struct {
				Enabled         bool
				MaxRetry        int
				InitialInterval time.Duration
				MaxInterval     time.Duration
				MaxElapsedTime  time.Duration
				Multiplier      float64
			}{
				Enabled:         true,
				MaxRetry:        -1, // Allow unlimited retries but limit by time
				InitialInterval: 100 * time.Millisecond,
				MaxInterval:     200 * time.Millisecond,
				MaxElapsedTime:  1 * time.Millisecond, // Very short time to prevent retries
				Multiplier:      2.0,
			},
		}
		client := NewClient("testClient", clientConfig)

		req, err := http.NewRequest("POST", server.URL, strings.NewReader("test data"))
		require.NoError(t, err)

		resp, err := client.Do(req)

		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
		require.Equal(t, 1, requestCount, "Should make exactly 1 request due to very short MaxElapsedTime")

		resp.Body.Close()
	})
}

func TestClient_PerpetualRetriesStatsTags(t *testing.T) {
	t.Run("merges context tags into perpetual retry stat", func(t *testing.T) {
		var requestCount int
		retryableResponses := 2
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requestCount++
			if requestCount <= retryableResponses {
				w.Header().Set(HeaderShouldRetry, "true")
				w.Header().Set(HeaderErrorReason, "temporary-overload")
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		statsStore, err := memstats.New()
		require.NoError(t, err)
		originalStats := stats.Default
		stats.Default = statsStore
		defer func() { stats.Default = originalStats }()

		clientConfig := &ClientConfig{
			ClientTimeout: 10 * time.Second,
			RetryRudderErrors: struct {
				Enabled         bool
				MaxRetry        int
				InitialInterval time.Duration
				MaxInterval     time.Duration
				MaxElapsedTime  time.Duration
				Multiplier      float64
			}{
				Enabled:         true,
				MaxRetry:        -1,
				InitialInterval: 10 * time.Millisecond,
				MaxInterval:     50 * time.Millisecond,
				MaxElapsedTime:  5 * time.Second,
				Multiplier:      2.0,
			},
		}
		client := NewClient("testClient", clientConfig)

		ctx := WithPerpetualRetriesStatsTags(context.Background(), map[string]string{"language": "python"})
		req, err := http.NewRequestWithContext(ctx, "POST", server.URL, strings.NewReader("test data"))
		require.NoError(t, err)

		resp, err := client.Do(req)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		resp.Body.Close()

		metrics := statsStore.GetByName("transformer_client_perpetual_retry_count")
		require.Len(t, metrics, retryableResponses, "should have one perpetual retry stat per retryable response")
		for i, m := range metrics {
			require.Equal(t, "testClient", m.Tags["name"])
			require.Equal(t, "temporary-overload", m.Tags["reason"])
			require.Equal(t, "python", m.Tags["language"], "context tag should be propagated")
			require.NotEmpty(t, m.Tags["attempt"], "attempt %d", i)
		}
	})

	t.Run("does not add language tag when context has no extra tags", func(t *testing.T) {
		var requestCount int
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requestCount++
			if requestCount == 1 {
				w.Header().Set(HeaderShouldRetry, "true")
				w.Header().Set(HeaderErrorReason, "temporary-overload")
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		statsStore, err := memstats.New()
		require.NoError(t, err)
		originalStats := stats.Default
		stats.Default = statsStore
		defer func() { stats.Default = originalStats }()

		clientConfig := &ClientConfig{
			ClientTimeout: 10 * time.Second,
			RetryRudderErrors: struct {
				Enabled         bool
				MaxRetry        int
				InitialInterval time.Duration
				MaxInterval     time.Duration
				MaxElapsedTime  time.Duration
				Multiplier      float64
			}{
				Enabled:         true,
				MaxRetry:        -1,
				InitialInterval: 10 * time.Millisecond,
				MaxInterval:     50 * time.Millisecond,
				MaxElapsedTime:  5 * time.Second,
				Multiplier:      2.0,
			},
		}
		client := NewClient("testClient", clientConfig)

		req, err := http.NewRequest("POST", server.URL, strings.NewReader("test data"))
		require.NoError(t, err)

		resp, err := client.Do(req)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		resp.Body.Close()

		metrics := statsStore.GetByName("transformer_client_perpetual_retry_count")
		require.Len(t, metrics, 1)
		require.NotContains(t, metrics[0].Tags, "language")
	})
}

func TestClient_RetryDisabled(t *testing.T) {
	t.Run("does not retry when RetryRudderErrors.Enabled is false", func(t *testing.T) {
		var requestCount int
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requestCount++
			// Return retriable error that would normally be retried
			w.Header().Set(HeaderShouldRetry, "true")
			w.Header().Set(HeaderErrorReason, "temporary-overload")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("Service temporarily unavailable"))
		}))
		defer server.Close()

		clientConfig := &ClientConfig{
			ClientTimeout: 10 * time.Second,
			RetryRudderErrors: struct {
				Enabled         bool
				MaxRetry        int
				InitialInterval time.Duration
				MaxInterval     time.Duration
				MaxElapsedTime  time.Duration
				Multiplier      float64
			}{
				Enabled: false, // Explicitly disable retries
			},
		}
		client := NewClient("testClient", clientConfig)

		req, err := http.NewRequest("POST", server.URL, strings.NewReader("test data"))
		require.NoError(t, err)

		start := time.Now()
		resp, err := client.Do(req)
		elapsed := time.Since(start)

		require.NoError(t, err)
		require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
		require.Equal(t, 1, requestCount, "Should make exactly 1 request - no retries")
		require.True(t, elapsed < 100*time.Millisecond, "Should complete quickly without retries")

		resp.Body.Close()
	})
}

// TestClient_ResponseBodyReadableAfterRetriesExhausted pins the contract that [Client.Do] returns a response whose body
// the caller can still read.
//
// The retryable transport closes the previous response between attempts to avoid leaking connections, but the attempt
// that exhausts the budget is not followed by a retry — its response is handed back to the caller, so its body must
// stay open.
func TestClient_ResponseBodyReadableAfterRetriesExhausted(t *testing.T) {
	const responseBody = `{"error":"geolocation timeout"}`

	newRetryingServer := func() *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set(HeaderShouldRetry, "true")
			w.Header().Set(HeaderErrorReason, "geolocation_timeout")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(responseBody))
		}))
	}

	retryConfig := func(maxRetry int, maxElapsedTime time.Duration) *ClientConfig {
		return &ClientConfig{
			ClientTimeout: 10 * time.Second,
			RetryRudderErrors: struct {
				Enabled         bool
				MaxRetry        int
				InitialInterval time.Duration
				MaxInterval     time.Duration
				MaxElapsedTime  time.Duration
				Multiplier      float64
			}{
				Enabled:         true,
				MaxRetry:        maxRetry,
				InitialInterval: 10 * time.Millisecond,
				MaxInterval:     20 * time.Millisecond,
				MaxElapsedTime:  maxElapsedTime,
				Multiplier:      2.0,
			},
		}
	}

	t.Run("maxRetry exhausted", func(t *testing.T) {
		server := newRetryingServer()
		defer server.Close()

		client := NewClient("testClient", retryConfig(1, 500*time.Millisecond))

		req, err := http.NewRequest("POST", server.URL, strings.NewReader("test data"))
		require.NoError(t, err)

		resp, err := client.Do(req)
		require.NoError(t, err)
		require.NotNil(t, resp)
		defer func() { httputil.CloseResponse(resp) }()

		require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)

		read, err := io.ReadAll(resp.Body)
		require.NoError(t, err, "body of the returned response must still be readable")
		require.Equal(t, responseBody, string(read))
	})

	// Documents a limitation this package cannot fix: when the context is cancelled during a backoff wait,
	// retryablehttp returns the response of the last *completed* attempt — one its own notify hook already drained and
	// closed in order to retry — and pairs it with a nil error.
	// Callers therefore cannot treat a nil error from Do as "this body is readable"; see user_transformer's
	// forwardTest, which reports the status and error reason instead of the bare read failure.
	t.Run("context cancelled mid-backoff returns a stale, closed response", func(t *testing.T) {
		server := newRetryingServer()
		defer server.Close()

		// Unlimited retries, as in production: only the caller's context ends them.
		cfg := retryConfig(-1, 0)
		cfg.RetryRudderErrors.InitialInterval = 2 * time.Second
		cfg.RetryRudderErrors.MaxInterval = 2 * time.Second
		client := NewClient("testClient", cfg)

		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()

		req, err := http.NewRequestWithContext(ctx, "POST", server.URL, strings.NewReader("test data"))
		require.NoError(t, err)

		resp, err := client.Do(req)
		require.NoError(t, err, "Do reports no error even though the context ended the retries")
		require.NotNil(t, resp)
		defer func() { httputil.CloseResponse(resp) }()

		require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
		require.Equal(t, "true", resp.Header.Get(HeaderShouldRetry), "headers survive; only the body is gone")

		_, err = io.ReadAll(resp.Body)
		require.ErrorContains(t, err, "read on closed response body",
			"upstream behaviour: change this expectation if rudder-go-kit stops returning a stale response")
	})
}
