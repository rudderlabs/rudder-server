package transformerclient

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

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
				w.Header().Set("X-Rudder-Should-Retry", "true")
				w.Header().Set("X-Rudder-Error-Reason", "temporary-overload")
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
		client := NewClient(clientConfig)

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
			w.Header().Set("X-Rudder-Should-Retry", "true")
			w.Header().Set("X-Rudder-Error-Reason", "persistent-overload")
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
		client := NewClient(clientConfig)

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
				w.Header().Set("X-Rudder-Should-Retry", "true")
				w.Header().Set("X-Rudder-Error-Reason", "temporary-overload")
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
		client := NewClient(clientConfig)

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
				client := NewClient(clientConfig)

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
		client := NewClient(clientConfig)

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
				MaxRetry:        0, // No retries
				InitialInterval: 10 * time.Millisecond,
				MaxInterval:     50 * time.Millisecond,
				MaxElapsedTime:  500 * time.Millisecond,
				Multiplier:      2.0,
			},
		}
		client := NewClient(clientConfig)

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
				MaxRetry:        0, // No retries
				InitialInterval: 10 * time.Millisecond,
				MaxInterval:     50 * time.Millisecond,
				MaxElapsedTime:  500 * time.Millisecond,
				Multiplier:      2.0,
			},
		}
		client := NewClient(clientConfig)

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
			w.Header().Set("X-Rudder-Should-Retry", "true")
			w.Header().Set("X-Rudder-Error-Reason", "test-overload")
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
		client := NewClient(clientConfig)

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
			w.Header().Set("X-Rudder-Should-Retry", "true")
			w.Header().Set("X-Rudder-Error-Reason", "test-overload")
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
		client := NewClient(clientConfig)

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

func TestClient_RetryDisabled(t *testing.T) {
	t.Run("does not retry when RetryRudderErrors.Enabled is false", func(t *testing.T) {
		var requestCount int
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requestCount++
			// Return retriable error that would normally be retried
			w.Header().Set("X-Rudder-Should-Retry", "true")
			w.Header().Set("X-Rudder-Error-Reason", "temporary-overload")
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
		client := NewClient(clientConfig)

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
