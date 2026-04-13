package pytransformer_contract

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"

	"github.com/rudderlabs/rudder-server/processor/types"
	"github.com/rudderlabs/rudder-server/processor/usertransformer"
)

// newSlowServer creates an HTTP server that delays every response by delay.
// Returns the server (already started) and an atomic call counter.
// The handler responds HTTP 200 with `{"ok": true}` after the delay,
// or stops early if the request context is cancelled (client gave up).
func newSlowServer(t *testing.T, delay time.Duration) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	calls := &atomic.Int64{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		select {
		case <-time.After(delay):
		case <-r.Context().Done():
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok": true}`))
	}))
	t.Cleanup(srv.Close)
	return srv, calls
}

// newConnectionCountingServer creates an HTTP server that uses ConnState to
// count the number of new TCP connections it accepts.  Every time the Go HTTP
// server transitions a connection to http.StateNew (i.e. a fresh TCP handshake
// completed), the atomic counter is incremented.  Subsequent HTTP requests on
// the same keep-alive TCP connection do NOT trigger http.StateNew, so the
// counter is a reliable proxy for "how many distinct TCP connections were
// opened."
//
// The handler responds HTTP 200 with a small JSON body and sets
// Content-Length so the client knows the response is complete and can keep
// the connection alive for the next request without chunked transfer.
func newConnectionCountingServer(t *testing.T) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	newConns := &atomic.Int64{}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := []byte(`{"ok": true}`)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	})
	srv := httptest.NewUnstartedServer(handler)
	srv.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			newConns.Add(1)
		}
	}
	srv.Start()
	t.Cleanup(srv.Close)
	return srv, newConns
}

// TestGeoTimeoutWithShorterHTTPTimeoutCap locks the contract that a slow
// geolocation backend is retried even when SANDBOX_HTTP_TIMEOUT_S is shorter
// than GEOLOCATION_TIMEOUT_SECS.
//
// When the urllib3 wall-clock cap fires during a geolocation() call, the
// resulting ReadTimeoutError is caught by the geolocation() function and
// re-raised as GeolocationServerError (a BaseException subclass). This
// bypasses the user's `except Exception` block and surfaces to the worker as
// a retryable HTTP 503 — identical to the behaviour when
// GEOLOCATION_TIMEOUT_SECS fires first.
//
// The test distinguishes this from a user code HTTP timeout (400,
// non-retryable) by verifying that the response carries a 503 status and
// that the client's retry counter increments.
func TestGeoTimeoutWithShorterHTTPTimeoutCap(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 2 * time.Minute

	const versionID = "geo-short-http-timeout-v1"

	// Mock geolocation server that responds after 300 ms — well above the
	// 100 ms HTTP cap but below the 500 ms geolocation timeout.  The urllib3
	// wall-clock cap fires at 100 ms, before the server answers.
	mockGeo, mockGeoCfg := newConfigurableMockGeolocationService(t)
	mockGeoCfg.setSlow(300 * time.Millisecond)

	entries := map[string]configBackendEntry{
		versionID: {code: `
def transformEvent(event, metadata):
    try:
        result = geolocation("1.2.3.4")
        event["geo"] = result
    except Exception as e:
        # Must NOT reach this branch: GeolocationServerError inherits
        # BaseException, so it bypasses except-Exception and propagates
        # to the worker as a retryable 503.
        event["geo_error"] = str(e)
    return event
`},
	}

	configBackend := newContractConfigBackend(t, entries)
	t.Cleanup(configBackend.Close)

	container, pyURL := startRudderPytransformer(
		t, pool, configBackend.URL,
		// geolocation URL must be reachable from inside the container
		"GEOLOCATION_URL="+mockGeo.URL,
		// HTTP cap is intentionally shorter than GEOLOCATION_TIMEOUT_SECS
		"SANDBOX_HTTP_TIMEOUT_S=0.1",
		"GEOLOCATION_TIMEOUT_SECS=0.5",
	)
	t.Cleanup(func() {
		if err := pool.Purge(container); err != nil {
			t.Logf("Failed to purge pytransformer container: %v", err)
		}
	})
	waitForHealthy(t, pool, pyURL, "pytransformer", container)

	// Build a new-arch client with limited retries so the test terminates
	// quickly and we can inspect the retry counter.
	newStats, err := memstats.New()
	require.NoError(t, err)
	conf := config.New()
	conf.Set("PYTHON_TRANSFORM_URL", pyURL)
	conf.Set("Processor.UserTransformer.maxRetry", 2)
	conf.Set("Processor.UserTransformer.cpDownEndlessRetries", false)
	conf.Set("Processor.UserTransformer.maxRetryBackoffInterval", 1*time.Millisecond)
	conf.Set("Processor.UserTransformer.failOnError", true)
	conf.Set("Transformer.Client.UserTransformer.retryRudderErrors.maxRetry", 2)
	conf.Set("Transformer.Client.UserTransformer.retryRudderErrors.maxInterval", 1*time.Millisecond)
	client := usertransformer.New(conf, logger.NOP, newStats)

	events := []types.TransformerEvent{makeEvent("msg-geo-short-1", versionID)}
	resp := client.Transform(context.Background(), events)

	// The geolocation() call times out (urllib3 HTTP cap fires at 100 ms).
	// GeolocationServerError bypasses the user's except-Exception block, so
	// no success event is produced.
	require.Len(t, resp.Events, 0,
		"geolocation timeout must NOT produce a success event: "+
			"GeolocationServerError must bypass user except-Exception")

	// The worker returns 503 retryable.  After 2 retries the client gives up
	// and surfaces the failure in FailedEvents.
	require.Len(t, resp.FailedEvents, 1,
		"geolocation timeout must surface as a failed event after retries are exhausted")
	require.Contains(t, resp.FailedEvents[0].Error, "503",
		"failed event must carry the 503 error from the retried geolocation timeout")

	// Verify that 2 retries actually happened — this is what proves the
	// failure was treated as a geolocation timeout (retryable) and NOT as a
	// user code HTTP timeout (non-retryable 400, zero retries).
	retriesCounter := newStats.GetByName("processor_user_transformer_http_retries")
	require.Len(t, retriesCounter, 1,
		"http_retries counter must be recorded")
	require.EqualValues(t, 2, retriesCounter[0].Value,
		"geolocation timeout must trigger exactly 2 retries before failing")
}

// TestUserHTTPTimeoutCapping verifies the urllib3 wall-clock cap behaviour for
// user-initiated HTTP calls:
//
//   - When the user passes a timeout that is larger than SANDBOX_HTTP_TIMEOUT_S,
//     the sandbox cap is honoured (our cap fires, not the user's).
//   - When the user passes a timeout that is smaller than SANDBOX_HTTP_TIMEOUT_S,
//     the user's timeout is honoured (user's fires, not ours).
//
// Both cases result in a non-retryable 400 per-event error (user code HTTP
// timeout is not retried) — in contrast to the retryable 503 from a
// geolocation timeout.
//
// A single pytransformer container (SANDBOX_HTTP_TIMEOUT_S=2) is shared by
// both subtests.
func TestUserHTTPTimeoutCapping(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 2 * time.Minute

	const (
		versionBigger  = "user-timeout-bigger-v1"
		versionSmaller = "user-timeout-smaller-v1"
	)

	// slowSrv3s: responds after 3s — longer than SANDBOX_HTTP_TIMEOUT_S (2s)
	// but shorter than the user's requested timeout (10s).  Our cap fires.
	slowSrv3s, _ := newSlowServer(t, 3*time.Second)

	// slowSrv1s: responds after 1s — longer than the user's requested timeout
	// (0.5s) but shorter than SANDBOX_HTTP_TIMEOUT_S (2s).  User's cap fires.
	slowSrv1s, _ := newSlowServer(t, 1*time.Second)

	entries := map[string]configBackendEntry{
		versionBigger: {code: fmt.Sprintf(`
import requests

def transformEvent(event, metadata):
    # timeout=10 — user wants 10 s, but SANDBOX_HTTP_TIMEOUT_S=2 caps it.
    # Timeout intentionally not caught — propagates as per-event status 400.
    resp = requests.get("%s/data", timeout=10)
    event["status"] = resp.status_code
    return event
`, toContainerURL(slowSrv3s.URL))},

		versionSmaller: {code: fmt.Sprintf(`
import requests

def transformEvent(event, metadata):
    # timeout=0.5 — user's cap is smaller than SANDBOX_HTTP_TIMEOUT_S=2.
    # Timeout intentionally not caught — propagates as per-event status 400.
    resp = requests.get("%s/data", timeout=0.5)
    event["status"] = resp.status_code
    return event
`, toContainerURL(slowSrv1s.URL))},
	}

	configBackend := newContractConfigBackend(t, entries)
	t.Cleanup(configBackend.Close)

	// SANDBOX_HTTP_TIMEOUT_S=2: our cap is between the user's bigger (10s)
	// and smaller (0.5s) values, so both subtests can verify the correct cap.
	container, pyURL := startRudderPytransformer(
		t, pool, configBackend.URL,
		"SANDBOX_HTTP_TIMEOUT_S=2",
	)
	t.Cleanup(func() {
		if err := pool.Purge(container); err != nil {
			t.Logf("Failed to purge pytransformer container: %v", err)
		}
	})
	waitForHealthy(t, pool, pyURL, "pytransformer", container)

	t.Run("OurCapHonouredWhenUserTimeoutIsBigger", func(t *testing.T) {
		// The user passes timeout=10 s, but the server only replies after 3s.
		// Our cap (2s) fires first.  The transformation fails with a 400
		// per-event error (non-retryable user code HTTP timeout).
		events := []types.TransformerEvent{makeEvent("msg-bigger-1", versionBigger)}
		status, items := sendRawTransform(t, pyURL, events)
		require.Equal(t, http.StatusOK, status,
			"/customTransform HTTP response must be 200 (per-event errors are in the payload)")
		require.Len(t, items, 1)
		require.Equal(t, http.StatusBadRequest, items[0].StatusCode,
			"our cap (2s) must fire before user timeout (10s) when server takes 3s")
		// The error message should mention a timeout — both ReadTimeout and
		// ConnectTimeout carry "timeout" or "timed out" in their string.
		require.NotEmpty(t, items[0].Error,
			"timeout error must be propagated as a non-empty per-event error")

		// User code HTTP timeouts are NOT retried (400 is non-retryable).
		// The event must appear in the failed payload, not silently swallowed.
		require.Empty(t, items[0].Output,
			"a timed-out event must not carry a successful output")
	})

	t.Run("UserTimeoutHonouredWhenSmallerThanCap", func(t *testing.T) {
		// The user passes timeout=0.5 s; the server replies after 1s.
		// The user's cap fires first (0.5s < our 2s cap < server's 1s delay).
		events := []types.TransformerEvent{makeEvent("msg-smaller-1", versionSmaller)}
		status, items := sendRawTransform(t, pyURL, events)
		require.Equal(t, http.StatusOK, status)
		require.Len(t, items, 1)
		require.Equal(t, http.StatusBadRequest, items[0].StatusCode,
			"user timeout (0.5s) must fire before our cap (2s) when server takes 1s")
		require.NotEmpty(t, items[0].Error,
			"timeout error must be propagated as a non-empty per-event error")
		require.Empty(t, items[0].Output,
			"a timed-out event must not carry a successful output")
	})
}

// TestConnectionPoolBehavior verifies that the ENABLE_CONN_POOL feature flag
// controls whether bare requests.get() calls reuse TCP connections.
//
//   - ENABLE_CONN_POOL=false (default): each call opens a fresh TCP connection.
//   - ENABLE_CONN_POOL=true:            repeated calls to the same host reuse
//     the pooled TCP connection.
//
// SANDBOX_POOL_MAX_SIZE=1 ensures a single worker subprocess handles every
// request, so the per-process user session (and its connection pool) is shared
// across the two sequential test requests.
//
// Connection reuse is verified server-side using the Go http.Server ConnState
// hook: http.StateNew fires exactly once per TCP handshake.  Two requests on
// a kept-alive connection produce only one StateNew event; two requests on
// fresh connections produce two.
func TestConnectionPoolBehavior(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 2 * time.Minute

	const versionID = "conn-pool-check-v1"

	// A single connection-counting server shared by both subtests.
	// Each subtest reads the delta to avoid interference.
	trackSrv, newConns := newConnectionCountingServer(t)

	entries := map[string]configBackendEntry{
		versionID: {code: fmt.Sprintf(`
import requests

def transformEvent(event, metadata):
    resp = requests.get("%s/check")
    event["ok"] = resp.status_code == 200
    return event
`, toContainerURL(trackSrv.URL))},
	}

	configBackend := newContractConfigBackend(t, entries)
	t.Cleanup(configBackend.Close)

	t.Run("NewConnectionPerRequestWhenPoolDisabled", func(t *testing.T) {
		// ENABLE_CONN_POOL=false (default): bare requests.get() creates a
		// temporary Session per call, then closes it — so every call opens a
		// fresh TCP connection even against a keep-alive-capable server.
		noPoolContainer, noPoolURL := startRudderPytransformer(
			t, pool, configBackend.URL,
			"ENABLE_CONN_POOL=false",
			// Single worker so both requests hit the same subprocess and
			// the absence of a persistent pool is not masked by subprocess
			// affinity.
			"SANDBOX_POOL_MAX_SIZE=1",
		)
		t.Cleanup(func() {
			if err := pool.Purge(noPoolContainer); err != nil {
				t.Logf("Failed to purge no-pool pytransformer: %v", err)
			}
		})
		waitForHealthy(t, pool, noPoolURL, "pytransformer (no-pool)", noPoolContainer)

		before := newConns.Load()

		ev1 := makeEvent("msg-nopool-1", versionID)
		status1, items1 := sendRawTransform(t, noPoolURL, []types.TransformerEvent{ev1})
		require.Equal(t, http.StatusOK, status1)
		require.Len(t, items1, 1)
		require.Equal(t, http.StatusOK, items1[0].StatusCode, "first request must succeed")

		ev2 := makeEvent("msg-nopool-2", versionID)
		status2, items2 := sendRawTransform(t, noPoolURL, []types.TransformerEvent{ev2})
		require.Equal(t, http.StatusOK, status2)
		require.Len(t, items2, 1)
		require.Equal(t, http.StatusOK, items2[0].StatusCode, "second request must succeed")

		delta := newConns.Load() - before
		require.EqualValues(t, 2, delta,
			"with ENABLE_CONN_POOL=false, each bare requests.get() must open "+
				"a fresh TCP connection (server-side StateNew count: want 2)")
	})

	t.Run("ConnectionReusedWhenPoolEnabled", func(t *testing.T) {
		// ENABLE_CONN_POOL=true: bare requests.get() routes through the
		// persistent user session.  The TCP connection established for the
		// first request is kept in the pool and reused for the second.
		poolContainer, poolURL := startRudderPytransformer(
			t, pool, configBackend.URL,
			"ENABLE_CONN_POOL=true",
			"USER_CONN_POOL_MAX_SIZE=1",
			// Single worker to guarantee the same session handles both requests.
			"SANDBOX_POOL_MAX_SIZE=1",
		)
		t.Cleanup(func() {
			if err := pool.Purge(poolContainer); err != nil {
				t.Logf("Failed to purge with-pool pytransformer: %v", err)
			}
		})
		waitForHealthy(t, pool, poolURL, "pytransformer (with-pool)", poolContainer)

		before := newConns.Load()

		ev1 := makeEvent("msg-pool-1", versionID)
		status1, items1 := sendRawTransform(t, poolURL, []types.TransformerEvent{ev1})
		require.Equal(t, http.StatusOK, status1)
		require.Len(t, items1, 1)
		require.Equal(t, http.StatusOK, items1[0].StatusCode, "first request must succeed")

		ev2 := makeEvent("msg-pool-2", versionID)
		status2, items2 := sendRawTransform(t, poolURL, []types.TransformerEvent{ev2})
		require.Equal(t, http.StatusOK, status2)
		require.Len(t, items2, 1)
		require.Equal(t, http.StatusOK, items2[0].StatusCode, "second request must succeed")

		delta := newConns.Load() - before
		require.EqualValues(t, 1, delta,
			"with ENABLE_CONN_POOL=true, the second bare requests.get() to the "+
				"same host must reuse the pooled TCP connection "+
				"(server-side StateNew count: want 1)")
	})
}
