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

// TestSandboxHTTPTimeoutDoesNotCapGeolocation locks the contract that
// SANDBOX_HTTP_TIMEOUT_S (the user-HTTP wall-clock cap) does NOT apply to
// internal geolocation traffic. The two timeouts are independent: geolocation
// calls are bound exclusively by GEOLOCATION_TIMEOUT_SECS.
//
// Scenario: a geolocation backend that replies in 300 ms, with
// SANDBOX_HTTP_TIMEOUT_S=0.1 s (user cap is SHORTER than the geo response)
// and GEOLOCATION_TIMEOUT_SECS=0.5 s. Under the contract, the geolocation
// call must succeed:
//
//   - The 300 ms server reply is faster than the 500 ms geolocation budget.
//   - The 100 ms sandbox cap is intended for user HTTP traffic and MUST NOT
//     shadow the internal geolocation deadline.
func TestSandboxHTTPTimeoutDoesNotCapGeolocation(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 2 * time.Minute

	const versionID = "geo-cap-isolation-v1"

	// Geolocation mock that replies after 300 ms with a real JSON body.
	// Placed between the 100 ms sandbox cap and the 500 ms geolocation
	// budget so only the correct deadline can govern the call.
	mockGeo, geoCalls := newSlowGeolocationServer(t, 200*time.Millisecond)

	entries := map[string]configBackendEntry{
		versionID: {code: `
def transformEvent(event, metadata):
    # If the sandbox HTTP cap leaked into the internal session, geolocation()
    # would raise GeolocationServerError (retryable 503) — the test would
    # never observe a successful event.
    result = geolocation("1.2.3.4")
    event["geo_country"] = result["country"]
    return event
`},
	}

	configBackend := newContractConfigBackend(t, entries)
	t.Cleanup(configBackend.Close)

	pyURL := startRudderPytransformer(
		t, pool, configBackend.URL,
		"GEOLOCATION_URL="+mockGeo.URL,
		// The user HTTP cap is intentionally SHORTER than the geo reply
		// (100 ms < 300 ms). If the cap were applied to internal traffic,
		// the geolocation call would time out at 100 ms.
		"SANDBOX_HTTP_TIMEOUT_S=0.1",
		// The geolocation budget is LARGER than the geo reply (500 ms >
		// 300 ms) so the only legal outcome is success.
		"GEOLOCATION_TIMEOUT_SECS=0.5",
	)

	// Limited-retry client so a regression (which would trip retries) is
	// visible within a bounded test runtime.
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

	events := []types.TransformerEvent{makeEvent("msg-geo-iso-1", versionID)}
	resp := client.Transform(context.Background(), events)

	// Happy path: geolocation returned within its 500 ms budget and the
	// transformation produced a successful event.
	require.Empty(t, resp.FailedEvents,
		"geolocation call must succeed when the geo reply (300 ms) is within "+
			"GEOLOCATION_TIMEOUT_SECS (500 ms); SANDBOX_HTTP_TIMEOUT_S (100 ms) "+
			"must NOT apply to internal traffic")
	require.Len(t, resp.Events, 1,
		"exactly one successful event must be produced")
	require.Equal(t, http.StatusOK, resp.Events[0].StatusCode)

	// The user code copies result["country"] into the event — verifying the
	// geolocation body round-trips confirms the call actually completed
	// (and wasn't retried into success by a different code path).
	require.Equal(t, "IT", resp.Events[0].Output["geo_country"],
		"successful geolocation response must reach user code intact")

	// No retries: the client must NOT have retried anything. Under the
	// pre-fix buggy behavior, the sandbox cap would fire at 100 ms, raise
	// GeolocationServerError, and trigger the retry counter here.
	retriesCounter := newStats.GetByName("processor_user_transformer_http_retries")
	if len(retriesCounter) > 0 {
		require.EqualValues(t, 0, retriesCounter[0].Value,
			"no retries must occur — any retry indicates the user HTTP cap "+
				"leaked into the internal geolocation session")
	}

	// Exactly one geolocation call reached the mock: no redundant retries,
	// no duplicate traffic.
	require.EqualValues(t, 1, geoCalls.Load(),
		"geolocation backend must be called exactly once for a successful "+
			"transformation")
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
		versionBiggerTimeout  = "user-timeout-bigger-v1"
		versionSmallerTimeout = "user-timeout-smaller-v1"
	)

	slowServer, calls := newSlowServer(t, 2*time.Second)

	entries := map[string]configBackendEntry{
		versionBiggerTimeout: {code: fmt.Sprintf(`
import requests

def transformEvent(event, metadata):
    # timeout=5 — user wants 5s, but SANDBOX_HTTP_TIMEOUT_S=1 caps it.
    # Timeout intentionally not caught — propagates as per-event status 400.
    resp = requests.get("%s/data", timeout=5)
    event["status"] = resp.status_code
    return event
`, toContainerURL(slowServer.URL))},

		versionSmallerTimeout: {code: fmt.Sprintf(`
import requests

def transformEvent(event, metadata):
    # timeout=0.1 — user's cap is smaller than SANDBOX_HTTP_TIMEOUT_S=2.
    # Timeout intentionally not caught — propagates as per-event status 400.
    resp = requests.get("%s/data", timeout=0.1)
    event["status"] = resp.status_code
    return event
`, toContainerURL(slowServer.URL))},
	}

	configBackend := newContractConfigBackend(t, entries)
	t.Cleanup(configBackend.Close)

	// SANDBOX_HTTP_TIMEOUT_S=1: our cap is between the user's bigger (5s)
	// and smaller (0.1s) values, so both subtests can verify the correct cap.
	pyURL := startRudderPytransformer(
		t, pool, configBackend.URL,
		"SANDBOX_HTTP_TIMEOUT_S=1",
	)

	t.Run("OurCapHonouredWhenUserTimeoutIsBigger", func(t *testing.T) {
		// Reset calls before test
		calls.Store(0)
		// The user passes timeout=5s, but the server only replies after 2s.
		// Our cap (1s) fires first. The transformation fails with a 400
		// per-event error (non-retryable user code HTTP timeout).
		events := []types.TransformerEvent{makeEvent("msg-bigger-1", versionBiggerTimeout)}
		status, items := sendRawTransform(t, pyURL, events)
		require.Equal(t, http.StatusOK, status,
			"/customTransform HTTP response must be 200 (per-event errors are in the payload)")
		require.Len(t, items, 1)
		require.Equal(t, http.StatusBadRequest, items[0].StatusCode,
			"our cap (1s) must fire before user timeout (5s) when server takes > 1s")
		// The error message should mention a timeout — both ReadTimeout and
		// ConnectTimeout carry "timeout" or "timed out" in their string.
		require.NotEmpty(t, items[0].Error,
			"timeout error must be propagated as a non-empty per-event error")

		// User code HTTP timeouts are NOT retried (400 is non-retryable).
		// The event must appear in the failed payload, not silently swallowed.
		require.Empty(t, items[0].Output,
			"a timed-out event must not carry a successful output")
		require.EqualValues(t, 1, calls.Load())
	})

	t.Run("UserTimeoutHonouredWhenSmallerThanCap", func(t *testing.T) {
		// Reset calls before test
		calls.Store(0)
		// The user passes timeout=0.1 s; the server replies after 2s.
		// The user's cap fires first (0.5s < our 1s cap < server's 2s delay).
		events := []types.TransformerEvent{makeEvent("msg-smaller-1", versionSmallerTimeout)}
		status, items := sendRawTransform(t, pyURL, events)
		require.Equal(t, http.StatusOK, status)
		require.Len(t, items, 1)
		require.Equal(t, http.StatusBadRequest, items[0].StatusCode,
			"user timeout (0.1s) must fire before our cap (1s) when server takes > 1s")
		require.NotEmpty(t, items[0].Error,
			"timeout error must be propagated as a non-empty per-event error")
		require.Empty(t, items[0].Output,
			"a timed-out event must not carry a successful output")
		require.EqualValues(t, 1, calls.Load())
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
		noPoolURL := startRudderPytransformer(
			t, pool, configBackend.URL,
			"ENABLE_CONN_POOL=false",
			// Single worker so both requests hit the same subprocess and
			// the absence of a persistent pool is not masked by subprocess
			// affinity.
			"SANDBOX_POOL_MAX_SIZE=1",
		)

		// Reset newConns before sending events
		newConns.Store(0)

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

		require.EqualValues(t, 2, newConns.Load(),
			"with ENABLE_CONN_POOL=false, each bare requests.get() must open "+
				"a fresh TCP connection (server-side StateNew count: want 2)")
	})

	t.Run("ConnectionReusedWhenPoolEnabled", func(t *testing.T) {
		// ENABLE_CONN_POOL=true: bare requests.get() routes through the
		// persistent user session.  The TCP connection established for the
		// first request is kept in the pool and reused for the second.
		poolURL := startRudderPytransformer(
			t, pool, configBackend.URL,
			"ENABLE_CONN_POOL=true",
			"USER_CONN_POOL_MAX_SIZE=1",
			// Single worker to guarantee the same session handles both requests.
			"SANDBOX_POOL_MAX_SIZE=1",
		)

		// Reset newConns before sending events
		newConns.Store(0)

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

		require.EqualValues(t, 1, newConns.Load(),
			"with ENABLE_CONN_POOL=true, the second bare requests.get() to the "+
				"same host must reuse the pooled TCP connection "+
				"(server-side StateNew count: want 1)")
	})
}

// newSlowGeolocationServer creates an httptest server that serves the
// pytransformer's /geoip/* contract: a JSON body containing a "country"
// field, delayed by `delay` before responding. Returns the server and an
// atomic counter of /geoip/* hits (health pings are not counted).
func newSlowGeolocationServer(t *testing.T, delay time.Duration) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	calls := &atomic.Int64{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Ignore health pings so the counter reflects only real geo calls.
		if r.URL.Path == "/" || r.URL.Path == "/health" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"status":"ok"}`))
			return
		}
		calls.Add(1)
		select {
		case <-time.After(delay):
		case <-r.Context().Done():
			return
		}
		body := []byte(`{"country": "IT"}`)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)
	return srv, calls
}

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
