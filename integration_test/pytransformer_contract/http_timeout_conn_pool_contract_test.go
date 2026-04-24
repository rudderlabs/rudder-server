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

// TestSandboxHTTPBudgetDoesNotCapGeolocation locks the contract that
// SANDBOX_HTTP_BUDGET_S (the user-HTTP wall-clock cap) does NOT apply to
// internal geolocation traffic. The two budgets are independent: geolocation
// calls are bound exclusively by GEOLOCATION_TIMEOUT_SECS.
//
// Scenario: a geolocation backend that replies in 300 ms, with
// SANDBOX_HTTP_BUDGET_S=0.1 s (user cap is SHORTER than the geo response)
// and GEOLOCATION_TIMEOUT_SECS=0.5 s. Under the contract, the geolocation
// call must succeed:
//
//   - The 300 ms server reply is faster than the 500 ms geolocation budget.
//   - The 100 ms sandbox cap is intended for user HTTP traffic and MUST NOT
//     shadow the internal geolocation deadline.
func TestSandboxHTTPBudgetDoesNotCapGeolocation(t *testing.T) {
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
		"SANDBOX_HTTP_BUDGET_S=0.1",
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
			"GEOLOCATION_TIMEOUT_SECS (500 ms); SANDBOX_HTTP_BUDGET_S (100 ms) "+
			"must NOT apply to internal traffic")
	require.Len(t, resp.Events, 1,
		"exactly one successful event must be produced")
	require.Equal(t, http.StatusOK, resp.Events[0].StatusCode)

	// The user code copies result["country"] into the event — verifying the
	// geolocation body round-trips confirms the call actually completed
	// (and wasn't retried into success by a different code path).
	require.Equal(t, "IT", resp.Events[0].Output["geo_country"],
		"successful geolocation response must reach user code intact")

	// No retries: the client must NOT have retried anything. If the
	// sandbox cap applied to this internal call, it would fire at 100 ms,
	// raise GeolocationServerError, and trigger the retry counter here.
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
//   - When the user passes a timeout that is larger than SANDBOX_HTTP_BUDGET_S,
//     the sandbox cap is honoured (our cap fires, not the user's).
//   - When the user passes a timeout that is smaller than SANDBOX_HTTP_BUDGET_S,
//     the user's timeout is honoured (user's fires, not ours).
//
// Both cases result in a non-retryable 400 per-event error (user code HTTP
// timeout is not retried) — in contrast to the retryable 503 from a
// geolocation timeout.
//
// A single pytransformer container (SANDBOX_HTTP_BUDGET_S=2) is shared by
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
    # timeout=5 — user wants 5s, but SANDBOX_HTTP_BUDGET_S=1 caps it.
    # Timeout intentionally not caught — propagates as per-event status 400.
    resp = requests.get("%s/data", timeout=5)
    event["status"] = resp.status_code
    return event
`, toContainerURL(slowServer.URL))},

		versionSmallerTimeout: {code: fmt.Sprintf(`
import requests

def transformEvent(event, metadata):
    # timeout=0.1 — user's cap is smaller than SANDBOX_HTTP_BUDGET_S=2.
    # Timeout intentionally not caught — propagates as per-event status 400.
    resp = requests.get("%s/data", timeout=0.1)
    event["status"] = resp.status_code
    return event
`, toContainerURL(slowServer.URL))},
	}

	configBackend := newContractConfigBackend(t, entries)
	t.Cleanup(configBackend.Close)

	// SANDBOX_HTTP_BUDGET_S=1: our cap is between the user's bigger (5s)
	// and smaller (0.1s) values, so both subtests can verify the correct cap.
	pyURL := startRudderPytransformer(
		t, pool, configBackend.URL,
		"SANDBOX_HTTP_BUDGET_S=1",
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

// TestConnectionPoolBehavior verifies bare requests.get() TCP reuse.
//
// Pooling of user HTTP traffic is mandatory: every call through
// requests.get/post/... routes through the process-shared pooled
// session, so sequential calls against the same host reuse the same
// TCP connection.
//
// Connection reuse is verified server-side using the Go http.Server
// ConnState hook: http.StateNew fires exactly once per TCP handshake.
// Two requests on a kept-alive connection produce only one StateNew event.
func TestConnectionPoolBehavior(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 2 * time.Minute

	const versionID = "conn-pool-check-v1"

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

	poolURL := startRudderPytransformer(
		t, pool, configBackend.URL,
		"USER_CONN_POOL_MAX_SIZE=1",
		// Single worker so both requests hit the same subprocess
		// and share the same pooled session.
		"SANDBOX_POOL_MAX_SIZE=1",
	)

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
		"the second bare requests.get() to the same host must reuse the "+
			"pooled TCP connection (server-side StateNew count: want 1)")
}

// TestConnectionPoolSharedAcrossTransformations locks the contract that
// two sequential /customTransform requests with different
// transformationVersionIds — served by the SAME worker subprocess
// (SANDBOX_POOL_MAX_SIZE=1) against the SAME host — SHARE a TCP
// connection.
//
// Pytransformer is deployed one container per customer, so in-process
// isolation between versions of the same tenant is not a requirement:
// a single process-shared pooled session serves every transformation
// in a subprocess. TLS / cookie footguns that could otherwise leak via
// shared session state are handled by “StatelessPooledSession“
// (strips cookies) and “ManagedSession“ (rejects TLS defaults,
// honours per-request “verify=“ / “cert=“).
//
// Connection reuse is verified server-side using the Go http.Server
// ConnState hook: http.StateNew fires exactly once per TCP handshake.
// Two requests across version_ids on the same kept-alive socket
// produce a single StateNew event.
func TestConnectionPoolSharedAcrossTransformations(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 2 * time.Minute

	const (
		versionIDAlpha = "shared-pool-alpha"
		versionIDBeta  = "shared-pool-beta"
	)

	trackSrv, newConns := newConnectionCountingServer(t)

	// Both versions hit the same URL. The transformation code is
	// identical — the ONLY thing that differs is the versionID they
	// are registered under, so observing a single TCP connection
	// server-side is direct evidence that both tids use the same
	// underlying pool.
	code := fmt.Sprintf(`
import requests

def transformEvent(event, metadata):
    resp = requests.get("%s/check")
    event["ok"] = resp.status_code == 200
    return event
`, toContainerURL(trackSrv.URL))

	entries := map[string]configBackendEntry{
		versionIDAlpha: {code: code},
		versionIDBeta:  {code: code},
	}

	configBackend := newContractConfigBackend(t, entries)
	t.Cleanup(configBackend.Close)

	sharedURL := startRudderPytransformer(
		t, pool, configBackend.URL,
		"USER_CONN_POOL_MAX_SIZE=1",
		// Pin to a single worker so both events land in the same
		// subprocess — otherwise the shared-pool hypothesis is not
		// exercised at all and the assertion is vacuous.
		"SANDBOX_POOL_MAX_SIZE=1",
	)

	newConns.Store(0)

	ev1 := makeEvent("msg-shared-alpha", versionIDAlpha)
	status1, items1 := sendRawTransform(t, sharedURL, []types.TransformerEvent{ev1})
	require.Equal(t, http.StatusOK, status1)
	require.Len(t, items1, 1)
	require.Equal(t, http.StatusOK, items1[0].StatusCode, "alpha request must succeed")

	ev2 := makeEvent("msg-shared-beta", versionIDBeta)
	status2, items2 := sendRawTransform(t, sharedURL, []types.TransformerEvent{ev2})
	require.Equal(t, http.StatusOK, status2)
	require.Len(t, items2, 1)
	require.Equal(t, http.StatusOK, items2[0].StatusCode, "beta request must succeed")

	require.EqualValues(t, 1, newConns.Load(),
		"two requests under distinct transformation_version_ids must share "+
			"a single TCP connection through the process-shared pool — "+
			"server-side StateNew count: want 1, got %d", newConns.Load())
}

// TestSlowDripBodyFiresSandboxHTTPBudget locks in the contract that a server
// which flushes response headers quickly and then drips the body one byte at
// a time must NOT pin a sandbox worker for longer than SANDBOX_HTTP_BUDGET_S.
//
// Scenario:
//   - Body is 30 chunks × 200 ms = 6.0 s of wall-clock drip.
//   - SANDBOX_HTTP_BUDGET_S = 1 s.
//   - SANDBOX_TRANSFORMATION_TIMEOUT_S is set high enough that the
//     subprocess-level SIGVTALRM safety net does NOT fire first — otherwise
//     we would be measuring the wrong budget.
//
// Legal outcomes are narrow: the per-event statusCode must be 400 (HTTP
// timeout surfaces as a user-code failure), and the request must complete
// well inside the full 6s body duration. Anything else means the slow-drip
// body is still capable of pinning the worker.
func TestSlowDripBodyFiresSandboxHTTPBudget(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 2 * time.Minute

	const versionID = "slow-drip-body-v1"

	slowSrv := newSlowDripBodyServer(t, 30, 200*time.Millisecond)

	entries := map[string]configBackendEntry{
		versionID: {code: fmt.Sprintf(`
import requests

def transformEvent(event, metadata):
    # No explicit timeout — let SANDBOX_HTTP_BUDGET_S cap the wall-clock budget.
    resp = requests.get("%s/drip")
    event["status"] = resp.status_code
    event["body_len"] = len(resp.content)
    return event
`, toContainerURL(slowSrv.URL))},
	}

	configBackend := newContractConfigBackend(t, entries)
	t.Cleanup(configBackend.Close)

	// SANDBOX_HTTP_BUDGET_S=1 < 6s body drip → our cap must fire.
	// SANDBOX_TRANSFORMATION_TIMEOUT_S=20 / SANDBOX_PROCESS_TIMEOUT_S=30 keep
	// the subprocess-level deadlines far above any plausible cap firing time,
	// so the only thing that can cause a per-event 400 here is the HTTP-level
	// wall-clock deadline we are testing.
	pyURL := startRudderPytransformer(
		t, pool, configBackend.URL,
		"SANDBOX_HTTP_BUDGET_S=1",
		"SANDBOX_TRANSFORMATION_TIMEOUT_S=20",
		"SANDBOX_PROCESS_TIMEOUT_S=30",
	)

	events := []types.TransformerEvent{makeEvent("msg-slow-drip-1", versionID)}

	start := time.Now()
	status, items := sendRawTransform(t, pyURL, events)
	elapsed := time.Since(start)
	t.Logf("slow-drip request elapsed: %s", elapsed)

	require.Equal(t, http.StatusOK, status,
		"/customTransform HTTP response must be 200 (per-event errors live in the payload)")
	require.Len(t, items, 1, "exactly one per-event result expected")

	require.Equal(t, http.StatusBadRequest, items[0].StatusCode,
		"slow-drip body must fire SANDBOX_HTTP_BUDGET_S and surface as a 400 per-event error; "+
			"statusCode=%d error=%q", items[0].StatusCode, items[0].Error)
	require.NotEmpty(t, items[0].Error,
		"timeout error message must be propagated to the caller")
	require.Empty(t, items[0].Output,
		"a timed-out event must not carry a successful transformation output")

	// Wall-clock budget: without the body-read deadline, the worker would
	// be pinned for the full 6s body duration. The cap must fire within
	// ~1s + overhead, comfortably below 4s — and critically far below the
	// 6s drip ceiling.
	// Using 4 s as the bound keeps the assertion resilient to normal Docker
	// container-start jitter while still catching any regression that
	// reverts the deadline wrapper.
	require.Less(t, elapsed, 4*time.Second,
		"SANDBOX_HTTP_BUDGET_S must cap the wall-clock budget for slow-drip "+
			"body reads; elapsed=%s (uncapped would be ~6s)", elapsed)
}

// newSlowDripBodyServer creates an httptest server that flushes response
// headers immediately, then trickles the body one byte at a time using
// HTTP/1.1 chunked transfer encoding, pausing “delay“ between each chunk.
//
// The header flush happens before any sleep so urllib3's “urlopen“ (which
// caps the time-to-first-byte) returns fast — the attack surface is the body
// read phase. The connection is closed after the final chunk so the test
// server shuts down cleanly when the httptest.Server is torn down.
func newSlowDripBodyServer(t *testing.T, chunks int, delay time.Duration) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		// ``Connection: close`` prevents the handler from hanging in a
		// keep-alive loop after the response completes — important for
		// clean test shutdown when the client succeeds in reading the
		// whole body (the bypass / happy path).
		w.Header().Set("Connection", "close")
		w.WriteHeader(http.StatusOK)
		flusher, ok := w.(http.Flusher)
		if !ok {
			t.Error("response writer is not a flusher")
			return
		}
		// Flush headers immediately so urlopen returns fast. Everything
		// after this point exercises the body-read deadline.
		flusher.Flush()
		for range chunks {
			select {
			case <-time.After(delay):
			case <-r.Context().Done():
				// Client gave up (deadline fired upstream). Stop dripping
				// to free the handler goroutine promptly.
				return
			}
			if _, err := w.Write([]byte("a")); err != nil {
				return
			}
			flusher.Flush()
		}
	}))
	t.Cleanup(srv.Close)
	return srv
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
