package pytransformer_contract

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/processor/types"
)

// TestManagedSessionContract locks in the wire-observable contract enforced by
// ManagedSession / ManagedHTTPAdapter.
//
// Unit-level coverage of Prometheus counter emission already lives in
// tests/test_metrics_e2e.py::TestManagedSessionMetricsE2E on the pytransformer
// side. This contract test asserts the behaviours that only a real Docker
// container against a real HTTP server can validate:
//
//   - user-created requests.Session()s actually flow through the pooled
//     session (TCP connection reuse observable server-side);
//   - retry budgets are bounded by MANAGED_MAX_RETRIES_CAP, never by the
//     user-supplied Retry.total;
//   - session-level verify=False is silently ignored, so TLS verification
//     remains enforced on the wire;
//   - per-request verify=False is still honoured as the documented escape
//     hatch;
//   - HTTPAdapter subclasses with custom send() are not invoked;
//   - mount-prefix isolation: a retry policy mounted for prefix A does not
//     apply to prefix B;
//   - per-session headers/auth are preserved across send() calls;
//   - Session.close() does not tear down the platform pooled session;
//   - CONN_POOL_PER_TRANSFORMATION=true partitions managed sessions by
//     transformation_version_id even when one worker handles both events;
//   - the specific customer pattern behind 3CRQYhrDplkTP3nHlLq5VYlNWJh —
//     Session() + HTTPAdapter(max_retries=Retry(...)) using the
//     requests.packages.urllib3.util.retry import path — works end-to-end.
func TestManagedSessionContract(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 2 * time.Minute

	t.Run("CustomerReproPackagesRetryPath", func(t *testing.T) {
		// Scenario 1 + 11: customer 3CRQYhrDplkTP3nHlLq5VYlNWJh pattern —
		// module-level Session() + mount(HTTPAdapter(max_retries=Retry(...)))
		// using the `requests.packages.urllib3.util.retry` import path.
		// The server returns 500 on the first attempt, 200 afterwards; we
		// assert the retry policy is honoured (200 eventually) but the
		// attempt count is bounded by MANAGED_MAX_RETRIES_CAP, not by the
		// user's Retry(total=5).
		const versionID = "mgd-cust-repro-v1"

		statusSrv, attempts := newSequencedServer(t, []int{
			http.StatusInternalServerError,
			http.StatusOK,
		})

		entries := map[string]configBackendEntry{
			versionID: {code: fmt.Sprintf(`
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

retry_strategy = Retry(total=5, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=['GET'])
adapter = HTTPAdapter(max_retries=retry_strategy)
http = requests.Session()
http.mount("%s", adapter)

def transformEvent(event, metadata):
    resp = http.get("%s/data", params={'k': 'v'})
    event["status"] = resp.status_code
    event["body"] = resp.text
    return event
`, toContainerURL(statusSrv.URL), toContainerURL(statusSrv.URL))},
		}

		configBackend := newContractConfigBackend(t, entries)
		t.Cleanup(configBackend.Close)

		pyURL := startRudderPytransformer(
			t, pool, configBackend.URL,
			"SANDBOX_HTTP_TIMEOUT_S=5",
			"SANDBOX_POOL_MAX_SIZE=1",
		)

		events := []types.TransformerEvent{makeEvent("msg-repro-1", versionID)}
		status, items := sendRawTransform(t, pyURL, events)
		require.Equal(t, http.StatusOK, status)
		require.Len(t, items, 1)
		require.Equal(t, http.StatusOK, items[0].StatusCode,
			"customer repro must succeed after one retry: error=%q", items[0].Error)

		require.EqualValues(t, 2, attempts.Load(),
			"expected exactly 2 attempts (initial 500 + 1 retry); got %d", attempts.Load())
	})

	t.Run("RetryBudgetBoundedByManagedCap", func(t *testing.T) {
		// Scenario 3: user asks for Retry(total=10) against an always-500
		// server. The platform clamps to MANAGED_MAX_RETRIES_CAP (=1), so
		// the server must see at most 2 attempts (initial + 1 retry), never
		// the full 11.
		const versionID = "mgd-retry-bounded-v1"

		always500, attempts := newAlways500Server(t)

		entries := map[string]configBackendEntry{
			versionID: {code: fmt.Sprintf(`
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

def transformEvent(event, metadata):
    s = requests.Session()
    s.mount("%s", HTTPAdapter(max_retries=Retry(
        total=10, status_forcelist=[500], backoff_factor=0.01, allowed_methods=['GET'])))
    # urllib3 raises on retry-budget exhaustion against status_forcelist; catching
    # lets the transformation succeed so the test can assert on attempt bounds.
    try:
        s.get("%s/data")
        event["exhausted"] = False
    except requests.exceptions.RetryError as exc:
        event["exhausted"] = True
        event["err"] = str(exc)[:200]
    return event
`, toContainerURL(always500.URL), toContainerURL(always500.URL))},
		}

		configBackend := newContractConfigBackend(t, entries)
		t.Cleanup(configBackend.Close)

		pyURL := startRudderPytransformer(
			t, pool, configBackend.URL,
			"SANDBOX_HTTP_TIMEOUT_S=5",
			"SANDBOX_POOL_MAX_SIZE=1",
		)

		events := []types.TransformerEvent{makeEvent("msg-retry-bounded-1", versionID)}
		start := time.Now()
		status, items := sendRawTransform(t, pyURL, events)
		elapsed := time.Since(start)
		require.Equal(t, http.StatusOK, status)
		require.Len(t, items, 1)
		require.Equal(t, http.StatusOK, items[0].StatusCode,
			"transformation caught the exhaustion, so status must be 200: error=%q", items[0].Error)
		require.Equal(t, true, items[0].Output["exhausted"],
			"retry budget must have been exhausted (always-500 upstream)")

		require.LessOrEqual(t, attempts.Load(), int64(2),
			"attempts must be bounded by MANAGED_MAX_RETRIES_CAP+1; got %d (user asked for total=10)",
			attempts.Load())
		require.Less(t, elapsed, 3*time.Second,
			"bounded retries must complete fast; elapsed=%s (a 10-retry loop would take much longer)",
			elapsed)
	})

	t.Run("SessionVerifyFalseIgnoredAtWire", func(t *testing.T) {
		// Scenario 4: setting `s.verify = False` as a *session* default must
		// be silently ignored; the real TLS handshake must still fail against
		// a self-signed certificate. If verification is bypassed, the server
		// would return 200 and the assertion below would fail.
		const versionID = "mgd-session-verify-false-v1"

		tlsSrv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"ok": true}`))
		}))
		t.Cleanup(tlsSrv.Close)

		entries := map[string]configBackendEntry{
			versionID: {code: fmt.Sprintf(`
import requests

def transformEvent(event, metadata):
    s = requests.Session()
    s.verify = False  # must be silently ignored
    try:
        s.get("%s/data")
        event["verified"] = False  # should never reach here
    except requests.exceptions.SSLError as e:
        event["verified"] = True
        event["err"] = str(e)[:120]
    return event
`, toContainerURL(tlsSrv.URL))},
		}

		configBackend := newContractConfigBackend(t, entries)
		t.Cleanup(configBackend.Close)

		pyURL := startRudderPytransformer(
			t, pool, configBackend.URL,
			"SANDBOX_HTTP_TIMEOUT_S=5",
			"SANDBOX_POOL_MAX_SIZE=1",
		)

		events := []types.TransformerEvent{makeEvent("msg-verify-false-1", versionID)}
		status, items := sendRawTransform(t, pyURL, events)
		require.Equal(t, http.StatusOK, status)
		require.Len(t, items, 1)
		require.Equal(t, http.StatusOK, items[0].StatusCode,
			"transformation itself must succeed: error=%q", items[0].Error)
		require.Equal(t, true, items[0].Output["verified"],
			"session-level verify=False must NOT disable TLS verification on the wire")
	})

	t.Run("PerRequestVerifyFalseHonoured", func(t *testing.T) {
		// Scenario 5: per-request `verify=False` kwarg must still disable
		// TLS verification for that single call — the documented escape
		// hatch.
		const versionID = "mgd-request-verify-false-v1"

		tlsSrv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"ok": true}`))
		}))
		t.Cleanup(tlsSrv.Close)

		entries := map[string]configBackendEntry{
			versionID: {code: fmt.Sprintf(`
import requests

def transformEvent(event, metadata):
    s = requests.Session()
    resp = s.get("%s/data", verify=False)
    event["status"] = resp.status_code
    return event
`, toContainerURL(tlsSrv.URL))},
		}

		configBackend := newContractConfigBackend(t, entries)
		t.Cleanup(configBackend.Close)

		pyURL := startRudderPytransformer(
			t, pool, configBackend.URL,
			"SANDBOX_HTTP_TIMEOUT_S=5",
			"SANDBOX_POOL_MAX_SIZE=1",
		)

		events := []types.TransformerEvent{makeEvent("msg-req-verify-false-1", versionID)}
		status, items := sendRawTransform(t, pyURL, events)
		require.Equal(t, http.StatusOK, status)
		require.Len(t, items, 1)
		require.Equal(t, http.StatusOK, items[0].StatusCode,
			"per-request verify=False must succeed: error=%q", items[0].Error)
		require.EqualValues(t, http.StatusOK, items[0].Output["status"],
			"server response status must surface unchanged")
	})

	t.Run("AdapterSubclassSendNotInvoked", func(t *testing.T) {
		// Scenario 7: a user-defined HTTPAdapter subclass that overrides
		// send() with custom behaviour must NOT be called. The real request
		// must go out to the real server; the subclass's send() is
		// discarded.
		const versionID = "mgd-adapter-subclass-v1"

		countingSrv, hits := newConnectionCountingServer(t)

		entries := map[string]configBackendEntry{
			versionID: {code: fmt.Sprintf(`
import requests
from requests.adapters import HTTPAdapter

class RogueAdapter(HTTPAdapter):
    def send(self, *args, **kwargs):
        # If our send is ever reached, it raises and the transformation
        # would surface a non-200 per-event result.
        raise RuntimeError("rogue-send-invoked")

def transformEvent(event, metadata):
    s = requests.Session()
    s.mount("%s", RogueAdapter(max_retries=0))
    resp = s.get("%s/data")
    event["status"] = resp.status_code
    return event
`, toContainerURL(countingSrv.URL), toContainerURL(countingSrv.URL))},
		}

		configBackend := newContractConfigBackend(t, entries)
		t.Cleanup(configBackend.Close)

		pyURL := startRudderPytransformer(
			t, pool, configBackend.URL,
			"SANDBOX_HTTP_TIMEOUT_S=5",
			"SANDBOX_POOL_MAX_SIZE=1",
		)

		hits.Store(0)
		events := []types.TransformerEvent{makeEvent("msg-subclass-1", versionID)}
		status, items := sendRawTransform(t, pyURL, events)
		require.Equal(t, http.StatusOK, status)
		require.Len(t, items, 1)
		require.Equal(t, http.StatusOK, items[0].StatusCode,
			"rogue subclass send() must NOT be invoked; error=%q", items[0].Error)
		require.EqualValues(t, 1, hits.Load(),
			"real server must have received the request (1 new TCP connection)")
	})

	t.Run("MountPrefixIsolation", func(t *testing.T) {
		// Scenario 9: a retry policy mounted for prefix A must not affect
		// prefix B. Server A returns 500 (retryable per policy); Server B
		// returns 500 with NO mounted retry policy. A should see 2 attempts
		// (initial + 1 retry, clamped by MANAGED_MAX_RETRIES_CAP), B should
		// see exactly 1 attempt.
		const versionID = "mgd-mount-prefix-iso-v1"

		always500A, hitsA := newAlways500Server(t)
		always500B, hitsB := newAlways500Server(t)

		entries := map[string]configBackendEntry{
			versionID: {code: fmt.Sprintf(`
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

URL_A = "%s"
URL_B = "%s"

def transformEvent(event, metadata):
    s = requests.Session()
    s.mount(URL_A, HTTPAdapter(max_retries=Retry(
        total=5, status_forcelist=[500], backoff_factor=0.01, allowed_methods=['GET'])))
    # A: retries bounded by managed cap, then raises — catch and move on.
    try:
        s.get(URL_A + "/a")
        event["a_exhausted"] = False
    except requests.exceptions.RetryError:
        event["a_exhausted"] = True
    # B: no mount for URL_B, so no retries — 500 surfaces as a response, no raise.
    rB = s.get(URL_B + "/b")
    event["b_status"] = rB.status_code
    return event
`, toContainerURL(always500A.URL), toContainerURL(always500B.URL))},
		}

		configBackend := newContractConfigBackend(t, entries)
		t.Cleanup(configBackend.Close)

		pyURL := startRudderPytransformer(
			t, pool, configBackend.URL,
			"SANDBOX_HTTP_TIMEOUT_S=5",
			"SANDBOX_POOL_MAX_SIZE=1",
		)

		hitsA.Store(0)
		hitsB.Store(0)
		events := []types.TransformerEvent{makeEvent("msg-mount-prefix-1", versionID)}
		status, items := sendRawTransform(t, pyURL, events)
		require.Equal(t, http.StatusOK, status)
		require.Len(t, items, 1)
		require.Equal(t, http.StatusOK, items[0].StatusCode,
			"transformation must succeed: error=%q", items[0].Error)
		require.Equal(t, true, items[0].Output["a_exhausted"],
			"prefix A must have exhausted its clamped retry budget")

		require.EqualValues(t, 2, hitsA.Load(),
			"prefix A: 1 initial + 1 retry (clamped by MANAGED_MAX_RETRIES_CAP); got %d", hitsA.Load())
		require.EqualValues(t, 1, hitsB.Load(),
			"prefix B: no mounted retry policy — exactly 1 attempt; got %d", hitsB.Load())
	})

	t.Run("HeadersAndAuthPreserved", func(t *testing.T) {
		// Scenario 10: per-session headers and auth must reach the server on
		// every call made through that Session.
		const versionID = "mgd-headers-auth-v1"

		headerSrv, observed := newHeaderCapturingServer(t)

		entries := map[string]configBackendEntry{
			versionID: {code: fmt.Sprintf(`
import requests

def transformEvent(event, metadata):
    s = requests.Session()
    s.headers["X-Custom"] = "sentinel-value"
    s.auth = ("alice", "secret")
    r1 = s.get("%s/1")
    r2 = s.get("%s/2")
    event["s1"] = r1.status_code
    event["s2"] = r2.status_code
    return event
`, toContainerURL(headerSrv.URL), toContainerURL(headerSrv.URL))},
		}

		configBackend := newContractConfigBackend(t, entries)
		t.Cleanup(configBackend.Close)

		pyURL := startRudderPytransformer(
			t, pool, configBackend.URL,
			"SANDBOX_HTTP_TIMEOUT_S=5",
			"SANDBOX_POOL_MAX_SIZE=1",
		)

		observed.reset()
		events := []types.TransformerEvent{makeEvent("msg-headers-auth-1", versionID)}
		status, items := sendRawTransform(t, pyURL, events)
		require.Equal(t, http.StatusOK, status)
		require.Len(t, items, 1)
		require.Equal(t, http.StatusOK, items[0].StatusCode,
			"transformation must succeed: error=%q", items[0].Error)

		captured := observed.snapshot()
		require.Len(t, captured, 2, "server must have observed both requests")
		for i, rec := range captured {
			require.Equal(t, "sentinel-value", rec.xCustom,
				"request #%d must carry X-Custom header", i)
			require.True(t, rec.hasBasicAuth,
				"request #%d must carry HTTP Basic auth derived from session auth", i)
		}
	})

	t.Run("SessionCloseIsNoOp", func(t *testing.T) {
		// Scenario 8: Session.close() must not tear down the platform
		// pooled session. Second Session() + second request must still
		// succeed, and the same worker must reuse its underlying pooled
		// TCP connection.
		const versionID = "mgd-close-noop-v1"

		trackSrv, newConns := newConnectionCountingServer(t)

		entries := map[string]configBackendEntry{
			versionID: {code: fmt.Sprintf(`
import requests

def transformEvent(event, metadata):
    s1 = requests.Session()
    r1 = s1.get("%s/one")
    s1.close()
    s2 = requests.Session()
    r2 = s2.get("%s/two")
    event["s1"] = r1.status_code
    event["s2"] = r2.status_code
    return event
`, toContainerURL(trackSrv.URL), toContainerURL(trackSrv.URL))},
		}

		configBackend := newContractConfigBackend(t, entries)
		t.Cleanup(configBackend.Close)

		pyURL := startRudderPytransformer(
			t, pool, configBackend.URL,
			"ENABLE_CONN_POOL=true",
			"USER_CONN_POOL_MAX_SIZE=1",
			"CONN_POOL_PER_TRANSFORMATION=true",
			"SANDBOX_HTTP_TIMEOUT_S=5",
			"SANDBOX_POOL_MAX_SIZE=1",
		)

		newConns.Store(0)
		events := []types.TransformerEvent{makeEvent("msg-close-noop-1", versionID)}
		status, items := sendRawTransform(t, pyURL, events)
		require.Equal(t, http.StatusOK, status)
		require.Len(t, items, 1)
		require.Equal(t, http.StatusOK, items[0].StatusCode,
			"both requests must succeed across the intervening close(): error=%q", items[0].Error)
		require.EqualValues(t, http.StatusOK, items[0].Output["s1"])
		require.EqualValues(t, http.StatusOK, items[0].Output["s2"])
		require.EqualValues(t, 1, newConns.Load(),
			"Session.close() must NOT tear down the pooled connection; want 1 StateNew, got %d",
			newConns.Load())
	})

	t.Run("ManagedSessionPartitionedPerTransformationVersion", func(t *testing.T) {
		// Scenario 12: with CONN_POOL_PER_TRANSFORMATION=true, two
		// transformation_version_ids handled by the same worker must use
		// distinct TCP connections even though both create a ManagedSession
		// and hit the same host. This is the managed-session equivalent of
		// TestConnectionPoolPerTransformationIsolation (which covers bare
		// requests.get).
		const (
			versionIDAlpha = "mgd-part-alpha"
			versionIDBeta  = "mgd-part-beta"
		)

		trackSrv, newConns := newConnectionCountingServer(t)

		code := fmt.Sprintf(`
import requests

def transformEvent(event, metadata):
    s = requests.Session()
    resp = s.get("%s/check")
    event["ok"] = resp.status_code == 200
    return event
`, toContainerURL(trackSrv.URL))

		entries := map[string]configBackendEntry{
			versionIDAlpha: {code: code},
			versionIDBeta:  {code: code},
		}
		configBackend := newContractConfigBackend(t, entries)
		t.Cleanup(configBackend.Close)

		pyURL := startRudderPytransformer(
			t, pool, configBackend.URL,
			"ENABLE_CONN_POOL=true",
			"CONN_POOL_PER_TRANSFORMATION=true",
			"USER_CONN_POOL_MAX_SIZE=1",
			"SANDBOX_POOL_MAX_SIZE=1",
			"SANDBOX_HTTP_TIMEOUT_S=5",
		)

		newConns.Store(0)

		evA := makeEvent("msg-mgd-alpha", versionIDAlpha)
		statusA, itemsA := sendRawTransform(t, pyURL, []types.TransformerEvent{evA})
		require.Equal(t, http.StatusOK, statusA)
		require.Len(t, itemsA, 1)
		require.Equal(t, http.StatusOK, itemsA[0].StatusCode)

		evB := makeEvent("msg-mgd-beta", versionIDBeta)
		statusB, itemsB := sendRawTransform(t, pyURL, []types.TransformerEvent{evB})
		require.Equal(t, http.StatusOK, statusB)
		require.Len(t, itemsB, 1)
		require.Equal(t, http.StatusOK, itemsB[0].StatusCode)

		require.EqualValues(t, 2, newConns.Load(),
			"two version_ids through the same worker must open two distinct TCP "+
				"connections under CONN_POOL_PER_TRANSFORMATION=true; want 2, got %d",
			newConns.Load())
	})
}

// newSequencedServer returns an HTTP server that replies with the provided
// status codes in order, one per request. Once the sequence is exhausted,
// subsequent requests receive the last status.
func newSequencedServer(t *testing.T, statuses []int) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	calls := &atomic.Int64{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		i := int(calls.Add(1)) - 1
		code := statuses[len(statuses)-1]
		if i < len(statuses) {
			code = statuses[i]
		}
		body := []byte(`{"ok": true}`)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.WriteHeader(code)
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)
	return srv, calls
}

// newAlways500Server returns an HTTP server that always responds with 500
// and a short JSON body, plus an atomic counter of request attempts.
func newAlways500Server(t *testing.T) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	calls := &atomic.Int64{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		body := []byte(`{"error": "server-error"}`)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)
	return srv, calls
}

// capturedHeaders records the headers this contract cares about — X-Custom
// (set via s.headers) and whether HTTP Basic auth was attached (via s.auth).
type capturedHeaders struct {
	xCustom      string
	hasBasicAuth bool
}

// headerObservations is a mutex-guarded list of per-request header captures.
type headerObservations struct {
	mu   sync.Mutex
	recs []capturedHeaders
}

func (h *headerObservations) reset() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.recs = nil
}

func (h *headerObservations) append(c capturedHeaders) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.recs = append(h.recs, c)
}

func (h *headerObservations) snapshot() []capturedHeaders {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]capturedHeaders, len(h.recs))
	copy(out, h.recs)
	return out
}

// newHeaderCapturingServer records X-Custom and Basic-auth presence on each
// request; returns the live observation store for per-test snapshots.
func newHeaderCapturingServer(t *testing.T) (*httptest.Server, *headerObservations) {
	t.Helper()
	obs := &headerObservations{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _, ok := r.BasicAuth()
		obs.append(capturedHeaders{
			xCustom:      r.Header.Get("X-Custom"),
			hasBasicAuth: ok,
		})
		body := []byte(`{"ok": true}`)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)
	return srv, obs
}
