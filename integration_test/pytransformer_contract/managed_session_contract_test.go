package pytransformer_contract

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/require"

	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/registry"

	"github.com/rudderlabs/rudder-server/processor/types"
)

// TestManagedSessionContract is the end-to-end black-box contract for the
// managed user-HTTP layer in rudder-pytransformer.
//
// Every subtest stands on its own: it spins up its own pytransformer
// container (with the Prometheus endpoint published), serves a hand-written
// Python transformation through the mock config backend, drives traffic
// against a controlled local HTTP server, and asserts both wire-observable
// behaviour (status codes, TCP connection counts, headers on the wire, TLS
// enforcement) AND the Prometheus counter/gauge semantics that the
// operator-facing dashboards rely on. Each test is complete; none defers
// any assertion to coverage elsewhere.
func TestManagedSessionContract(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 2 * time.Minute

	t.Run("ModuleLevelSessionMountWithRetry", func(t *testing.T) {
		// A user transformation creates a Session at module scope,
		// mounts an adapter carrying a user-supplied retry policy, and
		// uses it inside transformEvent. The upstream returns 500 on
		// the first attempt and 200 on the second, so a correctly
		// honoured retry budget produces exactly 2 attempts.
		//
		// The platform clamps Retry.total down to its own cap, so even
		// if the user asks for 5 retries we must see no more than
		// (cap + 1) attempts on the wire — and the clamp must show up
		// on the managed_session_mount_ignored_total counter under
		// dropped_attr="max_retries_beyond_budget" so operators can
		// surface customers whose retry policy got tightened.
		//
		// Also asserts the label-only propagation of the build-time
		// transformation_id: module-level Session() and mount() fire
		// their counters from outside a full transformation context,
		// yet the samples must carry the real tid (never "unknown"),
		// otherwise per-customer attribution on the Managed Sessions
		// dashboard is lost.
		const versionID = "mgd-module-session-retry-v1"

		statusSrv, attempts := newSequencedServer(t, []int{
			http.StatusInternalServerError,
			http.StatusOK,
		})
		serverURL := toContainerURL(statusSrv.URL)

		entries := map[string]configBackendEntry{
			versionID: {code: fmt.Sprintf(`
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

retry_strategy = Retry(total=5, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=['GET'])
adapter = HTTPAdapter(max_retries=retry_strategy)
http = requests.Session()
http.mount("%[1]s", adapter)

def transformEvent(event, metadata):
    resp = http.get("%[1]s/data", params={'k': 'v'})
    event["status"] = resp.status_code
    event["body"] = resp.text
    return event
`, serverURL)},
		}

		configBackend := newContractConfigBackend(t, entries)
		t.Cleanup(configBackend.Close)

		pyURL, metricsURL := startRudderPytransformerWithMetrics(
			t, pool, configBackend.URL,
			"SANDBOX_HTTP_TIMEOUT_S=5",
			"SANDBOX_POOL_MAX_SIZE=1",
		)

		events := []types.TransformerEvent{makeEvent("msg-module-retry-1", versionID)}
		status, items := sendRawTransform(t, pyURL, events)
		require.Equal(t, http.StatusOK, status)
		require.Len(t, items, 1)
		require.Equal(t, http.StatusOK, items[0].StatusCode,
			"transform must succeed after one retry: error=%q", items[0].Error)

		require.EqualValues(t, 2, attempts.Load(),
			"expected exactly 2 attempts (initial 500 + 1 retry, clamped by the platform cap); got %d",
			attempts.Load())

		requireMetricAtLeast(t, metricsURL,
			"managed_session_created_total",
			map[string]string{"transformation_id": versionID},
			1,
			"module-level requests.Session() must increment managed_session_created_total "+
				"under the real transformation_id, not 'unknown'")
		requireMetricEquals(t, metricsURL,
			"managed_session_created_total",
			map[string]string{"transformation_id": "unknown"},
			0,
			"no module-level Session sample should leak to the anonymous 'unknown' bucket")
		requireMetricAtLeast(t, metricsURL,
			"managed_session_mount_total",
			map[string]string{"transformation_id": versionID, "has_retries": "true"},
			1,
			"http.mount(HTTPAdapter(max_retries=Retry(...))) must increment "+
				"managed_session_mount_total with has_retries=true")
		requireMetricAtLeast(t, metricsURL,
			"managed_session_mount_ignored_total",
			map[string]string{"transformation_id": versionID, "dropped_attr": "max_retries_beyond_budget"},
			1,
			"user asked for Retry(total=5), platform clamps to its own cap; the clamp "+
				"must show up as managed_session_mount_ignored_total{dropped_attr=max_retries_beyond_budget}")
	})

	t.Run("RetryBudgetBoundedByManagedCap", func(t *testing.T) {
		// A user transformation asks for up to 10 retries against an
		// always-500 upstream. The platform clamps Retry.total to its
		// own wall-clock-safe cap, so the server must see at most
		// cap+1 attempts — never the full 11 the user asked for.
		//
		// The clamp must also be observable on the mount_ignored
		// counter so operators can reach out to customers whose retry
		// policy was effectively overridden.
		const versionID = "mgd-retry-bounded-v1"

		always500, attempts := newAlways500Server(t)
		serverURL := toContainerURL(always500.URL)

		entries := map[string]configBackendEntry{
			versionID: {code: fmt.Sprintf(`
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

def transformEvent(event, metadata):
    s = requests.Session()
    s.mount("%[1]s", HTTPAdapter(max_retries=Retry(
        total=10, status_forcelist=[500], backoff_factor=0.01, allowed_methods=['GET'])))
    # urllib3 raises on retry-budget exhaustion against status_forcelist; catching
    # lets the transformation succeed so the test can assert on attempt bounds.
    try:
        s.get("%[1]s/data")
        event["exhausted"] = False
    except requests.exceptions.RetryError as exc:
        event["exhausted"] = True
        event["err"] = str(exc)[:200]
    return event
`, serverURL)},
		}

		configBackend := newContractConfigBackend(t, entries)
		t.Cleanup(configBackend.Close)

		pyURL, metricsURL := startRudderPytransformerWithMetrics(
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
			"transformation catches the exhaustion, so status must be 200: error=%q", items[0].Error)
		require.Equal(t, true, items[0].Output["exhausted"],
			"retry budget must have been exhausted (always-500 upstream)")

		require.LessOrEqual(t, attempts.Load(), int64(2),
			"attempts must be bounded by the platform cap (<= 2); got %d "+
				"(user asked for total=10)", attempts.Load())
		require.Less(t, elapsed, 3*time.Second,
			"bounded retries must complete fast; elapsed=%s (a 10-retry loop would take longer)",
			elapsed)

		requireMetricAtLeast(t, metricsURL,
			"managed_session_created_total",
			map[string]string{"transformation_id": versionID},
			1,
			"in-function requests.Session() must increment managed_session_created_total")
		requireMetricAtLeast(t, metricsURL,
			"managed_session_mount_total",
			map[string]string{"transformation_id": versionID, "has_retries": "true"},
			1,
			"mounting an adapter with max_retries=Retry(...) must fire managed_session_mount_total "+
				"with has_retries=true")
		requireMetricAtLeast(t, metricsURL,
			"managed_session_mount_ignored_total",
			map[string]string{"transformation_id": versionID, "dropped_attr": "max_retries_beyond_budget"},
			1,
			"Retry(total=10) clamped to the platform cap must fire "+
				"managed_session_mount_ignored_total{dropped_attr=max_retries_beyond_budget}")
	})

	t.Run("SessionVerifyFalseIgnoredAtWire", func(t *testing.T) {
		// User assigns s.verify = False as a session-level default.
		// The platform silently ignores it, so the real TLS handshake
		// against a self-signed server must still fail — proving
		// verification remains enforced regardless of the user's
		// session-level assignment.
		//
		// The ignore must also show up on the mount_ignored counter
		// under dropped_attr="session_verify" so operators can track
		// customers who assumed their opt-out took effect.
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
    s.verify = False  # must be silently ignored at the wire
    try:
        s.get("%s/data")
        event["verified"] = False  # should never reach here if TLS is enforced
    except requests.exceptions.SSLError as e:
        event["verified"] = True
        event["err"] = str(e)[:120]
    return event
`, toContainerURL(tlsSrv.URL))},
		}

		configBackend := newContractConfigBackend(t, entries)
		t.Cleanup(configBackend.Close)

		pyURL, metricsURL := startRudderPytransformerWithMetrics(
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

		requireMetricAtLeast(t, metricsURL,
			"managed_session_mount_ignored_total",
			map[string]string{"transformation_id": versionID, "dropped_attr": "session_verify"},
			1,
			"assigning s.verify=False must fire managed_session_mount_ignored_total"+
				"{dropped_attr=session_verify}")
	})

	t.Run("PerRequestVerifyFalseHonoured", func(t *testing.T) {
		// The per-request verify=False kwarg is the documented escape
		// hatch: it must still disable TLS verification for that one
		// call, without tripping the session_verify ignore path.
		// Assertion includes a negative: managed_session_mount_ignored
		// with dropped_attr=session_verify must stay at 0 so we don't
		// accidentally alarm operators on an intended usage.
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

		pyURL, metricsURL := startRudderPytransformerWithMetrics(
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

		requireMetricEquals(t, metricsURL,
			"managed_session_mount_ignored_total",
			map[string]string{"transformation_id": versionID, "dropped_attr": "session_verify"},
			0,
			"per-request verify kwarg must NOT fire the session_verify ignore path "+
				"(that would alarm operators on intended usage)")
	})

	t.Run("AdapterSubclassSendNotInvoked", func(t *testing.T) {
		// A user subclasses HTTPAdapter and overrides send() with a
		// raise. If the override were invoked, the transformation
		// would surface a non-200 per-event result and the server
		// would never be hit. The platform must route through its
		// own adapter, so the real request goes out and the subclass
		// is ignored.
		//
		// The ignore must also fire managed_adapter_subclass_ignored_total
		// so operators can page-alert on this (rare) case — any
		// non-zero rate is usually a customer-correctness incident.
		const versionID = "mgd-adapter-subclass-v1"

		countingSrv, hits := newConnectionCountingServer(t)

		entries := map[string]configBackendEntry{
			versionID: {code: fmt.Sprintf(`
import requests
from requests.adapters import HTTPAdapter

class RogueAdapter(HTTPAdapter):
    def send(self, *args, **kwargs):
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

		pyURL, metricsURL := startRudderPytransformerWithMetrics(
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

		requireMetricAtLeast(t, metricsURL,
			"managed_adapter_subclass_ignored_total",
			map[string]string{"transformation_id": versionID},
			1,
			"mounting an HTTPAdapter subclass that overrides send() must fire "+
				"managed_adapter_subclass_ignored_total (page-worthy signal)")
	})

	t.Run("MountPrefixIsolation", func(t *testing.T) {
		// A retry policy mounted for prefix A must not apply to
		// prefix B. Against two always-500 servers, prefix A sees
		// cap+1 attempts (its retry policy is honoured up to the
		// clamp), prefix B sees exactly 1 attempt (no mount, no
		// retries, the 500 surfaces as a plain response).
		//
		// The mount counter must fire once with has_retries=true
		// (only one prefix got a Retry-carrying adapter), confirming
		// per-prefix isolation on the observability side.
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
    try:
        s.get(URL_A + "/a")
        event["a_exhausted"] = False
    except requests.exceptions.RetryError:
        event["a_exhausted"] = True
    rB = s.get(URL_B + "/b")
    event["b_status"] = rB.status_code
    return event
`, toContainerURL(always500A.URL), toContainerURL(always500B.URL))},
		}

		configBackend := newContractConfigBackend(t, entries)
		t.Cleanup(configBackend.Close)

		pyURL, metricsURL := startRudderPytransformerWithMetrics(
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
			"prefix A: 1 initial + 1 retry (clamped by the platform cap); got %d", hitsA.Load())
		require.EqualValues(t, 1, hitsB.Load(),
			"prefix B: no mounted retry policy — exactly 1 attempt; got %d", hitsB.Load())

		requireMetricEquals(t, metricsURL,
			"managed_session_mount_total",
			map[string]string{"transformation_id": versionID, "has_retries": "true"},
			1,
			"exactly one mount carried retries (for prefix A); the mount counter must reflect that")
	})

	t.Run("HeadersAndAuthPreserved", func(t *testing.T) {
		// Per-session headers and HTTP Basic auth (via s.auth) must
		// reach the server on every call made through that Session.
		// Confirms the legitimate Session API surface keeps working
		// end-to-end alongside the managed-session rerouting.
		//
		// Also asserts managed_session_created_total fires — any user
		// Session construction must be attributable to this tid.
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

		pyURL, metricsURL := startRudderPytransformerWithMetrics(
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

		requireMetricAtLeast(t, metricsURL,
			"managed_session_created_total",
			map[string]string{"transformation_id": versionID},
			1,
			"the Session construction must increment managed_session_created_total "+
				"under the real transformation_id")
	})

	t.Run("SessionCloseIsNoOp", func(t *testing.T) {
		// Session.close() must not tear down the platform's pooled
		// session: a second Session() created after the close must
		// still issue requests successfully and must reuse the same
		// underlying pooled TCP connection (proving the pool stayed
		// alive across the user's close).
		//
		// Two Session constructions happen in the transformation, so
		// managed_session_created_total must increment by at least 2.
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

		pyURL, metricsURL := startRudderPytransformerWithMetrics(
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

		requireMetricAtLeast(t, metricsURL,
			"managed_session_created_total",
			map[string]string{"transformation_id": versionID},
			2,
			"two Session() constructions in the transformation must be reflected "+
				"in managed_session_created_total >= 2")
	})

	t.Run("ManagedSessionPartitionedPerTransformationVersion", func(t *testing.T) {
		// With per-transformation connection pooling enabled, two
		// transformation_version_ids served by the same worker must
		// use distinct TCP connections even though both open a
		// Session against the same host. Confirms the partitioning
		// guarantee extends to user-created Sessions (not just the
		// bare requests.* path exercised elsewhere).
		//
		// Also asserts managed_session_created_total and
		// user_session_count are both attributable per tid — the
		// partition must show up in observability, not just on the
		// wire.
		const (
			versionIDAlpha = "mgd-part-alpha"
			versionIDBeta  = "mgd-part-beta"
		)

		trackSrv, newConns := newConnectionCountingServer(t)
		serverURL := toContainerURL(trackSrv.URL)

		code := fmt.Sprintf(`
import requests

def transformEvent(event, metadata):
    s = requests.Session()
    resp = s.get("%s/check")
    event["ok"] = resp.status_code == 200
    return event
`, serverURL)

		entries := map[string]configBackendEntry{
			versionIDAlpha: {code: code},
			versionIDBeta:  {code: code},
		}
		configBackend := newContractConfigBackend(t, entries)
		t.Cleanup(configBackend.Close)

		pyURL, metricsURL := startRudderPytransformerWithMetrics(
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

		requireMetricAtLeast(t, metricsURL,
			"managed_session_created_total",
			map[string]string{"transformation_id": versionIDAlpha},
			1,
			"alpha's Session must be attributable under its own transformation_id label")
		requireMetricAtLeast(t, metricsURL,
			"managed_session_created_total",
			map[string]string{"transformation_id": versionIDBeta},
			1,
			"beta's Session must be attributable under its own transformation_id label")
		requireMetricGreater(t, metricsURL,
			"user_session_count",
			map[string]string{"transformation_id": versionIDAlpha},
			0,
			"user_session_count must be > 0 for alpha (pooled session in use under its tid)")
		requireMetricGreater(t, metricsURL,
			"user_session_count",
			map[string]string{"transformation_id": versionIDBeta},
			0,
			"user_session_count must be > 0 for beta (pooled session in use under its tid)")
	})

	// userHTTPEntryPoints feeds the UserHttpFlowsThroughManagedPool
	// table-driven test. The two variants share harness, env and
	// assertions; only the Python transformation code differs so we
	// exercise both entry points (bare requests.get and a module-level
	// Session+mount) through the same contract.
	userHTTPEntryPoints := []struct {
		name    string
		codeFmt string // %[1]s is substituted with the container-reachable server URL.
	}{
		{
			name: "BareRequestsGet",
			codeFmt: `
import requests

def transformEvent(event, metadata):
    r = requests.get("%[1]s/data")
    event["status"] = r.status_code
    return event
`,
		},
		{
			name: "ModuleLevelSessionWithMount",
			codeFmt: `
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

retry_strategy = Retry(total=3, status_forcelist=[500], allowed_methods=['GET'])
adapter = HTTPAdapter(max_retries=retry_strategy)
http = requests.Session()
http.mount("%[1]s", adapter)

def transformEvent(event, metadata):
    r = http.get("%[1]s/data")
    event["status"] = r.status_code
    return event
`,
		},
	}

	t.Run("UserHttpFlowsThroughManagedPool", func(t *testing.T) {
		// Every user HTTP entry point — bare requests.get and a
		// user-created Session — must converge on the platform's
		// pooled session. The primary signal is the user_session_count
		// gauge: it must be > 0 for the transformation_id after any
		// user HTTP call reaches transformEvent.
		//
		// The Session variant (module-level pattern common in
		// customer code) additionally locks in build-time metric
		// labeling: module-level requests.Session() must increment
		// managed_session_created_total under the real tid, never
		// under "unknown". Per-customer attribution on the Managed
		// Sessions dashboard depends on that.
		//
		// The bare variant must NOT increment managed_session_created
		// because no user Session is constructed — only the internal
		// pooled session path is exercised. That negative assertion
		// keeps the counter semantically clean ("user code created
		// a Session"), not accidentally tripped by every bare
		// requests.get.
		for _, variant := range userHTTPEntryPoints {
			t.Run(variant.name, func(t *testing.T) {
				versionID := "mgd-flows-" + variant.name
				const eventsPerRun = 3

				countSrv, newConns := newConnectionCountingServer(t)
				serverURL := toContainerURL(countSrv.URL)

				entries := map[string]configBackendEntry{
					versionID: {code: fmt.Sprintf(variant.codeFmt, serverURL)},
				}
				configBackend := newContractConfigBackend(t, entries)
				t.Cleanup(configBackend.Close)

				pyURL, metricsURL := startRudderPytransformerWithMetrics(
					t, pool, configBackend.URL,
					"ENABLE_CONN_POOL=true",
					"USER_CONN_POOL_MAX_SIZE=1",
					"CONN_POOL_PER_TRANSFORMATION=true",
					"SANDBOX_POOL_MAX_SIZE=1",
					"SANDBOX_HTTP_TIMEOUT_S=5",
				)

				newConns.Store(0)
				evs := make([]types.TransformerEvent, eventsPerRun)
				for i := range evs {
					evs[i] = makeEvent(fmt.Sprintf("msg-%s-%d", variant.name, i), versionID)
				}
				status, items := sendRawTransform(t, pyURL, evs)
				require.Equal(t, http.StatusOK, status)
				require.Len(t, items, eventsPerRun)
				for i, it := range items {
					require.Equal(t, http.StatusOK, it.StatusCode,
						"event %d must succeed: error=%q", i, it.Error)
				}

				// Primary observability assertion: every user HTTP
				// entry point must cause user_session_count to reflect
				// a pooled user session for this transformation.
				requireMetricGreater(t, metricsURL,
					"user_session_count",
					map[string]string{"transformation_id": versionID},
					0,
					"user_session_count must be > 0 after user HTTP runs; "+
						"a 0 here means the entry point bypassed the pool")

				// Wire-observable cross-check: with USER_CONN_POOL_MAX_SIZE=1,
				// sequential calls must reuse a single pooled TCP connection.
				require.EqualValues(t, 1, newConns.Load(),
					"expected a single pooled TCP connection across %d events; "+
						"server saw %d StateNew transitions (pool bypass would produce more)",
					eventsPerRun, newConns.Load())

				switch variant.name {
				case "BareRequestsGet":
					// Bare requests.get does not construct a user
					// Session, so managed_session_created_total
					// must not fire. Keeps the counter semantically
					// clean.
					requireMetricEquals(t, metricsURL,
						"managed_session_created_total",
						map[string]string{"transformation_id": versionID},
						0,
						"bare requests.get() must NOT increment managed_session_created_total "+
							"(no user Session is constructed)")
				case "ModuleLevelSessionWithMount":
					// Module-level Session() must be attributable.
					requireMetricAtLeast(t, metricsURL,
						"managed_session_created_total",
						map[string]string{"transformation_id": versionID},
						1,
						"module-level Session() must increment managed_session_created_total "+
							"under the real transformation_id")
					requireMetricEquals(t, metricsURL,
						"managed_session_created_total",
						map[string]string{"transformation_id": "unknown"},
						0,
						"no module-level Session sample should leak to the 'unknown' bucket "+
							"(that would hide per-customer attribution)")
					// mount() also runs at module load; it must
					// land under the real tid.
					requireMetricAtLeast(t, metricsURL,
						"managed_session_mount_total",
						map[string]string{"transformation_id": versionID, "has_retries": "true"},
						1,
						"module-level http.mount(HTTPAdapter(max_retries=Retry(...))) must "+
							"increment managed_session_mount_total with has_retries=true")
				}
			})
		}
	})

	t.Run("AdapterPoolMaxsizeIgnored", func(t *testing.T) {
		// HTTPAdapter(pool_maxsize=N, pool_connections=M) kwargs that
		// govern connection-pool sizing must be silently ignored —
		// pool sizing is governed by the platform's
		// USER_CONN_POOL_MAX_SIZE, not by user-supplied adapter
		// kwargs. Without the ignore, a user could mount
		// HTTPAdapter(pool_maxsize=100) and override the platform's
		// single-connection bound.
		//
		// Wire signal: sequential calls must fit in a single pooled
		// TCP connection (USER_CONN_POOL_MAX_SIZE=1).
		//
		// Metric signal: each dropped kwarg must fire
		// managed_session_mount_ignored_total with the matching
		// dropped_attr label, so operators can surface customers
		// whose configuration got ignored.
		//
		// The mount is at module scope (common customer pattern);
		// the real transformation_id must flow through the
		// build-time path into the counter labels.
		const versionID = "mgd-pool-maxsize-ignored-v1"
		const eventsPerRun = 3

		countSrv, newConns := newConnectionCountingServer(t)
		serverURL := toContainerURL(countSrv.URL)

		code := fmt.Sprintf(`
import requests
from requests.adapters import HTTPAdapter

adapter = HTTPAdapter(pool_maxsize=100, pool_connections=50, max_retries=0)
http = requests.Session()
http.mount("%[1]s", adapter)

def transformEvent(event, metadata):
    r = http.get("%[1]s/data")
    event["status"] = r.status_code
    return event
`, serverURL)

		entries := map[string]configBackendEntry{
			versionID: {code: code},
		}
		configBackend := newContractConfigBackend(t, entries)
		t.Cleanup(configBackend.Close)

		pyURL, metricsURL := startRudderPytransformerWithMetrics(
			t, pool, configBackend.URL,
			"ENABLE_CONN_POOL=true",
			"USER_CONN_POOL_MAX_SIZE=1",
			"CONN_POOL_PER_TRANSFORMATION=true",
			"SANDBOX_POOL_MAX_SIZE=1",
			"SANDBOX_HTTP_TIMEOUT_S=5",
		)

		newConns.Store(0)
		evs := make([]types.TransformerEvent, eventsPerRun)
		for i := range evs {
			evs[i] = makeEvent(fmt.Sprintf("msg-pool-maxsize-%d", i), versionID)
		}
		status, items := sendRawTransform(t, pyURL, evs)
		require.Equal(t, http.StatusOK, status)
		require.Len(t, items, eventsPerRun)
		for i, it := range items {
			require.Equal(t, http.StatusOK, it.StatusCode,
				"event %d must succeed: error=%q", i, it.Error)
		}

		require.EqualValues(t, 1, newConns.Load(),
			"USER_CONN_POOL_MAX_SIZE=1 must cap connections regardless of "+
				"user-supplied pool_maxsize=100; server saw %d StateNew transitions",
			newConns.Load())

		requireMetricAtLeast(t, metricsURL,
			"managed_session_mount_ignored_total",
			map[string]string{"transformation_id": versionID, "dropped_attr": "pool_maxsize"},
			1,
			"user HTTPAdapter(pool_maxsize=100) must fire managed_session_mount_ignored_total"+
				"{dropped_attr=pool_maxsize}")
		requireMetricAtLeast(t, metricsURL,
			"managed_session_mount_ignored_total",
			map[string]string{"transformation_id": versionID, "dropped_attr": "pool_connections"},
			1,
			"user HTTPAdapter(pool_connections=50) must fire managed_session_mount_ignored_total"+
				"{dropped_attr=pool_connections} — confirms each dropped kwarg is emitted "+
				"as its own label value, not collapsed")
	})
}

// startRudderPytransformerWithMetrics is startRudderPytransformer plus the
// Prometheus metrics port wired through to the host so contract tests can
// scrape counters and gauges.
func startRudderPytransformerWithMetrics(
	t *testing.T, pool *dockertest.Pool,
	configBackendURL string,
	extraEnv ...string,
) (pyURL, metricsURL string) {
	t.Helper()
	const (
		apiContainerPort     = "8080"
		metricsContainerPort = "9091"
	)

	cfg := newContainerConfig(t, apiContainerPort)

	// On macOS (bridge networking) the metrics port needs an explicit
	// binding — it defaults to 9091 inside the container and is not
	// reachable otherwise. On Linux (host networking) we allocate a free
	// port on the host and pass it via METRICS_PORT so the container
	// doesn't clash with any sibling pytransformer containers sharing
	// the namespace.
	env := []string{
		"CONFIG_BACKEND_URL=" + toContainerURL(configBackendURL),
		"UVICORN_PORT=" + cfg.portStr(apiContainerPort),
	}

	var linuxMetricsHostPort string
	if runtime.GOOS == "darwin" {
		cfg.PortBindings[docker.Port(metricsContainerPort+"/tcp")] = []docker.PortBinding{
			{HostIP: "127.0.0.1", HostPort: "0"},
		}
		env = append(env, "METRICS_PORT="+metricsContainerPort)
	} else {
		freePort, err := kithelper.GetFreePort()
		require.NoError(t, err)
		linuxMetricsHostPort = strconv.Itoa(freePort)
		env = append(env, "METRICS_PORT="+linuxMetricsHostPort)
	}

	for _, e := range extraEnv {
		env = append(env, toContainerURL(e))
	}

	container, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "422074288268.dkr.ecr.us-east-1.amazonaws.com/rudderstack/rudder-pytransformer",
		Tag:          "main",
		Auth:         registry.AuthConfiguration(),
		Env:          env,
		ExtraHosts:   cfg.ExtraHosts,
		PortBindings: cfg.PortBindings,
	}, cfg.hostConfigFn)
	require.NoError(t, err, "failed to start rudder-pytransformer container")

	t.Cleanup(func() {
		if err := pool.Purge(container); err != nil {
			t.Logf("Failed to purge pytransformer container: %v", err)
		}
	})

	pyURL = cfg.url(container, apiContainerPort)
	waitForHealthy(t, pool, pyURL, "rudder-pytransformer", container)

	if runtime.GOOS == "darwin" {
		metricsURL = fmt.Sprintf("http://%s:%s",
			container.GetBoundIP(metricsContainerPort+"/tcp"),
			container.GetPort(metricsContainerPort+"/tcp"),
		)
	} else {
		metricsURL = "http://localhost:" + linuxMetricsHostPort
	}

	return pyURL, metricsURL
}

// scrapePytransformerMetric fetches /metrics from the pytransformer
// Prometheus endpoint and returns the value of the sample matching the
// given metric name and label set. Missing metric or missing label
// combination returns 0; an HTTP failure fails the test.
//
// The parser is deliberately line-oriented and label-order agnostic —
// Prometheus text format guarantees one sample per line but not a stable
// label ordering, so we normalise both sides into a map before comparing.
func scrapePytransformerMetric(
	t *testing.T, metricsURL, name string, wantLabels map[string]string,
) float64 {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, metricsURL+"/metrics", nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err, "scrape %s failed", metricsURL)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode, "metrics endpoint returned non-200")

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	// Pattern: NAME{k="v",k2="v2"} VALUE  (whitespace-separated, optional
	// trailing comment which we ignore). Restricting the leading name
	// match to ``^<name>[{ ]`` avoids accidental matches on metrics that
	// share a prefix (e.g. managed_session_created vs
	// managed_session_created_total).
	pattern := regexp.MustCompile(
		`^` + regexp.QuoteMeta(name) + `(?:\{([^}]*)\})?\s+([0-9eE.+\-]+)`,
	)
	labelPattern := regexp.MustCompile(`([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\]|\\.)*)"`)

	for _, line := range splitLines(string(body)) {
		if line == "" || line[0] == '#' {
			continue
		}
		m := pattern.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		labels := map[string]string{}
		for _, lm := range labelPattern.FindAllStringSubmatch(m[1], -1) {
			labels[lm[1]] = lm[2]
		}
		matched := true
		for k, v := range wantLabels {
			if labels[k] != v {
				matched = false
				break
			}
		}
		if !matched {
			continue
		}
		val, err := strconv.ParseFloat(m[2], 64)
		require.NoError(t, err, "failed to parse metric value %q", m[2])
		return val
	}
	return 0
}

// requireMetricAtLeast scrapes /metrics and fails if the labeled sample
// value is below the minimum, with a descriptive message that names the
// metric and label set so the failure explains itself without a stack
// trace read.
func requireMetricAtLeast(
	t *testing.T,
	metricsURL, name string,
	labels map[string]string,
	minValue float64,
	why string,
) {
	t.Helper()
	got := scrapePytransformerMetric(t, metricsURL, name, labels)
	require.GreaterOrEqual(t, got, minValue,
		"%s{%s} must be >= %v; got %v — %s",
		name, formatLabelsForMessage(labels), minValue, got, why)
}

// requireMetricEquals scrapes /metrics and fails if the labeled sample
// value differs from want. Used for negative assertions ("this path
// must not fire") as well as exact-count assertions.
func requireMetricEquals(
	t *testing.T,
	metricsURL, name string,
	labels map[string]string,
	want float64,
	why string,
) {
	t.Helper()
	got := scrapePytransformerMetric(t, metricsURL, name, labels)
	require.InDelta(t, want, got, 1e-9,
		"%s{%s} must equal %v; got %v — %s",
		name, formatLabelsForMessage(labels), want, got, why)
}

// requireMetricGreater scrapes /metrics and fails if the labeled sample
// value is not strictly greater than threshold. Used for "must be non-
// zero" gauges like user_session_count where the exact value is an
// implementation detail but the 0/non-0 distinction is load-bearing.
func requireMetricGreater(
	t *testing.T,
	metricsURL, name string,
	labels map[string]string,
	threshold float64,
	why string,
) {
	t.Helper()
	got := scrapePytransformerMetric(t, metricsURL, name, labels)
	require.Greater(t, got, threshold,
		"%s{%s} must be > %v; got %v — %s",
		name, formatLabelsForMessage(labels), threshold, got, why)
}

// formatLabelsForMessage renders a label map as "k=v,k2=v2" for
// inclusion in assertion failure messages. Order is unspecified; the
// message is human-targeted, not machine-parsed.
func formatLabelsForMessage(labels map[string]string) string {
	var parts []string
	for k, v := range labels {
		parts = append(parts, fmt.Sprintf("%s=%q", k, v))
	}
	out := ""
	for i, p := range parts {
		if i > 0 {
			out += ","
		}
		out += p
	}
	return out
}

func splitLines(s string) []string {
	var out []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			out = append(out, s[start:i])
			start = i + 1
		}
	}
	if start < len(s) {
		out = append(out, s[start:])
	}
	return out
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
