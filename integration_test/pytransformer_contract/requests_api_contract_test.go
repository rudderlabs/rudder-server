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

	"github.com/rudderlabs/rudder-server/processor/types"
)

// TestRequestsModuleWrapperContract locks the contract that user code
// accessing HTTP helpers through alternative module paths produces
// identical results to the standard "import requests; requests.get(url)"
// path on both versions.
//
// pytransformer's "wrap_requests_methods" must rebind names on both
// "requests" and "requests.api", and must also wrap
// "requests.request" — the verb-parameterized entry point. Without
// this, any of the following user-code patterns bypass metrics and
// connection-pool wrappers:
//
//   - "from requests.api import get" — binds at import time
//   - "requests.api.get(url)" — attribute chain resolved at call time
//   - "requests.request("GET", url)" — verb-parameterized entry point
//
// All three must produce the same output as the baseline
// (vanilla "requests").
//
// Additional "ConnectionReuse" subtests prove that the calls actually
// flow through the pooling wrapper by sending two sequential requests
// and asserting that the server observed a single TCP handshake
// (connection reuse). Without the wrapper, vanilla
// "requests.api.get" / "requests.request" creates a throwaway
// "Session" per call and opens a fresh TCP connection each time — so
// "newConns == 1" can only be true if the call went through the
// persistent pooled session installed by the wrapper.
func TestRequestsModuleWrapperContract(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 2 * time.Minute

	const (
		parityVersionID       = "requests-wrapper-parity-v1"
		reuseApiVersionID     = "requests-wrapper-reuse-api-v1"
		reuseRequestVersionID = "requests-wrapper-reuse-request-v1"
	)

	echo := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("q")
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"echo": %q, "style": %q}`, q, r.URL.Query().Get("style"))
	}))
	t.Cleanup(echo.Close)

	// Connection-counting echo server for the ConnectionReuse subtests.
	// ConnState fires StateNew exactly once per TCP handshake, so
	// newConns == 1 after two HTTP calls proves the second call reused
	// the pooled connection instead of opening a fresh one.
	newConns := &atomic.Int64{}
	countingEcho := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := []byte(`{"echo": "reuse-check"}`)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	}))
	countingEcho.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			newConns.Add(1)
		}
	}
	countingEcho.Start()
	t.Cleanup(countingEcho.Close)

	// Dispatcher user code: the "style" field in the incoming event
	// selects between the three alternative import / call paths.
	parityCode := fmt.Sprintf(`
import requests
from requests.api import get as api_get

def transformEvent(event, metadata):
    style = event["style"]
    url = "%s/search"
    if style == "from_import":
        resp = api_get(url, params={"q": "hello", "style": style}, timeout=5)
    elif style == "dotted":
        resp = requests.api.get(url, params={"q": "hello", "style": style}, timeout=5)
    elif style == "request":
        resp = requests.request("GET", url, params={"q": "hello", "style": style}, timeout=5)
    else:
        raise ValueError("unknown style: " + repr(style))
    body = resp.json()
    event["echo"] = body["echo"]
    event["resp_style"] = body["style"]
    return event
`, toContainerURL(echo.URL))

	// Separate code for each connection-reuse subtest — each hits the
	// counting server through a single call path so we can assert that
	// the pooling wrapper is actually in the call path.
	reuseApiCode := fmt.Sprintf(`
from requests.api import get

def transformEvent(event, metadata):
    resp = get("%s/reuse-check", timeout=5)
    event["echo"] = resp.json()["echo"]
    return event
`, toContainerURL(countingEcho.URL))

	reuseRequestCode := fmt.Sprintf(`
import requests

def transformEvent(event, metadata):
    resp = requests.request("GET", "%s/reuse-check", timeout=5)
    event["echo"] = resp.json()["echo"]
    return event
`, toContainerURL(countingEcho.URL))

	configBackend := newContractConfigBackend(t, map[string]configBackendEntry{
		parityVersionID:       {code: parityCode},
		reuseApiVersionID:     {code: reuseApiCode},
		reuseRequestVersionID: {code: reuseRequestCode},
	})
	t.Cleanup(configBackend.Close)

	// Pin pool + subprocess count to 1 so the ConnectionReuse subtests
	// actually exercise the shared pooled session. Identical on both sides
	// so the comparison isolates the version difference.
	pytEnv := []string{
		"USER_CONN_POOL_MAX_SIZE=1",
		"SANDBOX_POOL_MAX_SIZE=1",
	}

	t.Log("Starting baseline rudder-pytransformer...")
	baselineURL := startBaselinePytransformer(t, pool, configBackend.URL, pytEnv...)

	t.Log("Starting candidate rudder-pytransformer...")
	candidateURL := startRudderPytransformer(t, pool, configBackend.URL, pytEnv...)

	styleCases := []struct {
		style string
	}{
		{style: "from_import"},
		{style: "dotted"},
		{style: "request"},
	}

	for _, sc := range styleCases {
		t.Run(sc.style, func(t *testing.T) {
			env := newBCTestEnv(t, baselineURL, candidateURL,
				withFailOnError(),
				withLimitedRetryableHTTPRetries(),
			)

			event := makeEvent("msg-"+sc.style, parityVersionID)
			event.Message["style"] = sc.style
			events := []types.TransformerEvent{event}

			t.Log("Sending request to baseline...")
			baselineResp := env.BaselineClient.Transform(context.Background(), events)
			t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))

			t.Log("Sending request to candidate...")
			candidateResp := env.CandidateClient.Transform(context.Background(), events)
			t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))

			require.Equal(t, 1, len(baselineResp.Events), "baseline: 1 success event expected")
			require.Equal(t, 0, len(baselineResp.FailedEvents), "baseline: no failed events expected")
			require.Equalf(t, 1, len(candidateResp.Events),
				"candidate (style=%s): 1 success event expected", sc.style)
			require.Equal(t, 0, len(candidateResp.FailedEvents), "candidate: no failed events expected")

			require.Equal(t, "hello", baselineResp.Events[0].Output["echo"],
				"baseline must echo q=hello")
			require.Equalf(t, "hello", candidateResp.Events[0].Output["echo"],
				"candidate (style=%s) must echo q=hello", sc.style)

			diff, equal := baselineResp.Equal(&candidateResp)
			require.Truef(t, equal,
				"style=%s: baseline and candidate must produce identical responses:\n%s",
				sc.style, diff)

			env.assertRetryCountsMatch(t)
		})
	}

	// Prove that the alternative call paths actually flow through the
	// connection-pool wrapper, not just that they return the correct
	// response. A wrapped call routes through the process-shared pooled
	// Session that keeps TCP connections alive across requests. An
	// unwrapped call (vanilla requests.api.get / requests.request)
	// creates a throwaway Session per invocation — every call opens a
	// fresh TCP connection. Asserting that two sequential calls produced
	// AT MOST one server-side TCP handshake (StateNew) proves the pooled
	// Session was used, which is only possible if the function was
	// rebound to the wrapper. (The count can legitimately be 0 if an
	// earlier subtest already warmed a connection to the shared
	// countingEcho host; both subtests run against the same
	// candidate container and thus the same subprocess-shared pool.)
	reuseCases := []struct {
		name      string
		versionID string
	}{
		{name: "from_requests.api_import_get", versionID: reuseApiVersionID},
		{name: "requests.request", versionID: reuseRequestVersionID},
	}
	for _, rc := range reuseCases {
		t.Run("ConnectionReuse/"+rc.name, func(t *testing.T) {
			newConns.Store(0)

			ev1 := makeEvent("msg-reuse-1", rc.versionID)
			status1, _, items1 := sendRawTransform(t, candidateURL, []types.TransformerEvent{ev1})
			require.Equal(t, http.StatusOK, status1)
			require.Len(t, items1, 1)
			require.Equal(t, http.StatusOK, items1[0].StatusCode, "first request must succeed")

			ev2 := makeEvent("msg-reuse-2", rc.versionID)
			status2, _, items2 := sendRawTransform(t, candidateURL, []types.TransformerEvent{ev2})
			require.Equal(t, http.StatusOK, status2)
			require.Len(t, items2, 1)
			require.Equal(t, http.StatusOK, items2[0].StatusCode, "second request must succeed")

			require.LessOrEqualf(t, newConns.Load(), int64(1),
				"two sequential calls via %s must reuse the pooled TCP "+
					"connection (server-side StateNew count: want <= 1, got %d). "+
					"A count of 2 means the calls bypassed the pooling wrapper "+
					"and each created a throwaway Session.",
				rc.name, newConns.Load())
		})
	}
}
