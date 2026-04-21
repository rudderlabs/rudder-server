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
// path on both architectures.
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
// All three must produce the same output as the old architecture
// (vanilla "requests") under every "ENABLE_CONN_POOL" setting.
//
// Under "ENABLE_CONN_POOL=true", additional "ConnectionReuse" subtests
// prove that the calls actually flow through the pooling wrapper by
// sending two sequential requests and asserting that the server
// observed a single TCP handshake (connection reuse). Without the
// wrapper, vanilla "requests.api.get" / "requests.request" creates a
// throwaway "Session" per call and opens a fresh TCP connection each
// time — so "newConns == 1" can only be true if the call went through
// the persistent pooled session installed by the wrapper.
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

	t.Log("Starting openfaas-flask-base (old arch backend)...")
	openFaasURL := startOpenFaasFlask(t, pool, parityVersionID, configBackend.URL)

	t.Log("Starting mock OpenFaaS gateway...")
	mockGateway, _ := newMockOpenFaaSGateway(t, func() string { return openFaasURL })
	t.Cleanup(mockGateway.Close)

	t.Log("Starting rudder-transformer (old arch frontend)...")
	transformerURL := startRudderTransformer(t, pool, configBackend.URL, mockGateway.URL)

	newArchCases := []struct {
		name            string
		enableConnPool  string
		extraPytransEnv []string
	}{
		{
			name:            "ConnPoolDisabled",
			enableConnPool:  "false",
			extraPytransEnv: nil,
		},
		{
			name:           "ConnPoolEnabled",
			enableConnPool: "true",
			extraPytransEnv: []string{
				"USER_CONN_POOL_MAX_SIZE=1",
				"SANDBOX_POOL_MAX_SIZE=1",
			},
		},
	}

	styleCases := []struct {
		style string
	}{
		{style: "from_import"},
		{style: "dotted"},
		{style: "request"},
	}

	for _, tc := range newArchCases {
		t.Run(tc.name, func(t *testing.T) {
			pyEnv := append([]string{"ENABLE_CONN_POOL=" + tc.enableConnPool}, tc.extraPytransEnv...)
			t.Logf("Starting rudder-pytransformer with %v...", pyEnv)
			pyTransformerURL := startRudderPytransformer(t, pool, configBackend.URL, pyEnv...)

			for _, sc := range styleCases {
				t.Run(sc.style, func(t *testing.T) {
					env := newBCTestEnv(t, transformerURL, pyTransformerURL,
						withFailOnError(),
						withLimitedRetryableHTTPRetries(),
					)

					event := makeEvent("msg-"+sc.style, parityVersionID)
					event.Message["style"] = sc.style
					events := []types.TransformerEvent{event}

					t.Log("Sending request to old architecture...")
					oldResp := env.OldClient.Transform(context.Background(), events)
					t.Logf("Old arch: Events=%d, FailedEvents=%d", len(oldResp.Events), len(oldResp.FailedEvents))

					t.Log("Sending request to new architecture...")
					newResp := env.NewClient.Transform(context.Background(), events)
					t.Logf("New arch: Events=%d, FailedEvents=%d", len(newResp.Events), len(newResp.FailedEvents))

					require.Equal(t, 1, len(oldResp.Events), "old arch: 1 success event expected")
					require.Equal(t, 0, len(oldResp.FailedEvents), "old arch: no failed events expected")
					require.Equalf(t, 1, len(newResp.Events),
						"new arch (ENABLE_CONN_POOL=%s, style=%s): 1 success event expected",
						tc.enableConnPool, sc.style)
					require.Equal(t, 0, len(newResp.FailedEvents), "new arch: no failed events expected")

					require.Equal(t, "hello", oldResp.Events[0].Output["echo"],
						"old arch must echo q=hello")
					require.Equalf(t, "hello", newResp.Events[0].Output["echo"],
						"new arch (ENABLE_CONN_POOL=%s, style=%s) must echo q=hello",
						tc.enableConnPool, sc.style)

					diff, equal := oldResp.Equal(&newResp)
					require.Truef(t, equal,
						"ENABLE_CONN_POOL=%s, style=%s: old and new architectures "+
							"must produce identical responses:\n%s",
						tc.enableConnPool, sc.style, diff)

					env.assertRetryCountsMatch(t)
				})
			}

			// Prove that the alternative call paths actually flow through
			// the connection-pool wrapper, not just that they return the
			// correct response. With ENABLE_CONN_POOL=true, a wrapped call
			// routes through a persistent pooled Session that keeps TCP
			// connections alive across requests. An unwrapped call (vanilla
			// requests.api.get / requests.request) creates a throwaway
			// Session per invocation — every call opens a fresh TCP
			// connection. Asserting that two sequential calls produced only
			// one server-side TCP handshake (StateNew) proves the pooled
			// Session was used, which is only possible if the function was
			// rebound to the wrapper.
			if tc.enableConnPool == "true" {
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
						status1, items1 := sendRawTransform(t, pyTransformerURL, []types.TransformerEvent{ev1})
						require.Equal(t, http.StatusOK, status1)
						require.Len(t, items1, 1)
						require.Equal(t, http.StatusOK, items1[0].StatusCode, "first request must succeed")

						ev2 := makeEvent("msg-reuse-2", rc.versionID)
						status2, items2 := sendRawTransform(t, pyTransformerURL, []types.TransformerEvent{ev2})
						require.Equal(t, http.StatusOK, status2)
						require.Len(t, items2, 1)
						require.Equal(t, http.StatusOK, items2[0].StatusCode, "second request must succeed")

						require.EqualValues(t, 1, newConns.Load(),
							"with ENABLE_CONN_POOL=true, two sequential calls "+
								"via %s must reuse the same TCP connection "+
								"(server-side StateNew count: want 1). A count "+
								"of 2 means the calls bypassed the pooling "+
								"wrapper and each created a throwaway Session.",
							rc.name)
					})
				}
			}
		})
	}
}
