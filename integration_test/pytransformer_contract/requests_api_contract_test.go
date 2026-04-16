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

// TestRequestsApiModuleContract locks the contract that user code importing
// HTTP helpers from “requests.api“ (the submodule where the functions are
// defined) produces identical results to the standard “import requests;
// requests.get(url)“ path on both architectures.
//
// “requests.__init__“ does “from .api import get, post, ...“ so the
// top-level names and the submodule names start as the same object.
// pytransformer's “wrap_requests_methods“ must rebind both namespaces;
// otherwise “from requests.api import get“ or “requests.api.get(url)“
// bypasses metrics and connection-pool wrappers.
//
// The test exercises two user-code import styles:
//   - “from requests.api import get“ — binds at import time
//   - “requests.api.get(url)“ — attribute chain resolved at call time
//
// Both must produce the same output as the old architecture (vanilla
// “requests“) under every “ENABLE_CONN_POOL“ setting.
//
// Under “ENABLE_CONN_POOL=true“, an additional “ConnectionReuse“ subtest
// proves that the calls actually flow through the pooling wrapper by sending
// two sequential requests and asserting that the server observed a single TCP
// handshake (connection reuse). Without the wrapper, vanilla
// “requests.api.get“ creates a throwaway “Session“ per call and opens a
// fresh TCP connection each time — so “newConns == 1“ can only be true if
// the call went through the persistent pooled session installed by the
// wrapper.
func TestRequestsApiModuleContract(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 2 * time.Minute

	const (
		parityVersionID = "requests-api-module-v1"
		reuseVersionID  = "requests-api-reuse-v1"
	)

	echo := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("q")
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"echo": %q, "style": %q}`, q, r.URL.Query().Get("style"))
	}))
	t.Cleanup(echo.Close)

	// Connection-counting echo server for the ConnectionReuse subtest.
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
	// selects between ``from requests.api import get`` and the dotted
	// ``requests.api.get`` attribute-chain path.
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
    else:
        raise ValueError("unknown style: " + repr(style))
    body = resp.json()
    event["echo"] = body["echo"]
    event["resp_style"] = body["style"]
    return event
`, toContainerURL(echo.URL))

	// Separate code for the connection-reuse subtest — hits the
	// counting server via ``from requests.api import get`` so we can
	// assert that the pooling wrapper is actually in the call path.
	reuseCode := fmt.Sprintf(`
from requests.api import get

def transformEvent(event, metadata):
    resp = get("%s/reuse-check", timeout=5)
    event["echo"] = resp.json()["echo"]
    return event
`, toContainerURL(countingEcho.URL))

	configBackend := newContractConfigBackend(t, map[string]configBackendEntry{
		parityVersionID: {code: parityCode},
		reuseVersionID:  {code: reuseCode},
	})
	t.Cleanup(configBackend.Close)

	t.Log("Starting openfaas-flask-base (old arch backend)...")
	openFaasURL := startOpenFaasFlask(t, pool, parityVersionID, configBackend.URL)

	t.Log("Starting mock OpenFaaS gateway...")
	mockGateway, _ := newMockOpenFaaSGateway(t, func() string { return openFaasURL })
	t.Cleanup(mockGateway.Close)

	t.Log("Starting rudder-transformer (old arch frontend)...")
	transformerContainer, transformerURL := startRudderTransformer(t, pool, configBackend.URL, mockGateway.URL)
	t.Cleanup(func() {
		if err := pool.Purge(transformerContainer); err != nil {
			t.Logf("Failed to purge rudder-transformer: %v", err)
		}
	})
	waitForHealthy(t, pool, transformerURL, "rudder-transformer")

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
							"must produce identical responses for requests.api import:\n%s",
						tc.enableConnPool, sc.style, diff)

					env.assertRetryCountsMatch(t)
				})
			}

			// Prove that ``from requests.api import get`` actually goes
			// through the connection-pool wrapper, not just that it
			// returns the correct response. With ENABLE_CONN_POOL=true,
			// a wrapped call routes through a persistent pooled Session
			// that keeps TCP connections alive across requests. An
			// unwrapped call (vanilla ``requests.api.get``) creates a
			// throwaway Session per invocation — every call opens a
			// fresh TCP connection. Asserting that two sequential calls
			// produced only one server-side TCP handshake (StateNew)
			// proves the pooled Session was used, which is only possible
			// if ``requests.api.get`` was rebound to the wrapper.
			if tc.enableConnPool == "true" {
				t.Run("ConnectionReuse", func(t *testing.T) {
					newConns.Store(0)

					ev1 := makeEvent("msg-reuse-1", reuseVersionID)
					status1, items1 := sendRawTransform(t, pyTransformerURL, []types.TransformerEvent{ev1})
					require.Equal(t, http.StatusOK, status1)
					require.Len(t, items1, 1)
					require.Equal(t, http.StatusOK, items1[0].StatusCode, "first request must succeed")

					ev2 := makeEvent("msg-reuse-2", reuseVersionID)
					status2, items2 := sendRawTransform(t, pyTransformerURL, []types.TransformerEvent{ev2})
					require.Equal(t, http.StatusOK, status2)
					require.Len(t, items2, 1)
					require.Equal(t, http.StatusOK, items2[0].StatusCode, "second request must succeed")

					require.EqualValues(t, 1, newConns.Load(),
						"with ENABLE_CONN_POOL=true, two sequential "+
							"``from requests.api import get; get(url)`` calls "+
							"must reuse the same TCP connection (server-side "+
							"StateNew count: want 1). A count of 2 means the "+
							"calls bypassed the pooling wrapper and each "+
							"created a throwaway Session.")
				})
			}
		})
	}
}
