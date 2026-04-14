package pytransformer_contract

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/processor/types"
)

// TestBareRequestsPositionalParamsContract locks the contract that valid
// user code calling bare “requests“ helpers with a second positional
// argument produces identical results on both architectures, regardless
// of the pytransformer connection-pool feature flag.
//
// The module-level “requests“ helpers expose a second positional argument
// whose name depends on the verb:
//
//	requests.get(url, params=None, **kwargs)
//	requests.post(url, data=None, json=None, **kwargs)
//	requests.put(url, data=None, **kwargs)
//	requests.patch(url, data=None, **kwargs)
//
// The corresponding “requests.Session“ methods accept only “url“ as a
// positional; “params“/“data“ are keyword-only on “Session“. The
// pytransformer connection pool reroutes bare “requests.<method>()“ calls
// through a shared “Session“ when “ENABLE_CONN_POOL=true“, so the pooling
// layer must bridge the two signature shapes. The contract is:
//
//  1. Old arch (rudder-transformer + openfaas-flask-base): user code runs
//     against vanilla “requests“, so “requests.get(url, {"q": "hello"})“
//     succeeds and the backend receives “?q=hello“.
//  2. New arch (rudder-pytransformer), “ENABLE_CONN_POOL=false“: bare
//     calls reach “requests“ unmodified, same as old arch.
//  3. New arch (rudder-pytransformer), “ENABLE_CONN_POOL=true“: bare
//     calls flow through the pooled “Session“, but the promotion of the
//     second positional to the matching keyword keeps the observable result
//     identical to the other two paths.
//
// Every configuration must produce the same successful event output.
func TestBareRequestsPositionalParamsContract(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 2 * time.Minute

	const versionID = "bare-requests-positional-params-v1"

	// Echo server: returns the ``q`` query parameter back in a JSON body.
	// Successful round-trip requires the user transformation to forward
	// ``params={"q": "hello"}`` to the HTTP layer — regardless of which
	// runtime the code executed in.
	echo := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("q")
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"echo": %q}`, q)
	}))
	t.Cleanup(echo.Close)

	// User code that exercises the second-positional form of ``requests.get``.
	// Every runtime must treat this as equivalent to ``requests.get(url,
	// params={"q": "hello"})``.
	code := fmt.Sprintf(`
import requests

def transformEvent(event, metadata):
    resp = requests.get("%s/search", {"q": "hello"})
    event["echo"] = resp.json()["echo"]
    return event
`, toContainerURL(echo.URL))

	configBackend := newContractConfigBackend(t, map[string]configBackendEntry{
		versionID: {code: code},
	})
	t.Cleanup(configBackend.Close)

	// --- Old architecture (rudder-transformer + openfaas-flask-base) ---
	//
	// The old stack runs the user code under vanilla ``requests`` with no
	// pooling layer in front of it, so it defines the reference behaviour
	// every new-arch configuration must match.

	t.Log("Starting openfaas-flask-base (old arch backend)...")
	openFaasContainer, openFaasURL := startOpenFaasFlask(t, pool, versionID, configBackend.URL)
	t.Cleanup(func() {
		if err := pool.Purge(openFaasContainer); err != nil {
			t.Logf("Failed to purge openfaas-flask-base: %v", err)
		}
	})
	waitForOpenFaasFlask(t, pool, openFaasURL)

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

	oldArchEvents := []types.TransformerEvent{makeEvent("msg-old-arch", versionID)}
	t.Log("Sending request to old architecture (rudder-transformer + openfaas)...")
	oldStatus, oldItems := sendRawTransform(t, transformerURL, oldArchEvents)
	require.Equal(t, http.StatusOK, oldStatus,
		"old arch /customTransform must return HTTP 200")
	require.Len(t, oldItems, 1)
	require.Equalf(t, http.StatusOK, oldItems[0].StatusCode,
		"old arch must succeed: requests.get(url, {\"q\": \"hello\"}) is a valid "+
			"call against the real `requests` module. Got StatusCode=%d, Error=%q",
		oldItems[0].StatusCode, oldItems[0].Error)
	require.Equal(t, "hello", oldItems[0].Output["echo"],
		"old arch must forward the positional params dict as a ?q=hello query string")

	// --- New architecture (rudder-pytransformer) ---
	//
	// Exercised twice — once with the connection pool disabled (bare
	// ``requests.<method>`` calls reach the underlying helpers untouched)
	// and once with it enabled (bare calls are routed through a shared
	// pooled ``Session``). Both configurations must match the old-arch
	// reference.
	newArchCases := []struct {
		name              string
		enableConnPool    string
		extraPytransEnv   []string
		eventMessageIDTag string
	}{
		{
			name:              "ConnPoolDisabled",
			enableConnPool:    "false",
			extraPytransEnv:   nil,
			eventMessageIDTag: "msg-new-arch-nopool",
		},
		{
			name:           "ConnPoolEnabled",
			enableConnPool: "true",
			// Pin pool + subprocess count to 1 so a single long-lived
			// user session handles every call: no subprocess affinity
			// or pool recycling can influence the outcome.
			extraPytransEnv: []string{
				"USER_CONN_POOL_MAX_SIZE=1",
				"SANDBOX_POOL_MAX_SIZE=1",
			},
			eventMessageIDTag: "msg-new-arch-pool",
		},
	}

	for _, tc := range newArchCases {
		t.Run(tc.name, func(t *testing.T) {
			env := append([]string{"ENABLE_CONN_POOL=" + tc.enableConnPool}, tc.extraPytransEnv...)
			t.Logf("Starting rudder-pytransformer with %v...", env)
			pyTransformerURL := startRudderPytransformer(t, pool, configBackend.URL, env...)

			events := []types.TransformerEvent{makeEvent(tc.eventMessageIDTag, versionID)}
			t.Log("Sending request to new architecture (rudder-pytransformer)...")
			newStatus, newItems := sendRawTransform(t, pyTransformerURL, events)
			require.Equal(t, http.StatusOK, newStatus,
				"new arch /customTransform must return HTTP 200 "+
					"(per-event errors sit inside the payload, not at the HTTP layer)")
			require.Len(t, newItems, 1)

			require.Equalf(t, http.StatusOK, newItems[0].StatusCode,
				"new arch must succeed with ENABLE_CONN_POOL=%s: the pooling "+
					"layer must promote the second positional argument of bare "+
					"requests.<method>() calls to the matching keyword before "+
					"forwarding to the session. Got StatusCode=%d, Error=%q, Output=%v",
				tc.enableConnPool, newItems[0].StatusCode, newItems[0].Error, newItems[0].Output)
			require.Equalf(t, "hello", newItems[0].Output["echo"],
				"new arch (ENABLE_CONN_POOL=%s) must round-trip the positional "+
					"params dict identically to old arch (expected echo=%q)",
				tc.enableConnPool, "hello")
		})
	}
}
