package pytransformer_contract

import (
	"context"
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
// For GET, the corresponding “Session.get“ signature drops “params“ —
// it's keyword-only on the session method — so the pytransformer pooling
// layer must bridge the two shapes. For POST/PUT/PATCH the session
// method shares the module-level signature, so the pooling layer must
// forward positional arguments verbatim and must NOT re-promote them to
// keywords (doing so makes “requests.post(url, body, json_payload)“
// raise “TypeError: got multiple values for argument 'data'“). The
// contract is:
//
//  1. Old arch (rudder-transformer + openfaas-flask-base): user code runs
//     against vanilla “requests“, so every two-positional call reaches
//     the backend with the expected shape.
//  2. New arch (rudder-pytransformer), “ENABLE_CONN_POOL=false“: bare
//     calls reach “requests“ unmodified, same as old arch.
//  3. New arch (rudder-pytransformer), “ENABLE_CONN_POOL=true“: bare
//     calls flow through the pooled “Session“. GET goes through the
//     params-promotion bridge; POST/PUT/PATCH are forwarded verbatim.
//     The observable result stays identical to the other two paths.
//
// Every verb × every new-arch configuration: the old-arch and new-arch
// responses must compare equal field-for-field via “types.Response.Equal“.
func TestBareRequestsPositionalParamsContract(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 2 * time.Minute

	const versionID = "bare-requests-positional-params-v1"

	// Echo server: returns the ``q`` field back in a JSON body.
	// ``r.FormValue`` pulls from the URL query string (GET) AND the
	// form-encoded body (POST/PUT/PATCH), so the same handler can echo
	// all four verbs without branching.
	echo := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.FormValue("q")
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"echo": %q, "method": %q}`, q, r.Method)
	}))
	t.Cleanup(echo.Close)

	// Dispatcher user code: picks the verb from the incoming event so
	// the same versionID can exercise all four two-positional shapes
	// without spinning up a separate openfaas-flask-base container per
	// verb. The line actually under test — ``requests.<verb>(url,
	// {"q": "hello"})`` — is identical to what a real customer would
	// write; only the surrounding if/elif selects which verb runs.
	//
	// For GET the positional dict becomes the query string; for
	// POST/PUT/PATCH the positional dict is form-encoded into the body.
	// Both paths make the echo server's ``r.FormValue("q")`` return
	// ``"hello"`` so the assertion shape is uniform across verbs.
	code := fmt.Sprintf(`
import requests

def transformEvent(event, metadata):
    verb = event["verb"]
    url = "%s/search"
    if verb == "get":
        resp = requests.get(url, {"q": "hello"})
    elif verb == "post":
        resp = requests.post(url, {"q": "hello"})
    elif verb == "put":
        resp = requests.put(url, {"q": "hello"})
    elif verb == "patch":
        resp = requests.patch(url, {"q": "hello"})
    else:
        raise ValueError("unknown verb: " + repr(verb))
    body = resp.json()
    event["echo"] = body["echo"]
    event["method"] = body["method"]
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
	// every new-arch configuration must match. Started once and shared
	// across every new-arch / verb combination.

	t.Log("Starting openfaas-flask-base (old arch backend)...")
	openFaasURL := startOpenFaasFlask(t, pool, versionID, configBackend.URL)

	t.Log("Starting mock OpenFaaS gateway...")
	mockGateway, _ := newMockOpenFaaSGateway(t, func() string { return openFaasURL })
	t.Cleanup(mockGateway.Close)

	t.Log("Starting rudder-transformer (old arch frontend)...")
	transformerURL := startRudderTransformer(t, pool, configBackend.URL, mockGateway.URL)

	// --- New architecture (rudder-pytransformer) ---
	//
	// Exercised twice — once with the connection pool disabled (bare
	// ``requests.<method>`` calls reach the underlying helpers untouched)
	// and once with it enabled (bare calls are routed through a shared
	// pooled ``Session``). Both configurations must match the old-arch
	// reference for every verb.
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
			// Pin pool + subprocess count to 1 so a single long-lived
			// user session handles every call: no subprocess affinity
			// or pool recycling can influence the outcome.
			extraPytransEnv: []string{
				"USER_CONN_POOL_MAX_SIZE=1",
				"SANDBOX_POOL_MAX_SIZE=1",
			},
		},
	}

	// Every verb that accepts a second positional argument must survive
	// the pooling bridge. GET exercises the params-promotion path; the
	// rest exercise the verbatim forwarding path.
	verbCases := []struct {
		verb   string
		method string // HTTP method the echo server should observe
	}{
		{verb: "get", method: "GET"},
		{verb: "post", method: "POST"},
		{verb: "put", method: "PUT"},
		{verb: "patch", method: "PATCH"},
	}

	for _, tc := range newArchCases {
		t.Run(tc.name, func(t *testing.T) {
			pyEnv := append([]string{"ENABLE_CONN_POOL=" + tc.enableConnPool}, tc.extraPytransEnv...)
			t.Logf("Starting rudder-pytransformer with %v...", pyEnv)
			pyTransformerURL := startRudderPytransformer(t, pool, configBackend.URL, pyEnv...)

			for _, vc := range verbCases {
				t.Run(vc.verb, func(t *testing.T) {
					// Fresh env per verb so memstats retry counters don't bleed between subtests (memstats accumulates
					// and cannot be reset).
					env := newBCTestEnv(t, transformerURL, pyTransformerURL,
						withFailOnError(),
						withLimitedRetryableHTTPRetries(),
					)

					event := makeEvent("msg-"+vc.verb, versionID)
					event.Message["verb"] = vc.verb
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
						"new arch (ENABLE_CONN_POOL=%s, verb=%s): 1 success event "+
							"expected — incorrect argument forwarding raises TypeError "+
							"before the HTTP call and fails the event instead",
						tc.enableConnPool, vc.verb)
					require.Equal(t, 0, len(newResp.FailedEvents), "new arch: no failed events expected")

					// Round-trip sanity check: the echo server must have
					// seen ``q=hello`` on both stacks, which means the
					// positional dict was forwarded correctly whether
					// that happens via the GET params-promotion bridge
					// or verbatim positional forwarding for the body
					// verbs.
					require.Equal(t, "hello", oldResp.Events[0].Output["echo"],
						"old arch must forward the positional dict as q=hello")
					require.Equalf(t, "hello", newResp.Events[0].Output["echo"],
						"new arch (ENABLE_CONN_POOL=%s, verb=%s) must forward the "+
							"positional dict as q=hello", tc.enableConnPool, vc.verb)

					// Method sanity: the echo server must also have seen
					// the right HTTP verb, proving that the pooling
					// wrapper dispatched to the right Session method and
					// didn't silently downgrade to GET.
					require.Equalf(t, vc.method, newResp.Events[0].Output["method"],
						"new arch (ENABLE_CONN_POOL=%s): echo server must have "+
							"observed HTTP %s", tc.enableConnPool, vc.method)

					// Strict parity: every field of the two responses must match.
					diff, equal := oldResp.Equal(&newResp)
					require.Truef(t, equal,
						"ENABLE_CONN_POOL=%s, verb=%s: old and new architectures "+
							"must produce identical responses for bare "+
							"requests.%s(url, positional_dict):\n%s",
						tc.enableConnPool, vc.verb, vc.verb, diff)

					env.assertRetryCountsMatch(t)
				})
			}
		})
	}
}

// TestBareRequestsPostThreePositionalArgsContract locks the contract that
// “requests.post(url, data, json)“ — all three arguments passed
// positionally — behaves identically under both architectures and under
// both values of the pytransformer connection-pool flag.
//
// The module-level signature “requests.post(url, data=None, json=None, **kwargs)“
// allows every user to write:
//
//	requests.post("https://example.com/events", body, json_payload)
//
// The new-arch pooling layer used to mishandle this shape: it promoted
// “args[1]“ (the body) to “data=“ while leaving “args[2]“ (the JSON
// payload) positional, so the forwarded call became
// “session.post(url, json_payload, data=body)“ — which binds
// “json_payload“ to “data“ and then collides with the promoted
// keyword, raising “TypeError: post() got multiple values for argument
// 'data'“. The failure only appears when “ENABLE_CONN_POOL=true“ and a
// three-positional “post“ call reaches the wrapper.
//
// This contract pins the correct behaviour: the old arch (vanilla
// “requests“) accepts this call shape; every new-arch configuration must
// too.
func TestBareRequestsPostThreePositionalArgsContract(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 2 * time.Minute

	const versionID = "bare-requests-post-three-positional-v1"

	// Echo server: captures the raw body seen by the handler and surfaces
	// it in the response. “requests.PreparedRequest.prepare_body“ picks
	// “data“ over “json“ when both are provided, so the server sees the
	// raw ``data`` bytes. That's fine for this test — what we need to
	// verify is that the HTTP call completes at all, not which payload
	// wins the precedence game.
	echo := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := make([]byte, r.ContentLength)
		if r.ContentLength > 0 {
			_, _ = r.Body.Read(body)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"received": %q}`, string(body))
	}))
	t.Cleanup(echo.Close)

	// User code exercising the three-positional “post“ form. The second
	// positional is the request body (“data“), the third is the JSON
	// payload (“json“). Incorrect argument forwarding raises “TypeError“
	// before the server is hit; the correct path echoes the body back.
	code := fmt.Sprintf(`
import requests

def transformEvent(event, metadata):
    resp = requests.post("%s/events", b"raw=payload", {"flush": True})
    event["received"] = resp.json()["received"]
    return event
`, toContainerURL(echo.URL))

	configBackend := newContractConfigBackend(t, map[string]configBackendEntry{
		versionID: {code: code},
	})
	t.Cleanup(configBackend.Close)

	t.Log("Starting openfaas-flask-base (old arch backend)...")
	openFaasURL := startOpenFaasFlask(t, pool, versionID, configBackend.URL)

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
			// Pin pool + subprocess count to 1 so incorrect argument
			// forwarding cannot be masked by a cold subprocess bypassing
			// the pool wrapper.
			extraPytransEnv: []string{
				"USER_CONN_POOL_MAX_SIZE=1",
				"SANDBOX_POOL_MAX_SIZE=1",
			},
		},
	}

	for _, tc := range newArchCases {
		t.Run(tc.name, func(t *testing.T) {
			pyEnv := append([]string{"ENABLE_CONN_POOL=" + tc.enableConnPool}, tc.extraPytransEnv...)
			t.Logf("Starting rudder-pytransformer with %v...", pyEnv)
			pyTransformerURL := startRudderPytransformer(t, pool, configBackend.URL, pyEnv...)

			env := newBCTestEnv(t, transformerURL, pyTransformerURL,
				withFailOnError(),
				withLimitedRetryableHTTPRetries(),
			)

			events := []types.TransformerEvent{makeEvent("msg-1", versionID)}

			t.Log("Sending request to old architecture...")
			oldResp := env.OldClient.Transform(context.Background(), events)
			t.Logf("Old arch: Events=%d, FailedEvents=%d", len(oldResp.Events), len(oldResp.FailedEvents))

			t.Log("Sending request to new architecture...")
			newResp := env.NewClient.Transform(context.Background(), events)
			t.Logf("New arch: Events=%d, FailedEvents=%d", len(newResp.Events), len(newResp.FailedEvents))

			require.Equal(t, 1, len(oldResp.Events), "old arch: 1 success event expected")
			require.Equal(t, 0, len(oldResp.FailedEvents), "old arch: no failed events expected")
			require.Equalf(t, 1, len(newResp.Events),
				"new arch (ENABLE_CONN_POOL=%s): 1 success event expected — a "+
					"bad pooling wrapper raises TypeError before the HTTP "+
					"call and fails the event instead",
				tc.enableConnPool)
			require.Equal(t, 0, len(newResp.FailedEvents), "new arch: no failed events expected")

			// Round-trip sanity check: the echo server must have seen
			// the raw body on both stacks, which means the second
			// positional bound to ``data`` and the call completed.
			require.Equal(t, "raw=payload", oldResp.Events[0].Output["received"],
				"old arch must forward the positional body as the request payload")
			require.Equalf(t, "raw=payload", newResp.Events[0].Output["received"],
				"new arch (ENABLE_CONN_POOL=%s) must forward the positional "+
					"body as the request payload", tc.enableConnPool)

			diff, equal := oldResp.Equal(&newResp)
			require.Truef(t, equal,
				"ENABLE_CONN_POOL=%s: old and new architectures must produce "+
					"identical responses for bare requests.post(url, body, json_payload):\n%s",
				tc.enableConnPool, diff)

			env.assertRetryCountsMatch(t)
		})
	}
}
