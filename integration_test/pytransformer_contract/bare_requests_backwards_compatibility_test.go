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
// user code calling bare "requests" helpers with a second positional
// argument produces identical results on both versions, regardless
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
//  1. Baseline (last released rudder-pytransformer): the reference
//     behaviour — every two-positional call reaches the backend with
//     the expected shape.
//  2. Candidate (rudder-pytransformer): bare calls flow through the
//     pooled “Session“. GET goes through the params-promotion bridge;
//     POST/PUT/PATCH are forwarded verbatim. The observable result stays
//     identical to the baseline.
//
// Every verb: the baseline and candidate responses must compare equal
// field-for-field via “types.Response.Equal“.
func TestBareRequestsPositionalParamsContract(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 2 * time.Minute

	const versionID = "bare-requests-positional-params-v1"

	// Echo server: returns the `q` field back in a JSON body.
	// `r.FormValue` pulls from the URL query string (GET) AND the
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
	// without a separate container per verb. The line actually under
	// test — `requests.<verb>(url, {"q": "hello"})` — is identical to
	// what a real customer would write; only the surrounding if/elif
	// selects which verb runs.
	//
	// For GET the positional dict becomes the query string; for
	// POST/PUT/PATCH the positional dict is form-encoded into the body.
	// Both paths make the echo server's `r.FormValue("q")` return
	// `"hello"` so the assertion shape is uniform across verbs.
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

	// Bare `requests.<method>` calls are routed through a per-transformation
	// pooled `Session`. Pin pool + subprocess count to 1 so a single long-lived
	// user session handles every call: no subprocess affinity or pool recycling
	// can influence the outcome.
	//
	// Both versions get identical environment so the comparison isolates the
	// version difference and nothing else.
	pytEnv := []string{
		"USER_CONN_POOL_MAX_SIZE=1",
		"SANDBOX_POOL_MAX_SIZE=1",
	}

	t.Log("Starting baseline rudder-pytransformer...")
	baselineURL := startBaselinePytransformer(t, pool, configBackend.URL, pytEnv...)

	t.Log("Starting candidate rudder-pytransformer...")
	candidateURL := startRudderPytransformer(t, pool, configBackend.URL, pytEnv...)

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

	for _, vc := range verbCases {
		t.Run(vc.verb, func(t *testing.T) {
			// Fresh env per verb so memstats retry counters don't bleed between subtests (memstats accumulates
			// and cannot be reset).
			env := newBCTestEnv(t, baselineURL, candidateURL,
				withFailOnError(),
				withLimitedRetryableHTTPRetries(),
			)

			event := makeEvent("msg-"+vc.verb, versionID)
			event.Message["verb"] = vc.verb
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
				"candidate (verb=%s): 1 success event expected — incorrect "+
					"argument forwarding raises TypeError before the HTTP "+
					"call and fails the event instead",
				vc.verb)
			require.Equal(t, 0, len(candidateResp.FailedEvents), "candidate: no failed events expected")

			// Round-trip sanity check: the echo server must have
			// seen `q=hello` on both stacks, which means the
			// positional dict was forwarded correctly whether
			// that happens via the GET params-promotion bridge
			// or verbatim positional forwarding for the body
			// verbs.
			require.Equal(t, "hello", baselineResp.Events[0].Output["echo"],
				"baseline must forward the positional dict as q=hello")
			require.Equalf(t, "hello", candidateResp.Events[0].Output["echo"],
				"candidate (verb=%s) must forward the positional dict as q=hello",
				vc.verb)

			// Method sanity: the echo server must also have seen
			// the right HTTP verb, proving that the pooling
			// wrapper dispatched to the right Session method and
			// didn't silently downgrade to GET.
			require.Equalf(t, vc.method, candidateResp.Events[0].Output["method"],
				"candidate: echo server must have observed HTTP %s", vc.method)

			// Strict parity: every field of the two responses must match.
			diff, equal := baselineResp.Equal(&candidateResp)
			require.Truef(t, equal,
				"verb=%s: baseline and candidate must produce identical "+
					"responses for bare requests.%s(url, positional_dict):\n%s",
				vc.verb, vc.verb, diff)

			env.assertRetryCountsMatch(t)
		})
	}
}

// TestBareRequestsPostThreePositionalArgsContract locks the contract that
// “requests.post(url, data, json)“ — all three arguments passed
// positionally — behaves identically under both versions and under
// both values of the pytransformer connection-pool flag.
//
// The module-level signature “requests.post(url, data=None, json=None, **kwargs)“
// allows every user to write:
//
//	requests.post("https://example.com/events", body, json_payload)
//
// The candidate pooling layer used to mishandle this shape: it promoted
// “args[1]“ (the body) to “data=“ while leaving “args[2]“ (the JSON
// payload) positional, so the forwarded call became
// “session.post(url, json_payload, data=body)“ — which binds
// “json_payload“ to “data“ and then collides with the promoted
// keyword, raising “TypeError: post() got multiple values for argument
// 'data'“.
//
// This contract pins the correct behaviour: the baseline (vanilla
// “requests“) accepts this call shape; the candidate must too.
func TestBareRequestsPostThreePositionalArgsContract(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 2 * time.Minute

	const versionID = "bare-requests-post-three-positional-v1"

	// Echo server: captures the raw body seen by the handler and surfaces
	// it in the response. “requests.PreparedRequest.prepare_body“ picks
	// “data“ over “json“ when both are provided, so the server sees the
	// raw `data` bytes. That's fine for this test — what we need to
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

	// Pin pool + subprocess count to 1 so incorrect argument forwarding
	// cannot be masked by a cold subprocess bypassing the pool wrapper.
	pytEnv := []string{
		"USER_CONN_POOL_MAX_SIZE=1",
		"SANDBOX_POOL_MAX_SIZE=1",
	}

	t.Log("Starting baseline rudder-pytransformer...")
	baselineURL := startBaselinePytransformer(t, pool, configBackend.URL, pytEnv...)

	t.Log("Starting candidate rudder-pytransformer...")
	candidateURL := startRudderPytransformer(t, pool, configBackend.URL, pytEnv...)

	env := newBCTestEnv(t, baselineURL, candidateURL,
		withFailOnError(),
		withLimitedRetryableHTTPRetries(),
	)

	events := []types.TransformerEvent{makeEvent("msg-1", versionID)}

	t.Log("Sending request to baseline...")
	baselineResp := env.BaselineClient.Transform(context.Background(), events)
	t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))

	t.Log("Sending request to candidate...")
	candidateResp := env.CandidateClient.Transform(context.Background(), events)
	t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))

	require.Equal(t, 1, len(baselineResp.Events), "baseline: 1 success event expected")
	require.Equal(t, 0, len(baselineResp.FailedEvents), "baseline: no failed events expected")
	require.Equal(t, 1, len(candidateResp.Events),
		"candidate: 1 success event expected — a bad pooling wrapper raises "+
			"TypeError before the HTTP call and fails the event instead")
	require.Equal(t, 0, len(candidateResp.FailedEvents), "candidate: no failed events expected")

	// Round-trip sanity check: the echo server must have seen the raw
	// body on both stacks, which means the second positional bound to
	// `data` and the call completed.
	require.Equal(t, "raw=payload", baselineResp.Events[0].Output["received"],
		"baseline must forward the positional body as the request payload")
	require.Equal(t, "raw=payload", candidateResp.Events[0].Output["received"],
		"candidate must forward the positional body as the request payload")

	diff, equal := baselineResp.Equal(&candidateResp)
	require.Truef(t, equal,
		"baseline and candidate must produce identical responses for "+
			"bare requests.post(url, body, json_payload):\n%s", diff)

	env.assertRetryCountsMatch(t)
}
