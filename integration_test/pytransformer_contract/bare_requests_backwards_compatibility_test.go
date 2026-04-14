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

// TestBareRequestsPositionalParamsRegression
//
// The module-level helper `requests.get(url, params=None, **kwargs)` exposes
// `params` as a SECOND POSITIONAL argument, so passing a dict positionally is
// a supported, documented form. `requests.Session.get(self, url, **kwargs)` —
// which the pytransformer routes bare calls through when ENABLE_CONN_POOL=true —
// does NOT accept `params` positionally. The pooling wrapper forwards `*args`
// verbatim, so the call blows up with:
//
//	TypeError: Session.get() takes 2 positional arguments but 3 were given
//
// The same divergence affects `requests.post(url, data)`, `requests.put(url,
// data)`, and `requests.patch(url, data)` where `data` is the second positional.
//
// This test proves the regression by running the SAME user transformation
// against both architectures:
//
//   - Old arch (rudder-transformer + openfaas-flask-base): the code runs under
//     vanilla `requests`, so `requests.get(url, {"q": "hello"})` succeeds and
//     the server sees `?q=hello`.
//   - New arch (rudder-pytransformer with ENABLE_CONN_POOL=true): the bare
//     `requests.get` is rerouted through the pooled user Session, and the
//     positional dict triggers TypeError.
//
// The contract is that the two architectures MUST produce the same successful
// response for valid user code. This test fails today because of Bug 1 and will
// go green when the pooling wrapper maps the module-level positional signature
// to the Session equivalent.
func TestBareRequestsPositionalParamsRegression(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 2 * time.Minute

	const versionID = "bare-requests-positional-params-v1"

	// Echo server: returns the `q` query parameter back in a JSON body.
	// If the user transformation correctly forwarded `params={"q": "hello"}`,
	// the server observes `?q=hello` and the event round-trips the value.
	// If the dict is dropped (or the call crashes), the round-trip breaks.
	echo := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("q")
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"echo": %q}`, q)
	}))
	t.Cleanup(echo.Close)

	// User code that is valid against plain `requests` (old arch) but is
	// broken by the pytransformer pooling wrapper (new arch, Bug 1).
	// Note: `{"q": "hello"}` is passed POSITIONALLY — this is the exact
	// shape the bug turns into a TypeError.
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

	// --- New architecture (rudder-pytransformer with ENABLE_CONN_POOL=true) ---

	t.Log("Starting rudder-pytransformer with ENABLE_CONN_POOL=true (new arch)...")
	pyTransformerURL := startRudderPytransformer(
		t, pool, configBackend.URL,
		// Flag under test: flip the bare-requests path through the pooled
		// user session. This is the code path Bug 1 lives on.
		"ENABLE_CONN_POOL=true",
		// Pin pool + subprocess count to 1 so a single long-lived user
		// session handles every call — removes any chance the regression
		// is masked by subprocess affinity or pool recycling.
		"USER_CONN_POOL_MAX_SIZE=1",
		"SANDBOX_POOL_MAX_SIZE=1",
	)

	events := []types.TransformerEvent{makeEvent("msg-positional-params-1", versionID)}

	// --- Old arch: must succeed, round-tripping "hello" through the echo server. ---

	t.Log("Sending request to old architecture (rudder-transformer + openfaas)...")
	oldStatus, oldItems := sendRawTransform(t, transformerURL, events)
	require.Equal(t, http.StatusOK, oldStatus,
		"old arch /customTransform must return HTTP 200")
	require.Len(t, oldItems, 1)
	require.Equalf(t, http.StatusOK, oldItems[0].StatusCode,
		"old arch must succeed: requests.get(url, {\"q\": \"hello\"}) is valid "+
			"Python against the real `requests` module. Got StatusCode=%d, Error=%q",
		oldItems[0].StatusCode, oldItems[0].Error)
	require.Equal(t, "hello", oldItems[0].Output["echo"],
		"old arch must forward the positional params dict as a ?q=hello query string")

	// --- New arch: must ALSO succeed under the contract, but currently fails. ---
	//
	// Under Bug 1, the pooling wrapper forwards the positional dict to
	// Session.get(self, url, **kwargs), triggering:
	//   TypeError: Session.get() takes 2 positional arguments but 3 were given
	// which the sandbox surfaces as a 400 per-event failure. This assertion
	// therefore fails today and will pass only once the wrapper maps the
	// module-level positional signature to the Session equivalent.

	t.Log("Sending request to new architecture (rudder-pytransformer, ENABLE_CONN_POOL=true)...")
	newStatus, newItems := sendRawTransform(t, pyTransformerURL, events)
	require.Equal(t, http.StatusOK, newStatus,
		"new arch /customTransform must return HTTP 200 "+
			"(per-event errors sit inside the payload, not at the HTTP layer)")
	require.Len(t, newItems, 1)

	require.Equalf(t, http.StatusOK, newItems[0].StatusCode,
		"REGRESSION: with ENABLE_CONN_POOL=true, bare "+
			"requests.get(url, {\"q\": \"hello\"}) must succeed the same way it "+
			"does on the old arch. The pooling wrapper forwards the positional "+
			"params dict to Session.get, which only accepts params as a keyword, "+
			"raising TypeError. Got StatusCode=%d, Error=%q, Output=%v",
		newItems[0].StatusCode, newItems[0].Error, newItems[0].Output)
	require.Equalf(t, "hello", newItems[0].Output["echo"],
		"new arch must round-trip the positional params dict identically to "+
			"old arch (expected echo=%q)", "hello")
}
