package pytransformer_contract

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"

	"github.com/rudderlabs/rudder-server/processor/types"
)

// TestConnectionPoolCookieIsolation
//
// The test:
//
//  1. Starts a mock HTTP server that stamps a unique Set-Cookie header on
//     every response AND echoes any incoming Cookie header back in the JSON
//     body. Any leak is visible in the echoed body of a later request.
//  2. Starts pytransformer with SANDBOX_POOL_MAX_SIZE=1 so ALL 10
//     parallel requests are serialized through ONE worker subprocess and
//     therefore ONE _user_session. This is the worst case for leakage:
//     if cookies could ever persist on the shared session, they will be
//     observed here.
//  3. Fires 10 concurrent /customTransform POSTs. Each transformation
//     calls the cookie-echoing server once and copies the echoed Cookie
//     header into the transformed event under "received_cookie".
//  4. Asserts every transformation observed an empty Cookie header — if
//     any carried a cookie from another invocation, the session leaked.
//  5. Asserts the server saw exactly 10 hits so no event was silently
//     dropped.
//  6. Asserts the server saw exactly 1 new TCP connection (via the
//     “ConnState“ hook used by “TestConnectionPoolBehavior“): with
//     “USER_CONN_POOL_MAX_SIZE=1“ and a single worker subprocess the
//     pooled session must reuse a single kept-alive socket across all
//     10 invocations, proving that connection pooling is effective AND
//     that stripping cookies did not accidentally force fresh handshakes.
func TestConnectionPoolCookieIsolation(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 2 * time.Minute

	const (
		versionID        = "cookie-isolation-v1"
		parallelRequests = 10
	)

	cookieSrv, hits, newConns := newCookieEchoServer(t)

	entries := map[string]configBackendEntry{
		versionID: {code: fmt.Sprintf(`
import requests

def transformEvent(event, metadata):
    resp = requests.get("%s/check")
    data = resp.json()
    # The mock echoes the incoming Cookie header. If the pooled session
    # leaked state from a prior transformation, "received_cookie" will be
    # non-empty — surfaced to the test via the transformed event.
    event["received_cookie"] = data["received_cookie"]
    # The response itself DID carry a Set-Cookie — we expose it so the
    # test can assert the leak channel exists (the server sent a cookie,
    # requests parsed it) even though nothing carried it forward.
    event["response_cookie"] = resp.cookies.get("leaky", "")
    event["status"] = resp.status_code
    return event
`, toContainerURL(cookieSrv.URL))},
	}

	configBackend := newContractConfigBackend(t, entries)
	t.Cleanup(configBackend.Close)

	pyURL := startRudderPytransformer(
		t, pool, configBackend.URL,
		// Single worker subprocess so every /customTransform request is
		// dispatched through the SAME _user_session. Any cookie
		// accumulated by an earlier invocation is guaranteed to be visible
		// to later invocations, maximising leak-detection sensitivity.
		"SANDBOX_POOL_MAX_SIZE=1",
		// A single pooled connection forces the same underlying TCP
		// socket to be reused across requests — the most aggressive
		// pooling configuration, where leak mechanisms would be most
		// pronounced.
		"USER_CONN_POOL_MAX_SIZE=1",
	)

	type result struct {
		idx            int
		status         int
		items          []types.TransformerResponse
		receivedCookie string
		responseCookie string
		ok             bool
	}

	var (
		wg      sync.WaitGroup
		results = make([]result, parallelRequests)
	)
	for i := range parallelRequests {
		wg.Go(func() {
			msgID := fmt.Sprintf("msg-cookie-iso-%d", i)
			events := []types.TransformerEvent{makeEvent(msgID, versionID)}
			status, items := sendRawTransform(t, pyURL, events)

			res := result{idx: i, status: status, items: items}
			if len(items) == 1 && items[0].StatusCode == http.StatusOK {
				gotReceived, okR := items[0].Output["received_cookie"].(string)
				gotResponse, okP := items[0].Output["response_cookie"].(string)
				if okR && okP {
					res.receivedCookie = gotReceived
					res.responseCookie = gotResponse
					res.ok = true
				}
			}
			results[i] = res
		})
	}
	wg.Wait()

	// Sanity: every transformation returned a 200 response and produced a
	// single successful event whose output was parsed into our struct.
	for _, r := range results {
		require.Equal(t, http.StatusOK, r.status,
			"request %d: /customTransform must return HTTP 200", r.idx)
		require.Len(t, r.items, 1,
			"request %d: expected exactly one transformer response item", r.idx)
		require.Equal(t, http.StatusOK, r.items[0].StatusCode,
			"request %d: per-event status must be 200 (error: %s)",
			r.idx, r.items[0].Error)
		require.True(t, r.ok,
			"request %d: transformed event missing received_cookie/response_cookie fields", r.idx)
	}

	// PROOF THE LEAK CHANNEL EXISTS: every response MUST have carried a
	// Set-Cookie ``leaky=secret_N`` header that the ``requests`` library
	// parsed into ``resp.cookies``. Without this check, a regression that
	// silently stopped the mock from setting cookies would make the
	// leak-detection assertion below vacuously true.
	for _, r := range results {
		require.Regexp(t, "secret_[0-9]+", r.responseCookie,
			"request %d: response must carry a leaky=secret_N cookie "+
				"(got %q) — the leak channel must exist for the "+
				"isolation assertion below to be meaningful",
			r.idx, r.responseCookie)
	}

	// CORE ASSERTION: no transformation observed a cookie from any other
	// transformation. A non-empty received_cookie on ANY event is proof
	// that the shared pooled session leaked state across invocations.
	for _, r := range results {
		require.Empty(t, r.receivedCookie,
			"request %d observed leaked cookie %q — the shared pooled "+
				"user session must not carry Set-Cookie state across "+
				"transformations",
			r.idx, r.receivedCookie)
	}

	// Exactly 10 hits: no silent retries, no silent drops. If this count
	// drifts up, the test is noisy; if it drifts down, some transformation
	// silently skipped its HTTP call and the cookie check above is
	// vacuously true.
	require.EqualValues(t, parallelRequests, hits.Load(),
		"cookie server must have been hit exactly %d times "+
			"(one per parallel transformation); got %d",
		parallelRequests, hits.Load())

	// Exactly ONE new TCP connection: with USER_CONN_POOL_MAX_SIZE=1 and
	// SANDBOX_POOL_MAX_SIZE=1 the pooled session must serve every
	// invocation from the same kept-alive socket. This is the same
	// server-side proof used in ``TestConnectionPoolBehavior``:
	// ``http.StateNew`` fires exactly once per TCP handshake, so a
	// count > 1 means the pool failed to reuse the connection and the
	// test no longer exercises the shared-session path.
	require.EqualValues(t, 1, newConns.Load(),
		"expected exactly 1 new TCP connection for %d pooled requests, "+
			"got %d — the pool must reuse a single kept-alive socket",
		parallelRequests, newConns.Load())
}

// TestConnectionPoolRequestOptionIsolation locks the cross-architecture
// contract that per-request options attached to a bare “requests.<verb>“
// call never become defaults on the user-facing HTTP session.
//
// The goal is parity, not a one-sided property check. The same
// transformation code runs through two stacks:
//
//  1. Old arch — rudder-transformer + openfaas-flask-base (vanilla
//     “requests“ inside the OpenFaaS container).
//  2. New arch — rudder-pytransformer routing every bare call through a
//     “StatelessPooledSession“ pinned to a single TCP socket. This is
//     the path that could regress if the pooling wrapper ever started
//     persisting “headers“ / “auth“ / “params“ / “cookies“ / “hooks“
//     onto the shared session.
//
// Both stacks must produce the exact same “types.Response“, so any drift
// introduced by the pooling layer surfaces via “oldResp.Equal(&newResp)“.
//
// The batch of transformation events is larger than one so the assertion
// covers cross-transformation isolation on the shared session, not only
// cross-call isolation within one transformation.
//
// The echo server's “ConnState“ counter is also asserted: with
// “SANDBOX_POOL_MAX_SIZE=1“ and “USER_CONN_POOL_MAX_SIZE=1“ every HTTP
// call the transformations make MUST flow through one kept-alive socket,
// otherwise the test is not actually exercising the shared-session path
// and the isolation assertion is vacuously true.
func TestConnectionPoolRequestOptionIsolation(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 2 * time.Minute

	const (
		versionID = "request-options-isolation-v1"
		// Each event makes 2 sequential HTTP calls. ``numEvents`` > 1 so the
		// batch exercises isolation ACROSS transformations sharing the
		// pooled session, not just across sequential calls inside one
		// transformation.
		numEvents = 3
	)

	echoSrv, newConns := newSessionDefaultsEchoServer(t)

	// User code: every ``transformEvent`` invocation issues two sequential
	// requests. The first primes a vanilla ``requests.Session`` with every
	// per-request option ``requests`` supports; the second issues a bare
	// GET. If any option persisted to session state — on the old-arch
	// vanilla session OR the new-arch ``StatelessPooledSession`` — the
	// second call would observe it and the parity check would fail.
	code := fmt.Sprintf(`
import requests


def hook(response, **kwargs):
    response.headers["X-Hook-Ran"] = "1"
    return response


def transformEvent(event, metadata):
    first = requests.get(
        "%s/check",
        params={"leak": "1"},
        headers={"X-Leak": "1"},
        auth=("user", "pass"),
        cookies={"client_cookie": "1"},
        hooks={"response": [hook]},
        timeout=2,
    )
    first_data = first.json()

    second = requests.get("%s/check", timeout=2)
    second_data = second.json()

    event["first_status"] = first.status_code
    event["first_x_leak"] = first_data["x_leak"]
    event["first_authorization"] = first_data["authorization"]
    event["first_query"] = first_data["query"]
    event["first_cookie"] = first_data["cookie"]
    event["first_hook_ran"] = first.headers.get("X-Hook-Ran", "")
    # Proof-of-mechanism (not a persistence check): the echo server sends
    # Set-Cookie: server_cookie=secret on every response, and the requests
    # library parses it into the per-RESPONSE cookie jar (first.cookies is
    # a RequestsCookieJar bound to the response, not to the session). If
    # this field were ever empty the parity / isolation assertions below
    # would be vacuously true, so the Go test explicitly requires it to
    # equal "secret" on both stacks — mirrors the proof-of-leak-channel
    # check in TestConnectionPoolCookieIsolation.
    event["first_response_cookie"] = first.cookies.get("server_cookie", "")

    event["second_status"] = second.status_code
    event["second_x_leak"] = second_data["x_leak"]
    event["second_authorization"] = second_data["authorization"]
    event["second_query"] = second_data["query"]
    event["second_cookie"] = second_data["cookie"]
    event["second_hook_ran"] = second.headers.get("X-Hook-Ran", "")
    return event
`, toContainerURL(echoSrv.URL), toContainerURL(echoSrv.URL))

	configBackend := newContractConfigBackend(t, map[string]configBackendEntry{
		versionID: {code: code},
	})
	t.Cleanup(configBackend.Close)

	// --- Old architecture (rudder-transformer + openfaas-flask-base) ---
	//
	// Becomes the reference behaviour the new arch must match.

	t.Log("Starting openfaas-flask-base (old arch backend)...")
	openFaasURL := startOpenFaasFlask(t, pool, versionID, configBackend.URL)

	t.Log("Starting mock OpenFaaS gateway...")
	mockGateway, _ := newMockOpenFaaSGateway(t, func() string { return openFaasURL })
	t.Cleanup(mockGateway.Close)

	t.Log("Starting rudder-transformer (old arch frontend)...")
	transformerURL := startRudderTransformer(t, pool, configBackend.URL, mockGateway.URL)

	events := make([]types.TransformerEvent, numEvents)
	for i := range events {
		events[i] = makeEvent(fmt.Sprintf("msg-req-opt-iso-%d", i), versionID)
	}

	// --- New architecture (rudder-pytransformer) ---
	//
	// Every bare requests.<verb> call routes through StatelessPooledSession.
	// Pin SANDBOX_POOL_MAX_SIZE=1 and USER_CONN_POOL_MAX_SIZE=1 so the
	// test actually exercises the shared-session path, not a fresh
	// session per subprocess.
	pyTransformerURL := startRudderPytransformer(
		t, pool, configBackend.URL,
		"SANDBOX_POOL_MAX_SIZE=1",
		"USER_CONN_POOL_MAX_SIZE=1",
	)

	env := newBCTestEnv(t, transformerURL, pyTransformerURL,
		withFailOnError(),
		withLimitedRetryableHTTPRetries(),
	)

	t.Log("Sending batch to old arch (openfaas-flask-base)...")
	oldResp := env.OldClient.Transform(context.Background(), events)
	t.Logf("Old arch: Events=%d, FailedEvents=%d", len(oldResp.Events), len(oldResp.FailedEvents))

	// Only the NEW-arch run's TCP activity counts toward the pool reuse
	// assertion — reset the counter so every connection the old-arch
	// stack opened to the shared echo server is excluded.
	newConns.Store(0)

	t.Log("Sending batch to new arch (rudder-pytransformer)...")
	newResp := env.NewClient.Transform(context.Background(), events)
	t.Logf("New arch: Events=%d, FailedEvents=%d", len(newResp.Events), len(newResp.FailedEvents))

	require.Equalf(t, numEvents, len(oldResp.Events), "old arch: all %d events must succeed", numEvents)
	require.Empty(t, oldResp.FailedEvents, "old arch: no failed events expected")
	require.Equalf(t, numEvents, len(newResp.Events),
		"new arch: all %d events must succeed", numEvents)
	require.Empty(t, newResp.FailedEvents, "new arch: no failed events expected")

	// Proof-of-mechanism: every event on BOTH stacks must have carried
	// the full set of per-request options on its first call. Without
	// these checks, a regression that stopped the mock echo from
	// observing any single option would make the isolation parity check
	// below vacuously true for that option.
	for _, resp := range []*types.Response{&oldResp, &newResp} {
		for _, ev := range resp.Events {
			require.Equalf(t, "1", ev.Output["first_x_leak"],
				"msg=%s: first request must carry X-Leak header",
				ev.Metadata.MessageID)
			// Exact match (rather than NotEmpty) — base64("user:pass").
			// A malformed Authorization header would still be
			// non-empty but would silently diverge from the
			// old-arch reference.
			require.Equalf(t, "Basic dXNlcjpwYXNz", ev.Output["first_authorization"],
				"msg=%s: first request must carry Basic user:pass auth",
				ev.Metadata.MessageID)
			require.Equalf(t, "leak=1", ev.Output["first_query"],
				"msg=%s: first request must carry params",
				ev.Metadata.MessageID)
			require.Equalf(t, "client_cookie=1", ev.Output["first_cookie"],
				"msg=%s: first request must carry Cookie",
				ev.Metadata.MessageID)
			require.Equalf(t, "1", ev.Output["first_hook_ran"],
				"msg=%s: response hook must have stamped X-Hook-Ran",
				ev.Metadata.MessageID)
			require.Equalf(t, "secret", ev.Output["first_response_cookie"],
				"msg=%s: echo server must have sent Set-Cookie and requests must have parsed it — otherwise the parity check below is vacuous",
				ev.Metadata.MessageID)
		}
	}

	// Core isolation assertion: the second bare GET, issued without any
	// options, must see NONE of the state the first call attached.
	// Vanilla ``requests.Session`` does not persist per-request kwargs,
	// so the old arch passes this trivially; ``StatelessPooledSession``
	// must match that guarantee on the new-arch pooled path. Per-field
	// asserts keep failure output actionable; ``Equal`` below is the
	// strict parity catch-all.
	for _, resp := range []*types.Response{&oldResp, &newResp} {
		for _, ev := range resp.Events {
			require.Emptyf(t, ev.Output["second_x_leak"],
				"msg=%s: header must not carry into the next request",
				ev.Metadata.MessageID)
			require.Emptyf(t, ev.Output["second_authorization"],
				"msg=%s: auth must not carry into the next request",
				ev.Metadata.MessageID)
			require.Emptyf(t, ev.Output["second_query"],
				"msg=%s: params must not carry into the next request",
				ev.Metadata.MessageID)
			require.Emptyf(t, ev.Output["second_cookie"],
				"msg=%s: cookies must not carry into the next request",
				ev.Metadata.MessageID)
			require.Emptyf(t, ev.Output["second_hook_ran"],
				"msg=%s: response hook must not carry into the next request",
				ev.Metadata.MessageID)
		}
	}

	// Strict parity: old arch and new arch must produce identical
	// responses field-for-field. Any divergence the per-field asserts
	// above missed (e.g. a difference in ``Metadata``, ``StatTags``, or
	// an Output key that was added to the transformation code but not
	// to this test) surfaces here.
	diff, equal := oldResp.Equal(&newResp)
	require.Truef(t, equal,
		"old and new architectures must produce identical responses:\n%s", diff)

	env.assertRetryCountsMatch(t)

	// With ``SANDBOX_POOL_MAX_SIZE=1`` and ``USER_CONN_POOL_MAX_SIZE=1``
	// every one of the ``numEvents * 2`` HTTP calls must have been
	// served by the SAME kept-alive socket. A count > 1 means the pool
	// silently failed to reuse the connection and the test no longer
	// exercises the shared-session path — the isolation assertion above
	// becomes meaningless (each call would be getting a fresh session).
	require.EqualValuesf(t, 1, newConns.Load(),
		"expected exactly 1 new TCP connection for %d pooled requests, got %d — the pool must reuse a single kept-alive socket",
		numEvents*2, newConns.Load())
}

// newCookieEchoServer returns an HTTP server that, on every request:
//   - Stamps a unique Set-Cookie response header (leaky=secret_N). A naive
//     shared requests.Session would store this cookie and re-send it on
//     the next request to the same host.
//   - Echoes the incoming Cookie header back in the JSON body under
//     "received_cookie". A transformation that sees a non-empty value on
//     its first (and only) call has observed a leaked cookie from another
//     transformation that ran earlier on the same pooled session.
//   - Setting Content-Length lets the client reuse the TCP connection
//     without chunked transfer, so “http.StateNew“ fires exactly once
//     per distinct TCP handshake — turning newConns into a reliable
//     "how many connections were opened" counter.
func newCookieEchoServer(t *testing.T) (*httptest.Server, *atomic.Int64, *atomic.Int64) {
	t.Helper()
	hits := &atomic.Int64{}
	newConns := &atomic.Int64{}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := hits.Add(1)
		incoming := r.Header.Get("Cookie")
		body := fmt.Appendf(nil, `{"received_cookie": %q}`, incoming)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		// Unique cookie per response
		w.Header().Set("Set-Cookie", fmt.Sprintf("leaky=secret_%d; Path=/", n))
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
	return srv, hits, newConns
}

// newSessionDefaultsEchoServer returns an HTTP server used by the per-request
// option isolation contract. Every response:
//   - Echoes the incoming “X-Leak“ / “Authorization“ / query string /
//     “Cookie“ headers back as JSON so user transformation code can prove
//     which options reached the server.
//   - Stamps “Set-Cookie: server_cookie=secret“ so the caller can verify
//     the mock leak channel exists (see the “first_response_cookie“
//     proof-of-mechanism check in “TestConnectionPoolRequestOptionIsolation“).
//   - Sets “Content-Length“ so the client can keep-alive the TCP socket
//     without chunked transfer.
//
// The “ConnState“ hook counts “http.StateNew“ transitions into the
// returned “*atomic.Int64“ so the test can assert that the pytransformer
// pooled session reused a single kept-alive connection across every call —
// the same technique “newCookieEchoServer“ uses.
func newSessionDefaultsEchoServer(t *testing.T) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	newConns := &atomic.Int64{}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := jsonrs.Marshal(map[string]string{
			"x_leak":        r.Header.Get("X-Leak"),
			"authorization": r.Header.Get("Authorization"),
			"query":         r.URL.RawQuery,
			"cookie":        r.Header.Get("Cookie"),
		})
		if err != nil {
			t.Errorf("sessionDefaultsEchoServer: failed to marshal body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.Header().Set("Set-Cookie", "server_cookie=secret; Path=/")
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
