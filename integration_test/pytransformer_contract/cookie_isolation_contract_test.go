package pytransformer_contract

import (
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

	"github.com/rudderlabs/rudder-server/processor/types"
)

// TestConnectionPoolCookieIsolation
//
// The test:
//
//  1. Starts a mock HTTP server that stamps a unique Set-Cookie header on
//     every response AND echoes any incoming Cookie header back in the JSON
//     body. Any leak is visible in the echoed body of a later request.
//  2. Starts pytransformer with ENABLE_CONN_POOL=true and
//     SANDBOX_POOL_MAX_SIZE=1 so ALL 10 parallel requests are serialized
//     through ONE worker subprocess and therefore ONE _user_session. This
//     is the worst case for leakage: if cookies could ever persist on the
//     shared session, they will be observed here.
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
		"ENABLE_CONN_POOL=true",
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
				"transformations (Bug 1 in rudder-pytransformer bugs.md)",
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

	// Exactly ONE new TCP connection: with ENABLE_CONN_POOL=true,
	// USER_CONN_POOL_MAX_SIZE=1 and SANDBOX_POOL_MAX_SIZE=1 the pooled
	// session must serve every invocation from the same kept-alive
	// socket. This is the same server-side proof used in
	// ``TestConnectionPoolBehavior/ConnectionReusedWhenPoolEnabled``:
	// ``http.StateNew`` fires exactly once per TCP handshake, so a
	// count > 1 means the pool failed to reuse the connection and the
	// test no longer exercises the "shared session" path that Bug 1
	// lived on.
	require.EqualValues(t, 1, newConns.Load(),
		"expected exactly 1 new TCP connection for %d pooled requests, "+
			"got %d — the pool must reuse a single kept-alive socket",
		parallelRequests, newConns.Load())
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
		body := []byte(fmt.Sprintf(`{"received_cookie": %q}`, incoming))
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
