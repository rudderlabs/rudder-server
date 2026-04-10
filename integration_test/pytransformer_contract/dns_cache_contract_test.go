package pytransformer_contract

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/httputil"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"

	"github.com/rudderlabs/rudder-server/processor/types"
)

// TestDNSCaching verifies that DNS caching in pytransformer works correctly
// and does not cause cross-request pollution.
//
// A single pytransformer container is started with DNS_CACHE_ENABLED=true.
// Four mock API servers (alpha, bravo, charlie, delta) each return a unique
// identifier. Four transformation versions each call a different mock server.
// The tests verify that sequential and parallel requests always reach the
// correct mock server, proving the DNS cache does not pollute across requests.
func TestDNSCaching(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 2 * time.Minute

	names := []string{"alpha", "bravo", "charlie", "delta"}

	type mockServer struct {
		server *httptest.Server
		calls  *atomic.Int64
	}
	mocks := make(map[string]mockServer, len(names))
	versionIDs := make(map[string]string, len(names))
	entries := make(map[string]configBackendEntry, len(names))

	for _, name := range names {
		srv, calls := newMockAPIServer(t, name)
		mocks[name] = mockServer{server: srv, calls: calls}
		vid := "dns-cache-" + name + "-v1"
		versionIDs[name] = vid
		entries[vid] = configBackendEntry{
			code: fmt.Sprintf(`
import requests

def transformEvent(event, metadata):
    resp = requests.get("%s/data", timeout=5)
    event['external'] = resp.json()
    return event
`, toContainerURL(srv.URL)),
		}
	}

	configBackend := newContractConfigBackend(t, entries)
	t.Cleanup(configBackend.Close)

	container, pyURL := startRudderPytransformer(
		t, pool, configBackend.URL,
		"DNS_CACHE_ENABLED=true",
		"DNS_CACHE_TTL_S=300",
	)
	t.Cleanup(func() { _ = pool.Purge(container) })
	waitForHealthy(t, pool, pyURL, "pytransformer", container)

	// requireCorrectServer asserts that the response contains a single
	// successfully transformed event whose external.server field matches
	// the expected mock server name.
	requireCorrectServer := func(t *testing.T, name string, status int, items []types.TransformerResponse) {
		t.Helper()
		require.Equal(t, http.StatusOK, status, "HTTP status for %s", name)
		require.Len(t, items, 1, "expected 1 item for %s", name)
		require.Equal(t, http.StatusOK, items[0].StatusCode, "event status for %s: error=%s", name, items[0].Error)

		external, ok := items[0].Output["external"].(map[string]any)
		require.True(t, ok, "external field should be a map for %s, got %v", name, items[0].Output["external"])
		require.Equal(t, name, external["server"],
			"should reach mock server %s, not another (DNS cache pollution check)", name)
	}

	// resetCallCounts zeroes every mock server's invocation counter so each
	// subtest can assert absolute call counts independently.
	resetCallCounts := func() {
		for _, m := range mocks {
			m.calls.Store(0)
		}
	}

	t.Run("SequentialRequestsNoPollution", func(t *testing.T) {
		resetCallCounts()

		// Send one request per mock server, sequentially
		for _, name := range names {
			events := []types.TransformerEvent{makeEvent("msg-"+name, versionIDs[name])}
			status, items := sendRawTransform(t, pyURL, events)
			requireCorrectServer(t, name, status, items)
		}

		// After one request to each, every mock should have been hit exactly once
		for _, name := range names {
			require.EqualValues(t, 1, mocks[name].calls.Load(),
				"mock server %s should have been called exactly once", name)
		}

		// Now repeat the first two — DNS cache should still resolve correctly
		for _, name := range []string{"alpha", "bravo"} {
			events := []types.TransformerEvent{makeEvent("msg-"+name+"-repeat", versionIDs[name])}
			status, items := sendRawTransform(t, pyURL, events)
			requireCorrectServer(t, name, status, items)
		}

		// alpha/bravo now called twice; charlie/delta still once (no pollution)
		require.EqualValues(t, 2, mocks["alpha"].calls.Load(), "alpha should be called twice")
		require.EqualValues(t, 2, mocks["bravo"].calls.Load(), "bravo should be called twice")
		require.EqualValues(t, 1, mocks["charlie"].calls.Load(), "charlie should not be called again")
		require.EqualValues(t, 1, mocks["delta"].calls.Load(), "delta should not be called again")
	})

	t.Run("ParallelRequestsNoPollution", func(t *testing.T) {
		resetCallCounts()

		// Send 4 requests in parallel, each calling a different mock server.
		// Using wg.Go (instead of t.Parallel() subtests) so we can verify the
		// call counts once all requests have finished.
		type result struct {
			name   string
			status int
			items  []types.TransformerResponse
		}
		results := make([]result, len(names))
		var wg sync.WaitGroup
		for i, name := range names {
			idx, n := i, name
			wg.Go(func() {
				events := []types.TransformerEvent{makeEvent("msg-parallel-"+n, versionIDs[n])}
				status, items := sendRawTransform(t, pyURL, events)
				results[idx] = result{name: n, status: status, items: items}
			})
		}
		wg.Wait()

		for _, r := range results {
			requireCorrectServer(t, r.name, r.status, r.items)
		}

		// Each mock should have been called exactly once — proves parallel
		// requests did not cross-pollute via a shared DNS cache.
		for _, name := range names {
			require.EqualValues(t, 1, mocks[name].calls.Load(),
				"parallel: mock server %s should have been called exactly once", name)
		}
	})

	t.Run("RepeatedRequestsSameEndpoint", func(t *testing.T) {
		resetCallCounts()

		// Same transformation 3 times — DNS cache must remain correct
		for i := range 3 {
			events := []types.TransformerEvent{makeEvent(fmt.Sprintf("msg-repeat-%d", i), versionIDs["alpha"])}
			status, items := sendRawTransform(t, pyURL, events)
			requireCorrectServer(t, "alpha", status, items)
		}

		// alpha called 3 times; others never touched
		require.Equal(t, int64(3), mocks["alpha"].calls.Load(), "alpha should be called 3 times")
		for _, name := range []string{"bravo", "charlie", "delta"} {
			require.Equal(t, int64(0), mocks[name].calls.Load(),
				"mock server %s should not have been called", name)
		}
	})
}

// TestDNSOverrides verifies that the DNS_OVERRIDES environment variable
// correctly overrides DNS resolution for transformation HTTP calls.
//
// A mock HTTP server runs on the host. DNS_OVERRIDES maps a fake hostname
// (custom-api.test) to an IP that reaches the host's loopback from inside
// the pytransformer container. The transformation calls
// http://custom-api.test:PORT/data and should reach the mock server.
func TestDNSOverrides(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 2 * time.Minute

	hostIP := hostReachableIP(t, pool)
	mockSrv, mockCalls := newMockAPIServer(t, "override-target")

	u, err := url.Parse(mockSrv.URL)
	require.NoError(t, err)
	mockPort := u.Port()

	const versionID = "dns-override-v1"
	entries := map[string]configBackendEntry{
		versionID: {
			code: fmt.Sprintf(`
import requests

def transformEvent(event, metadata):
    resp = requests.get("http://custom-api.test:%s/data", timeout=5)
    event['external'] = resp.json()
    return event
`, mockPort),
		},
	}

	configBackend := newContractConfigBackend(t, entries)
	t.Cleanup(configBackend.Close)

	container, pyURL := startRudderPytransformer(
		t, pool, configBackend.URL,
		"DNS_OVERRIDES=custom-api.test,"+hostIP,
	)
	t.Cleanup(func() { _ = pool.Purge(container) })
	waitForHealthy(t, pool, pyURL, "pytransformer", container)

	t.Run("OverrideResolvesToCorrectServer", func(t *testing.T) {
		events := []types.TransformerEvent{makeEvent("msg-override-1", versionID)}
		status, items := sendRawTransform(t, pyURL, events)

		require.Equal(t, http.StatusOK, status)
		require.Len(t, items, 1)
		require.Equal(t, http.StatusOK, items[0].StatusCode, "event error: %s", items[0].Error)

		external, ok := items[0].Output["external"].(map[string]any)
		require.True(t, ok, "external should be a map")
		require.Equal(t, "override-target", external["server"],
			"DNS override should route custom-api.test to the mock server")
		require.GreaterOrEqual(t, mockCalls.Load(), int64(1),
			"mock server should have been called")
	})

	t.Run("OverrideWorksOnRepeatedCalls", func(t *testing.T) {
		callsBefore := mockCalls.Load()
		for i := range 3 {
			events := []types.TransformerEvent{makeEvent(fmt.Sprintf("msg-override-repeat-%d", i), versionID)}
			status, items := sendRawTransform(t, pyURL, events)

			require.Equal(t, http.StatusOK, status)
			require.Len(t, items, 1)
			require.Equal(t, http.StatusOK, items[0].StatusCode, "event error: %s", items[0].Error)

			external := items[0].Output["external"].(map[string]any)
			require.Equal(t, "override-target", external["server"])
		}
		require.GreaterOrEqual(t, mockCalls.Load(), callsBefore+3,
			"mock server should have been called 3 more times")
	})
}

// TestDNSCachingWithOverrides verifies that DNS caching and DNS overrides
// work correctly together — overrides are served from the override map
// (not from the DNS cache), while non-overridden hosts use the DNS cache.
func TestDNSCachingWithOverrides(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 2 * time.Minute

	hostIP := hostReachableIP(t, pool)

	// Mock server A: reached via DNS override (custom-api.test → hostIP)
	mockSrvA, _ := newMockAPIServer(t, "override-server")
	uA, err := url.Parse(mockSrvA.URL)
	require.NoError(t, err)
	portA := uA.Port()

	// Mock server B: reached via normal DNS resolution (localhost / host.docker.internal)
	mockSrvB, _ := newMockAPIServer(t, "cached-server")

	const (
		versionOverride = "dns-combined-override-v1"
		versionCached   = "dns-combined-cached-v1"
	)

	entries := map[string]configBackendEntry{
		versionOverride: {
			code: fmt.Sprintf(`
import requests

def transformEvent(event, metadata):
    resp = requests.get("http://custom-api.test:%s/data", timeout=5)
    event['external'] = resp.json()
    return event
`, portA),
		},
		versionCached: {
			code: fmt.Sprintf(`
import requests

def transformEvent(event, metadata):
    resp = requests.get("%s/data", timeout=5)
    event['external'] = resp.json()
    return event
`, toContainerURL(mockSrvB.URL)),
		},
	}

	configBackend := newContractConfigBackend(t, entries)
	t.Cleanup(configBackend.Close)

	container, pyURL := startRudderPytransformer(
		t, pool, configBackend.URL,
		"DNS_CACHE_ENABLED=true",
		"DNS_CACHE_TTL_S=300",
		"DNS_OVERRIDES=custom-api.test,"+hostIP,
	)
	t.Cleanup(func() { _ = pool.Purge(container) })
	waitForHealthy(t, pool, pyURL, "pytransformer", container)

	// Interleave requests between override and cached paths
	var wg sync.WaitGroup
	type result struct {
		label          string
		expectedServer string
		status         int
		items          []types.TransformerResponse
	}
	results := make([]result, 4)

	wg.Go(func() {
		events := []types.TransformerEvent{makeEvent("msg-combined-override-1", versionOverride)}
		s, items := sendRawTransform(t, pyURL, events)
		results[0] = result{"override-1", "override-server", s, items}
	})
	wg.Go(func() {
		events := []types.TransformerEvent{makeEvent("msg-combined-cached-1", versionCached)}
		s, items := sendRawTransform(t, pyURL, events)
		results[1] = result{"cached-1", "cached-server", s, items}
	})
	wg.Go(func() {
		events := []types.TransformerEvent{makeEvent("msg-combined-override-2", versionOverride)}
		s, items := sendRawTransform(t, pyURL, events)
		results[2] = result{"override-2", "override-server", s, items}
	})
	wg.Go(func() {
		events := []types.TransformerEvent{makeEvent("msg-combined-cached-2", versionCached)}
		s, items := sendRawTransform(t, pyURL, events)
		results[3] = result{"cached-2", "cached-server", s, items}
	})
	wg.Wait()

	for _, r := range results {
		require.Equal(t, http.StatusOK, r.status, "HTTP status for %s", r.label)
		require.Len(t, r.items, 1, "expected 1 item for %s", r.label)
		require.Equal(t, http.StatusOK, r.items[0].StatusCode,
			"event status for %s: error=%s", r.label, r.items[0].Error)

		external, ok := r.items[0].Output["external"].(map[string]any)
		require.True(t, ok, "external should be a map for %s", r.label)
		require.Equal(t, r.expectedServer, external["server"],
			"%s should reach %s", r.label, r.expectedServer)
	}
}

// hostReachableIP returns an IPv4 address that, from inside a Docker
// container, reaches the host machine's loopback — i.e. the address where
// httptest.NewServer has bound its listener.
//
// On Linux (host networking), containers share the host's network namespace
// so 127.0.0.1 is the correct answer.
//
// On macOS (bridge networking), containers run inside a Docker Desktop VM
// and must go through a special gateway to reach the host. We spawn a
// short-lived alpine container with `host.docker.internal:host-gateway` in
// ExtraHosts, resolve that name inside the container, and return the
// resulting IP. Connecting to that IP from any subsequent container is
// routed by Docker Desktop to the Mac host's loopback.
//
// We cannot use the literal string "host.docker.internal" because pytransformer's
// parse_dns_overrides() validates the override target as IPv4 — hostnames
// are rejected. We also cannot hardcode "127.0.0.1" on macOS because Docker
// Desktop's bridge networking makes that the container's own loopback.
func hostReachableIP(t *testing.T, pool *dockertest.Pool) string {
	t.Helper()
	if runtime.GOOS != "darwin" {
		return "127.0.0.1"
	}

	// On Docker Desktop for Mac, host.docker.internal resolves to both an
	// IPv4 and an IPv6 address. We need the IPv4 one because
	// parse_dns_overrides() in pytransformer only accepts IPv4. musl's
	// `getent ahostsv4` forces an IPv4-only lookup and returns lines of the
	// form `<ipv4>  STREAM  host.docker.internal` — the first field is the
	// IPv4 we want.
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "alpine",
		Tag:        "3",
		Entrypoint: []string{"/bin/sh", "-c"},
		Cmd:        []string{"getent ahostsv4 host.docker.internal | awk 'NR==1 {print $1}'"},
		ExtraHosts: []string{"host.docker.internal:host-gateway"},
	})
	require.NoError(t, err, "failed to start alpine helper container")
	t.Cleanup(func() { _ = pool.Purge(resource) })

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	exit, err := pool.Client.WaitContainerWithContext(resource.Container.ID, ctx)
	require.NoError(t, err, "failed to wait for alpine helper container")
	require.Zero(t, exit, "alpine helper container exited non-zero")

	var buf bytes.Buffer
	err = pool.Client.Logs(docker.LogsOptions{
		Container:    resource.Container.ID,
		OutputStream: &buf,
		Stdout:       true,
	})
	require.NoError(t, err, "failed to read alpine helper container logs")

	ip := strings.TrimSpace(buf.String())
	require.NotEmpty(t, ip, "could not resolve host.docker.internal inside helper container")
	parsed := net.ParseIP(ip)
	require.NotNil(t, parsed, "expected a valid IP from alpine helper, got %q", ip)
	require.NotNil(t, parsed.To4(), "expected an IPv4 address (not IPv6), got %q", ip)
	t.Logf("resolved host.docker.internal → %s", ip)
	return ip
}

// newMockAPIServer creates a test HTTP server that responds to all requests
// with a JSON payload identifying the server. Tracks invocation count.
func newMockAPIServer(t *testing.T, name string) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	calls := &atomic.Int64{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_ = jsonrs.NewEncoder(w).Encode(map[string]string{"server": name})
	}))
	t.Cleanup(srv.Close)
	return srv, calls
}

// sendRawTransform sends events directly to pytransformer's /customTransform
// endpoint and returns the HTTP status code and parsed response items.
// Unlike the usertransformer.Client, this allows inspecting raw HTTP status.
func sendRawTransform(
	t *testing.T,
	baseURL string,
	events []types.TransformerEvent,
) (
	int,
	[]types.TransformerResponse,
) {
	t.Helper()
	payload := make([]any, len(events))
	for i, ev := range events {
		payload[i] = map[string]any{
			"message":  ev.Message,
			"metadata": ev.Metadata,
			"destination": map[string]any{
				"Transformations": ev.Destination.Transformations,
			},
		}
	}
	body, err := jsonrs.Marshal(payload)
	require.NoError(t, err)

	req, err := http.NewRequest("POST", baseURL+"/customTransform", bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	resp, err := httputil.DefaultHttpClient().Do(req)
	require.NoError(t, err)
	defer func() { httputil.CloseResponse(resp) }()

	var items []types.TransformerResponse
	require.NoError(t, jsonrs.NewDecoder(resp.Body).Decode(&items))

	return resp.StatusCode, items
}
