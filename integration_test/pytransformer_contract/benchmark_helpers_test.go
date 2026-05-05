//go:build threading_benchmark

package pytransformer_contract

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/httputil"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/registry"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/processor/types"
)

// pytransformerEndpoints holds the two host-reachable URLs for one container.
type pytransformerEndpoints struct {
	apiURL     string
	metricsURL string
}

// startRudderPytransformerWithTag is a copy of startRudderPytransformer that
// accepts an explicit image tag and ALSO binds the Prometheus metrics port
// so benchmarks can scrape it. Returns both URLs.
func startRudderPytransformerWithTag(
	t *testing.T,
	pool *dockertest.Pool,
	tag string,
	configBackendURL string,
	extraEnv ...string,
) pytransformerEndpoints {
	t.Helper()
	const (
		apiContainerPort     = "8080"
		metricsContainerPort = "9091"
	)
	cfg := newContainerConfig(t, apiContainerPort)
	env := []string{
		"CONFIG_BACKEND_URL=" + toContainerURL(configBackendURL),
		"UVICORN_PORT=" + cfg.portStr(apiContainerPort),
	}
	if runtime.GOOS != "darwin" {
		metricsPort, err := kithelper.GetFreePort()
		require.NoError(t, err)
		env = append(env, "METRICS_PORT="+strconv.Itoa(metricsPort))
	}
	for _, e := range extraEnv {
		env = append(env, toContainerURL(e))
	}

	// On macOS we need to also expose the metrics port (default 9091) so the
	// benchmark can scrape it from the host.
	portBindings := cfg.PortBindings
	if runtime.GOOS == "darwin" {
		portBindings = map[docker.Port][]docker.PortBinding{
			docker.Port(apiContainerPort + "/tcp"):     {{HostIP: "127.0.0.1", HostPort: "0"}},
			docker.Port(metricsContainerPort + "/tcp"): {{HostIP: "127.0.0.1", HostPort: "0"}},
		}
	}

	container, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "422074288268.dkr.ecr.us-east-1.amazonaws.com/rudderstack/rudder-pytransformer",
		Tag:          tag,
		Auth:         registry.AuthConfiguration(),
		Env:          env,
		ExtraHosts:   cfg.ExtraHosts,
		PortBindings: portBindings,
	}, cfg.hostConfigFn)
	require.NoErrorf(t, err, "failed to start rudder-pytransformer:%s", tag)

	t.Cleanup(func() {
		if err := pool.Purge(container); err != nil {
			t.Logf("Failed to purge pytransformer:%s container: %v", tag, err)
		}
	})

	apiURL := cfg.url(container, apiContainerPort)
	waitForHealthy(t, pool, apiURL, "rudder-pytransformer:"+tag, container)

	var metricsURL string
	if runtime.GOOS == "darwin" {
		metricsURL = fmt.Sprintf("http://%s:%s",
			container.GetBoundIP(metricsContainerPort+"/tcp"),
			container.GetPort(metricsContainerPort+"/tcp"),
		)
	} else {
		// On Linux (host networking) the container's metrics server binds to
		// the assigned METRICS_PORT directly on the host.
		// (envValue not preserved here — for benchmarks on Linux, leave blank.)
		metricsURL = ""
	}
	return pytransformerEndpoints{apiURL: apiURL, metricsURL: metricsURL}
}

// makeBenchEvents builds N events targeting the given versionID. Each event
// is small; benchmarks measure the per-request overhead, not event size.
func makeBenchEvents(n int, versionID string) []types.TransformerEvent {
	events := make([]types.TransformerEvent, n)
	for i := range n {
		events[i] = types.TransformerEvent{
			Message: types.SingularEventT{
				"messageId": fmt.Sprintf("bench-%d", i),
				"type":      "track",
				"event":     "Bench",
			},
			Metadata: types.Metadata{
				SourceID:         "src-1",
				DestinationID:    "dest-1",
				WorkspaceID:      "ws-1",
				MessageID:        fmt.Sprintf("bench-%d", i),
				TransformationID: versionID,
			},
			Destination: backendconfig.DestinationT{
				Transformations: []backendconfig.TransformationT{
					{VersionID: versionID, ID: "transformation-1", Language: "pythonfaas"},
				},
			},
		}
	}
	return events
}

// buildBenchPayload pre-encodes the request body so the per-request cost in
// the load loop is just network I/O (mirrors what rudder-server sends).
func buildBenchPayload(t *testing.T, events []types.TransformerEvent) []byte {
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
	return body
}

// benchResult captures the outcome of one load run against one image.
type benchResult struct {
	label       string
	duration    time.Duration
	concurrency int
	successes   int64
	failures    int64
	latencies   []time.Duration // per-request latencies for percentile analysis
}

func (r benchResult) rps() float64 {
	return float64(r.successes) / r.duration.Seconds()
}

func (r benchResult) percentile(p float64) time.Duration {
	if len(r.latencies) == 0 {
		return 0
	}
	sorted := make([]time.Duration, len(r.latencies))
	copy(sorted, r.latencies)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	idx := int(float64(len(sorted)) * p / 100.0)
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func (r benchResult) mean() time.Duration {
	if len(r.latencies) == 0 {
		return 0
	}
	var sum time.Duration
	for _, d := range r.latencies {
		sum += d
	}
	return sum / time.Duration(len(r.latencies))
}

// runLoad drives `concurrency` workers against `url` for `duration`
// seconds. Each worker re-uses the same payload bytes and a shared HTTP
// client (with keep-alive) — mirroring how rudder-server hits the
// transformer in production.
func runLoad(
	t *testing.T,
	label, url string,
	payload []byte,
	concurrency int,
	duration time.Duration,
) benchResult {
	t.Helper()
	client := httputil.DefaultHttpClient()
	deadline := time.Now().Add(duration)

	var (
		successes atomic.Int64
		failures  atomic.Int64
		latMu     sync.Mutex
		// Reserve enough capacity so per-request append doesn't reallocate
		// in the hot path (pessimistic estimate of 5 000 RPS × duration).
		latencies = make([]time.Duration, 0, int(duration.Seconds())*5_000*concurrency/16)
	)

	var (
		wg    sync.WaitGroup
		start = time.Now()
	)
	for range concurrency {
		wg.Go(func() {
			localLatencies := make([]time.Duration, 0, 1024)
			for time.Now().Before(deadline) {
				reqStart := time.Now()
				req, err := http.NewRequest(http.MethodPost, url+"/customTransform", bytes.NewReader(payload))
				if err != nil {
					failures.Add(1)
					continue
				}
				req.Header.Set("Content-Type", "application/json")
				resp, err := client.Do(req)
				if err != nil {
					failures.Add(1)
					continue
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
				lat := time.Since(reqStart)
				if resp.StatusCode == http.StatusOK {
					successes.Add(1)
					localLatencies = append(localLatencies, lat)
				} else {
					failures.Add(1)
				}
			}
			latMu.Lock()
			latencies = append(latencies, localLatencies...)
			latMu.Unlock()
		})
	}
	wg.Wait()

	return benchResult{
		label:       label,
		duration:    time.Since(start),
		concurrency: concurrency,
		successes:   successes.Load(),
		failures:    failures.Load(),
		latencies:   latencies,
	}
}

// warmup primes both the parent's L2 source cache and every subprocess's L1
// transformer cache. With SANDBOX_POOL_MAX_SIZE=4 (default), 200 sequential
// requests reliably reach every subprocess and primes their caches.
//
// The first request is decoded and asserted at the per-event level — HTTP
// 200 alone is not enough since a per-event compile / security failure
// still surfaces as 200 with statusCode=400 inside the JSON body. Catching
// that at warmup time keeps the load loop fast (no per-request decode).
func warmup(t *testing.T, label, url string, payload []byte, n int) {
	t.Helper()
	client := httputil.DefaultHttpClient()
	t.Logf("[%s] warmup: %d sequential requests", label, n)
	for i := range n {
		req, err := http.NewRequest(http.MethodPost, url+"/customTransform", bytes.NewReader(payload))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		require.NoError(t, err)
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		_ = resp.Body.Close()
		require.Equalf(t, http.StatusOK, resp.StatusCode,
			"[%s] warmup request %d failed: %s", label, i, string(body))
		if i == 0 {
			var items []types.TransformerResponse
			require.NoErrorf(t, jsonrs.Unmarshal(body, &items),
				"[%s] warmup decode failed: %s", label, string(body))
			require.NotEmptyf(t, items, "[%s] warmup: no items in response", label)
			for _, item := range items {
				require.Equalf(t, 200, item.StatusCode,
					"[%s] warmup: per-event statusCode=%d, error=%q (transformation likely rejected by sandbox)",
					label, item.StatusCode, item.Error)
			}
		}
	}
}

// fmtMs formats a duration as a fixed-precision millisecond string ("22.23ms")
// so output tables align regardless of magnitude.
func fmtMs(d time.Duration) string {
	return fmt.Sprintf("%.2fms", float64(d)/float64(time.Millisecond))
}

// envInt reads an integer env var with a fallback default.
func envInt(t *testing.T, name string, def int) int {
	t.Helper()
	if v := os.Getenv(name); v != "" {
		n, err := strconv.Atoi(v)
		require.NoErrorf(t, err, "invalid integer for %s: %q", name, v)
		return n
	}
	return def
}
