//go:build ipc_benchmark

// Package pytransformer_contract — TestIPCBenchmark.
//
// One-off benchmark comparing the IPC code-strip optimization (`:main`)
// against the production baseline (`:0.7.3`) on rudder-pytransformer.
//
// Build-tag gated (`ipc_benchmark`) so it never runs as part of the
// regular contract test suite. Run with:
//
//	go test -v -tags=ipc_benchmark -count 1 \
//	    -run TestIPCBenchmark \
//	    ./integration_test/pytransformer_contract/ \
//	    -timeout=15m
//
// Two pytransformer containers are spun up, both pointed at the same mock
// config backend that serves a deliberately large transformation
// (~production p99 payload size). After warming both containers' L1
// caches, the benchmark drives identical concurrent HTTP load against
// each and reports requests-per-second + latency percentiles.

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
	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/registry"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/processor/types"
)

// TestIPCBenchmark is the one-off benchmark. Build-tag gated.
func TestIPCBenchmark(t *testing.T) {
	const (
		optimizedTag       = "main"
		baselineTag        = "0.7.3"
		benchmarkVersionID = "ipc-bench-v1"
	)

	// Workload parameters — tuneable via env.
	codeSizeBytes := envInt(t, "IPC_BENCH_CODE_SIZE_BYTES", 2_600_000) // production avg
	eventsPerRequest := envInt(t, "IPC_BENCH_EVENTS_PER_REQUEST", 1)
	concurrency := envInt(t, "IPC_BENCH_CONCURRENCY", 16)
	durationSeconds := envInt(t, "IPC_BENCH_DURATION_SECONDS", 30)
	warmupRequests := envInt(t, "IPC_BENCH_WARMUP_REQUESTS", 200)

	t.Logf("Benchmark parameters:")
	t.Logf("  code size:         %d bytes (~%d MB)", codeSizeBytes, codeSizeBytes/1_000_000)
	t.Logf("  events/request:    %d", eventsPerRequest)
	t.Logf("  concurrency:       %d", concurrency)
	t.Logf("  duration:          %ds", durationSeconds)
	t.Logf("  warmup:            %d sequential requests", warmupRequests)

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 3 * time.Minute

	// Single shared config backend serves the same large transformation to
	// both images.
	configBackend := newContractConfigBackend(t, map[string]configBackendEntry{
		benchmarkVersionID: {code: makeBenchTransformationCode(codeSizeBytes)},
	})
	t.Cleanup(configBackend.Close)

	t.Logf("Starting baseline pytransformer (%s)...", baselineTag)
	baselineEP := startRudderPytransformerWithTag(t, pool, baselineTag, configBackend.URL)
	t.Logf("Starting optimized pytransformer (%s)...", optimizedTag)
	optimizedEP := startRudderPytransformerWithTag(t, pool, optimizedTag, configBackend.URL)

	events := makeBenchEvents(eventsPerRequest, benchmarkVersionID)
	payload := buildBenchPayload(t, events)
	t.Logf("Per-request payload (events + metadata, no code): %d bytes", len(payload))

	warmup(t, "baseline", baselineEP.apiURL, payload, warmupRequests)
	warmup(t, "optimized", optimizedEP.apiURL, payload, warmupRequests)

	loadDuration := time.Duration(durationSeconds) * time.Second

	// Always run the BASELINE first so the OPTIMIZED run is not affected by
	// any host-level warmth (page cache, scheduler) that the baseline may
	// have created. (If anything this biases against the optimized image,
	// which is the more conservative direction.)
	t.Logf("Running BASELINE load for %s @ %d concurrent workers...", loadDuration, concurrency)
	baselineResult := runLoad(t, "baseline", baselineEP.apiURL, payload, concurrency, loadDuration)
	t.Logf("BASELINE done: %d successes, %d failures, %.1f RPS",
		baselineResult.successes, baselineResult.failures, baselineResult.rps())

	t.Logf("Running OPTIMIZED load for %s @ %d concurrent workers...", loadDuration, concurrency)
	optimizedResult := runLoad(t, "optimized", optimizedEP.apiURL, payload, concurrency, loadDuration)
	t.Logf("OPTIMIZED done: %d successes, %d failures, %.1f RPS",
		optimizedResult.successes, optimizedResult.failures, optimizedResult.rps())

	// Scrape /metrics from both containers to verify the optimization is
	// actually firing (or NOT firing, in which case the headline RPS
	// comparison is misleading).
	logIPCMetrics(t, "baseline", baselineEP.metricsURL)
	logIPCMetrics(t, "optimized", optimizedEP.metricsURL)

	// Headline summary
	t.Logf("=================== IPC BENCHMARK RESULTS ===================")
	t.Logf("Code size: %d bytes (~%.1f MB)   Events/req: %d   Concurrency: %d   Duration: %s",
		codeSizeBytes, float64(codeSizeBytes)/1_000_000, eventsPerRequest, concurrency, loadDuration)
	t.Logf("")
	t.Logf("Image                                | RPS       | Mean lat | p50 lat | p95 lat | p99 lat | Failures")
	t.Logf("-------------------------------------|-----------|----------|---------|---------|---------|---------")
	for _, r := range []benchResult{baselineResult, optimizedResult} {
		t.Logf("%-37s| %9.1f | %8s | %7s | %7s | %7s | %d",
			"rudder-pytransformer:"+resultTag(r.label, baselineTag, optimizedTag),
			r.rps(),
			r.mean().Round(time.Microsecond),
			r.percentile(50).Round(time.Microsecond),
			r.percentile(95).Round(time.Microsecond),
			r.percentile(99).Round(time.Microsecond),
			r.failures,
		)
	}
	if baselineResult.rps() > 0 {
		ratio := optimizedResult.rps() / baselineResult.rps()
		latRatio := float64(optimizedResult.mean()) / float64(baselineResult.mean())
		t.Logf("")
		t.Logf("Throughput speedup (optimized / baseline): %.2fx", ratio)
		t.Logf("Mean-latency ratio (optimized / baseline): %.2fx", latRatio)
	}
	t.Logf("=============================================================")

	// No assertions — this is a pure benchmark, not a regression gate.
	require.Greater(t, baselineResult.successes, int64(0), "baseline run must produce at least one success")
	require.Greater(t, optimizedResult.successes, int64(0), "optimized run must produce at least one success")
}

// pytransformerEndpoints holds the two host-reachable URLs for one container.
type pytransformerEndpoints struct {
	apiURL     string
	metricsURL string
}

// startRudderPytransformerWithTag is a copy of startRudderPytransformer that
// accepts an explicit image tag and ALSO binds the Prometheus metrics port
// so the benchmark can scrape it. Returns both URLs.
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
		// (envValue not preserved here — for the benchmark on Linux, leave blank.)
		metricsURL = ""
	}
	return pytransformerEndpoints{apiURL: apiURL, metricsURL: metricsURL}
}

// makeBenchTransformationCode returns a Python transformation whose source
// code is approximately “sizeBytes“ long. The bulk is a docstring on the
// transformEvent function so the IPC payload size matches production
// (avg ~2.6 MB, p99 ~5 MB) without triggering RestrictedPython's
// security policy (no leading underscore on names, no globals at module
// level beyond the function definition).
func makeBenchTransformationCode(sizeBytes int) string {
	if sizeBytes < 200 {
		sizeBytes = 200
	}
	// Padding goes inside a triple-quoted docstring on the transformEvent
	// function. RestrictedPython compiles the function fine; the docstring
	// is just bytes in the source. Avoid characters that need escaping
	// inside a Python triple-quoted string (no triple-quotes in the blob).
	blob := strings.Repeat("x", sizeBytes-200)
	return fmt.Sprintf(`# benchmark transformation
def transformEvent(event, metadata):
    """Padding-only docstring to inflate source size:
%s
"""
    event["bench"] = "ok"
    return event
`, blob)
}

// makeBenchEvents builds N events targeting the given versionID. Each event
// is small; the benchmark measures the cost of the IPC payload, dominated
// by the transformation code (~MB-scale) — not by the event itself.
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

// runLoad drives “concurrency“ workers against “url“ for “duration“
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

	var wg sync.WaitGroup
	wg.Add(concurrency)
	start := time.Now()
	for range concurrency {
		go func() {
			defer wg.Done()
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
		}()
	}
	wg.Wait()
	actualDuration := time.Since(start)

	return benchResult{
		label:       label,
		duration:    actualDuration,
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

// logIPCMetrics scrapes /metrics from a pytransformer container and logs
// the IPC-related counters / histograms so we can confirm whether the
// optimization is actually firing.
func logIPCMetrics(t *testing.T, label, metricsURL string) {
	t.Helper()
	if metricsURL == "" {
		t.Logf("[%s] no metrics URL available (linux host networking?), skipping scrape", label)
		return
	}
	resp, err := http.Get(metricsURL + "/metrics")
	if err != nil {
		t.Logf("[%s] metrics scrape failed: %v", label, err)
		return
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Logf("[%s] metrics read failed: %v", label, err)
		return
	}
	t.Logf("[%s] /metrics IPC-related lines:", label)
	for _, line := range strings.Split(string(body), "\n") {
		if strings.HasPrefix(line, "#") {
			continue
		}
		switch {
		case strings.HasPrefix(line, "sandbox_ipc_code_sent_total"),
			strings.HasPrefix(line, "transformer_cache_hits_total"),
			strings.HasPrefix(line, "transformer_cache_misses_total"),
			strings.HasPrefix(line, "transformer_cache_size"),
			strings.HasPrefix(line, "transformer_compilations_total"),
			strings.HasPrefix(line, "sandbox_ipc_payload_bytes_count"),
			strings.HasPrefix(line, "sandbox_ipc_payload_bytes_sum"),
			strings.HasPrefix(line, "sandbox_pool_size"),
			strings.HasPrefix(line, "sandbox_pool_idle"),
			strings.HasPrefix(line, "sandbox_process_spawns_total"),
			strings.HasPrefix(line, "sandbox_worker_crash_total"),
			strings.HasPrefix(line, "transform_code_fetch_seconds_count"),
			strings.HasPrefix(line, "transform_code_fetch_seconds_sum"):
			t.Logf("  %s", line)
		}
	}
}

func resultTag(label, baselineTag, optimizedTag string) string {
	if label == "baseline" {
		return baselineTag
	}
	return optimizedTag
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
