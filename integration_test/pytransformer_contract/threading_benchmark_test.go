//go:build threading_benchmark

// Package pytransformer_contract — TestThreadingBenchmark.
//
// One-off benchmark comparing the same rudder-pytransformer image (`:main`)
// run twice, once with `ENABLE_THREAD_POOL_IO_BOUND=false` (sequential per-event execution) and once with
// `ENABLE_THREAD_POOL_IO_BOUND=true` (the new threaded `transformEvent` loop).
//
// Build-tag gated (`threading_benchmark`) so it never runs as part of the regular contract test suite.
//
// Run with "make bench-multi-threading"
//
// What the workload exercises:
//
//   - Two pytransformer containers, identical image, distinct env.
//     Same mock config backend serves a transformation that issues ONE outbound HTTP call per event against a
//     deliberately slow mock server (default 50ms artificial latency) — making each event I/O-bound.
//   - The L1 transformer cache is warmed by a sequence of identical warmup requests.
//     The first execution per subprocess is always synchronous (the detection pass); subsequent executions use the
//     threaded path on the treatment container only.
//   - The benchmark drives identical concurrent load against each container and reports requests-per-second +
//     latency percentiles.
//
// The expected signal: with `IO_THREAD_POOL_SIZE=8` and 10 events per request, the sequential run
// takes ~10 * 50ms = ~500ms per request; the threaded run is bounded by the slowest event, ~50ms — i.e. up to 10x
// throughput improvement, capped by the thread pool size and the per-subprocess HTTP connection pool sizing.
//
// Tunable via env (with defaults):
//
//	THREAD_BENCH_EVENTS_PER_REQUEST  — events per group
//	THREAD_BENCH_HTTP_LATENCY_MS     — artificial server latency
//	THREAD_BENCH_THREAD_POOL_SIZE    — IO_THREAD_POOL_SIZE
//	THREAD_BENCH_SANDBOX_POOL_SIZE   — SANDBOX_POOL_MAX_SIZE
//	THREAD_BENCH_USER_CONN_POOL      — USER_CONN_POOL_MAX_SIZE
//	THREAD_BENCH_CONCURRENCY         — concurrent HTTP workers
//	THREAD_BENCH_DURATION_SECONDS    — load-loop duration
//	THREAD_BENCH_WARMUP_REQUESTS     — warmup requests per container

package pytransformer_contract

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
)

// TestThreadingBenchmark is the one-off threading benchmark. Build-tag gated.
//
// We're measuring END-TO-END improvement, not just micro-benchmarks of the
// thread pool dispatch. The transformation is intentionally simple
// (one HTTP call per event) so the runtime is dominated by the artificial
// server latency rather than CPU work — this is the regime where threading
// is supposed to help, and the regime where the L1 cache marks the
// transformation as I/O-bound.
func TestThreadingBenchmark(t *testing.T) {
	const (
		imageTag           = "main"
		benchmarkVersionID = "thread-bench-v1"
	)

	// Workload parameters — tuneable via env (envInt helper from ipc_benchmark_test.go).
	eventsPerRequest := envInt(t, "THREAD_BENCH_EVENTS_PER_REQUEST", 10)
	httpLatencyMs := envInt(t, "THREAD_BENCH_HTTP_LATENCY_MS", 50)
	threadPoolSize := envInt(t, "THREAD_BENCH_THREAD_POOL_SIZE", 8)
	sandboxPoolSize := envInt(t, "THREAD_BENCH_SANDBOX_POOL_SIZE", 4)
	userConnPool := envInt(t, "THREAD_BENCH_USER_CONN_POOL", 8)
	concurrency := envInt(t, "THREAD_BENCH_CONCURRENCY", 16)
	durationSeconds := envInt(t, "THREAD_BENCH_DURATION_SECONDS", 30)
	warmupRequests := envInt(t, "THREAD_BENCH_WARMUP_REQUESTS", 50)

	t.Logf("Benchmark parameters:")
	t.Logf("  events/request:          %d", eventsPerRequest)
	t.Logf("  HTTP server latency:     %d ms (per upstream call)", httpLatencyMs)
	t.Logf("  IO_THREAD_POOL_SIZE:     %d", threadPoolSize)
	t.Logf("  SANDBOX_POOL_MAX_SIZE:   %d", sandboxPoolSize)
	t.Logf("  USER_CONN_POOL_MAX_SIZE: %d", userConnPool)
	t.Logf("  client concurrency:      %d", concurrency)
	t.Logf("  duration:                %ds", durationSeconds)
	t.Logf("  warmup:                  %d sequential requests", warmupRequests)

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 3 * time.Minute

	// Slow upstream: every GET sleeps for `httpLatencyMs` and returns a
	// trivial JSON body. With 10 events per request, sequential execution
	// pays ~10 * latency per request; threaded execution pays roughly one
	// latency (clipped by IO_THREAD_POOL_SIZE).
	hits := &atomic.Int64{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		time.Sleep(time.Duration(httpLatencyMs) * time.Millisecond)
		body := []byte(`{"ok":true}`)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(body)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	}))
	t.Cleanup(server.Close)

	// Single shared config backend serves the same I/O-bound transformation
	// to both containers.
	configBackend := newContractConfigBackend(t, map[string]configBackendEntry{
		benchmarkVersionID: {code: makeIOBoundTransformationCode(toContainerURL(server.URL))},
	})
	t.Cleanup(configBackend.Close)

	// Both containers use the SAME image; the only difference is the env var
	// gating the threaded path. SANDBOX_POOL_MAX_SIZE / USER_CONN_POOL_MAX_SIZE
	// are matched so the only experimental variable is threading.
	commonEnv := []string{
		fmt.Sprintf("SANDBOX_POOL_MAX_SIZE=%d", sandboxPoolSize),
		fmt.Sprintf("USER_CONN_POOL_MAX_SIZE=%d", userConnPool),
		fmt.Sprintf("IO_THREAD_POOL_SIZE=%d", threadPoolSize),
	}

	t.Logf("Starting baseline pytransformer (threading OFF)...")
	baselineEP := startRudderPytransformerWithTag(
		t, pool, imageTag, configBackend.URL,
		append(commonEnv, "ENABLE_THREAD_POOL_IO_BOUND=false")...,
	)
	t.Logf("Starting threaded pytransformer (threading ON)...")
	threadedEP := startRudderPytransformerWithTag(
		t, pool, imageTag, configBackend.URL,
		append(commonEnv, "ENABLE_THREAD_POOL_IO_BOUND=true")...,
	)

	events := makeBenchEvents(eventsPerRequest, benchmarkVersionID)
	payload := buildBenchPayload(t, events)
	t.Logf("Per-request payload: %d bytes", len(payload))

	// Warm both containers' L1 caches IN PARALLEL so total wall-clock for
	// warmup doesn't double. The threaded container especially needs warmup:
	// the first execution per subprocess is always synchronous (detection
	// pass); without warmup, the early load-loop requests would still run
	// sequentially on each subprocess and the first ~SANDBOX_POOL_MAX_SIZE
	// requests would be biased toward the sequential baseline.
	var wg sync.WaitGroup
	wg.Go(func() { warmup(t, "baseline (off)", baselineEP.apiURL, payload, warmupRequests) })
	wg.Go(func() { warmup(t, "threaded (on)", threadedEP.apiURL, payload, warmupRequests) })
	wg.Wait()
	// Reset the upstream hit counter so warmup traffic isn't conflated with
	// the load run's per-event accounting below.
	hits.Store(0)

	loadDuration := time.Duration(durationSeconds) * time.Second

	// Run the BASELINE first so the THREADED run isn't affected by host-level
	// warmth (page cache, scheduler) the baseline created. If anything this
	// biases AGAINST the threaded run, which is the conservative direction
	// for an "is this faster?" check.
	t.Logf("Running BASELINE (threading OFF) load for %s @ %d concurrent workers...",
		loadDuration, concurrency)
	baselineResult := runLoad(t, "baseline", baselineEP.apiURL, payload, concurrency, loadDuration)
	t.Logf("BASELINE done: %d successes, %d failures, %.1f RPS",
		baselineResult.successes, baselineResult.failures, baselineResult.rps())

	t.Logf("Running THREADED (threading ON) load for %s @ %d concurrent workers...",
		loadDuration, concurrency)
	threadedResult := runLoad(t, "threaded", threadedEP.apiURL, payload, concurrency, loadDuration)
	t.Logf("THREADED done: %d successes, %d failures, %.1f RPS",
		threadedResult.successes, threadedResult.failures, threadedResult.rps())

	// Scrape /metrics from both containers so we can confirm:
	//   - the threaded container actually took the threaded path
	//     (transformer_executions_total{mode="single_event_threaded"} > 0
	//     AND transformer_io_bound_detections_total > 0)
	//   - the baseline container did NOT (mode="single_event_threaded" == 0).
	// If those don't hold, the headline RPS comparison is misleading.
	logThreadingMetrics(t, "baseline (off)", baselineEP.metricsURL)
	logThreadingMetrics(t, "threaded (on)", threadedEP.metricsURL)

	// Headline summary
	t.Logf("================ THREADING BENCHMARK RESULTS ================")
	t.Logf("Events/req: %d   HTTP latency: %dms   Threads: %d   Sandbox pool: %d   Conn pool: %d",
		eventsPerRequest, httpLatencyMs, threadPoolSize, sandboxPoolSize, userConnPool)
	t.Logf("Client concurrency: %d   Duration: %s", concurrency, loadDuration)
	t.Logf("")
	t.Logf("%-37s | %8s | %9s | %9s | %9s | %9s | %s",
		"Run", "RPS", "Mean lat", "p50 lat", "p95 lat", "p99 lat", "Failures")
	t.Logf("%s-+-%s-+-%s-+-%s-+-%s-+-%s-+-%s",
		strings.Repeat("-", 37), strings.Repeat("-", 8),
		strings.Repeat("-", 9), strings.Repeat("-", 9),
		strings.Repeat("-", 9), strings.Repeat("-", 9),
		strings.Repeat("-", 8))
	for _, r := range []benchResult{baselineResult, threadedResult} {
		t.Logf("%-37s | %8.1f | %9s | %9s | %9s | %9s | %d",
			"ENABLE_THREAD_POOL_IO_BOUND="+threadingResultTag(r.label),
			r.rps(),
			fmtMs(r.mean()),
			fmtMs(r.percentile(50)),
			fmtMs(r.percentile(95)),
			fmtMs(r.percentile(99)),
			r.failures,
		)
	}
	if baselineResult.rps() > 0 {
		ratio := threadedResult.rps() / baselineResult.rps()
		latRatio := float64(threadedResult.mean()) / float64(baselineResult.mean())
		t.Logf("")
		t.Logf("Throughput speedup (threaded / baseline): %.2fx", ratio)
		t.Logf("Mean-latency ratio (threaded / baseline): %.2fx", latRatio)
		// Theoretical upper bound: min(events_per_request, IO_THREAD_POOL_SIZE).
		// Useful for "did we leave a lot on the table?" reasoning, e.g.
		// 2x speedup with a 10x ceiling suggests the bottleneck is
		// elsewhere (USER_CONN_POOL_MAX_SIZE? upstream throttling?).
		ceiling := eventsPerRequest
		if threadPoolSize < ceiling {
			ceiling = threadPoolSize
		}
		t.Logf("Theoretical max speedup (events/req capped by thread pool): %dx", ceiling)
	}
	t.Logf("=============================================================")

	// No regression gates — pure benchmark. Just sanity-check both runs
	// produced traffic; otherwise the comparison is meaningless.
	require.Greater(t, baselineResult.successes, int64(0),
		"baseline (threading OFF) run must produce at least one success")
	require.Greater(t, threadedResult.successes, int64(0),
		"threaded (threading ON) run must produce at least one success")
	require.Greater(t, hits.Load(), int64(0),
		"upstream HTTP server should have received traffic from both containers")
}

// makeIOBoundTransformationCode returns a Python transformation that issues
// one HTTP GET per event against the given URL. The wrapper marks the
// transformation as I/O-bound on first execution; subsequent executions
// take the threaded path when `ENABLE_THREAD_POOL_IO_BOUND=true`.
func makeIOBoundTransformationCode(serverURL string) string {
	return fmt.Sprintf(`# threading benchmark transformation
import requests


def transformEvent(event, metadata):
    # One HTTP GET per event — the wrapper tags the transformation
    # I/O-bound on first execution. Threading offers parallel speedup
    # because each event spends nearly all its time waiting on the
    # upstream rather than on CPU.
    resp = requests.get("%s/echo", timeout=5)
    event["status"] = resp.status_code
    return event
`, serverURL)
}

// threadingResultTag maps the internal label used by `runLoad` to the
// flag value the run exercised — keeps the result table self-documenting.
func threadingResultTag(label string) string {
	if label == "baseline" {
		return "false"
	}
	return "true"
}

// logThreadingMetrics scrapes /metrics from one container and logs the
// threading-related lines so the operator can verify the experimental
// variable was actually exercised. Mirrors `logIPCMetrics` in shape.
func logThreadingMetrics(t *testing.T, label, metricsURL string) {
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
	t.Logf("[%s] /metrics threading-related lines:", label)
	for _, line := range strings.Split(string(body), "\n") {
		if strings.HasPrefix(line, "#") {
			continue
		}
		switch {
		case strings.HasPrefix(line, "transformer_executions_total"),
			strings.HasPrefix(line, "transformer_io_bound_detections_total"),
			strings.HasPrefix(line, "transformer_thread_pool_size_count"),
			strings.HasPrefix(line, "transformer_thread_pool_size_sum"),
			strings.HasPrefix(line, "transformer_threaded_batch_duration_seconds_count"),
			strings.HasPrefix(line, "transformer_threaded_batch_duration_seconds_sum"),
			strings.HasPrefix(line, "transformer_http_requests_total"),
			strings.HasPrefix(line, "transformer_execution_duration_seconds_count"),
			strings.HasPrefix(line, "transformer_execution_duration_seconds_sum"),
			strings.HasPrefix(line, "sandbox_pool_size"),
			strings.HasPrefix(line, "sandbox_pool_idle"),
			strings.HasPrefix(line, "sandbox_process_spawns_total"):
			t.Logf("  %s", line)
		}
	}
}
