package pytransformer_contract

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/processor/types"
)

// TestIOBoundDetectionSurvivesL1CacheFull verifies that an I/O-bound
// transformation is correctly detected and threaded even when the subprocess
// L1 transformer cache is full of other active entries — i.e. when
// `_cache_put` returns `None` for the I/O-bound key.
//
// Setup:
//   - SANDBOX_CACHE_MAX_SIZE=1 so a single CPU-bound transformation occupies
//     the only L1 slot and any subsequent cache_put for a different key
//     returns `None`.
//   - SANDBOX_POOL_MAX_SIZE=1 so every request lands on the same subprocess
//     and shares the same L1 + auxiliary I/O-bound flag cache.
//   - ENABLE_THREAD_POOL_IO_BOUND=true / IO_THREAD_POOL_SIZE=4 so a detected
//     I/O-bound transformation actually takes the threaded path.
//
// Sequence:
//  1. Send a single-event request for `vid_filler` (pure CPU). It claims the
//     only L1 slot — the slot is fresh and active for the rest of the test.
//  2. Send a 4-event request for `vid_io` (issues an outbound HTTP call). The
//     L1 cache is full of an active entry, so this transformation never
//     enters the L1 cache. The detection pass runs synchronously and must
//     still flip the auxiliary flag cache and increment the detection
//     counter — that is the operator-visibility piece of the fix.
//  3. Send the same 4-event request for `vid_io` again. Still no L1 entry
//     (the filler is fresh and active), but the auxiliary flag cache holds
//     the cache_key, so the threaded path runs — that is the detection-
//     persistence piece of the fix.
//
// Assertions:
//   - transformer_io_bound_detections_total{transformation_id=vid_io} > 0:
//     proves the counter ticks even when no CachedTransformer was created.
//   - transformer_executions_total{mode="single_event_threaded",
//     transformation_id=vid_io} > 0: proves the next request takes the
//     threaded path despite the L1 cache still being full.
//
// Without the auxiliary flag cache both metrics would be 0 — the detection
// would be observed and discarded, and the same transformation would re-run
// the synchronous detection pass on every subsequent request.
func TestIOBoundDetectionSurvivesL1CacheFull(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 2 * time.Minute

	const (
		fillerVersionID = "io-flag-cache-filler-v1"
		ioVersionID     = "io-flag-cache-io-v1"
		eventsPerBatch  = 4
	)

	// Mock echo server: counts hits so we can verify the I/O code actually
	// ran on every request (sanity check independent of metrics).
	hits := new(atomic.Int64)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", strconv.Itoa(len(`{"ok":true}`)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(server.Close)

	entries := map[string]configBackendEntry{
		fillerVersionID: {code: `
def transformEvent(event, metadata):
    event["filled"] = True
    return event
`},
		ioVersionID: {code: fmt.Sprintf(`
import requests


def transformEvent(event, metadata):
    requests.get("%s/echo", timeout=5)
    return event
`, toContainerURL(server.URL))},
	}

	configBackend := newContractConfigBackend(t, entries)
	t.Cleanup(configBackend.Close)

	pyURL, metricsURL := startRudderPytransformerWithMetrics(
		t, pool, configBackend.URL,
		"ENABLE_THREAD_POOL_IO_BOUND=true",
		"IO_THREAD_POOL_SIZE=4",
		// Single-request windows so the classifier evaluates after each batch.
		// The default (50) would need 50 requests before any promotion, drowning
		// the test in unrelated traffic.
		"IO_BOUND_RECHECK_INTERVAL=1",
		"SANDBOX_POOL_MAX_SIZE=1",
		// Single L1 slot — once the filler claims it, the I/O-bound key
		// gets `_cache_put -> None` and never enters the L1 cache.
		"SANDBOX_CACHE_MAX_SIZE=1",
		// Match the thread pool so threaded workers reuse pooled
		// connections instead of falling back to overflow sockets.
		"USER_CONN_POOL_MAX_SIZE=4",
	)

	// 1) Prime the only L1 slot with a CPU-bound transformation.
	fillerEvent := makeEvent("filler-1", fillerVersionID)
	status, items := sendRawTransform(t, pyURL, []types.TransformerEvent{fillerEvent})
	require.Equal(t, http.StatusOK, status, "filler request must return 200")
	require.Len(t, items, 1, "filler request must produce one response item")
	require.Equal(t, http.StatusOK, items[0].StatusCode,
		"filler per-event status must be 200 (error=%s)", items[0].Error)

	// 2) Send the I/O-bound transformation. The L1 cache is full, so this
	// request never gets a CachedTransformer. Detection must still tick.
	ioBatch1 := make([]types.TransformerEvent, eventsPerBatch)
	for i := range eventsPerBatch {
		ioBatch1[i] = makeEvent(fmt.Sprintf("io1-%d", i), ioVersionID)
	}
	status, items = sendRawTransform(t, pyURL, ioBatch1)
	require.Equal(t, http.StatusOK, status, "first I/O batch must return 200")
	require.Len(t, items, eventsPerBatch, "first I/O batch must produce one item per event")
	for i, item := range items {
		require.Equal(t, http.StatusOK, item.StatusCode,
			"first I/O batch event %d per-event status must be 200 (error=%s)", i, item.Error)
	}

	// Operator-visibility piece: the counter ticks even though the L1 cache
	// was full and no CachedTransformer was ever created for this key.
	requireMetricGreater(t, metricsURL, "transformer_io_bound_detections_total",
		map[string]string{"transformation_id": ioVersionID}, 0,
		"io_bound_detections must increment on detection even when "+
			"_cache_put returned None — without the auxiliary flag cache "+
			"the detection is silently discarded and operators never see it")

	// 3) Send the same I/O-bound transformation again. Still no L1 entry
	// (filler is fresh and active), but the auxiliary flag cache must
	// persist the detection across requests so the threaded path fires.
	ioBatch2 := make([]types.TransformerEvent, eventsPerBatch)
	for i := range eventsPerBatch {
		ioBatch2[i] = makeEvent(fmt.Sprintf("io2-%d", i), ioVersionID)
	}
	status, items = sendRawTransform(t, pyURL, ioBatch2)
	require.Equal(t, http.StatusOK, status, "second I/O batch must return 200")
	require.Len(t, items, eventsPerBatch, "second I/O batch must produce one item per event")
	for i, item := range items {
		require.Equal(t, http.StatusOK, item.StatusCode,
			"second I/O batch event %d per-event status must be 200 (error=%s)", i, item.Error)
	}

	// Detection-persistence piece: the second request takes the threaded
	// path despite the L1 cache still being full of an active filler entry.
	requireMetricGreater(t, metricsURL, "transformer_executions_total",
		map[string]string{
			"transformation_id": ioVersionID,
			"mode":              "single_event_threaded",
		}, 0,
		"single_event_threaded execution must fire on the second I/O "+
			"request — without the auxiliary flag cache the detection "+
			"would be lost between requests and every batch would re-run "+
			"the synchronous detection pass forever")

	// Sanity check: the I/O code actually ran on every event of both batches.
	require.EqualValues(t, 2*eventsPerBatch, hits.Load(),
		"echo server must have been called %d times (one per I/O event); got %d",
		2*eventsPerBatch, hits.Load())
}

// TestIOBoundClassifierDemotesOnCPUShift verifies that the windowed classifier
// flips back to sequential when the same cache_key shifts from I/O-heavy to
// CPU-heavy work — i.e. “transformer_io_bound_demotions_total“ ticks and
// the threaded path stops firing.
//
// Setup:
//   - ENABLE_THREAD_POOL_IO_BOUND=true / IO_THREAD_POOL_SIZE=4 so the threaded
//     path can fire when the classifier promotes.
//   - IO_BOUND_RECHECK_INTERVAL=1 so each request closes a window — the test
//     can deterministically observe both transitions.
//   - SANDBOX_POOL_MAX_SIZE=1 so every request lands on the same subprocess
//     (and the same auxiliary classifier state).
//
// Sequence (same versionID throughout, gated by a per-event flag):
//  1. I/O-heavy batch — every event hits the echo server. Window 1 closes
//     with “ratio > 0.5“ → promote.
//  2. CPU-heavy batch — every event skips HTTP. Window 2 closes with
//     “ratio ≈ 0“ → demote.
//
// Assertions:
//   - transformer_io_bound_detections_total{transformation_id} > 0:
//     proves window 1 promoted.
//   - transformer_io_bound_demotions_total{transformation_id} > 0:
//     proves window 2 demoted — the new metric that backs the adaptive
//     classifier's reverse direction.
func TestIOBoundClassifierDemotesOnCPUShift(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 2 * time.Minute

	const (
		versionID      = "io-bound-classifier-shape-shift-v1"
		eventsPerBatch = 4
	)

	// Echo server: only hit by I/O-heavy events. Counter is a sanity check
	// that the gating flag actually short-circuited the CPU batch.
	hits := new(atomic.Int64)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", strconv.Itoa(len(`{"ok":true}`)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(server.Close)

	entries := map[string]configBackendEntry{
		versionID: {code: fmt.Sprintf(`
import requests


def transformEvent(event, metadata):
    if event.get("do_http"):
        requests.get("%s/echo", timeout=5)
    return event
`, toContainerURL(server.URL))},
	}

	configBackend := newContractConfigBackend(t, entries)
	t.Cleanup(configBackend.Close)

	pyURL, metricsURL := startRudderPytransformerWithMetrics(
		t, pool, configBackend.URL,
		"ENABLE_THREAD_POOL_IO_BOUND=true",
		"IO_THREAD_POOL_SIZE=4",
		// Single-request windows so we can observe both transitions in two requests.
		"IO_BOUND_RECHECK_INTERVAL=1",
		"SANDBOX_POOL_MAX_SIZE=1",
		"USER_CONN_POOL_MAX_SIZE=4",
	)

	tidLabels := map[string]string{"transformation_id": versionID}

	// 1) I/O-heavy window → promote.
	ioBatch := make([]types.TransformerEvent, eventsPerBatch)
	for i := range eventsPerBatch {
		ev := makeEvent(fmt.Sprintf("io-%d", i), versionID)
		ev.Message["do_http"] = true
		ioBatch[i] = ev
	}
	status, items := sendRawTransform(t, pyURL, ioBatch)
	require.Equal(t, http.StatusOK, status, "I/O batch must return 200")
	require.Len(t, items, eventsPerBatch)
	for i, item := range items {
		require.Equal(t, http.StatusOK, item.StatusCode,
			"I/O event %d per-event status must be 200 (error=%s)", i, item.Error)
	}
	requireMetricGreater(t, metricsURL, "transformer_io_bound_detections_total",
		tidLabels, 0,
		"the I/O-heavy window must have promoted the classifier — "+
			"if this is 0, ratio computation or window evaluation regressed")

	// 2) CPU-heavy window → demote.
	cpuBatch := make([]types.TransformerEvent, eventsPerBatch)
	for i := range eventsPerBatch {
		ev := makeEvent(fmt.Sprintf("cpu-%d", i), versionID)
		ev.Message["do_http"] = false
		cpuBatch[i] = ev
	}
	status, items = sendRawTransform(t, pyURL, cpuBatch)
	require.Equal(t, http.StatusOK, status, "CPU batch must return 200")
	require.Len(t, items, eventsPerBatch)
	for i, item := range items {
		require.Equal(t, http.StatusOK, item.StatusCode,
			"CPU event %d per-event status must be 200 (error=%s)", i, item.Error)
	}
	requireMetricGreater(t, metricsURL, "transformer_io_bound_demotions_total",
		tidLabels, 0,
		"the CPU-heavy window must have demoted the classifier — without "+
			"the True → False transition the threaded path stays armed for "+
			"a workload that no longer benefits from it")

	// Sanity check: only the I/O batch hit the echo server.
	require.EqualValues(t, eventsPerBatch, hits.Load(),
		"echo server must have been called %d times (one per I/O event in batch 1); got %d",
		eventsPerBatch, hits.Load())
}
