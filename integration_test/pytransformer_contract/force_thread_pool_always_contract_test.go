package pytransformer_contract

import (
	"net/http"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/processor/types"
)

// TestForceThreadPoolAlwaysContract verifies the FORCE_THREAD_POOL_ALWAYS env
// var actually forces the threaded transformEvent path end-to-end:
//
//  1. CPU-only transformation (no HTTP, no time.sleep) — the windowed
//     classifier would never promote it to I/O-bound, so the only way the
//     threaded path fires is via FORCE_THREAD_POOL_ALWAYS.
//  2. ENABLE_THREAD_POOL_IO_BOUND=false — proves the override is
//     independent of the classifier flag.
//  3. Multi-event batch — single-event batches still take the sync path
//     (the executor short-circuits before checking either threading flag).
//
// Asserts on /metrics:
//   - transformer_executions_total{mode="single_event_threaded"} > 0
//     (forced threaded path actually ran)
//   - transformer_executions_total{mode="single_event"} == 0
//     (sequential path never fired for this transformation)
//   - transformer_io_bound_detections_total == 0
//     (classifier never promoted — FORCE_THREAD_POOL_ALWAYS bypasses it)
//   - transformer_thread_pool_size_count > 0
//     (per-batch thread-pool size histogram has samples)
//
// And the dynamic correctness invariants held by the existing threaded test:
// each input event maps 1:1 to an output, no drops, no duplicates, metadata
// pairs correctly across the threadpool boundary.
func TestForceThreadPoolAlwaysContract(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 2 * time.Minute

	const (
		versionID        = "force-thread-cpu-v1"
		threadPoolSize   = 4
		eventsPerRequest = 8
		// 3 multi-event requests is enough to verify the threaded path
		// fires repeatedly without spending real wall-clock on warm-up.
		// We also send one single-event request to verify the sync
		// short-circuit still wins under FORCE_THREAD_POOL_ALWAYS=true.
		multiEventRequests = 3
	)

	// CPU-only transformation: no HTTP, no time.sleep, just dict + arithmetic.
	// The classifier feeds (http_acc / total_acc) — http_acc stays at 0, so
	// the ratio is 0 and the verdict can never flip to I/O-bound. The only
	// way the threaded path can fire here is FORCE_THREAD_POOL_ALWAYS=true.
	entries := map[string]configBackendEntry{
		versionID: {code: `
def transformEvent(event, metadata):
    n = event.get("n", 0)
    acc = 0
    for i in range(1000):
        acc = (acc * 1103515245 + i + n) & 0xFFFFFFFF
    event["acc"] = acc
    event["message_id"] = event.get("messageId", "")
    event["source_id"] = metadata(event).get("sourceId", "")
    return event
`},
	}

	configBackend := newContractConfigBackend(t, entries)
	t.Cleanup(configBackend.Close)

	pyURL, metricsURL := startRudderPytransformerWithMetrics(
		t, pool, configBackend.URL,
		"FORCE_THREAD_POOL_ALWAYS=true",
		// Crucially OFF: prove the force flag works independently of the classifier.
		"ENABLE_THREAD_POOL_IO_BOUND=false",
		"IO_THREAD_POOL_SIZE=4",
		"SANDBOX_POOL_MAX_SIZE=2",
		"USER_CONN_POOL_MAX_SIZE=4",
	)

	// --- 1. Single-event request: must take the SYNC path even under force flag.
	{
		ev := makeEvent("single-evt", versionID)
		ev.Message["n"] = 1
		status, items := sendRawTransform(t, pyURL, []types.TransformerEvent{ev})
		require.Equal(t, http.StatusOK, status, "single-event /customTransform failed")
		require.Len(t, items, 1)
		require.Equalf(t, http.StatusOK, items[0].StatusCode,
			"single-event must succeed (error=%s)", items[0].Error)
	}

	// --- 2. Multi-event requests: must take the THREADED path.
	type inputEvent struct {
		messageID string
		n         int
	}
	inputs := make([]inputEvent, 0, multiEventRequests*eventsPerRequest)
	type outputRecord struct {
		acc       float64
		messageID string
		sourceID  string
	}
	outputs := make(map[string]outputRecord)

	for r := range multiEventRequests {
		batch := make([]types.TransformerEvent, eventsPerRequest)
		for j := range eventsPerRequest {
			messageID := "msg-" + string(rune('a'+r)) + "-" + string(rune('0'+j))
			ev := makeEvent(messageID, versionID)
			n := r*eventsPerRequest + j
			ev.Message["n"] = n
			batch[j] = ev
			inputs = append(inputs, inputEvent{messageID: messageID, n: n})
		}
		status, items := sendRawTransform(t, pyURL, batch)
		require.Equalf(t, http.StatusOK, status, "request %d /customTransform failed", r)
		require.Lenf(t, items, eventsPerRequest, "request %d expected %d items", r, eventsPerRequest)

		for _, item := range items {
			require.Equalf(t, http.StatusOK, item.StatusCode,
				"request %d per-event must be 200 (error=%s)", r, item.Error)
			messageID, ok := item.Output["message_id"].(string)
			require.Truef(t, ok, "request %d output.message_id missing", r)
			sourceID, ok := item.Output["source_id"].(string)
			require.Truef(t, ok, "request %d output.source_id missing", r)
			acc, ok := item.Output["acc"].(float64)
			require.Truef(t, ok, "request %d output.acc missing or non-numeric", r)
			_, dup := outputs[messageID]
			require.Falsef(t, dup,
				"message_id %s appeared more than once — duplicate dispatch under threading",
				messageID)
			outputs[messageID] = outputRecord{
				acc:       acc,
				messageID: messageID,
				sourceID:  sourceID,
			}
		}
	}

	// Every input must have produced exactly one output with matching metadata.
	require.Lenf(t, outputs, multiEventRequests*eventsPerRequest,
		"expected %d distinct outputs, got %d (drops or dupes)",
		multiEventRequests*eventsPerRequest, len(outputs))
	for _, in := range inputs {
		got, ok := outputs[in.messageID]
		require.Truef(t, ok, "input %s missing from outputs (drop)", in.messageID)
		require.Equalf(t, "src-1", got.sourceID,
			"output for %s has source_id %q, expected src-1 — metadata pairing broke",
			in.messageID, got.sourceID)
	}

	// --- 3. Metrics assertions ---------------------------------------------
	// (a) The threaded path actually ran for the multi-event batches.
	tidThreaded := map[string]string{
		"transformation_id": versionID,
		"mode":              "single_event_threaded",
	}
	requireMetricGreater(t, metricsURL, "transformer_executions_total",
		tidThreaded, 0,
		"FORCE_THREAD_POOL_ALWAYS=true must fire the threaded path on a "+
			"CPU-only transformation; if this is 0 the override is not wired up")

	// (b) The single-event request stayed on the sync path; the per-batch
	// short-circuit (`len(events) > 1`) must still win under force flag.
	tidSync := map[string]string{
		"transformation_id": versionID,
		"mode":              "single_event",
	}
	requireMetricEquals(t, metricsURL, "transformer_executions_total",
		tidSync, 1,
		"single-event batch must take the synchronous path even with "+
			"FORCE_THREAD_POOL_ALWAYS=true (no parallelism to exploit)")

	// (c) The classifier never promoted — http_acc stayed 0 because the
	// transformation issues no HTTP. If this fires, FORCE_THREAD_POOL_ALWAYS
	// is not actually independent of the classifier and the override would
	// be redundant.
	requireMetricEquals(t, metricsURL, "transformer_io_bound_detections_total",
		map[string]string{"transformation_id": versionID}, 0,
		"FORCE_THREAD_POOL_ALWAYS=true must NOT need a classifier promotion "+
			"to fire — the http_acc is 0 for a CPU-only transformation, so "+
			"any non-zero promotion count means the path is gated on the "+
			"classifier rather than on the force flag")

	// (d) Per-batch thread-pool size histogram has at least one sample —
	// confirms _run_events_threaded ran (the metric is observed there only).
	requireMetricGreater(t, metricsURL, "transformer_thread_pool_size_count",
		map[string]string{"transformation_id": versionID}, 0,
		"transformer_thread_pool_size must record at least one observation "+
			"per threaded batch — a 0 here means _run_events_threaded was "+
			"never invoked despite the executions_total counter")
}
