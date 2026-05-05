package pytransformer_contract

import (
	"fmt"
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

// TestThreadedExecutionThreadSafety
//
//  1. Boots pytransformer with ENABLE_THREAD_POOL_IO_BOUND=true, IO_THREAD_POOL_SIZE=8, SANDBOX_POOL_MAX_SIZE=4.
//     Multi-event batches hit the threaded transformEvent loop after the first execution detects I/O.
//  2. Stands up a mock HTTP server with a small artificial delay, so each event's transform_fn is forced to overlap
//     with siblings on the same subprocess thread pool.
//     The server echoes a per-event "marker" query parameter back in the response body.
//  3. Fires 50 concurrent /customTransform requests, each carrying 20 events with globally-unique markers
//     (1000 events, 250 saturating the thread pools across 4 subprocesses).
//     Each transformation:
//     - calls log(<marker>) — verified via transformer_log_messages_total
//     - issues a GET passing its marker to the echo server
//     - copies the echoed marker AND the original message_id into the output, plus the source_id from metadata
//  4. Verifies, for every event:
//     - No data leakage: output.echoed_marker == input event marker
//     - Correct metadata pairing: output.message_id == input messageId and output.source_id == input metadata.sourceId
//     - No drop / dup: every input marker appears exactly once
//  5. Verifies metrics consistency:
//     - transformer_executions_total{mode="single_event_threaded"} > 0 — proves the threaded path actually ran
//     - transformer_io_bound_detections_total > 0 — proves first-execution I/O detection fired and persisted the L1 cache flag
//     - transformer_log_messages_total == number of events — every log() call must be observed and attributed (catches both drops and dups)
//
// `go test -race` covers the Go side; the Python side cannot run with a thread sanitiser, so the assertions that we do
// here are the dynamic proof that concurrent threaded execution does not corrupt per-event state.
func TestThreadedExecutionThreadSafety(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 2 * time.Minute

	const (
		versionID        = "threaded-thread-safety-v1"
		threadPoolSize   = 8
		parallelRequests = 50
		eventsPerRequest = 20
		// 50 * 20 = 1000 total events. With SANDBOX_POOL_MAX_SIZE=4 and
		// IO_THREAD_POOL_SIZE=8 the upper bound on simultaneously-running
		// transformEvent threads is 4 * 8 = 32 — easily saturated by the
		// 50 in-flight requests.
		totalEvents = parallelRequests * eventsPerRequest
	)

	// Mock echo server: returns the marker passed in via ?m=… query string.
	// The artificial 10ms delay forces sibling events on the same subprocess to overlap inside the thread pool —
	// without the artificial delay, transformations could finish before the next thread even starts, and
	// the test would not exercise concurrent execution.
	hits := new(atomic.Int64{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		marker := r.URL.Query().Get("m")
		// Sleep a few milliseconds so threaded events on the same subprocess overlap inside the thread pool.
		time.Sleep(10 * time.Millisecond)
		body := fmt.Appendf(nil, `{"marker": %q}`, marker)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	}))
	t.Cleanup(server.Close)

	entries := map[string]configBackendEntry{
		versionID: {code: fmt.Sprintf(`
import requests


def transformEvent(event, metadata):
    marker = event["marker"]
    log(marker)
    resp = requests.get("%s/echo", params={"m": marker}, timeout=5)
    data = resp.json()
    event["echoed_marker"] = data["marker"]
    event["message_id"] = event.get("messageId", "")
    event["source_id"] = metadata(event).get("sourceId", "")
    return event
`, toContainerURL(server.URL))},
	}

	configBackend := newContractConfigBackend(t, entries)
	t.Cleanup(configBackend.Close)

	pyURL, metricsURL := startRudderPytransformerWithMetrics(
		t, pool, configBackend.URL,
		"ENABLE_THREAD_POOL_IO_BOUND=true",
		"IO_THREAD_POOL_SIZE=8",
		"SANDBOX_POOL_MAX_SIZE=4",
		// Match the thread pool: each thread can claim one pooled without falling back to overflow sockets.
		"USER_CONN_POOL_MAX_SIZE=8",
	)

	type result struct {
		idx    int
		status int
		items  []types.TransformerResponse
	}

	// Build the events up-front so we can cross-check inputs against outputs after the parallel run.
	type inputEvent struct {
		messageID string
		marker    string
	}
	inputs := make([]inputEvent, 0, totalEvents)
	requests := make([][]types.TransformerEvent, parallelRequests)

	for i := range parallelRequests {
		batch := make([]types.TransformerEvent, eventsPerRequest)
		for j := range eventsPerRequest {
			messageID := fmt.Sprintf("msg-%d-%d", i, j)
			marker := fmt.Sprintf("marker-%d-%d", i, j)
			ev := makeEvent(messageID, versionID)
			ev.Message["marker"] = marker
			batch[j] = ev
			inputs = append(inputs, inputEvent{messageID: messageID, marker: marker})
		}
		requests[i] = batch
	}

	// Pre-warm the L1 cache: the first execution per subprocess is the
	// synchronous detection pass (no threading), so we send an initial
	// batch of single-event requests to flip `is_io_bound` on every
	// subprocess. SANDBOX_POOL_MAX_SIZE=4 means at most 4 subprocesses;
	// "threadPoolSize" warmup requests give every subprocess at least one shot at the detection pass.
	// Without this, ~4 of our parallel requests would land on a fresh subprocess and run sequentially, biasing the
	// metrics assertions and exercising less of the threaded path.
	for i := range threadPoolSize {
		warmupID := fmt.Sprintf("warmup-%d", i)
		warmupEvent := makeEvent(warmupID, versionID)
		warmupEvent.Message["marker"] = warmupID
		_, items := sendRawTransform(t, pyURL, []types.TransformerEvent{warmupEvent})
		require.Len(t, items, 1, "warmup %d: expected one response item", i)
		require.Equal(t, http.StatusOK, items[0].StatusCode,
			"warmup %d: expected HTTP 200 (error=%s)", i, items[0].Error)
	}

	// Reset the server hit counter so the post-warmup assertion isn't inflated by warmup traffic.
	hits.Store(0)

	var (
		wg      sync.WaitGroup
		results = make([]result, parallelRequests)
	)
	for i := range parallelRequests {
		wg.Go(func() {
			status, items := sendRawTransform(t, pyURL, requests[i])
			results[i] = result{idx: i, status: status, items: items}
		})
	}
	wg.Wait()

	// Build a marker → output index so we can prove every input marker landed in exactly one output slot AND
	// that slot's other fields match the input it came from.
	// Detects:
	//   - dropped events (marker missing from outputs)
	//   - duplicated events (marker appears twice)
	//   - data leakage (output's marker doesn't match its message_id's input)
	type outputRecord struct {
		echoedMarker string
		messageID    string
		sourceID     string
	}
	outputs := make(map[string]outputRecord) // key = output.message_id

	for _, r := range results {
		require.Equalf(t, http.StatusOK, r.status, "request %d: /customTransform must return HTTP 200", r.idx)
		require.Lenf(
			t, r.items, eventsPerRequest, "request %d: expected %d response items, got %d",
			r.idx, eventsPerRequest, len(r.items),
		)

		for _, item := range r.items {
			require.Equalf(
				t, http.StatusOK, item.StatusCode, "request %d: per-event status must be 200 (error=%s)", r.idx, item.Error,
			)
			echoed, ok := item.Output["echoed_marker"].(string)
			require.Truef(
				t, ok, "request %d: output.echoed_marker missing or non-string: %v", r.idx, item.Output["echoed_marker"],
			)
			messageID, ok := item.Output["message_id"].(string)
			require.Truef(t, ok, "request %d: output.message_id missing: %v", r.idx, item.Output["message_id"])
			sourceID, ok := item.Output["source_id"].(string)
			require.Truef(t, ok, "request %d: output.source_id missing: %v", r.idx, item.Output["source_id"])

			// Each message_id must appear exactly once across every response — duplicates would be cross-contamination
			_, dup := outputs[messageID]
			require.Falsef(t, dup,
				"message_id %s appeared in more than one output slot — duplication",
				messageID)
			outputs[messageID] = outputRecord{
				echoedMarker: echoed,
				messageID:    messageID,
				sourceID:     sourceID,
			}
		}
	}

	// Every input must produce exactly one output, with that output's echoed_marker matching the input's marker, and
	// message_id matching the input (proves metadata pairing held under concurrency and state leaked across threads).
	require.Lenf(t, outputs, totalEvents, "expected exactly %d distinct outputs, got %d", totalEvents, len(outputs))

	for _, in := range inputs {
		got, ok := outputs[in.messageID]
		require.Truef(t, ok, "input %s missing from outputs (drop)", in.messageID)
		require.Equalf(t, in.marker, got.echoedMarker,
			"output for %s carries echoed_marker %q, expected %q — per-event data leaked across threads",
			in.messageID, got.echoedMarker, in.marker)
		require.Equalf(t, "src-1", got.sourceID,
			"output for %s has source_id %q, expected src-1 — metadata pairing broke under concurrency",
			in.messageID, got.sourceID)
	}

	// Every input event must have hit the echo server exactly once.
	// A missing call ⇒ silent drop; an extra call ⇒ retry / duplicate dispatch.
	// Both would also surface in the marker map above, but asserting on the server side gives a complementary witness.
	require.EqualValuesf(t, totalEvents, hits.Load(),
		"echo server must have been called exactly %d times (one per event); got %d",
		totalEvents, hits.Load())

	// Metrics: prove the threaded path actually ran.
	// After the warmup, every subsequent multi-event request goes through the threaded loop on its subprocess.
	// The counter is summed across subprocesses so it only needs to be > 0.
	tidLabels := map[string]string{
		"transformation_id": versionID,
		"mode":              "single_event_threaded",
	}
	requireMetricGreater(t, metricsURL, "transformer_executions_total",
		tidLabels, 0, "threaded execution must have fired at least once after warmup")

	// Detection counter: every subprocess that ran during warmup should have flipped is_io_bound.
	// Asserting > 0 (not == 4) because any of the 4 subprocesses might not have picked up a
	// warmup request if the pool's load balancer favoured a subset.
	requireMetricGreater(t, metricsURL, "transformer_io_bound_detections_total",
		map[string]string{"transformation_id": versionID}, 0,
		"first-execution I/O detection must have fired during warmup — "+
			"if this is 0 the L1 cache never flipped is_io_bound and "+
			"every subsequent request would still run sequentially")

	// log(marker) is called exactly once per event. The metric counts
	// EVERY log() call across every subprocess for this transformation.
	// We expect totalEvents (post-warmup) + "threadPoolSize" (warmup) = totalEvents + "threadPoolSize".
	// A drop or duplication in the log path would surface here.
	requireMetricEquals(t, metricsURL, "transformer_log_messages_total",
		map[string]string{"transformation_id": versionID},
		float64(totalEvents+threadPoolSize),
		fmt.Sprintf("expected exactly %d log() calls (%d post-warmup + %d warmup); "+
			"a different value means logs were dropped, duplicated, or "+
			"misattributed under concurrent threaded execution",
			totalEvents+threadPoolSize, totalEvents, threadPoolSize))
}
