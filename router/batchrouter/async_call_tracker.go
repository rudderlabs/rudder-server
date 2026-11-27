package batchrouter

import (
	"context"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-go-kit/stats"
)

const (
	asyncCallUpload = "upload"
	asyncCallPoll   = "poll"
	asyncCallImport = "import"
)

// asyncCallTracker publishes, per destination, the age of the oldest upload, poll and import still in progress.
// Durations are recorded when a call ends, so a hung call shows up only here.
type asyncCallTracker struct {
	destType string
	stats    stats.Stats

	mu     sync.Mutex
	nextID int64
	calls  map[asyncCallKey]map[int64]time.Time // call ID -> start time
	gauges map[asyncCallKey]stats.Measurement   // age of the oldest call in progress
}

type asyncCallKey struct {
	destinationID string
	kind          string
}

func newAsyncCallTracker(destType string, statsFactory stats.Stats) *asyncCallTracker {
	return &asyncCallTracker{
		destType: destType,
		stats:    statsFactory,
		calls:    map[asyncCallKey]map[int64]time.Time{},
		gauges:   map[asyncCallKey]stats.Measurement{},
	}
}

// start tracks a call of the given kind for a destination, that started at the given time, until the returned
// function is called
func (t *asyncCallTracker) start(destinationID, kind string, startedAt time.Time) (done func()) {
	t.mu.Lock()
	defer t.mu.Unlock()
	key := asyncCallKey{destinationID: destinationID, kind: kind}
	if _, ok := t.calls[key]; !ok {
		t.calls[key] = map[int64]time.Time{}
		t.gauges[key] = t.stats.NewTaggedStat("async_worker_oldest_in_progress_seconds", stats.GaugeType, stats.Tags{
			"module":        "batch_router",
			"destType":      t.destType,
			"destinationId": destinationID,
			"kind":          kind,
		})
	}
	t.nextID++
	id := t.nextID
	t.calls[key][id] = startedAt
	return func() {
		t.mu.Lock()
		defer t.mu.Unlock()
		delete(t.calls[key], id)
	}
}

// publish sets each gauge to the age of the oldest call of its destination and kind, or zero. A destination keeps its
// gauges, at zero, after its calls end.
func (t *asyncCallTracker) publish(now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for key, calls := range t.calls {
		var age time.Duration
		for _, startedAt := range calls {
			age = max(age, now.Sub(startedAt))
		}
		t.gauges[key].Gauge(age.Seconds())
	}
}

func (t *asyncCallTracker) run(ctx context.Context, interval time.Duration) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
			t.publish(time.Now())
		}
	}
}
