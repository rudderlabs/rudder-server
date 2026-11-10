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

// asyncCallTracker publishes, per destination type, the age of the oldest upload, poll and import still in progress.
// Durations are recorded when a call ends, so a hung call shows up only here.
type asyncCallTracker struct {
	mu     sync.Mutex
	nextID int64
	calls  map[string]map[int64]time.Time // kind -> call ID -> start time
	gauges map[string]stats.Measurement   // kind -> age of the oldest call in progress
}

func newAsyncCallTracker(destType string, statsFactory stats.Stats) *asyncCallTracker {
	t := &asyncCallTracker{
		calls:  map[string]map[int64]time.Time{},
		gauges: map[string]stats.Measurement{},
	}
	for _, kind := range []string{asyncCallUpload, asyncCallPoll, asyncCallImport} {
		t.calls[kind] = map[int64]time.Time{}
		t.gauges[kind] = statsFactory.NewTaggedStat("async_worker_oldest_in_progress_seconds", stats.GaugeType, stats.Tags{
			"module":   "batch_router",
			"destType": destType,
			"kind":     kind,
		})
	}
	return t
}

// start tracks a call of the given kind that started at the given time, until the returned function is called
func (t *asyncCallTracker) start(kind string, startedAt time.Time) (done func()) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.nextID++
	id := t.nextID
	t.calls[kind][id] = startedAt
	var once sync.Once
	return func() {
		once.Do(func() {
			t.mu.Lock()
			defer t.mu.Unlock()
			delete(t.calls[kind], id)
		})
	}
}

// publish sets each gauge to the age of the oldest call of its kind, or zero
func (t *asyncCallTracker) publish(now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for kind, calls := range t.calls {
		var age time.Duration
		for _, startedAt := range calls {
			age = max(age, now.Sub(startedAt))
		}
		t.gauges[kind].Gauge(age.Seconds())
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
