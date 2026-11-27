package batchrouter

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
)

func TestAsyncCallTracker(t *testing.T) {
	statsStore, err := memstats.New()
	require.NoError(t, err)
	tracker := newAsyncCallTracker("ELOQUA", statsStore)
	now := time.Now()
	requireAge := func(destinationID, kind string, age time.Duration) {
		t.Helper()
		m := statsStore.Get("async_worker_oldest_in_progress_seconds", stats.Tags{"module": "batch_router", "destType": "ELOQUA", "destinationId": destinationID, "kind": kind})
		require.NotNil(t, m, destinationID+" "+kind)
		require.Equal(t, age.Seconds(), m.LastValue(), destinationID+" "+kind)
	}

	oldestDone := tracker.start("destination-1", asyncCallUpload, now.Add(-time.Hour))
	otherDone := tracker.start("destination-1", asyncCallUpload, now.Add(-time.Minute))
	importDone := tracker.start("destination-1", asyncCallImport, now.Add(-time.Second))
	pollDone := tracker.start("destination-2", asyncCallPoll, now.Add(-time.Minute))
	tracker.publish(now)
	requireAge("destination-1", "upload", time.Hour)
	requireAge("destination-1", "import", time.Second)
	requireAge("destination-2", "poll", time.Minute)
	require.Nil(t, statsStore.Get("async_worker_oldest_in_progress_seconds", stats.Tags{"module": "batch_router", "destType": "ELOQUA", "destinationId": "destination-2", "kind": "upload"}),
		"no gauge for a kind that a destination never started")

	oldestDone()
	tracker.publish(now)
	requireAge("destination-1", "upload", time.Minute)

	otherDone()
	importDone()
	pollDone()
	tracker.publish(now)
	requireAge("destination-1", "upload", 0)
	requireAge("destination-1", "import", 0)
	requireAge("destination-2", "poll", 0)
}
