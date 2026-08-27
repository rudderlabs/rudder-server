package processor

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
)

// A3.4: pins expire on a fixed schedule measured from the first resolution, and the store stays
// bounded - both by reclaiming expired entries and by refusing to grow past its cap.
func TestCaptureOptInPinsLifecycle(t *testing.T) {
	t.Run("a live pin wins over a later resolution", func(t *testing.T) {
		pins := newCaptureOptInPins()
		require.True(t, pins.pin("run-1", true))
		require.True(t, pins.pin("run-1", false), "pin must report the stored decision, not the candidate")

		value, ok := pins.lookup("run-1")
		require.True(t, ok)
		require.True(t, value)
	})

	t.Run("an expired pin is re-resolved", func(t *testing.T) {
		now := time.Now()
		pins := newCaptureOptInPins()
		pins.now = func() time.Time { return now }

		require.True(t, pins.pin("run-1", true))

		now = now.Add(pins.ttl - time.Second)
		value, ok := pins.lookup("run-1")
		require.True(t, ok, "the pin must survive its whole TTL")
		require.True(t, value)

		now = now.Add(2 * time.Second)
		_, ok = pins.lookup("run-1")
		require.False(t, ok, "the pin must not outlive its TTL")

		require.False(t, pins.pin("run-1", false), "re-resolution must replace the expired pin")
		require.Equal(t, 1, pins.size(), "replacing an expired pin must not grow the store")
	})

	t.Run("reads do not refresh the TTL", func(t *testing.T) {
		now := time.Now()
		pins := newCaptureOptInPins()
		pins.now = func() time.Time { return now }

		require.True(t, pins.pin("run-1", true))
		for i := 0; i < 5; i++ {
			now = now.Add(pins.ttl / 3)
			pins.lookup("run-1")
		}
		_, ok := pins.lookup("run-1")
		require.False(t, ok, "a busy run must not keep its pin alive indefinitely")
	})

	t.Run("expired pins are swept even when nobody looks them up again", func(t *testing.T) {
		now := time.Now()
		pins := newCaptureOptInPins()
		pins.now = func() time.Time { return now }

		for i := 0; i < 1_000; i++ {
			pins.pin(fmt.Sprintf("run-%d", i), true)
		}
		require.Equal(t, 1_000, pins.size())

		now = now.Add(pins.ttl + time.Second)
		pins.pin("run-after-expiry", false)
		require.Equal(t, 1, pins.size(), "the sweep must reclaim every expired pin")
	})

	t.Run("the sweep is throttled", func(t *testing.T) {
		now := time.Now()
		pins := newCaptureOptInPins()
		pins.now = func() time.Time { return now }
		pins.ttl = time.Second
		pins.sweepInterval = time.Hour

		pins.pin("run-1", true)
		now = now.Add(2 * time.Second)
		// the entry is expired but less than a sweep interval has passed, so it is still resident:
		// it costs one map slot, never a wrong decision
		pins.pin("run-2", true)
		require.Equal(t, 2, pins.size())
		_, ok := pins.lookup("run-1")
		require.False(t, ok, "a resident expired pin must still report a miss")

		now = now.Add(pins.sweepInterval)
		pins.pin("run-3", true)
		require.Equal(t, 1, pins.size(), "the next sweep reclaims every expired pin")
	})

	t.Run("the store refuses to grow past its cap", func(t *testing.T) {
		pins := newCaptureOptInPins()
		pins.maxPins = 3

		for i := 0; i < 50; i++ {
			require.True(t, pins.pin(fmt.Sprintf("run-%d", i), true),
				"a run that cannot be pinned must still get its resolved answer")
		}
		require.Equal(t, 3, pins.size(), "an unbounded job run id space must not grow the store")

		// the runs that did get pinned still keep their decision
		value, ok := pins.lookup("run-0")
		require.True(t, ok)
		require.True(t, value)
	})

	t.Run("concurrent resolutions agree on one decision", func(t *testing.T) {
		pins := newCaptureOptInPins()

		const goroutines = 64
		results := make([]bool, goroutines)
		var wg sync.WaitGroup
		start := make(chan struct{})
		for i := 0; i < goroutines; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				if value, ok := pins.lookup("run-1"); ok {
					results[i] = value
					return
				}
				results[i] = pins.pin("run-1", i%2 == 0)
			}(i)
		}
		close(start)
		wg.Wait()

		for i := 1; i < goroutines; i++ {
			require.Equal(t, results[0], results[i], "every record of a run must see the same decision")
		}
	})
}

// captureErrorForRun is the only entry point the marshal site uses; these are the invariants it owes
// its caller regardless of how the pin store behaves underneath.
func TestCaptureErrorForRun(t *testing.T) {
	proc := &Handle{captureOptInPins: newCaptureOptInPins()}
	proc.config.connectionConfigMap = map[connection]backendconfig.Connection{
		{sourceID: captureOptInSourceID, destinationID: captureOptInDestID}: {
			SourceID:      captureOptInSourceID,
			DestinationID: captureOptInDestID,
			Config:        connectionConfig(t, captureOptInEnabledCfg),
		},
	}

	t.Run("an empty job run id is never captured and never pinned", func(t *testing.T) {
		require.False(t, proc.captureErrorForRun(captureOptInSourceID, captureOptInDestID, ""))
		require.Zero(t, proc.captureOptInPins.size())
	})

	t.Run("an rETL run resolves and pins once", func(t *testing.T) {
		require.True(t, proc.captureErrorForRun(captureOptInSourceID, captureOptInDestID, "run-1"))
		require.Equal(t, 1, proc.captureOptInPins.size())

		// the connection disappears entirely: the pinned run is unaffected, a new run fails closed
		proc.config.connectionConfigMap = map[connection]backendconfig.Connection{}
		require.True(t, proc.captureErrorForRun(captureOptInSourceID, captureOptInDestID, "run-1"))
		require.False(t, proc.captureErrorForRun(captureOptInSourceID, captureOptInDestID, "run-2"))
	})
}
