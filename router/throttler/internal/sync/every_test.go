package sync

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestEvery(t *testing.T) {
	t.Run("NewEvery", func(t *testing.T) {
		interval := 100 * time.Millisecond
		every := NewEvery(interval)

		require.NotNil(t, every, "NewEvery returned nil")
		require.Equal(t, interval, every.interval, "Expected interval %v, got %v", interval, every.interval)
		require.True(t, every.lastRun.IsZero(), "Expected lastRun to be zero time, got %v", every.lastRun)
	})

	t.Run("Do_FirstCall", func(t *testing.T) {
		every := NewEvery(100 * time.Millisecond)
		var called bool

		every.Do(func() {
			called = true
		})

		require.True(t, called, "Function should be called on first invocation")
		require.False(t, every.lastRun.IsZero(), "lastRun should be updated after first call")
	})

	t.Run("Do_WithinInterval", func(t *testing.T) {
		every := NewEvery(100 * time.Millisecond)
		var callCount int

		// First call should execute
		every.Do(func() {
			callCount++
		})

		// Second call immediately after should not execute
		every.Do(func() {
			callCount++
		})

		require.Equal(t, 1, callCount, "Expected function to be called once, got %d calls", callCount)
	})

	t.Run("Do_AfterInterval", func(t *testing.T) {
		every := NewEvery(50 * time.Millisecond)
		var callCount int

		// First call
		every.Do(func() {
			callCount++
		})

		// Wait for interval to pass
		time.Sleep(60 * time.Millisecond)

		// Second call should execute
		every.Do(func() {
			callCount++
		})

		require.Equal(t, 2, callCount, "Expected function to be called twice, got %d calls", callCount)
	})

	t.Run("Do_ConcurrentAccess", func(t *testing.T) {
		every := NewEvery(50 * time.Millisecond)
		var callCount int64
		var wg sync.WaitGroup

		// Launch multiple goroutines that try to call Do simultaneously
		numGoroutines := 10
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				every.Do(func() {
					atomic.AddInt64(&callCount, 1)
				})
			}()
		}

		wg.Wait()

		// Only one call should succeed since they all happen at the same time
		require.Equal(t, int64(1), callCount, "Expected function to be called once due to concurrent access, got %d calls", callCount)
	})

	t.Run("Do_ConcurrentAccessWithDelay", func(t *testing.T) {
		every := NewEvery(30 * time.Millisecond)
		var callCount int64
		var wg sync.WaitGroup

		// First batch of goroutines
		for range 5 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				every.Do(func() {
					atomic.AddInt64(&callCount, 1)
				})
			}()
		}

		// Wait for interval to pass
		time.Sleep(40 * time.Millisecond)

		// Second batch of goroutines
		for range 5 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				every.Do(func() {
					atomic.AddInt64(&callCount, 1)
				})
			}()
		}

		wg.Wait()

		// Should have exactly 2 calls (one from each batch)
		require.Equal(t, int64(2), callCount, "Expected function to be called twice, got %d calls", callCount)
	})

	t.Run("Do_MultipleIntervalsWithTiming", func(t *testing.T) {
		every := NewEvery(25 * time.Millisecond)
		var callCount int
		var callTimes []time.Time

		start := time.Now()

		// Call multiple times with delays
		for i := range 4 {
			every.Do(func() {
				callCount++
				callTimes = append(callTimes, time.Now())
			})

			if i < 3 {
				time.Sleep(30 * time.Millisecond)
			}
		}

		require.Equal(t, 4, callCount, "Expected 4 calls, got %d", callCount)

		// Verify timing between calls
		for i := 1; i < len(callTimes); i++ {
			duration := callTimes[i].Sub(callTimes[i-1])
			require.GreaterOrEqual(t, duration, 25*time.Millisecond, "Calls %d and %d were too close: %v", i-1, i, duration)
		}

		totalDuration := time.Since(start)
		expectedMinDuration := 3 * 25 * time.Millisecond // 3 intervals
		require.GreaterOrEqual(t, totalDuration, expectedMinDuration, "Total duration %v was less than expected minimum %v", totalDuration, expectedMinDuration)
	})

	t.Run("Do_ZeroInterval", func(t *testing.T) {
		every := NewEvery(0)
		var callCount int

		// Multiple calls with zero interval should all execute
		for range 3 {
			every.Do(func() {
				callCount++
			})
		}

		require.Equal(t, 3, callCount, "Expected 3 calls with zero interval, got %d", callCount)
	})

	t.Run("Do_FunctionPanic", func(t *testing.T) {
		every := NewEvery(50 * time.Millisecond)
		var recovered bool

		func() {
			defer func() {
				if r := recover(); r != nil {
					recovered = true
				}
			}()

			every.Do(func() {
				panic("test panic")
			})
		}()

		require.True(t, recovered, "Expected panic to be propagated")

		// Verify that Every is still functional after panic
		var called bool
		time.Sleep(60 * time.Millisecond)
		every.Do(func() {
			called = true
		})

		require.True(t, called, "Every should still be functional after a panic in the function")
	})

	t.Run("Do_LongRunningFunction", func(t *testing.T) {
		every := NewEvery(50 * time.Millisecond)
		var callCount int

		// First call with long-running function
		every.Do(func() {
			callCount++
			time.Sleep(30 * time.Millisecond)
		})

		// Immediate second call should not execute
		every.Do(func() {
			callCount++
		})

		require.Equal(t, 1, callCount, "Expected 1 call, got %d", callCount)

		// Wait for interval and try again
		time.Sleep(30 * time.Millisecond) // Total 60ms since first call
		every.Do(func() {
			callCount++
		})

		require.Equal(t, 2, callCount, "Expected 2 calls after interval, got %d", callCount)
	})
}

func BenchmarkEvery(b *testing.B) {
	b.Run("Do", func(b *testing.B) {
		every := NewEvery(time.Hour) // Long interval so function never executes

		b.ResetTimer()
		for b.Loop() {
			every.Do(func() {
				// This should never execute due to long interval
			})
		}
	})

	b.Run("Do_Executing", func(b *testing.B) {
		every := NewEvery(0) // Zero interval so function always executes

		b.ResetTimer()
		for b.Loop() {
			every.Do(func() {
				// Simple function
			})
		}
	})
}
