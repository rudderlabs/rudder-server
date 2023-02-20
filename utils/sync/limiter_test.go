package sync_test

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/rudderlabs/rudder-server/services/stats/memstats"
	miscsync "github.com/rudderlabs/rudder-server/utils/sync"
	"github.com/stretchr/testify/require"
)

func TestLimiter(t *testing.T) {
	t.Run("without priority", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		var wg sync.WaitGroup
		ms := memstats.New()

		statsTriggerCh := make(chan time.Time)
		triggerFn := func() <-chan time.Time {
			return statsTriggerCh
		}

		limiter := miscsync.NewLimiter(ctx, &wg, "test", 1, ms, miscsync.WithLimiterStatsTriggerFunc(triggerFn))
		var counter int
		statsTriggerCh <- time.Now()
		statsTriggerCh <- time.Now()

		require.NotNil(t, ms.Get("test_limiter_active_routines", nil))
		require.EqualValues(t, 0, ms.Get("test_limiter_active_routines", nil).LastValue(), "shouldn't have any active")

		require.NotNil(t, ms.Get("test_limiter_availability", nil))
		require.EqualValues(t, 1, ms.Get("test_limiter_availability", nil).LastValue(), "should be available")

		require.Nil(t, ms.Get("test_limiter_working", nil))

		for i := 0; i < 100; i++ {
			wg.Add(1)
			key := strconv.Itoa(i)
			go func() {
				limiter.Do(key, func() {
					counter++ // since the limiter's limit is 1, we shouldn't need an atomic counter
					wg.Done()
				})
			}()
		}

		cancel()
		wg.Wait()

		require.EqualValues(t, 100, counter, "counter should be 100")

		select {
		case statsTriggerCh <- time.Now():
			require.Fail(t, "shouldn't be listening to triggerCh anymore")
		default:
		}
		for i := 0; i < 100; i++ {
			m := ms.Get("test_limiter_working", map[string]string{"key": strconv.Itoa(i)})
			require.NotNil(t, m)
			require.Lenf(t, m.Durations(), 1, "should have recorded 1 timer duration for key %d", i)
		}
	})

	t.Run("with priority", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		var wg sync.WaitGroup
		ms := memstats.New()

		limiter := miscsync.NewLimiter(ctx, &wg, "test", 1, ms)
		var counterLow int
		var counterHigh int
		sleepTime := 100 * time.Microsecond
		for i := 0; i < 1000; i++ {
			wg.Add(1)
			key := strconv.Itoa(i)
			go func() {
				limiter.DoWithPriority(key, miscsync.LimiterPriorityValueHigh, func() {
					time.Sleep(sleepTime)
					counterHigh++ // since the limiter's limit is 1, we shouldn't need an atomic counter
					require.Equal(t, 0, counterLow, "counterLow should be 0")
					wg.Done()
				})
			}()
		}

		time.Sleep(10 * sleepTime)
		for i := 0; i < 1000; i++ {
			wg.Add(1)
			key := strconv.Itoa(i)
			go func() {
				limiter.DoWithPriority(key, miscsync.LimiterPriorityValueLow, func() {
					counterLow++ // since the limiter's limit is 1, we shouldn't need an atomic counter
					wg.Done()
				})
			}()
		}

		cancel()
		wg.Wait()

		require.EqualValues(t, 1000, counterHigh, "high priority counter should be 1000")
		require.EqualValues(t, 1000, counterLow, "low priority counter should be 1000")
	})

	t.Run("with dynamic priority", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		var wg sync.WaitGroup
		ms := memstats.New()

		sleepTime := 1 * time.Millisecond
		limiter := miscsync.NewLimiter(ctx, &wg, "test", 1, ms, miscsync.WithLimiterDynamicPeriod(sleepTime/100))
		var counterLow int
		var counterHigh int

		var dynamicPriorityVerified bool
		for i := 0; i < 1000; i++ {
			wg.Add(1)
			key := strconv.Itoa(i)
			go func() {
				limiter.DoWithPriority(key, miscsync.LimiterPriorityValueHigh, func() {
					time.Sleep(sleepTime)
					counterHigh++ // since the limiter's limit is 1, we shouldn't need an atomic counter
					if counterLow > 0 {
						dynamicPriorityVerified = true
					}
					wg.Done()
				})
			}()
		}

		for i := 0; i < 10; i++ {
			wg.Add(1)
			key := strconv.Itoa(i)
			go func() {
				limiter.DoWithPriority(key, miscsync.LimiterPriorityValueLow, func() {
					counterLow++ // since the limiter's limit is 1, we shouldn't need an atomic counter
					wg.Done()
				})
			}()
		}

		cancel()
		wg.Wait()

		require.True(t, dynamicPriorityVerified, "dynamic priority should have been verified")
		require.EqualValues(t, 1000, counterHigh, "high priority counter should be 1000")
		require.EqualValues(t, 10, counterLow, "low priority counter should be 10")
	})
}
