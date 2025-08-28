package delivery

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
)

func TestDeliveryThrottler(t *testing.T) {
	t.Run("NewThrottler", func(t *testing.T) {
		t.Run("with destination id configuration", func(t *testing.T) {
			config := config.New()
			statsStore := stats.NOP
			mockLimiter := &MockLimiter{AllowAfterResult: true}

			destType := "WEBHOOK"
			destinationID := "dest123"
			endpointPath := "endpoint1"

			// Set configuration
			config.Set("Router.throttler.delivery.WEBHOOK.dest123.endpoint1.limit", 100)
			config.Set("Router.throttler.delivery.WEBHOOK.dest123.endpoint1.timeWindow", "10s")

			throttler := NewThrottler(destType, destinationID, endpointPath, mockLimiter, config, statsStore, logger.NOP)

			require.NotNil(t, throttler)
			require.Equal(t, destinationID, throttler.destinationID)
			require.Equal(t, endpointPath, throttler.endpointPath)
			require.Equal(t, "delivery:dest123:endpoint1", throttler.key)
			require.Equal(t, mockLimiter, throttler.limiter)
			require.Equal(t, int64(100), throttler.getLimit())
		})

		t.Run("with destination type configuration", func(t *testing.T) {
			config := config.New()
			statsStore := stats.NOP
			mockLimiter := &MockLimiter{AllowAfterResult: true}

			destType := "WEBHOOK"
			destinationID := "dest123"
			endpointPath := "endpoint1"

			// Set only fallback config (destination type + endpoint)
			config.Set("Router.throttler.delivery.WEBHOOK.endpoint1.limit", 50)
			config.Set("Router.throttler.delivery.WEBHOOK.endpoint1.timeWindow", "5s")

			throttler := NewThrottler(destType, destinationID, endpointPath, mockLimiter, config, statsStore, logger.NOP)

			require.Equal(t, int64(50), throttler.getLimit())
			require.Equal(t, int64(5), throttler.getTimeWindowInSeconds())
		})

		t.Run("stats", func(t *testing.T) {
			config := config.New()
			statsStore := stats.NOP
			mockLimiter := &MockLimiter{AllowAfterResult: true}

			destType := "WEBHOOK"
			destinationID := "dest123"
			endpointPath := "endpoint1"

			throttler := NewThrottler(destType, destinationID, endpointPath, mockLimiter, config, statsStore, logger.NOP)

			require.NotNil(t, throttler.rateLimitGauge)
			require.NotNil(t, throttler.waitTimerSuccess)
			require.NotNil(t, throttler.waitTimerFailure)
		})
	})

	t.Run("Wait", func(t *testing.T) {
		t.Run("returns immediately when allowed", func(t *testing.T) {
			config := config.New()
			statsStore := stats.NOP
			mockLimiter := &MockLimiter{AllowAfterResult: true, RetryAfter: 0}

			destType := "WEBHOOK"
			destinationID := "dest123"
			endpointPath := "endpoint1"

			config.Set("Router.throttler.delivery.WEBHOOK.dest123.endpoint1.limit", 100)
			config.Set("Router.throttler.delivery.WEBHOOK.dest123.endpoint1.timeWindow", "10s")

			throttler := NewThrottler(destType, destinationID, endpointPath, mockLimiter, config, statsStore, logger.NOP)

			start := time.Now()
			duration, err := throttler.Wait(context.Background())

			require.NoError(t, err)
			require.Less(t, duration, 100*time.Millisecond) // Should be very quick
			require.Greater(t, time.Since(start), time.Duration(0))

			require.Len(t, mockLimiter.CallLog, 1)
			require.Equal(t, int64(1), mockLimiter.CallLog[0].Cost)
			require.Equal(t, int64(100), mockLimiter.CallLog[0].Rate)
			require.Equal(t, int64(10), mockLimiter.CallLog[0].Window)
			require.Equal(t, "delivery:dest123:endpoint1", mockLimiter.CallLog[0].Key)
		})

		t.Run("waits when not allowed", func(t *testing.T) {
			config := config.New()
			statsStore := stats.NOP

			// First call not allowed, second call allowed
			mockLimiter := &MockLimiter{
				AllowAfterResults: []bool{false, true},
				RetryAfters:       []time.Duration{50 * time.Millisecond, 0},
			}

			destType := "WEBHOOK"
			destinationID := "dest123"
			endpointPath := "endpoint1"

			config.Set("Router.throttler.delivery.WEBHOOK.dest123.endpoint1.limit", 100)
			config.Set("Router.throttler.delivery.WEBHOOK.dest123.endpoint1.timeWindow", "10s")

			throttler := NewThrottler(destType, destinationID, endpointPath, mockLimiter, config, statsStore, logger.NOP)

			duration, err := throttler.Wait(context.Background())

			require.NoError(t, err)
			require.GreaterOrEqual(t, duration, 50*time.Millisecond) // Should wait at least the retry duration
			require.Less(t, duration, 200*time.Millisecond)          // But not too long

			require.Len(t, mockLimiter.CallLog, 2) // Should have called twice
		})

		t.Run("errors when limiter fails", func(t *testing.T) {
			config := config.New()
			statsStore := stats.NOP
			mockLimiter := &MockLimiter{
				AllowAfterResult: false,
				AllowAfterError:  errors.New("limiter error"),
			}

			destType := "WEBHOOK"
			destinationID := "dest123"
			endpointPath := "endpoint1"

			config.Set("Router.throttler.delivery.WEBHOOK.dest123.endpoint1.limit", 100)
			config.Set("Router.throttler.delivery.WEBHOOK.dest123.endpoint1.timeWindow", "10s")

			throttler := NewThrottler(destType, destinationID, endpointPath, mockLimiter, config, statsStore, logger.NOP)

			duration, err := throttler.Wait(context.Background())

			require.Error(t, err)
			require.Contains(t, err.Error(), "throttling failed for")
			require.Contains(t, err.Error(), "delivery:dest123:endpoint1")
			require.Greater(t, duration, time.Duration(0)) // Should return some duration even on error
		})

		t.Run("returns immediately when throttler not enabled", func(t *testing.T) {
			config := config.New()
			statsStore := stats.NOP
			mockLimiter := &MockLimiter{AllowAfterResult: true}

			destType := "WEBHOOK"
			destinationID := "dest123"
			endpointPath := "endpoint1"

			// Invalid configuration - no limit or window set
			throttler := NewThrottler(destType, destinationID, endpointPath, mockLimiter, config, statsStore, logger.NOP)

			duration, err := throttler.Wait(context.Background())

			require.NoError(t, err)
			require.Equal(t, time.Duration(0), duration)
			require.Empty(t, mockLimiter.CallLog) // Should not call limiter
		})

		t.Run("returns immediately for zero limit", func(t *testing.T) {
			config := config.New()
			statsStore := stats.NOP
			mockLimiter := &MockLimiter{AllowAfterResult: true}

			destType := "WEBHOOK"
			destinationID := "dest123"
			endpointPath := "endpoint1"

			config.Set("Router.throttler.delivery.WEBHOOK.dest123.endpoint1.limit", 0)
			config.Set("Router.throttler.delivery.WEBHOOK.dest123.endpoint1.timeWindow", "10s")

			throttler := NewThrottler(destType, destinationID, endpointPath, mockLimiter, config, statsStore, logger.NOP)

			duration, err := throttler.Wait(context.Background())

			require.NoError(t, err)
			require.Equal(t, time.Duration(0), duration)
			require.Empty(t, mockLimiter.CallLog)
		})

		t.Run("returns immediately for zero window", func(t *testing.T) {
			config := config.New()
			statsStore := stats.NOP
			mockLimiter := &MockLimiter{AllowAfterResult: true}

			destType := "WEBHOOK"
			destinationID := "dest123"
			endpointPath := "endpoint1"

			config.Set("Router.throttler.delivery.WEBHOOK.dest123.endpoint1.limit", 100)
			config.Set("Router.throttler.delivery.WEBHOOK.dest123.endpoint1.timeWindow", "0s")

			throttler := NewThrottler(destType, destinationID, endpointPath, mockLimiter, config, statsStore, logger.NOP)

			duration, err := throttler.Wait(context.Background())

			require.NoError(t, err)
			require.Equal(t, time.Duration(0), duration)
			require.Empty(t, mockLimiter.CallLog)
		})
	})

	t.Run("enabled", func(t *testing.T) {
		t.Run("enabled when limit and window are set", func(t *testing.T) {
			config := config.New()
			statsStore := stats.NOP
			mockLimiter := &MockLimiter{AllowAfterResult: true}

			destType := "WEBHOOK"
			destinationID := "dest123"
			endpointPath := "endpoint1"

			config.Set("Router.throttler.delivery.WEBHOOK.dest123.endpoint1.limit", 100)
			config.Set("Router.throttler.delivery.WEBHOOK.dest123.endpoint1.timeWindow", "10s")

			throttler := NewThrottler(destType, destinationID, endpointPath, mockLimiter, config, statsStore, logger.NOP)

			require.True(t, throttler.enabled())
		})

		t.Run("disabled when limit is zero", func(t *testing.T) {
			config := config.New()
			statsStore := stats.NOP
			mockLimiter := &MockLimiter{AllowAfterResult: true}

			destType := "WEBHOOK"
			destinationID := "dest123"
			endpointPath := "endpoint1"

			config.Set("Router.throttler.delivery.WEBHOOK.dest123.endpoint1.limit", 0)
			config.Set("Router.throttler.delivery.WEBHOOK.dest123.endpoint1.timeWindow", "10s")

			throttler := NewThrottler(destType, destinationID, endpointPath, mockLimiter, config, statsStore, logger.NOP)

			require.False(t, throttler.enabled())
		})

		t.Run("disabled when window is zero", func(t *testing.T) {
			config := config.New()
			statsStore := stats.NOP
			mockLimiter := &MockLimiter{AllowAfterResult: true}

			destType := "WEBHOOK"
			destinationID := "dest123"
			endpointPath := "endpoint1"

			config.Set("Router.throttler.delivery.WEBHOOK.dest123.endpoint1.limit", 100)
			config.Set("Router.throttler.delivery.WEBHOOK.dest123.endpoint1.timeWindow", "0s")

			throttler := NewThrottler(destType, destinationID, endpointPath, mockLimiter, config, statsStore, logger.NOP)

			require.False(t, throttler.enabled())
		})
	})

	t.Run("GetLimit", func(t *testing.T) {
		t.Run("returns configured limit", func(t *testing.T) {
			config := config.New()
			statsStore := stats.NOP
			mockLimiter := &MockLimiter{AllowAfterResult: true}

			destType := "WEBHOOK"
			destinationID := "dest123"
			endpointPath := "endpoint1"

			config.Set("Router.throttler.delivery.WEBHOOK.dest123.endpoint1.limit", 250)

			throttler := NewThrottler(destType, destinationID, endpointPath, mockLimiter, config, statsStore, logger.NOP)

			require.Equal(t, int64(250), throttler.getLimit())
		})

		t.Run("detects config changes", func(t *testing.T) {
			config := config.New()
			statsStore := stats.NOP
			mockLimiter := &MockLimiter{AllowAfterResult: true}

			destType := "WEBHOOK"
			destinationID := "dest123"
			endpointPath := "endpoint1"

			config.Set("Router.throttler.delivery.WEBHOOK.dest123.endpoint1.limit", 100)

			throttler := NewThrottler(destType, destinationID, endpointPath, mockLimiter, config, statsStore, logger.NOP)

			require.Equal(t, int64(100), throttler.getLimit())

			// Update config
			config.Set("Router.throttler.delivery.WEBHOOK.dest123.endpoint1.limit", 200)

			require.Equal(t, int64(200), throttler.getLimit())
		})
	})

	t.Run("getTimeWindowInSeconds", func(t *testing.T) {
		t.Run("returns correct seconds", func(t *testing.T) {
			config := config.New()
			statsStore := stats.NOP
			mockLimiter := &MockLimiter{AllowAfterResult: true}

			destType := "WEBHOOK"
			destinationID := "dest123"
			endpointPath := "endpoint1"

			config.Set("Router.throttler.delivery.WEBHOOK.dest123.endpoint1.timeWindow", "30s")

			throttler := NewThrottler(destType, destinationID, endpointPath, mockLimiter, config, statsStore, logger.NOP)

			require.Equal(t, int64(30), throttler.getTimeWindowInSeconds())
		})
	})

	t.Run("updateGauges", func(t *testing.T) {
		t.Run("updates rate limit gauge", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowAfterResult: true}

			destType := "WEBHOOK"
			destinationID := "dest123"
			endpointPath := "endpoint1"

			config.Set("Router.throttler.delivery.WEBHOOK.dest123.endpoint1.limit", 100)
			config.Set("Router.throttler.delivery.WEBHOOK.dest123.endpoint1.timeWindow", "10s")

			throttler := NewThrottler(destType, destinationID, endpointPath, mockLimiter, config, statsStore, logger.NOP)

			// Trigger gauge update by calling Wait (which calls updateGauges)
			_, err = throttler.Wait(context.Background())
			require.NoError(t, err)

			// Check if gauge was updated (rate = limit/window = 100/10 = 10)
			metrics := statsStore.GetByName("delivery_throttling_rate_limit")
			require.NotEmpty(t, metrics)

			// Find the metric with correct tags
			found := false
			for _, metric := range metrics {
				if metric.Tags["destinationId"] == destinationID &&
					metric.Tags["destType"] == destType &&
					metric.Tags["endpointPath"] == endpointPath {
					require.Equal(t, float64(10), metric.Value) // 100/10 = 10
					found = true
					break
				}
			}
			require.True(t, found, "Should find metric with correct tags")
		})

		t.Run("does not update gauge when disabled", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowAfterResult: true}

			destType := "WEBHOOK"
			destinationID := "dest123"
			endpointPath := "endpoint1"

			config.Set("Router.throttler.delivery.WEBHOOK.dest123.endpoint1.limit", 100)
			config.Set("Router.throttler.delivery.WEBHOOK.dest123.endpoint1.timeWindow", "0s")

			throttler := NewThrottler(destType, destinationID, endpointPath, mockLimiter, config, statsStore, logger.NOP)

			// This should not call limiter due to invalid config, but should still call updateGauges
			_, err = throttler.Wait(context.Background())
			require.NoError(t, err)

			// Check that the gauge exists but has not been updated with a rate calculation
			// The gauge is created with initial value 0 and should remain 0 since window is 0
			metrics := statsStore.GetByName("delivery_throttling_rate_limit")
			require.NotEmpty(t, metrics)

			// Find the metric with correct tags and verify it has value 0 (not updated)
			found := false
			for _, metric := range metrics {
				if metric.Tags["destinationId"] == destinationID &&
					metric.Tags["destType"] == destType &&
					metric.Tags["endpointPath"] == endpointPath {
					require.Equalf(t, float64(0), metric.Value, "Gauge value of %s should be 0 when window is 0 (disabled)", metric.Name) // Should remain 0
					found = true
					break
				}
			}
			require.True(t, found, "Should find metric with correct tags")
		})

		t.Run("updates only once per second", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowAfterResult: true}

			destType := "WEBHOOK"
			destinationID := "dest123"
			endpointPath := "endpoint1"

			config.Set("Router.throttler.delivery.WEBHOOK.dest123.endpoint1.limit", 100)
			config.Set("Router.throttler.delivery.WEBHOOK.dest123.endpoint1.timeWindow", "10s")

			throttler := NewThrottler(destType, destinationID, endpointPath, mockLimiter, config, statsStore, logger.NOP)

			// Call Wait multiple times rapidly
			_, err = throttler.Wait(context.Background())
			require.NoError(t, err)
			_, err = throttler.Wait(context.Background())
			require.NoError(t, err)
			_, err = throttler.Wait(context.Background())
			require.NoError(t, err)

			// Check that gauge was updated only once (due to OnceEvery mechanism)
			metrics := statsStore.GetByName("delivery_throttling_rate_limit")
			require.NotEmpty(t, metrics)

			// Count how many metrics have the expected value
			count := 0
			for _, metric := range metrics {
				if metric.Tags["destinationId"] == destinationID &&
					metric.Tags["destType"] == destType &&
					metric.Tags["endpointPath"] == endpointPath &&
					metric.Value == float64(10) {
					count++
				}
			}
			require.Equal(t, 1, count, "Should only have one metric update")
		})
	})

	t.Run("WaitTimers", func(t *testing.T) {
		t.Run("records success timer", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowAfterResult: true}

			destType := "WEBHOOK"
			destinationID := "dest123"
			endpointPath := "endpoint1"

			config.Set("Router.throttler.delivery.WEBHOOK.dest123.endpoint1.limit", 100)
			config.Set("Router.throttler.delivery.WEBHOOK.dest123.endpoint1.timeWindow", "10s")

			throttler := NewThrottler(destType, destinationID, endpointPath, mockLimiter, config, statsStore, logger.NOP)

			_, err = throttler.Wait(context.Background())
			require.NoError(t, err)

			// Check that success timer was recorded
			metrics := statsStore.GetByName("delivery_throttling_wait_seconds")
			require.NotEmpty(t, metrics)

			// Find the success metric with correct tags
			found := false
			for _, metric := range metrics {
				if metric.Tags["destinationId"] == destinationID &&
					metric.Tags["destType"] == destType &&
					metric.Tags["endpointPath"] == endpointPath &&
					metric.Tags["success"] == "true" {
					require.GreaterOrEqual(t, metric.Value, float64(0)) // Should have non-negative duration
					found = true
					break
				}
			}
			require.True(t, found, "Should find success timer metric with correct tags")
		})

		t.Run("records failure timer", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{
				AllowAfterResult: false,
				AllowAfterError:  errors.New("limiter error"),
			}

			destType := "WEBHOOK"
			destinationID := "dest123"
			endpointPath := "endpoint1"

			config.Set("Router.throttler.delivery.WEBHOOK.dest123.endpoint1.limit", 100)
			config.Set("Router.throttler.delivery.WEBHOOK.dest123.endpoint1.timeWindow", "10s")

			throttler := NewThrottler(destType, destinationID, endpointPath, mockLimiter, config, statsStore, logger.NOP)

			_, err = throttler.Wait(context.Background())
			require.Error(t, err)

			// Check that failure timer was recorded
			metrics := statsStore.GetByName("delivery_throttling_wait_seconds")
			require.NotEmpty(t, metrics)

			// Find the failure metric with correct tags
			found := false
			for _, metric := range metrics {
				if metric.Tags["destinationId"] == destinationID &&
					metric.Tags["destType"] == destType &&
					metric.Tags["endpointPath"] == endpointPath &&
					metric.Tags["success"] == "false" {
					require.GreaterOrEqual(t, metric.Value, float64(0)) // Should have non-negative duration
					found = true
					break
				}
			}
			require.True(t, found, "Should find failure timer metric with correct tags")
		})

		t.Run("records wait time correctly", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)

			// First call not allowed, second call allowed after 50ms
			mockLimiter := &MockLimiter{
				AllowAfterResults: []bool{false, true},
				RetryAfters:       []time.Duration{50 * time.Millisecond, 0},
			}

			destType := "WEBHOOK"
			destinationID := "dest123"
			endpointPath := "endpoint1"

			config.Set("Router.throttler.delivery.WEBHOOK.dest123.endpoint1.limit", 100)
			config.Set("Router.throttler.delivery.WEBHOOK.dest123.endpoint1.timeWindow", "10s")

			throttler := NewThrottler(destType, destinationID, endpointPath, mockLimiter, config, statsStore, logger.NOP)

			start := time.Now()
			_, err = throttler.Wait(context.Background())
			actualDuration := time.Since(start)
			require.NoError(t, err)

			// Verify that we actually waited some time
			require.GreaterOrEqual(t, actualDuration, 50*time.Millisecond)

			// Check that success timer was recorded
			metrics := statsStore.GetByName("delivery_throttling_wait_seconds")
			require.NotEmpty(t, metrics)

			// Find the success metric and verify it exists (exact timing may vary)
			found := false
			for _, metric := range metrics {
				if metric.Tags["destinationId"] == destinationID &&
					metric.Tags["destType"] == destType &&
					metric.Tags["endpointPath"] == endpointPath &&
					metric.Tags["success"] == "true" {
					// Just verify that some timing was recorded, timing precision may vary
					require.GreaterOrEqual(t, metric.Value, float64(0))
					found = true
					break
				}
			}
			require.True(t, found, "Should find success timer metric")
		})

		t.Run("records minimal timers for disabled throttler", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowAfterResult: true}

			destType := "WEBHOOK"
			destinationID := "dest123"
			endpointPath := "endpoint1"

			// No configuration - throttler should be disabled
			throttler := NewThrottler(destType, destinationID, endpointPath, mockLimiter, config, statsStore, logger.NOP)

			_, err = throttler.Wait(context.Background())
			require.NoError(t, err)

			// For disabled throttlers, timers are still recorded but should be very fast (near zero duration)
			// since no actual throttling occurs
			metrics := statsStore.GetByName("delivery_throttling_wait_seconds")

			// Find the success metric for this throttler
			found := false
			for _, metric := range metrics {
				if metric.Tags["destinationId"] == destinationID &&
					metric.Tags["destType"] == destType &&
					metric.Tags["endpointPath"] == endpointPath &&
					metric.Tags["success"] == "true" {
					// Should be very fast since no throttling occurred
					require.Less(t, metric.Value, 0.001) // Less than 1ms
					found = true
					break
				}
			}
			require.True(t, found, "Should find timer metric for disabled throttler with very low duration")
		})
	})

	t.Run("Context Cancellation", func(t *testing.T) {
		t.Run("returns error when context cancelled during sleep", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)

			// First call not allowed with long retry duration, second call allowed
			mockLimiter := &MockLimiter{
				AllowAfterResults: []bool{false, true},
				RetryAfters:       []time.Duration{1 * time.Second, 0}, // Long retry duration
			}

			destType := "WEBHOOK"
			destinationID := "dest123"
			endpointPath := "endpoint1"

			config.Set("Router.throttler.delivery.WEBHOOK.dest123.endpoint1.limit", 100)
			config.Set("Router.throttler.delivery.WEBHOOK.dest123.endpoint1.timeWindow", "10s")

			throttler := NewThrottler(destType, destinationID, endpointPath, mockLimiter, config, statsStore, logger.NOP)

			// Create a context that will be cancelled after 50ms
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()

			duration, err := throttler.Wait(ctx)

			require.Error(t, err)
			require.Contains(t, err.Error(), "throttling interrupted for")
			require.Contains(t, err.Error(), "delivery:dest123:endpoint1")
			require.Less(t, duration, 200*time.Millisecond)          // Should be interrupted quickly
			require.GreaterOrEqual(t, duration, 50*time.Millisecond) // But not too quickly

			// Should have called limiter at least once
			require.GreaterOrEqual(t, len(mockLimiter.CallLog), 1)
		})

		t.Run("returns error when context cancelled before call", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)

			// Set limiter to not allow and require retry
			mockLimiter := &MockLimiter{
				AllowAfterResult: false,
				RetryAfter:       100 * time.Millisecond,
			}

			destType := "WEBHOOK"
			destinationID := "dest123"
			endpointPath := "endpoint1"

			config.Set("Router.throttler.delivery.WEBHOOK.dest123.endpoint1.limit", 100)
			config.Set("Router.throttler.delivery.WEBHOOK.dest123.endpoint1.timeWindow", "10s")

			throttler := NewThrottler(destType, destinationID, endpointPath, mockLimiter, config, statsStore, logger.NOP)

			// Create an already cancelled context
			ctx, cancel := context.WithCancel(context.Background())
			cancel() // Cancel immediately

			duration, err := throttler.Wait(ctx)

			require.Error(t, err)
			require.Contains(t, err.Error(), "throttling interrupted for")
			require.Contains(t, err.Error(), "delivery:dest123:endpoint1")
			require.Less(t, duration, 50*time.Millisecond) // Should fail quickly during sleep

			// Should have called limiter once (before attempting sleep)
			require.Len(t, mockLimiter.CallLog, 1)
		})

		t.Run("records failure timer on context cancellation", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)

			// First call not allowed with long retry duration
			mockLimiter := &MockLimiter{
				AllowAfterResults: []bool{false, true},
				RetryAfters:       []time.Duration{500 * time.Millisecond, 0},
			}

			destType := "WEBHOOK"
			destinationID := "dest123"
			endpointPath := "endpoint1"

			config.Set("Router.throttler.delivery.WEBHOOK.dest123.endpoint1.limit", 100)
			config.Set("Router.throttler.delivery.WEBHOOK.dest123.endpoint1.timeWindow", "10s")

			throttler := NewThrottler(destType, destinationID, endpointPath, mockLimiter, config, statsStore, logger.NOP)

			// Create a context that will be cancelled after 100ms
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			_, err = throttler.Wait(ctx)
			require.Error(t, err)

			// Check that failure timer was recorded
			metrics := statsStore.GetByName("delivery_throttling_wait_seconds")
			require.NotEmpty(t, metrics)

			// Find the failure metric with correct tags
			found := false
			for _, metric := range metrics {
				if metric.Tags["destinationId"] == destinationID &&
					metric.Tags["destType"] == destType &&
					metric.Tags["endpointPath"] == endpointPath &&
					metric.Tags["success"] == "false" {
					// Timer should record some non-negative duration
					require.GreaterOrEqual(t, metric.Value, float64(0))
					found = true
					break
				}
			}
			require.True(t, found, "Should find failure timer metric for context cancellation")
		})

		t.Run("does not return error for disabled throttler with cancelled context", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowAfterResult: true}

			destType := "WEBHOOK"
			destinationID := "dest123"
			endpointPath := "endpoint1"

			// No configuration - throttler should be disabled
			throttler := NewThrottler(destType, destinationID, endpointPath, mockLimiter, config, statsStore, logger.NOP)

			// Create an already cancelled context
			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			duration, err := throttler.Wait(ctx)

			// Should not return error because throttler is disabled (early return)
			require.NoError(t, err)
			require.Equal(t, time.Duration(0), duration)
			require.Empty(t, mockLimiter.CallLog) // Should not call limiter
		})

		t.Run("handles context deadline exceeded", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)

			// Always not allowed to force timeout
			mockLimiter := &MockLimiter{
				AllowAfterResult: false,
				RetryAfter:       200 * time.Millisecond,
			}

			destType := "WEBHOOK"
			destinationID := "dest123"
			endpointPath := "endpoint1"

			config.Set("Router.throttler.delivery.WEBHOOK.dest123.endpoint1.limit", 100)
			config.Set("Router.throttler.delivery.WEBHOOK.dest123.endpoint1.timeWindow", "10s")

			throttler := NewThrottler(destType, destinationID, endpointPath, mockLimiter, config, statsStore, logger.NOP)

			// Create a context with a deadline
			ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(150*time.Millisecond))
			defer cancel()

			duration, err := throttler.Wait(ctx)

			require.Error(t, err)
			require.Contains(t, err.Error(), "throttling interrupted for")

			// Should timeout around the deadline
			require.GreaterOrEqual(t, duration, 150*time.Millisecond)
			require.Less(t, duration, 300*time.Millisecond)

			// Verify the context deadline was exceeded
			require.True(t, errors.Is(err, context.DeadlineExceeded) ||
				errors.Is(ctx.Err(), context.DeadlineExceeded),
				"Error should be related to context deadline")

			// Should have called limiter at least once before timeout
			require.GreaterOrEqual(t, len(mockLimiter.CallLog), 1)
		})

		t.Run("handles multiple retries with context cancellation", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)

			// Multiple retries, each with 30ms delay
			mockLimiter := &MockLimiter{
				AllowAfterResults: []bool{false, false, false, true},
				RetryAfters:       []time.Duration{30 * time.Millisecond, 30 * time.Millisecond, 30 * time.Millisecond, 0},
			}

			destType := "WEBHOOK"
			destinationID := "dest123"
			endpointPath := "endpoint1"

			config.Set("Router.throttler.delivery.WEBHOOK.dest123.endpoint1.limit", 100)
			config.Set("Router.throttler.delivery.WEBHOOK.dest123.endpoint1.timeWindow", "10s")

			throttler := NewThrottler(destType, destinationID, endpointPath, mockLimiter, config, statsStore, logger.NOP)

			// Cancel context after ~70ms (should allow 2 retries but cancel during 3rd)
			ctx, cancel := context.WithTimeout(context.Background(), 70*time.Millisecond)
			defer cancel()

			duration, err := throttler.Wait(ctx)

			require.Error(t, err)
			require.Contains(t, err.Error(), "throttling interrupted for")

			// Should have been interrupted around 70ms
			require.GreaterOrEqual(t, duration, 60*time.Millisecond)
			require.Less(t, duration, 120*time.Millisecond)

			// Should have made at least 2 calls (possibly 3)
			require.GreaterOrEqual(t, len(mockLimiter.CallLog), 2)
			require.LessOrEqual(t, len(mockLimiter.CallLog), 3)
		})
	})
}

// MockLimiter implements delivery.Limiter for testing
type MockLimiter struct {
	AllowAfterResult  bool
	AllowAfterResults []bool // For multiple sequential calls
	AllowAfterError   error
	RetryAfter        time.Duration
	RetryAfters       []time.Duration // For multiple sequential calls
	CallLog           []MockLimiterCall
	callIndex         int
}

type MockLimiterCall struct {
	Cost   int64
	Rate   int64
	Window int64
	Key    string
}

func (m *MockLimiter) AllowAfter(ctx context.Context, cost, rate, window int64, key string) (bool, time.Duration, func(context.Context) error, error) {
	m.CallLog = append(m.CallLog, MockLimiterCall{
		Cost:   cost,
		Rate:   rate,
		Window: window,
		Key:    key,
	})

	// Determine result and retry duration
	var allowed bool
	var retryAfter time.Duration

	if len(m.AllowAfterResults) > 0 {
		if m.callIndex < len(m.AllowAfterResults) {
			allowed = m.AllowAfterResults[m.callIndex]
		} else {
			allowed = m.AllowAfterResults[len(m.AllowAfterResults)-1]
		}
	} else {
		allowed = m.AllowAfterResult
	}

	if len(m.RetryAfters) > 0 {
		if m.callIndex < len(m.RetryAfters) {
			retryAfter = m.RetryAfters[m.callIndex]
		} else {
			retryAfter = m.RetryAfters[len(m.RetryAfters)-1]
		}
	} else {
		retryAfter = m.RetryAfter
	}

	m.callIndex++

	return allowed, retryAfter, func(context.Context) error { return nil }, m.AllowAfterError
}

func (m *MockLimiter) Reset() {
	m.CallLog = nil
	m.callIndex = 0
}
