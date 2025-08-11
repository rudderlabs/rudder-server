package static

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
)

func TestPerEventTypeThrottler(t *testing.T) {
	t.Run("NewPerEventTypeThrottler", func(t *testing.T) {
		t.Run("CreatesThrottlerWithCorrectConfiguration", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: true}

			destType := "WEBHOOK"
			destinationID := "dest123"
			eventType := "track"

			// Set configuration
			config.Set("Router.throttler.WEBHOOK.dest123.track.limit", 100)
			config.Set("Router.throttler.WEBHOOK.dest123.track.timeWindow", "10s")

			throttler := NewPerEventTypeThrottler(destType, destinationID, eventType, mockLimiter, config, statsStore, logger.NOP)

			require.NotNil(t, throttler)
			require.Equal(t, destinationID, throttler.destinationID)
			require.Equal(t, eventType, throttler.eventType)
			require.Equal(t, mockLimiter, throttler.limiter)
			require.Equal(t, int64(100), throttler.GetLimit())
		})

		t.Run("UsesConfigurationFallbacks", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: true}

			destType := "WEBHOOK"
			destinationID := "dest123"
			eventType := "track"

			// Set destination type + event config (fallback)
			config.Set("Router.throttler.WEBHOOK.track.limit", 75)
			config.Set("Router.throttler.WEBHOOK.track.timeWindow", "8s")

			throttler := NewPerEventTypeThrottler(destType, destinationID, eventType, mockLimiter, config, statsStore, logger.NOP)

			require.Equal(t, int64(75), throttler.GetLimit())
		})

		t.Run("CreatesStatsGaugeWithCorrectTags", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: true}

			destType := "WEBHOOK"
			destinationID := "dest123"
			eventType := "track"

			throttler := NewPerEventTypeThrottler(destType, destinationID, eventType, mockLimiter, config, statsStore, logger.NOP)

			require.NotNil(t, throttler.rateLimitGauge)
		})
	})

	t.Run("CheckLimitReached", func(t *testing.T) {
		t.Run("ReturnsNotLimitedWhenAllowed", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: true}

			destType := "WEBHOOK"
			destinationID := "dest123"
			eventType := "track"

			config.Set("Router.throttler.WEBHOOK.dest123.track.limit", 100)
			config.Set("Router.throttler.WEBHOOK.dest123.track.timeWindow", "10s")

			throttler := NewPerEventTypeThrottler(destType, destinationID, eventType, mockLimiter, config, statsStore, logger.NOP)

			limited, err := throttler.CheckLimitReached(context.Background(), 5) // cost should be ignored

			require.NoError(t, err)
			require.False(t, limited)
			require.Len(t, mockLimiter.CallLog, 1)
			require.Equal(t, int64(1), mockLimiter.CallLog[0].Cost) // Should always be 1
			require.Equal(t, int64(100), mockLimiter.CallLog[0].Rate)
			require.Equal(t, int64(10), mockLimiter.CallLog[0].Window)
			require.Equal(t, destinationID+":"+eventType, mockLimiter.CallLog[0].Key)
		})

		t.Run("ReturnsLimitedWhenNotAllowed", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: false}

			destType := "WEBHOOK"
			destinationID := "dest123"
			eventType := "track"

			config.Set("Router.throttler.WEBHOOK.dest123.track.limit", 100)
			config.Set("Router.throttler.WEBHOOK.dest123.track.timeWindow", "10s")

			throttler := NewPerEventTypeThrottler(destType, destinationID, eventType, mockLimiter, config, statsStore, logger.NOP)

			limited, err := throttler.CheckLimitReached(context.Background(), 5)

			require.NoError(t, err)
			require.True(t, limited)
		})

		t.Run("ReturnsErrorWhenLimiterFails", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{
				AllowResult: false,
				AllowError:  errors.New("limiter error"),
			}

			destType := "WEBHOOK"
			destinationID := "dest123"
			eventType := "track"

			config.Set("Router.throttler.WEBHOOK.dest123.track.limit", 100)
			config.Set("Router.throttler.WEBHOOK.dest123.track.timeWindow", "10s")

			throttler := NewPerEventTypeThrottler(destType, destinationID, eventType, mockLimiter, config, statsStore, logger.NOP)

			limited, err := throttler.CheckLimitReached(context.Background(), 5)

			require.Error(t, err)
			require.Contains(t, err.Error(), "throttling failed for")
			require.False(t, limited)
		})

		t.Run("ReturnsNotLimitedForInvalidConfiguration", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: true}
			mockLogger := &MockLogger{}

			destType := "WEBHOOK"
			destinationID := "dest123"
			eventType := "track"

			// Invalid configuration - no limit or window set
			throttler := NewPerEventTypeThrottler(destType, destinationID, eventType, mockLimiter, config, statsStore, mockLogger)

			limited, err := throttler.CheckLimitReached(context.Background(), 5)

			require.NoError(t, err)
			require.False(t, limited)
			require.Empty(t, mockLimiter.CallLog) // Should not call limiter

		})

		t.Run("ReturnsNotLimitedForZeroLimit", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: true}
			mockLogger := &MockLogger{}

			destType := "WEBHOOK"
			destinationID := "dest123"
			eventType := "track"

			config.Set("Router.throttler.WEBHOOK.dest123.track.limit", 0)
			config.Set("Router.throttler.WEBHOOK.dest123.track.timeWindow", "10s")

			throttler := NewPerEventTypeThrottler(destType, destinationID, eventType, mockLimiter, config, statsStore, mockLogger)

			limited, err := throttler.CheckLimitReached(context.Background(), 5)

			require.NoError(t, err)
			require.False(t, limited)
			require.Empty(t, mockLimiter.CallLog)

		})

		t.Run("ReturnsNotLimitedForZeroWindow", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: true}
			mockLogger := &MockLogger{}

			destType := "WEBHOOK"
			destinationID := "dest123"
			eventType := "track"

			config.Set("Router.throttler.WEBHOOK.dest123.track.limit", 100)
			config.Set("Router.throttler.WEBHOOK.dest123.track.timeWindow", "0s")

			throttler := NewPerEventTypeThrottler(destType, destinationID, eventType, mockLimiter, config, statsStore, mockLogger)

			limited, err := throttler.CheckLimitReached(context.Background(), 5)

			require.NoError(t, err)
			require.False(t, limited)
			require.Empty(t, mockLimiter.CallLog)

		})

		t.Run("IgnoresCostParameterAndUsesConstantCost", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: true}

			destType := "WEBHOOK"
			destinationID := "dest123"
			eventType := "track"

			config.Set("Router.throttler.WEBHOOK.dest123.track.limit", 100)
			config.Set("Router.throttler.WEBHOOK.dest123.track.timeWindow", "10s")

			throttler := NewPerEventTypeThrottler(destType, destinationID, eventType, mockLimiter, config, statsStore, logger.NOP)

			// Try with different cost values - should always use cost = 1
			_, err = throttler.CheckLimitReached(context.Background(), 999)
			require.NoError(t, err)

			require.Len(t, mockLimiter.CallLog, 1)
			require.Equal(t, int64(1), mockLimiter.CallLog[0].Cost) // Should always be 1
		})
	})

	t.Run("enabled", func(t *testing.T) {
		t.Run("ReturnsTrueForValidConfig", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: true}

			destType := "WEBHOOK"
			destinationID := "dest123"
			eventType := "track"

			config.Set("Router.throttler.WEBHOOK.dest123.track.limit", 100)
			config.Set("Router.throttler.WEBHOOK.dest123.track.timeWindow", "10s")

			throttler := NewPerEventTypeThrottler(destType, destinationID, eventType, mockLimiter, config, statsStore, logger.NOP)

			require.True(t, throttler.enabled())
		})

		t.Run("ReturnsFalseForZeroLimit", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: true}

			destType := "WEBHOOK"
			destinationID := "dest123"
			eventType := "track"

			config.Set("Router.throttler.WEBHOOK.dest123.track.limit", 0)
			config.Set("Router.throttler.WEBHOOK.dest123.track.timeWindow", "10s")

			throttler := NewPerEventTypeThrottler(destType, destinationID, eventType, mockLimiter, config, statsStore, logger.NOP)

			require.False(t, throttler.enabled())
		})

		t.Run("ReturnsFalseForZeroWindow", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: true}

			destType := "WEBHOOK"
			destinationID := "dest123"
			eventType := "track"

			config.Set("Router.throttler.WEBHOOK.dest123.track.limit", 100)
			config.Set("Router.throttler.WEBHOOK.dest123.track.timeWindow", "0s")

			throttler := NewPerEventTypeThrottler(destType, destinationID, eventType, mockLimiter, config, statsStore, logger.NOP)

			require.False(t, throttler.enabled())
		})
	})

	t.Run("GetLimit", func(t *testing.T) {
		t.Run("ReturnsConfiguredLimit", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: true}

			destType := "WEBHOOK"
			destinationID := "dest123"
			eventType := "track"

			config.Set("Router.throttler.WEBHOOK.dest123.track.limit", 250)

			throttler := NewPerEventTypeThrottler(destType, destinationID, eventType, mockLimiter, config, statsStore, logger.NOP)

			require.Equal(t, int64(250), throttler.GetLimit())
		})

		t.Run("UpdatesWithConfigChanges", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: true}

			destType := "WEBHOOK"
			destinationID := "dest123"
			eventType := "track"

			config.Set("Router.throttler.WEBHOOK.dest123.track.limit", 100)

			throttler := NewPerEventTypeThrottler(destType, destinationID, eventType, mockLimiter, config, statsStore, logger.NOP)

			require.Equal(t, int64(100), throttler.GetLimit())

			// Update config
			config.Set("Router.throttler.WEBHOOK.dest123.track.limit", 200)

			require.Equal(t, int64(200), throttler.GetLimit())
		})
	})

	t.Run("getTimeWindowInSeconds", func(t *testing.T) {
		t.Run("ReturnsCorrectSeconds", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: true}

			destType := "WEBHOOK"
			destinationID := "dest123"
			eventType := "track"

			config.Set("Router.throttler.WEBHOOK.dest123.track.timeWindow", "30s")

			throttler := NewPerEventTypeThrottler(destType, destinationID, eventType, mockLimiter, config, statsStore, logger.NOP)

			require.Equal(t, int64(30), throttler.getTimeWindowInSeconds())
		})

		t.Run("HandlesMinutes", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: true}

			destType := "WEBHOOK"
			destinationID := "dest123"
			eventType := "track"

			config.Set("Router.throttler.WEBHOOK.dest123.track.timeWindow", "2m")

			throttler := NewPerEventTypeThrottler(destType, destinationID, eventType, mockLimiter, config, statsStore, logger.NOP)

			require.Equal(t, int64(120), throttler.getTimeWindowInSeconds())
		})
	})

	t.Run("ResponseCodeReceived", func(t *testing.T) {
		t.Run("DoesNotPanic", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: true}

			destType := "WEBHOOK"
			destinationID := "dest123"
			eventType := "track"

			throttler := NewPerEventTypeThrottler(destType, destinationID, eventType, mockLimiter, config, statsStore, logger.NOP)

			require.NotPanics(t, func() {
				throttler.ResponseCodeReceived(200)
				throttler.ResponseCodeReceived(429)
				throttler.ResponseCodeReceived(500)
			})
		})
	})

	t.Run("Shutdown", func(t *testing.T) {
		t.Run("DoesNotPanic", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: true}

			destType := "WEBHOOK"
			destinationID := "dest123"
			eventType := "track"

			throttler := NewPerEventTypeThrottler(destType, destinationID, eventType, mockLimiter, config, statsStore, logger.NOP)

			require.NotPanics(t, func() {
				throttler.Shutdown()
			})
		})
	})

	t.Run("updateGauges", func(t *testing.T) {
		t.Run("UpdatesRateLimitGauge", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: true}

			destType := "WEBHOOK"
			destinationID := "dest123"
			eventType := "track"

			config.Set("Router.throttler.WEBHOOK.dest123.track.limit", 100)
			config.Set("Router.throttler.WEBHOOK.dest123.track.timeWindow", "10s")

			throttler := NewPerEventTypeThrottler(destType, destinationID, eventType, mockLimiter, config, statsStore, logger.NOP)

			// Trigger gauge update by calling CheckLimitReached (which calls updateGauges)
			_, err = throttler.CheckLimitReached(context.Background(), 1)
			require.NoError(t, err)

			// Check if gauge was updated (rate = limit/window = 100/10 = 10)
			metrics := statsStore.GetByName("throttling_rate_limit")
			require.NotEmpty(t, metrics)

			// Find the metric with correct tags
			found := false
			for _, metric := range metrics {
				if metric.Tags["destinationId"] == destinationID &&
					metric.Tags["destType"] == destType &&
					metric.Tags["eventType"] == eventType &&
					metric.Tags["adaptive"] == "false" {
					require.Equal(t, float64(10), metric.Value) // 100/10 = 10
					found = true
					break
				}
			}
			require.True(t, found, "Should find metric with correct tags")
		})

		t.Run("DoesNotUpdateGaugeForZeroWindow", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: true}

			destType := "WEBHOOK"
			destinationID := "dest123"
			eventType := "track"

			config.Set("Router.throttler.WEBHOOK.dest123.track.limit", 100)
			config.Set("Router.throttler.WEBHOOK.dest123.track.timeWindow", "0s")

			throttler := NewPerEventTypeThrottler(destType, destinationID, eventType, mockLimiter, config, statsStore, logger.NOP)

			// This should not call limiter due to invalid config, but should still call updateGauges
			_, err = throttler.CheckLimitReached(context.Background(), 1)
			require.NoError(t, err)

			// Check that the gauge exists but has not been updated with a rate calculation
			// The gauge is created with initial value 0 and should remain 0 since window is 0
			metrics := statsStore.GetByName("throttling_rate_limit")
			require.NotEmpty(t, metrics)

			// Find the metric with correct tags and verify it has value 0 (not updated)
			found := false
			for _, metric := range metrics {
				if metric.Tags["destinationId"] == destinationID &&
					metric.Tags["destType"] == destType &&
					metric.Tags["eventType"] == eventType &&
					metric.Tags["adaptive"] == "false" {
					require.Equal(t, float64(0), metric.Value) // Should remain 0
					found = true
					break
				}
			}
			require.True(t, found, "Should find metric with correct tags")
		})
	})

	t.Run("ConfigurationHierarchy", func(t *testing.T) {
		t.Run("PrioritizesEventSpecificConfig", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: true}

			destType := "WEBHOOK"
			destinationID := "dest123"
			eventType := "track"

			// Set all possible configs, most specific should win
			config.Set("Router.throttler.WEBHOOK.dest123.track.limit", 300)
			config.Set("Router.throttler.WEBHOOK.dest123.limit", 200)
			config.Set("Router.throttler.WEBHOOK.track.limit", 150)
			config.Set("Router.throttler.WEBHOOK.limit", 100)

			config.Set("Router.throttler.WEBHOOK.dest123.track.timeWindow", "30s")
			config.Set("Router.throttler.WEBHOOK.dest123.timeWindow", "20s")
			config.Set("Router.throttler.WEBHOOK.track.timeWindow", "15s")
			config.Set("Router.throttler.WEBHOOK.timeWindow", "10s")

			throttler := NewPerEventTypeThrottler(destType, destinationID, eventType, mockLimiter, config, statsStore, logger.NOP)

			require.Equal(t, int64(300), throttler.GetLimit())
			require.Equal(t, int64(30), throttler.getTimeWindowInSeconds())
		})

		t.Run("FallsBackToDestinationConfig", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: true}

			destType := "WEBHOOK"
			destinationID := "dest123"
			eventType := "track"

			// Set destination and fallback configs
			config.Set("Router.throttler.WEBHOOK.dest123.limit", 200)
			config.Set("Router.throttler.WEBHOOK.track.limit", 150)
			config.Set("Router.throttler.WEBHOOK.limit", 100)

			config.Set("Router.throttler.WEBHOOK.dest123.timeWindow", "20s")
			config.Set("Router.throttler.WEBHOOK.track.timeWindow", "15s")
			config.Set("Router.throttler.WEBHOOK.timeWindow", "10s")

			throttler := NewPerEventTypeThrottler(destType, destinationID, eventType, mockLimiter, config, statsStore, logger.NOP)

			require.Equal(t, int64(200), throttler.GetLimit())
			require.Equal(t, int64(20), throttler.getTimeWindowInSeconds())
		})

		t.Run("FallsBackToDestinationTypeEventConfig", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: true}

			destType := "WEBHOOK"
			destinationID := "dest123"
			eventType := "track"

			// Set destination type + event and fallback configs
			config.Set("Router.throttler.WEBHOOK.track.limit", 150)
			config.Set("Router.throttler.WEBHOOK.limit", 100)

			config.Set("Router.throttler.WEBHOOK.track.timeWindow", "15s")
			config.Set("Router.throttler.WEBHOOK.timeWindow", "10s")

			throttler := NewPerEventTypeThrottler(destType, destinationID, eventType, mockLimiter, config, statsStore, logger.NOP)

			require.Equal(t, int64(150), throttler.GetLimit())
			require.Equal(t, int64(15), throttler.getTimeWindowInSeconds())
		})

		t.Run("FallsBackToDestinationTypeConfig", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: true}

			destType := "WEBHOOK"
			destinationID := "dest123"
			eventType := "track"

			// Set only destination type config
			config.Set("Router.throttler.WEBHOOK.limit", 100)
			config.Set("Router.throttler.WEBHOOK.timeWindow", "10s")

			throttler := NewPerEventTypeThrottler(destType, destinationID, eventType, mockLimiter, config, statsStore, logger.NOP)

			require.Equal(t, int64(100), throttler.GetLimit())
			require.Equal(t, int64(10), throttler.getTimeWindowInSeconds())
		})
	})

	t.Run("KeyGeneration", func(t *testing.T) {
		t.Run("UsesCorrectKeyFormat", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: true}

			destType := "WEBHOOK"
			destinationID := "dest123"
			eventType := "track"

			config.Set("Router.throttler.WEBHOOK.dest123.track.limit", 100)
			config.Set("Router.throttler.WEBHOOK.dest123.track.timeWindow", "10s")

			throttler := NewPerEventTypeThrottler(destType, destinationID, eventType, mockLimiter, config, statsStore, logger.NOP)

			_, err = throttler.CheckLimitReached(context.Background(), 1)
			require.NoError(t, err)

			require.Len(t, mockLimiter.CallLog, 1)
			require.Equal(t, "dest123:track", mockLimiter.CallLog[0].Key)
		})

		t.Run("HandlesSpecialCharactersInIDs", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: true}

			destType := "WEBHOOK"
			destinationID := "dest-123_test"
			eventType := "custom.event"

			config.Set("Router.throttler.WEBHOOK.dest-123_test.custom.event.limit", 100)
			config.Set("Router.throttler.WEBHOOK.dest-123_test.custom.event.timeWindow", "10s")

			throttler := NewPerEventTypeThrottler(destType, destinationID, eventType, mockLimiter, config, statsStore, logger.NOP)

			_, err = throttler.CheckLimitReached(context.Background(), 1)
			require.NoError(t, err)

			require.Len(t, mockLimiter.CallLog, 1)
			require.Equal(t, "dest-123_test:custom.event", mockLimiter.CallLog[0].Key)
		})
	})
}
