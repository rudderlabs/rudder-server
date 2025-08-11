package adaptive

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
)

func TestAdaptivePerEventTypeThrottler(t *testing.T) {
	t.Run("NewPerEventTypeThrottler", func(t *testing.T) {
		t.Run("CreatesThrottlerWithCorrectConfiguration", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: true}
			mockAlgorithm := &MockAlgorithm{LimitFactorValue: 0.5}

			destType := "WEBHOOK"
			destinationID := "dest123"
			eventType := "track"

			// Set configuration
			config.Set("Router.throttler.adaptive.WEBHOOK.dest123.track.minLimit", 10)
			config.Set("Router.throttler.adaptive.WEBHOOK.dest123.track.maxLimit", 100)
			config.Set("Router.throttler.adaptive.timeWindow", "10s")

			throttler := NewPerEventTypeThrottler(destType, destinationID, eventType, mockAlgorithm, mockLimiter, config, statsStore, logger.NOP)

			require.NotNil(t, throttler)
			require.Equal(t, destinationID, throttler.destinationID)
			require.Equal(t, eventType, throttler.eventType)
			require.Equal(t, mockLimiter, throttler.limiter)
			require.Equal(t, mockAlgorithm, throttler.algorithm)
			require.Equal(t, int64(10), throttler.getMinLimit())
			require.Equal(t, int64(100), throttler.getMaxLimit())
		})

		t.Run("UsesConfigurationFallbacks", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: true}
			mockAlgorithm := &MockAlgorithm{LimitFactorValue: 0.5}

			destType := "WEBHOOK"
			destinationID := "dest123"
			eventType := "track"

			// Set destination type + event config (fallback)
			config.Set("Router.throttler.adaptive.WEBHOOK.track.minLimit", 8)
			config.Set("Router.throttler.adaptive.WEBHOOK.track.maxLimit", 80)
			config.Set("Router.throttler.adaptive.timeWindow", "8s")

			throttler := NewPerEventTypeThrottler(destType, destinationID, eventType, mockAlgorithm, mockLimiter, config, statsStore, logger.NOP)

			require.Equal(t, int64(8), throttler.getMinLimit())
			require.Equal(t, int64(80), throttler.getMaxLimit())
		})

		t.Run("CreatesStatsGaugesWithCorrectTags", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: true}
			mockAlgorithm := &MockAlgorithm{LimitFactorValue: 0.5}

			destType := "WEBHOOK"
			destinationID := "dest123"
			eventType := "track"

			throttler := NewPerEventTypeThrottler(destType, destinationID, eventType, mockAlgorithm, mockLimiter, config, statsStore, logger.NOP)

			require.NotNil(t, throttler.limitFactorGauge)
			require.NotNil(t, throttler.rateLimitGauge)
		})
	})

	t.Run("CheckLimitReached", func(t *testing.T) {
		t.Run("ReturnsNotLimitedWhenAllowed", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: true}
			mockAlgorithm := &MockAlgorithm{LimitFactorValue: 0.8}

			destType := "WEBHOOK"
			destinationID := "dest123"
			eventType := "track"

			config.Set("Router.throttler.adaptive.WEBHOOK.dest123.track.minLimit", 10)
			config.Set("Router.throttler.adaptive.WEBHOOK.dest123.track.maxLimit", 100)
			config.Set("Router.throttler.adaptive.timeWindow", "10s")

			throttler := NewPerEventTypeThrottler(destType, destinationID, eventType, mockAlgorithm, mockLimiter, config, statsStore, logger.NOP)

			limited, err := throttler.CheckLimitReached(context.Background(), 5) // cost should be ignored

			require.NoError(t, err)
			require.False(t, limited)
			require.Len(t, mockLimiter.CallLog, 1)
			require.Equal(t, int64(1), mockLimiter.CallLog[0].Cost)  // Should always be 1
			require.Equal(t, int64(80), mockLimiter.CallLog[0].Rate) // 100 * 0.8 = 80
			require.Equal(t, int64(10), mockLimiter.CallLog[0].Window)
			require.Equal(t, destinationID+":"+eventType, mockLimiter.CallLog[0].Key)
		})

		t.Run("ReturnsLimitedWhenNotAllowed", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: false}
			mockAlgorithm := &MockAlgorithm{LimitFactorValue: 0.8}

			destType := "WEBHOOK"
			destinationID := "dest123"
			eventType := "track"

			config.Set("Router.throttler.adaptive.WEBHOOK.dest123.track.minLimit", 10)
			config.Set("Router.throttler.adaptive.WEBHOOK.dest123.track.maxLimit", 100)
			config.Set("Router.throttler.adaptive.timeWindow", "10s")

			throttler := NewPerEventTypeThrottler(destType, destinationID, eventType, mockAlgorithm, mockLimiter, config, statsStore, logger.NOP)

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
			mockAlgorithm := &MockAlgorithm{LimitFactorValue: 0.8}

			destType := "WEBHOOK"
			destinationID := "dest123"
			eventType := "track"

			config.Set("Router.throttler.adaptive.WEBHOOK.dest123.track.minLimit", 10)
			config.Set("Router.throttler.adaptive.WEBHOOK.dest123.track.maxLimit", 100)
			config.Set("Router.throttler.adaptive.timeWindow", "10s")

			throttler := NewPerEventTypeThrottler(destType, destinationID, eventType, mockAlgorithm, mockLimiter, config, statsStore, logger.NOP)

			limited, err := throttler.CheckLimitReached(context.Background(), 5)

			require.Error(t, err)
			require.Contains(t, err.Error(), "throttling failed for")
			require.False(t, limited)
		})

		t.Run("ReturnsNotLimitedForInenabled", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: true}
			mockAlgorithm := &MockAlgorithm{LimitFactorValue: 0.8}
			mockLogger := &MockLogger{}

			destType := "WEBHOOK"
			destinationID := "dest123"
			eventType := "track"

			// Invalid configuration - minLimit > maxLimit
			config.Set("Router.throttler.adaptive.WEBHOOK.dest123.track.minLimit", 200)
			config.Set("Router.throttler.adaptive.WEBHOOK.dest123.track.maxLimit", 100)
			throttler := NewPerEventTypeThrottler(destType, destinationID, eventType, mockAlgorithm, mockLimiter, config, statsStore, mockLogger)

			limited, err := throttler.CheckLimitReached(context.Background(), 5)

			require.NoError(t, err)
			require.False(t, limited)
			require.Empty(t, mockLimiter.CallLog) // Should not call limiter

		})

		t.Run("ReturnsNotLimitedWhenMinLimitGreaterThanMaxLimit", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: true}
			mockAlgorithm := &MockAlgorithm{LimitFactorValue: 0.8}
			mockLogger := &MockLogger{}

			destType := "WEBHOOK"
			destinationID := "dest123"
			eventType := "track"

			config.Set("Router.throttler.adaptive.WEBHOOK.dest123.track.minLimit", 200)
			config.Set("Router.throttler.adaptive.WEBHOOK.dest123.track.maxLimit", 100)
			config.Set("Router.throttler.adaptive.timeWindow", "10s")

			throttler := NewPerEventTypeThrottler(destType, destinationID, eventType, mockAlgorithm, mockLimiter, config, statsStore, mockLogger)

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
			mockAlgorithm := &MockAlgorithm{LimitFactorValue: 0.8}

			destType := "WEBHOOK"
			destinationID := "dest123"
			eventType := "track"

			config.Set("Router.throttler.adaptive.WEBHOOK.dest123.track.minLimit", 10)
			config.Set("Router.throttler.adaptive.WEBHOOK.dest123.track.maxLimit", 100)
			config.Set("Router.throttler.adaptive.timeWindow", "10s")

			throttler := NewPerEventTypeThrottler(destType, destinationID, eventType, mockAlgorithm, mockLimiter, config, statsStore, logger.NOP)

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
			mockAlgorithm := &MockAlgorithm{LimitFactorValue: 0.8}

			destType := "WEBHOOK"
			destinationID := "dest123"
			eventType := "track"

			config.Set("Router.throttler.adaptive.WEBHOOK.dest123.track.minLimit", 10)
			config.Set("Router.throttler.adaptive.WEBHOOK.dest123.track.maxLimit", 100)
			config.Set("Router.throttler.adaptive.timeWindow", "10s")

			throttler := NewPerEventTypeThrottler(destType, destinationID, eventType, mockAlgorithm, mockLimiter, config, statsStore, logger.NOP)

			require.True(t, throttler.enabled())
		})

		t.Run("ReturnsFalseForZeroMinLimit", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: true}
			mockAlgorithm := &MockAlgorithm{LimitFactorValue: 0.8}

			destType := "WEBHOOK"
			destinationID := "dest123"
			eventType := "track"

			config.Set("Router.throttler.adaptive.WEBHOOK.dest123.track.minLimit", 0)
			config.Set("Router.throttler.adaptive.WEBHOOK.dest123.track.maxLimit", 100)
			config.Set("Router.throttler.adaptive.timeWindow", "10s")

			throttler := NewPerEventTypeThrottler(destType, destinationID, eventType, mockAlgorithm, mockLimiter, config, statsStore, logger.NOP)

			require.False(t, throttler.enabled())
		})

		t.Run("ReturnsFalseForZeroMaxLimit", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: true}
			mockAlgorithm := &MockAlgorithm{LimitFactorValue: 0.8}

			destType := "WEBHOOK"
			destinationID := "dest123"
			eventType := "track"

			// Set zero max limit and disable fallbacks
			config.Set("Router.throttler.adaptive.WEBHOOK.dest123.track.minLimit", 10)
			config.Set("Router.throttler.adaptive.WEBHOOK.dest123.track.maxLimit", 0)
			config.Set("Router.throttler.adaptive.WEBHOOK.dest123.maxLimit", 0)
			config.Set("Router.throttler.adaptive.WEBHOOK.track.maxLimit", 0)
			config.Set("Router.throttler.adaptive.WEBHOOK.maxLimit", 0)
			config.Set("Router.throttler.adaptive.maxLimit", 0)
			config.Set("Router.throttler.adaptive.defaultMaxLimit", 0)
			config.Set("Router.throttler.adaptive.timeWindow", "10s")

			throttler := NewPerEventTypeThrottler(destType, destinationID, eventType, mockAlgorithm, mockLimiter, config, statsStore, logger.NOP)

			require.False(t, throttler.enabled())
		})

		t.Run("ReturnsFalseForZeroWindow", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: true}
			mockAlgorithm := &MockAlgorithm{LimitFactorValue: 0.8}

			destType := "WEBHOOK"
			destinationID := "dest123"
			eventType := "track"

			config.Set("Router.throttler.adaptive.WEBHOOK.dest123.track.minLimit", 10)
			config.Set("Router.throttler.adaptive.WEBHOOK.dest123.track.maxLimit", 100)
			config.Set("Router.throttler.adaptive.timeWindow", "0s")

			throttler := NewPerEventTypeThrottler(destType, destinationID, eventType, mockAlgorithm, mockLimiter, config, statsStore, logger.NOP)

			require.False(t, throttler.enabled())
		})

		t.Run("ReturnsFalseWhenMinLimitGreaterThanMaxLimit", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: true}
			mockAlgorithm := &MockAlgorithm{LimitFactorValue: 0.8}

			destType := "WEBHOOK"
			destinationID := "dest123"
			eventType := "track"

			config.Set("Router.throttler.adaptive.WEBHOOK.dest123.track.minLimit", 200)
			config.Set("Router.throttler.adaptive.WEBHOOK.dest123.track.maxLimit", 100)
			config.Set("Router.throttler.adaptive.timeWindow", "10s")

			throttler := NewPerEventTypeThrottler(destType, destinationID, eventType, mockAlgorithm, mockLimiter, config, statsStore, logger.NOP)

			require.False(t, throttler.enabled())
		})
	})

	t.Run("GetLimit", func(t *testing.T) {
		t.Run("ReturnsLimitBasedOnAlgorithmFactor", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: true}
			mockAlgorithm := &MockAlgorithm{LimitFactorValue: 0.6}

			destType := "WEBHOOK"
			destinationID := "dest123"
			eventType := "track"

			config.Set("Router.throttler.adaptive.WEBHOOK.dest123.track.minLimit", 10)
			config.Set("Router.throttler.adaptive.WEBHOOK.dest123.track.maxLimit", 100)

			throttler := NewPerEventTypeThrottler(destType, destinationID, eventType, mockAlgorithm, mockLimiter, config, statsStore, logger.NOP)

			// Expected: 100 * 0.6 = 60
			require.Equal(t, int64(60), throttler.GetLimit())
		})

		t.Run("EnforcesMinimumLimit", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: true}
			mockAlgorithm := &MockAlgorithm{LimitFactorValue: 0.05} // Very low factor

			destType := "WEBHOOK"
			destinationID := "dest123"
			eventType := "track"

			config.Set("Router.throttler.adaptive.WEBHOOK.dest123.track.minLimit", 10)
			config.Set("Router.throttler.adaptive.WEBHOOK.dest123.track.maxLimit", 100)

			throttler := NewPerEventTypeThrottler(destType, destinationID, eventType, mockAlgorithm, mockLimiter, config, statsStore, logger.NOP)

			// Expected: max(10, 100 * 0.05) = max(10, 5) = 10
			require.Equal(t, int64(10), throttler.GetLimit())
		})

		t.Run("UpdatesWithAlgorithmChanges", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: true}
			mockAlgorithm := &MockAlgorithm{LimitFactorValue: 0.5}

			destType := "WEBHOOK"
			destinationID := "dest123"
			eventType := "track"

			config.Set("Router.throttler.adaptive.WEBHOOK.dest123.track.minLimit", 10)
			config.Set("Router.throttler.adaptive.WEBHOOK.dest123.track.maxLimit", 100)

			throttler := NewPerEventTypeThrottler(destType, destinationID, eventType, mockAlgorithm, mockLimiter, config, statsStore, logger.NOP)

			require.Equal(t, int64(50), throttler.GetLimit())

			// Update algorithm factor
			mockAlgorithm.LimitFactorValue = 0.8
			require.Equal(t, int64(80), throttler.GetLimit())
		})
	})

	t.Run("ResponseCodeReceived", func(t *testing.T) {
		t.Run("ForwardsToAlgorithm", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: true}
			mockAlgorithm := &MockAlgorithm{LimitFactorValue: 0.8}

			destType := "WEBHOOK"
			destinationID := "dest123"
			eventType := "track"

			throttler := NewPerEventTypeThrottler(destType, destinationID, eventType, mockAlgorithm, mockLimiter, config, statsStore, logger.NOP)

			throttler.ResponseCodeReceived(200)
			throttler.ResponseCodeReceived(429)
			throttler.ResponseCodeReceived(500)

			require.Equal(t, []int{200, 429, 500}, mockAlgorithm.ResponseCodeReceivedLog)
		})
	})

	t.Run("Shutdown", func(t *testing.T) {
		t.Run("ForwardsToAlgorithm", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: true}
			mockAlgorithm := &MockAlgorithm{LimitFactorValue: 0.8}

			destType := "WEBHOOK"
			destinationID := "dest123"
			eventType := "track"

			throttler := NewPerEventTypeThrottler(destType, destinationID, eventType, mockAlgorithm, mockLimiter, config, statsStore, logger.NOP)

			throttler.Shutdown()

			require.True(t, mockAlgorithm.ShutdownCalled)
		})
	})

	t.Run("updateGauges", func(t *testing.T) {
		t.Run("UpdatesBothGauges", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: true}
			mockAlgorithm := &MockAlgorithm{LimitFactorValue: 0.6}

			destType := "WEBHOOK"
			destinationID := "dest123"
			eventType := "track"

			config.Set("Router.throttler.adaptive.WEBHOOK.dest123.track.minLimit", 10)
			config.Set("Router.throttler.adaptive.WEBHOOK.dest123.track.maxLimit", 100)
			config.Set("Router.throttler.adaptive.timeWindow", "10s")

			throttler := NewPerEventTypeThrottler(destType, destinationID, eventType, mockAlgorithm, mockLimiter, config, statsStore, logger.NOP)

			// Trigger gauge update by calling CheckLimitReached
			_, err = throttler.CheckLimitReached(context.Background(), 1)
			require.NoError(t, err)

			// Check rate limit gauge (limit/window = 60/10 = 6)
			rateLimitMetrics := statsStore.GetByName("throttling_rate_limit")
			require.NotEmpty(t, rateLimitMetrics)

			found := false
			for _, metric := range rateLimitMetrics {
				if metric.Tags["destinationId"] == destinationID &&
					metric.Tags["destType"] == destType &&
					metric.Tags["eventType"] == eventType &&
					metric.Tags["adaptive"] == "true" {
					require.Equal(t, float64(6), metric.Value) // 60/10 = 6
					found = true
					break
				}
			}
			require.True(t, found, "Should find rate limit metric with correct tags")

			// Check limit factor gauge
			factorMetrics := statsStore.GetByName("adaptive_throttler_limit_factor")
			require.NotEmpty(t, factorMetrics)

			found = false
			for _, metric := range factorMetrics {
				if metric.Tags["destinationId"] == destinationID &&
					metric.Tags["destType"] == destType &&
					metric.Tags["eventType"] == eventType {
					require.Equal(t, float64(0.6), metric.Value)
					found = true
					break
				}
			}
			require.True(t, found, "Should find limit factor metric with correct tags")
		})

		t.Run("DoesNotUpdateRateLimitGaugeForZeroWindow", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: true}
			mockAlgorithm := &MockAlgorithm{LimitFactorValue: 0.6}

			destType := "WEBHOOK"
			destinationID := "dest123"
			eventType := "track"

			config.Set("Router.throttler.adaptive.WEBHOOK.dest123.track.minLimit", 10)
			config.Set("Router.throttler.adaptive.WEBHOOK.dest123.track.maxLimit", 100)
			config.Set("Router.throttler.adaptive.timeWindow", "0s")

			throttler := NewPerEventTypeThrottler(destType, destinationID, eventType, mockAlgorithm, mockLimiter, config, statsStore, logger.NOP)

			// This should not call limiter due to invalid config
			_, err = throttler.CheckLimitReached(context.Background(), 1)
			require.NoError(t, err)

			// Rate limit gauge should exist but have value 0
			rateLimitMetrics := statsStore.GetByName("throttling_rate_limit")
			require.NotEmpty(t, rateLimitMetrics)

			// Limit factor gauge should also exist but have value 0
			factorMetrics := statsStore.GetByName("adaptive_throttler_limit_factor")
			require.NotEmpty(t, factorMetrics)

		})
	})

	t.Run("ConfigurationHierarchy", func(t *testing.T) {
		t.Run("PrioritizesEventSpecificConfig", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: true}
			mockAlgorithm := &MockAlgorithm{LimitFactorValue: 0.8}

			destType := "WEBHOOK"
			destinationID := "dest123"
			eventType := "track"

			// Set all possible configs, most specific should win
			config.Set("Router.throttler.adaptive.WEBHOOK.dest123.track.minLimit", 25)
			config.Set("Router.throttler.adaptive.WEBHOOK.dest123.minLimit", 20)
			config.Set("Router.throttler.adaptive.WEBHOOK.track.minLimit", 15)
			config.Set("Router.throttler.adaptive.WEBHOOK.minLimit", 10)
			config.Set("Router.throttler.adaptive.minLimit", 5)

			config.Set("Router.throttler.adaptive.WEBHOOK.dest123.track.maxLimit", 250)
			config.Set("Router.throttler.adaptive.WEBHOOK.dest123.maxLimit", 200)
			config.Set("Router.throttler.adaptive.WEBHOOK.track.maxLimit", 150)
			config.Set("Router.throttler.adaptive.WEBHOOK.maxLimit", 100)
			config.Set("Router.throttler.adaptive.maxLimit", 50)

			throttler := NewPerEventTypeThrottler(destType, destinationID, eventType, mockAlgorithm, mockLimiter, config, statsStore, logger.NOP)

			require.Equal(t, int64(25), throttler.getMinLimit())
			require.Equal(t, int64(250), throttler.getMaxLimit())
		})

		t.Run("FallsBackToDestinationConfig", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: true}
			mockAlgorithm := &MockAlgorithm{LimitFactorValue: 0.8}

			destType := "WEBHOOK"
			destinationID := "dest123"
			eventType := "track"

			// Set destination and fallback configs
			config.Set("Router.throttler.adaptive.WEBHOOK.dest123.minLimit", 20)
			config.Set("Router.throttler.adaptive.WEBHOOK.track.minLimit", 15)
			config.Set("Router.throttler.adaptive.WEBHOOK.minLimit", 10)
			config.Set("Router.throttler.adaptive.minLimit", 5)

			config.Set("Router.throttler.adaptive.WEBHOOK.dest123.maxLimit", 200)
			config.Set("Router.throttler.adaptive.WEBHOOK.track.maxLimit", 150)
			config.Set("Router.throttler.adaptive.WEBHOOK.maxLimit", 100)
			config.Set("Router.throttler.adaptive.maxLimit", 50)

			throttler := NewPerEventTypeThrottler(destType, destinationID, eventType, mockAlgorithm, mockLimiter, config, statsStore, logger.NOP)

			require.Equal(t, int64(20), throttler.getMinLimit())
			require.Equal(t, int64(200), throttler.getMaxLimit())
		})

		t.Run("FallsBackToDestinationTypeEventConfig", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: true}
			mockAlgorithm := &MockAlgorithm{LimitFactorValue: 0.8}

			destType := "WEBHOOK"
			destinationID := "dest123"
			eventType := "track"

			// Set destination type + event and fallback configs
			config.Set("Router.throttler.adaptive.WEBHOOK.track.minLimit", 15)
			config.Set("Router.throttler.adaptive.WEBHOOK.minLimit", 10)
			config.Set("Router.throttler.adaptive.minLimit", 5)

			config.Set("Router.throttler.adaptive.WEBHOOK.track.maxLimit", 150)
			config.Set("Router.throttler.adaptive.WEBHOOK.maxLimit", 100)
			config.Set("Router.throttler.adaptive.maxLimit", 50)

			throttler := NewPerEventTypeThrottler(destType, destinationID, eventType, mockAlgorithm, mockLimiter, config, statsStore, logger.NOP)

			require.Equal(t, int64(15), throttler.getMinLimit())
			require.Equal(t, int64(150), throttler.getMaxLimit())
		})

		t.Run("FallsBackToDestinationTypeConfig", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: true}
			mockAlgorithm := &MockAlgorithm{LimitFactorValue: 0.8}

			destType := "WEBHOOK"
			destinationID := "dest123"
			eventType := "track"

			// Set destination type and global configs
			config.Set("Router.throttler.adaptive.WEBHOOK.minLimit", 10)
			config.Set("Router.throttler.adaptive.minLimit", 5)

			config.Set("Router.throttler.adaptive.WEBHOOK.maxLimit", 100)
			config.Set("Router.throttler.adaptive.maxLimit", 50)

			throttler := NewPerEventTypeThrottler(destType, destinationID, eventType, mockAlgorithm, mockLimiter, config, statsStore, logger.NOP)

			require.Equal(t, int64(10), throttler.getMinLimit())
			require.Equal(t, int64(100), throttler.getMaxLimit())
		})

		t.Run("FallsBackToGlobalConfig", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: true}
			mockAlgorithm := &MockAlgorithm{LimitFactorValue: 0.8}

			destType := "WEBHOOK"
			destinationID := "dest123"
			eventType := "track"

			// Set only global configs
			config.Set("Router.throttler.adaptive.minLimit", 5)
			config.Set("Router.throttler.adaptive.maxLimit", 50)

			throttler := NewPerEventTypeThrottler(destType, destinationID, eventType, mockAlgorithm, mockLimiter, config, statsStore, logger.NOP)

			require.Equal(t, int64(5), throttler.getMinLimit())
			require.Equal(t, int64(50), throttler.getMaxLimit())
		})
	})

	t.Run("KeyGeneration", func(t *testing.T) {
		t.Run("UsesCorrectKeyFormat", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: true}
			mockAlgorithm := &MockAlgorithm{LimitFactorValue: 0.8}

			destType := "WEBHOOK"
			destinationID := "dest123"
			eventType := "track"

			config.Set("Router.throttler.adaptive.WEBHOOK.dest123.track.minLimit", 10)
			config.Set("Router.throttler.adaptive.WEBHOOK.dest123.track.maxLimit", 100)
			config.Set("Router.throttler.adaptive.timeWindow", "10s")

			throttler := NewPerEventTypeThrottler(destType, destinationID, eventType, mockAlgorithm, mockLimiter, config, statsStore, logger.NOP)

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
			mockAlgorithm := &MockAlgorithm{LimitFactorValue: 0.8}

			destType := "WEBHOOK"
			destinationID := "dest-123_test"
			eventType := "custom.event"

			config.Set("Router.throttler.adaptive.WEBHOOK.dest-123_test.custom.event.minLimit", 10)
			config.Set("Router.throttler.adaptive.WEBHOOK.dest-123_test.custom.event.maxLimit", 100)
			config.Set("Router.throttler.adaptive.timeWindow", "10s")

			throttler := NewPerEventTypeThrottler(destType, destinationID, eventType, mockAlgorithm, mockLimiter, config, statsStore, logger.NOP)

			_, err = throttler.CheckLimitReached(context.Background(), 1)
			require.NoError(t, err)

			require.Len(t, mockLimiter.CallLog, 1)
			require.Equal(t, "dest-123_test:custom.event", mockLimiter.CallLog[0].Key)
		})
	})

	t.Run("AlgorithmIntegration", func(t *testing.T) {
		t.Run("LimitChangesWithAlgorithmFactor", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: true}
			mockAlgorithm := &MockAlgorithm{LimitFactorValue: 1.0}

			destType := "WEBHOOK"
			destinationID := "dest123"
			eventType := "track"

			config.Set("Router.throttler.adaptive.WEBHOOK.dest123.track.minLimit", 10)
			config.Set("Router.throttler.adaptive.WEBHOOK.dest123.track.maxLimit", 100)
			config.Set("Router.throttler.adaptive.timeWindow", "10s")

			throttler := NewPerEventTypeThrottler(destType, destinationID, eventType, mockAlgorithm, mockLimiter, config, statsStore, logger.NOP)

			// Initial state: factor = 1.0, so limit = 100
			_, err = throttler.CheckLimitReached(context.Background(), 1)
			require.NoError(t, err)
			require.Equal(t, int64(100), mockLimiter.CallLog[0].Rate)

			// Algorithm adapts down
			mockLimiter.Reset()
			mockAlgorithm.LimitFactorValue = 0.5
			_, err = throttler.CheckLimitReached(context.Background(), 1)
			require.NoError(t, err)
			require.Equal(t, int64(50), mockLimiter.CallLog[0].Rate)

			// Algorithm adapts very low but min limit is enforced
			mockLimiter.Reset()
			mockAlgorithm.LimitFactorValue = 0.05
			_, err = throttler.CheckLimitReached(context.Background(), 1)
			require.NoError(t, err)
			require.Equal(t, int64(10), mockLimiter.CallLog[0].Rate) // min limit enforced
		})
	})
}
