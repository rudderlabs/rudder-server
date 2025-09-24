package adaptive

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
)

func TestAdaptiveAllEventTypesThrottler(t *testing.T) {
	t.Run("NewAllEventTypesThrottler", func(t *testing.T) {
		t.Run("CreatesThrottlerWithCorrectConfiguration", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: true}
			mockAlgorithm := &MockAlgorithm{LimitFactorValue: 0.5}

			destType := "WEBHOOK"
			destinationID := "dest123"

			// Set configuration
			config.Set("Router.throttler.WEBHOOK.dest123.minLimit", 10)
			config.Set("Router.throttler.WEBHOOK.dest123.maxLimit", 100)
			config.Set("Router.throttler.WEBHOOK.timeWindow", "10s")

			throttler := NewAllEventTypesThrottler(destType, destinationID, mockAlgorithm, mockLimiter, config, statsStore, logger.NOP)

			require.NotNil(t, throttler)
			require.Equal(t, destinationID, throttler.destinationID)
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

			// Set only destination type config (fallback)
			config.Set("Router.throttler.WEBHOOK.minLimit", 5)
			config.Set("Router.throttler.WEBHOOK.maxLimit", 50)
			config.Set("Router.throttler.WEBHOOK.timeWindow", "5s")

			throttler := NewAllEventTypesThrottler(destType, destinationID, mockAlgorithm, mockLimiter, config, statsStore, logger.NOP)

			require.Equal(t, int64(5), throttler.getMinLimit())
			require.Equal(t, int64(50), throttler.getMaxLimit())
		})

		t.Run("CreatesStatsGaugesWithCorrectTags", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: true}
			mockAlgorithm := &MockAlgorithm{LimitFactorValue: 0.5}

			destType := "WEBHOOK"
			destinationID := "dest123"

			throttler := NewAllEventTypesThrottler(destType, destinationID, mockAlgorithm, mockLimiter, config, statsStore, logger.NOP)

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

			config.Set("Router.throttler.WEBHOOK.dest123.minLimit", 10)
			config.Set("Router.throttler.WEBHOOK.dest123.maxLimit", 100)
			config.Set("Router.throttler.WEBHOOK.timeWindow", "10s")

			throttler := NewAllEventTypesThrottler(destType, destinationID, mockAlgorithm, mockLimiter, config, statsStore, logger.NOP)

			limited, err := throttler.CheckLimitReached(context.Background(), 1)

			require.NoError(t, err)
			require.False(t, limited)
			require.Len(t, mockLimiter.CallLog, 1)
			require.Equal(t, int64(1), mockLimiter.CallLog[0].Cost)
			require.Equal(t, int64(80), mockLimiter.CallLog[0].Rate) // 100 * 0.8 = 80
			require.Equal(t, int64(10), mockLimiter.CallLog[0].Window)
			require.Equal(t, destinationID, mockLimiter.CallLog[0].Key)
		})

		t.Run("ReturnsLimitedWhenNotAllowed", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: false}
			mockAlgorithm := &MockAlgorithm{LimitFactorValue: 0.8}

			destType := "WEBHOOK"
			destinationID := "dest123"

			config.Set("Router.throttler.WEBHOOK.dest123.minLimit", 10)
			config.Set("Router.throttler.WEBHOOK.dest123.maxLimit", 100)
			config.Set("Router.throttler.WEBHOOK.timeWindow", "10s")

			throttler := NewAllEventTypesThrottler(destType, destinationID, mockAlgorithm, mockLimiter, config, statsStore, logger.NOP)

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

			config.Set("Router.throttler.WEBHOOK.dest123.minLimit", 10)
			config.Set("Router.throttler.WEBHOOK.dest123.maxLimit", 100)
			config.Set("Router.throttler.WEBHOOK.timeWindow", "10s")

			throttler := NewAllEventTypesThrottler(destType, destinationID, mockAlgorithm, mockLimiter, config, statsStore, logger.NOP)

			limited, err := throttler.CheckLimitReached(context.Background(), 5)

			require.Error(t, err)
			require.Contains(t, err.Error(), "throttling failed for")
			require.False(t, limited)
		})

		t.Run("ReturnsNotLimitedIfNotEnabled", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: true}
			mockAlgorithm := &MockAlgorithm{LimitFactorValue: 0.8}
			mockLogger := &MockLogger{}

			destType := "WEBHOOK"
			destinationID := "dest123"

			// Invalid configuration - minLimit > maxLimit
			config.Set("Router.throttler.WEBHOOK.dest123.minLimit", 200)
			config.Set("Router.throttler.WEBHOOK.dest123.maxLimit", 100)
			throttler := NewAllEventTypesThrottler(destType, destinationID, mockAlgorithm, mockLimiter, config, statsStore, mockLogger)

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

			config.Set("Router.throttler.WEBHOOK.dest123.minLimit", 200)
			config.Set("Router.throttler.WEBHOOK.dest123.maxLimit", 100)
			config.Set("Router.throttler.WEBHOOK.timeWindow", "10s")

			throttler := NewAllEventTypesThrottler(destType, destinationID, mockAlgorithm, mockLimiter, config, statsStore, mockLogger)

			limited, err := throttler.CheckLimitReached(context.Background(), 5)

			require.NoError(t, err)
			require.False(t, limited)
			require.Empty(t, mockLimiter.CallLog)
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

			config.Set("Router.throttler.WEBHOOK.dest123.minLimit", 10)
			config.Set("Router.throttler.WEBHOOK.dest123.maxLimit", 100)
			config.Set("Router.throttler.WEBHOOK.timeWindow", "10s")

			throttler := NewAllEventTypesThrottler(destType, destinationID, mockAlgorithm, mockLimiter, config, statsStore, logger.NOP)

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

			config.Set("Router.throttler.WEBHOOK.dest123.minLimit", 0)
			config.Set("Router.throttler.WEBHOOK.dest123.maxLimit", 100)
			config.Set("Router.throttler.WEBHOOK.timeWindow", "10s")

			throttler := NewAllEventTypesThrottler(destType, destinationID, mockAlgorithm, mockLimiter, config, statsStore, logger.NOP)

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

			// Set zero max limit and disable fallbacks
			config.Set("Router.throttler.WEBHOOK.dest123.minLimit", 10)
			config.Set("Router.throttler.WEBHOOK.dest123.maxLimit", 0)
			config.Set("Router.throttler.WEBHOOK.maxLimit", 0)
			config.Set("Router.throttler.maxLimit", 0)
			config.Set("Router.throttler.defaultMaxLimit", 0)
			config.Set("Router.throttler.WEBHOOK.timeWindow", "10s")

			throttler := NewAllEventTypesThrottler(destType, destinationID, mockAlgorithm, mockLimiter, config, statsStore, logger.NOP)

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

			config.Set("Router.throttler.WEBHOOK.dest123.minLimit", 10)
			config.Set("Router.throttler.WEBHOOK.dest123.maxLimit", 100)
			config.Set("Router.throttler.WEBHOOK.timeWindow", "0s")

			throttler := NewAllEventTypesThrottler(destType, destinationID, mockAlgorithm, mockLimiter, config, statsStore, logger.NOP)

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

			config.Set("Router.throttler.WEBHOOK.dest123.minLimit", 200)
			config.Set("Router.throttler.WEBHOOK.dest123.maxLimit", 100)
			config.Set("Router.throttler.WEBHOOKtimeWindow", "10s")

			throttler := NewAllEventTypesThrottler(destType, destinationID, mockAlgorithm, mockLimiter, config, statsStore, logger.NOP)

			require.False(t, throttler.enabled())
		})
	})

	t.Run("InenabledWarningLogs", func(t *testing.T) {
		t.Run("EmitsWarningForZeroMinLimit", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: true}
			mockAlgorithm := &MockAlgorithm{LimitFactorValue: 0.8}
			mockLogger := &MockLogger{}

			destType := "WEBHOOK"
			destinationID := "dest123"

			config.Set("Router.throttler.WEBHOOK.dest123.minLimit", 0)
			config.Set("Router.throttler.WEBHOOK.dest123.maxLimit", 100)
			config.Set("Router.throttler.WEBHOOK.timeWindow", "10s")

			throttler := NewAllEventTypesThrottler(destType, destinationID, mockAlgorithm, mockLimiter, config, statsStore, mockLogger)

			limited, err := throttler.CheckLimitReached(context.Background(), 5)

			require.NoError(t, err)
			require.False(t, limited)
			require.Empty(t, mockLimiter.CallLog) // Should not call limiter
		})

		t.Run("EmitsWarningForZeroMaxLimit", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: true}
			mockAlgorithm := &MockAlgorithm{LimitFactorValue: 0.8}
			mockLogger := &MockLogger{}

			destType := "WEBHOOK"
			destinationID := "dest123"

			// Set zero max limit and disable fallbacks
			config.Set("Router.throttler.WEBHOOK.dest123.minLimit", 10)
			config.Set("Router.throttler.WEBHOOK.dest123.maxLimit", 0)
			config.Set("Router.throttler.WEBHOOK.maxLimit", 0)
			config.Set("Router.throttler.maxLimit", 0)
			config.Set("Router.throttler.defaultMaxLimit", 0)
			config.Set("Router.throttler.WEBHOOK.timeWindow", "10s")

			throttler := NewAllEventTypesThrottler(destType, destinationID, mockAlgorithm, mockLimiter, config, statsStore, mockLogger)

			limited, err := throttler.CheckLimitReached(context.Background(), 5)

			require.NoError(t, err)
			require.False(t, limited)
			require.Empty(t, mockLimiter.CallLog) // Should not call limiter
		})

		t.Run("EmitsWarningForZeroWindow", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: true}
			mockAlgorithm := &MockAlgorithm{LimitFactorValue: 0.8}
			mockLogger := &MockLogger{}

			destType := "WEBHOOK"
			destinationID := "dest123"

			config.Set("Router.throttler.WEBHOOK.dest123.minLimit", 10)
			config.Set("Router.throttler.WEBHOOK.dest123.maxLimit", 100)
			config.Set("Router.throttler.WEBHOOK.timeWindow", "0s")

			throttler := NewAllEventTypesThrottler(destType, destinationID, mockAlgorithm, mockLimiter, config, statsStore, mockLogger)

			limited, err := throttler.CheckLimitReached(context.Background(), 5)

			require.NoError(t, err)
			require.False(t, limited)
			require.Empty(t, mockLimiter.CallLog) // Should not call limiter
		})

		t.Run("EmitsWarningWhenMinLimitGreaterThanMaxLimit", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: true}
			mockAlgorithm := &MockAlgorithm{LimitFactorValue: 0.8}
			mockLogger := &MockLogger{}

			destType := "WEBHOOK"
			destinationID := "dest123"

			config.Set("Router.throttler.WEBHOOK.dest123.minLimit", 200)
			config.Set("Router.throttler.WEBHOOK.dest123.maxLimit", 100)
			config.Set("Router.throttler.WEBHOOK.timeWindow", "10s")

			throttler := NewAllEventTypesThrottler(destType, destinationID, mockAlgorithm, mockLimiter, config, statsStore, mockLogger)

			limited, err := throttler.CheckLimitReached(context.Background(), 5)

			require.NoError(t, err)
			require.False(t, limited)
			require.Empty(t, mockLimiter.CallLog) // Should not call limiter
		})

		t.Run("DoesNotEmitWarningForenabled", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: true}
			mockAlgorithm := &MockAlgorithm{LimitFactorValue: 0.8}
			mockLogger := &MockLogger{}

			destType := "WEBHOOK"
			destinationID := "dest123"

			config.Set("Router.throttler.WEBHOOK.dest123.minLimit", 10)
			config.Set("Router.throttler.WEBHOOK.dest123.maxLimit", 100)
			config.Set("Router.throttler.WEBHOOK.timeWindow", "10s")

			throttler := NewAllEventTypesThrottler(destType, destinationID, mockAlgorithm, mockLimiter, config, statsStore, mockLogger)

			limited, err := throttler.CheckLimitReached(context.Background(), 5)

			require.NoError(t, err)
			require.False(t, limited)
			require.Len(t, mockLimiter.CallLog, 1) // Should call limiter for valid config

			// Verify no warning log was emitted
			require.Empty(t, mockLogger.WarningLogs)
		})
	})

	t.Run("GetLimitPerSecond", func(t *testing.T) {
		t.Run("ReturnsLimitBasedOnAlgorithmFactor", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: true}
			mockAlgorithm := &MockAlgorithm{LimitFactorValue: 0.6}

			destType := "WEBHOOK"
			destinationID := "dest123"

			config.Set("Router.throttler.WEBHOOK.dest123.minLimit", 10)
			config.Set("Router.throttler.WEBHOOK.dest123.maxLimit", 100)

			throttler := NewAllEventTypesThrottler(destType, destinationID, mockAlgorithm, mockLimiter, config, statsStore, logger.NOP)

			// Expected: 100 * 0.6 = 60
			require.Equal(t, int64(60), throttler.GetLimitPerSecond())
		})

		t.Run("EnforcesMinimumLimit", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: true}
			mockAlgorithm := &MockAlgorithm{LimitFactorValue: 0.05} // Very low factor

			destType := "WEBHOOK"
			destinationID := "dest123"

			config.Set("Router.throttler.WEBHOOK.dest123.minLimit", 10)
			config.Set("Router.throttler.WEBHOOK.dest123.maxLimit", 100)

			throttler := NewAllEventTypesThrottler(destType, destinationID, mockAlgorithm, mockLimiter, config, statsStore, logger.NOP)

			// Expected: max(10, 100 * 0.05) = max(10, 5) = 10
			require.Equal(t, int64(10), throttler.GetLimitPerSecond())
		})

		t.Run("UpdatesWithAlgorithmChanges", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: true}
			mockAlgorithm := &MockAlgorithm{LimitFactorValue: 0.5}

			destType := "WEBHOOK"
			destinationID := "dest123"

			config.Set("Router.throttler.WEBHOOK.dest123.minLimit", 10)
			config.Set("Router.throttler.WEBHOOK.dest123.maxLimit", 100)

			throttler := NewAllEventTypesThrottler(destType, destinationID, mockAlgorithm, mockLimiter, config, statsStore, logger.NOP)

			require.Equal(t, int64(50), throttler.GetLimitPerSecond())

			// Update algorithm factor
			mockAlgorithm.LimitFactorValue = 0.8
			require.Equal(t, int64(80), throttler.GetLimitPerSecond())
		})

		t.Run("RoundsUp", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: true}
			mockAlgorithm := &MockAlgorithm{LimitFactorValue: 1.0}

			destType := "WEBHOOK"
			destinationID := "dest123"

			config.Set("Router.throttler.WEBHOOK.dest123.minLimit", 10)
			config.Set("Router.throttler.WEBHOOK.dest123.maxLimit", 15)
			config.Set("Router.throttler.WEBHOOK.timeWindow", 2*time.Second)

			throttler := NewAllEventTypesThrottler(destType, destinationID, mockAlgorithm, mockLimiter, config, statsStore, logger.NOP)

			require.Equal(t, int64(8), throttler.GetLimitPerSecond())
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

			throttler := NewAllEventTypesThrottler(destType, destinationID, mockAlgorithm, mockLimiter, config, statsStore, logger.NOP)

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

			throttler := NewAllEventTypesThrottler(destType, destinationID, mockAlgorithm, mockLimiter, config, statsStore, logger.NOP)

			throttler.Shutdown()

			require.True(t, mockAlgorithm.ShutdownCalled)
		})
	})

	t.Run("GetEventType", func(t *testing.T) {
		t.Run("ReturnsAllForAllEventTypesThrottler", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: true}
			mockAlgorithm := &MockAlgorithm{LimitFactorValue: 0.8}

			destType := "WEBHOOK"
			destinationID := "dest123"

			// Set valid configuration
			config.Set("Router.throttler.WEBHOOK.dest123.minLimit", 10)
			config.Set("Router.throttler.WEBHOOK.dest123.maxLimit", 100)
			config.Set("Router.throttler.WEBHOOK.dest123.timeWindow", "10s")

			throttler := NewAllEventTypesThrottler(destType, destinationID, mockAlgorithm, mockLimiter, config, statsStore, &MockLogger{})

			eventType := throttler.GetEventType()
			require.Equal(t, "all", eventType)
		})
	})

	t.Run("GetLastUsed", func(t *testing.T) {
		config := config.New()
		statsStore, err := memstats.New()
		require.NoError(t, err)
		mockLimiter := &MockLimiter{AllowResult: true}
		mockAlgorithm := &MockAlgorithm{LimitFactorValue: 0.8}

		destType := "WEBHOOK"
		destinationID := "dest123"

		// Set valid configuration
		config.Set("Router.throttler.WEBHOOK.dest123.minLimit", 10)
		config.Set("Router.throttler.WEBHOOK.dest123.maxLimit", 100)
		config.Set("Router.throttler.WEBHOOK.dest123.timeWindow", "10s")

		throttler := NewAllEventTypesThrottler(destType, destinationID, mockAlgorithm, mockLimiter, config, statsStore, &MockLogger{})

		lastUsed := throttler.GetLastUsed()
		require.Zero(t, lastUsed, "Last used should be zero before any access")
		_, _ = throttler.CheckLimitReached(context.Background(), 1)
		lastUsed = throttler.GetLastUsed()
		require.NotZero(t, lastUsed, "Last used should be updated after access")
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

			config.Set("Router.throttler.WEBHOOK.dest123.minLimit", 10)
			config.Set("Router.throttler.WEBHOOK.dest123.maxLimit", 100)
			config.Set("Router.throttler.WEBHOOK.timeWindow", "10s")

			throttler := NewAllEventTypesThrottler(destType, destinationID, mockAlgorithm, mockLimiter, config, statsStore, logger.NOP)

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
					metric.Tags["destType"] == destType {
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

			config.Set("Router.throttler.WEBHOOK.dest123.minLimit", 10)
			config.Set("Router.throttler.WEBHOOK.dest123.maxLimit", 100)
			config.Set("Router.throttler.WEBHOOK.timeWindow", "0s")

			throttler := NewAllEventTypesThrottler(destType, destinationID, mockAlgorithm, mockLimiter, config, statsStore, logger.NOP)

			// This should not call limiter due to not being enabled
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
		t.Run("PrioritizesSpecificDestinationConfig", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: true}
			mockAlgorithm := &MockAlgorithm{LimitFactorValue: 0.8}

			destType := "WEBHOOK"
			destinationID := "dest123"

			// Set both specific and fallback configs
			config.Set("Router.throttler.WEBHOOK.dest123.minLimit", 20)
			config.Set("Router.throttler.WEBHOOK.minLimit", 10)
			config.Set("Router.throttler.minLimit", 5)

			config.Set("Router.throttler.WEBHOOK.dest123.maxLimit", 200)
			config.Set("Router.throttler.WEBHOOK.maxLimit", 100)
			config.Set("Router.throttler.maxLimit", 50)

			throttler := NewAllEventTypesThrottler(destType, destinationID, mockAlgorithm, mockLimiter, config, statsStore, logger.NOP)

			require.Equal(t, int64(20), throttler.getMinLimit())
			require.Equal(t, int64(200), throttler.getMaxLimit())
		})

		t.Run("FallsBackToDestinationTypeConfig", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: true}
			mockAlgorithm := &MockAlgorithm{LimitFactorValue: 0.8}

			destType := "WEBHOOK"
			destinationID := "dest123"

			// Set only destination type and global configs
			config.Set("Router.throttler.WEBHOOK.minLimit", 15)
			config.Set("Router.throttler.minLimit", 5)

			config.Set("Router.throttler.WEBHOOK.maxLimit", 150)
			config.Set("Router.throttler.maxLimit", 50)

			throttler := NewAllEventTypesThrottler(destType, destinationID, mockAlgorithm, mockLimiter, config, statsStore, logger.NOP)

			require.Equal(t, int64(15), throttler.getMinLimit())
			require.Equal(t, int64(150), throttler.getMaxLimit())
		})

		t.Run("FallsBackToGlobalConfig", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: true}
			mockAlgorithm := &MockAlgorithm{LimitFactorValue: 0.8}

			destType := "WEBHOOK"
			destinationID := "dest123"

			// Set only global configs
			config.Set("Router.throttler.minLimit", 5)
			config.Set("Router.throttler.maxLimit", 50)

			throttler := NewAllEventTypesThrottler(destType, destinationID, mockAlgorithm, mockLimiter, config, statsStore, logger.NOP)

			require.Equal(t, int64(5), throttler.getMinLimit())
			require.Equal(t, int64(50), throttler.getMaxLimit())
		})
	})
}

// MockLimiter implements types.Limiter for testing
type MockLimiter struct {
	AllowResult bool
	AllowError  error
	CallLog     []MockLimiterCall
}

type MockLimiterCall struct {
	Cost   int64
	Rate   int64
	Window int64
	Key    string
}

func (m *MockLimiter) Allow(ctx context.Context, cost, rate, window int64, key string) (bool, func(context.Context) error, error) {
	m.CallLog = append(m.CallLog, MockLimiterCall{
		Cost:   cost,
		Rate:   rate,
		Window: window,
		Key:    key,
	})
	return m.AllowResult, func(context.Context) error { return nil }, m.AllowError
}

func (m *MockLimiter) Reset() {
	m.CallLog = nil
}

// MockAlgorithm implements Algorithm for testing
type MockAlgorithm struct {
	LimitFactorValue        float64
	ResponseCodes           []int
	ShutdownCalled          bool
	ResponseCodeReceivedLog []int
}

func (m *MockAlgorithm) ResponseCodeReceived(code int) {
	m.ResponseCodeReceivedLog = append(m.ResponseCodeReceivedLog, code)
}

func (m *MockAlgorithm) Shutdown() {
	m.ShutdownCalled = true
}

func (m *MockAlgorithm) LimitFactor() float64 {
	return m.LimitFactorValue
}

func (m *MockAlgorithm) Reset() {
	m.ResponseCodeReceivedLog = nil
	m.ShutdownCalled = false
}

// MockLogger implements Logger for testing
type MockLogger struct {
	WarningLogs []MockLogEntry
}

type MockLogEntry struct {
	Message string
	Fields  []logger.Field
}

func (m *MockLogger) Warnn(msg string, fields ...logger.Field) {
	m.WarningLogs = append(m.WarningLogs, MockLogEntry{
		Message: msg,
		Fields:  fields,
	})
}

func (m *MockLogger) Reset() {
	m.WarningLogs = nil
}
