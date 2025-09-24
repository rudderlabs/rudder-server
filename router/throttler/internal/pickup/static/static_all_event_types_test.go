package static

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

func TestAllEventTypesThrottler(t *testing.T) {
	t.Run("NewAllEventTypesThrottler", func(t *testing.T) {
		t.Run("CreatesThrottlerWithCorrectConfiguration", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: true}

			destType := "WEBHOOK"
			destinationID := "dest123"

			// Set configuration
			config.Set("Router.throttler.WEBHOOK.dest123.limit", 100)
			config.Set("Router.throttler.WEBHOOK.dest123.timeWindow", "10s")

			throttler := NewAllEventTypesThrottler(destType, destinationID, mockLimiter, config, statsStore, logger.NOP)

			require.NotNil(t, throttler)
			require.Equal(t, destinationID, throttler.destinationID)
			require.Equal(t, mockLimiter, throttler.limiter)
			require.Equal(t, int64(100), throttler.getLimit())
			require.Equal(t, int64(10), throttler.GetLimitPerSecond())
		})

		t.Run("UsesConfigurationFallbacks", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: true}

			destType := "WEBHOOK"
			destinationID := "dest123"

			// Set only destination type config (fallback)
			config.Set("Router.throttler.WEBHOOK.limit", 50)
			config.Set("Router.throttler.WEBHOOK.timeWindow", "5s")

			throttler := NewAllEventTypesThrottler(destType, destinationID, mockLimiter, config, statsStore, logger.NOP)

			require.Equal(t, int64(50), throttler.getLimit())
			require.Equal(t, int64(10), throttler.GetLimitPerSecond())
		})

		t.Run("CreatesStatsGaugeWithCorrectTags", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: true}

			destType := "WEBHOOK"
			destinationID := "dest123"

			throttler := NewAllEventTypesThrottler(destType, destinationID, mockLimiter, config, statsStore, logger.NOP)

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

			config.Set("Router.throttler.WEBHOOK.dest123.limit", 100)
			config.Set("Router.throttler.WEBHOOK.dest123.timeWindow", "10s")

			throttler := NewAllEventTypesThrottler(destType, destinationID, mockLimiter, config, statsStore, logger.NOP)

			limited, err := throttler.CheckLimitReached(context.Background(), 1)

			require.NoError(t, err)
			require.False(t, limited)
			require.Len(t, mockLimiter.CallLog, 1)
			require.Equal(t, int64(1), mockLimiter.CallLog[0].Cost)
			require.Equal(t, int64(100), mockLimiter.CallLog[0].Rate)
			require.Equal(t, int64(10), mockLimiter.CallLog[0].Window)
			require.Equal(t, destinationID, mockLimiter.CallLog[0].Key)
		})

		t.Run("ReturnsLimitedWhenNotAllowed", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: false}

			destType := "WEBHOOK"
			destinationID := "dest123"

			config.Set("Router.throttler.WEBHOOK.dest123.limit", 100)
			config.Set("Router.throttler.WEBHOOK.dest123.timeWindow", "10s")

			throttler := NewAllEventTypesThrottler(destType, destinationID, mockLimiter, config, statsStore, logger.NOP)

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

			config.Set("Router.throttler.WEBHOOK.dest123.limit", 100)
			config.Set("Router.throttler.WEBHOOK.dest123.timeWindow", "10s")

			throttler := NewAllEventTypesThrottler(destType, destinationID, mockLimiter, config, statsStore, logger.NOP)

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

			// Invalid configuration - no limit or window set
			throttler := NewAllEventTypesThrottler(destType, destinationID, mockLimiter, config, statsStore, mockLogger)

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

			config.Set("Router.throttler.WEBHOOK.dest123.limit", 0)
			config.Set("Router.throttler.WEBHOOK.dest123.timeWindow", "10s")

			throttler := NewAllEventTypesThrottler(destType, destinationID, mockLimiter, config, statsStore, mockLogger)

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

			config.Set("Router.throttler.WEBHOOK.dest123.limit", 100)
			config.Set("Router.throttler.WEBHOOK.dest123.timeWindow", "0s")

			throttler := NewAllEventTypesThrottler(destType, destinationID, mockLimiter, config, statsStore, mockLogger)

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

			destType := "WEBHOOK"
			destinationID := "dest123"

			config.Set("Router.throttler.WEBHOOK.dest123.limit", 100)
			config.Set("Router.throttler.WEBHOOK.dest123.timeWindow", "10s")

			throttler := NewAllEventTypesThrottler(destType, destinationID, mockLimiter, config, statsStore, logger.NOP)

			require.True(t, throttler.enabled())
		})

		t.Run("ReturnsFalseForZeroLimit", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: true}

			destType := "WEBHOOK"
			destinationID := "dest123"

			config.Set("Router.throttler.WEBHOOK.dest123.limit", 0)
			config.Set("Router.throttler.WEBHOOK.dest123.timeWindow", "10s")

			throttler := NewAllEventTypesThrottler(destType, destinationID, mockLimiter, config, statsStore, logger.NOP)

			require.False(t, throttler.enabled())
		})

		t.Run("ReturnsFalseForZeroWindow", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: true}

			destType := "WEBHOOK"
			destinationID := "dest123"

			config.Set("Router.throttler.WEBHOOK.dest123.limit", 100)
			config.Set("Router.throttler.WEBHOOK.dest123.timeWindow", "0s")

			throttler := NewAllEventTypesThrottler(destType, destinationID, mockLimiter, config, statsStore, logger.NOP)

			require.False(t, throttler.enabled())
		})
	})

	t.Run("GetLimitPerSecond", func(t *testing.T) {
		t.Run("ReturnsConfiguredLimit", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: true}

			destType := "WEBHOOK"
			destinationID := "dest123"

			config.Set("Router.throttler.WEBHOOK.dest123.limit", 250)
			config.Set("Router.throttler.WEBHOOK.dest123.timeWindow", time.Second)

			throttler := NewAllEventTypesThrottler(destType, destinationID, mockLimiter, config, statsStore, logger.NOP)

			require.Equal(t, int64(250), throttler.GetLimitPerSecond())
		})

		t.Run("UpdatesWithConfigChanges", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: true}

			destType := "WEBHOOK"
			destinationID := "dest123"

			config.Set("Router.throttler.WEBHOOK.dest123.limit", 100)
			config.Set("Router.throttler.WEBHOOK.dest123.timeWindow", time.Second)

			throttler := NewAllEventTypesThrottler(destType, destinationID, mockLimiter, config, statsStore, logger.NOP)

			require.Equal(t, int64(100), throttler.GetLimitPerSecond())

			// Update config
			config.Set("Router.throttler.WEBHOOK.dest123.limit", 200)

			require.Equal(t, int64(200), throttler.GetLimitPerSecond())
		})

		t.Run("Rounds up", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: true}

			destType := "WEBHOOK"
			destinationID := "dest123"

			config.Set("Router.throttler.WEBHOOK.dest123.limit", 15)
			config.Set("Router.throttler.WEBHOOK.dest123.timeWindow", 2*time.Second)

			throttler := NewAllEventTypesThrottler(destType, destinationID, mockLimiter, config, statsStore, logger.NOP)

			require.Equal(t, int64(8), throttler.GetLimitPerSecond())
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

			config.Set("Router.throttler.WEBHOOK.dest123.timeWindow", "30s")

			throttler := NewAllEventTypesThrottler(destType, destinationID, mockLimiter, config, statsStore, logger.NOP)

			require.Equal(t, int64(30), throttler.getTimeWindowInSeconds())
		})

		t.Run("HandlesMinutes", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: true}

			destType := "WEBHOOK"
			destinationID := "dest123"

			config.Set("Router.throttler.WEBHOOK.dest123.timeWindow", "2m")

			throttler := NewAllEventTypesThrottler(destType, destinationID, mockLimiter, config, statsStore, logger.NOP)

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

			throttler := NewAllEventTypesThrottler(destType, destinationID, mockLimiter, config, statsStore, logger.NOP)

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

			throttler := NewAllEventTypesThrottler(destType, destinationID, mockLimiter, config, statsStore, logger.NOP)

			require.NotPanics(t, func() {
				throttler.Shutdown()
			})
		})
	})

	t.Run("GetEventType", func(t *testing.T) {
		t.Run("ReturnsAllForAllEventTypesThrottler", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: true}

			destType := "WEBHOOK"
			destinationID := "dest123"

			// Set valid configuration
			config.Set("Router.throttler.WEBHOOK.dest123.limit", 100)
			config.Set("Router.throttler.WEBHOOK.dest123.timeWindow", "10s")

			throttler := NewAllEventTypesThrottler(destType, destinationID, mockLimiter, config, statsStore, &MockLogger{})

			eventType := throttler.GetEventType()
			require.Equal(t, "all", eventType)
		})
	})

	t.Run("GetLastUsed", func(t *testing.T) {
		config := config.New()
		statsStore, err := memstats.New()
		require.NoError(t, err)
		mockLimiter := &MockLimiter{AllowResult: true}

		destType := "WEBHOOK"
		destinationID := "dest123"

		// Set valid configuration
		config.Set("Router.throttler.WEBHOOK.dest123.limit", 100)
		config.Set("Router.throttler.WEBHOOK.dest123.timeWindow", "10s")

		throttler := NewAllEventTypesThrottler(destType, destinationID, mockLimiter, config, statsStore, &MockLogger{})

		lastUsed := throttler.GetLastUsed()
		require.Zero(t, lastUsed, "Last used should be zero before any access")
		_, _ = throttler.CheckLimitReached(context.Background(), 1)
		lastUsed = throttler.GetLastUsed()
		require.NotZero(t, lastUsed, "Last used should be updated after access")
	})

	t.Run("updateGauges", func(t *testing.T) {
		t.Run("UpdatesRateLimitGauge", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: true}

			destType := "WEBHOOK"
			destinationID := "dest123"

			config.Set("Router.throttler.WEBHOOK.dest123.limit", 100)
			config.Set("Router.throttler.WEBHOOK.dest123.timeWindow", "10s")

			throttler := NewAllEventTypesThrottler(destType, destinationID, mockLimiter, config, statsStore, logger.NOP)

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

			config.Set("Router.throttler.WEBHOOK.dest123.limit", 100)
			config.Set("Router.throttler.WEBHOOK.dest123.timeWindow", "0s")

			throttler := NewAllEventTypesThrottler(destType, destinationID, mockLimiter, config, statsStore, logger.NOP)

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
		t.Run("PrioritizesSpecificDestinationConfig", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: true}

			destType := "WEBHOOK"
			destinationID := "dest123"

			// Set both specific and fallback configs
			config.Set("Router.throttler.WEBHOOK.dest123.limit", 200)
			config.Set("Router.throttler.WEBHOOK.limit", 100)
			config.Set("Router.throttler.WEBHOOK.dest123.timeWindow", "20s")
			config.Set("Router.throttler.WEBHOOK.timeWindow", "10s")

			throttler := NewAllEventTypesThrottler(destType, destinationID, mockLimiter, config, statsStore, logger.NOP)

			require.Equal(t, int64(200), throttler.getLimit())
			require.Equal(t, int64(20), throttler.getTimeWindowInSeconds())
			require.Equal(t, int64(10), throttler.GetLimitPerSecond())
		})

		t.Run("FallsBackToDestinationTypeConfig", func(t *testing.T) {
			config := config.New()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			mockLimiter := &MockLimiter{AllowResult: true}

			destType := "WEBHOOK"
			destinationID := "dest123"

			// Set only destination type config
			config.Set("Router.throttler.WEBHOOK.limit", 150)
			config.Set("Router.throttler.WEBHOOK.timeWindow", "15s")

			throttler := NewAllEventTypesThrottler(destType, destinationID, mockLimiter, config, statsStore, logger.NOP)

			require.Equal(t, int64(150), throttler.getLimit())
			require.Equal(t, int64(15), throttler.getTimeWindowInSeconds())
			require.Equal(t, int64(10), throttler.GetLimitPerSecond()) // 150/15 = 10
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

// MockLogger implements logger.Logger interface for testing
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
