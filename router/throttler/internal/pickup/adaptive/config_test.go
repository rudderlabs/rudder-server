package adaptive

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
)

func TestConfigFunctions(t *testing.T) {
	t.Run("GetAllEventsWindowConfig", func(t *testing.T) {
		t.Run("ReturnsCorrectValueLoader", func(t *testing.T) {
			config := config.New()
			destType := "WEBHOOK"
			destinationID := "dest123"

			loader := GetAllEventsWindowConfig(config, destType, destinationID)

			require.NotNil(t, loader)
			// Should return default value (1 second) when no config is set
			require.Equal(t, 1*time.Second, loader.Load())
		})

		t.Run("UsesSpecificDestinationConfig", func(t *testing.T) {
			config := config.New()
			destType := "WEBHOOK"
			destinationID := "dest123"

			// Set specific destination config
			specificKey := "Router.throttler.WEBHOOK.dest123.timeWindow"
			config.Set(specificKey, "5s")

			loader := GetAllEventsWindowConfig(config, destType, destinationID)

			require.Equal(t, 5*time.Second, loader.Load())
		})

		t.Run("FallsBackToDestinationTypeConfig", func(t *testing.T) {
			config := config.New()
			destType := "WEBHOOK"
			destinationID := "dest123"

			// Set destination type config (fallback)
			typeKey := "Router.throttler.WEBHOOK.timeWindow"
			config.Set(typeKey, "3s")

			loader := GetAllEventsWindowConfig(config, destType, destinationID)

			require.Equal(t, 3*time.Second, loader.Load())
		})

		t.Run("FallsBackToAdaptiveConfig", func(t *testing.T) {
			config := config.New()
			destType := "WEBHOOK"
			destinationID := "dest123"

			// Set adaptive config (final fallback)
			adaptiveKey := "Router.throttler.adaptive.timeWindow"
			config.Set(adaptiveKey, "2s")

			loader := GetAllEventsWindowConfig(config, destType, destinationID)

			require.Equal(t, 2*time.Second, loader.Load())
		})

		t.Run("PrioritizesMoreSpecificConfig", func(t *testing.T) {
			config := config.New()
			destType := "WEBHOOK"
			destinationID := "dest123"

			// Set multiple configs, more specific should win
			config.Set("Router.throttler.WEBHOOK.dest123.timeWindow", "10s")
			config.Set("Router.throttler.WEBHOOK.timeWindow", "5s")
			config.Set("Router.throttler.adaptive.timeWindow", "2s")

			loader := GetAllEventsWindowConfig(config, destType, destinationID)

			require.Equal(t, 10*time.Second, loader.Load())
		})
	})

	t.Run("GetPerEventWindowConfig", func(t *testing.T) {
		t.Run("ReturnsCorrectValueLoader", func(t *testing.T) {
			config := config.New()
			destType := "WEBHOOK"
			destinationID := "dest123"
			eventType := "track"

			loader := GetPerEventWindowConfig(config, destType, destinationID, eventType)

			require.NotNil(t, loader)
			// Should return default value (1 second) when no config is set
			require.Equal(t, 1*time.Second, loader.Load())
		})

		t.Run("UsesEventSpecificConfig", func(t *testing.T) {
			config := config.New()
			destType := "WEBHOOK"
			destinationID := "dest123"
			eventType := "track"

			// Set event-specific config (most specific)
			eventKey := "Router.throttler.WEBHOOK.dest123.track.timeWindow"
			config.Set(eventKey, "8s")

			loader := GetPerEventWindowConfig(config, destType, destinationID, eventType)

			require.Equal(t, 8*time.Second, loader.Load())
		})

		t.Run("FallsBackToDestinationConfig", func(t *testing.T) {
			config := config.New()
			destType := "WEBHOOK"
			destinationID := "dest123"
			eventType := "track"

			// Set destination config
			destKey := "Router.throttler.WEBHOOK.dest123.timeWindow"
			config.Set(destKey, "6s")

			loader := GetPerEventWindowConfig(config, destType, destinationID, eventType)

			require.Equal(t, 6*time.Second, loader.Load())
		})

		t.Run("FallsBackToDestinationTypeEventConfig", func(t *testing.T) {
			config := config.New()
			destType := "WEBHOOK"
			destinationID := "dest123"
			eventType := "track"

			// Set destination type + event config
			typeEventKey := "Router.throttler.WEBHOOK.track.timeWindow"
			config.Set(typeEventKey, "4s")

			loader := GetPerEventWindowConfig(config, destType, destinationID, eventType)

			require.Equal(t, 4*time.Second, loader.Load())
		})

		t.Run("FallsBackToDestinationTypeConfig", func(t *testing.T) {
			config := config.New()
			destType := "WEBHOOK"
			destinationID := "dest123"
			eventType := "track"

			// Set destination type config
			typeKey := "Router.throttler.WEBHOOK.timeWindow"
			config.Set(typeKey, "3s")

			loader := GetPerEventWindowConfig(config, destType, destinationID, eventType)

			require.Equal(t, 3*time.Second, loader.Load())
		})

		t.Run("FallsBackToAdaptiveConfig", func(t *testing.T) {
			config := config.New()
			destType := "WEBHOOK"
			destinationID := "dest123"
			eventType := "track"

			// Set adaptive config (final fallback)
			adaptiveKey := "Router.throttler.adaptive.timeWindow"
			config.Set(adaptiveKey, "2s")

			loader := GetPerEventWindowConfig(config, destType, destinationID, eventType)

			require.Equal(t, 2*time.Second, loader.Load())
		})

		t.Run("PrioritizesMoreSpecificConfig", func(t *testing.T) {
			config := config.New()
			destType := "WEBHOOK"
			destinationID := "dest123"
			eventType := "track"

			// Set all possible configs, most specific should win
			config.Set("Router.throttler.WEBHOOK.dest123.track.timeWindow", "15s")
			config.Set("Router.throttler.WEBHOOK.dest123.timeWindow", "10s")
			config.Set("Router.throttler.WEBHOOK.track.timeWindow", "8s")
			config.Set("Router.throttler.WEBHOOK.timeWindow", "5s")
			config.Set("Router.throttler.adaptive.timeWindow", "2s")

			loader := GetPerEventWindowConfig(config, destType, destinationID, eventType)

			require.Equal(t, 15*time.Second, loader.Load())
		})
	})

	t.Run("maxLimitFunc", func(t *testing.T) {
		t.Run("ReturnsMaxLimitWhenSet", func(t *testing.T) {
			config := config.New()
			destType := "WEBHOOK"
			destinationID := "dest123"
			maxLimitKeys := []string{"Router.throttler.WEBHOOK.dest123.maxLimit"}

			// Set max limit
			config.Set("Router.throttler.WEBHOOK.dest123.maxLimit", 500)

			limitFunc := maxLimitFunc(config, destType, destinationID, maxLimitKeys)

			require.Equal(t, int64(500), limitFunc())
		})

		t.Run("UsesStaticLimitWithMultiplier", func(t *testing.T) {
			config := config.New()
			destType := "WEBHOOK"
			destinationID := "dest123"
			maxLimitKeys := []string{"Router.throttler.WEBHOOK.dest123.maxLimit"}

			// Set static limit and multiplier
			config.Set("Router.throttler.WEBHOOK.dest123.limit", 100)
			config.Set("Router.throttler.adaptive.WEBHOOK.dest123.limitMultiplier", 2.0)

			limitFunc := maxLimitFunc(config, destType, destinationID, maxLimitKeys)

			require.Equal(t, int64(200), limitFunc()) // 100 * 2.0
		})

		t.Run("FallsBackToDestinationTypeStaticLimit", func(t *testing.T) {
			config := config.New()
			destType := "WEBHOOK"
			destinationID := "dest123"
			maxLimitKeys := []string{"Router.throttler.WEBHOOK.dest123.maxLimit"}

			// Set destination type static limit and default multiplier
			config.Set("Router.throttler.WEBHOOK.limit", 200)
			// Default multiplier is 1.5

			limitFunc := maxLimitFunc(config, destType, destinationID, maxLimitKeys)

			require.Equal(t, int64(300), limitFunc()) // 200 * 1.5
		})

		t.Run("FallsBackToDestinationTypeMultiplier", func(t *testing.T) {
			config := config.New()
			destType := "WEBHOOK"
			destinationID := "dest123"
			maxLimitKeys := []string{"Router.throttler.WEBHOOK.dest123.maxLimit"}

			// Set specific static limit and destination type multiplier
			config.Set("Router.throttler.WEBHOOK.dest123.limit", 150)
			config.Set("Router.throttler.adaptive.WEBHOOK.limitMultiplier", 3.0)

			limitFunc := maxLimitFunc(config, destType, destinationID, maxLimitKeys)

			require.Equal(t, int64(450), limitFunc()) // 150 * 3.0
		})

		t.Run("FallsBackToGlobalMultiplier", func(t *testing.T) {
			config := config.New()
			destType := "WEBHOOK"
			destinationID := "dest123"
			maxLimitKeys := []string{"Router.throttler.WEBHOOK.dest123.maxLimit"}

			// Set static limit and global multiplier
			config.Set("Router.throttler.WEBHOOK.dest123.limit", 80)
			config.Set("Router.throttler.adaptive.limitMultiplier", 4.0)

			limitFunc := maxLimitFunc(config, destType, destinationID, maxLimitKeys)

			require.Equal(t, int64(320), limitFunc()) // 80 * 4.0
		})

		t.Run("FallsBackToDefaultMaxLimit", func(t *testing.T) {
			config := config.New()
			destType := "WEBHOOK"
			destinationID := "dest123"
			maxLimitKeys := []string{"Router.throttler.WEBHOOK.dest123.maxLimit"}

			// No configs set, should use DefaultMaxLimit
			limitFunc := maxLimitFunc(config, destType, destinationID, maxLimitKeys)

			require.Equal(t, int64(DefaultMaxThrottlingLimit), limitFunc())
		})

		t.Run("UsesCustomDefaultMaxLimit", func(t *testing.T) {
			config := config.New()
			destType := "WEBHOOK"
			destinationID := "dest123"
			maxLimitKeys := []string{"Router.throttler.WEBHOOK.dest123.maxLimit"}

			// Set custom default max limit
			config.Set("Router.throttler.adaptive.defaultMaxLimit", 2000)

			limitFunc := maxLimitFunc(config, destType, destinationID, maxLimitKeys)

			require.Equal(t, int64(2000), limitFunc())
		})

		t.Run("IgnoresZeroStaticLimit", func(t *testing.T) {
			config := config.New()
			destType := "WEBHOOK"
			destinationID := "dest123"
			maxLimitKeys := []string{"Router.throttler.WEBHOOK.dest123.maxLimit"}

			// Set zero static limit (should be ignored)
			config.Set("Router.throttler.WEBHOOK.dest123.limit", 0)
			config.Set("Router.throttler.adaptive.limitMultiplier", 2.0)

			limitFunc := maxLimitFunc(config, destType, destinationID, maxLimitKeys)

			require.Equal(t, int64(DefaultMaxThrottlingLimit), limitFunc())
		})

		t.Run("IgnoresZeroMultiplier", func(t *testing.T) {
			config := config.New()
			destType := "WEBHOOK"
			destinationID := "dest123"
			maxLimitKeys := []string{"Router.throttler.WEBHOOK.dest123.maxLimit"}

			// Set static limit but zero multiplier (should be ignored)
			config.Set("Router.throttler.WEBHOOK.dest123.limit", 100)
			config.Set("Router.throttler.adaptive.limitMultiplier", 0)

			limitFunc := maxLimitFunc(config, destType, destinationID, maxLimitKeys)

			require.Equal(t, int64(DefaultMaxThrottlingLimit), limitFunc())
		})

		t.Run("PrioritizesMaxLimitOverStaticLimit", func(t *testing.T) {
			config := config.New()
			destType := "WEBHOOK"
			destinationID := "dest123"
			maxLimitKeys := []string{"Router.throttler.WEBHOOK.dest123.maxLimit"}

			// Set both max limit and static limit, max limit should win
			config.Set("Router.throttler.WEBHOOK.dest123.maxLimit", 300)
			config.Set("Router.throttler.WEBHOOK.dest123.limit", 100)
			config.Set("Router.throttler.adaptive.limitMultiplier", 5.0)

			limitFunc := maxLimitFunc(config, destType, destinationID, maxLimitKeys)

			require.Equal(t, int64(300), limitFunc()) // Should use max limit, not 100 * 5.0
		})

		t.Run("MultipleMaxLimitKeys", func(t *testing.T) {
			config := config.New()
			destType := "WEBHOOK"
			destinationID := "dest123"
			maxLimitKeys := []string{
				"Router.throttler.WEBHOOK.dest123.maxLimit",
				"Router.throttler.WEBHOOK.maxLimit",
				"Router.throttler.maxLimit",
			}

			// Set second priority key
			config.Set("Router.throttler.WEBHOOK.maxLimit", 750)

			limitFunc := maxLimitFunc(config, destType, destinationID, maxLimitKeys)

			require.Equal(t, int64(750), limitFunc())
		})

		t.Run("DynamicConfigChanges", func(t *testing.T) {
			config := config.New()
			destType := "WEBHOOK"
			destinationID := "dest123"
			maxLimitKeys := []string{"Router.throttler.WEBHOOK.dest123.maxLimit"}

			// Set initial static limit
			config.Set("Router.throttler.WEBHOOK.dest123.limit", 100)

			limitFunc := maxLimitFunc(config, destType, destinationID, maxLimitKeys)

			// Should use static limit * default multiplier (1.5)
			require.Equal(t, int64(150), limitFunc())

			// Change the multiplier
			config.Set("Router.throttler.adaptive.limitMultiplier", 3.0)

			// Should reflect the new multiplier
			require.Equal(t, int64(300), limitFunc())

			// Set max limit which should override everything
			config.Set("Router.throttler.WEBHOOK.dest123.maxLimit", 500)

			// Should use max limit now
			require.Equal(t, int64(500), limitFunc())
		})
	})
}
