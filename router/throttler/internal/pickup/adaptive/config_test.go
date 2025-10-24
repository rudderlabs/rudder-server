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
			adaptiveKey := "Router.throttler.WEBHOOK.timeWindow"
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
			adaptiveKey := "Router.throttler.WEBHOOK.timeWindow"
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

			loader := GetPerEventWindowConfig(config, destType, destinationID, eventType)

			require.Equal(t, 15*time.Second, loader.Load())
		})
	})
}
