package adaptivethrottlercounter

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
)

const float64EqualityThreshold = 1e-9

func TestAdaptiveRateLimit(t *testing.T) {
	cfg := config.New()
	al := New(cfg, config.SingleValueLoader(500*time.Millisecond))
	defer al.Shutdown()
	t.Run("when there is a 429s in the last decrease limit counter window", func(t *testing.T) {
		al.ResponseCodeReceived(429)
		require.Eventually(t, func() bool {
			return floatCheck(al.LimitFactor(), float64(0.7))
		}, time.Second, 100*time.Millisecond) // reduces by 30% since there is an error in the last 1 second
	})

	t.Run("when there are no 429s ins the last increase limit counter window", func(t *testing.T) {
		require.Eventually(t, func() bool {
			return floatCheck(al.LimitFactor(), float64(0.8))
		}, 2*time.Second, 100*time.Millisecond) // increases by 10% since there is no error in the last 2 seconds
	})

	t.Run("429s less than resolution", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			al.ResponseCodeReceived(200)
		}
		al.ResponseCodeReceived(429)
		require.True(t, floatCheck(al.LimitFactor(), float64(0.8))) // does not change since 429s less than resolution
	})

	t.Run("429s more than resolution", func(t *testing.T) {
		for i := 0; i < 5; i++ {
			al.ResponseCodeReceived(200)
		}
		al.ResponseCodeReceived(429)
		require.Eventually(t, func() bool {
			return floatCheck(al.LimitFactor(), float64(0.5))
		}, time.Second, 100*time.Millisecond) // does not change since 429s less than resolution
	})

	t.Run("should delay for few windows before decreasing again", func(t *testing.T) {
		cfg := config.New()
		cfg.Set("Router.throttler.adaptive.decreaseRateDelay", 2)
		al := New(cfg, config.SingleValueLoader(500*time.Millisecond))
		defer al.Shutdown()
		al.ResponseCodeReceived(429)
		require.Eventually(t, func() bool {
			return floatCheck(al.LimitFactor(), float64(0.7))
		}, time.Second, 100*time.Millisecond) // reduces by 30% since there is an error in the last 1 second

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go func(context.Context) {
			for {
				select {
				case <-ctx.Done():
					return
				default:
					al.ResponseCodeReceived(429)
				}
			}
		}(ctx)
		require.True(t, floatCheck(al.LimitFactor(), float64(0.7))) // does not change since there is a delay
		require.Eventually(t, func() bool {
			return floatCheck(al.LimitFactor(), float64(0.4))
		}, 2*time.Second, 100*time.Millisecond)
	})
}

func floatCheck(a, b float64) bool {
	return math.Abs(a-b) < float64EqualityThreshold
}
