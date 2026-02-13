package algorithm

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
	al := NewAdaptiveAlgorithm("dest", cfg, config.SingleValueLoader(500*time.Millisecond))
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

	t.Run("429s less than threshold", func(t *testing.T) {
		for range 10 {
			al.ResponseCodeReceived(200)
		}
		al.ResponseCodeReceived(429)

		require.Eventually(t, func() bool {
			require.False(t, floatCheck(al.LimitFactor(), float64(0.7)), "limit factor should not decrease")
			return floatCheck(al.LimitFactor(), float64(0.9)) // eventually increases by 10% since there is no error in the last 2 seconds
		}, 3*time.Second, 100*time.Millisecond)
	})

	t.Run("429s more than threshold", func(t *testing.T) {
		for range 4 {
			al.ResponseCodeReceived(200)
		}
		al.ResponseCodeReceived(429) // throttledRate is 1/5 = 0.2 > 0.1 (throttleTolerancePercentage)
		require.Eventually(t, func() bool {
			return floatCheck(al.LimitFactor(), 0.84) // reduces by 6% (30%*0.2)
		}, time.Second, 100*time.Millisecond) // does not change since 429s less than resolution
	})

	t.Run("should delay for few windows before decreasing again", func(t *testing.T) {
		cfg := config.New()
		cfg.Set("Router.throttler.adaptiveDecreaseRateDelay", 2)
		al := NewAdaptiveAlgorithm("dest", cfg, config.SingleValueLoader(500*time.Millisecond))
		defer al.Shutdown()
		al.ResponseCodeReceived(429)
		require.Eventually(t, func() bool {
			return floatCheck(al.LimitFactor(), float64(0.7))
		}, time.Second, 100*time.Millisecond) // reduces by 30% since there is an error in the last 1 second

		ctx := t.Context()
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
