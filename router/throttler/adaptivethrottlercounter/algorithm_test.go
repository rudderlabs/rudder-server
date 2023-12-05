package adaptivethrottlercounter

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

const float64EqualityThreshold = 1e-9

func TestAdaptiveRateLimit(t *testing.T) {
	t.Run("when there is a 429s in the last shortTimer frequency", func(t *testing.T) {
		config := config.New()
		al := New(config, misc.SingleValueLoader(1*time.Second))
		defer al.Shutdown()
		al.ResponseCodeReceived(429)
		require.True(t, floatCheck(al.LimitFactor(), float64(0.7))) // reduces by 30% since there is an error in the last 1 second
	})

	t.Run("when there are no 429s ins the last longTimer frequency", func(t *testing.T) {
		config := config.New()
		al := New(config, misc.SingleValueLoader(1*time.Second))
		defer al.Shutdown()
		require.True(t, floatCheck(al.LimitFactor(), float64(1.1))) // increases by 10% since there is no error in the last 2 seconds
	})

	t.Run("429s less than resolution", func(t *testing.T) {
		config := config.New()
		al := New(config, misc.SingleValueLoader(1*time.Second))
		defer al.Shutdown()
		for i := 0; i < 10; i++ {
			al.ResponseCodeReceived(200)
		}
		al.ResponseCodeReceived(429)
		require.True(t, floatCheck(al.LimitFactor(), float64(1.0))) // does not change since 429s less than resolution
	})

	t.Run("429s more than resolution", func(t *testing.T) {
		config := config.New()
		al := New(config, misc.SingleValueLoader(1*time.Second))
		defer al.Shutdown()
		for i := 0; i < 5; i++ {
			al.ResponseCodeReceived(200)
		}
		al.ResponseCodeReceived(429)
		require.True(t, floatCheck(al.LimitFactor(), float64(0.7))) // does not change since 429s less than resolution
	})

	t.Run("should delay for few windows before decreasing again", func(t *testing.T) {
		config := config.New()
		config.Set("Router.throttler.adaptive.decreaseRateDelay", 2)
		al := New(config, misc.SingleValueLoader(1*time.Second))
		defer al.Shutdown()
		al.ResponseCodeReceived(429)
		require.True(t, floatCheck(al.LimitFactor(), float64(0.7)))

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
		require.True(t, floatCheck(al.LimitFactor(), float64(1.0)))
		require.Eventually(t, func() bool {
			return floatCheck(al.LimitFactor(), float64(0.7))
		}, 4*time.Second, 100*time.Millisecond)
	})
}

func floatCheck(a, b float64) bool {
	return math.Abs(a-b) < float64EqualityThreshold
}
