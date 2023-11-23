package adaptivethrottlercounter

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
)

func TestAdaptiveRateLimit(t *testing.T) {
	config := config.New() // TODO: change to config.New()
	config.Set("Router.throttler.adaptiveRateLimit.shortTimeFrequency", 1*time.Second)
	config.Set("Router.throttler.adaptiveRateLimit.longTimeFrequency", 2*time.Second)
	al := New(config)

	t.Run("when there is a 429 in the last shortTimeFrequency", func(t *testing.T) {
		al.ResponseCodeReceived(429)
		require.Eventually(t, func() bool {
			return al.LimitFactor() == float64(-0.3) // reduces by 30% since there is an error in the last 1 second
		}, time.Second, 10*time.Millisecond)
	})

	t.Run("when there are no 429 ins the last longTimeFrequency", func(t *testing.T) {
		require.Eventually(t, func() bool {
			return al.LimitFactor() == float64(+0.1) // increases by 10% since there is no error in the last 2 seconds
		}, 2*time.Second, 10*time.Millisecond)
	})
}
