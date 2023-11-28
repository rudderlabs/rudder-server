package adaptivethrottlercounter

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
)

const float64EqualityThreshold = 1e-9

func TestAdaptiveRateLimit(t *testing.T) {
	config := config.New()
	config.Set("Router.throttler.adaptive.shortTimeFrequency", 1*time.Second)
	config.Set("Router.throttler.adaptive.longTimeFrequency", 2*time.Second)
	al := New(config)

	t.Run("when there is a 429s in the last shortTimeFrequency", func(t *testing.T) {
		al.ResponseCodeReceived(429)
		require.Eventually(t, func() bool {
			return math.Abs(al.LimitFactor()-float64(-0.3)) < float64EqualityThreshold // reduces by 30% since there is an error in the last 1 second
		}, 2*time.Second, 10*time.Millisecond)
	})

	t.Run("when there are no 429s ins the last longTimeFrequency", func(t *testing.T) {
		require.Eventually(t, func() bool {
			return math.Abs(al.LimitFactor()-float64(+0.1)) < float64EqualityThreshold // increases by 10% since there is no error in the last 2 seconds
		}, 3*time.Second, 10*time.Millisecond)
	})
}
