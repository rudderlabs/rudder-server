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
	t.Run("when there is a 429s in the last shortTimeFrequency", func(t *testing.T) {
		config := config.New()
		al := New(config, 1*time.Second)
		defer al.Shutdown()
		al.ResponseCodeReceived(429)
		require.Eventually(t, func() bool {
			return math.Abs(al.LimitFactor()-float64(0.7)) < float64EqualityThreshold // reduces by 30% since there is an error in the last 1 second
		}, 2*time.Second, 10*time.Millisecond)
	})

	t.Run("when there are no 429s ins the last longTimeFrequency", func(t *testing.T) {
		config := config.New()
		al := New(config, 1*time.Second)
		defer al.Shutdown()
		require.Eventually(t, func() bool {
			return math.Abs(al.LimitFactor()-float64(1.1)) < float64EqualityThreshold // increases by 10% since there is no error in the last 2 seconds
		}, 3*time.Second, 10*time.Millisecond)
	})

	t.Run("429s less than resolution", func(t *testing.T) {
		config := config.New()
		al := New(config, 1*time.Second)
		defer al.Shutdown()
		for i := 0; i < 10; i++ {
			al.ResponseCodeReceived(200)
		}
		al.ResponseCodeReceived(429)
		require.Eventually(t, func() bool {
			return math.Abs(al.LimitFactor()-float64(1.0)) < float64EqualityThreshold // does not change since 429s less than resolution
		}, 2*time.Second, 10*time.Millisecond)
	})

	t.Run("429s more than resolution", func(t *testing.T) {
		config := config.New()
		al := New(config, 1*time.Second)
		defer al.Shutdown()
		for i := 0; i < 5; i++ {
			al.ResponseCodeReceived(200)
		}
		al.ResponseCodeReceived(429)
		require.Eventually(t, func() bool {
			return math.Abs(al.LimitFactor()-float64(0.7)) < float64EqualityThreshold // does not change since 429s less than resolution
		}, 2*time.Second, 10*time.Millisecond)
	})
}
