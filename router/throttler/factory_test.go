package throttler

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
)

func TestFactory(t *testing.T) {
	config := config.New()

	t.Run("when adaptive throttling is enabled", func(t *testing.T) {
		config.Set("Router.throttler.adaptive.enabled", true)
		f, err := NewFactory(config, nil)
		require.NoError(t, err)
		defer f.Shutdown()
		ta := f.Get("destName", "destID")

		t.Run("when there is a 429s in the last shortTimer frequency", func(t *testing.T) {
			currentLimit := ta.getLimit()
			require.NotZero(t, currentLimit)
			ta.ResponseCodeReceived(429)
			require.Eventually(t, func() bool {
				return floatCheck(ta.getLimit(), currentLimit*7/10) // reduces by 30% since there is an error in the last 1 second
			}, 2*time.Second, 10*time.Millisecond, "limit: %d, expectedLimit: %d", ta.getLimit(), currentLimit*7/10)
		})

		t.Run("when there are no 429s in the last longTimer frequency", func(t *testing.T) {
			currentLimit := ta.getLimit()
			require.Eventually(t, func() bool {
				return floatCheck(ta.getLimit(), currentLimit*110/100) // increases by 10% since there is no error in the last 2 seconds
			}, 3*time.Second, 10*time.Millisecond, "limit: %d, expectedLimit: %d", ta.getLimit(), currentLimit*110/100)
		})

		t.Run("min limit", func(t *testing.T) {
			newLimit := ta.getLimit() * 9 / 10
			config.Set("Router.throttler.adaptive.destID.minLimit", newLimit)
			ta.ResponseCodeReceived(429)
			require.Eventually(t, func() bool {
				return floatCheck(newLimit, ta.getLimit()) // will not reduce below newLimit
			}, 2*time.Second, 10*time.Millisecond, "limit: %d, expectedLimit: %d", ta.getLimit(), newLimit)
		})

		t.Run("max limit", func(t *testing.T) {
			newLimit := ta.getLimit() * 105 / 100
			config.Set("Router.throttler.adaptive.destID.maxLimit", newLimit)
			require.Eventually(t, func() bool {
				return floatCheck(newLimit, ta.getLimit()) // will not increase above newLimit
			}, 3*time.Second, 10*time.Millisecond, "limit: %d, expectedLimit: %d", ta.getLimit(), newLimit)
		})

		t.Run("adaptive rate limit back to disabled", func(t *testing.T) {
			config.Set("Router.throttler.adaptive.enabled", false)
			currentLimit := ta.getLimit()
			ta.ResponseCodeReceived(429)
			require.Eventually(t, func() bool {
				return floatCheck(currentLimit, ta.getLimit()) // will not change since adaptive rate limiter is disabled
			}, 2*time.Second, 10*time.Millisecond, "limit: %d, expectedLimit: %d", ta.getLimit(), currentLimit)
			config.Set("Router.throttler.adaptive.enabled", true)
		})

		t.Run("adaptive rate limit back to enabled", func(t *testing.T) {
			config.Set("Router.throttler.adaptive.enabled", true)
			currentLimit := ta.getLimit()
			config.Set("Router.throttler.adaptive.destID.minLimit", currentLimit*4/10)
			config.Set("Router.throttler.adaptive.decreaseLimitPercentage", 30)
			ta.ResponseCodeReceived(429)
			require.Eventually(t, func() bool {
				return floatCheck(ta.getLimit(), currentLimit*7/10) // reduces by 30% since there is an error in the last 1 second
			}, 2*time.Second, 10*time.Millisecond, "limit: %d, expectedLimit: %d", ta.getLimit(), currentLimit*7/10)
		})
	})
}

func floatCheck(a, b int64) bool {
	return math.Abs(float64(a-b)) <= 1
}
