package throttler

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
)

func TestFactory(t *testing.T) {
	t.Run("when adaptive throttling is enabled", func(t *testing.T) {
		config := config.New()
		config.Set("Router.throttler.adaptive.enabled", true)
		maxLimit := int64(300)
		config.Set("Router.throttler.adaptive.maxLimit", maxLimit)
		config.Set("Router.throttler.adaptive.minLimit", int64(100))
		config.Set("Router.throttler.adaptive.timeWindow", time.Second)
		f, err := NewFactory(config, nil)
		require.NoError(t, err)
		defer f.Shutdown()
		ta := f.Get("destName", "destID")

		t.Run("when there is a 429s in the last decrease limit counter window", func(t *testing.T) {
			ta.ResponseCodeReceived(429)
			require.Eventually(t, func() bool {
				return floatCheck(ta.getLimit(), maxLimit*7/10) // reduces by 30% since there is an error in the last 1 second
			}, 2*time.Second, 100*time.Millisecond, "limit: %d, expectedLimit: %d", ta.getLimit(), maxLimit*7/10)
		})

		t.Run("when there are no 429s in the last increase limit counter window", func(t *testing.T) {
			require.Eventually(t, func() bool {
				return floatCheck(ta.getLimit(), maxLimit*8/10) // increases by 10% since there is no error in the last 2 seconds
			}, 4*time.Second, 100*time.Millisecond, "limit: %d, expectedLimit: %d", ta.getLimit(), maxLimit*8/10)
		})

		t.Run("adaptive rate limit back to disabled", func(t *testing.T) {
			config.Set("Router.throttler.adaptive.enabled", false)
			require.Eventually(t, func() bool {
				return floatCheck(ta.getLimit(), 0) // should be 0 since adaptive rate limiter is disabled
			}, 2*time.Second, 100*time.Millisecond, "limit: %d, expectedLimit: %d", ta.getLimit(), maxLimit*8/10)
			config.Set("Router.throttler.adaptive.enabled", true)
		})

		t.Run("adaptive rate limit back to enabled", func(t *testing.T) {
			config.Set("Router.throttler.adaptive.enabled", true)
			ta.ResponseCodeReceived(429)
			require.Eventually(t, func() bool {
				return floatCheck(ta.getLimit(), maxLimit*5/10) // reduces by 30% since there is an error in the last 1 second
			}, 2*time.Second, 100*time.Millisecond, "limit: %d, expectedLimit: %d", ta.getLimit(), maxLimit*5/10)
		})
	})

	t.Run("when throttlerV2 is false", func(t *testing.T) {
		config := config.New()
		config.Set("Router.throttlerV2.enabled", false)
		config.Set("Router.throttler.destName.timeWindow", time.Second)
		config.Set("Router.throttler.destName.limit", int64(100))
		f, err := NewFactory(config, nil)
		require.NoError(t, err)
		defer f.Shutdown()
		ta := f.Get("destName", "destID")
		require.Eventually(t, func() bool {
			return ta.getLimit() == int64(100)
		}, 2*time.Second, 100*time.Millisecond)

		config.Set("Router.throttler.adaptive.enabled", true)
		require.Eventually(t, func() bool {
			return ta.getLimit() == int64(0)
		}, 2*time.Second, 100*time.Millisecond)
	})

	t.Run("check stats when adaptive is enabled", func(t *testing.T) {
		maxLimit := int64(300)
		window := 2 * time.Second

		c := config.New()
		c.Set("Router.throttler.adaptive.enabled", true)
		c.Set("Router.throttler.adaptive.maxLimit", maxLimit)
		c.Set("Router.throttler.adaptive.minLimit", int64(100))
		c.Set("Router.throttler.adaptive.timeWindow", window)

		statsStore, err := memstats.New()
		require.NoError(t, err)

		f, err := NewFactory(c, statsStore)
		require.NoError(t, err)
		defer f.Shutdown()

		_ = f.Get("destName", "destID")

		require.EqualValues(t, maxLimit/getWindowInSecs(window), statsStore.Get("throttling_rate_limit", stats.Tags{
			"destinationId": "destID",
			"destType":      "destName",
			"adaptive":      "true",
		}).LastValue())
		require.EqualValues(t, 1, statsStore.Get("adaptive_throttler_limit_factor", stats.Tags{
			"destinationId": "destID",
			"destType":      "destName",
		}).LastValue())
	})

	t.Run("when no limit is set", func(t *testing.T) {
		conf := config.New()
		conf.Set("Router.throttler.adaptive.enabled", true)
		f, err := NewFactory(conf, nil)
		require.NoError(t, err)
		defer f.Shutdown()
		ta := f.Get("destName", "destID")
		require.EqualValues(t, adaptiveDefaultMaxLimit, ta.getLimit())
	})
}

func floatCheck(a, b int64) bool {
	return math.Abs(float64(a-b)) <= 1
}
