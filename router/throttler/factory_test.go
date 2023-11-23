package throttler

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
)

func TestFactory(t *testing.T) {
	config := config.New()
	t.Run("when adaptive throttling is disabled", func(t *testing.T) {
		config.Set("Router.throttler.adaptiveRateLimit.enabled", false)
		ta := newAdaptiveAlgorithm(config)
		_, ok := ta.(*noopAdaptiveAlgorithm)
		require.True(t, ok)
	})

	t.Run("when adaptive throttling is enabled", func(t *testing.T) {
		config.Set("Router.throttler.adaptiveRateLimit.enabled", true)
		f, err := NewFactory(config, nil)
		require.NoError(t, err)
		defer f.Shutdown()
		ta := f.Get("destName", "destID")

		t.Run("when there is a 429s in the last shortTimeFrequency", func(t *testing.T) {
			config.Set("Router.throttler.adaptiveRateLimit.shortTimeFrequency", 1*time.Second)
			currentLimit := ta.getLimit()
			ta.ResponseCodeReceived(429)
			require.Eventually(t, func() bool {
				fmt.Println(ta.getLimit(), currentLimit)
				return float64(ta.getLimit()) == 0.7*float64(currentLimit) // reduces by 30% since there is an error in the last 1 second
			}, 2*time.Second, 10*time.Millisecond)
		})

		t.Run("when there are no 429s in the last longTimeFrequency", func(t *testing.T) {
			config.Set("Router.throttler.adaptiveRateLimit.longTimeFrequency", 2*time.Second)
			currentLimit := ta.getLimit()
			require.Eventually(t, func() bool {
				return float64(ta.getLimit()) == 1.1*float64(currentLimit) // increases by 10% since there is no error in the last 2 seconds
			}, 3*time.Second, 10*time.Millisecond)
		})

		t.Run("min limit", func(t *testing.T) {
			newLimit := ta.getLimit() * 9 / 10
			config.Set("Router.throttler.adaptiveRateLimit.destName.dest1.minLimit", newLimit)
			require.Eventually(t, func() bool {
				return newLimit == ta.getLimit() // will not reduce below newLimit
			}, 1*time.Second, 10*time.Millisecond)
		})

		t.Run("max limit", func(t *testing.T) {
			newLimit := ta.getLimit() * 105 / 100
			config.Set("Router.throttler.adaptiveRateLimit.destName.dest1.maxLimit", newLimit)
			require.Eventually(t, func() bool {
				return newLimit == ta.getLimit() // will not increase above newLimit
			}, 2*time.Second, 10*time.Millisecond)
		})

		t.Run("percentage decrease", func(t *testing.T) {
			currentLimit := ta.getLimit()
			config.Set("Router.throttler.adaptiveRateLimit.decreaseLimitPercentage", 50)
			require.Eventually(t, func() bool {
				return currentLimit*5/10 == ta.getLimit() // will reduce by 50% since there is an error in the last 1 second
			}, 1*time.Second, 10*time.Millisecond)
		})

		t.Run("percentage increase", func(t *testing.T) {
			currentLimit := ta.getLimit()
			config.Set("Router.throttler.adaptiveRateLimit.increaseLimitPercentage", 20)
			require.Eventually(t, func() bool {
				return currentLimit*6/5 == ta.getLimit() // will increase by 20% since there is no error in the last 2 seconds
			}, 2*time.Second, 10*time.Millisecond)
		})

		t.Run("adaptive rate limit back to disabled", func(t *testing.T) {
			config.Set("Router.throttler.adaptiveRateLimit.enabled", false)
			currentLimit := ta.getLimit()
			ta.ResponseCodeReceived(429)
			require.Eventually(t, func() bool {
				return currentLimit == ta.getLimit() // will not change since adaptive rate limiter is disabled
			}, 2*time.Second, 10*time.Millisecond)
			config.Set("Router.throttler.adaptiveRateLimit.enabled", true)
		})

		t.Run("adaptive rate limit back to enabled", func(t *testing.T) {
			config.Set("Router.throttler.adaptiveRateLimit.enabled", true)
			currentLimit := ta.getLimit()
			ta.ResponseCodeReceived(429)
			require.Eventually(t, func() bool {
				return float64(ta.getLimit()) == 0.7*float64(currentLimit) // reduces by 30% since there is an error in the last 1 second
			}, 2*time.Second, 10*time.Millisecond)
		})
	})
}
