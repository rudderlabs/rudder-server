package throttler

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
)

func TestAdaptiveRateLimit(t *testing.T) {
	config := config.Default // TODO: change to config.New()
	config.Set("Router.throttler.adaptiveRateLimit.enabled", true)
	config.Set("Router.throttler.adaptiveRateLimit.shortTimeFrequency", 1*time.Second)
	config.Set("Router.throttler.adaptiveRateLimit.longTimeFrequency", 2*time.Second)
	f, err := New(nil, config)
	require.NoError(t, err)
	require.Equal(t, int64(250), f.Get("destName", "dest1").config.limit)

	t.Run("short timer", func(t *testing.T) {
		f.SetLimitReached("dest1")
		require.Eventually(t, func() bool {
			return int64(175) == f.Get("destName", "dest1").config.limit // reduces by 30% since there is an error in the last 1 second
		}, time.Second, 10*time.Millisecond)
	})

	t.Run("long timer", func(t *testing.T) {
		require.Eventually(t, func() bool {
			return int64(192) == f.Get("destName", "dest1").config.limit // increases by 10% since there is no error in the last 2 seconds
		}, 2*time.Second, 10*time.Millisecond)
	})

	t.Run("min limit", func(t *testing.T) {
		config.Set("Router.throttler.adaptiveRateLimit.destName.dest1.minLimit", 200)
		require.Eventually(t, func() bool {
			return int64(200) == f.Get("destName", "dest1").config.limit // will not reduce below 200
		}, 1*time.Second, 10*time.Millisecond)
	})

	t.Run("max limit", func(t *testing.T) {
		config.Set("Router.throttler.adaptiveRateLimit.destName.dest1.maxLimit", 210)
		require.Eventually(t, func() bool {
			return int64(210) == f.Get("destName", "dest1").config.limit // will not increase above 210
		}, 2*time.Second, 10*time.Millisecond)
	})

	t.Run("min change percentage", func(t *testing.T) {
		f.SetLimitReached("dest1")
		config.Set("Router.throttler.adaptiveRateLimit.destName.dest1.minLimit", 100)
		config.Set("Router.throttler.adaptiveRateLimit.destName.dest1.minChangePercentage", 50)
		require.Eventually(t, func() bool {
			return int64(105) == f.Get("destName", "dest1").config.limit // will reduce by 50% since there is an error in the last 1 second
		}, 1*time.Second, 10*time.Millisecond)
	})

	t.Run("max change percentage", func(t *testing.T) {
		config.Set("Router.throttler.adaptiveRateLimit.destName.dest1.maxChangePercentage", 20)
		require.Eventually(t, func() bool {
			return int64(126) == f.Get("destName", "dest1").config.limit // will increase by 20% since there is no error in the last 2 seconds
		}, 2*time.Second, 10*time.Millisecond)
	})

	t.Run("adaptive rate limit disabled", func(t *testing.T) {
		config.Set("Router.throttler.adaptiveRateLimit.enabled", false)
		f.SetLimitReached("dest1")
		require.Eventually(t, func() bool {
			return int64(126) == f.Get("destName", "dest1").config.limit // will not change since adaptive rate limiter is disabled
		}, 2*time.Second, 10*time.Millisecond)
		config.Set("Router.throttler.adaptiveRateLimit.enabled", true)
	})

	t.Run("destination id adaptive rate limit disabled", func(t *testing.T) {
		config.Set("Router.throttler.adaptiveRateLimit.destName.dest1.enabled", false)
		config.Set("Router.throttler.adaptiveRateLimit.destName.dest2.enabled", true)
		f.SetLimitReached("dest1")
		f.SetLimitReached("dest2")
		require.Eventually(t, func() bool {
			return int64(126) == f.Get("destName", "dest1").config.limit // will not change since adaptive rate limiter is disabled for destination
		}, 2*time.Second, 10*time.Millisecond)
		require.Eventually(t, func() bool {
			return int64(175) == f.Get("destName", "dest2").config.limit // will reduce by 30% since there is an error in the last 1 second
		}, time.Second, 10*time.Millisecond)
	})

	t.Run("destination name adaptive rate limit disabled", func(t *testing.T) {
		config.Set("Router.throttler.adaptiveRateLimit.destName.enabled", false)
		require.Eventually(t, func() bool {
			return int64(126) == f.Get("destName", "dest1").config.limit // will not change since adaptive rate limiter is disabled for destination
		}, 2*time.Second, 10*time.Millisecond)
		require.Eventually(t, func() bool {
			return int64(192) == f.Get("destName", "dest2").config.limit // will not change since adaptive rate limiter is disabled for destination
		}, 2*time.Second, 10*time.Millisecond)
	})
}
