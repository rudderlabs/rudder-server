package throttler

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/throttling"
)

func TestGateway_Throttler(t *testing.T) {
	var (
		workspaceId = "testID"
		conf        throttlingConfig
		eventLimit  = 100
		timeWindow  = 1
	)
	config.Set("RateLimit.eventLimit", eventLimit)
	config.Set("RateLimit.timeWindow", timeWindow)
	defer config.Reset()
	conf.readThrottlingConfig(workspaceId)
	require.Equal(t, conf.limit, int64(eventLimit))
	require.Equal(t, conf.window, time.Duration(timeWindow)*time.Minute)

	l, err := throttling.New(throttling.WithInMemoryGCRA(0))
	require.NoError(t, err)
	testThrottler := throttler{
		config:  conf,
		limiter: l,
	}

	for i := 0; i < eventLimit; i++ {
		_, err := testThrottler.checkLimitReached(context.TODO(), workspaceId, 1)
		require.NoError(t, err)
	}

	startTime := time.Now()
	var passed int
	for i := 0; i < 2*eventLimit; i++ {
		allowed, err := testThrottler.checkLimitReached(context.TODO(), workspaceId, 1)
		require.NoError(t, err)
		if allowed {
			passed++
		}
	}
	require.GreaterOrEqual(t, passed, eventLimit)
	require.Less(
		t, time.Since(startTime), time.Duration(int64(timeWindow)*int64(time.Second)),
		"we should've been able to make the required request in less than the window duration due to the burst setting",
	)
}

func TestGateway_Factory(t *testing.T) {
	var (
		workspaceId = "testID"
		eventLimit  = 100
		timeWindow  = 1
	)

	config.Set("RateLimit.eventLimit", eventLimit)
	config.Set("RateLimit.timeWindow", timeWindow)
	defer config.Reset()
	rateLimiter, err := New(stats.Default)
	require.NoError(t, err)
	require.NotNil(t, rateLimiter)

	for i := 0; i < eventLimit; i++ {
		_, err := rateLimiter.CheckLimitReached(context.TODO(), workspaceId, 1)
		require.NoError(t, err)
	}

	startTime := time.Now()
	var passed int
	for i := 0; i < 2*eventLimit; i++ {
		allowed, err := rateLimiter.CheckLimitReached(context.TODO(), workspaceId, 1)
		require.NoError(t, err)
		if allowed {
			passed++
		}
	}
	require.GreaterOrEqual(t, passed, eventLimit)
	require.Less(
		t, time.Since(startTime), time.Duration(int64(timeWindow)*int64(time.Second)),
		"we should've been able to make the required request in less than the window duration due to the burst setting",
	)
}

func Test_readThrottlingConfig(t *testing.T) {
	var (
		workspaceId = "testID"
		conf        throttlingConfig
		eventLimit  = 100
		timeWindow  = 1
	)
	config.Set("RateLimit.testID.eventLimit", eventLimit)
	config.Set("RateLimit.testID.timeWindow", timeWindow)
	defer config.Reset()
	conf.readThrottlingConfig(workspaceId)
	require.Equal(t, conf.limit, int64(eventLimit))
	require.Equal(t, conf.window, time.Duration(timeWindow)*time.Minute)
}
