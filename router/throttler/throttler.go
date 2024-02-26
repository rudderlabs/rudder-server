package throttler

import (
	"context"
	"time"
)

const (
	throttlingAlgoTypeGCRA           = "gcra"
	throttlingAlgoTypeRedisGCRA      = "redis-gcra"
	throttlingAlgoTypeRedisSortedSet = "redis-sorted-set"
)

type Throttler interface {
	CheckLimitReached(ctx context.Context, key string, cost int64) (limited bool, retErr error)
	ResponseCodeReceived(code int)
	Shutdown()
	getLimit() int64
	getTimeWindow() time.Duration
}

type limiter interface {
	// Allow returns true if the limit is not exceeded, false otherwise.
	Allow(ctx context.Context, cost, rate, window int64, key string) (bool, func(context.Context) error, error)
}

func getWindowInSecs(d time.Duration) int64 {
	return int64(d.Seconds())
}
