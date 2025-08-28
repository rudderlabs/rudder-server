package delivery

import (
	"context"
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	kitsync "github.com/rudderlabs/rudder-go-kit/sync"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

// NewThrottler constructs a new static delivery throttler for a destination endpoint
func NewThrottler(destType, destinationID, endpointPath string, limiter Limiter, config *config.Config, stat stats.Stats, log logger.Logger) *throttler {
	return &throttler{
		destinationID: destinationID,
		endpointPath:  endpointPath,
		key:           "delivery:" + destinationID + ":" + endpointPath, // key is destinationID:endpointPath

		limiter: limiter,
		log:     log,
		limit: config.GetReloadableInt64Var(0, 1,
			fmt.Sprintf(`Router.throttler.delivery.%s.%s.%s.limit`, destType, destinationID, endpointPath),
			fmt.Sprintf(`Router.throttler.delivery.%s.%s.limit`, destType, endpointPath),
		),
		window: config.GetReloadableDurationVar(0, time.Second,
			fmt.Sprintf(`Router.throttler.delivery.%s.%s.%s.timeWindow`, destType, destinationID, endpointPath),
			fmt.Sprintf(`Router.throttler.delivery.%s.%s.timeWindow`, destType, endpointPath),
		),

		onceEveryGauge: kitsync.NewOnceEvery(time.Second),
		rateLimitGauge: stat.NewTaggedStat("delivery_throttling_rate_limit", stats.GaugeType, stats.Tags{
			"destType":      destType,
			"destinationId": destinationID,
			"endpointPath":  endpointPath,
		}),
		waitTimerSuccess: stat.NewTaggedStat("delivery_throttling_wait_seconds", stats.TimerType, stats.Tags{
			"destType":      destType,
			"destinationId": destinationID,
			"endpointPath":  endpointPath,
			"success":       "true",
		}),
		waitTimerFailure: stat.NewTaggedStat("delivery_throttling_wait_seconds", stats.TimerType, stats.Tags{
			"destType":      destType,
			"destinationId": destinationID,
			"endpointPath":  endpointPath,
			"success":       "false",
		}),
	}
}

type throttler struct {
	destinationID string
	endpointPath  string
	key           string

	limiter Limiter
	log     logger.Logger
	limit   config.ValueLoader[int64]
	window  config.ValueLoader[time.Duration]

	onceEveryGauge   *kitsync.OnceEvery
	rateLimitGauge   stats.Gauge
	waitTimerSuccess stats.Timer
	waitTimerFailure stats.Timer
}

func (t *throttler) Wait(ctx context.Context) (dur time.Duration, err error) {
	if !t.enabled() {
		return 0, nil
	}
	start := time.Now()
	defer func() {
		if err == nil {
			t.waitTimerSuccess.Since(start)
		} else {
			t.waitTimerFailure.Since(start)
		}
	}()
	t.updateGauges()
	var allowed bool
	var retryAfter time.Duration
	for !allowed {
		allowed, retryAfter, _, err = t.limiter.AllowAfter(ctx, 1, t.getLimit(), t.getTimeWindowInSeconds(), t.key)
		if err != nil {
			return time.Since(start), fmt.Errorf("throttling failed for %s: %w", t.key, err)
		}
		if !allowed {
			if err = misc.SleepCtx(ctx, retryAfter); err != nil {
				return time.Since(start), fmt.Errorf("throttling interrupted for %s: %w", t.key, err)
			}
		}
	}
	return time.Since(start), nil
}

func (t *throttler) enabled() bool {
	limit := t.limit.Load()
	window := t.window.Load()
	return limit > 0 && window > 0
}

func (t *throttler) getLimit() int64 {
	return t.limit.Load()
}

func (t *throttler) getTimeWindowInSeconds() int64 {
	return int64(t.window.Load().Seconds())
}

func (t *throttler) updateGauges() {
	t.onceEveryGauge.Do(func() {
		if window := t.getTimeWindowInSeconds(); window > 0 {
			t.rateLimitGauge.Gauge(t.getLimit() / window)
		}
	})
}
