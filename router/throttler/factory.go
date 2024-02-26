package throttler

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/throttling"
)

type Factory interface {
	Get(destName, destID string) Throttler
	Shutdown()
}

// NewFactory constructs a new Throttler Factory
func NewFactory(config *config.Config, stats stats.Stats) (Factory, error) {
	f := &factory{
		config:     config,
		Stats:      stats,
		throttlers: make(map[string]Throttler),
	}
	if err := f.initThrottlerFactory(); err != nil {
		return nil, err
	}
	return f, nil
}

type factory struct {
	config          *config.Config
	Stats           stats.Stats
	limiter         limiter
	adaptiveLimiter limiter
	throttlers      map[string]Throttler // map key is the destinationID
	throttlersMu    sync.Mutex
}

func (f *factory) Get(destName, destID string) Throttler {
	f.throttlersMu.Lock()
	defer f.throttlersMu.Unlock()
	defer func() {
		if f.Stats != nil {
			tags := stats.Tags{
				"destinationId": destID,
				"destType":      destName,
				"adaptive":      fmt.Sprintf("%t", f.throttlers[destID].(*switchingThrottler).adaptiveEnabled.Load()),
			}
			throttler := f.throttlers[destID]
			if window := getWindowInSecs(throttler.getTimeWindow()); window > 0 {
				f.Stats.NewTaggedStat("throttling_rate_limit", stats.GaugeType, tags).Gauge(throttler.getLimit() / window)
			}
		}
	}()
	if t, ok := f.throttlers[destID]; ok {
		return t
	}

	var conf staticThrottleConfig
	conf.readThrottlingConfig(f.config, destName, destID)
	st := &staticThrottler{
		limiter: f.limiter,
		config:  conf,
	}

	var at Throttler = &noOpThrottler{}
	if f.config.GetBool("Router.throttlerV2.enabled", true) {
		var adaptiveConf adaptiveThrottleConfig
		adaptiveConf.readThrottlingConfig(f.config, destName, destID)
		var limitFactorMeasurement stats.Measurement = nil
		if f.Stats != nil {
			limitFactorMeasurement = f.Stats.NewTaggedStat("adaptive_throttler_limit_factor", stats.GaugeType, stats.Tags{
				"destinationId": destID,
				"destType":      destName,
			})
		}
		at = &adaptiveThrottler{
			limiter:                f.adaptiveLimiter,
			algorithm:              newAdaptiveAlgorithm(f.config, adaptiveConf.window),
			config:                 adaptiveConf,
			limitFactorMeasurement: limitFactorMeasurement,
		}
	}

	f.throttlers[destID] = &switchingThrottler{
		adaptiveEnabled: f.config.GetReloadableBoolVar(false,
			fmt.Sprintf(`Router.throttler.adaptive.%s.%s.enabled`, destName, destID),
			fmt.Sprintf(`Router.throttler.adaptive.%s.enabled`, destName),
			"Router.throttler.adaptive.enabled"),
		static:   st,
		adaptive: at,
	}
	return f.throttlers[destID]
}

func (f *factory) Shutdown() {
	f.throttlersMu.Lock()
	defer f.throttlersMu.Unlock()
	for _, t := range f.throttlers {
		t.Shutdown()
	}
}

func (f *factory) initThrottlerFactory() error {
	var redisClient *redis.Client
	if f.config.IsSet("Router.throttler.redis.addr") {
		redisClient = redis.NewClient(&redis.Options{
			Addr:     f.config.GetString("Router.throttler.redis.addr", "localhost:6379"),
			Username: f.config.GetString("Router.throttler.redis.username", ""),
			Password: f.config.GetString("Router.throttler.redis.password", ""),
		})
	}

	throttlingAlgorithm := f.config.GetString("Router.throttler.limiter.type", throttlingAlgoTypeGCRA)
	if throttlingAlgorithm == throttlingAlgoTypeRedisGCRA || throttlingAlgorithm == throttlingAlgoTypeRedisSortedSet {
		if redisClient == nil {
			return fmt.Errorf("redis client is nil with algorithm %s", throttlingAlgorithm)
		}
	}

	var (
		err  error
		l    *throttling.Limiter
		opts []throttling.Option
	)
	if f.Stats != nil {
		opts = append(opts, throttling.WithStatsCollector(f.Stats))
	}
	switch throttlingAlgorithm {
	case throttlingAlgoTypeGCRA:
		l, err = throttling.New(append(opts, throttling.WithInMemoryGCRA(0))...)
	case throttlingAlgoTypeRedisGCRA:
		l, err = throttling.New(append(opts, throttling.WithRedisGCRA(redisClient, 0))...)
	case throttlingAlgoTypeRedisSortedSet:
		l, err = throttling.New(append(opts, throttling.WithRedisSortedSet(redisClient))...)
	default:
		return fmt.Errorf("invalid throttling algorithm: %s", throttlingAlgorithm)
	}
	if err != nil {
		return fmt.Errorf("create throttler: %w", err)
	}

	f.limiter = l

	al, err := throttling.New(append(opts, throttling.WithInMemoryGCRA(0))...)
	if err != nil {
		return fmt.Errorf("create adaptive throttler: %w", err)
	}
	f.adaptiveLimiter = al

	return nil
}

type NewNoOpFactory struct{}

func NewNoOpThrottlerFactory() Factory {
	return &NewNoOpFactory{}
}

func (f *NewNoOpFactory) Get(destName, destID string) Throttler {
	return &noOpThrottler{}
}

func (f *NewNoOpFactory) Shutdown() {}

type noOpThrottler struct{}

func (t *noOpThrottler) CheckLimitReached(ctx context.Context, key string, cost int64) (limited bool, retErr error) {
	return false, nil
}

func (t *noOpThrottler) ResponseCodeReceived(code int) {}

func (t *noOpThrottler) Shutdown() {}

func (t *noOpThrottler) getLimit() int64 {
	return 0
}

func (t *noOpThrottler) getTimeWindow() time.Duration {
	return 0
}
