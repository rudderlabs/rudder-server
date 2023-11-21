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

const (
	throttlingAlgoTypeGCRA           = "gcra"
	throttlingAlgoTypeRedisGCRA      = "redis-gcra"
	throttlingAlgoTypeRedisSortedSet = "redis-sorted-set"
)

type limiter interface {
	// Allow returns true if the limit is not exceeded, false otherwise.
	Allow(ctx context.Context, cost, rate, window int64, key string) (bool, func(context.Context) error, error)
}

type Factory struct {
	Stats        stats.Stats
	limiter      limiter
	throttlers   map[string]*Throttler // map key is the destinationID
	throttlersMu sync.Mutex
	adaptive     *Adaptive
}

// New constructs a new Throttler Factory
func New(stats stats.Stats, config *config.Config) (*Factory, error) {
	f := Factory{
		Stats:      stats,
		throttlers: make(map[string]*Throttler),
	}
	if err := f.initThrottlerFactory(config); err != nil {
		return nil, err
	}
	return &f, nil
}

func (f *Factory) Get(destName, destID string) *Throttler {
	f.throttlersMu.Lock()
	defer f.throttlersMu.Unlock()
	if t, ok := f.throttlers[destID]; ok {
		if f.adaptive != nil {
			t.config.limit = f.adaptive.Limit(destName, destID, t.config.limit)
		}
		return t
	}

	var conf throttlingConfig
	conf.readThrottlingConfig(destName, destID)
	if f.adaptive != nil {
		conf.limit = f.adaptive.Limit(destName, destID, conf.limit)
	}
	f.throttlers[destID] = &Throttler{
		limiter: f.limiter,
		config:  conf,
	}
	return f.throttlers[destID]
}

func (f *Factory) SetLimitReached(destID string) {
	f.throttlersMu.Lock()
	defer f.throttlersMu.Unlock()
	f.adaptive.SetLimitReached(destID)
}

func (f *Factory) ShutDown() {
	if f.adaptive != nil {
		f.adaptive.ShutDown()
	}
}

func (f *Factory) initThrottlerFactory(config *config.Config) error {
	var redisClient *redis.Client
	if config.IsSet("Router.throttler.redis.addr") {
		redisClient = redis.NewClient(&redis.Options{
			Addr:     config.GetString("Router.throttler.redis.addr", "localhost:6379"),
			Username: config.GetString("Router.throttler.redis.username", ""),
			Password: config.GetString("Router.throttler.redis.password", ""),
		})
	}

	throttlingAlgorithm := throttlingAlgoTypeGCRA
	if config.GetBool("Router.throttler.adaptiveRateLimit.enabled", false) {
		f.adaptive = NewAdaptive(config)
	} else {
		throttlingAlgorithm = config.GetString("Router.throttler.algorithm", throttlingAlgoTypeGCRA)
		if throttlingAlgorithm == throttlingAlgoTypeRedisGCRA || throttlingAlgorithm == throttlingAlgoTypeRedisSortedSet {
			if redisClient == nil {
				return fmt.Errorf("redis client is nil with algorithm %s", throttlingAlgorithm)
			}
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
		return fmt.Errorf("failed to create throttler: %w", err)
	}

	f.limiter = l

	return nil
}

type Throttler struct {
	limiter limiter
	config  throttlingConfig
}

// CheckLimitReached returns true if we're not allowed to process the number of events we asked for with cost.
func (t *Throttler) CheckLimitReached(key string, cost int64) (limited bool, retErr error) {
	if !t.config.enabled {
		return false, nil
	}

	ctx := context.TODO()
	allowed, _, err := t.limiter.Allow(ctx, cost, t.config.limit, getWindowInSecs(t.config.window), key)
	if err != nil {
		return false, fmt.Errorf("could not limit: %w", err)
	}
	if !allowed {
		return true, nil // no token to return when limited
	}
	return false, nil
}

type throttlingConfig struct {
	enabled bool
	limit   int64
	window  time.Duration
}

func (c *throttlingConfig) readThrottlingConfig(destName, destID string) {
	if config.IsSet(fmt.Sprintf(`Router.throttler.%s.%s.limit`, destName, destID)) {
		c.limit = config.GetInt64(fmt.Sprintf(`Router.throttler.%s.%s.limit`, destName, destID), 0)
	} else {
		c.limit = config.GetInt64(fmt.Sprintf(`Router.throttler.%s.limit`, destName), 0)
	}

	if config.IsSet(fmt.Sprintf(`Router.throttler.%s.%s.timeWindow`, destName, destID)) {
		c.window = config.GetDuration(fmt.Sprintf(`Router.throttler.%s.%s.timeWindow`, destName, destID), 0, time.Second)
	} else {
		c.window = config.GetDuration(fmt.Sprintf(`Router.throttler.%s.timeWindow`, destName), 0, time.Second)
	}

	// enable dest throttler
	if c.limit > 0 && c.window > 0 {
		c.enabled = true
	}
}

func getWindowInSecs(d time.Duration) int64 {
	return int64(d.Seconds())
}
