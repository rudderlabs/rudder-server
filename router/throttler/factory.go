package throttler

import (
	"context"
	"fmt"
	"sync"

	"github.com/go-redis/redis/v8"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/throttling"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
	"github.com/rudderlabs/rudder-server/router/throttler/internal/adaptive"
	"github.com/rudderlabs/rudder-server/router/throttler/internal/adaptive/algorithm"
	"github.com/rudderlabs/rudder-server/router/throttler/internal/static"
	"github.com/rudderlabs/rudder-server/router/throttler/internal/switcher"
	"github.com/rudderlabs/rudder-server/router/throttler/internal/types"
)

const (
	throttlingAlgoTypeGCRA           = "gcra"
	throttlingAlgoTypeRedisGCRA      = "redis-gcra"
	throttlingAlgoTypeRedisSortedSet = "redis-sorted-set"
)

type Throttler = types.Throttler

type Factory interface {
	Get(destType, destID, eventType string) Throttler
	Shutdown()
}

// NewFactory constructs a new Throttler Factory
func NewFactory(config *config.Config, stats stats.Stats, log logger.Logger) (Factory, error) {
	f := &factory{
		config:             config,
		Stats:              stats,
		throttlers:         make(map[string]Throttler),
		allEventTypesAlgos: make(map[string]adaptive.Algorithm),
		log:                log,
	}
	if err := f.initThrottlerFactory(); err != nil {
		return nil, err
	}
	return f, nil
}

type factory struct {
	config          *config.Config
	log             logger.Logger
	Stats           stats.Stats
	staticLimiter   types.Limiter // limiter to use when static throttling is enabled
	adaptiveLimiter types.Limiter // limiter to use when adaptive throttling is enabled

	mu                 sync.RWMutex                  // protects the two maps below
	throttlers         map[string]Throttler          // map key is the destinationID:eventType
	allEventTypesAlgos map[string]adaptive.Algorithm // map key is the destinationID
}

func (f *factory) Get(destType, destinationID, eventType string) Throttler {
	key := destinationID + ":" + eventType
	// Use read lock first for common case
	f.mu.RLock()
	if t, ok := f.throttlers[key]; ok {
		f.mu.RUnlock()
		return t
	}
	f.mu.RUnlock()
	// Upgrade to write lock only when needed
	f.mu.Lock()
	defer f.mu.Unlock()
	// Double-check after acquiring write lock
	if t, ok := f.throttlers[key]; ok {
		return t
	}
	allEventsAlgorithm, ok := f.allEventTypesAlgos[destinationID]
	if !ok {
		allEventsAlgorithm = algorithm.NewAdaptiveAlgorithm(destType, f.config, adaptive.GetAllEventsWindowConfig(f.config, destType, destinationID))
		f.allEventTypesAlgos[destinationID] = allEventsAlgorithm
	}
	perEventAlgorithm := algorithm.NewAdaptiveAlgorithm(destType, f.config, adaptive.GetPerEventWindowConfig(f.config, destType, destinationID, eventType))
	adaptiveThrottlerEnabled := f.config.GetReloadableBoolVar(false,
		fmt.Sprintf(`Router.throttler.adaptive.%s.%s.enabled`, destType, destinationID),
		fmt.Sprintf(`Router.throttler.adaptive.%s.enabled`, destType),
		"Router.throttler.adaptive.enabled")

	log := f.log.Withn(
		obskit.DestinationType(destType),
		obskit.DestinationID(destinationID),
		logger.NewStringField("eventType", eventType),
	)
	// switching between static and adaptive throttling
	t := switcher.NewThrottlerSwitcher(
		adaptiveThrottlerEnabled,
		static.NewThrottler(destType, destinationID, eventType, f.staticLimiter, f.config, f.Stats, log.Withn(logger.NewStringField("throttlerType", "static"))),
		adaptive.NewThrottler(destType, destinationID, eventType, perEventAlgorithm, allEventsAlgorithm, f.adaptiveLimiter, f.config, f.Stats, log.Withn(logger.NewStringField("throttlerType", "adaptive"))),
	)
	f.throttlers[key] = t
	return t
}

func (f *factory) Shutdown() {
	f.mu.Lock()
	defer f.mu.Unlock()
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
		err           error
		staticLimiter *throttling.Limiter
		opts          []throttling.Option
	)
	if f.Stats != nil {
		opts = append(opts, throttling.WithStatsCollector(f.Stats))
	}
	switch throttlingAlgorithm {
	case throttlingAlgoTypeGCRA:
		staticLimiter, err = throttling.New(append(opts, throttling.WithInMemoryGCRA(0))...)
	case throttlingAlgoTypeRedisGCRA:
		staticLimiter, err = throttling.New(append(opts, throttling.WithRedisGCRA(redisClient, 0))...)
	case throttlingAlgoTypeRedisSortedSet:
		staticLimiter, err = throttling.New(append(opts, throttling.WithRedisSortedSet(redisClient))...)
	default:
		return fmt.Errorf("invalid throttling algorithm: %s", throttlingAlgorithm)
	}
	if err != nil {
		return fmt.Errorf("create throttler: %w", err)
	}

	f.staticLimiter = staticLimiter

	adaptiveLimiter, err := throttling.New(append(opts, throttling.WithInMemoryGCRA(0))...)
	if err != nil {
		return fmt.Errorf("create adaptive throttler: %w", err)
	}
	f.adaptiveLimiter = adaptiveLimiter

	return nil
}

type NewNoOpFactory struct{}

func NewNoOpThrottlerFactory() Factory {
	return &NewNoOpFactory{}
}

func (f *NewNoOpFactory) Get(destName, destID, eventType string) Throttler {
	return &noOpThrottler{}
}

func (f *NewNoOpFactory) Shutdown() {}

type noOpThrottler struct{}

func (t *noOpThrottler) CheckLimitReached(ctx context.Context, cost int64) (limited bool, retErr error) {
	return false, nil
}

func (t *noOpThrottler) ResponseCodeReceived(code int) {}

func (t *noOpThrottler) Shutdown() {}

func (t *noOpThrottler) GetLimit() int64 {
	return 0
}

func (t *noOpThrottler) UpdateRateLimitGauge() {}
