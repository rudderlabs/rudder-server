package throttler

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/throttling"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
	"github.com/rudderlabs/rudder-server/router/throttler/internal/delivery"
	"github.com/rudderlabs/rudder-server/router/throttler/internal/pickup/adaptive"
	"github.com/rudderlabs/rudder-server/router/throttler/internal/pickup/adaptive/algorithm"
	"github.com/rudderlabs/rudder-server/router/throttler/internal/pickup/static"
	"github.com/rudderlabs/rudder-server/router/throttler/internal/pickup/switcher"
	"github.com/rudderlabs/rudder-server/router/throttler/internal/types"
)

const (
	throttlingAlgoTypeGCRA           = "gcra"
	throttlingAlgoTypeRedisGCRA      = "redis-gcra"
	throttlingAlgoTypeRedisSortedSet = "redis-sorted-set"
)

type (
	PickupThrottler   = types.PickupThrottler
	DeliveryThrottler = types.DeliveryThrottler
)

type Factory interface {
	// GetPickupThrottler returns a PickupThrottler for the given destination type, ID, and event type.
	GetPickupThrottler(destType, destID, eventType string) PickupThrottler
	// GetActivePickupThrottlers returns all instantiated PickupThrottlers for the given destination ID.
	GetActivePickupThrottlers(destinationID string) []PickupThrottler
	// GetDeliveryThrottler returns a DeliveryThrottler for the given destination type, ID, and endpoint path.
	GetDeliveryThrottler(destType, destID, endpointPath string) DeliveryThrottler
	// Shutdown gracefully shuts down the factory and all its throttlers.
	Shutdown()
}

// NewFactory constructs a new Throttler Factory
func NewFactory(config *config.Config, stats stats.Stats, log logger.Logger) (Factory, error) {
	f := &factory{
		config:                   config,
		Stats:                    stats,
		pickupThrottlers:         &pickupThrottlers{all: make(map[string]map[string]PickupThrottler)},
		allEventTypesPickupAlgos: make(map[string]adaptive.Algorithm),
		deliveryThrottlers:       make(map[string]DeliveryThrottler),
		log:                      log,
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
	staticLimiter   limiter // limiter to use when static throttling is enabled
	adaptiveLimiter limiter // limiter to use when adaptive throttling is enabled

	mu                       sync.RWMutex                  // protects the resources below
	pickupThrottlers         *pickupThrottlers             // map key is the destinationID:eventType
	allEventTypesPickupAlgos map[string]adaptive.Algorithm // map key is the destinationID
	deliveryThrottlers       map[string]DeliveryThrottler  // map key is the destinationID:endpointPath
}

func (f *factory) GetPickupThrottler(destType, destinationID, eventType string) PickupThrottler {
	// Use read lock first for common case
	f.mu.RLock()
	if t, ok := f.pickupThrottlers.Get(destinationID, eventType); ok {
		f.mu.RUnlock()
		return t
	}
	f.mu.RUnlock()
	// Upgrade to write lock only when needed
	f.mu.Lock()
	defer f.mu.Unlock()
	// Double-check after acquiring write lock
	if t, ok := f.pickupThrottlers.Get(destinationID, eventType); ok {
		return t
	}
	allEventsAlgorithm, ok := f.allEventTypesPickupAlgos[destinationID]
	if !ok {
		allEventsAlgorithm = algorithm.NewAdaptiveAlgorithm(destType, f.config, adaptive.GetAllEventsWindowConfig(f.config, destType, destinationID))
		f.allEventTypesPickupAlgos[destinationID] = allEventsAlgorithm
	}
	perEventAlgorithm := algorithm.NewAdaptiveAlgorithm(destType, f.config, adaptive.GetPerEventWindowConfig(f.config, destType, destinationID, eventType))
	adaptiveThrottlerEnabled := f.config.GetReloadableBoolVar(false,
		fmt.Sprintf(`Router.throttler.%s.%s.adaptiveEnabled`, destType, destinationID),
		fmt.Sprintf(`Router.throttler.%s.adaptiveEnabled`, destType),
		"Router.throttler.adaptiveEnabled",
		// TODO: delete the following deprecated keys in the future
		fmt.Sprintf(`Router.throttler.adaptive.%s.%s.enabled`, destType, destinationID),
		fmt.Sprintf(`Router.throttler.adaptive.%s.enabled`, destType),
		"Router.throttler.adaptive.enabled")

	log := f.log.Withn(
		obskit.DestinationType(destType),
		obskit.DestinationID(destinationID),
		logger.NewStringField("eventType", eventType),
		logger.NewStringField("throttlerKind", "pickup"),
	)
	// switching between static and adaptive throttling
	t := switcher.NewThrottlerSwitcher(
		adaptiveThrottlerEnabled,
		static.NewThrottler(destType, destinationID, eventType, f.staticLimiter, f.config, f.Stats, log.Withn(logger.NewStringField("throttlerType", "static"))),
		adaptive.NewThrottler(destType, destinationID, eventType, perEventAlgorithm, allEventsAlgorithm, f.adaptiveLimiter, f.config, f.Stats, log.Withn(logger.NewStringField("throttlerType", "adaptive"))),
	)
	f.pickupThrottlers.Set(destinationID, eventType, t)
	return t
}

func (f *factory) GetActivePickupThrottlers(destinationID string) []PickupThrottler {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if destThrottlers, ok := f.pickupThrottlers.all[destinationID]; ok {
		return lo.Values(destThrottlers)
	}
	return nil
}

func (f *factory) GetDeliveryThrottler(destType, destinationID, endpointPath string) DeliveryThrottler {
	key := destinationID + ":" + endpointPath
	// Use read lock first for common case
	f.mu.RLock()
	if t, ok := f.deliveryThrottlers[key]; ok {
		f.mu.RUnlock()
		return t
	}
	f.mu.RUnlock()
	// Upgrade to write lock only when needed
	f.mu.Lock()
	defer f.mu.Unlock()
	// Double-check after acquiring write lock
	if t, ok := f.deliveryThrottlers[key]; ok {
		return t
	}

	log := f.log.Withn(
		obskit.DestinationType(destType),
		obskit.DestinationID(destinationID),
		logger.NewStringField("endpointPath", endpointPath),
		logger.NewStringField("throttlerKind", "delivery"),
	)
	// delivery throttler shall be using the static limiter exclusively (redis or in-memory)
	t := delivery.NewThrottler(destType, destinationID, endpointPath, f.staticLimiter, f.config, f.Stats, log)
	f.deliveryThrottlers[key] = t
	return t
}

func (f *factory) Shutdown() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, m := range f.pickupThrottlers.all {
		for _, t := range m {
			t.Shutdown()
		}
	}
}

func (f *factory) initThrottlerFactory() error {
	var redisClient *redis.Client
	if f.config.IsSet("Router.throttler.redisThrottler.addr") ||
		// TODO: remove the following deprecated key in the future
		f.config.IsSet("Router.throttler.redis.addr") {
		redisClient = redis.NewClient(&redis.Options{
			Addr: f.config.GetStringVar("localhost:6379",
				"Router.throttler.redisThrottler.addr",
				"Router.throttler.redis.addr", // TODO: remove this deprecated key in the future
			),
			Username: f.config.GetStringVar("",
				"Router.throttler.redisThrottler.username",
				"Router.throttler.redis.username", // TODO: remove this deprecated key in the future
			),
			Password: f.config.GetStringVar("",
				"Router.throttler.redisThrottler.password",
				"Router.throttler.redis.password", // TODO: remove this deprecated key in the future
			),
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

func (f *NewNoOpFactory) GetPickupThrottler(destName, destID, eventType string) PickupThrottler {
	return &noOpThrottler{}
}

func (f *NewNoOpFactory) GetActivePickupThrottlers(destinationID string) []PickupThrottler {
	return nil
}

func (f *NewNoOpFactory) GetDeliveryThrottler(destType, destID, endpointPath string) DeliveryThrottler {
	return &noOpDeliveryThrottler{}
}

func (f *NewNoOpFactory) Shutdown() {}

type noOpThrottler struct{}

func (t *noOpThrottler) CheckLimitReached(ctx context.Context, cost int64) (limited bool, retErr error) {
	return false, nil
}

func (t *noOpThrottler) ResponseCodeReceived(code int) {}

func (t *noOpThrottler) Shutdown() {}

func (t *noOpThrottler) GetLimitPerSecond() int64 {
	return 0
}

func (t *noOpThrottler) GetEventType() string {
	return "all"
}

func (t *noOpThrottler) GetLastUsed() time.Time {
	return time.Time{}
}

type noOpDeliveryThrottler struct{}

func (*noOpDeliveryThrottler) Wait(ctx context.Context) (time.Duration, error) {
	return 0, nil
}

type limiter interface {
	Allow(ctx context.Context, cost, rate, window int64, key string) (bool, func(context.Context) error, error)
	AllowAfter(ctx context.Context, cost, rate, window int64, key string) (bool, time.Duration, func(context.Context) error, error)
}

type pickupThrottlers struct {
	all map[string]map[string]PickupThrottler
}

func (p *pickupThrottlers) Get(destID, eventType string) (PickupThrottler, bool) {
	if destThrottlers, ok := p.all[destID]; ok {
		t, ok := destThrottlers[eventType]
		return t, ok
	}
	return nil, false
}

func (p *pickupThrottlers) Set(destID, eventType string, throttler PickupThrottler) {
	var destThrottlers map[string]PickupThrottler
	var ok bool
	if destThrottlers, ok = p.all[destID]; !ok {
		destThrottlers = make(map[string]PickupThrottler)
		p.all[destID] = destThrottlers
	}
	destThrottlers[eventType] = throttler
}
