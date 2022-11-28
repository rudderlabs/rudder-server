package router

import (
	"sync"

	"github.com/go-redis/redis/v8"

	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/internal/throttling"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/router/throttler"
	"github.com/rudderlabs/rudder-server/services/rsources"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/services/transientsource"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

const (
	throttlingAlgoTypeGoRate         = "gorate"
	throttlingAlgoTypeGCRA           = "gcra"
	throttlingAlgoTypeRedisGCRA      = "redis-gcra"
	throttlingAlgoTypeRedisSortedSet = "redis-sorted-set"
)

type limiter interface {
	CheckLimitReached(key string, cost int64) (limited bool, retErr error)
}

type Factory struct {
	Reporting        reporter
	Multitenant      tenantStats
	BackendConfig    backendconfig.BackendConfig
	RouterDB         jobsdb.MultiTenantJobsDB
	ProcErrorDB      jobsdb.JobsDB
	TransientSources transientsource.Service
	RsourcesService  rsources.JobService
	Logger           logger.Logger
	Stats            stats.Stats

	// Throttling
	throttlerFactoryOnce sync.Once
	throttlerFactory     *throttler.Factory
}

func (f *Factory) New(destination *backendconfig.DestinationT, identifier string) *HandleT {
	f.throttlerFactoryOnce.Do(f.initThrottlerFactory)

	r := &HandleT{
		Reporting:        f.Reporting,
		MultitenantI:     f.Multitenant,
		throttlerFactory: f.throttlerFactory,
	}
	destConfig := getRouterConfig(destination, identifier)
	r.Setup(f.BackendConfig, f.RouterDB, f.ProcErrorDB, destConfig, f.TransientSources, f.RsourcesService)
	return r
}

func (f *Factory) initThrottlerFactory() {
	var redisClient *redis.Client
	if config.IsSet("Router.throttler.redis.addr") {
		redisClient = redis.NewClient(&redis.Options{
			Addr:     config.GetString("Router.throttler.redis.addr", "localhost:6379"),
			Username: config.GetString("Router.throttler.redis.username", ""),
			Password: config.GetString("Router.throttler.redis.password", ""),
		})
	}

	var throttlingAlgorithm string
	config.RegisterStringConfigVariable(
		throttlingAlgoTypeGoRate, &throttlingAlgorithm, false, "Router.throttler.algorithm",
	)

	var (
		err  error
		l    *throttling.Limiter
		opts = []throttling.Option{
			throttling.WithStatsCollector(f.Stats),
		}
	)
	switch throttlingAlgorithm {
	case throttlingAlgoTypeGoRate:
		l, err = throttling.New(append(opts, throttling.WithGoRate())...)
	case throttlingAlgoTypeGCRA:
		l, err = throttling.New(append(opts, throttling.WithGCRA())...)
	case throttlingAlgoTypeRedisGCRA, throttlingAlgoTypeRedisSortedSet:
		if redisClient == nil {
			f.Logger.Errorf("Redis client is nil with algorithm %s", throttlingAlgorithm)
			return
		}
		if throttlingAlgorithm == throttlingAlgoTypeRedisGCRA {
			opts = append(opts, throttling.WithGCRA())
		}
		l, err = throttling.New(append(opts, throttling.WithRedisClient(redisClient))...)
	default:
		f.Logger.Errorf("Invalid throttling algorithm: %s", throttlingAlgorithm)
		return
	}
	if err != nil {
		f.Logger.Errorf("Failed to create throttler: %s", err)
		f.Stats.NewTaggedStat("init_throttler_factory_error", stats.CountType,
			stats.Tags{"algorithm": throttlingAlgorithm},
		).Increment()
		return
	}

	f.throttlerFactory = &throttler.Factory{Limiter: l}
}

type destinationConfig struct {
	name          string
	responseRules map[string]interface{}
	config        map[string]interface{}
	destinationID string
}

func getRouterConfig(destination *backendconfig.DestinationT, identifier string) destinationConfig {
	return destinationConfig{
		name:          destination.DestinationDefinition.Name,
		destinationID: identifier,
		config:        destination.DestinationDefinition.Config,
		responseRules: destination.DestinationDefinition.ResponseRules,
	}
}
