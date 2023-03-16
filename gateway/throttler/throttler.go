package throttler

//go:generate mockgen -destination=../../mocks/gateway/throttler.go -package=mocks_gateway github.com/rudderlabs/rudder-server/gateway/throttler Throttler

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/internal/throttling"
	"github.com/rudderlabs/rudder-server/services/stats"
)

const (
	throttlingAlgoTypeGCRA = "gcra"
)

type Limiter interface {
	// Allow returns true if the limit is not exceeded, false otherwise.
	Allow(ctx context.Context, cost, rate, window int64, key string) (bool, func(context.Context) error, error)
}

type Throttler interface {
	CheckLimitReached(context context.Context, workspaceId string) (bool, error)
}

type Factory struct {
	Stats        stats.Stats
	limiter      Limiter
	throttlers   map[string]*throttler // map key is the workspaceId
	throttlersMu sync.Mutex
}

// New constructs a new Throttler Factory
func New(stats stats.Stats) (*Factory, error) {
	f := Factory{
		Stats:      stats,
		throttlers: make(map[string]*throttler),
	}
	if err := f.initThrottlerFactory(); err != nil {
		return nil, err
	}
	return &f, nil
}

func (f *Factory) CheckLimitReached(context context.Context, workspaceId string) (bool, error) {
	t := f.get(workspaceId)
	return t.checkLimitReached(context, workspaceId)
}

func (f *Factory) get(workspaceId string) *throttler {
	f.throttlersMu.Lock()
	defer f.throttlersMu.Unlock()
	if t, ok := f.throttlers[workspaceId]; ok {
		return t
	}

	var conf throttlingConfig
	conf.readThrottlingConfig(workspaceId)
	f.throttlers[workspaceId] = &throttler{
		limiter: f.limiter,
		config:  conf,
	}
	return f.throttlers[workspaceId]
}

func (f *Factory) initThrottlerFactory() error {
	throttlingAlgorithm := config.GetString("Gateway.throttler.algorithm", throttlingAlgoTypeGCRA)

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
	default:
		return fmt.Errorf("invalid throttling algorithm: %s", throttlingAlgorithm)
	}
	if err != nil {
		return fmt.Errorf("failed to create throttler: %w", err)
	}

	f.limiter = l

	return nil
}

type throttler struct {
	limiter Limiter
	config  throttlingConfig
}

// checkLimitReached returns true if we're not allowed to process the number of event
func (t *throttler) checkLimitReached(ctx context.Context, key string) (limited bool, retErr error) {
	allowed, _, err := t.limiter.Allow(ctx, 1, t.config.limit, getWindowInSecs(t.config.window), key)
	if err != nil {
		return false, fmt.Errorf("could not limit: %w", err)
	}
	if !allowed {
		return true, nil // no token to return when limited
	}
	return false, nil
}

type throttlingConfig struct {
	limit  int64
	window time.Duration
}

func (c *throttlingConfig) readThrottlingConfig(workspaceID string) {
	rateLimitKey := fmt.Sprintf("RateLimit.%s.eventLimit", workspaceID)
	if config.IsSet(rateLimitKey) {
		c.limit = config.GetInt64(rateLimitKey, 1000)
	} else {
		c.limit = config.GetInt64("RateLimit.eventLimit", 1000)
	}

	rateLimitWindowKey := fmt.Sprintf("RateLimit.%s.rateLimitWindow", workspaceID)
	if config.IsSet(rateLimitWindowKey) {
		c.window = config.GetDuration(rateLimitWindowKey, 60, time.Second)
	} else {
		c.window = config.GetDuration("RateLimit.rateLimitWindow", 60, time.Second)
	}
}

func getWindowInSecs(d time.Duration) int64 {
	return int64(d.Seconds())
}
