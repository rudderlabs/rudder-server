package throttler

//go:generate mockgen -destination=../../mocks/gateway/throttler.go -package=mocks_gateway github.com/rudderlabs/rudder-server/gateway/throttler GetThrottler

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

type GetThrottler interface {
	CheckLimitReached(workspaceId string) (bool, error)
}

type Factory struct {
	Stats        stats.Stats
	limiter      Limiter
	throttlers   map[string]*Throttler // map key is the workspaceId
	throttlersMu sync.Mutex
}

// New constructs a new Throttler Factory
func New(stats stats.Stats) (*Factory, error) {
	f := Factory{
		Stats:      stats,
		throttlers: make(map[string]*Throttler),
	}
	if err := f.initThrottlerFactory(); err != nil {
		return nil, err
	}
	return &f, nil
}

func (f *Factory) CheckLimitReached(workspaceId string) (bool, error) {
	t := f.get(workspaceId)
	return t.checkLimitReached(workspaceId)
}

func (f *Factory) get(workspaceId string) *Throttler {
	f.throttlersMu.Lock()
	defer f.throttlersMu.Unlock()
	if t, ok := f.throttlers[workspaceId]; ok {
		return t
	}

	var conf throttlingConfig
	conf.readThrottlingConfig(workspaceId)
	f.throttlers[workspaceId] = &Throttler{
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

type Throttler struct {
	limiter Limiter
	config  throttlingConfig
}

// checkLimitReached returns true if we're not allowed to process the number of event
func (t *Throttler) checkLimitReached(key string) (limited bool, retErr error) {
	ctx := context.TODO()
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
	if config.IsSet(fmt.Sprintf("RateLimit.%s.eventLimit", workspaceID)) {
		c.limit = config.GetInt64(fmt.Sprintf("RateLimit.%s.eventLimit", workspaceID), 1000)
	} else {
		c.limit = config.GetInt64("RateLimit.eventLimit", 1000)
	}

	if config.IsSet(fmt.Sprintf("RateLimit.%s.rateLimitWindow", workspaceID)) {
		c.window = config.GetDuration(fmt.Sprintf("RateLimit.%s.rateLimitWindow", workspaceID), 60, time.Minute)
	} else {
		c.window = config.GetDuration("RateLimit.rateLimitWindow", 60, time.Minute)
	}
}

func getWindowInSecs(d time.Duration) int64 {
	return int64(d.Seconds())
}
