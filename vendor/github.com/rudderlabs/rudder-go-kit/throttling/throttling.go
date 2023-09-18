package throttling

import (
	"context"
	_ "embed"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/rudderlabs/rudder-go-kit/stats"
)

/*
TODOs:
* guard against concurrency? according to benchmarks, Redis performs better if we have no more than 16 routines
  * see https://github.com/rudderlabs/redis-throttling-playground/blob/main/Benchmarks.md#best-concurrency-setting-with-sortedset---save-1-1-and---appendonly-yes
*/

var (
	//go:embed lua/gcra.lua
	gcraLua         string
	gcraRedisScript *redis.Script
	//go:embed lua/sortedset.lua
	sortedSetLua    string
	sortedSetScript *redis.Script
)

func init() {
	gcraRedisScript = redis.NewScript(gcraLua)
	sortedSetScript = redis.NewScript(sortedSetLua)
}

type redisSpeaker interface {
	redis.Scripter
	redisSortedSetRemover
}

type statsCollector interface {
	NewTaggedStat(name, statType string, tags stats.Tags) stats.Measurement
}

type Limiter struct {
	// for Redis configurations
	// a default redisSpeaker should always be provided for Redis configurations
	redisSpeaker redisSpeaker

	// for in-memory configurations
	gcra *gcra

	// other flags
	useGCRA   bool
	gcraBurst int64

	// metrics
	statsCollector statsCollector
}

func New(options ...Option) (*Limiter, error) {
	rl := &Limiter{}
	for i := range options {
		options[i](rl)
	}
	if rl.statsCollector == nil {
		rl.statsCollector = stats.Default
	}
	if rl.redisSpeaker != nil {
		return rl, nil
	}
	// Default to in-memory GCRA
	rl.gcra = &gcra{}
	rl.useGCRA = true
	return rl, nil
}

// Allow returns true if the limit is not exceeded, false otherwise.
func (l *Limiter) Allow(ctx context.Context, cost, rate, window int64, key string) (
	bool, func(context.Context) error, error,
) {
	if cost < 1 {
		return false, nil, fmt.Errorf("cost must be greater than 0")
	}
	if rate < 1 {
		return false, nil, fmt.Errorf("rate must be greater than 0")
	}
	if window < 1 {
		return false, nil, fmt.Errorf("window must be greater than 0")
	}
	if key == "" {
		return false, nil, fmt.Errorf("key must not be empty")
	}

	if l.redisSpeaker != nil {
		if l.useGCRA {
			defer l.getTimer(key, "redis-gcra", rate, window)()
			_, allowed, tr, err := l.redisGCRA(ctx, cost, rate, window, key)
			return allowed, tr, err
		}

		defer l.getTimer(key, "redis-sorted-set", rate, window)()
		_, allowed, tr, err := l.redisSortedSet(ctx, cost, rate, window, key)
		return allowed, tr, err
	}

	defer l.getTimer(key, "gcra", rate, window)()
	return l.gcraLimit(ctx, cost, rate, window, key)
}

func (l *Limiter) redisSortedSet(ctx context.Context, cost, rate, window int64, key string) (
	time.Duration, bool, func(context.Context) error, error,
) {
	res, err := sortedSetScript.Run(ctx, l.redisSpeaker, []string{key}, cost, rate, window).Result()
	if err != nil {
		return 0, false, nil, fmt.Errorf("could not run SortedSet Redis script: %v", err)
	}

	result, ok := res.([]interface{})
	if !ok {
		return 0, false, nil, fmt.Errorf("unexpected result from SortedSet Redis script of type %T: %v", res, res)
	}
	if len(result) != 2 {
		return 0, false, nil, fmt.Errorf("unexpected result from SortedSet Redis script of length %d: %+v", len(result), result)
	}

	t, ok := result[0].(int64)
	if !ok {
		return 0, false, nil, fmt.Errorf("unexpected result[0] from SortedSet Redis script of type %T: %v", result[0], result[0])
	}
	redisTime := time.Duration(t) * time.Microsecond

	members, ok := result[1].(string)
	if !ok {
		return redisTime, false, nil, fmt.Errorf("unexpected result[1] from SortedSet Redis script of type %T: %v", result[1], result[1])
	}
	if members == "" { // limit exceeded
		return redisTime, false, nil, nil
	}

	r := &sortedSetRedisReturn{
		key:     key,
		members: strings.Split(members, ","),
		remover: l.redisSpeaker,
	}
	return redisTime, true, r.Return, nil
}

func (l *Limiter) redisGCRA(ctx context.Context, cost, rate, window int64, key string) (
	time.Duration, bool, func(context.Context) error, error,
) {
	burst := rate
	if l.gcraBurst > 0 {
		burst = l.gcraBurst
	}
	res, err := gcraRedisScript.Run(ctx, l.redisSpeaker, []string{key}, burst, rate, window, cost).Result()
	if err != nil {
		return 0, false, nil, fmt.Errorf("could not run GCRA Redis script: %v", err)
	}

	result, ok := res.([]interface{})
	if !ok {
		return 0, false, nil, fmt.Errorf("unexpected result from GCRA Redis script of type %T: %v", res, res)
	}
	if len(result) != 5 {
		return 0, false, nil, fmt.Errorf("unexpected result from GCRA Redis scrip of length %d: %+v", len(result), result)
	}

	t, ok := result[0].(int64)
	if !ok {
		return 0, false, nil, fmt.Errorf("unexpected result[0] from GCRA Redis script of type %T: %v", result[0], result[0])
	}
	redisTime := time.Duration(t) * time.Microsecond

	allowed, ok := result[1].(int64)
	if !ok {
		return redisTime, false, nil, fmt.Errorf("unexpected result[1] from GCRA Redis script of type %T: %v", result[1], result[1])
	}
	if allowed < 1 { // limit exceeded
		return redisTime, false, nil, nil
	}

	r := &unsupportedReturn{}
	return redisTime, true, r.Return, nil
}

func (l *Limiter) gcraLimit(ctx context.Context, cost, rate, window int64, key string) (
	bool, func(context.Context) error, error,
) {
	burst := rate
	if l.gcraBurst > 0 {
		burst = l.gcraBurst
	}
	allowed, err := l.gcra.limit(ctx, key, cost, burst, rate, window)
	if err != nil {
		return false, nil, fmt.Errorf("could not limit: %w", err)
	}
	if !allowed {
		return false, nil, nil // limit exceeded
	}
	r := &unsupportedReturn{}
	return true, r.Return, nil
}

func (l *Limiter) getTimer(key, algo string, rate, window int64) func() {
	m := l.statsCollector.NewTaggedStat("throttling", stats.TimerType, stats.Tags{
		"key":    key,
		"algo":   algo,
		"rate":   strconv.FormatInt(rate, 10),
		"window": strconv.FormatInt(window, 10),
	})
	start := time.Now()
	return func() {
		m.Since(start)
	}
}
