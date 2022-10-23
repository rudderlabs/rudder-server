package throttling

import (
	"context"
	_ "embed"
	"fmt"
	"strings"
	"time"

	"github.com/go-redis/redis/v9"
)

/*
TODOs:
* finish test multiple redis clients
* metrics inside client (especially for returning tokens)
* benchmark AWS Elasticache Redis
* replace old limiters
* feature flags for algo type
* guard against concurrency?
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

type redisTalker interface {
	redis.Scripter
	redisSortedSetRemover
}

type TokenReturner interface {
	Return(context.Context) error
}

type Limiter struct {
	// for Redis configurations
	// a default redisTalker should always be provided for Redis configurations
	redisTalker redisTalker
	// redisToClientMap does not require a Mutex because it is only read from and written once in the constructor
	redisKeyToClientMap map[string]redisTalker

	// for in-memory configurations
	gcra   *gcra
	goRate *goRate

	// other flags
	useGCRA            bool
	useGCRABurstAsRate bool
	useGoRate          bool
}

func New(options ...Option) (*Limiter, error) {
	rl := &Limiter{}
	for i := range options {
		options[i].apply(rl)
	}
	if rl.redisTalker != nil {
		if rl.useGoRate {
			return nil, fmt.Errorf("redis and go-rate are mutually exclusive")
		}
		return rl, nil
	}
	if len(rl.redisKeyToClientMap) > 0 {
		return nil, fmt.Errorf("redis key to client map is configured without a default client")
	}

	switch {
	case rl.useGCRA:
		rl.gcra = &gcra{}
	default:
		rl.goRate = &goRate{}
	}
	return rl, nil
}

// Limit returns true if the limit is not exceeded, false otherwise.
func (l *Limiter) Limit(ctx context.Context, cost, rate, window int64, key string) (
	bool, TokenReturner, error,
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
	switch {
	case l.useGCRA:
		if l.redisTalker != nil {
			return l.redisGCRA(ctx, cost, rate, window, key)
		}
		return l.gcraLimit(ctx, cost, rate, window, key)
	case l.redisTalker != nil:
		return l.redisSortedSet(ctx, cost, rate, window, key)
	default:
		return l.goRateLimit(ctx, cost, rate, window, key)
	}
}

func (l *Limiter) redisSortedSet(ctx context.Context, cost, rate, window int64, key string) (
	bool, TokenReturner, error,
) {
	talker := l.getRedisTalker(key)
	res, err := sortedSetScript.Run(ctx, talker, []string{key}, cost, rate, window).Result()
	if err != nil {
		return false, nil, fmt.Errorf("could not run SortedSet Redis script: %v", err)
	}
	result, ok := res.([]interface{})
	if !ok {
		return false, nil, fmt.Errorf("unexpected result from SortedSet Redis script of type %T: %v", res, res)
	}
	if len(result) != 2 {
		return false, nil, fmt.Errorf("unexpected result from SortedSet Redis script of length %d: %+v", len(result), result)
	}
	t, ok := result[0].(int64)
	if !ok {
		return false, nil, fmt.Errorf("unexpected result[0] from SortedSet Redis script of type %T: %v", result[0], result[0])
	}
	members, ok := result[1].(string)
	if !ok {
		return false, nil, fmt.Errorf("unexpected result[1] from SortedSet Redis script of type %T: %v", result[1], result[1])
	}
	if members == "" { // limit exceeded
		return false, &redisTimerReturn{
			time: time.Duration(t) * time.Microsecond,
		}, nil
	}
	return true, &sortedSetRedisReturn{
		key:     key,
		members: strings.Split(members, ","),
		remover: talker,
		redisTimerReturn: redisTimerReturn{
			time: time.Duration(t) * time.Microsecond,
		},
	}, nil
}

func (l *Limiter) redisGCRA(ctx context.Context, cost, rate, window int64, key string) (bool, TokenReturner, error) {
	burst := int64(1)
	if l.useGCRABurstAsRate {
		burst = rate
	}
	talker := l.getRedisTalker(key)
	res, err := gcraRedisScript.Run(ctx, talker, []string{key}, burst, rate, window, cost).Result()
	if err != nil {
		return false, nil, fmt.Errorf("could not run GCRA Redis script: %v", err)
	}
	result, ok := res.([]interface{})
	if !ok {
		return false, nil, fmt.Errorf("unexpected result from GCRA Redis script of type %T: %v", res, res)
	}
	if len(result) != 5 {
		return false, nil, fmt.Errorf("unexpected result from GCRA Redis scrip of length %d: %+v", len(result), result)
	}
	t, ok := result[0].(int64)
	if !ok {
		return false, nil, fmt.Errorf("unexpected result[0] from GCRA Redis script of type %T: %v", result[0], result[0])
	}
	allowed, ok := result[1].(int64)
	if !ok {
		return false, nil, fmt.Errorf("unexpected result[1] from GCRA Redis script of type %T: %v", result[1], result[1])
	}
	if allowed < 1 { // limit exceeded
		return false, &redisTimerReturn{
			time: time.Duration(t) * time.Microsecond,
		}, nil
	}
	return true, &redisTimerReturn{
		time: time.Duration(t) * time.Microsecond,
	}, nil
}

func (l *Limiter) gcraLimit(_ context.Context, cost, rate, window int64, key string) (bool, TokenReturner, error) {
	burst := int64(1)
	if l.useGCRABurstAsRate {
		burst = rate
	}
	allowed, err := l.gcra.limit(key, cost, burst, rate, window)
	if err != nil {
		return false, nil, fmt.Errorf("could not limit: %w", err)
	}
	if !allowed {
		return false, nil, nil // limit exceeded
	}
	return true, &unsupportedReturn{}, nil
}

func (l *Limiter) goRateLimit(_ context.Context, cost, rate, window int64, key string) (bool, TokenReturner, error) {
	res := l.goRate.limit(key, cost, rate, window)
	if !res.Allowed() {
		res.CancelFuture()
		return false, nil, nil // limit exceeded
	}
	return true, &goRateReturn{reservation: res}, nil
}

func (l *Limiter) getRedisTalker(key string) redisTalker {
	if l.redisKeyToClientMap != nil {
		if client, ok := l.redisKeyToClientMap[key]; ok {
			return client
		}
	}
	return l.redisTalker
}
