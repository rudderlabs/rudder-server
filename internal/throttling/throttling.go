package throttling

import (
	"context"
	_ "embed"
	"fmt"
	"strings"

	"github.com/go-redis/redis/v9"
)

/*
TODOs:
* support for multiple clients (for org level limits)
* generic API to be able to switch from GCRA to custom implementation seamlessly
* metrics inside client (especially for returning tokens)
* benchmark AWS Elasticache Redis
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

type TokenReturner interface {
	Return(context.Context) error
}

type Limiter struct {
	// for Redis configurations
	redisScripter         redis.Scripter
	redisSortedSetRemover redisSortedSetRemover

	// for in-memory configurations
	gcra      *gcra
	sortedSet *sortedSet
	goRate    *goRate

	// other flags
	useGCRA               bool
	useGCRABurstAsRate    bool
	useInMemorySortedSets bool
	useGoRate             bool
}

func New(options ...Option) (*Limiter, error) {
	rl := &Limiter{}
	for i := range options {
		options[i].apply(rl)
	}
	if rl.redisScripter != nil {
		if rl.useGoRate || rl.useInMemorySortedSets {
			return nil, fmt.Errorf("cannot use Redis client with go rate or in-memory sorted sets")
		}
		return rl, nil
	}

	switch {
	case rl.useInMemorySortedSets:
		rl.sortedSet = &sortedSet{}
	case rl.useGCRA:
		rl.gcra = &gcra{}
	default:
		rl.goRate = &goRate{}
	}
	return rl, nil
}

func (l *Limiter) Limit(ctx context.Context, cost, rate, window int64, key string) (TokenReturner, error) {
	if l.redisScripter != nil && l.redisSortedSetRemover != nil {
		if l.useGCRA {
			return l.redisGCRA(ctx, cost, rate, window, key)
		}
		return l.redisSortedSet(ctx, cost, rate, window, key)
	}
	if l.useInMemorySortedSets {
		return l.sortedSetLimit(ctx, cost, rate, window, key)
	}
	if l.useGCRA {
		return l.gcraLimit(ctx, cost, rate, window, key)
	}
	return l.goRateLimit(ctx, cost, rate, window, key)
}

func (l *Limiter) redisSortedSet(ctx context.Context, cost, rate, window int64, key string) (TokenReturner, error) {
	res, err := sortedSetScript.Run(ctx, l.redisScripter, []string{key}, cost, rate, window).Result()
	if err != nil {
		return nil, fmt.Errorf("could not run SortedSet Redis script: %v", err)
	}
	members, ok := res.(string)
	if !ok {
		return nil, fmt.Errorf("unexpected result from SortedSet Redis script of type %T: %v", res, res)
	}
	if members == "0" {
		return nil, nil
	}
	return &sortedSetRedisReturn{
		key:     key,
		members: strings.Split(members, ","),
		remover: l.redisSortedSetRemover,
	}, nil
}

func (l *Limiter) redisGCRA(ctx context.Context, cost, rate, window int64, key string) (TokenReturner, error) {
	burst := int64(1)
	if l.useGCRABurstAsRate {
		burst = rate
	}
	res, err := gcraRedisScript.Run(ctx, l.redisScripter, []string{key}, burst, rate, window, cost).Result()
	if err != nil {
		return nil, fmt.Errorf("could not run GCRA Redis script: %v", err)
	}
	result, ok := res.([]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected result from GCRA Redis script of type %T: %v", res, res)
	}
	if len(result) != 4 {
		return nil, fmt.Errorf("unexpected result of length %d: %+v", len(result), result)
	}
	allowed, ok := result[0].(int64)
	if !ok {
		return nil, fmt.Errorf("unexpected allowed value of type %T: %v", result[0], result[0])
	}
	if allowed < 1 {
		return nil, nil // limit exceeded
	}
	return &unsupportedReturn{}, nil
}

func (l *Limiter) gcraLimit(_ context.Context, cost, rate, window int64, key string) (TokenReturner, error) {
	burst := int64(1)
	if l.useGCRABurstAsRate {
		burst = rate
	}
	allowed, err := l.gcra.limit(key, cost, burst, rate, window)
	if err != nil {
		return nil, fmt.Errorf("could not limit: %w", err)
	}
	if !allowed {
		return nil, nil // limit exceeded
	}
	return &unsupportedReturn{}, nil
}

func (l *Limiter) sortedSetLimit(_ context.Context, cost, rate, window int64, key string) (TokenReturner, error) {
	members, err := l.sortedSet.limit(key, cost, rate, window)
	if err != nil {
		return nil, fmt.Errorf("could not limit: %w", err)
	}

	return &sortedSetInMemoryReturn{
		key:     key,
		members: members,
		remover: l.sortedSet,
	}, nil
}

func (l *Limiter) goRateLimit(_ context.Context, cost, rate, window int64, key string) (TokenReturner, error) {
	res := l.goRate.limit(key, cost, rate, window)
	if !res.OK() {
		res.Cancel()
		return nil, nil // limit exceeded
	}
	if res.Delay() > 0 {
		res.Cancel()
		return nil, nil // limit exceeded
	}
	return &goRateReturn{reservation: res}, nil
}
