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

// RedisLimiter TODO constructor
type RedisLimiter struct {
	scripter         redis.Scripter
	sortedSetRemover sortedSetRemover
}

func (r *RedisLimiter) Limit(ctx context.Context, cost, rate, window int64, key string) (
	interface{ Return(context.Context) error }, // TODO see if more convenient to return real interface
	error,
) {
	return r.gcraLimit(ctx, cost, rate, window, key)
}

func (r *RedisLimiter) sortedSetLimit(ctx context.Context, cost, rate, window int64, key string) (
	interface{ Return(context.Context) error },
	error,
) {
	res, err := sortedSetScript.Run(ctx, r.scripter, []string{key}, cost, rate, window).Result()
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
	return &sortedSetZRemReturn{
		key:     key,
		members: strings.Split(members, ","),
		remover: r.sortedSetRemover,
	}, nil
}

func (r *RedisLimiter) gcraLimit(ctx context.Context, cost, rate, window int64, key string) (
	interface{ Return(context.Context) error },
	error,
) {
	// rate is repeated twice because we are using the rate parameter also for burst.
	// this is done to keep compatibility between GCRA and the SortedSet approach.
	res, err := gcraRedisScript.Run(ctx, r.scripter, []string{key}, 1, rate, window, cost).Result()
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

// InMemoryLimiter TODO constructor
// It allows to use the throttling package without Redis with GCRA or SortedSets.
type InMemoryLimiter struct {
	gcra      *gcra
	sortedSet *sortedSet
	goRate    *goRate
}

func (i *InMemoryLimiter) Limit(ctx context.Context, cost, rate, window int64, key string) (
	interface{ Return(context.Context) error },
	error,
) {
	return i.gcraLimit(ctx, cost, rate, window, key)
}

func (i *InMemoryLimiter) gcraLimit(_ context.Context, cost, rate, window int64, key string) (
	interface{ Return(context.Context) error },
	error,
) {
	allowed, _, _, _, err := i.gcra.limit(key, cost, rate, rate, window)
	if err != nil {
		return nil, fmt.Errorf("could not limit: %w", err)
	}
	if allowed < 1 {
		return nil, nil // limit exceeded
	}
	return &unsupportedReturn{}, nil
}

func (i *InMemoryLimiter) sortedSetLimit(_ context.Context, cost, rate, window int64, key string) (
	interface{ Return(context.Context) error },
	error,
) {
	members, err := i.sortedSet.limit(key, cost, rate, window)
	if err != nil {
		return nil, fmt.Errorf("could not limit: %w", err)
	}

	return &sortedSetInMemoryReturn{
		key:     key,
		members: members,
		remover: i.sortedSet,
	}, nil
}

func (i *InMemoryLimiter) goRateLimit(_ context.Context, cost, rate, window int64, key string) (
	interface{ Return(context.Context) error },
	error,
) {
	res := i.goRate.limit(key, cost, rate, window)
	if !res.OK() {
		return nil, nil // limit exceeded
	}
	if res.Delay() > 0 {
		res.Cancel()
		return nil, nil // limit exceeded
	}
	return &goRateReturn{reservation: res}, nil
}
