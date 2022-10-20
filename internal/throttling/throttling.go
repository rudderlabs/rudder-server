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
* finish tests
* metrics inside client (especially for returning tokens)
* benchmark AWS Elasticache Redis
* replace old limiters
* feature flags for algo type
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

func (l *Limiter) Limit(ctx context.Context, cost, rate, window int64, key string) (TokenReturner, error) {
	if l.redisTalker != nil {
		if l.useGCRA {
			return l.redisGCRA(ctx, cost, rate, window, key)
		}
		return l.redisSortedSet(ctx, cost, rate, window, key)
	}
	if l.useGCRA {
		return l.gcraLimit(ctx, cost, rate, window, key)
	}
	return l.goRateLimit(ctx, cost, rate, window, key)
}

func (l *Limiter) getRedisTalker(key string) redisTalker {
	if l.redisKeyToClientMap != nil {
		if client, ok := l.redisKeyToClientMap[key]; ok {
			return client
		}
	}
	return l.redisTalker
}

func (l *Limiter) redisSortedSet(ctx context.Context, cost, rate, window int64, key string) (TokenReturner, error) {
	talker := l.getRedisTalker(key)
	res, err := sortedSetScript.Run(ctx, talker, []string{key}, cost, rate, window).Result()
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
		remover: talker,
	}, nil
}

func (l *Limiter) redisGCRA(ctx context.Context, cost, rate, window int64, key string) (TokenReturner, error) {
	burst := int64(1)
	if l.useGCRABurstAsRate {
		burst = rate
	}
	talker := l.getRedisTalker(key)
	res, err := gcraRedisScript.Run(ctx, talker, []string{key}, burst, rate, window, cost).Result()
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

func (l *Limiter) goRateLimit(_ context.Context, cost, rate, window int64, key string) (TokenReturner, error) {
	res := l.goRate.limit(key, cost, rate, window)
	if !res.Allowed() {
		res.CancelFuture()
		return nil, nil // limit exceeded
	}
	return &goRateReturn{reservation: res}, nil
}
