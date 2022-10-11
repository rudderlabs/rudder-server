package throttling

import (
	"context"
	_ "embed"
	"fmt"

	"github.com/go-redis/redis/v9"
)

var (
	//go:embed gcra.lua
	gcraLua         string
	gcraRedisScript *redis.Script
)

func init() {
	gcraRedisScript = redis.NewScript(gcraLua)
}

type RedisLimiter struct {
	scripter redis.Scripter
}

func (r *RedisLimiter) Limit(ctx context.Context, cost, rate, window int64, key string) (bool, error) {
	return r.gcraLimit(ctx, cost, rate, window, key)
}

func (r *RedisLimiter) gcraLimit(ctx context.Context, cost, rate, window int64, key string) (bool, error) {
	res, err := gcraRedisScript.Run(ctx, r.scripter, []string{}).Result()
	if err != nil {
		return false, fmt.Errorf("could not run GCRA Redis script: %v", err)
	}
	result, ok := res.([]interface{})
	if !ok {
		return false, fmt.Errorf("unexpected result from GCRA Redis script of type %T: %v", res, res)
	}
	if len(result) != 4 {
		return false, fmt.Errorf("unexpected result of length %d: %+v", len(result), result)
	}
	allowed, ok := result[0].(int64)
	if !ok {
		return false, fmt.Errorf("unexpected allowed value of type %T: %v", result[0], result[0])
	}
	return allowed > 0, nil
}

type InMemoryLimiter struct{}

type gcraPayload struct {
	allowed, remaining     int
	retryAfter, resetAfter string
}
