package throttling

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v9"
)

type unsupportedReturn struct {
	// time is the current time in milliseconds from Redis perspective
	time time.Duration
}

func (u *unsupportedReturn) getTime() time.Duration       { return u.time }
func (*unsupportedReturn) Return(_ context.Context) error { return nil }

type redisSortedSetRemover interface {
	ZRem(ctx context.Context, key string, members ...interface{}) *redis.IntCmd
}

type sortedSetRedisReturn struct {
	// key is the key of the sorted set
	key string
	// time is the current time in milliseconds from Redis perspective
	time time.Duration
	// members are the members (tokens) in the sorted set (bucket)
	members []string
	// remover is the redisTalker that removes the members from the sorted set (aka *redis.Client)
	remover redisSortedSetRemover
}

func (r *sortedSetRedisReturn) getTime() time.Duration { return r.time }
func (r *sortedSetRedisReturn) Return(ctx context.Context) error {
	var (
		length = len(r.members)
		slice  = make([]interface{}, length)
	)
	for i, v := range r.members {
		slice[i] = v
	}
	_, err := r.remover.ZRem(ctx, r.key, slice...).Result()
	if err != nil {
		return fmt.Errorf("could not remove members from sorted set: %v", err)
	}
	return nil
}

type goRateRemover interface {
	Cancel()
}

type goRateReturn struct {
	reservation goRateRemover
}

func (r *goRateReturn) Return(_ context.Context) error {
	r.reservation.Cancel()
	return nil
}
