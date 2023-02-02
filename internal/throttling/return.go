package throttling

import (
	"context"
	"fmt"

	"github.com/go-redis/redis/v8"
)

type unsupportedReturn struct{}

func (*unsupportedReturn) Return(_ context.Context) error { return nil }

type redisSortedSetRemover interface {
	ZRem(ctx context.Context, key string, members ...interface{}) *redis.IntCmd
}

type sortedSetRedisReturn struct {
	// key is the key of the sorted set
	key string
	// members are the members (tokens) in the sorted set (bucket)
	members []string
	// remover is the redisSpeaker that removes the members from the sorted set (aka *redis.Client)
	remover redisSortedSetRemover
}

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
