package throttling

import (
	"context"
	"fmt"

	"github.com/go-redis/redis/v9"
)

type unsupportedReturn struct{}

func (*unsupportedReturn) Return(_ context.Context) error { return nil }

type redisSortedSetRemover interface {
	ZRem(ctx context.Context, key string, members ...interface{}) *redis.IntCmd
}

type sortedSetRedisReturn struct {
	key     string
	members []string
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

type sortedSetMemoryRemover interface {
	remove(key string, members []string)
}

type sortedSetInMemoryReturn struct {
	key     string
	members []string
	remover sortedSetMemoryRemover
}

func (r *sortedSetInMemoryReturn) Return(_ context.Context) error {
	r.remover.remove(r.key, r.members)
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
