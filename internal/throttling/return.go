package throttling

import (
	"context"
	"errors"
	"fmt"

	"github.com/go-redis/redis/v9"
)

var ErrorUnsupportedReturn = errors.New("token return not supported")

type unsupportedReturn struct{}

func (r *unsupportedReturn) Return(ctx context.Context) error { return ErrorUnsupportedReturn }

type sortedSetRemover interface {
	ZRem(ctx context.Context, key string, members ...interface{}) *redis.IntCmd
	ZRemRangeByLex(ctx context.Context, key, min, max string) *redis.IntCmd
}

type sortedSetZRemReturn struct {
	key     string
	members []string
	remover sortedSetRemover
}

func (r *sortedSetZRemReturn) Return(ctx context.Context) error {
	var (
		length = len(r.members)
		slice  = make([]interface{}, length)
	)
	for i, v := range r.members {
		slice[i] = v
	}
	res, err := r.remover.ZRem(ctx, r.key, slice...).Result()
	if err != nil {
		return fmt.Errorf("could not remove members from sorted set: %v", err)
	}
	if res != int64(length) {
		return fmt.Errorf("could not remove all members from sorted set: %d instead of %d", res, length)
	}
	return nil
}
