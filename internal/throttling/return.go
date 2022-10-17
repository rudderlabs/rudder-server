package throttling

import (
	"context"
	"errors"
	"fmt"

	"github.com/go-redis/redis/v9"
)

var ErrorUnsupportedReturn = errors.New("token return not supported")

type unsupportedReturn struct{}

// Return TODO for reviewer(we could return nil instead and pretend that tokens were returned simplifying client logic)
func (*unsupportedReturn) Return(_ context.Context) error { return ErrorUnsupportedReturn }

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
