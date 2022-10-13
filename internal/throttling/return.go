package throttling

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/go-redis/redis/v9"
)

var ErrorUnsupportedReturn = errors.New("token return not supported")

type unsupportedReturn struct{}

func (r *unsupportedReturn) Return(ctx context.Context) error { return ErrorUnsupportedReturn }

type sortedSetRemover interface {
	ZRem(ctx context.Context, key string, members ...interface{}) *redis.IntCmd
	ZRemRangeByLex(ctx context.Context, key, min, max string) *redis.IntCmd
}

type sortedSetReturn struct {
	key     string
	members []string
	remover sortedSetRemover
}

type sortedSetZRemReturn struct{ sortedSetReturn }

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

type sortedSetZRemRangeByLexReturn struct{ sortedSetReturn }

func (r *sortedSetZRemRangeByLexReturn) Return(ctx context.Context) error {
	var (
		length   = len(r.members)
		min, max string
	)
	first, err := strconv.ParseInt(r.members[0], 10, 64)
	if err != nil {
		return fmt.Errorf("could not convert first member to int64: %v", err)
	}
	min, max = strconv.FormatInt(first-1, 10), r.members[0]
	if length > 1 {
		max = r.members[length-1]
	}
	_, err = r.remover.ZRemRangeByLex(ctx, r.key, "("+min, "["+max).Result()
	if err != nil {
		return fmt.Errorf("could not remove members from sorted set (min %s, max: %s): %v", min, max, err)
	}
	// no need to check if we actually deleted any member given that they could have been expired in the meantime
	return nil
}
