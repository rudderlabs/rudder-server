package event_sampler

import (
	"context"
	"time"

	"github.com/rudderlabs/rudder-go-kit/cachettl"
	"github.com/rudderlabs/rudder-go-kit/config"
)

type InMemoryCacheEventSampler struct {
	ctx    context.Context
	cancel context.CancelFunc
	cache  *cachettl.Cache[string, bool]
	ttl    config.ValueLoader[time.Duration]
	limit  config.ValueLoader[int]
	length int
}

func NewInMemoryCacheEventSampler(ctx context.Context, ttl config.ValueLoader[time.Duration], limit config.ValueLoader[int]) (*InMemoryCacheEventSampler, error) {
	c := cachettl.New[string, bool](cachettl.WithNoRefreshTTL)
	ctx, cancel := context.WithCancel(ctx)

	es := &InMemoryCacheEventSampler{
		ctx:    ctx,
		cancel: cancel,
		cache:  c,
		ttl:    ttl,
		limit:  limit,
		length: 0,
	}

	es.cache.OnEvicted(func(key string, value bool) {
		es.length--
	})

	return es, nil
}

func (es *InMemoryCacheEventSampler) Get(key string) (bool, error) {
	value := es.cache.Get(key)
	return value, nil
}

func (es *InMemoryCacheEventSampler) Put(key string) error {
	if es.length >= es.limit.Load() {
		return nil
	}

	es.cache.Put(key, true, es.ttl.Load())
	es.length++
	return nil
}

func (es *InMemoryCacheEventSampler) Close() {
	es.cancel()
}
