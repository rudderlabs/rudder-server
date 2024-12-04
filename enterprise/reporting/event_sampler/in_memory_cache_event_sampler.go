package event_sampler

import (
	"sync"
	"time"

	"github.com/rudderlabs/rudder-go-kit/cachettl"
	"github.com/rudderlabs/rudder-go-kit/config"
)

type InMemoryCacheEventSampler struct {
	cache  *cachettl.Cache[string, bool]
	mu     sync.Mutex
	ttl    config.ValueLoader[time.Duration]
	limit  config.ValueLoader[int]
	length int
}

func NewInMemoryCacheEventSampler(ttl config.ValueLoader[time.Duration], limit config.ValueLoader[int]) (*InMemoryCacheEventSampler, error) {
	c := cachettl.New[string, bool](cachettl.WithNoRefreshTTL)

	es := &InMemoryCacheEventSampler{
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
	es.mu.Lock()
	defer es.mu.Unlock()
	value := es.cache.Get(key)
	return value, nil
}

func (es *InMemoryCacheEventSampler) Put(key string) error {
	es.mu.Lock()
	defer es.mu.Unlock()
	if es.length >= es.limit.Load() {
		return nil
	}

	es.cache.Put(key, true, es.ttl.Load())
	es.length++
	return nil
}

func (es *InMemoryCacheEventSampler) Close() {
}
