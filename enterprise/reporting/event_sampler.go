package reporting

import (
	"sync"
	"time"

	"github.com/rudderlabs/rudder-go-kit/cachettl"
	"github.com/rudderlabs/rudder-go-kit/config"
)

type EventSampler[K comparable] struct {
	cache  *cachettl.Cache[K, bool]
	mu     sync.Mutex
	ttl    config.ValueLoader[time.Duration]
	limit  config.ValueLoader[int]
	length int
}

func NewEventSampler[K comparable](ttl config.ValueLoader[time.Duration], limit config.ValueLoader[int]) *EventSampler[K] {
	c := cachettl.New[K, bool](cachettl.WithNoRefreshTTL)

	es := &EventSampler[K]{
		cache:  c,
		ttl:    ttl,
		limit:  limit,
		length: 0,
	}

	es.cache.OnEvicted(func(key K, value bool) {
		es.length--
	})

	return es
}

func (es *EventSampler[K]) Get(key K) (ok bool) {
	es.mu.Lock()
	defer es.mu.Unlock()
	value := es.cache.Get(key)
	return value
}

func (es *EventSampler[K]) Put(key K) {
	es.mu.Lock()
	defer es.mu.Unlock()
	if es.length >= es.limit.Load() {
		return
	}

	es.cache.Put(key, true, es.ttl.Load())
	es.length++
}
