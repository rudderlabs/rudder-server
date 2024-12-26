package event_sampler

import (
	"context"
	"time"

	"github.com/rudderlabs/rudder-go-kit/cachettl"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/stats"
)

type InMemoryCacheEventSampler struct {
	ctx    context.Context
	cancel context.CancelFunc
	cache  *cachettl.Cache[string, bool]
	ttl    config.ValueLoader[time.Duration]
	limit  config.ValueLoader[int]
	length int
	sc     *StatsCollector
}

func NewInMemoryCacheEventSampler(
	ctx context.Context,
	module string,
	ttl config.ValueLoader[time.Duration],
	limit config.ValueLoader[int],
	stats stats.Stats,
) (*InMemoryCacheEventSampler, error) {
	c := cachettl.New[string, bool](cachettl.WithNoRefreshTTL)
	ctx, cancel := context.WithCancel(ctx)

	es := &InMemoryCacheEventSampler{
		ctx:    ctx,
		cancel: cancel,
		cache:  c,
		ttl:    ttl,
		limit:  limit,
		length: 0,
		sc:     NewStatsCollector(InMemoryCacheTypeEventSampler, module, stats),
	}

	es.cache.OnEvicted(func(key string, value bool) {
		es.length--
	})

	return es, nil
}

func (es *InMemoryCacheEventSampler) Get(key string) (bool, error) {
	start := time.Now()
	defer es.sc.RecordGetDuration(start)
	es.sc.RecordGet()

	value := es.cache.Get(key)
	return value, nil
}

func (es *InMemoryCacheEventSampler) Put(key string) error {
	if es.length >= es.limit.Load() {
		return nil
	}

	start := time.Now()
	defer es.sc.RecordPutDuration(start)
	es.sc.RecordPut()

	es.cache.Put(key, true, es.ttl.Load())
	es.length++
	return nil
}

func (es *InMemoryCacheEventSampler) Close() {
	es.cancel()
}
