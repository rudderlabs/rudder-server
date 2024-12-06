package event_sampler

import (
	"context"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
)

const (
	BadgerTypeEventSampler        = "badger"
	InMemoryCacheTypeEventSampler = "in_memory_cache"
	BadgerEventSamplerPathName    = "/reporting-badger"
)

type EventSampler interface {
	Put(key string) error
	Get(key string) (bool, error)
	Close()
}

func NewEventSampler(
	ctx context.Context,
	ttl config.ValueLoader[time.Duration],
	eventSamplerType config.ValueLoader[string],
	eventSamplingCardinality config.ValueLoader[int],
	conf *config.Config,
	log logger.Logger,
) (es EventSampler, err error) {
	switch eventSamplerType.Load() {
	case BadgerTypeEventSampler:
		es, err = NewBadgerEventSampler(ctx, BadgerEventSamplerPathName, ttl, conf, log)
	case InMemoryCacheTypeEventSampler:
		es, err = NewInMemoryCacheEventSampler(ctx, ttl, eventSamplingCardinality)
	default:
		log.Warnf("invalid event sampler type: %s. Using default badger event sampler", eventSamplerType.Load())
		es, err = NewBadgerEventSampler(ctx, BadgerEventSamplerPathName, ttl, conf, log)
	}

	if err != nil {
		return nil, err
	}
	return es, nil
}
