package event_sampler

import (
	"fmt"
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
	ttl config.ValueLoader[time.Duration],
	eventSamplerType config.ValueLoader[string],
	eventSamplingCardinality config.ValueLoader[int],
	conf *config.Config,
	log logger.Logger,
) (EventSampler, error) {
	var es EventSampler
	var err error

	switch eventSamplerType.Load() {
	case BadgerTypeEventSampler:
		es, err = NewBadgerEventSampler(BadgerEventSamplerPathName, ttl, conf, log)
	case InMemoryCacheTypeEventSampler:
		es, err = NewInMemoryCacheEventSampler(ttl, eventSamplingCardinality)
	default:
		err = fmt.Errorf("invalid event sampler type: %s", eventSamplerType.Load())
	}

	if err != nil {
		return nil, err
	}
	return es, nil
}
