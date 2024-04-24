package reporting

import (
	"sync"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
)

type EventSampler struct {
	collectedSamples map[string]bool
	mu               sync.Mutex
	resetDuration    config.ValueLoader[time.Duration]
}

func NewEventSampler(resetDuration config.ValueLoader[time.Duration]) *EventSampler {
	return &EventSampler{
		collectedSamples: make(map[string]bool),
		resetDuration:    resetDuration,
	}
}

func (es *EventSampler) IsSampleEventCollected(groupingColumns string) bool {
	es.mu.Lock()
	defer es.mu.Unlock()

	_, exists := es.collectedSamples[groupingColumns]
	return exists
}

func (es *EventSampler) MarkSampleEventAsCollected(groupingColumns string) {
	es.mu.Lock()
	defer es.mu.Unlock()

	es.collectedSamples[groupingColumns] = true
}

func (es *EventSampler) StartResetLoop() error {
	go func() {
		for {
			time.Sleep(es.resetDuration.Load())
			es.mu.Lock()
			es.collectedSamples = make(map[string]bool)
			es.mu.Unlock()
		}
	}()
	return nil
}
