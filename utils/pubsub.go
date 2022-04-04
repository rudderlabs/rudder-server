//go:generate mockgen -destination=../mocks/utils/mock_pubsub.go -package=utils github.com/rudderlabs/rudder-server/utils PublishSubscriber

package utils

import (
	"sync"
)

type DataEvent struct {
	Data  interface{}
	Topic string
}

// DataChannel is a channel which can accept an DataEvent
type DataChannel chan DataEvent

type PublishSubscriber interface {
	Publish(topic string, data interface{})
	Subscribe(topic string, ch DataChannel)
}

// EventBus stores the information about subscribers interested for a particular topic
type EventBus struct {
	lastEventMutex sync.RWMutex
	// lastEvent holds the last event for each topic so that we can send it to new subscribers
	lastEvent map[string]*DataEvent

	subscribersMutex sync.RWMutex
	// subscribers keep the list of subscription publishers per topic
	subscribers map[string]publisherSlice
}

func (eb *EventBus) Publish(topic string, data interface{}) {
	eb.subscribersMutex.RLock()
	defer eb.subscribersMutex.RUnlock()
	eb.lastEventMutex.Lock()
	defer eb.lastEventMutex.Unlock()

	evt := &DataEvent{Data: data, Topic: topic}
	if eb.lastEvent == nil {
		eb.lastEvent = map[string]*DataEvent{}
	}
	eb.lastEvent[topic] = evt

	if publishers, found := eb.subscribers[topic]; found {
		for _, publisher := range publishers {
			publisher.publish(evt)
		}
	}

}

func (eb *EventBus) Subscribe(topic string, ch DataChannel) {
	eb.subscribersMutex.Lock()
	defer eb.subscribersMutex.Unlock()
	eb.lastEventMutex.RLock()
	defer eb.lastEventMutex.RUnlock()

	p := &publisher{channel: ch}
	if prev, found := eb.subscribers[topic]; found {
		eb.subscribers[topic] = append(prev, p)
	} else {
		if eb.subscribers == nil {
			eb.subscribers = map[string]publisherSlice{}
		}
		eb.subscribers[topic] = publisherSlice{p}
	}
	if eb.lastEvent[topic] != nil {
		p.publish(eb.lastEvent[topic])
	}
}

// publisherSlice is a slice of publisher pointers
type publisherSlice []*publisher

// publisher is responsible to publish an event to a single subscriber (channel).
type publisher struct {
	// the channel where events are published
	channel chan DataEvent

	lastValueLock sync.Mutex
	// the last value waiting to be published to the channel
	lastValue *DataEvent

	startedLock sync.Mutex
	// flag indicating whether the publisher's internal goroutine is started or not
	started bool
}

// publish sets the publisher's lastValue and starts the
// internal goroutine if it is not already started
func (r *publisher) publish(data *DataEvent) {

	r.lastValueLock.Lock()
	defer r.lastValueLock.Unlock()
	r.startedLock.Lock()
	defer r.startedLock.Unlock()

	// update last value
	r.lastValue = data

	// start publish loop if not started
	if !r.started {
		go r.startLoop()
		r.started = true
	}
}

// startLoop publishes lastValues to the subscriber's channel until there is no other lastValue to publish
func (r *publisher) startLoop() {

	for r.lastValue != nil {
		r.lastValueLock.Lock()
		v := *r.lastValue
		r.lastValue = nil
		r.lastValueLock.Unlock()
		r.channel <- v
	}
	r.startedLock.Lock()
	r.started = false
	r.startedLock.Unlock()
}
