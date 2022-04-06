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

	subscriptionsMutex sync.RWMutex
	// subscriptions keep the list of subscription publishers per topic
	subscriptions map[string]subPublishers
}

func (eb *EventBus) Publish(topic string, data interface{}) {
	eb.subscriptionsMutex.RLock()
	defer eb.subscriptionsMutex.RUnlock()
	eb.lastEventMutex.Lock()
	defer eb.lastEventMutex.Unlock()

	evt := &DataEvent{Data: data, Topic: topic}
	if eb.lastEvent == nil {
		eb.lastEvent = map[string]*DataEvent{}
	}
	eb.lastEvent[topic] = evt

	if subPublishers, found := eb.subscriptions[topic]; found {
		for _, subPublisher := range subPublishers {
			subPublisher.publish(evt)
		}
	}
}

func (eb *EventBus) Subscribe(topic string, ch DataChannel) {
	eb.subscriptionsMutex.Lock()
	defer eb.subscriptionsMutex.Unlock()
	eb.lastEventMutex.RLock()
	defer eb.lastEventMutex.RUnlock()

	newSubPublisher := &subPublisher{channel: ch}
	if prev, found := eb.subscriptions[topic]; found {
		eb.subscriptions[topic] = append(prev, newSubPublisher)
	} else {
		if eb.subscriptions == nil {
			eb.subscriptions = map[string]subPublishers{}
		}
		eb.subscriptions[topic] = subPublishers{newSubPublisher}
	}
	if eb.lastEvent[topic] != nil {
		newSubPublisher.publish(eb.lastEvent[topic])
	}
}

// subPublishers is a slice of subPublisher pointers
type subPublishers []*subPublisher

// subPublisher is responsible to publish events to a single subscription (channel).
type subPublisher struct {
	// the channel of the subscription where events are published
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
func (r *subPublisher) publish(data *DataEvent) {

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

// startLoop publishes lastValues to the subscription's channel until there is no other lastValue to publish
func (r *subPublisher) startLoop() {

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
