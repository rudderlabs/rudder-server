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

	startedOnce sync.Once
	ping        chan struct{}
}

// publish sets the publisher's lastValue and starts the
// internal goroutine if it is not already started
func (r *subPublisher) publish(data *DataEvent) {
	r.startedOnce.Do(func() {
		r.ping = make(chan struct{}, 1)
		go r.startLoop()
	})

	r.lastValueLock.Lock()
	defer r.lastValueLock.Unlock()

	// update last value
	r.lastValue = data

	select {
	case r.ping <- struct{}{}: // signals the startLoop that it has to read the value
	default:
		// do nothing - leaky bucket
	}
}

// startLoop publishes lastValues to the subscription's channel until there is no other lastValue to publish
func (r *subPublisher) startLoop() {
	for range r.ping {
		r.lastValueLock.Lock()
		v := r.lastValue
		r.lastValueLock.Unlock()
		if v != nil {
			r.channel <- *v
		}
	}
}
