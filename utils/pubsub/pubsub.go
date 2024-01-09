package pubsub

import (
	"context"
	"sync"
)

type DataEvent struct {
	Data  interface{}
	Topic string
}

// DataChannel is a channel which can accept an DataEvent
type DataChannel <-chan DataEvent

// PublishSubscriber stores the information about subscribers interested for a particular topic
type PublishSubscriber struct {
	lastEventMutex sync.RWMutex
	// lastEvent holds the last event for each topic so that we can send it to new subscribers
	lastEvent map[string]*DataEvent

	subscriptionsMutex sync.RWMutex
	// subscriptions keep the list of subscription publishers per topic
	subscriptions map[string]subPublishers
}

func New() *PublishSubscriber {
	return &PublishSubscriber{}
}

func (eb *PublishSubscriber) Publish(topic string, data interface{}) {
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

func (eb *PublishSubscriber) Subscribe(ctx context.Context, topic string) DataChannel {
	eb.subscriptionsMutex.Lock()
	defer eb.subscriptionsMutex.Unlock()
	eb.lastEventMutex.RLock()
	defer eb.lastEventMutex.RUnlock()

	ch := make(chan DataEvent)

	newSubPublisher := newListener(ch)
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

	go func() {
		<-ctx.Done()
		eb.removePubSub(topic, newSubPublisher)
		newSubPublisher.close()
	}()

	return ch
}

func (eb *PublishSubscriber) removePubSub(topic string, r *listener) {
	eb.subscriptionsMutex.Lock()
	defer eb.subscriptionsMutex.Unlock()
	eb.lastEventMutex.RLock()
	defer eb.lastEventMutex.RUnlock()

	listeners := eb.subscriptions[topic]

	for i, item := range listeners {
		if item == r {
			eb.subscriptions[topic] = append(listeners[:i], listeners[i+1:]...)
			return
		}
	}
}

func (eb *PublishSubscriber) Close() {
	eb.subscriptionsMutex.Lock()
	defer eb.subscriptionsMutex.Unlock()
	eb.lastEventMutex.RLock()
	defer eb.lastEventMutex.RUnlock()

	for _, subPublishers := range eb.subscriptions {
		for _, subPublisher := range subPublishers {
			subPublisher.close()
		}
	}

	eb.subscriptions = nil
}

// listener is a slice of subPublisher pointers
type subPublishers []*listener

// listener is responsible to publish events to a single subscription (channel).
type listener struct {
	// the channel of the subscription where events are published
	channel chan DataEvent

	lastValueLock sync.Mutex
	// the last value waiting to be published to the channel
	lastValue *DataEvent

	// channel for signaling the loop to read a new value
	ping chan struct{}
}

func newListener(channel chan DataEvent) *listener {
	l := &listener{
		ping:    make(chan struct{}, 1),
		channel: channel,
	}
	go l.startLoop()
	return l
}

// publish sets the publisher's lastValue and starts the
// internal goroutine if it is not already started
func (r *listener) publish(data *DataEvent) {
	r.lastValueLock.Lock()
	r.lastValue = data
	r.lastValueLock.Unlock()
	select {
	case r.ping <- struct{}{}: // signals the startLoop that it has to read the value
	default:
		// do nothing - leaky bucket
	}
}

// startLoop publishes lastValues to the subscription's channel until there is no other lastValue to publish
func (r *listener) startLoop() {
	for range r.ping {
		r.lastValueLock.Lock()
		v := r.lastValue
		r.lastValueLock.Unlock()
		if v != nil {
			r.channel <- *v
		}
	}
	close(r.channel)
}

func (r *listener) close() {
	close(r.ping)
}
