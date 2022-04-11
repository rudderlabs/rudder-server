//go:generate mockgen -destination=../../mocks/utils/pubsub/mock_pubsub.go -package=utils github.com/rudderlabs/rudder-server/utils/pubsub PublishSubscriber

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
type DataChannel chan DataEvent

type PublishSubscriber interface {
	Publish(topic string, data interface{})
	Subscribe(topic string, ch DataChannel)
}

func NewPublishSubscriber(ctx context.Context) PublishSubscriber {
	return &publishSubscriber{ctx: ctx}
}

// publishSubscriber stores the information about subscribers interested for a particular topic
type publishSubscriber struct {
	ctx            context.Context
	lastEventMutex sync.RWMutex
	// lastEvent holds the last event for each topic so that we can send it to new subscribers
	lastEvent map[string]*DataEvent

	subscriptionsMutex sync.RWMutex
	// subscriptions keep the list of subscription publishers per topic
	subscriptions map[string]subPublishers
}

func (eb *publishSubscriber) Publish(topic string, data interface{}) {
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

func (eb *publishSubscriber) Subscribe(topic string, ch DataChannel) {
	eb.subscriptionsMutex.Lock()
	defer eb.subscriptionsMutex.Unlock()
	eb.lastEventMutex.RLock()
	defer eb.lastEventMutex.RUnlock()

	newSubPublisher := &subPublisher{channel: ch, ctx: eb.ctx}
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
	ctx context.Context
	// the channel of the subscription where events are published
	channel chan DataEvent

	lastValueLock sync.Mutex
	// the last value waiting to be published to the channel
	lastValue *DataEvent

	// used for initialization
	startedOnce sync.Once
	// channel for signaling the loop to read a new value
	ping chan struct{}
}

// publish sets the publisher's lastValue and starts the
// internal goroutine if it is not already started
func (r *subPublisher) publish(data *DataEvent) {
	r.startedOnce.Do(func() {
		r.ping = make(chan struct{}, 1)
		go r.startLoop()
	})

	r.lastValueLock.Lock()
	r.lastValue = data
	r.lastValueLock.Unlock()

	select {
	case <-r.ctx.Done():
		return
	case r.ping <- struct{}{}: // signals the startLoop that it has to read the value
	default:
		// do nothing - leaky bucket
	}
}

// startLoop publishes lastValues to the subscription's channel until there is no other lastValue to publish
func (r *subPublisher) startLoop() {
	for {
		select {
		case <-r.ctx.Done():
			return
		case <-r.ping:
			r.lastValueLock.Lock()
			v := r.lastValue
			r.lastValueLock.Unlock()
			if v != nil {
				r.channel <- *v
			}
		}
	}
}
