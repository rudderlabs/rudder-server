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

// DataChannelSlice is a slice of DataChannels
type DataChannelSlice []DataChannel
type PublishSubscriber interface {
	Publish(topic string, data interface{})
	PublishToChannel(channel DataChannel, topic string, data interface{})
	Subscribe(topic string, ch DataChannel)
}

// EventBus stores the information about subscribers interested for a particular topic
type EventBus struct {
	subscribers map[string]DataChannelSlice
	rm          sync.RWMutex
}

func (eb *EventBus) Publish(topic string, data interface{}) {
	eb.rm.RLock()
	if chans, found := eb.subscribers[topic]; found {
		// this is done because the slices refer to same array even though they are passed by value
		// thus we are creating a new slice with our elements thus preserve locking correctly.
		// special thanks for /u/freesid who pointed it out
		channels := append(DataChannelSlice{}, chans...)
		go func(data DataEvent, dataChannelSlices DataChannelSlice) {
			for _, ch := range dataChannelSlices {
				ch <- data
			}
		}(DataEvent{Data: data, Topic: topic}, channels)
	}
	eb.rm.RUnlock()
}

func (eb *EventBus) PublishToChannel(channel DataChannel, topic string, data interface{}) {
	eb.rm.RLock()
	go func() {
		channel <- DataEvent{Data: data, Topic: topic}
	}()
	eb.rm.RUnlock()
}

func (eb *EventBus) Subscribe(topic string, ch DataChannel) {
	eb.rm.Lock()
	if prev, found := eb.subscribers[topic]; found {
		eb.subscribers[topic] = append(prev, ch)
	} else {
		if eb.subscribers == nil {
			eb.subscribers = map[string]DataChannelSlice{}
		}
		eb.subscribers[topic] = append([]DataChannel{}, ch)
	}
	eb.rm.Unlock()
}
