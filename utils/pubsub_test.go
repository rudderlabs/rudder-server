package utils

import (
	"sync"
	"testing"
	"time"
)

func TestPubSub_single_slow_consumer(t *testing.T) {
	const topic = "topic"
	bus := EventBus{}
	slow := testConsumer{}
	defer func() { slow.done <- true }() // stop consumer

	slow.subscribe(topic, &bus)
	bus.Publish(topic, 1)
	<-slow.started // wait consumer to receive the 1st message
	bus.Publish(topic, 2)
	bus.Publish(topic, 3)
	bus.Publish(topic, 4)
	bus.Publish(topic, 5)

	v := <-slow.out
	if v != 1 {
		t.Errorf("Expected slow subscriber to have only consumed first value 1, instead got %d.", v)
	}
	v = <-slow.out
	if v != 5 {
		t.Errorf("Expected slow subscriber to consume latest value 5, instead got %d.", v)
	}
}

func TestPubSub_two_consumers_slow_and_fast(t *testing.T) {
	const topic = "topic"
	bus := EventBus{}

	slow := testConsumer{}
	slow.subscribe(topic, &bus)
	defer func() { slow.done <- true }() // stop consumer

	normal := testConsumer{}
	normal.subscribe(topic, &bus)
	defer func() { normal.done <- true }() // stop consumer

	bus.Publish(topic, 1)
	<-slow.started   // wait consumer to receive the 1st message
	<-normal.started // wait consumer to receive the 1st message

	// normal consumer should consume 1, 2, 3, 4 & 5
	v := <-normal.out
	if v != 1 {
		t.Errorf("Expected normal consumer to have consumed 1, instead got %d.", v)
	}

	bus.Publish(topic, 2)
	v = <-normal.out
	if v != 2 {
		t.Errorf("Expected normal consumer to have consumed 2, instead got %d.", v)
	}

	bus.Publish(topic, 3)
	v = <-normal.out
	if v != 3 {
		t.Errorf("Expected normal consumer to have consumed 3, instead got %d.", v)
	}

	bus.Publish(topic, 4)
	v = <-normal.out
	if v != 4 {
		t.Errorf("Expected normal consumer to have consumed 4, instead got %d.", v)
	}

	bus.Publish(topic, 5)
	v = <-normal.out
	if v != 5 {
		t.Errorf("Expected normal consumer to have consumed 5, instead got %d.", v)
	}

	// slow consumer should consume 5 as the last value
	timeout := time.NewTimer(time.Second)
loop:
	for {
		select {
		case v = <-slow.out:
		case <-timeout.C:
			break loop
		}
	}
	if v != 5 {
		t.Errorf("Expected slow consumer to have consumed 5, instead got %d.", v)
	}
}

func TestPubSub_late_subscription_gets_latest_value(t *testing.T) {
	const topic = "topic"
	bus := EventBus{}

	// Given I publish to a topic
	bus.Publish(topic, 1)
	bus.Publish(topic, 2)

	// When I subscribe to the topic after the published event
	normal := testConsumer{}
	normal.subscribe(topic, &bus)
	defer func() { normal.done <- true }() // stop consumer

	<-normal.started // wait consumer to receive the 1st message

	// Then the new subscriber receives the latest published value
	v := <-normal.out
	if v != 2 {
		t.Errorf("Expected late consumer to have consumed latest value 2, instead got %d.", v)
	}

}

type testConsumer struct {
	ch        chan DataEvent
	out       chan int
	done      chan bool
	markStart sync.Once
	started   chan bool
}

func (r *testConsumer) subscribe(topic string, bus *EventBus) {
	r.out = make(chan int)
	r.ch = make(chan DataEvent)
	r.started = make(chan bool)
	r.done = make(chan bool)
	bus.Subscribe(topic, r.ch)
	go func() {
		var done bool
		for !done {
			select {
			case <-r.done:
				done = true
			case v := <-r.ch:
				r.markStart.Do(func() {
					r.started <- true
				})
				res := v.Data.(int)
				r.out <- res
			}
		}
	}()

}
