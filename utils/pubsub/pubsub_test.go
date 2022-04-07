package pubsub

import (
	"context"
	"testing"
)

func TestPubSub_two_consumers_slow_and_fast(t *testing.T) {
	const topic = "topic"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	bus := NewPublishSubscriber(ctx)

	slow := testConsumer{}
	slow.subscribe("slow", topic, bus)

	normal := testConsumer{}
	normal.subscribe("normal", topic, bus)

	bus.Publish(topic, 1)
	t.Log("slow consumer started")
	normal.startConsuming(ctx)
	t.Log("normal consumer started")

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

	slow.startConsuming(ctx)
	// slow consumer should consume 1 & 5
	v = <-slow.out
	if v != 1 {
		t.Errorf("Expected slow consumer to have consumed 1, instead got %d.", v)
	}
	v = <-slow.out
	if v != 5 {
		t.Errorf("Expected slow consumer to have consumed 5, instead got %d.", v)
	}

}

func TestPubSub_late_subscription_gets_latest_value(t *testing.T) {
	const topic = "topic"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	bus := NewPublishSubscriber(ctx)

	// Given I publish to a topic
	bus.Publish(topic, 1)
	bus.Publish(topic, 2)

	// When I subscribe to the topic after the published event
	normal := testConsumer{}
	normal.subscribe("normal", topic, bus)
	normal.startConsuming(ctx)

	// Then the new subscriber receives the latest published value
	v := <-normal.out
	if v != 2 {
		t.Errorf("Expected late consumer to have consumed latest value 2, instead got %d.", v)
	}

}

type testConsumer struct {
	name string
	in   chan DataEvent
	out  chan int
}

// startConsuming starts the consumer goroutine and blocks until the first value is received
func (r *testConsumer) startConsuming(ctx context.Context) {
	firstValueReceived := make(chan struct{})
	go func() {
		var count int
		for {
			select {
			case <-ctx.Done():
				return
			case v := <-r.in:
				if count == 0 {
					close(firstValueReceived)
				}
				res := v.Data.(int)
				r.out <- res
				count++
			}
		}
	}()
	<-firstValueReceived
}

func (r *testConsumer) subscribe(name string, topic string, bus PublishSubscriber) {
	r.name = name
	r.out = make(chan int)
	r.in = make(chan DataEvent)
	bus.Subscribe(topic, r.in)
}
