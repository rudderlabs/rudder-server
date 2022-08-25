package pubsub

import (
	"context"
	"testing"
)

func TestPubSub_two_consumers_slow_and_fast(t *testing.T) {
	const topic = "topic"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	bus := PublishSubscriber{}

	slow := bus.Subscribe(ctx, topic)
	normal := bus.Subscribe(ctx, topic)

	bus.Publish(topic, 1)
	v := <-normal
	if v.Data.(int) != 1 {
		t.Errorf("Expected normal consumer to have consumed 1, instead got %d.", v.Data.(int))
	}
	v = <-slow
	if v.Data.(int) != 1 {
		t.Errorf("Expected slow consumer to have consumed 1, instead got %d.", v.Data.(int))
	}

	t.Log("normal consumer gets ahead by 4 publishes")
	numbers := []int{2, 3, 4, 5}
	for _, n := range numbers {
		bus.Publish(topic, n)
		// normal consumer should consume 1, 2, 3, 4 & 5
		v := <-normal
		if v.Data.(int) != n {
			t.Errorf("Expected normal consumer to have consumed %d, instead got %d.", n, v.Data.(int))
		}
	}

	// slow consumer
	t.Log("slow consumer should consume only the latest one, 5")

	v = <-slow

	if v.Data.(int) < 5 { // due to pubsub internal channels, the slow consumer may consume yet another value before consuming 5
		v = <-slow
	}

	if v.Data.(int) != 5 {
		t.Errorf("Expected slow consumer to have consumed 5, instead got %d.", v.Data.(int))
	}
}

func TestPubSub_late_subscription_gets_latest_value(t *testing.T) {
	const topic = "topic"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	bus := &PublishSubscriber{}

	// Given I publish to a topic
	bus.Publish(topic, 1)
	bus.Publish(topic, 2)

	// When I subscribe to the topic after the published event
	normal := bus.Subscribe(ctx, topic)

	// Then the new subscriber receives the latest published value
	v := <-normal
	if v.Data.(int) != 2 {
		t.Errorf("Expected late consumer to have consumed latest value 2, instead got %d.", v.Data.(int))
	}
}

func TestPubSub_Close(t *testing.T) {
	const topic = "topic"

	bus := &PublishSubscriber{}

	// Given I publish to a topic
	bus.Publish(topic, 1)

	// When I subscribe to the topic after the published event
	ch := bus.Subscribe(context.Background(), topic)

	bus.Close()

	v := <-ch
	if v.Data.(int) != 1 {
		t.Errorf("Expected late consumer to have consumed latest value 1, instead got %d.", v.Data.(int))
	}

	_, ok := <-ch
	if ok {
		t.Error("Expected channel to be closed.")
	}
}
