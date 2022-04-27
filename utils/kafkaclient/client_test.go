package kafkaclient

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/testhelper/destination"
)

func TestClient_Ping(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	kafkaContainer, err := destination.SetupKafka(pool, &testCleanup{t}, destination.WithLogger(t))
	require.NoError(t, err)

	kafkaHost := fmt.Sprintf("localhost:%s", kafkaContainer.Port)
	c, err := New("tcp", kafkaHost)
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, c.Ping(ctx))

	require.NoError(t, kafkaContainer.Destroy())
	err = c.Ping(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "connection refused")
}

func TestClient_ProducerBatch_ConsumerGroup(t *testing.T) {
	// Prepare cluster - Zookeeper + 3 Kafka brokers
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	kafkaContainer, err := destination.SetupKafka(pool, &testCleanup{t},
		destination.WithLogger(t),
		destination.WithBrokers(3))
	require.NoError(t, err)

	kafkaHost := fmt.Sprintf("localhost:%s", kafkaContainer.Port)
	c, err := New("tcp", kafkaHost)
	require.NoError(t, err)

	var (
		messagesWaitGroup   sync.WaitGroup
		gracefulTermination sync.WaitGroup
		c01Count, c02Count  int32
		noOfMessages        = 50
		ctx, cancel         = context.WithCancel(context.Background())
	)

	t.Cleanup(gracefulTermination.Wait)
	t.Cleanup(cancel)

	// Check connectivity and try to create the desired topic until the brokers are up and running (max 30s)
	require.NoError(t, c.Ping(ctx))
	require.Eventually(t, func() bool {
		err := c.createTopic(ctx, t.Name(), 2, 3) // partitions = 2, replication factor = 3
		if err != nil {
			t.Logf("Could not create topic: %v", err)
		}
		return err == nil
	}, 30*time.Second, time.Second)

	// Check that the topic has been created with the right number of partitions
	topics, err := c.listTopics(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{
		t.Name() + " [partition 0]",
		t.Name() + " [partition 1]",
	}, topics)

	// Produce X messages in a single batch
	producerOpts := []ProducerOption{
		WithProducerClientID("producer-01"),
		WithProducerBatchTimeout(time.Second),
		WithProducerWriteTimeout(5 * time.Second),
		WithProducerWriteTimeout(5 * time.Second),
	}
	if testing.Verbose() {
		producerOpts = append(producerOpts,
			WithProducerLogger(&testLogger{t}),
			WithProducerErrorLogger(&testLogger{t}),
		)
	}
	p, err := c.NewProducer(t.Name(), producerOpts...)
	require.NoError(t, err)
	publishMessages(ctx, t, p, noOfMessages)
	messagesWaitGroup.Add(noOfMessages)

	// Starting consumers with group-01 and FirstOffset
	var (
		// The ticker is used so that the test won't end as long as we keep getting messages since the consumers
		// will reset the ticker each time they receive a message
		tickerMu    sync.Mutex
		tickerReset = 10 * time.Second
		ticker      = time.NewTicker(30 * time.Second)
	)
	consumerOpts := []ConsumerOption{
		WithConsumerGroup("group-01"),
		WithConsumerStartOffset(FirstOffset),
		WithConsumerFetchBatchesMaxWait(10 * time.Second),
		WithConsumerCommitInterval(time.Second), // to make the test faster instead of committing each single message
	}
	if testing.Verbose() {
		consumerOpts = append(consumerOpts,
			WithConsumerLogger(&testLogger{t}),
			WithConsumerErrorLogger(&testLogger{t}),
		)
	}
	consume := func(c *Consumer, id string, count *int32) {
		defer gracefulTermination.Done()
		for {
			_, err := c.Receive(ctx)
			if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
				t.Logf("Closing %s: %v", id, err)
				return
			}
			require.NoError(t, err)
			t.Logf("Got a message on %s", id)
			tickerMu.Lock()
			ticker.Reset(tickerReset)
			tickerMu.Unlock()
			_ = atomic.AddInt32(count, 1)
			messagesWaitGroup.Done()
		}
	}
	closeConsumer := func(c *Consumer, id string) func() {
		return func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := c.Close(ctx); err != nil {
				t.Logf("Error closing %s: %v", id, err)
			}
		}
	}

	c01 := c.NewConsumer(t.Name(), consumerOpts...)
	t.Cleanup(closeConsumer(c01, "c01"))
	gracefulTermination.Add(1)
	go consume(c01, "c01", &c01Count)

	c02 := c.NewConsumer(t.Name(), consumerOpts...)
	t.Cleanup(closeConsumer(c02, "c02"))
	gracefulTermination.Add(1)
	go consume(c02, "c02", &c02Count)

	done := make(chan struct{})
	go func() {
		messagesWaitGroup.Wait() // this is satisfied once the consumers receive at least "noOfMessages" messages
		close(done)
	}()

	select {
	case <-done:
		require.Greater(t, atomic.LoadInt32(&c01Count), int32(0))
		require.Greater(t, atomic.LoadInt32(&c02Count), int32(0))
		require.EqualValues(t, noOfMessages, atomic.LoadInt32(&c01Count)+atomic.LoadInt32(&c02Count))
	// the test won't end as long as we keep getting messages since the consumers reset the ticker
	// when they receive a message
	case <-ticker.C:
		t.Error("Could not complete within timeout")
	}

	t.Logf("Messages consumed by c01: %d", atomic.LoadInt32(&c01Count))
	t.Logf("Messages consumed by c02: %d", atomic.LoadInt32(&c02Count))
}

func publishMessages(ctx context.Context, t *testing.T, p *Producer, noOfMessages int) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := p.Close(ctx); err != nil {
			t.Logf("Error closing producer: %v", err)
		}
	})

	messages := make([]Message, noOfMessages)
	for i := 0; i < noOfMessages; i++ {
		messages[i] = Message{
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(fmt.Sprintf("value-%d", i)),
		}
	}

	pubCtx, pubCancel := context.WithTimeout(ctx, 10*time.Second)
	err := p.Publish(pubCtx, messages...)
	pubCancel()
	require.NoError(t, err)
	t.Logf("Messages published (%d)", noOfMessages)
}

type testLogger struct{ *testing.T }

func (l *testLogger) Printf(format string, args ...interface{}) {
	l.Helper()
	l.Logf(format, args...)
}

type testCleanup struct{ *testing.T }

func (t *testCleanup) Defer(fn func() error) {
	t.Cleanup(func() {
		if err := fn(); err != nil {
			t.Log(err)
		}
	})
}
