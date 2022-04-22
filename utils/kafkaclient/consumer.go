package kafkaclient

import (
	"context"

	"github.com/segmentio/kafka-go"
)

// ConsumerOption is an abstraction used to allow the configuration of consumers
type ConsumerOption interface {
	apply(*kafka.ReaderConfig)
}

type withConsumerOption struct{ setup func(*kafka.ReaderConfig) }

func (w withConsumerOption) apply(c *kafka.ReaderConfig) { w.setup(c) }

// WithConsumerGroup sets the groupID for the consumer (cannot be used together with the partition since they are
// mutually exclusive).
func WithConsumerGroup(group string) ConsumerOption {
	return withConsumerOption{setup: func(c *kafka.ReaderConfig) {
		c.GroupID = group
	}}
}

// WithPartition sets the partition for the consumer (cannot be used together with the consumer group since they are
// mutually exclusive).
func WithPartition(partition int) ConsumerOption {
	return withConsumerOption{setup: func(c *kafka.ReaderConfig) {
		c.Partition = partition
	}}
}

// Consumer provides a high-level API for reading messages from Kafka
type Consumer struct {
	reader *kafka.Reader
}

// NewConsumer instantiates a new consumer.
func (c *client) NewConsumer(topic string, opts ...ConsumerOption) *Consumer {
	var conf kafka.ReaderConfig
	for _, opt := range opts {
		opt.apply(&conf)
	}

	conf.Topic = topic
	conf.Dialer = c.dialer
	conf.MaxWait = c.config.opTimeout

	return &Consumer{
		reader: kafka.NewReader(conf),
	}
}

// Close tries to close the consumer, but it will return sooner if the context is canceled.
// A routine in background will still try to close the producer since the underlying library does not support
// contexts on Close().
func (c *Consumer) Close(ctx context.Context) error {
	done := make(chan error, 1)
	go func() {
		done <- c.reader.Close()
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-done:
		return err
	}
}

// Receive reads and returns the next message from the consumer.
// The method blocks until a message becomes available, or an error occurs.
// The program may also specify a context to asynchronously cancel the blocking operation.
func (c *Consumer) Receive(ctx context.Context) (Message, error) {
	msg, err := c.reader.ReadMessage(ctx)
	if err != nil {
		return Message{}, err
	}

	var headers []MessageHeader
	if l := len(msg.Headers); l > 0 {
		headers = make([]MessageHeader, l)
		for i := range msg.Headers {
			headers[i] = MessageHeader{
				Key:   msg.Headers[i].Key,
				Value: msg.Headers[i].Value,
			}
		}
	}

	return Message{
		Key:       msg.Key,
		Value:     msg.Value,
		Topic:     msg.Topic,
		Partition: int32(msg.Partition),
		Offset:    msg.Offset,
		Headers:   headers,
		Timestamp: msg.Time,
	}, nil
}
