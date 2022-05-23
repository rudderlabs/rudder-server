package client

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
)

type ConsumerStartOffset int64

const (
	// LastOffset is the most recent offset available for a partition
	LastOffset ConsumerStartOffset = iota
	// FirstOffset is the least recent offset available for a partition
	FirstOffset
)

type ConsumerConfig struct {
	GroupID             string
	Partition           int
	StartOffset         ConsumerStartOffset
	CommitInterval      time.Duration
	FetchBatchesMaxWait time.Duration
	Logger              Logger
	ErrorLogger         Logger
}

// Consumer provides a high-level API for reading messages from Kafka
type Consumer struct {
	reader *kafka.Reader
}

// NewConsumer instantiates a new consumer.
func (c *Client) NewConsumer(topic string, conf ConsumerConfig) *Consumer { // skipcq: CRT-P0003
	var readerConf kafka.ReaderConfig

	readerConf.Brokers = c.addresses
	readerConf.Topic = topic
	readerConf.Dialer = c.dialer

	readerConf.GroupID = conf.GroupID
	readerConf.Partition = conf.Partition
	readerConf.CommitInterval = conf.CommitInterval
	readerConf.MaxWait = conf.FetchBatchesMaxWait
	readerConf.StartOffset = kafka.FirstOffset
	if conf.StartOffset == LastOffset {
		readerConf.StartOffset = kafka.LastOffset
	}

	readerConf.Logger = conf.Logger
	readerConf.ErrorLogger = conf.ErrorLogger

	return &Consumer{
		reader: kafka.NewReader(readerConf),
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
