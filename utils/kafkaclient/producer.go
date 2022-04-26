package kafkaclient

import (
	"context"
	"net"

	"github.com/segmentio/kafka-go"
)

// Producer provides a high-level API for producing messages to Kafka
type Producer struct {
	writer *kafka.Writer
}

// NewProducer instantiates a new producer. To use it asynchronously just do "go p.Publish(ctx, msgs)".
func (c *client) NewProducer(topic string) *Producer {
	return &Producer{
		writer: &kafka.Writer{
			Addr:                   c,
			Topic:                  topic,
			Balancer:               &kafka.Hash{},
			BatchTimeout:           c.config.batchTimeout,
			WriteTimeout:           c.config.writeTimeout,
			ReadTimeout:            c.config.readTimeout,
			RequiredAcks:           kafka.RequireAll,
			AllowAutoTopicCreation: true,
			Async:                  false,
			Compression:            0,
			Transport: &kafka.Transport{
				DialTimeout: c.config.dialTimeout,
				Dial: func(ctx context.Context, network string, address string) (net.Conn, error) {
					return c.dialer.DialContext(ctx, network, address)
				},
			},
		},
	}
}

// Close tries to close the producer, but it will return sooner if the context is canceled.
// A routine in background will still try to close the producer since the underlying library does not support
// contexts on Close().
func (p *Producer) Close(ctx context.Context) error {
	done := make(chan error, 1)
	go func() {
		done <- p.writer.Close()
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-done:
		return err
	}
}

// Publish allows the production of one or more message to Kafka.
// To use it asynchronously just do "go p.Publish(ctx, msgs)".
func (p *Producer) Publish(ctx context.Context, msgs ...Message) error {
	messages := make([]kafka.Message, len(msgs))
	for i := range msgs {
		var headers []kafka.Header
		if l := len(msgs[i].Headers); l > 0 {
			headers = make([]kafka.Header, l)
			for k := range msgs[i].Headers {
				headers[k] = kafka.Header{
					Key:   msgs[i].Headers[k].Key,
					Value: msgs[i].Headers[k].Value,
				}
			}
		}
		messages[i] = kafka.Message{
			Key:     msgs[i].Key,
			Value:   msgs[i].Value,
			Time:    msgs[i].Timestamp,
			Headers: headers,
		}
	}
	return p.writer.WriteMessages(ctx, messages...)
}
