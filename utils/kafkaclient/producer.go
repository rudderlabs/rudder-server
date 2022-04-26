package kafkaclient

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/segmentio/kafka-go"
)

// ProducerOption is an abstraction used to allow the configuration of consumers
type ProducerOption interface {
	apply(*producerConfig)
}

type withProducerOption struct{ setup func(*producerConfig) }

func (w withProducerOption) apply(c *producerConfig) { w.setup(c) }

// WithProducerClientID sets a unique identifier that the transport communicates to the brokers when it
// sends requests.
func WithProducerClientID(clientID string) ProducerOption {
	return withProducerOption{setup: func(c *producerConfig) {
		c.clientID = clientID
	}}
}

// WithProducerBatchTimeout sets the time limit on how often incomplete message batches will be flushed to
//	// kafka.
func WithProducerBatchTimeout(t time.Duration) ProducerOption {
	return withProducerOption{setup: func(c *producerConfig) {
		c.batchTimeout = t
	}}
}

// WithProducerWriteTimeout sets the timeout for write operations performed by the Writer
func WithProducerWriteTimeout(t time.Duration) ProducerOption {
	return withProducerOption{setup: func(c *producerConfig) {
		c.writeTimeout = t
	}}
}

// WithProducerReadTimeout sets the timeout for write operations performed by the Writer
func WithProducerReadTimeout(t time.Duration) ProducerOption {
	return withProducerOption{setup: func(c *producerConfig) {
		c.readTimeout = t
	}}
}

type producerConfig struct {
	clientID string
	batchTimeout,
	writeTimeout,
	readTimeout time.Duration
}

func (c *producerConfig) defaults() {
	if c.batchTimeout < 1 {
		c.batchTimeout = time.Second
	}
	if c.writeTimeout < 1 {
		c.writeTimeout = 10 * time.Second
	}
	if c.readTimeout < 1 {
		c.readTimeout = 10 * time.Second
	}
}

// Producer provides a high-level API for producing messages to Kafka
type Producer struct {
	writer *kafka.Writer
}

// NewProducer instantiates a new producer. To use it asynchronously just do "go p.Publish(ctx, msgs)".
func (c *client) NewProducer(topic string, opts ...ProducerOption) (p *Producer, err error) {
	producerConf := producerConfig{}
	for _, opt := range opts {
		opt.apply(&producerConf)
	}
	producerConf.defaults()

	dialer := &net.Dialer{
		Timeout: c.config.dialTimeout,
	}
	transport := &kafka.Transport{
		DialTimeout: c.config.dialTimeout,
		Dial:        dialer.DialContext,
	}
	if producerConf.clientID != "" {
		transport.ClientID = producerConf.clientID
	}
	if c.config.tlsConfig != nil {
		transport.TLS, err = c.config.tlsConfig.build()
		if err != nil {
			return nil, fmt.Errorf("could not build TLS configuration: %w", err)
		}
	}
	if c.config.saslConfig != nil {
		transport.SASL, err = c.config.saslConfig.build()
		if err != nil {
			return nil, fmt.Errorf("could not build SASL configuration: %w", err)
		}
	}

	p = &Producer{
		writer: &kafka.Writer{
			Addr:                   c,
			Topic:                  topic,
			Balancer:               &kafka.Hash{},
			BatchTimeout:           producerConf.batchTimeout,
			WriteTimeout:           producerConf.writeTimeout,
			ReadTimeout:            producerConf.readTimeout,
			RequiredAcks:           kafka.RequireAll,
			AllowAutoTopicCreation: true,
			Async:                  false,
			Compression:            0,
			Transport:              transport,
		},
	}
	return
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
