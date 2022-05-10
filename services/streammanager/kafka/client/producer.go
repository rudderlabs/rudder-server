package client

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/segmentio/kafka-go"
)

type ProducerConfig struct {
	ClientID string
	WriteTimeout,
	ReadTimeout,
	DefaultOpTimeout time.Duration
	MaxRetries  int
	Logger      Logger
	ErrorLogger Logger
}

func (c *ProducerConfig) defaults() {
	if c.WriteTimeout < 1 {
		c.WriteTimeout = 10 * time.Second
	}
	if c.ReadTimeout < 1 {
		c.ReadTimeout = 10 * time.Second
	}
	if c.MaxRetries < 1 {
		c.MaxRetries = 10
	}
}

// Producer provides a high-level API for producing messages to Kafka
type Producer struct {
	writer *kafka.Writer
	config ProducerConfig
}

// NewProducer instantiates a new producer. To use it asynchronously just do "go p.Publish(ctx, msgs)".
func (c *Client) NewProducer(topic string, producerConf ProducerConfig) (p *Producer, err error) { // skipcq: CRT-P0003
	producerConf.defaults()

	dialer := &net.Dialer{
		Timeout: c.config.DialTimeout,
	}
	transport := &kafka.Transport{
		DialTimeout: c.config.DialTimeout,
		Dial:        dialer.DialContext,
	}
	if producerConf.ClientID != "" {
		transport.ClientID = producerConf.ClientID
	} else if c.config.ClientID != "" {
		transport.ClientID = c.config.ClientID
	}
	if c.config.TLS != nil {
		transport.TLS, err = c.config.TLS.build()
		if err != nil {
			return nil, fmt.Errorf("could not build TLS configuration: %w", err)
		}
	}
	if c.config.SASL != nil {
		transport.SASL, err = c.config.SASL.build()
		if err != nil {
			return nil, fmt.Errorf("could not build SASL configuration: %w", err)
		}
	}

	p = &Producer{
		config: producerConf,
		writer: &kafka.Writer{
			Addr:                   c,
			Topic:                  topic,
			Balancer:               &kafka.ReferenceHash{}, // TODO replace: https://github.com/segmentio/kafka-go/pull/906
			BatchTimeout:           time.Nanosecond,
			WriteTimeout:           producerConf.WriteTimeout,
			ReadTimeout:            producerConf.ReadTimeout,
			MaxAttempts:            producerConf.MaxRetries,
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
		if p.writer != nil {
			done <- p.writer.Close()
		}
		close(done)
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

	if _, ok := ctx.Deadline(); !ok {
		var cancel func()
		ctx, cancel = context.WithTimeout(ctx, p.config.DefaultOpTimeout)
		defer cancel()
	}
	return p.writer.WriteMessages(ctx, messages...)
}
