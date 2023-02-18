package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

type ProducerConfig struct {
	ClientID string
	WriteTimeout,
	ReadTimeout time.Duration
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
}

// Producer provides a high-level API for producing messages to Kafka
type Producer struct {
	writer *kafka.Writer
	config ProducerConfig
}

// NewProducer instantiates a new producer. To use it asynchronously just do "go p.Publish(ctx, msgs)".
func (c *Client) NewProducer(producerConf ProducerConfig) (p *Producer, err error) { // skipcq: CRT-P0003
	producerConf.defaults()

	transport := &kafka.Transport{
		DialTimeout: c.config.DialTimeout,
		Dial:        c.dialer.DialFunc,
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
			Addr:                   kafka.TCP(c.addresses...),
			Balancer:               &kafka.ReferenceHash{},
			BatchTimeout:           time.Nanosecond,
			WriteTimeout:           producerConf.WriteTimeout,
			ReadTimeout:            producerConf.ReadTimeout,
			MaxAttempts:            3,
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
		if msgs[i].Topic == "" {
			return fmt.Errorf("no topic provided for message %d", i)
		}
		headers := headers(msgs[i])
		messages[i] = kafka.Message{
			Topic:   msgs[i].Topic,
			Key:     msgs[i].Key,
			Value:   msgs[i].Value,
			Time:    msgs[i].Timestamp,
			Headers: headers,
		}
	}

	return p.writer.WriteMessages(ctx, messages...)
}

func headers(msg Message) (headers []kafka.Header) {
	if l := len(msg.Headers); l > 0 {
		headers = make([]kafka.Header, l)
		for k := range msg.Headers {
			headers[k] = kafka.Header{
				Key:   msg.Headers[k].Key,
				Value: msg.Headers[k].Value,
			}
		}
	}
	return headers
}

func isErrTemporary(err error) bool {
	isTransientNetworkError := errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.EPIPE)
	if isTransientNetworkError {
		return true
	}
	var tempError interface{ Temporary() bool }
	if errors.As(err, &tempError) {
		return tempError.Temporary()
	}
	if os.IsTimeout(err) {
		return true
	}
	return false
}

func IsProducerErrTemporary(err error) bool {
	var we kafka.WriteErrors
	if errors.As(err, &we) {
		for _, err := range we {
			// if at least one was temporary then we treat the whole batch as such
			if isErrTemporary(err) {
				return true
			}
		}
		return false
	}
	return isErrTemporary(err)
}
