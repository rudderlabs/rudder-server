package client

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

// Logger specifies a logger used to report internal changes within the consumer
type Logger interface {
	Printf(format string, args ...interface{})
}

// MessageHeader is a key/value pair type representing headers set on records
type MessageHeader struct {
	Key   string
	Value []byte
}

// Message is a data structure representing a Kafka message
type Message struct {
	Key, Value []byte
	Topic      string
	Partition  int32
	Offset     int64
	Headers    []MessageHeader
	Timestamp  time.Time
}

type client struct {
	network, address string
	dialer           *kafka.Dialer
	config           *Config
}

// New returns a new Kafka client
func New(network, address string, conf Config) (*client, error) { // skipcq: RVV-B0011
	conf.defaults()

	dialer := kafka.Dialer{
		DualStack: true,
		Timeout:   conf.DialTimeout,
	}

	if conf.ClientID != "" {
		dialer.ClientID = conf.ClientID
	}
	if conf.TLS != nil {
		var err error
		dialer.TLS, err = conf.TLS.build()
		if err != nil {
			return nil, err
		}
	}

	if conf.SASL != nil {
		var err error
		dialer.SASLMechanism, err = conf.SASL.build()
		if err != nil {
			return nil, err
		}
	}

	return &client{
		network: network,
		address: address,
		dialer:  &dialer,
		config:  &conf,
	}, nil
}

// Network returns name of the network (for example, "tcp", "udp")
// see net.Addr interface
func (c *client) Network() string { return c.network }

// String returns string form of address (for example, "192.0.2.1:25", "[2001:db8::1]:80")
// see net.Addr interface
func (c *client) String() string { return c.address }

// Ping is used to check the connectivity only, then it discards the connection
func (c *client) Ping(ctx context.Context) error {
	conn, err := c.dialer.DialContext(ctx, c.network, c.address)
	if err != nil {
		return fmt.Errorf("could not dial %s/%s: %w", c.network, c.address, err)
	}

	defer func() {
		// close asynchronously, if we block we might not respect the context
		go func() { _ = conn.Close() }()
	}()

	return nil
}

func (c *client) createTopic(ctx context.Context, topic string, numPartitions, replicationFactor int) error {
	conn, err := c.dialer.DialContext(ctx, c.network, c.address)
	if err != nil {
		return fmt.Errorf("could not dial %s/%s: %w", c.network, c.address, err)
	}

	defer func() {
		// close asynchronously, if we block we might not respect the context
		go func() { _ = conn.Close() }()
	}()

	var (
		errors  = make(chan error, 1)
		brokers = make(chan kafka.Broker, 1)
	)

	go func() { // doing it asynchronously because conn.Controller() does not honour the context
		b, err := conn.Controller()
		if err != nil {
			errors <- fmt.Errorf("could not get controller: %w", err)
			return
		}
		if b.Host == "" {
			errors <- fmt.Errorf("create topic: empty host")
			return
		}
		brokers <- b
	}()

	var broker kafka.Broker
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err = <-errors:
		return err
	case broker = <-brokers:
	}

	controllerConn, err := kafka.DialContext(ctx, c.network, net.JoinHostPort(broker.Host, strconv.Itoa(broker.Port)))
	if err != nil {
		return fmt.Errorf("could not dial via controller: %w", err)
	}
	defer func() {
		// close asynchronously, if we block we might not respect the context
		go func() { _ = controllerConn.Close() }()
	}()

	go func() { // doing it asynchronously because controllerConn.CreateTopics() does not honour the context
		errors <- controllerConn.CreateTopics(kafka.TopicConfig{
			Topic:             topic,
			NumPartitions:     numPartitions,
			ReplicationFactor: replicationFactor,
		})
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err = <-errors:
		return err
	}
}

func (c *client) listTopics(ctx context.Context) ([]string, error) {
	conn, err := c.dialer.DialContext(ctx, c.network, c.address)
	if err != nil {
		return nil, fmt.Errorf("could not dial %s/%s: %w", c.network, c.address, err)
	}

	defer func() {
		// close asynchronously, if we block we might not respect the context
		go func() { _ = conn.Close() }()
	}()

	var (
		done   = make(chan []kafka.Partition, 1)
		errors = make(chan error, 1)
	)
	go func() { // doing it asynchronously because conn.ReadPartitions() does not honour the context
		partitions, err := conn.ReadPartitions()
		if err != nil {
			errors <- err
		} else {
			done <- partitions
		}
	}()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case err = <-errors:
		return nil, fmt.Errorf("could not read partitions: %w", err)
	case partitions := <-done:
		var topics []string
		for i := range partitions {
			topics = append(topics, fmt.Sprintf("%s [partition %d]", partitions[i].Topic, partitions[i].ID))
		}
		return topics, nil
	}
}
