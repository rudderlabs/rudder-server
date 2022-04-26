package kafkaclient

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

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
	config           *config
}

// New returns a new Kafka client
func New(network, address string, opts ...Option) (*client, error) {
	conf := config{}
	for _, opt := range opts {
		opt.apply(&conf)
	}

	conf.defaults()
	dialer := kafka.Dialer{
		DualStack: true,
		Timeout:   conf.dialTimeout,
	}

	if conf.clientID != nil {
		dialer.ClientID = *conf.clientID
	}
	if conf.tlsConfig != nil {
		var err error
		dialer.TLS, err = conf.tlsConfig.build()
		if err != nil {
			return nil, err
		}
	}

	if conf.saslConfig != nil {
		var err error
		dialer.SASLMechanism, err = conf.saslConfig.build()
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
		go func() { // close asynchronously, if we block we might not respect the context
			_ = conn.Close()
		}()
	}()

	return nil
}

func (c *client) createTopic(
	ctx context.Context, topic string, numPartitions, replicationFactor int,
) error {
	conn, err := c.dialer.DialContext(ctx, c.network, c.address)
	if err != nil {
		return fmt.Errorf("could not dial %s/%s: %w", c.network, c.address, err)
	}

	defer func() {
		go func() { // close asynchronously, if we block we might not respect the context
			_ = conn.Close()
		}()
	}()

	broker, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("could not get controller: %w", err)
	}
	if broker.Host == "" {
		return fmt.Errorf("create topic: empty host")
	}

	controllerConn, err := kafka.DialContext(ctx, c.network, net.JoinHostPort(broker.Host, strconv.Itoa(broker.Port)))
	if err != nil {
		return fmt.Errorf("could not dial via controller: %w", err)
	}
	defer func() {
		go func() { // close asynchronously, if we block we might not respect the context
			_ = controllerConn.Close()
		}()
	}()

	return controllerConn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     numPartitions,
		ReplicationFactor: replicationFactor,
	})
}

func (c *client) listTopics(ctx context.Context) ([]string, error) {
	conn, err := c.dialer.DialContext(ctx, c.network, c.address)
	if err != nil {
		return nil, fmt.Errorf("could not dial %s/%s: %w", c.network, c.address, err)
	}

	defer func() {
		go func() { // close asynchronously, if we block we might not respect the context
			_ = conn.Close()
		}()
	}()

	partitions, err := conn.ReadPartitions() // @TODO does not honour context
	if err != nil {
		return nil, fmt.Errorf("could not read partitions: %w", err)
	}

	var topics []string
	for _, p := range partitions {
		topics = append(topics, p.Topic)
	}
	return topics, nil
}
