package client

import (
	"context"
	"fmt"
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

type Client struct {
	network   string
	addresses []string
	dialer    *kafka.Dialer
	config    *Config
}

// New returns a new Kafka client
func New(network string, addresses []string, conf Config) (*Client, error) {
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

	return &Client{
		network:   network,
		addresses: addresses,
		dialer:    &dialer,
		config:    &conf,
	}, nil
}

// NewConfluentCloud returns a Kafka client pre-configured to connect to Confluent Cloud
func NewConfluentCloud(address, key, secret string, conf Config) (*Client, error) {
	conf.SASL = &SASL{
		ScramHashGen: ScramPlainText,
		Username:     key,
		Password:     secret,
	}
	conf.TLS = &TLS{
		WithSystemCertPool: true,
	}
	return New("tcp", []string{address}, conf)
}

// NewAzureEventHubs returns a Kafka client pre-configured to connect to Azure Event Hubs
func NewAzureEventHubs(address, connectionString string, conf Config) (*Client, error) {
	conf.SASL = &SASL{
		ScramHashGen: ScramPlainText,
		Username:     "$ConnectionString",
		Password:     connectionString,
	}
	conf.TLS = &TLS{
		WithSystemCertPool: true,
	}
	return New("tcp", []string{address}, conf)
}

// Ping is used to check the connectivity only, then it discards the connection
func (c *Client) Ping(ctx context.Context) error {
	address := kafka.TCP(c.addresses...).String()
	conn, err := c.dialer.DialContext(ctx, c.network, address)
	if err != nil {
		return fmt.Errorf("could not dial %s/%s: %w", c.network, address, err)
	}

	defer func() {
		// close asynchronously, if we block we might not respect the context
		go func() { _ = conn.Close() }()
	}()

	return nil
}
