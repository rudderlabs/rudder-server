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
func NewConfluentCloud(addresses []string, key, secret string, conf Config) (*Client, error) {
	conf.SASL = &SASL{
		ScramHashGen: ScramPlainText,
		Username:     key,
		Password:     secret,
	}
	conf.TLS = &TLS{
		WithSystemCertPool: true,
	}
	return New("tcp", addresses, conf)
}

// NewAzureEventHubs returns a Kafka client pre-configured to connect to Azure Event Hubs
// addresses should be in the form of "host:port" where the port is usually 9093 on Azure Event Hubs
// Also make sure to select at least the Standard tier since the Basic tier does not support Kafka
func NewAzureEventHubs(addresses []string, connectionString string, conf Config) (*Client, error) {
	conf.SASL = &SASL{
		ScramHashGen: ScramPlainText,
		Username:     "$ConnectionString",
		Password:     connectionString,
	}
	conf.TLS = &TLS{
		WithSystemCertPool: true,
	}
	return New("tcp", addresses, conf)
}

// Ping is used to check the connectivity only, then it discards the connection
// Ping ensures that at least one of the provided addresses is reachable.
func (c *Client) Ping(ctx context.Context) error {
	var lastErr error
	for _, addr := range c.addresses {
		conn, err := c.dialer.DialContext(ctx, c.network, kafka.TCP(addr).String())
		if err == nil {
			go func() { _ = conn.Close() }()
			// we can connect to at least one address, no need to check all of them
			return nil
		}
		lastErr = err
	}

	return fmt.Errorf(
		"could not dial any of the addresses %s/%s: %w", c.network, kafka.TCP(c.addresses...).String(), lastErr,
	)
}
