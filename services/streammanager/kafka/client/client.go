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

// NewConfluentCloud returns a Kafka client pre-configured to connect to Confluent Cloud
func NewConfluentCloud(address, key, secret string, conf Config) (*client, error) { // skipcq: RVV-B0011
	conf.SASL = &SASL{
		ScramHashGen: ScramPlainText,
		Username:     key,
		Password:     secret,
	}
	conf.TLS = &TLS{
		WithSystemCertPool: true,
	}
	return New("tcp", address, conf)
}

// NewAzureEventHubs returns a Kafka client pre-configured to connect to Azure Event Hubs
func NewAzureEventHubs(address, connectionString string, conf Config) (*client, error) { // skipcq: RVV-B0011
	conf.SASL = &SASL{
		ScramHashGen: ScramPlainText,
		Username:     "$ConnectionString",
		Password:     connectionString,
	}
	conf.TLS = &TLS{
		WithSystemCertPool: true,
	}
	return New("tcp", address, conf)
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
