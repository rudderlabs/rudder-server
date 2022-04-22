package kafkaclient

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
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

	if conf.tlsConfig != nil {
		certificate, err := tls.X509KeyPair(conf.tlsConfig.cert, conf.tlsConfig.key)
		if err != nil {
			return nil, fmt.Errorf("could not get TLS certificate: %w", err)
		}

		caCertPool := x509.NewCertPool()
		if ok := caCertPool.AppendCertsFromPEM(conf.tlsConfig.caCertificate); !ok {
			return nil, fmt.Errorf("could not append certs from PEM")
		}

		dialer.TLS = &tls.Config{
			Certificates: []tls.Certificate{certificate},
			RootCAs:      caCertPool,
		}
		if conf.tlsConfig.insecureSkipVerify {
			dialer.TLS.InsecureSkipVerify = true
		}
	}

	if conf.saslConfig != nil {
		switch conf.saslConfig.scramHashGen {
		case ScramPlainText:
			dialer.SASLMechanism = plain.Mechanism{
				Username: conf.saslConfig.username,
				Password: conf.saslConfig.password,
			}
		case ScramSHA256, ScramSHA512:
			algo := scram.SHA256
			if conf.saslConfig.scramHashGen == ScramSHA512 {
				algo = scram.SHA512
			}
			mechanism, err := scram.Mechanism(algo, conf.saslConfig.username, conf.saslConfig.password)
			if err != nil {
				return nil, fmt.Errorf("could not create scram mechanism: %w", err)
			}
			dialer.SASLMechanism = mechanism
		default:
			return nil, fmt.Errorf("scram hash generator out of the known domain: %v", conf.saslConfig.scramHashGen)
		}
	}

	return &client{
		network: network,
		address: address,
		dialer:  &dialer,
		config:  &conf,
	}, nil
}

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
