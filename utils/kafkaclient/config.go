package kafkaclient

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
)

type ScramHashGenerator uint8

const (
	ScramPlainText ScramHashGenerator = iota
	ScramSHA256
	ScramSHA512
)

func (s ScramHashGenerator) String() string {
	switch s {
	case ScramPlainText:
		return "plain"
	case ScramSHA256:
		return "sha-256"
	case ScramSHA512:
		return "sha-512"
	default:
		panic(fmt.Errorf("scram hash generator out of the known domain %d", s))
	}
}

// Option is an abstraction used to allow the configuration of a client
type Option interface {
	apply(*config)
}

type withOption struct{ setup func(*config) }

func (w withOption) apply(c *config) { w.setup(c) }

type config struct {
	clientID    string
	dialTimeout time.Duration
	tlsConfig   *tlsConfig
	saslConfig  *saslConfig
}

func (c *config) defaults() {
	if c.dialTimeout < 1 {
		c.dialTimeout = 10 * time.Second
	}
}

type tlsConfig struct {
	cert, key,
	caCertificate []byte
	insecureSkipVerify bool
}

func (c *tlsConfig) build() (*tls.Config, error) {
	loadCerts := len(c.cert) > 0 && len(c.key) > 0 && len(c.caCertificate) > 0

	if !loadCerts && !c.insecureSkipVerify {
		return nil, fmt.Errorf("invalid TLS configuration, either provide certificates or skip validation")
	}

	conf := &tls.Config{
		MinVersion: tls.VersionTLS11,
		MaxVersion: tls.VersionTLS12,
	}
	if c.insecureSkipVerify {
		conf.InsecureSkipVerify = true
	}

	if loadCerts {
		caCertPool := x509.NewCertPool()
		if ok := caCertPool.AppendCertsFromPEM(c.caCertificate); !ok {
			return nil, fmt.Errorf("could not append certs from PEM")
		}

		certificate, err := tls.X509KeyPair(c.cert, c.key)
		if err != nil {
			return nil, fmt.Errorf("could not get TLS certificate: %w", err)
		}

		conf.RootCAs = caCertPool
		conf.Certificates = []tls.Certificate{certificate}
	}

	return conf, nil
}

type saslConfig struct {
	scramHashGen       ScramHashGenerator
	username, password string
}

func (c *saslConfig) build() (mechanism sasl.Mechanism, err error) {
	switch c.scramHashGen {
	case ScramPlainText:
		mechanism = plain.Mechanism{
			Username: c.username,
			Password: c.password,
		}
		return
	case ScramSHA256, ScramSHA512:
		algo := scram.SHA256
		if c.scramHashGen == ScramSHA512 {
			algo = scram.SHA512
		}
		mechanism, err = scram.Mechanism(algo, c.username, c.password)
		return
	default:
		return nil, fmt.Errorf("scram hash generator out of the known domain: %v", c.scramHashGen)
	}
}

// WithClientID is used to set a unique identifier for client connections established by this client's Dialer
func WithClientID(clientID string) Option {
	return withOption{setup: func(c *config) {
		c.clientID = clientID
	}}
}

// WithDialTimeout sets the maximum amount of time a dial will wait for a connect to complete
func WithDialTimeout(t time.Duration) Option {
	return withOption{setup: func(c *config) {
		c.dialTimeout = t
	}}
}

// WithTLS adds TLS support
func WithTLS(cert, key, caCertificate []byte, insecureSkipVerify bool) Option {
	return withOption{setup: func(c *config) {
		c.tlsConfig = &tlsConfig{
			cert:               cert,
			key:                key,
			caCertificate:      caCertificate,
			insecureSkipVerify: insecureSkipVerify,
		}
	}}
}

// WithSASL adds SASL support
func WithSASL(hg ScramHashGenerator, username, password string) Option {
	return withOption{setup: func(c *config) {
		c.saslConfig = &saslConfig{
			scramHashGen: hg,
			username:     username,
			password:     password,
		}
	}}
}
