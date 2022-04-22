package kafkaclient

import "time"

type ScramHashGenerator uint8

const (
	ScramPlainText ScramHashGenerator = iota
	ScramSHA256
	ScramSHA512
)

const (
	defaultDialTimeout = 10 * time.Second
	defaultOpTimeout   = 2 * time.Second
)

// Option is an abstraction used to allow the configuration of a client
type Option interface {
	apply(*config)
}

type withOption struct{ setup func(*config) }

func (w withOption) apply(c *config) { w.setup(c) }

type config struct {
	dialTimeout time.Duration
	opTimeout   time.Duration
	tlsConfig   *tlsConfig
	saslConfig  *saslConfig
}

func (c *config) defaults() {
	if c.dialTimeout < 1 {
		c.dialTimeout = defaultDialTimeout
	}
	if c.opTimeout < 1 {
		c.opTimeout = defaultOpTimeout
	}
}

type tlsConfig struct {
	cert, key,
	caCertificate []byte
	insecureSkipVerify bool
}

type saslConfig struct {
	scramHashGen       ScramHashGenerator
	username, password string
}

// WithDialTimeout sets the maximum amount of time a dial will wait for a connect to complete
func WithDialTimeout(t time.Duration) Option {
	return withOption{setup: func(c *config) {
		c.dialTimeout = t
	}}
}

// WithOpTimeout sets the maximum amount of time for operations to complete (e.g. read, write, wait for batches)
func WithOpTimeout(t time.Duration) Option {
	return withOption{setup: func(c *config) {
		c.opTimeout = t
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
