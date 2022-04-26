package kafkaclient

import "time"

type ScramHashGenerator uint8

const (
	ScramPlainText ScramHashGenerator = iota
	ScramSHA256
	ScramSHA512
)

const (
	defaultDialTimeout  = 10 * time.Second
	defaultBatchTimeout = 10 * time.Second
	defaultWriteTimeout = 10 * time.Second
	defaultReadTimeout  = 10 * time.Second
)

// Option is an abstraction used to allow the configuration of a client
type Option interface {
	apply(*config)
}

type withOption struct{ setup func(*config) }

func (w withOption) apply(c *config) { w.setup(c) }

type config struct {
	dialTimeout  time.Duration
	batchTimeout time.Duration
	writeTimeout time.Duration
	readTimeout  time.Duration
	tlsConfig    *tlsConfig
	saslConfig   *saslConfig
}

func (c *config) defaults() {
	if c.dialTimeout < 1 {
		c.dialTimeout = defaultDialTimeout
	}
	if c.batchTimeout < 1 {
		c.batchTimeout = defaultBatchTimeout
	}
	if c.writeTimeout < 1 {
		c.writeTimeout = defaultWriteTimeout
	}
	if c.readTimeout < 1 {
		c.readTimeout = defaultReadTimeout
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

// WithBatchTimeout sets the maximum amount of time for batch operations to complete
func WithBatchTimeout(t time.Duration) Option {
	return withOption{setup: func(c *config) {
		c.batchTimeout = t
	}}
}

// WithWriteTimeout sets the maximum amount of time for write operations to complete
func WithWriteTimeout(t time.Duration) Option {
	return withOption{setup: func(c *config) {
		c.writeTimeout = t
	}}
}

// WithReadTimeout sets the maximum amount of time for read operations to complete
func WithReadTimeout(t time.Duration) Option {
	return withOption{setup: func(c *config) {
		c.readTimeout = t
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
