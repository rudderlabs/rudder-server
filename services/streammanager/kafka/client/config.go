package client

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
		return "sha256"
	case ScramSHA512:
		return "sha512"
	default:
		panic(fmt.Errorf("scram hash generator out of the known domain %d", s))
	}
}

// ScramHashGeneratorFromString returns the proper ScramHashGenerator from its string counterpart
func ScramHashGeneratorFromString(s string) (ScramHashGenerator, error) {
	switch s {
	case "plain":
		return ScramPlainText, nil
	case "sha256":
		return ScramSHA256, nil
	case "sha512":
		return ScramSHA512, nil
	}
	var hg ScramHashGenerator
	return hg, fmt.Errorf("scram hash generator out of the known domain: %s", s)
}

type Config struct {
	ClientID    string
	DialTimeout time.Duration
	TLS         *TLS
	SASL        *SASL
}

func (c *Config) defaults() {
	if c.DialTimeout < 1 {
		c.DialTimeout = 10 * time.Second
	}
}

type TLS struct {
	Cert, Key,
	CACertificate []byte
	WithSystemCertPool,
	InsecureSkipVerify bool
}

func (c *TLS) build() (*tls.Config, error) {
	if len(c.CACertificate) == 0 && !c.InsecureSkipVerify && !c.WithSystemCertPool {
		return nil, fmt.Errorf("invalid TLS configuration, either provide certificates or skip validation")
	}

	conf := &tls.Config{ // skipcq: GSC-G402
		MinVersion: tls.VersionTLS11,
		MaxVersion: tls.VersionTLS12,
	}

	if c.InsecureSkipVerify {
		conf.InsecureSkipVerify = true // skipcq: GSC-G402
	}

	if c.WithSystemCertPool {
		caCertPool, err := x509.SystemCertPool()
		if err != nil {
			return nil, fmt.Errorf("could not copy of the system cert pool: %w", err)
		}

		conf.RootCAs = caCertPool
	}

	if len(c.CACertificate) > 0 {
		if conf.RootCAs == nil {
			conf.RootCAs = x509.NewCertPool()
		}
		if ok := conf.RootCAs.AppendCertsFromPEM(c.CACertificate); !ok {
			return nil, fmt.Errorf("could not append certs from PEM")
		}
	}

	if len(c.Cert) > 0 && len(c.Key) > 0 {
		certificate, err := tls.X509KeyPair(c.Cert, c.Key)
		if err != nil {
			return nil, fmt.Errorf("could not get TLS certificate: %w", err)
		}

		conf.Certificates = []tls.Certificate{certificate}
	}

	return conf, nil
}

type SASL struct {
	ScramHashGen       ScramHashGenerator
	Username, Password string
}

func (c *SASL) build() (mechanism sasl.Mechanism, err error) {
	switch c.ScramHashGen {
	case ScramPlainText:
		mechanism = plain.Mechanism{
			Username: c.Username,
			Password: c.Password,
		}
		return
	case ScramSHA256, ScramSHA512:
		algo := scram.SHA256
		if c.ScramHashGen == ScramSHA512 {
			algo = scram.SHA512
		}
		mechanism, err = scram.Mechanism(algo, c.Username, c.Password)
		return
	default:
		return nil, fmt.Errorf("scram hash generator out of the known domain: %v", c.ScramHashGen)
	}
}
