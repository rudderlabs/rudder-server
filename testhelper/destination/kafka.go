package destination

import (
	_ "encoding/json"
	"fmt"
	"strconv"

	_ "github.com/lib/pq"
	"github.com/ory/dockertest/v3"
	dc "github.com/ory/dockertest/v3/docker"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-server/testhelper"
)

type scramHashGenerator uint8

const (
	scramPlainText scramHashGenerator = iota
	scramSHA256
	scramSHA512

	kafkaClientPort = "9092"
)

type User struct {
	Username, Password string
}

type Option interface {
	apply(*config)
}

type withOption struct{ setup func(*config) }

func (w withOption) apply(c *config) { w.setup(c) }

type SASLConfig struct {
	BrokerUser                   User
	Users                        []User
	CertificatePassword          string
	KeyStorePath, TrustStorePath string

	hashType scramHashGenerator
}

type config struct {
	logger                     Logger
	brokers                    uint
	saslConfig                 *SASLConfig
	network                    *dc.Network
	dontUseDockerHostListeners bool
}

func (c *config) defaults() {
	if c.logger == nil {
		c.logger = &NOPLogger{}
	}
	if c.brokers < 1 {
		c.brokers = 1
	}
}

// WithLogger allows to set a logger that prints debugging information
func WithLogger(l Logger) Option {
	return withOption{setup: func(c *config) {
		c.logger = l
	}}
}

// WithBrokers allows to set the number of brokers in the cluster
func WithBrokers(noOfBrokers uint) Option {
	return withOption{setup: func(c *config) {
		c.brokers = noOfBrokers
	}}
}

// WithSASLPlain is used to configure SASL authentication (PLAIN)
func WithSASLPlain(conf *SASLConfig) Option {
	return withSASL(scramPlainText, conf)
}

// WithSASLScramSHA256 is used to configure SASL authentication (Scram SHA-256)
func WithSASLScramSHA256(conf *SASLConfig) Option {
	return withSASL(scramSHA256, conf)
}

// WithSASLScramSHA512 is used to configure SASL authentication (Scram SHA-512)
func WithSASLScramSHA512(conf *SASLConfig) Option {
	return withSASL(scramSHA512, conf)
}

func withSASL(hashType scramHashGenerator, conf *SASLConfig) Option {
	conf.hashType = hashType
	return withOption{setup: func(c *config) {
		c.saslConfig = conf
	}}
}

// WithNetwork allows to set a docker network to use for the cluster
func WithNetwork(network *dc.Network) Option {
	return withOption{setup: func(c *config) {
		c.network = network
	}}
}

// WithoutDockerHostListeners allows to not set the advertised listener to the host mapped port
func WithoutDockerHostListeners() Option {
	return withOption{setup: func(c *config) {
		c.dontUseDockerHostListeners = true
	}}
}

type KafkaResource struct {
	Port string

	pool       *dockertest.Pool
	containers []*dockertest.Resource
}

func (k *KafkaResource) Destroy() error {
	g := errgroup.Group{}
	for i := range k.containers {
		i := i
		g.Go(func() error {
			return k.pool.Purge(k.containers[i])
		})
	}
	return g.Wait()
}

func SetupKafka(pool *dockertest.Pool, cln Cleaner, opts ...Option) (*KafkaResource, error) {
	// lock so no two tests can run at the same time and try to listen on the same port
	var c config
	for _, opt := range opts {
		opt.apply(&c)
	}
	c.defaults()

	network := c.network
	if c.network == nil {
		var err error
		network, err = pool.Client.CreateNetwork(dc.CreateNetworkOptions{Name: "kafka_network"})
		if err != nil {
			return nil, fmt.Errorf("could not create docker network: %w", err)
		}
		cln.Cleanup(func() {
			if err := pool.Client.RemoveNetwork(network.ID); err != nil {
				cln.Log(fmt.Errorf("could not remove kafka network: %w", err))
			}
		})
	}

	zookeeperPortInt, err := testhelper.GetFreePort()
	if err != nil {
		return nil, err
	}
	zkImage := "bitnami/zookeeper"
	zookeeperPort := fmt.Sprintf("%s/tcp", strconv.Itoa(zookeeperPortInt))
	zookeeperContainer, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: zkImage,
		Tag:        "latest",
		NetworkID:  network.ID,
		Hostname:   "zookeeper",
		PortBindings: map[dc.Port][]dc.PortBinding{
			"2181/tcp": {{HostIP: "zookeeper", HostPort: zookeeperPort}},
		},
		Env: []string{"ALLOW_ANONYMOUS_LOGIN=yes"},
	})
	if err != nil {
		return nil, err
	}
	cln.Cleanup(func() {
		if err := pool.Purge(zookeeperContainer); err != nil {
			cln.Log("Could not purge resource", err)
		}
	})

	c.logger.Log("Zookeeper localhost port", zookeeperContainer.GetPort("2181/tcp"))

	bootstrapServers := ""
	for i := uint(1); i <= c.brokers; i++ {
		bootstrapServers += fmt.Sprintf("kafka%d:9090,", i)
	}
	bootstrapServers = bootstrapServers[:len(bootstrapServers)-1] // removing trailing comma

	envVariables := []string{
		"KAFKA_CFG_ZOOKEEPER_CONNECT=zookeeper:2181",
		"KAFKA_CFG_INTER_BROKER_LISTENER_NAME=INTERNAL",
		"ALLOW_PLAINTEXT_LISTENER=yes",
		"BOOTSTRAP_SERVERS=" + bootstrapServers,
	}

	var mounts []string
	if c.saslConfig != nil {
		if c.saslConfig.BrokerUser.Username == "" {
			return nil, fmt.Errorf("SASL broker user must be provided")
		}
		if len(c.saslConfig.Users) < 1 {
			return nil, fmt.Errorf("SASL users must be provided")
		}
		if c.saslConfig.CertificatePassword == "" {
			return nil, fmt.Errorf("SASL certificate password cannot be empty")
		}
		if c.saslConfig.KeyStorePath == "" {
			return nil, fmt.Errorf("SASL keystore path cannot be empty")
		}
		if c.saslConfig.TrustStorePath == "" {
			return nil, fmt.Errorf("SASL truststore path cannot be empty")
		}

		mounts = []string{
			c.saslConfig.KeyStorePath + ":/opt/bitnami/kafka/config/certs/kafka.keystore.jks",
			c.saslConfig.TrustStorePath + ":/opt/bitnami/kafka/config/certs/kafka.truststore.jks",
		}

		var users, passwords string
		for _, user := range c.saslConfig.Users {
			users += user.Username + ","
			passwords += user.Password + ","
		}

		switch c.saslConfig.hashType {
		case scramPlainText:
			envVariables = append(envVariables,
				"KAFKA_CFG_SASL_ENABLED_MECHANISMS=PLAIN",
				"KAFKA_CFG_SASL_MECHANISM_INTER_BROKER_PROTOCOL=PLAIN",
			)
		case scramSHA256:
			envVariables = append(envVariables,
				"KAFKA_CFG_SASL_ENABLED_MECHANISMS=SCRAM-SHA-256",
				"KAFKA_CFG_SASL_MECHANISM_INTER_BROKER_PROTOCOL=SCRAM-SHA-256",
			)
		case scramSHA512:
			envVariables = append(envVariables,
				"KAFKA_CFG_SASL_ENABLED_MECHANISMS=SCRAM-SHA-512",
				"KAFKA_CFG_SASL_MECHANISM_INTER_BROKER_PROTOCOL=SCRAM-SHA-512",
			)
		default:
			return nil, fmt.Errorf("scram algorithm out of the known domain")
		}

		envVariables = append(envVariables,
			"KAFKA_CLIENT_USERS="+users[:len(users)-1],             // removing trailing comma
			"KAFKA_CLIENT_PASSWORDS="+passwords[:len(passwords)-1], // removing trailing comma
			"KAFKA_INTER_BROKER_USER="+c.saslConfig.BrokerUser.Username,
			"KAFKA_INTER_BROKER_PASSWORD="+c.saslConfig.BrokerUser.Password,
			"KAFKA_CERTIFICATE_PASSWORD="+c.saslConfig.CertificatePassword,
			"KAFKA_CFG_TLS_TYPE=JKS",
			"KAFKA_CFG_TLS_CLIENT_AUTH=none",
			"KAFKA_CFG_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM=",
			"KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=INTERNAL:SASL_SSL,CLIENT:SASL_SSL",
			"KAFKA_ZOOKEEPER_TLS_VERIFY_HOSTNAME=false",
		)
	} else {
		envVariables = append(envVariables,
			"KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=INTERNAL:PLAINTEXT,CLIENT:PLAINTEXT",
		)
	}

	containers := make([]*dockertest.Resource, c.brokers)
	for i := uint(0); i < c.brokers; i++ {
		i := i
		localhostPortInt, err := testhelper.GetFreePort()
		if err != nil {
			return nil, err
		}
		localhostPort := fmt.Sprintf("%s/tcp", strconv.Itoa(localhostPortInt))
		c.logger.Log("Kafka broker localhost port", i+1, localhostPort)

		nodeID := fmt.Sprintf("%d", i+1)
		hostname := "kafka" + nodeID
		kImage := "bitnami/kafka"
		nodeEnvVars := append(envVariables, []string{ // skipcq: CRT-D0001
			"KAFKA_BROKER_ID=" + nodeID,
			"KAFKA_CFG_LISTENERS=" + fmt.Sprintf("INTERNAL://%s:9090,CLIENT://:%s", hostname, kafkaClientPort),
		}...)
		if c.dontUseDockerHostListeners {
			nodeEnvVars = append(nodeEnvVars, "KAFKA_CFG_ADVERTISED_LISTENERS="+fmt.Sprintf(
				"INTERNAL://%s:9090,CLIENT://%s:%s", hostname, hostname, kafkaClientPort,
			))
		} else {
			nodeEnvVars = append(nodeEnvVars, "KAFKA_CFG_ADVERTISED_LISTENERS="+fmt.Sprintf(
				"INTERNAL://%s:9090,CLIENT://localhost:%d", hostname, localhostPortInt,
			))
		}
		containers[i], err = pool.RunWithOptions(&dockertest.RunOptions{
			Repository: kImage,
			Tag:        "latest",
			NetworkID:  network.ID,
			Hostname:   hostname,
			PortBindings: map[dc.Port][]dc.PortBinding{
				kafkaClientPort + "/tcp": {{HostIP: "localhost", HostPort: localhostPort}},
			},
			Mounts: mounts,
			Env:    nodeEnvVars,
		})
		if err != nil {
			return nil, err
		}
		cln.Cleanup(func() {
			if err := pool.Purge(containers[i]); err != nil {
				cln.Log(fmt.Errorf("could not purge Kafka resource: %w", err))
			}
		})
	}

	return &KafkaResource{
		Port: containers[0].GetPort(kafkaClientPort + "/tcp"),

		pool:       pool,
		containers: containers,
	}, nil
}
