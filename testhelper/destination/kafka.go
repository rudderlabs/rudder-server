package destination

import (
	_ "encoding/json"
	"fmt"
	"log"
	"strconv"

	_ "github.com/Shopify/sarama"
	_ "github.com/lib/pq"
	"github.com/ory/dockertest/v3"
	dc "github.com/ory/dockertest/v3/docker"
	"github.com/phayes/freeport"
	"golang.org/x/sync/errgroup"
)

type Option interface {
	apply(*config)
}

type withOption struct{ setup func(*config) }

func (w withOption) apply(c *config) { w.setup(c) }

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

type config struct {
	logger  logger
	brokers uint
}

func (c *config) defaults() {
	if c.logger == nil {
		c.logger = &nopLogger{}
	}
	if c.brokers < 1 {
		c.brokers = 1
	}
}

type logger interface {
	Log(...interface{})
}

type deferer interface {
	Defer(func() error)
}

// WithLogger allows to set a logger that prints debugging information
func WithLogger(l logger) Option {
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

func SetupKafka(pool *dockertest.Pool, d deferer, opts ...Option) (*KafkaResource, error) {
	var c config
	for _, opt := range opts {
		opt.apply(&c)
	}
	c.defaults()

	network, err := pool.Client.CreateNetwork(dc.CreateNetworkOptions{Name: "kafka_network"})
	if err != nil {
		return nil, fmt.Errorf("could not create docker network: %w", err)
	}
	d.Defer(func() error {
		if err := pool.Client.RemoveNetwork(network.ID); err != nil {
			return fmt.Errorf("could not remove kafka network: %w", err)
		}
		return nil
	})

	zookeeperPortInt, err := freeport.GetFreePort()
	if err != nil {
		return nil, err
	}
	zookeeperPort := fmt.Sprintf("%s/tcp", strconv.Itoa(zookeeperPortInt))
	zookeeperContainer, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "confluentinc/cp-zookeeper",
		Tag:        "latest",
		NetworkID:  network.ID,
		Hostname:   "zookeeper",
		PortBindings: map[dc.Port][]dc.PortBinding{
			"2181/tcp": {{HostIP: "zookeeper", HostPort: zookeeperPort}},
		},
		Env: []string{"ZOOKEEPER_CLIENT_PORT=2181"},
	})
	if err != nil {
		return nil, err
	}
	d.Defer(func() error {
		if err := pool.Purge(zookeeperContainer); err != nil {
			log.Printf("Could not purge resource: %s \n", err)
		}
		return nil
	})

	c.logger.Log("Zookeeper localhost port", zookeeperContainer.GetPort("2181/tcp"))

	bootstrapServers := ""
	for i := uint(1); i <= c.brokers; i++ {
		bootstrapServers += fmt.Sprintf("kafka%d:9090,", i)
	}
	bootstrapServers = bootstrapServers[:len(bootstrapServers)-1] // removing trailing comma

	containers := make([]*dockertest.Resource, c.brokers)
	for i := uint(0); i < c.brokers; i++ {
		i := i
		localhostPortInt, err := freeport.GetFreePort()
		if err != nil {
			return nil, err
		}
		localhostPort := fmt.Sprintf("%s/tcp", strconv.Itoa(localhostPortInt))
		c.logger.Log("Kafka broker localhost port", i+1, localhostPort)

		nodeID := fmt.Sprintf("%d", i+1)
		hostname := "kafka" + nodeID
		containers[i], err = pool.RunWithOptions(&dockertest.RunOptions{
			Repository: "confluentinc/cp-kafka",
			Tag:        "7.0.0",
			NetworkID:  network.ID,
			Hostname:   hostname,
			PortBindings: map[dc.Port][]dc.PortBinding{
				"9092/tcp": {{HostIP: "localhost", HostPort: localhostPort}},
			},
			Env: []string{
				"KAFKA_BROKER_ID=" + nodeID,
				"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT",
				"KAFKA_ADVERTISED_LISTENERS=" + fmt.Sprintf(
					"INTERNAL://%s:9090,EXTERNAL://localhost:%d", hostname, localhostPortInt,
				),
				"KAFKA_LISTENERS=" + fmt.Sprintf("INTERNAL://%s:9090,EXTERNAL://:9092", hostname),
				"KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181",
				"KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL",
				"BOOTSTRAP_SERVERS=" + bootstrapServers,
			},
		})
		if err != nil {
			return nil, err
		}
		d.Defer(func() error {
			if err := pool.Purge(containers[i]); err != nil {
				return fmt.Errorf("could not purge Kafka resource: %w", err)
			}
			return nil
		})
	}

	return &KafkaResource{
		Port: containers[0].GetPort("9092/tcp"),

		pool:       pool,
		containers: containers,
	}, nil
}

type nopLogger struct{}

func (n *nopLogger) Log(...interface{}) {}
