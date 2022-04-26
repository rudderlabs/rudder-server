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
)

type KafkaResource struct {
	Port string

	pool      *dockertest.Pool
	container *dockertest.Resource
}

func (k *KafkaResource) Destroy() error {
	return k.pool.Purge(k.container)
}

type logger interface {
	Log(...interface{})
}

type deferer interface {
	Defer(func() error)
}

func SetupKafka(pool *dockertest.Pool, d deferer, l logger) (*KafkaResource, error) {
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

	l.Log("KAFKA_ZOOKEEPER_CONNECT: localhost:", zookeeperContainer.GetPort("2181/tcp"))
	brokerPortInt, err := freeport.GetFreePort()
	if err != nil {
		return nil, err
	}

	brokerPort := fmt.Sprintf("%s/tcp", strconv.Itoa(brokerPortInt))
	l.Log("broker Port:", brokerPort)

	localhostPortInt, err := freeport.GetFreePort()
	if err != nil {
		return nil, err
	}
	localhostPort := fmt.Sprintf("%s/tcp", strconv.Itoa(localhostPortInt))
	l.Log("localhost Port:", localhostPort)

	advertisedListeners := fmt.Sprintf("INTERNAL://broker:9090,EXTERNAL://localhost:%d", localhostPortInt)
	l.Log("KAFKA_ADVERTISED_LISTENERS", advertisedListeners)

	kafkaContainer, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "confluentinc/cp-kafka",
		Tag:        "7.0.0",
		NetworkID:  network.ID,
		Hostname:   "broker",
		PortBindings: map[dc.Port][]dc.PortBinding{
			"29092/tcp": {{HostIP: "broker", HostPort: brokerPort}},
			"9092/tcp":  {{HostIP: "localhost", HostPort: localhostPort}},
		},
		Env: []string{
			"KAFKA_BROKER_ID=1",
			"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT",
			"KAFKA_ADVERTISED_LISTENERS=" + advertisedListeners,
			"KAFKA_LISTENERS=INTERNAL://broker:9090,EXTERNAL://:9092",
			"KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181",
			"KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL",
		},
	})
	d.Defer(func() error {
		if err := pool.Purge(kafkaContainer); err != nil {
			return fmt.Errorf("could not purge Kafka resource: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	l.Log("Kafka PORT: ", kafkaContainer.GetPort("9092/tcp"))

	return &KafkaResource{
		Port: kafkaContainer.GetPort("9092/tcp"),

		pool:      pool,
		container: kafkaContainer,
	}, nil
}
