package destination

import (
	_ "encoding/json"
	"fmt"
	"log"
	"strconv"

	_ "github.com/Shopify/sarama"
	_ "github.com/lib/pq"
	"github.com/ory/dockertest"
	dc "github.com/ory/dockertest/docker"
	"github.com/phayes/freeport"
)

type KafkaResource struct {
	Port string
}

type deferer interface {
	Defer(func() error)
}

func SetupKafka(pool *dockertest.Pool, d deferer) (*KafkaResource, error) {
	network, err := pool.Client.CreateNetwork(dc.CreateNetworkOptions{Name: "kafka_network"})
	if err != nil {
		return nil, fmt.Errorf("Could not create docker network: %w", err)
	}
	d.Defer(func() error {
		// TODO delete network
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

	log.Println("KAFKA_ZOOKEEPER_CONNECT: localhost:", zookeeperContainer.GetPort("2181/tcp"))
	brokerPortInt, err := freeport.GetFreePort()
	if err != nil {
		return nil, err
	}
	brokerPort := fmt.Sprintf("%s/tcp", strconv.Itoa(brokerPortInt))
	log.Println("broker Port:", brokerPort)

	localhostPortInt, err := freeport.GetFreePort()
	if err != nil {
		return nil, err
	}
	localhostPort := fmt.Sprintf("%s/tcp", strconv.Itoa(localhostPortInt))
	log.Printf("localhost Port: %s \n", localhostPort)

	KAFKA_ADVERTISED_LISTENERS := fmt.Sprintf("KAFKA_ADVERTISED_LISTENERS=INTERNAL://broker:9090,EXTERNAL://localhost:%d", localhostPortInt)
	KAFKA_LISTENERS := "KAFKA_LISTENERS=INTERNAL://broker:9090,EXTERNAL://:9092"

	log.Println("KAFKA_ADVERTISED_LISTENERS", KAFKA_ADVERTISED_LISTENERS)

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
			KAFKA_ADVERTISED_LISTENERS,
			KAFKA_LISTENERS,
			"KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181",
			"KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL",
		},
	})
	d.Defer(func() error {
		if err := pool.Purge(kafkaContainer); err != nil {
			return fmt.Errorf("Could not purge Kafka resource: %w \n", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	log.Println("Kafka PORT:- ", kafkaContainer.GetPort("9092/tcp"))

	return &KafkaResource{
		Port: kafkaContainer.GetPort("9092/tcp"),
	}, nil
}
