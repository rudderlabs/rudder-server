package main_test

import (
	_ "encoding/json"
	"fmt"
	_ "github.com/Shopify/sarama"
	_ "github.com/lib/pq"
	"github.com/ory/dockertest"
	dc "github.com/ory/dockertest/docker"
	"github.com/phayes/freeport"
	"log"
	"strconv"
)

var (
	pool             *dockertest.Pool
	err              error
	network          *dc.Network
	brokerPort       string
	localhostPort    string
	localhostPortInt int
)

func SetZookeeper(kafkapool *dockertest.Pool) *dockertest.Resource {
	pool = kafkapool
	fmt.Println("Set zookeper")
	network, err = pool.Client.CreateNetwork(dc.CreateNetworkOptions{Name: "kafka_network"})
	if err != nil {
		log.Printf("Could not create docker network: %s", err)
	}
	zookeeperPortInt, err := freeport.GetFreePort()
	if err != nil {
		fmt.Println(err)
	}
	zookeeperPort := fmt.Sprintf("%s/tcp", strconv.Itoa(zookeeperPortInt))
	zookeeperclientPort := fmt.Sprintf("ZOOKEEPER_CLIENT_PORT=%s", strconv.Itoa(zookeeperPortInt))
	log.Println("zookeeper Port:", zookeeperPort)
	log.Println("zookeeper client Port :", zookeeperclientPort)

	z, err := pool.RunWithOptions(&dockertest.RunOptions{
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
		fmt.Println(err)
	}
	return z
}

func SetKafka(z *dockertest.Resource) *dockertest.Resource {
	// Set Kafka: pulls an image, creates a container based on it and runs it
	KAFKA_ZOOKEEPER_CONNECT := fmt.Sprintf("KAFKA_ZOOKEEPER_CONNECT= zookeeper:%s", z.GetPort("2181/tcp"))
	log.Println("KAFKA_ZOOKEEPER_CONNECT:", KAFKA_ZOOKEEPER_CONNECT)

	brokerPortInt, err := freeport.GetFreePort()
	if err != nil {
		fmt.Println(err)
	}
	brokerPort = fmt.Sprintf("%s/tcp", strconv.Itoa(brokerPortInt))
	log.Println("broker Port:", brokerPort)

	localhostPortInt, err = freeport.GetFreePort()
	if err != nil {
		fmt.Println(err)
	}
	localhostPort = fmt.Sprintf("%s/tcp", strconv.Itoa(localhostPortInt))
	log.Println("localhost Port:", localhostPort)

	KAFKA_ADVERTISED_LISTENERS := fmt.Sprintf("KAFKA_ADVERTISED_LISTENERS=INTERNAL://broker:9090,EXTERNAL://localhost:%s", strconv.Itoa(localhostPortInt))
	KAFKA_LISTENERS := "KAFKA_LISTENERS=INTERNAL://broker:9090,EXTERNAL://:9092"

	log.Println("KAFKA_ADVERTISED_LISTENERS", KAFKA_ADVERTISED_LISTENERS)

	resourceKafka, err := pool.RunWithOptions(&dockertest.RunOptions{
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
	if err != nil {
		fmt.Println(err)
	}
	log.Println("Kafka PORT:- ", resourceKafka.GetPort("9092/tcp"))
	return resourceKafka
}
