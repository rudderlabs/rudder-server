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

type KafkaTest struct {
	pool             *dockertest.Pool
	err              error
	network          *dc.Network
	brokerPort       string
	localhostPort    string
	localhostPortInt int
}
var (
	Test *KafkaTest
)

func SetZookeeper(kafkapool *dockertest.Pool) *dockertest.Resource {
	Test = &KafkaTest{}
	Test.pool = kafkapool
	fmt.Println("Set zookeper")
	Test.network, Test.err = Test.pool.Client.CreateNetwork(dc.CreateNetworkOptions{Name: "kafka_network"})
	if Test.err != nil {
		log.Printf("Could not create docker network: %s", Test.err)
	}
	zookeeperPortInt, err := freeport.GetFreePort()
	if err != nil {
		fmt.Println(err)
	}
	zookeeperPort := fmt.Sprintf("%s/tcp", strconv.Itoa(zookeeperPortInt))
	zookeeperclientPort := fmt.Sprintf("ZOOKEEPER_CLIENT_PORT=%s", strconv.Itoa(zookeeperPortInt))
	log.Println("zookeeper Port:", zookeeperPort)
	log.Println("zookeeper client Port :", zookeeperclientPort)

	z, err := Test.pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "confluentinc/cp-zookeeper",
		Tag:        "latest",
		NetworkID:  Test.network.ID,
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
	Test.brokerPort = fmt.Sprintf("%s/tcp", strconv.Itoa(brokerPortInt))
	log.Println("broker Port:", Test.brokerPort)

	Test.localhostPortInt, err = freeport.GetFreePort()
	if err != nil {
		fmt.Println(err)
	}
	Test.localhostPort = fmt.Sprintf("%s/tcp", strconv.Itoa(Test.localhostPortInt))
	log.Println("localhost Port:", Test.localhostPort)

	KAFKA_ADVERTISED_LISTENERS := fmt.Sprintf("KAFKA_ADVERTISED_LISTENERS=INTERNAL://broker:9090,EXTERNAL://localhost:%s", strconv.Itoa(Test.localhostPortInt))
	KAFKA_LISTENERS := "KAFKA_LISTENERS=INTERNAL://broker:9090,EXTERNAL://:9092"

	log.Println("KAFKA_ADVERTISED_LISTENERS", KAFKA_ADVERTISED_LISTENERS)

	resourceKafka, err := Test.pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "confluentinc/cp-kafka",
		Tag:        "7.0.0",
		NetworkID:  Test.network.ID,
		Hostname:   "broker",
		PortBindings: map[dc.Port][]dc.PortBinding{
			"29092/tcp": {{HostIP: "broker", HostPort: Test.brokerPort}},
			"9092/tcp":  {{HostIP: "localhost", HostPort: Test.localhostPort}},
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
