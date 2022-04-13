//go:build integration

package kafka

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/ory/dockertest"
	"github.com/phayes/freeport"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/testhelper/destination"
	"github.com/rudderlabs/rudder-server/utils/tests/tcpproxy"
)

func TestTimeout(t *testing.T) {
	// Setup Docker containers
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	cleanup := testCleanup{t}
	kafkaContainer, err := destination.SetupKafka(pool, &cleanup)
	require.NoError(t, err)

	// Setup Sarama client
	var (
		topic   = t.Name()
		timeout = time.Second
	)
	saramaConfig := sarama.NewConfig()
	saramaConfig.ClientID = t.Name()
	saramaConfig.Net.DialTimeout = 30 * time.Second
	saramaConfig.Net.WriteTimeout = timeout
	saramaConfig.Net.ReadTimeout = timeout
	saramaConfig.Producer.Timeout = timeout
	saramaConfig.Producer.Return.Errors = true
	saramaConfig.Producer.Return.Successes = true
	saramaConfig.Producer.Partitioner = sarama.NewReferenceHashPartitioner
	saramaConfig.Producer.RequiredAcks = sarama.WaitForAll
	saramaConfig.Version = sarama.V1_0_0_0

	proxyPort, err := freeport.GetFreePort()
	require.NoError(t, err)

	var (
		kafkaEndpoint = fmt.Sprintf("localhost:%s", kafkaContainer.Port)
		proxyEndpoint = fmt.Sprintf("localhost:%d", proxyPort)
		brokers       = []string{proxyEndpoint}
	)

	// Start proxy which will stay in between the Kafka clients and the Kafka broker
	ctx, stopProxy := context.WithCancel(context.Background())
	proxyWg, err := tcpproxy.Start(ctx, proxyEndpoint, kafkaEndpoint, t)
	require.NoError(t, err)
	t.Cleanup(proxyWg.Wait)
	t.Cleanup(stopProxy)

	// Try to connect with Sarama client for a minute max
	var saramaProducer sarama.SyncProducer
	require.Eventually(t, func() bool {
		var err error
		saramaProducer, err = sarama.NewSyncProducer(brokers, saramaConfig)
		if err != nil {
			t.Logf("SyncProducer error: %v", err)
		}
		return err == nil
	}, time.Minute, 100*time.Millisecond)

	// Send one message with Sarama client
	_, _, err = saramaProducer.SendMessage(&sarama.ProducerMessage{
		Topic:     topic,
		Key:       sarama.StringEncoder("key-01"),
		Value:     sarama.StringEncoder("value-01"),
		Timestamp: time.Now(),
	})
	require.NoError(t, err)
	t.Log("Message produced successfully with Sarama")

	// Send one message with Confluent client
	confluentProducer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": proxyEndpoint,
	})
	require.NoError(t, err)
	t.Cleanup(confluentProducer.Close)
	confluentProducerDone := make(chan kafka.Event, 1)
	t.Cleanup(func() { close(confluentProducerDone) })
	err = confluentProducer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte("key-01"),
		Value:          []byte("value-01"),
	}, confluentProducerDone)
	require.NoError(t, err)
	<-confluentProducerDone
	t.Log("Message produced successfully with Confluent")

	stopProxy()
	proxyWg.Wait()
	t.Log("Proxy terminated")

	_, _, err = saramaProducer.SendMessage(&sarama.ProducerMessage{
		Topic:     t.Name(),
		Key:       sarama.StringEncoder("key-01"),
		Value:     sarama.StringEncoder("value-01"),
		Timestamp: time.Now(),
	})
	t.Log("Sarama SendMessage ERROR", err)

	_, err = sarama.NewSyncProducer(brokers, saramaConfig)
	t.Logf("NewSyncProducer: %v", err)

	err = confluentProducer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte("key-01"),
		Value:          []byte("value-01"),
	}, confluentProducerDone)
	t.Log("Sarama Confluent ERROR", err)
	if err == nil {
		<-confluentProducerDone
		t.Log("Confluent done!")
	}
}

type testCleanup struct{ *testing.T }

func (t *testCleanup) Defer(fn func() error) {
	t.Cleanup(func() {
		if err := fn(); err != nil {
			t.Log(err)
		}
	})
}
