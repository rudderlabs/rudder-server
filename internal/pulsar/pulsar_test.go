package pulsar

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/ory/dockertest/v3"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource"
	"github.com/stretchr/testify/require"
)

func Test_Pulsar(t *testing.T) {
	var (
		topic            = "test-topic"
		payload          = []byte("test-message")
		key              = "test-key"
		subscriptionName = "test-subscription"
		counter          uint32
	)
	pulsarContainer := PulsarResource(t)
	time.Sleep(5 * time.Second)

	conf := config.New()
	conf.Set("Pulsar.Client.url", pulsarContainer.URL)
	conf.Set("Pulsar.Producer.topic", topic)
	client, err := newPulsarClient(getClientConf(conf), logger.NewLogger())
	require.NoError(t, err)
	require.NotNil(t, client)
	producer, err := newProducer(client, getProducerConf(conf), logger.NewLogger())
	require.NoErrorf(t, err, "got error %+v", err)
	require.NotNil(t, producer)
	defer producer.Close()

	consumer, err := client.Client.Subscribe(pulsar.ConsumerOptions{
		Topic:            topic,
		SubscriptionName: subscriptionName,
	})
	require.NoError(t, err)
	defer consumer.Close()
	err = producer.SendMessage(context.Background(), key, "", payload)
	require.NoError(t, err)
	msg, err := consumer.Receive(context.Background())
	require.NoError(t, err)
	require.Equal(t, msg.Payload(), payload)
	require.Equal(t, msg.Key(), key)

	for i := 0; i < 10; i++ {
		producer.SendMessageAsync(context.Background(), key, "", []byte(fmt.Sprintf("test-message-%d", i)), func(id pulsar.MessageID, message *pulsar.ProducerMessage, err error) {
			require.NoError(t, err)
			atomic.AddUint32(&counter, 1)
		})
	}
	err = producer.Flush()
	require.NoError(t, err)
	require.Equal(t, atomic.LoadUint32(&counter), uint32(10))
	for i := 0; i < 10; i++ {
		msg, err := consumer.Receive(context.Background())
		require.NoError(t, err)
		require.Equal(t, msg.Payload(), []byte(fmt.Sprintf("test-message-%d", i)))
		require.Equal(t, msg.Key(), key)
	}
}

func Test_PulsarInterface(t *testing.T) {
	topic := "test-topic"
	pulsarContainer := PulsarResource(t)
	conf := config.New()
	conf.Set("Pulsar.Client.url", pulsarContainer.URL)
	conf.Set("Pulsar.Producer.topic", topic)
	producer, err := New(conf)
	require.NoError(t, err)
	require.NotNil(t, producer)
	defer producer.Close()
}

// PulsarResource returns a pulsar container resource
func PulsarResource(t *testing.T) *resource.PulsarResource {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pulsarContainer, err := resource.SetupPulsar(pool, t)
	require.NoError(t, err)
	return pulsarContainer
}
