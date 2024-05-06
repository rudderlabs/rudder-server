package ack

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	pulsarlog "github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/apache/pulsar-client-go/pulsaradmin"
	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/utils"
	"github.com/ory/dockertest/v3"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/logger"
	pulsardocker "github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/pulsar"
)

func TestCumulativeAck(t *testing.T) {
	// Set up Pulsar Standalone Docker container
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pulsarStandalone, err := pulsardocker.Setup(pool, t, pulsardocker.WithTag("3.2.2"))
	require.NoError(t, err)

	log := logger.NOP
	if testing.Verbose() {
		// Uncomment the next 3 lines to enable DEBUG logging
		// factory := logger.NewFactory(config.New())
		// require.NoError(t, factory.SetLogLevel("", "DEBUG"))
		// log = factory.NewLogger()
	}

	pulsarClient, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:                     pulsarStandalone.URL,
		MaxConnectionsPerBroker: 10,
		Logger:                  &pulsarLogAdapter{Logger: log},
	})
	require.NoError(t, err)

	// Setting up consumer
	var (
		topic            = "persistent://public/default/my-topic"
		subscriptionName = "my-subscription-name"
	)
	consumer, err := pulsarClient.Subscribe(pulsar.ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            subscriptionName,
		Type:                        pulsar.KeyShared,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
		ReceiverQueueSize:           1000,
		AckWithResponse:             true,
	})
	require.NoError(t, err)

	// Setting up producer
	producer, err := pulsarClient.CreateProducer(pulsar.ProducerOptions{
		Topic: topic,
	})
	require.NoError(t, err)

	var (
		producerDone              = make(chan struct{})
		producerCtx, stopProducer = context.WithCancel(context.Background())
	)
	defer func() {
		stopProducer()
		<-producerDone
	}()
	go func() {
		defer close(producerDone)
		t.Log("Starting producer...")
		for {
			select {
			case <-producerCtx.Done():
				return
			default:
				_, err := producer.Send(producerCtx, &pulsar.ProducerMessage{
					Key:     "my-key",
					Payload: []byte("hello"),
				})
				if err != nil {
					if errors.Is(err, context.Canceled) {
						t.Logf("Producer stopped: %v", err)
						return
					}
					require.NoError(t, err)
				}
			}
		}
	}()

	// Set up pulsar admin client to check for unacked messages
	pulsarAdmin, err := pulsaradmin.NewClient(&pulsaradmin.Config{
		WebServiceURL: pulsarStandalone.AdminURL,
	})
	require.NoError(t, err)
	topicName, err := utils.GetTopicName(topic)
	require.NoError(t, err)

	// Set up a ticker to check the unacked messages every 10 seconds
	ticker := time.NewTicker(10 * time.Second)

	go func() {
		for {
			select {
			case <-ticker.C:
				stats, err := pulsarAdmin.Topics().GetStats(*topicName)
				require.NoError(t, err)

				unackedMessages := stats.Subscriptions[subscriptionName].UnAckedMessages
				t.Logf("UnAcked messages: %d", unackedMessages)
			}
		}
	}()

	// Start consuming
	t.Log("Starting consumer...")
	for {
		batchBuffer, numMessages, bufferTime, ok := lo.BufferWithTimeout(consumer.Chan(), 1000, time.Second)
		if !ok {
			t.Log("Pulsar consumer channel closed")
			return
		}

		t.Logf("BufferWithTimeout: %d messages, %v buffer time", numMessages, bufferTime)

		if numMessages == 0 {
			continue
		}

		lastMsg := batchBuffer[len(batchBuffer)-1]
		t.Logf("We received %d messages. Acking last ID : %q", len(batchBuffer), lastMsg.ID())

		for _, msg := range batchBuffer {
			if msg.PublishTime().After(lastMsg.PublishTime()) {
				t.Logf("Detected a newer message: %q", msg.ID())
				lastMsg = msg
			}
		}

		err = consumer.AckIDCumulative(lastMsg.ID())
		require.NoError(t, err)
	}
}

type pulsarLogAdapter struct {
	logger.Logger
}

func (pl *pulsarLogAdapter) SubLogger(fields pulsarlog.Fields) pulsarlog.Logger {
	logFields := make([]logger.Field, 0, len(fields))
	for k, v := range fields {
		logFields = append(logFields, logger.NewField(k, v))
	}
	return &pulsarLogAdapter{pl.Logger.Withn(logFields...)}
}

func (pl *pulsarLogAdapter) WithFields(fields pulsarlog.Fields) pulsarlog.Entry {
	return pl.SubLogger(fields)
}

func (pl *pulsarLogAdapter) WithField(name string, value interface{}) pulsarlog.Entry {
	return &pulsarLogAdapter{pl.Logger.Withn(logger.NewField(name, value))}
}

func (pl *pulsarLogAdapter) WithError(err error) pulsarlog.Entry {
	return pl.WithField("error", err)
}
