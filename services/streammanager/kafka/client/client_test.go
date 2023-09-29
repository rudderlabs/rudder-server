package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	dc "github.com/ory/dockertest/v3/docker"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/services/streammanager/kafka/client/testutil"
	dockerKafka "github.com/rudderlabs/rudder-server/testhelper/destination/kafka"
	"github.com/rudderlabs/rudder-server/testhelper/destination/sshserver"
)

const (
	defaultTestTimeout = 60 * time.Second
)

func TestClient_Ping(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	kafkaContainer, err := dockerKafka.Setup(pool, &testCleanup{t}, dockerKafka.WithLogger(t))
	require.NoError(t, err)

	kafkaHost := fmt.Sprintf("localhost:%s", kafkaContainer.Ports[0])
	c, err := New("tcp", []string{"bad-host", kafkaHost}, Config{})
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, c.Ping(ctx))

	// shuffling hosts
	c, err = New("tcp", []string{kafkaHost, "bad-host", kafkaHost}, Config{})
	require.NoError(t, err)
	require.NoError(t, c.Ping(ctx))

	require.NoError(t, kafkaContainer.Destroy())
	err = c.Ping(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "connection refused")
}

func TestProducerBatchConsumerGroup(t *testing.T) {
	// Prepare cluster - Zookeeper + 3 Kafka brokers
	// We need more than one broker, or we'll be stuck with a "GROUP_COORDINATOR_NOT_AVAILABLE" error
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	kafkaContainer, err := dockerKafka.Setup(pool, &testCleanup{t},
		dockerKafka.WithLogger(t),
		dockerKafka.WithBrokers(3))
	require.NoError(t, err)

	addresses := make([]string, 0, len(kafkaContainer.Ports))
	for i := 0; i < len(kafkaContainer.Ports); i++ {
		addresses = append(addresses, fmt.Sprintf("localhost:%s", kafkaContainer.Ports[i]))
	}
	c, err := New("tcp", addresses, Config{ClientID: "some-client", DialTimeout: 5 * time.Second})
	require.NoError(t, err)

	var (
		messagesWaitGroup   sync.WaitGroup
		gracefulTermination sync.WaitGroup
		c01Count, c02Count  int32
		noOfMessages        = 50
		ctx, cancel         = context.WithCancel(context.Background())
		tc                  = testutil.NewWithDialer(c.dialer, c.network, c.addresses...)
	)

	t.Cleanup(gracefulTermination.Wait)
	t.Cleanup(cancel)

	// Check connectivity and try to create the desired topic until the brokers are up and running (max 30s)
	require.NoError(t, c.Ping(ctx))
	require.Eventually(t, func() bool {
		err := tc.CreateTopic(ctx, t.Name(), 2, 3) // partitions = 2, replication factor = 3
		if err != nil {
			t.Logf("Could not create topic: %v", err)
		}
		return err == nil
	}, defaultTestTimeout, time.Second)

	// Check that the topic has been created with the right number of partitions
	var topics []testutil.TopicPartition
	require.Eventually(t, func() bool {
		topics, err = tc.ListTopics(ctx)
		success := err == nil && len(topics) == 2
		if !success {
			t.Logf("List topics failure %+v: %v", topics, err)
		}
		return success
	}, defaultTestTimeout, time.Second)
	require.Equal(t, []testutil.TopicPartition{
		{Topic: t.Name(), Partition: 0},
		{Topic: t.Name(), Partition: 1},
	}, topics)

	// Produce X messages in a single batch
	producerConf := ProducerConfig{
		ClientID:     "producer-01",
		WriteTimeout: 5 * time.Second,
		ReadTimeout:  5 * time.Second,
	}
	if testing.Verbose() {
		producerConf.Logger = &testLogger{t}
		producerConf.ErrorLogger = producerConf.Logger
	}
	p, err := c.NewProducer(producerConf)
	require.NoError(t, err)
	publishMessages(ctx, t, p, noOfMessages)
	messagesWaitGroup.Add(noOfMessages)

	// Starting consumers with group-01 and FirstOffset
	var (
		// The ticker is used so that the test won't end as long as we keep getting messages since the consumers
		// will reset the ticker each time they receive a message
		tickerMu    sync.Mutex
		tickerReset = 10 * time.Second
		ticker      = time.NewTicker(30 * time.Second)
	)
	consumerConf := ConsumerConfig{
		GroupID:             "group-01",
		StartOffset:         FirstOffset,
		CommitInterval:      time.Second, // to make the test faster instead of committing each single message
		FetchBatchesMaxWait: 10 * time.Second,
	}
	if testing.Verbose() {
		consumerConf.Logger = &testLogger{t}
		consumerConf.ErrorLogger = consumerConf.Logger
	}
	consume := func(c *Consumer, id string, count *int32) {
		defer gracefulTermination.Done()
		for {
			_, err := c.Receive(ctx)
			if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
				t.Logf("Closing %s: %v", id, err)
				return
			}
			require.NoError(t, err)
			t.Logf("Got a message on %s", id)
			tickerMu.Lock()
			ticker.Reset(tickerReset)
			tickerMu.Unlock()
			_ = atomic.AddInt32(count, 1)
			messagesWaitGroup.Done()
		}
	}
	closeConsumer := func(c *Consumer, id string) func() {
		return func() {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if err := c.Close(ctx); err != nil {
				t.Logf("Error closing %s: %v", id, err)
			}
		}
	}

	c01 := c.NewConsumer(t.Name(), consumerConf)
	t.Cleanup(closeConsumer(c01, "c01"))
	gracefulTermination.Add(1)
	go consume(c01, "c01", &c01Count)

	c02 := c.NewConsumer(t.Name(), consumerConf)
	t.Cleanup(closeConsumer(c02, "c02"))
	gracefulTermination.Add(1)
	go consume(c02, "c02", &c02Count)

	done := make(chan struct{})
	go func() {
		messagesWaitGroup.Wait() // this is satisfied once the consumers receive at least "noOfMessages" messages
		close(done)
	}()

	select {
	case <-done:
		require.EqualValues(t, noOfMessages, atomic.LoadInt32(&c01Count)+atomic.LoadInt32(&c02Count))
	// the test won't end as long as we keep getting messages since the consumers reset the ticker
	// when they receive a message
	case <-ticker.C:
		t.Error("Could not complete within timeout")
	}

	t.Logf("Messages consumed by c01: %d", atomic.LoadInt32(&c01Count))
	t.Logf("Messages consumed by c02: %d", atomic.LoadInt32(&c02Count))
}

func TestConsumer_Partition(t *testing.T) {
	// Prepare cluster - Zookeeper and one Kafka broker
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	kafkaContainer, err := dockerKafka.Setup(pool, &testCleanup{t},
		dockerKafka.WithLogger(t),
		dockerKafka.WithBrokers(1))
	require.NoError(t, err)

	kafkaHost := fmt.Sprintf("localhost:%s", kafkaContainer.Ports[0])
	c, err := New("tcp", []string{"bad-host", kafkaHost}, Config{ClientID: "some-client", DialTimeout: 5 * time.Second})
	require.NoError(t, err)

	var (
		messagesWaitGroup   sync.WaitGroup
		gracefulTermination sync.WaitGroup
		c01Count, c02Count  int32
		noOfMessages        = 50
		ctx, cancel         = context.WithCancel(context.Background())
		tc                  = testutil.NewWithDialer(c.dialer, c.network, c.addresses...)
	)

	t.Cleanup(gracefulTermination.Wait)
	t.Cleanup(cancel)

	// Check connectivity and try to create the desired topic until the brokers are up and running (max 30s)
	require.NoError(t, c.Ping(ctx))
	require.Eventually(t, func() bool {
		err := tc.CreateTopic(ctx, t.Name(), 2, 1) // partitions = 2, replication factor = 1
		if err != nil {
			t.Logf("Could not create topic: %v", err)
		}
		return err == nil
	}, defaultTestTimeout, time.Second)

	// Check that the topic has been created with the right number of partitions
	topics, err := tc.ListTopics(ctx)
	require.NoError(t, err)
	require.Equal(t, []testutil.TopicPartition{
		{Topic: t.Name(), Partition: 0},
		{Topic: t.Name(), Partition: 1},
	}, topics)

	// Produce X messages in a single batch
	producerConf := ProducerConfig{
		ClientID: "producer-01",
	}
	if testing.Verbose() {
		producerConf.Logger = &testLogger{t}
		producerConf.ErrorLogger = producerConf.Logger
	}
	p, err := c.NewProducer(producerConf)
	require.NoError(t, err)
	publishMessages(ctx, t, p, noOfMessages)
	messagesWaitGroup.Add(noOfMessages)

	// Starting consumers with group-01 and FirstOffset
	var (
		// The ticker is used so that the test won't end as long as we keep getting messages since the consumers
		// will reset the ticker each time they receive a message
		tickerMu    sync.Mutex
		tickerReset = 10 * time.Second
		ticker      = time.NewTicker(30 * time.Second)
	)
	consumerConf := ConsumerConfig{
		StartOffset:         FirstOffset,
		CommitInterval:      time.Second, // to make the test faster instead of committing each single message
		FetchBatchesMaxWait: 10 * time.Second,
	}
	if testing.Verbose() {
		consumerConf.Logger = &testLogger{t}
		consumerConf.ErrorLogger = consumerConf.Logger
	}
	consume := func(c *Consumer, id string, count *int32) {
		defer gracefulTermination.Done()
		for {
			_, err := c.Receive(ctx)
			if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
				t.Logf("Closing %s: %v", id, err)
				return
			}
			require.NoError(t, err)
			t.Logf("Got a message on %s", id)
			tickerMu.Lock()
			ticker.Reset(tickerReset)
			tickerMu.Unlock()
			_ = atomic.AddInt32(count, 1)
			messagesWaitGroup.Done()
		}
	}
	closeConsumer := func(c *Consumer, id string) func() {
		return func() {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if err := c.Close(ctx); err != nil {
				t.Logf("Error closing %s: %v", id, err)
			}
		}
	}

	c01 := c.NewConsumer(t.Name(), consumerConf)
	t.Cleanup(closeConsumer(c01, "c01"))
	gracefulTermination.Add(1)
	go consume(c01, "c01", &c01Count)

	consumerConf.Partition = 1
	c02 := c.NewConsumer(t.Name(), consumerConf)
	t.Cleanup(closeConsumer(c02, "c02"))
	gracefulTermination.Add(1)
	go consume(c02, "c02", &c02Count)

	done := make(chan struct{})
	go func() {
		messagesWaitGroup.Wait() // this is satisfied once the consumers receive at least "noOfMessages" messages
		close(done)
	}()

	select {
	case <-done:
		require.Greater(t, atomic.LoadInt32(&c01Count), int32(0))
		require.Greater(t, atomic.LoadInt32(&c02Count), int32(0))
		require.EqualValues(t, noOfMessages, atomic.LoadInt32(&c01Count)+atomic.LoadInt32(&c02Count))
	// the test won't end as long as we keep getting messages since the consumers reset the ticker
	// when they receive a message
	case <-ticker.C:
		t.Error("Could not complete within timeout")
	}

	t.Logf("Messages consumed by c01: %d", atomic.LoadInt32(&c01Count))
	t.Logf("Messages consumed by c02: %d", atomic.LoadInt32(&c02Count))
}

func TestWithSASL(t *testing.T) {
	// Prepare cluster - Zookeeper and one Kafka broker
	path, err := os.Getwd()
	require.NoError(t, err)

	saslConfiguration := dockerKafka.SASLConfig{
		BrokerUser: dockerKafka.User{Username: "kafka1", Password: "password"},
		Users: []dockerKafka.User{
			{Username: "client1", Password: "password"},
		},
		CertificatePassword: "password",
		KeyStorePath:        filepath.Join(path, "testdata", "keystore", "kafka.keystore.jks"),
		TrustStorePath:      filepath.Join(path, "testdata", "truststore", "kafka.truststore.jks"),
	}

	hashTypes := []ScramHashGenerator{ScramPlainText, ScramSHA256, ScramSHA512}
	for _, hashType := range hashTypes {
		saslConfiguration := saslConfiguration // to avoid data race
		t.Run(hashType.String(), func(t *testing.T) {
			pool, err := dockertest.NewPool("")
			require.NoError(t, err)

			containerOptions := []dockerKafka.Option{dockerKafka.WithBrokers(1)}
			if testing.Verbose() {
				containerOptions = append(containerOptions, dockerKafka.WithLogger(t))
			}
			switch hashType {
			case ScramPlainText:
				containerOptions = append(containerOptions, dockerKafka.WithSASLPlain(&saslConfiguration))
			case ScramSHA256:
				containerOptions = append(containerOptions, dockerKafka.WithSASLScramSHA256(&saslConfiguration))
			case ScramSHA512:
				containerOptions = append(containerOptions, dockerKafka.WithSASLScramSHA512(&saslConfiguration))
			}
			kafkaContainer, err := dockerKafka.Setup(pool, &testCleanup{t}, containerOptions...)
			require.NoError(t, err)

			kafkaHost := fmt.Sprintf("localhost:%s", kafkaContainer.Ports[0])
			c, err := New("tcp", []string{"bad-host", kafkaHost}, Config{
				ClientID:    "some-client",
				DialTimeout: 10 * time.Second,
				SASL: &SASL{
					ScramHashGen: hashType,
					Username:     "client1",
					Password:     "password",
				},
				TLS: &TLS{
					InsecureSkipVerify: true,
				},
			})
			require.NoError(t, err)

			require.Eventually(t, func() bool {
				err := c.Ping(context.Background())
				if err != nil {
					t.Logf("Ping error: %v", err)
				}
				return err == nil
			}, defaultTestTimeout, 250*time.Millisecond)

			var producerConf ProducerConfig
			if testing.Verbose() {
				producerConf.Logger = &testLogger{t}
				producerConf.ErrorLogger = producerConf.Logger
			}
			p, err := c.NewProducer(producerConf)
			require.NoError(t, err)
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				if err := p.Close(ctx); err != nil {
					t.Logf("Error closing producer: %v", err)
				}
			})

			// We will now try to publish for 30s because the cluster could still be in a "Leader Not Available" state
			require.Eventually(t, func() bool {
				err := p.Publish(context.Background(), Message{
					Key:   []byte("hello"),
					Value: []byte("ciao"),
					Topic: "some-topic",
				})
				if err != nil {
					t.Logf("Publish error: %v", err)
				}
				return err == nil
			}, defaultTestTimeout, 100*time.Millisecond, "Could not publish within timeout")
		})
	}
}

func TestWithSASLBadCredentials(t *testing.T) {
	// Prepare cluster - Zookeeper and one Kafka broker
	path, err := os.Getwd()
	require.NoError(t, err)

	saslConfiguration := dockerKafka.SASLConfig{
		BrokerUser: dockerKafka.User{Username: "kafka1", Password: "password"},
		Users: []dockerKafka.User{
			{Username: "client1", Password: "password"},
		},
		CertificatePassword: "password",
		KeyStorePath:        filepath.Join(path, "testdata", "keystore", "kafka.keystore.jks"),
		TrustStorePath:      filepath.Join(path, "testdata", "truststore", "kafka.truststore.jks"),
	}

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	containerOptions := []dockerKafka.Option{
		dockerKafka.WithBrokers(1),
		dockerKafka.WithSASLPlain(&saslConfiguration),
	}
	if testing.Verbose() {
		containerOptions = append(containerOptions, dockerKafka.WithLogger(t))
	}
	kafkaContainer, err := dockerKafka.Setup(pool, &testCleanup{t}, containerOptions...)
	require.NoError(t, err)

	kafkaHost := fmt.Sprintf("localhost:%s", kafkaContainer.Ports[0])
	c, err := New("tcp", []string{"bad-host", kafkaHost}, Config{
		ClientID:    "some-client",
		DialTimeout: 10 * time.Second,
		SASL: &SASL{
			ScramHashGen: ScramPlainText,
			Username:     "A BAD USER",
			Password:     "A BAD PASSWORD",
		},
		TLS: &TLS{
			InsecureSkipVerify: true,
		},
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		err := c.Ping(context.Background())
		if err != nil {
			t.Logf("Ping error: %v", err)
		}
		return strings.Contains(err.Error(), "SASL Authentication failed")
	}, defaultTestTimeout, 250*time.Millisecond)
}

func TestProducer_Timeout(t *testing.T) {
	// Prepare cluster - Zookeeper and one Kafka broker
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	kafkaContainer, err := dockerKafka.Setup(pool, &testCleanup{t},
		dockerKafka.WithLogger(t),
		dockerKafka.WithBrokers(1))
	require.NoError(t, err)

	kafkaHost := fmt.Sprintf("localhost:%s", kafkaContainer.Ports[0])
	c, err := New("tcp", []string{"bad-host", kafkaHost}, Config{ClientID: "some-client", DialTimeout: 5 * time.Second})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	tc := testutil.NewWithDialer(c.dialer, c.network, c.addresses...)

	// Check connectivity and try to create the desired topic until the brokers are up and running (max 30s)
	require.NoError(t, c.Ping(ctx))
	require.Eventually(t, func() bool {
		err := tc.CreateTopic(ctx, t.Name(), 1, 1) // partitions = 2, replication factor = 1
		if err != nil {
			t.Logf("Could not create topic: %v", err)
		}
		return err == nil
	}, defaultTestTimeout, time.Second)

	// Check that the topic has been created with the right number of partitions
	topics, err := tc.ListTopics(ctx)
	require.NoError(t, err)
	require.Equal(t, []testutil.TopicPartition{{Topic: t.Name(), Partition: 0}}, topics)

	// Produce X messages in a single batch
	producerConf := ProducerConfig{ClientID: "producer-01"}
	if testing.Verbose() {
		producerConf.Logger = &testLogger{t}
		producerConf.ErrorLogger = producerConf.Logger
	}
	p, err := c.NewProducer(producerConf)
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := p.Close(ctx); err != nil {
			t.Logf("Error closing producer: %v", err)
		}
	})

	pubCtx, pubCancel := context.WithTimeout(ctx, 30*time.Second)
	err = p.Publish(pubCtx, Message{
		Key:   []byte("hello"),
		Value: []byte("world"),
		Topic: t.Name(),
	})
	pubCancel()
	require.NoError(t, err)

	pubCtx, pubCancel = context.WithTimeout(ctx, time.Nanosecond)
	err = p.Publish(pubCtx, Message{
		Key:   []byte("hello"),
		Value: []byte("world"),
		Topic: t.Name(),
	})
	defer pubCancel()
	require.Error(t, err)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestIsProducerErrTemporary(t *testing.T) {
	// Prepare cluster - Zookeeper and one Kafka broker
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	kafkaContainer, err := dockerKafka.Setup(pool, &testCleanup{t},
		dockerKafka.WithLogger(t),
		dockerKafka.WithBrokers(1))
	require.NoError(t, err)

	kafkaHost := fmt.Sprintf("localhost:%s", kafkaContainer.Ports[0])
	c, err := New("tcp", []string{"bad-host", kafkaHost}, Config{ClientID: "some-client", DialTimeout: 5 * time.Second})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	// Check connectivity and try to create the desired topic until the brokers are up and running (max 30s)
	require.NoError(t, c.Ping(ctx))

	tc := testutil.NewWithDialer(c.dialer, c.network, c.addresses...)
	require.Eventually(t, func() bool {
		err := tc.CreateTopic(ctx, t.Name(), 1, 1) // partitions = 2, replication factor = 1
		if err != nil {
			t.Logf("Could not create topic: %v", err)
		}
		return err == nil
	}, defaultTestTimeout, time.Second)

	// Check that the topic has been created with the right number of partitions
	topics, err := tc.ListTopics(ctx)
	require.NoError(t, err)
	require.Equal(t, []testutil.TopicPartition{{Topic: t.Name(), Partition: 0}}, topics)

	// Produce X messages in a single batch
	producerConf := ProducerConfig{ClientID: "producer-01"}
	if testing.Verbose() {
		producerConf.Logger = &testLogger{t}
		producerConf.ErrorLogger = producerConf.Logger
	}
	p, err := c.NewProducer(producerConf)
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := p.Close(ctx); err != nil {
			t.Logf("Error closing producer: %v", err)
		}
	})

	p.writer.AllowAutoTopicCreation = false // important to cause side effects needed next

	pubCtx, pubCancel := context.WithTimeout(ctx, 30*time.Second)
	err = p.Publish(pubCtx, Message{
		Key:   []byte("key-01"),
		Value: []byte("value-01"),
		Topic: "non-existent-topic",
	}, Message{
		Key:   []byte("key-02"),
		Value: []byte("value-02"),
		Topic: "non-existent-topic",
	})
	require.Truef(t, IsProducerErrTemporary(err), "Expected temporary error, got %v instead", err)
	pubCancel()
}

func TestIsProducerWrappedErrTemporary(t *testing.T) {
	err := kafka.WriteErrors{
		fmt.Errorf("some error: %w", kafka.LeaderNotAvailable),
		fmt.Errorf("some error: %w", kafka.RequestTimedOut),
		fmt.Errorf("some error: %w", kafka.OffsetOutOfRange),
		fmt.Errorf("some error: %w", kafka.Unknown),
	}
	require.True(t, IsProducerErrTemporary(err))

	wrappedErr := fmt.Errorf("could not publish to %q: %w", "some topic", err)
	require.True(t, IsProducerErrTemporary(wrappedErr))

	wrappedErr = fmt.Errorf("wrapping again: %w", wrappedErr)
	require.True(t, IsProducerErrTemporary(wrappedErr))
}

func TestWriteErrors(t *testing.T) {
	err := make(kafka.WriteErrors, 0)
	err = append(err, kafka.PolicyViolation)
	require.False(t, IsProducerErrTemporary(err))
	err = append(err, kafka.LeaderNotAvailable)
	require.True(t, IsProducerErrTemporary(err))
}

func TestConfluentAzureCloud(t *testing.T) {
	kafkaHost := os.Getenv("TEST_KAFKA_CONFLUENT_CLOUD_HOST")
	confluentCloudKey := os.Getenv("TEST_KAFKA_CONFLUENT_CLOUD_KEY")
	confluentCloudSecret := os.Getenv("TEST_KAFKA_CONFLUENT_CLOUD_SECRET")

	if kafkaHost == "" || confluentCloudKey == "" || confluentCloudSecret == "" {
		t.Skip("Skipping because credentials or host are not provided")
	}

	c, err := NewConfluentCloud([]string{"bad-host", kafkaHost}, confluentCloudKey, confluentCloudSecret, Config{
		ClientID:    "some-client",
		DialTimeout: 45 * time.Second,
	})
	require.NoError(t, err)
	require.NoError(t, c.Ping(context.Background()))

	producerConf := ProducerConfig{
		ClientID:     "producer-01",
		WriteTimeout: 30 * time.Second,
	}
	if testing.Verbose() {
		producerConf.Logger = &testLogger{t}
		producerConf.ErrorLogger = producerConf.Logger
	}
	p, err := c.NewProducer(
		producerConf,
	)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	// the topic needs to be created beforehand via the ConfluentCloud admin panel
	err = p.Publish(ctx, Message{Key: []byte("key-01"), Value: []byte("value-01"), Topic: t.Name()})
	cancel()
	require.NoError(t, err)

	c, err = NewConfluentCloud([]string{kafkaHost}, "A BAD KEY", confluentCloudSecret, Config{
		ClientID:    "some-client",
		DialTimeout: 45 * time.Second,
	})
	require.NoError(t, err)
	err = c.Ping(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "SASL Authentication failed")
}

func TestAzureEventHubsCloud(t *testing.T) {
	kafkaHost := os.Getenv("TEST_KAFKA_AZURE_EVENT_HUBS_CLOUD_HOST")
	azureEventHubName := os.Getenv("TEST_KAFKA_AZURE_EVENT_HUBS_CLOUD_EVENTHUB_NAME")
	azureEventHubsConnString := os.Getenv("TEST_KAFKA_AZURE_EVENT_HUBS_CLOUD_CONNECTION_STRING")

	if kafkaHost == "" || azureEventHubName == "" || azureEventHubsConnString == "" {
		t.Skip("Skipping because credentials or host are not provided")
	}

	c, err := NewAzureEventHubs([]string{kafkaHost}, azureEventHubsConnString, Config{
		ClientID:    "some-client",
		DialTimeout: 45 * time.Second,
	})
	require.NoError(t, err)
	require.NoError(t, c.Ping(context.Background()))

	producerConf := ProducerConfig{
		ClientID:     "producer-01",
		WriteTimeout: 30 * time.Second,
	}
	if testing.Verbose() {
		producerConf.Logger = &testLogger{t}
		producerConf.ErrorLogger = producerConf.Logger
	}
	p, err := c.NewProducer(producerConf)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	err = p.Publish(ctx, Message{Key: []byte("key-01"), Value: []byte("value-01"), Topic: azureEventHubName})

	cancel()
	require.NoError(t, err)

	c, err = NewAzureEventHubs([]string{"bad-host", kafkaHost}, "A BAD CONNECTION STRING", Config{
		ClientID:    "some-client",
		DialTimeout: 45 * time.Second,
	})
	require.NoError(t, err)
	err = c.Ping(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "SASL Authentication failed")
}

func TestSSH(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	// Start shared Docker network
	network, err := pool.Client.CreateNetwork(dc.CreateNetworkOptions{Name: "kafka_network"})
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := pool.Client.RemoveNetwork(network.ID); err != nil {
			t.Logf("Error while removing Docker network: %v", err)
		}
	})

	// Start Kafka cluster with ZooKeeper and three brokers
	_, err = dockerKafka.Setup(pool, &testCleanup{t},
		dockerKafka.WithBrokers(3),
		dockerKafka.WithLogger(t),
		dockerKafka.WithNetwork(network),
		dockerKafka.WithoutDockerHostListeners(),
	)
	require.NoError(t, err)

	// Let's setup the SSH server
	publicKeyPath, err := filepath.Abs("./testdata/ssh/test_key.pub")
	require.NoError(t, err)
	sshServer, err := sshserver.Setup(pool, &testCleanup{t},
		sshserver.WithPublicKeyPath(publicKeyPath),
		sshserver.WithCredentials("linuxserver.io", ""),
		sshserver.WithDockerNetwork(network),
		sshserver.WithLogger(t),
	)
	require.NoError(t, err)
	sshServerHost := fmt.Sprintf("localhost:%s", sshServer.Port)
	t.Logf("SSH server is listening on %s", sshServerHost)

	// Read private key
	privateKey, err := os.ReadFile("./testdata/ssh/test_key")
	require.NoError(t, err)

	// Setup client and ping
	ctx := context.Background()
	c, err := New("tcp", []string{"kafka1:9092", "kafka2:9092", "kafka3:9092"}, Config{
		SSHConfig: &SSHConfig{
			User:       "linuxserver.io",
			Host:       sshServerHost,
			PrivateKey: string(privateKey),
		},
	})
	require.NoError(t, err)
	require.Eventuallyf(t, func() bool { err = c.Ping(ctx); return err == nil }, 30*time.Second, time.Second,
		"could not ping kafka: %v", err,
	)

	// Create topic for test
	tc := testutil.NewWithDialer(c.dialer, c.network, c.addresses...)
	require.Eventually(t, func() bool {
		err := tc.CreateTopic(ctx, t.Name(), 1, 1) // partitions = 1, replication factor = 1
		if err != nil {
			t.Logf("Could not create topic: %v", err)
		}
		return err == nil
	}, defaultTestTimeout, time.Second)

	// Check that the topic has been created with the right number of partitions
	var topics []testutil.TopicPartition
	require.Eventually(t, func() bool {
		topics, err = tc.ListTopics(ctx)
		success := err == nil && len(topics) > 0
		if !success {
			t.Logf("List topics failure %+v: %v", topics, err)
		}
		return success
	}, defaultTestTimeout, time.Second)
	require.Equal(t, []testutil.TopicPartition{{Topic: t.Name(), Partition: 0}}, topics)

	// Check producer
	producerConf := ProducerConfig{ClientID: "producer-01"}
	if testing.Verbose() {
		producerConf.Logger = &testLogger{t}
		producerConf.ErrorLogger = producerConf.Logger
	}
	p, err := c.NewProducer(producerConf)
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := p.Close(ctx); err != nil {
			t.Logf("Error closing producer: %v", err)
		}
	})

	pubCtx, pubCancel := context.WithTimeout(ctx, 30*time.Second)
	defer pubCancel()
	require.Eventually(t, func() bool {
		err = p.Publish(pubCtx,
			Message{Key: []byte("key-01"), Value: []byte("value-01"), Topic: t.Name()},
		)
		if err != nil {
			t.Logf("Could not publish message: %v", err)
		}
		return err == nil
	}, 30*time.Second, time.Second, "could not publish message: %v", err)

	// Verify that the message has been published and it's readable
	consumer := c.NewConsumer(t.Name(), ConsumerConfig{})
	consumerCtx, consumerCancel := context.WithTimeout(ctx, 10*time.Second)
	defer consumerCancel()
	msg, err := consumer.Receive(consumerCtx)
	require.NoError(t, err)
	require.Equal(t, "key-01", string(msg.Key))
	require.Equal(t, "value-01", string(msg.Value))
}

func publishMessages(ctx context.Context, t *testing.T, p *Producer, noOfMessages int) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := p.Close(ctx); err != nil {
			t.Logf("Error closing producer: %v", err)
		}
	})

	messages := make([]Message, noOfMessages)
	for i := 0; i < noOfMessages; i++ {
		messages[i] = Message{
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(fmt.Sprintf("value-%d", i)),
			Topic: t.Name(),
		}
	}

	start, end := time.Now(), time.Duration(0)
	require.Eventually(t, func() bool {
		pubCtx, pubCancel := context.WithTimeout(ctx, 30*time.Second)
		err := p.Publish(pubCtx, messages...)
		end = time.Since(start)
		pubCancel()
		if err != nil {
			t.Logf("Got publish error: %v", err)
		}
		return err == nil
	}, defaultTestTimeout, time.Second)

	t.Logf("Messages published (%d) in %s", noOfMessages, end)
}

type testLogger struct{ *testing.T }

func (l *testLogger) Printf(format string, args ...interface{}) {
	l.Helper()
	l.Logf(format, args...)
}

type testCleanup struct{ *testing.T }

func (t *testCleanup) Defer(fn func() error) {
	t.Cleanup(func() {
		if err := fn(); err != nil {
			t.Log(err)
		}
	})
}
