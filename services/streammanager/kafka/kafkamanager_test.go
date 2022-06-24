package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/ory/dockertest/v3"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"

	mockStats "github.com/rudderlabs/rudder-server/mocks/services/stats"
	"github.com/rudderlabs/rudder-server/services/streammanager/kafka/client"
	"github.com/rudderlabs/rudder-server/testhelper/destination"
)

var (
	overrideArm64Check bool
	sinceDuration      = time.Second
)

func TestMain(m *testing.M) {
	kafkaStats = managerStats{}

	start := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	now = func() time.Time { return start }
	since = func(tt time.Time) time.Duration {
		if tt != start {
			panic(fmt.Errorf("since called with unexpected time, got %s instead of %s", tt, start))
		}
		return sinceDuration
	}

	if os.Getenv("OVERRIDE_ARM64_CHECK") == "1" {
		overrideArm64Check = true
	}
	pkgLogger = &nopLogger{}
	os.Exit(m.Run())
}

func TestNewProducer(t *testing.T) {
	t.Run("invalid destination configuration", func(t *testing.T) {
		kafkaStats.creationTime = getMockedTimer(t, gomock.NewController(t), 1)

		var destConfig func()
		p, err := NewProducer(destConfig, Opts{})
		require.Nil(t, p)
		require.EqualError(t, err, "[Kafka] Error while marshaling destination configuration <nil>, "+
			"got error: json: unsupported type: func()")
	})

	t.Run("missing configuration data", func(t *testing.T) {
		t.Run("missing topic", func(t *testing.T) {
			kafkaStats.creationTime = getMockedTimer(t, gomock.NewController(t), 1)

			var destConfig interface{}
			p, err := NewProducer(destConfig, Opts{})
			require.Nil(t, p)
			require.EqualError(t, err, "invalid configuration: topic cannot be empty")
		})
		t.Run("missing hostname", func(t *testing.T) {
			kafkaStats.creationTime = getMockedTimer(t, gomock.NewController(t), 1)

			destConfig := map[string]interface{}{
				"topic": "some-topic",
			}
			p, err := NewProducer(destConfig, Opts{})
			require.Nil(t, p)
			require.EqualError(t, err, "invalid configuration: hostname cannot be empty")
		})
		t.Run("missing port", func(t *testing.T) {
			kafkaStats.creationTime = getMockedTimer(t, gomock.NewController(t), 1)

			destConfig := map[string]interface{}{
				"topic":    "some-topic",
				"hostname": "some-hostname",
			}
			p, err := NewProducer(destConfig, Opts{})
			require.Nil(t, p)
			require.EqualError(t, err, `invalid configuration: invalid port: strconv.Atoi: parsing "": invalid syntax`)
		})
		t.Run("invalid port", func(t *testing.T) {
			kafkaStats.creationTime = getMockedTimer(t, gomock.NewController(t), 1)

			destConfig := map[string]interface{}{
				"topic":    "some-topic",
				"hostname": "some-hostname",
				"port":     "0",
			}
			p, err := NewProducer(destConfig, Opts{})
			require.Nil(t, p)
			require.EqualError(t, err, `invalid configuration: invalid port: 0`)
		})
	})

	t.Run("ok", func(t *testing.T) {
		if runtime.GOARCH == "arm64" && !overrideArm64Check {
			t.Skip("arm64 is not supported yet")
		}

		kafkaStats.creationTime = getMockedTimer(t, gomock.NewController(t), 1)

		pool, err := dockertest.NewPool("")
		require.NoError(t, err)

		kafkaContainer, err := destination.SetupKafka(pool, &testCleanup{t},
			destination.WithLogger(t),
			destination.WithBrokers(1))
		require.NoError(t, err)

		destConfig := map[string]interface{}{
			"topic":    "some-topic",
			"hostname": "localhost",
			"port":     kafkaContainer.Port,
		}
		p, err := NewProducer(destConfig, Opts{})
		require.NotNil(t, p)
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			err = p.Publish(ctx, client.Message{Key: []byte("key-01"), Value: []byte("message-01")})
			cancel()
			return err == nil
		}, 60*time.Second, 100*time.Millisecond)
	})
}

func TestNewProducerForAzureEventHubs(t *testing.T) {
	t.Run("invalid destination configuration", func(t *testing.T) {
		kafkaStats.creationTimeAzureEventHubs = getMockedTimer(t, gomock.NewController(t), 1)

		var destConfig func()
		p, err := NewProducerForAzureEventHubs(destConfig, Opts{})
		require.Nil(t, p)
		require.EqualError(t, err, "[Azure Event Hubs] Error while marshaling destination configuration <nil>, "+
			"got error: json: unsupported type: func()")
	})

	t.Run("missing configuration data", func(t *testing.T) {
		t.Run("missing topic", func(t *testing.T) {
			kafkaStats.creationTimeAzureEventHubs = getMockedTimer(t, gomock.NewController(t), 1)

			var destConfig interface{}
			p, err := NewProducerForAzureEventHubs(destConfig, Opts{})
			require.Nil(t, p)
			require.EqualError(t, err, "invalid configuration: topic cannot be empty")
		})
		t.Run("missing bootstrap server", func(t *testing.T) {
			kafkaStats.creationTimeAzureEventHubs = getMockedTimer(t, gomock.NewController(t), 1)

			destConfig := map[string]interface{}{
				"topic": "some-topic",
			}
			p, err := NewProducerForAzureEventHubs(destConfig, Opts{})
			require.Nil(t, p)
			require.EqualError(t, err, "invalid configuration: bootstrap server cannot be empty")
		})
		t.Run("missing connection string", func(t *testing.T) {
			kafkaStats.creationTimeAzureEventHubs = getMockedTimer(t, gomock.NewController(t), 1)

			destConfig := map[string]interface{}{
				"topic":           "some-topic",
				"bootstrapServer": "some-server",
			}
			p, err := NewProducerForAzureEventHubs(destConfig, Opts{})
			require.Nil(t, p)
			require.EqualError(t, err, "invalid configuration: connection string cannot be empty")
		})
	})

	t.Run("ok", func(t *testing.T) {
		kafkaHost := os.Getenv("TEST_KAFKA_AZURE_EVENT_HUBS_CLOUD_HOST")
		azureEventHubName := os.Getenv("TEST_KAFKA_AZURE_EVENT_HUBS_CLOUD_EVENTHUB_NAME")
		azureEventHubsConnString := os.Getenv("TEST_KAFKA_AZURE_EVENT_HUBS_CLOUD_CONNECTION_STRING")

		if kafkaHost == "" || azureEventHubName == "" || azureEventHubsConnString == "" {
			t.Skip("Skipping because credentials or host are not provided")
		}

		kafkaStats.creationTimeAzureEventHubs = getMockedTimer(t, gomock.NewController(t), 1)

		destConfig := map[string]interface{}{
			"topic":                     azureEventHubName,
			"bootstrapServer":           kafkaHost + "," + kafkaHost,
			"eventHubsConnectionString": azureEventHubsConnString,
		}
		p, err := NewProducerForAzureEventHubs(destConfig, Opts{})
		require.NotNil(t, p)
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			err = p.Publish(ctx, client.Message{Key: []byte("key-01"), Value: []byte("message-01")})
			cancel()
			return err == nil
		}, 60*time.Second, 100*time.Millisecond)
	})
}

func TestProducerForConfluentCloud(t *testing.T) {
	t.Run("invalid destination configuration", func(t *testing.T) {
		kafkaStats.creationTimeConfluentCloud = getMockedTimer(t, gomock.NewController(t), 1)

		var destConfig func()
		p, err := NewProducerForConfluentCloud(destConfig, Opts{})
		require.Nil(t, p)
		require.EqualError(t, err, "[Confluent Cloud] Error while marshaling destination configuration <nil>, "+
			"got error: json: unsupported type: func()")
	})

	t.Run("missing configuration data", func(t *testing.T) {
		t.Run("missing topic", func(t *testing.T) {
			kafkaStats.creationTimeConfluentCloud = getMockedTimer(t, gomock.NewController(t), 1)

			var destConfig interface{}
			p, err := NewProducerForConfluentCloud(destConfig, Opts{})
			require.Nil(t, p)
			require.EqualError(t, err, "invalid configuration: topic cannot be empty")
		})
		t.Run("missing bootstrap server", func(t *testing.T) {
			kafkaStats.creationTimeConfluentCloud = getMockedTimer(t, gomock.NewController(t), 1)

			destConfig := map[string]interface{}{
				"topic": "some-topic",
			}
			p, err := NewProducerForConfluentCloud(destConfig, Opts{})
			require.Nil(t, p)
			require.EqualError(t, err, "invalid configuration: bootstrap server cannot be empty")
		})
		t.Run("missing api key", func(t *testing.T) {
			kafkaStats.creationTimeConfluentCloud = getMockedTimer(t, gomock.NewController(t), 1)

			destConfig := map[string]interface{}{
				"topic":           "some-topic",
				"bootstrapServer": "some-server",
			}
			p, err := NewProducerForConfluentCloud(destConfig, Opts{})
			require.Nil(t, p)
			require.EqualError(t, err, "invalid configuration: API key cannot be empty")
		})
		t.Run("missing api secret", func(t *testing.T) {
			kafkaStats.creationTimeConfluentCloud = getMockedTimer(t, gomock.NewController(t), 1)

			destConfig := map[string]interface{}{
				"topic":           "some-topic",
				"bootstrapServer": "some-server",
				"apiKey":          "secret-key",
			}
			p, err := NewProducerForConfluentCloud(destConfig, Opts{})
			require.Nil(t, p)
			require.EqualError(t, err, "invalid configuration: API secret cannot be empty")
		})
	})

	t.Run("ok", func(t *testing.T) {
		kafkaHost := os.Getenv("TEST_KAFKA_CONFLUENT_CLOUD_HOST")
		confluentCloudKey := os.Getenv("TEST_KAFKA_CONFLUENT_CLOUD_KEY")
		confluentCloudSecret := os.Getenv("TEST_KAFKA_CONFLUENT_CLOUD_SECRET")

		if kafkaHost == "" || confluentCloudKey == "" || confluentCloudSecret == "" {
			t.Skip("Skipping because credentials or host are not provided")
		}

		kafkaStats.creationTimeConfluentCloud = getMockedTimer(t, gomock.NewController(t), 1)

		destConfig := map[string]interface{}{
			"topic":           "TestConfluentAzureCloud",
			"bootstrapServer": kafkaHost + "," + kafkaHost,
			"apiKey":          confluentCloudKey,
			"apiSecret":       confluentCloudSecret,
		}
		p, err := NewProducerForConfluentCloud(destConfig, Opts{})
		require.NoError(t, err)
		require.NotNil(t, p)

		require.Eventually(t, func() bool {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			err = p.Publish(ctx, client.Message{Key: []byte("key-01"), Value: []byte("message-01")})
			cancel()
			return err == nil
		}, 60*time.Second, 100*time.Millisecond)
	})
}

func TestPrepareBatchOfMessages(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockSkippedDueToUserID := mockStats.NewMockRudderStats(ctrl)
	mockSkippedDueToMessage := mockStats.NewMockRudderStats(ctrl)
	mockPrepareBatchTime := mockStats.NewMockRudderStats(ctrl)
	kafkaStats = managerStats{
		missingUserID:    mockSkippedDueToUserID,
		missingMessage:   mockSkippedDueToMessage,
		prepareBatchTime: mockPrepareBatchTime,
	}

	t.Run("nil", func(t *testing.T) {
		mockPrepareBatchTime.EXPECT().SendTiming(sinceDuration).Times(1)

		var data []map[string]interface{}
		batch, err := prepareBatchOfMessages("some-topic", data, time.Now())
		require.NoError(t, err)
		require.Nil(t, batch)
	})

	t.Run("no message", func(t *testing.T) {
		mockSkippedDueToMessage.EXPECT().Increment().Times(1)
		mockPrepareBatchTime.EXPECT().SendTiming(sinceDuration).Times(1)

		data := []map[string]interface{}{{
			"not-interesting": "some value",
		}}
		batch, err := prepareBatchOfMessages("some-topic", data, time.Now())
		require.NoError(t, err)
		require.Nil(t, batch)
	})

	t.Run("with message and user id", func(t *testing.T) {
		mockSkippedDueToUserID.EXPECT().Increment().Times(1)
		mockSkippedDueToMessage.EXPECT().Increment().Times(1)
		mockPrepareBatchTime.EXPECT().SendTiming(sinceDuration).Times(1)

		now := time.Now()
		data := []map[string]interface{}{
			{"not-interesting": "some value"},
			{"message": "msg01"},
			{"message": "msg02", "userId": "123"},
			{"message": map[string]interface{}{"a": 1, "b": 2}, "userId": "456"},
		}
		batch, err := prepareBatchOfMessages("some-topic", data, now)
		require.NoError(t, err)
		require.ElementsMatch(t, []client.Message{
			{
				Key:       []byte("123"),
				Value:     []byte(`"msg02"`),
				Topic:     "some-topic",
				Timestamp: now,
			},
			{
				Key:       []byte("456"),
				Value:     []byte(`{"a":1,"b":2}`),
				Topic:     "some-topic",
				Timestamp: now,
			},
		}, batch)
	})

	t.Run("with empty user id and allow empty", func(t *testing.T) {
		mockSkippedDueToMessage.EXPECT().Increment().Times(1)
		mockPrepareBatchTime.EXPECT().SendTiming(sinceDuration).Times(1)

		now := time.Now()
		allowReqsWithoutUserIDAndAnonymousID = true
		data := []map[string]interface{}{
			{"not-interesting": "some value"},
			{"message": "msg01"},
		}
		batch, err := prepareBatchOfMessages("some-topic", data, now)
		require.NoError(t, err)
		require.ElementsMatch(t, []client.Message{
			{
				Key:       []byte(""),
				Value:     []byte(`"msg01"`),
				Topic:     "some-topic",
				Timestamp: now,
			},
		}, batch)
	})
}

func TestCloseProducer(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		kafkaStats.closeProducerTime = getMockedTimer(t, gomock.NewController(t), 1)

		err := CloseProducer(context.Background(), nil)
		require.EqualError(t, err, "error while closing producer")
	})
	t.Run("not initialized", func(t *testing.T) {
		kafkaStats.closeProducerTime = getMockedTimer(t, gomock.NewController(t), 1)

		p := &producerImpl{p: &client.Producer{}}
		err := CloseProducer(context.Background(), p)
		require.NoError(t, err)
	})
	t.Run("correct", func(t *testing.T) {
		kafkaStats.closeProducerTime = getMockedTimer(t, gomock.NewController(t), 1)

		c, err := client.New("tcp", []string{"localhost:9092"}, client.Config{})
		require.NoError(t, err)
		p, err := c.NewProducer("some-topic", client.ProducerConfig{})
		require.NoError(t, err)
		err = CloseProducer(context.Background(), &producerImpl{p: p})
		require.NoError(t, err)
	})
	t.Run("error", func(t *testing.T) {
		kafkaStats.closeProducerTime = getMockedTimer(t, gomock.NewController(t), 1)

		var p producer = &pMockErr{error: fmt.Errorf("a bad error")}
		err := CloseProducer(context.Background(), p)
		require.EqualError(t, err, "a bad error")
	})
}

func TestProduce(t *testing.T) {
	t.Run("invalid producer", func(t *testing.T) {
		kafkaStats.produceTime = getMockedTimer(t, gomock.NewController(t), 1)

		sc, res, err := Produce(nil, nil, nil)
		require.Equal(t, 400, sc)
		require.Equal(t, "Could not create producer", res)
		require.Equal(t, "Could not create producer", err)
	})

	t.Run("invalid destination configuration", func(t *testing.T) {
		kafkaStats.produceTime = getMockedTimer(t, gomock.NewController(t), 1)

		p := &pMockErr{}
		destConfig := make(chan struct{}) // channels cannot be JSON marshalled
		sc, res, err := Produce(nil, p, destConfig)
		require.Equal(t, 400, sc)
		require.Equal(t, "json: unsupported type: chan struct {} error occurred.", res)
		require.Equal(t, "json: unsupported type: chan struct {}", err)
	})

	t.Run("empty destination configuration", func(t *testing.T) {
		kafkaStats.produceTime = getMockedTimer(t, gomock.NewController(t), 1)

		p := &pMockErr{}
		destConfig := map[string]interface{}{"foo": "bar"}
		sc, res, err := Produce(json.RawMessage(""), p, destConfig)
		require.Equal(t, 400, sc)
		require.Equal(t, "invalid destination configuration: no topic error occurred.", res)
		require.Equal(t, "invalid destination configuration: no topic", err)
	})

	t.Run("invalid message", func(t *testing.T) {
		kafkaStats.produceTime = getMockedTimer(t, gomock.NewController(t), 1)

		p := &pMockErr{}
		destConfig := map[string]interface{}{"topic": "foo-bar"}
		sc, res, err := Produce(json.RawMessage(""), p, destConfig)
		require.Equal(t, 400, sc)
		require.Equal(t, "Failure", res)
		require.Equal(t, "Invalid message", err)
	})

	t.Run("producer error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		kafkaStats.publishTime = getMockedTimer(t, ctrl, 1)
		kafkaStats.produceTime = getMockedTimer(t, ctrl, 1)

		p := &pMockErr{error: fmt.Errorf("super bad")}
		destConfig := map[string]interface{}{"topic": "foo-bar"}
		sc, res, err := Produce(json.RawMessage(`{"message":"ciao"}`), p, destConfig)
		require.Equal(t, 400, sc)
		require.Equal(t, "super bad error occurred.", res)
		require.Equal(t, "super bad", err)
	})

	t.Run("producer retryable error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		kafkaStats.publishTime = getMockedTimer(t, ctrl, 1)
		kafkaStats.produceTime = getMockedTimer(t, ctrl, 1)

		p := &pMockErr{error: kafka.LeaderNotAvailable}
		destConfig := map[string]interface{}{"topic": "foo-bar"}
		sc, res, err := Produce(json.RawMessage(`{"message":"ciao"}`), p, destConfig)
		require.Equal(t, 500, sc)
		require.Equal(t, kafka.LeaderNotAvailable.Error()+" error occurred.", res)
		require.Equal(t, kafka.LeaderNotAvailable.Error(), err)
	})

	t.Run("ok", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		kafkaStats.publishTime = getMockedTimer(t, ctrl, 1)
		kafkaStats.produceTime = getMockedTimer(t, ctrl, 1)

		p := &pMockErr{}
		destConfig := map[string]interface{}{"topic": "foo-bar"}
		sc, res, err := Produce(json.RawMessage(`{"message":"ciao"}`), p, destConfig)
		require.Equal(t, 200, sc)
		require.Equal(t, "Message delivered to topic: foo-bar", res)
		require.Equal(t, "Message delivered to topic: foo-bar", err)
	})
}

func TestSendBatchedMessage(t *testing.T) {
	t.Run("invalid json", func(t *testing.T) {
		sc, res, err := sendBatchedMessage(
			context.Background(),
			json.RawMessage("{{{"),
			nil,
			"some-topic",
		)
		require.Equal(t, 400, sc)
		require.Equal(t, "Failure", res)
		require.Equal(t, "Error while unmarshalling json data: "+
			"invalid character '{' looking for beginning of object key string", err)
	})

	t.Run("invalid data", func(t *testing.T) {
		sc, res, err := sendBatchedMessage(
			context.Background(),
			json.RawMessage(`{"message":"ciao"}`), // not a slice of map[string]interface{}
			nil,
			"some-topic",
		)
		require.Equal(t, 400, sc)
		require.Equal(t, "Failure", res)
		require.Equal(t, "Error while unmarshalling json data: "+
			"json: cannot unmarshal object into Go value of type []map[string]interface {}", err)
	})

	t.Run("publisher error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		kafkaStats.publishTime = getMockedTimer(t, ctrl, 1)
		kafkaStats.prepareBatchTime = getMockedTimer(t, ctrl, 1)

		p := &pMockErr{error: fmt.Errorf("something bad")}
		sc, res, err := sendBatchedMessage(
			context.Background(),
			json.RawMessage(`[{"message":"ciao","userId":"123"}]`),
			p,
			"some-topic",
		)
		require.Equal(t, 400, sc)
		require.Equal(t, "something bad error occurred.", res)
		require.Equal(t, "something bad", err)
		require.Len(t, p.calls, 1)
		require.Len(t, p.calls[0], 1)
		require.Equal(t, []byte("123"), p.calls[0][0].Key)
		require.Equal(t, []byte(`"ciao"`), p.calls[0][0].Value)
		require.Equal(t, "some-topic", p.calls[0][0].Topic)
		require.InDelta(t, time.Now().Unix(), p.calls[0][0].Timestamp.Unix(), 1)
	})

	t.Run("publisher retryable error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		kafkaStats.publishTime = getMockedTimer(t, ctrl, 1)
		kafkaStats.prepareBatchTime = getMockedTimer(t, ctrl, 1)

		p := &pMockErr{error: kafka.LeaderNotAvailable}
		sc, res, err := sendBatchedMessage(
			context.Background(),
			json.RawMessage(`[{"message":"ciao","userId":"123"}]`),
			p,
			"some-topic",
		)
		require.Equal(t, 500, sc)
		require.Equal(t, kafka.LeaderNotAvailable.Error()+" error occurred.", res)
		require.Equal(t, kafka.LeaderNotAvailable.Error(), err)
		require.Len(t, p.calls, 1)
		require.Len(t, p.calls[0], 1)
		require.Equal(t, []byte("123"), p.calls[0][0].Key)
		require.Equal(t, []byte(`"ciao"`), p.calls[0][0].Value)
		require.Equal(t, "some-topic", p.calls[0][0].Topic)
		require.InDelta(t, time.Now().Unix(), p.calls[0][0].Timestamp.Unix(), 1)
	})

	t.Run("ok", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		kafkaStats.publishTime = getMockedTimer(t, ctrl, 1)
		kafkaStats.prepareBatchTime = getMockedTimer(t, ctrl, 1)

		p := &pMockErr{error: nil}
		sc, res, err := sendBatchedMessage(
			context.Background(),
			json.RawMessage(`[{"message":"ciao","userId":"123"}]`),
			p,
			"some-topic",
		)
		require.Equal(t, 200, sc)
		require.Equal(t, "Kafka: Message delivered in batch", res)
		require.Equal(t, "Kafka: Message delivered in batch", err)
		require.Len(t, p.calls, 1)
		require.Len(t, p.calls[0], 1)
		require.Equal(t, []byte("123"), p.calls[0][0].Key)
		require.Equal(t, []byte(`"ciao"`), p.calls[0][0].Value)
		require.Equal(t, "some-topic", p.calls[0][0].Topic)
		require.InDelta(t, time.Now().Unix(), p.calls[0][0].Timestamp.Unix(), 1)
	})
}

func TestSendMessage(t *testing.T) {
	t.Run("invalid json", func(t *testing.T) {
		sc, res, err := sendMessage(context.Background(), json.RawMessage("{{{"), nil, "some-topic")
		require.Equal(t, 400, sc)
		require.Equal(t, "Failure", res)
		require.Equal(t, "Invalid message", err)
	})

	t.Run("no message", func(t *testing.T) {
		sc, res, err := sendMessage(context.Background(), json.RawMessage("{}"), nil, "some-topic")
		require.Equal(t, 400, sc)
		require.Equal(t, "Failure", res)
		require.Equal(t, "Invalid message", err)
	})

	t.Run("no userId", func(t *testing.T) {
		kafkaStats.publishTime = getMockedTimer(t, gomock.NewController(t), 1)

		p := &pMockErr{error: nil}
		sc, res, err := sendMessage(context.Background(), json.RawMessage(`{"message":"ciao"}`), p, "some-topic")
		require.Equal(t, 200, sc)
		require.Equal(t, "Message delivered to topic: some-topic", res)
		require.Equal(t, "Message delivered to topic: some-topic", err)
		require.Len(t, p.calls, 1)
		require.Len(t, p.calls[0], 1)
		require.Equal(t, []byte(""), p.calls[0][0].Key)
		require.Equal(t, []byte(`"ciao"`), p.calls[0][0].Value)
		require.Equal(t, "some-topic", p.calls[0][0].Topic)
		require.InDelta(t, time.Now().Unix(), p.calls[0][0].Timestamp.Unix(), 1)
	})

	t.Run("publisher error", func(t *testing.T) {
		kafkaStats.publishTime = getMockedTimer(t, gomock.NewController(t), 1)

		p := &pMockErr{error: fmt.Errorf("something bad")}
		sc, res, err := sendMessage(
			context.Background(),
			json.RawMessage(`{"message":"ciao","userId":"123"}`),
			p,
			"some-topic",
		)
		require.Equal(t, 400, sc)
		require.Equal(t, "something bad error occurred.", res)
		require.Equal(t, "something bad", err)
		require.Len(t, p.calls, 1)
		require.Len(t, p.calls[0], 1)
		require.Equal(t, []byte("123"), p.calls[0][0].Key)
		require.Equal(t, []byte(`"ciao"`), p.calls[0][0].Value)
		require.Equal(t, "some-topic", p.calls[0][0].Topic)
		require.InDelta(t, time.Now().Unix(), p.calls[0][0].Timestamp.Unix(), 1)
	})
}

func getMockedTimer(t *testing.T, ctrl *gomock.Controller, times int) *mockStats.MockRudderStats {
	t.Helper()
	mockedTimer := mockStats.NewMockRudderStats(ctrl)
	mockedTimer.EXPECT().SendTiming(sinceDuration).Times(times)
	return mockedTimer
}

// Mocks
type pMockErr struct {
	error error
	calls [][]client.Message
}

func (*pMockErr) getTimeout() time.Duration       { return 0 }
func (p *pMockErr) Close(_ context.Context) error { return p.error }
func (p *pMockErr) Publish(_ context.Context, msgs ...client.Message) error {
	p.calls = append(p.calls, msgs)
	return p.error
}

type nopLogger struct{}

func (*nopLogger) Error(...interface{})          {}
func (*nopLogger) Errorf(string, ...interface{}) {}

type testCleanup struct{ *testing.T }

func (t *testCleanup) Defer(fn func() error) {
	t.Cleanup(func() {
		if err := fn(); err != nil {
			t.Log(err)
		}
	})
}
