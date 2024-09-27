package kafka

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	kafkaConfluent "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/avro"
	"github.com/linkedin/goavro/v2"
	"github.com/ory/dockertest/v3"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/config"
	client "github.com/rudderlabs/rudder-go-kit/kafkaclient"
	"github.com/rudderlabs/rudder-go-kit/kafkaclient/testutil"
	"github.com/rudderlabs/rudder-go-kit/stats/mock_stats"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker"
	dockerKafka "github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/kafka"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/sshserver"
	"github.com/rudderlabs/rudder-go-kit/testhelper/keygen"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/services/streammanager/common"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

var sinceDuration = time.Second

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

	pkgLogger = &nopLogger{}
	os.Exit(m.Run())
}

func TestNewProducer(t *testing.T) {
	requiredFields := map[string]interface{}{
		"hostname": "some-hostname",
		"topic":    "some-topic",
		"port":     "1234",
	}
	withRequired := func(m map[string]interface{}) map[string]interface{} {
		for k, v := range requiredFields {
			m[k] = v
		}
		return m
	}
	buildTest := func(destConfig map[string]interface{}, expectedErr string) func(*testing.T) {
		return func(t *testing.T) {
			kafkaStats.creationTime = getMockedTimer(t, gomock.NewController(t), false)

			dest := backendconfig.DestinationT{Config: destConfig}

			_, err := NewProducer(&dest, common.Opts{})
			require.ErrorContains(t, err, expectedErr)
		}
	}
	t.Run("missing configuration data", func(t *testing.T) {
		t.Run("missing topic", buildTest(
			map[string]interface{}{},
			"invalid configuration: topic cannot be empty"),
		)
		t.Run("missing hostname", buildTest(
			map[string]interface{}{
				"topic": "some-topic",
			},
			"invalid configuration: hostname cannot be empty"),
		)
		t.Run("missing port", buildTest(
			map[string]interface{}{
				"topic":    "some-topic",
				"hostname": "some-hostname",
			},
			"invalid configuration: invalid port"),
		)
		t.Run("invalid port", buildTest(
			map[string]interface{}{
				"topic":    "some-topic",
				"hostname": "some-hostname",
				"port":     "0",
			},
			"invalid configuration: invalid port"),
		)
		t.Run("invalid schema", buildTest(
			withRequired(map[string]interface{}{
				"convertToAvro": true,
				"avroSchemas": []interface{}{
					map[string]string{"schemaId": "schema001"},
					map[string]interface{}{
						"schema": map[string]string{"name": "MyClass"},
					},
				},
			}),
			"Error while unmarshalling destination configuration "+
				"map[avroSchemas:[map[schemaId:schema001] map[schema:map[name:MyClass]]] "+
				"convertToAvro:true hostname:some-hostname port:1234 topic:some-topic], "+
				"got error: json: cannot unmarshal object into Go struct field "+
				"avroSchema.AvroSchemas.Schema of type string"),
		)
		t.Run("invalid ssh config", func(t *testing.T) {
			t.Run("missing ssh host", buildTest(
				withRequired(map[string]interface{}{
					"useSSH": true,
				}),
				"invalid configuration: ssh host cannot be empty",
			))
			t.Run("missing ssh port", buildTest(
				withRequired(map[string]interface{}{
					"useSSH":  true,
					"sshHost": "random-host",
					"sshUser": "johnDoe",
				}),
				"invalid configuration: invalid ssh port",
			))
			t.Run("invalid ssh port", buildTest(
				withRequired(map[string]interface{}{
					"useSSH":  true,
					"sshHost": "random-host",
					"sshUser": "johnDoe",
					"sshPort": "65536",
				}),
				"invalid configuration: invalid ssh port",
			))
			t.Run("missing ssh user", buildTest(
				withRequired(map[string]interface{}{
					"useSSH":  true,
					"sshHost": "random-host",
					"sshPort": "1234",
				}),
				"invalid configuration: ssh user cannot be empty",
			))
		})
	})

	t.Run("ok", func(t *testing.T) {
		kafkaStats.creationTime = getMockedTimer(t, gomock.NewController(t), false)

		pool, err := dockertest.NewPool("")
		require.NoError(t, err)

		kafkaContainer, err := dockerKafka.Setup(pool, &testCleanup{t}, dockerKafka.WithBrokers(1))
		require.NoError(t, err)

		kafkaHost, kafkaPort, err := net.SplitHostPort(kafkaContainer.Brokers[0])
		require.NoError(t, err)

		destConfig := map[string]interface{}{
			"topic":    "some-topic",
			"hostname": kafkaHost,
			"port":     kafkaPort,
		}
		dest := backendconfig.DestinationT{Config: destConfig}
		p, err := NewProducer(&dest, common.Opts{})
		require.NotNilf(t, p, "expected producer to be created, got nil: %v", err)
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			err = p.p.Publish(ctx, client.Message{
				Key:   []byte("key-01"),
				Value: []byte("message-01"),
				Topic: destConfig["topic"].(string),
			})
			cancel()
			return err == nil
		}, 60*time.Second, 100*time.Millisecond)
	})

	t.Run("ok ssh", func(t *testing.T) {
		kafkaStats.creationTime = getMockedTimer(t, gomock.NewController(t), false)

		pool, err := dockertest.NewPool("")
		require.NoError(t, err)

		// Start shared Docker network
		network, err := docker.CreateNetwork(pool, t, "kafka_network")
		require.NoError(t, err)
		t.Cleanup(func() {
			if err := pool.Client.RemoveNetwork(network.ID); err != nil {
				t.Logf("Error while removing Docker network: %v", err)
			}
		})

		// Start Kafka cluster with ZooKeeper and one broker
		_, err = dockerKafka.Setup(pool, &testCleanup{t},
			dockerKafka.WithBrokers(1),
			dockerKafka.WithNetwork(network),
			dockerKafka.WithoutDockerHostListeners(),
		)
		require.NoError(t, err)

		// Prepare key pair
		privateKeyPath, publicKeyPath, err := keygen.NewRSAKeyPair(2048, keygen.SaveTo(t.TempDir()))
		require.NoError(t, err)
		privateKey, err := os.ReadFile(privateKeyPath)
		require.NoError(t, err)
		publicKey, err := os.ReadFile(publicKeyPath)
		require.NoError(t, err)

		// Setup SSH server
		sshServer, err := sshserver.Setup(pool, &testCleanup{t},
			sshserver.WithPublicKeyPath(publicKeyPath),
			sshserver.WithCredentials("linuxserver.io", ""),
			sshserver.WithDockerNetwork(network),
		)
		require.NoError(t, err)

		// Setup ControlPlane to return the right SSH key pair
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "/dataplane/admin/destinations/123/sshKeys", r.URL.String())
			username, password, ok := r.BasicAuth()
			require.True(t, ok)
			require.Equal(t, "johnDoe", username)
			require.Equal(t, "so-secret", password)
			pvtKey := bytes.ReplaceAll(privateKey, []byte("\n"), []byte("\\n"))
			pubKey := bytes.ReplaceAll(publicKey, []byte("\n"), []byte("\\n"))
			_, _ = w.Write([]byte(`{
				"privateKey": "` + string(pvtKey) + `",
				"publicKey": "` + string(pubKey) + `"
			}`))
		}))
		defer t.Cleanup(srv.Close)

		t.Setenv("WORKSPACE_TOKEN", "someWorkspaceToken")
		t.Setenv("CONFIG_BACKEND_URL", srv.URL)
		t.Setenv("CP_INTERNAL_API_USERNAME", "johnDoe")
		t.Setenv("CP_INTERNAL_API_PASSWORD", "so-secret")

		require.NoError(t, backendconfig.Setup(nil))
		defer backendconfig.DefaultBackendConfig.Stop()

		// Populate destConfig according to the above configuration
		destConfig := map[string]interface{}{
			"topic":    "some-topic",
			"hostname": "kafka1",
			"port":     "9092",
			"useSSH":   true,
			"sshHost":  "localhost",
			"sshPort":  strconv.Itoa(sshServer.Port),
			"sshUser":  "linuxserver.io",
		}
		dest := backendconfig.DestinationT{ID: "123", Config: destConfig}

		p, err := NewProducer(&dest, common.Opts{})
		require.NotNilf(t, p, "expected producer to be created, got nil: %v", err)
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			err = p.p.Publish(ctx, client.Message{
				Key:   []byte("key-01"),
				Value: []byte("message-01"),
				Topic: destConfig["topic"].(string),
			})
			cancel()
			return err == nil
		}, 60*time.Second, 100*time.Millisecond)
	})
}

func TestIntegration(t *testing.T) {
	t.Run("batch", func(t *testing.T) {
		kafkaBatchingEnabled = true
		t.Cleanup(func() { kafkaBatchingEnabled = false })
		ctrl := gomock.NewController(t)
		kafkaStats.creationTime = getMockedTimer(t, ctrl, true)
		kafkaStats.produceTime = getMockedTimer(t, ctrl, true)
		kafkaStats.prepareBatchTime = getMockedTimer(t, ctrl, true)
		kafkaStats.publishTime = getMockedTimer(t, ctrl, true)
		kafkaStats.batchSize = mock_stats.NewMockMeasurement(ctrl)
		kafkaStats.batchSize.(*mock_stats.MockMeasurement).EXPECT().Observe(3.0).Times(1)

		pool, err := dockertest.NewPool("")
		require.NoError(t, err)

		kafkaContainer, err := dockerKafka.Setup(pool, &testCleanup{t}, dockerKafka.WithBrokers(1))
		require.NoError(t, err)
		kafkaHost, kafkaPort, err := net.SplitHostPort(kafkaContainer.Brokers[0])
		require.NoError(t, err)
		destConfig := map[string]interface{}{
			"topic":    "some-topic",
			"hostname": kafkaHost,
			"port":     kafkaPort,
		}
		dest := backendconfig.DestinationT{Config: destConfig}

		p, err := NewProducer(&dest, common.Opts{})
		require.NotNilf(t, p, "expected producer to be created, got nil: %v", err)
		require.NoError(t, err)

		var statusCode int
		var returnMessage, errMessage string
		require.Eventually(t, func() bool {
			statusCode, returnMessage, errMessage = p.Produce(json.RawMessage(`[
				{"message":"one","topic":"foo-bar","userId":"1234"},
				{"message":"two","topic":"foo-bar","userId":"1234"},
				{"message":"three","topic":"foo-bar","userId":"1234"}
			]`), destConfig)
			return statusCode == http.StatusOK
		}, 30*time.Second, 100*time.Millisecond)
		require.Equal(t, "Kafka: Message delivered in batch", returnMessage)
		require.Equal(t, "Kafka: Message delivered in batch", errMessage)

		c, err := client.New("tcp", kafkaContainer.Brokers, client.Config{})
		require.NoError(t, err)
		require.NoError(t, c.Ping(context.Background()))

		var (
			msgs     []client.Message
			consumer = c.NewConsumer("foo-bar", client.ConsumerConfig{})
		)

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		for i := 0; i < 3; {
			var message client.Message
			message, err = consumer.Receive(ctx)
			if err == nil {
				i++
				msgs = append(msgs, message)
			} else {
				break
			}
		}

		require.NoError(t, err)
		require.Len(t, msgs, 3)
		requireEqualMessage(t, "foo-bar", "1234", `"one"`, msgs[0])
		requireEqualMessage(t, "foo-bar", "1234", `"two"`, msgs[1])
		requireEqualMessage(t, "foo-bar", "1234", `"three"`, msgs[2])
	})
}

func TestAIOKafka(t *testing.T) {
	kafkaBatchingEnabled = true
	kafkaCompression = client.CompressionZstd
	t.Cleanup(func() {
		kafkaBatchingEnabled = false
		kafkaCompression = client.CompressionNone
	})
	ctrl := gomock.NewController(t)
	kafkaStats.creationTime = getMockedTimer(t, ctrl, true)
	kafkaStats.produceTime = getMockedTimer(t, ctrl, true)
	kafkaStats.prepareBatchTime = getMockedTimer(t, ctrl, true)
	kafkaStats.publishTime = getMockedTimer(t, ctrl, true)
	kafkaStats.batchSize = mock_stats.NewMockMeasurement(ctrl)
	kafkaStats.batchSize.(*mock_stats.MockMeasurement).EXPECT().Observe(3.0).Times(1)

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	kafkaNetwork, err := docker.CreateNetwork(pool, t, "kafka_network")
	require.NoError(t, err)

	kafkaContainer, err := dockerKafka.Setup(pool, &testCleanup{t},
		dockerKafka.WithBrokers(1),
		dockerKafka.WithNetwork(kafkaNetwork),
	)
	require.NoError(t, err)

	kafkaTopic := "some-topic"
	kafkaHost, kafkaPort, err := net.SplitHostPort(kafkaContainer.Brokers[0])
	require.NoError(t, err)

	destConfig := map[string]interface{}{
		"topic":    kafkaTopic,
		"hostname": kafkaHost,
		"port":     kafkaPort,
	}
	dest := backendconfig.DestinationT{Config: destConfig}

	p, err := NewProducer(&dest, common.Opts{})
	require.NotNilf(t, p, "expected producer to be created, got nil: %v", err)
	require.NoError(t, err)

	var statusCode int
	var returnMessage, errMessage string
	require.Eventually(t, func() bool {
		statusCode, returnMessage, errMessage = p.Produce(json.RawMessage(`[
			{"message":"one","topic":"foo-bar","userId":"1234"},
			{"message":"two","topic":"foo-bar","userId":"1234"},
			{"message":"three","topic":"foo-bar","userId":"1234"}
		]`), destConfig)
		return statusCode == http.StatusOK
	}, 30*time.Second, 100*time.Millisecond)
	require.Equal(t, "Kafka: Message delivered in batch", returnMessage)
	require.Equal(t, "Kafka: Message delivered in batch", errMessage)

	consumerContainer, err := pool.BuildAndRunWithOptions("./testdata/aiokafka/Dockerfile", &dockertest.RunOptions{
		Name:      fmt.Sprintf("aiokafka-%s", misc.FastUUID().String()),
		NetworkID: kafkaNetwork.ID,
		Cmd:       []string{"tail", "-f", "/dev/null"},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := pool.Purge(consumerContainer); err != nil {
			t.Logf("Error while purging Docker container: %v", err)
		}
	})

	buf := bytes.NewBuffer(nil)
	code, err := consumerContainer.Exec([]string{"python", "consumer.py"}, dockertest.ExecOptions{
		Env:    []string{"KAFKA_BROKER=kafka1:9090", "KAFKA_TOPIC=foo-bar", "EXPECTED_MESSAGE_COUNT=3"},
		StdOut: buf,
		StdErr: os.Stderr,
	})

	require.NoError(t, err)
	require.Equal(t, 0, code)
	require.JSONEq(t, `[{"key": "1234", "value": "\"one\""}, {"key": "1234", "value": "\"two\""}, {"key": "1234", "value": "\"three\""}]`, buf.String())
}

func TestNewProducerForAzureEventHubs(t *testing.T) {
	t.Run("missing configuration data", func(t *testing.T) {
		t.Run("missing topic", func(t *testing.T) {
			kafkaStats.creationTimeAzureEventHubs = getMockedTimer(t, gomock.NewController(t), false)

			dest := backendconfig.DestinationT{}
			p, err := NewProducerForAzureEventHubs(&dest, common.Opts{})
			require.Nil(t, p)
			require.ErrorContains(t, err, "invalid configuration: topic cannot be empty")
		})
		t.Run("missing bootstrap server", func(t *testing.T) {
			kafkaStats.creationTimeAzureEventHubs = getMockedTimer(t, gomock.NewController(t), false)

			destConfig := map[string]interface{}{
				"topic": "some-topic",
			}
			dest := backendconfig.DestinationT{Config: destConfig}

			p, err := NewProducerForAzureEventHubs(&dest, common.Opts{})
			require.Nil(t, p)
			require.ErrorContains(t, err, "invalid configuration: bootstrap server cannot be empty")
		})
		t.Run("missing connection string", func(t *testing.T) {
			kafkaStats.creationTimeAzureEventHubs = getMockedTimer(t, gomock.NewController(t), false)

			destConfig := map[string]interface{}{
				"topic":           "some-topic",
				"bootstrapServer": "some-server",
			}
			dest := backendconfig.DestinationT{Config: destConfig}

			p, err := NewProducerForAzureEventHubs(&dest, common.Opts{})
			require.Nil(t, p)
			require.ErrorContains(t, err, "invalid configuration: connection string cannot be empty")
		})
	})

	t.Run("ok", func(t *testing.T) {
		kafkaHost := os.Getenv("TEST_KAFKA_AZURE_EVENT_HUBS_CLOUD_HOST")
		azureEventHubName := os.Getenv("TEST_KAFKA_AZURE_EVENT_HUBS_CLOUD_EVENTHUB_NAME")
		azureEventHubsConnString := os.Getenv("TEST_KAFKA_AZURE_EVENT_HUBS_CLOUD_CONNECTION_STRING")

		if kafkaHost == "" || azureEventHubName == "" || azureEventHubsConnString == "" {
			t.Skip("Skipping because credentials or host are not provided")
		}

		kafkaStats.creationTimeAzureEventHubs = getMockedTimer(t, gomock.NewController(t), false)

		destConfig := map[string]interface{}{
			"topic":                     azureEventHubName,
			"bootstrapServer":           "bad-host," + kafkaHost + "," + kafkaHost,
			"eventHubsConnectionString": azureEventHubsConnString,
		}
		dest := backendconfig.DestinationT{Config: destConfig}

		p, err := NewProducerForAzureEventHubs(&dest, common.Opts{})
		require.NotNil(t, p)
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			err = p.p.Publish(ctx, client.Message{Key: []byte("key-01"), Value: []byte("message-01"), Topic: destConfig["topic"].(string)})
			cancel()
			return err == nil
		}, 60*time.Second, 100*time.Millisecond)
	})
}

func TestProducerForConfluentCloud(t *testing.T) {
	t.Run("missing configuration data", func(t *testing.T) {
		t.Run("missing topic", func(t *testing.T) {
			kafkaStats.creationTimeConfluentCloud = getMockedTimer(t, gomock.NewController(t), false)

			dest := backendconfig.DestinationT{}
			p, err := NewProducerForConfluentCloud(&dest, common.Opts{})
			require.Nil(t, p)
			require.ErrorContains(t, err, "invalid configuration: topic cannot be empty")
		})
		t.Run("missing bootstrap server", func(t *testing.T) {
			kafkaStats.creationTimeConfluentCloud = getMockedTimer(t, gomock.NewController(t), false)

			destConfig := map[string]interface{}{
				"topic": "some-topic",
			}
			dest := backendconfig.DestinationT{Config: destConfig}

			p, err := NewProducerForConfluentCloud(&dest, common.Opts{})
			require.Nil(t, p)
			require.ErrorContains(t, err, "invalid configuration: bootstrap server cannot be empty")
		})
		t.Run("missing api key", func(t *testing.T) {
			kafkaStats.creationTimeConfluentCloud = getMockedTimer(t, gomock.NewController(t), false)

			destConfig := map[string]interface{}{
				"topic":           "some-topic",
				"bootstrapServer": "some-server",
			}
			dest := backendconfig.DestinationT{Config: destConfig}

			p, err := NewProducerForConfluentCloud(&dest, common.Opts{})
			require.Nil(t, p)
			require.ErrorContains(t, err, "invalid configuration: API key cannot be empty")
		})
		t.Run("missing api secret", func(t *testing.T) {
			kafkaStats.creationTimeConfluentCloud = getMockedTimer(t, gomock.NewController(t), false)

			destConfig := map[string]interface{}{
				"topic":           "some-topic",
				"bootstrapServer": "some-server",
				"apiKey":          "secret-key",
			}
			dest := backendconfig.DestinationT{Config: destConfig}

			p, err := NewProducerForConfluentCloud(&dest, common.Opts{})
			require.Nil(t, p)
			require.ErrorContains(t, err, "invalid configuration: API secret cannot be empty")
		})
	})

	t.Run("ok", func(t *testing.T) {
		kafkaHost := os.Getenv("TEST_KAFKA_CONFLUENT_CLOUD_HOST")
		confluentCloudKey := os.Getenv("TEST_KAFKA_CONFLUENT_CLOUD_KEY")
		confluentCloudSecret := os.Getenv("TEST_KAFKA_CONFLUENT_CLOUD_SECRET")

		if kafkaHost == "" || confluentCloudKey == "" || confluentCloudSecret == "" {
			t.Skip("Skipping because credentials or host are not provided")
		}

		kafkaStats.creationTimeConfluentCloud = getMockedTimer(t, gomock.NewController(t), false)

		destConfig := map[string]interface{}{
			"topic":           "TestProducerForConfluentCloud_OK",
			"bootstrapServer": "bad-host," + kafkaHost + "," + kafkaHost,
			"apiKey":          confluentCloudKey,
			"apiSecret":       confluentCloudSecret,
		}
		dest := backendconfig.DestinationT{Config: destConfig}

		p, err := NewProducerForConfluentCloud(&dest, common.Opts{})
		require.NoError(t, err)
		require.NotNil(t, p)

		require.Eventually(t, func() bool {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			err = p.p.Publish(ctx, client.Message{Key: []byte("key-01"), Value: []byte("message-01"), Topic: destConfig["topic"].(string)})
			cancel()
			return err == nil
		}, 60*time.Second, 100*time.Millisecond)
	})
}

func TestPrepareBatchOfMessages(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockSkippedDueToUserID := mock_stats.NewMockMeasurement(ctrl)
	mockSkippedDueToMessage := mock_stats.NewMockMeasurement(ctrl)
	mockPrepareBatchTime := mock_stats.NewMockMeasurement(ctrl)
	kafkaStats = managerStats{
		missingUserID:    mockSkippedDueToUserID,
		missingMessage:   mockSkippedDueToMessage,
		prepareBatchTime: mockPrepareBatchTime,
	}
	allowReqsWithoutUserIDAndAnonymousID = config.SingleValueLoader(false)

	t.Run("nil", func(t *testing.T) {
		mockPrepareBatchTime.EXPECT().SendTiming(sinceDuration).Times(1)

		var data []map[string]interface{}
		pm := &ProducerManager{p: &pMockErr{error: nil}}
		batch, err := prepareBatchOfMessages(data, time.Now(), pm, "some-topic")
		require.Equal(t, []client.Message(nil), batch)
		require.Equal(t, fmt.Errorf("unable to process any of the event in the batch"), err)
	})

	t.Run("no message", func(t *testing.T) {
		mockSkippedDueToMessage.EXPECT().Increment().Times(1)
		mockPrepareBatchTime.EXPECT().SendTiming(sinceDuration).Times(1)
		pm := &ProducerManager{p: &pMockErr{error: nil}}
		data := []map[string]interface{}{{
			"not-interesting": "some value",
			"topic":           "some-topic",
		}}
		batch, err := prepareBatchOfMessages(data, time.Now(), pm, "some-topic")
		require.Equal(t, []client.Message(nil), batch)
		require.Equal(t, fmt.Errorf("unable to process any of the event in the batch: "+
			"some errors are: [batch from topic some-topic is missing the message attribute]"), err)
	})

	t.Run("with message and user id", func(t *testing.T) {
		mockSkippedDueToUserID.EXPECT().Increment().Times(1)
		mockSkippedDueToMessage.EXPECT().Increment().Times(1)
		mockPrepareBatchTime.EXPECT().SendTiming(sinceDuration).Times(1)

		now := time.Now()
		data := []map[string]interface{}{
			{"not-interesting": "some value", "topic": "some-topic"},
			{"message": "msg01", "topic": "some-topic"},
			{"message": "msg02", "userId": "123", "topic": "some-topic"},
			{"message": map[string]interface{}{"a": 1, "b": 2}, "userId": "456", "topic": "some-topic"},
		}
		pm := &ProducerManager{p: &pMockErr{error: nil}}
		batch, err := prepareBatchOfMessages(data, now, pm, "some-topic")
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
		allowReqsWithoutUserIDAndAnonymousID = config.SingleValueLoader(true)
		data := []map[string]interface{}{
			{"not-interesting": "some value", "topic": "some-topic"},
			{"message": "msg01", "topic": "some-topic"},
		}
		pm := &ProducerManager{p: &pMockErr{error: nil}}
		batch, err := prepareBatchOfMessages(data, now, pm, "some-topic")
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

func TestClose(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		pm := &ProducerManager{}
		require.NoError(t, pm.Close())
	})
	t.Run("not initialized", func(t *testing.T) {
		kafkaStats.closeProducerTime = getMockedTimer(t, gomock.NewController(t), false)

		pm := &ProducerManager{p: &client.Producer{}}
		require.NoError(t, pm.Close())
	})
	t.Run("correct", func(t *testing.T) {
		kafkaStats.closeProducerTime = getMockedTimer(t, gomock.NewController(t), false)

		c, err := client.New("tcp", []string{"localhost:9092"}, client.Config{})
		require.NoError(t, err)
		p, err := c.NewProducer(client.ProducerConfig{})
		require.NoError(t, err)
		pm := &ProducerManager{p: p}
		require.NoError(t, pm.Close())
	})
	t.Run("error", func(t *testing.T) {
		kafkaStats.closeProducerTime = getMockedTimer(t, gomock.NewController(t), false)

		var pm producerManager = &ProducerManager{p: &pMockErr{error: fmt.Errorf("a bad error")}}
		require.EqualError(t, pm.Close(), "failed to close producer: a bad error")
	})
}

func TestProduce(t *testing.T) {
	t.Run("invalid producer", func(t *testing.T) {
		pm := ProducerManager{}
		sc, res, err := pm.Produce(nil, nil)
		require.Equal(t, 400, sc)
		require.Equal(t, "Could not create producer", res)
		require.Equal(t, "Could not create producer", err)
	})

	t.Run("invalid destination configuration", func(t *testing.T) {
		kafkaStats.produceTime = getMockedTimer(t, gomock.NewController(t), false)

		pm := ProducerManager{p: &client.Producer{}}
		destConfig := make(chan struct{}) // channels cannot be JSON marshalled
		sc, res, err := pm.Produce(nil, destConfig)
		require.Equal(t, 400, sc)
		require.Equal(t, "json: unsupported type: chan struct {} error occurred.", res)
		require.Equal(t, "json: unsupported type: chan struct {}", err)
	})

	t.Run("empty destination configuration", func(t *testing.T) {
		kafkaStats.produceTime = getMockedTimer(t, gomock.NewController(t), false)

		pm := ProducerManager{p: &client.Producer{}}
		destConfig := map[string]interface{}{"foo": "bar"}
		sc, res, err := pm.Produce(json.RawMessage(""), destConfig)
		require.Equal(t, 400, sc)
		require.Equal(t, "invalid destination configuration: no topic error occurred.", res)
		require.Equal(t, "invalid destination configuration: no topic", err)
	})

	t.Run("invalid message", func(t *testing.T) {
		kafkaStats.produceTime = getMockedTimer(t, gomock.NewController(t), false)

		pm := ProducerManager{p: &client.Producer{}}
		destConfig := map[string]interface{}{"topic": "foo-bar"}
		sc, res, err := pm.Produce(json.RawMessage(""), destConfig)
		require.Equal(t, 400, sc)
		require.Equal(t, "Failure", res)
		require.Equal(t, "Invalid message", err)
	})

	t.Run("producer error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		kafkaStats.publishTime = getMockedTimer(t, ctrl, false)
		kafkaStats.produceTime = getMockedTimer(t, ctrl, false)

		pm := &ProducerManager{p: &pMockErr{error: fmt.Errorf("super bad")}}
		destConfig := map[string]interface{}{"topic": "foo-bar"}
		sc, res, err := pm.Produce(json.RawMessage(`{"message":"ciao","topic": "foo-bar"}`), destConfig)
		require.Equal(t, 400, sc)
		require.Contains(t, res, "super bad error occurred.")
		require.Contains(t, err, "super bad")
	})

	t.Run("producer retryable error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		kafkaStats.publishTime = getMockedTimer(t, ctrl, false)
		kafkaStats.produceTime = getMockedTimer(t, ctrl, false)

		pm := &ProducerManager{p: &pMockErr{error: kafka.LeaderNotAvailable}}
		destConfig := map[string]interface{}{"topic": "foo-bar"}
		sc, res, err := pm.Produce(json.RawMessage(`{"message":"ciao","topic": "foo-bar"}`), destConfig)
		require.Equal(t, 500, sc)
		require.Contains(t, res, kafka.LeaderNotAvailable.Error()+" error occurred.")
		require.Contains(t, err, kafka.LeaderNotAvailable.Error())
	})

	t.Run("default-topic-test", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		kafkaStats.publishTime = getMockedTimer(t, ctrl, false)
		kafkaStats.produceTime = getMockedTimer(t, ctrl, false)

		pm := &ProducerManager{p: &pMockErr{}}
		destConfig := map[string]interface{}{"topic": "foo-bar"}
		sc, res, err := pm.Produce(json.RawMessage(`{"message":"ciao"}`), destConfig)
		require.Equal(t, 200, sc)
		require.Equal(t, "Message delivered to topic: foo-bar", res)
		require.Equal(t, "Message delivered to topic: foo-bar", err)
	})

	t.Run("ok", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		kafkaStats.publishTime = getMockedTimer(t, ctrl, false)
		kafkaStats.produceTime = getMockedTimer(t, ctrl, false)

		pm := &ProducerManager{p: &pMockErr{}}
		destConfig := map[string]interface{}{"topic": "foo-bar"}
		sc, res, err := pm.Produce(json.RawMessage(`{"message":"ciao","topic": "foo-bar"}`), destConfig)
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
		kafkaStats.publishTime = getMockedTimer(t, ctrl, false)
		kafkaStats.prepareBatchTime = getMockedTimer(t, ctrl, false)

		p := &pMockErr{error: fmt.Errorf("something bad")}
		pm := &ProducerManager{p: p}
		sc, res, err := sendBatchedMessage(
			context.Background(),
			json.RawMessage(`[{"message":"ciao","userId":"123","topic":"some-topic"}]`),
			pm,
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
		kafkaStats.publishTime = getMockedTimer(t, ctrl, false)
		kafkaStats.prepareBatchTime = getMockedTimer(t, ctrl, false)

		p := &pMockErr{error: kafka.LeaderNotAvailable}
		pm := &ProducerManager{p: p}
		sc, res, err := sendBatchedMessage(
			context.Background(),
			json.RawMessage(`[{"message":"ciao","userId":"123","topic":"some-topic"}]`),
			pm,
			"some-topic",
		)
		require.Equal(t, 500, sc)
		require.Contains(t, res, kafka.LeaderNotAvailable.Error()+" error occurred.")
		require.Equal(t, err, kafka.LeaderNotAvailable.Error())
		require.Len(t, p.calls, 1)
		require.Len(t, p.calls[0], 1)
		require.Equal(t, []byte("123"), p.calls[0][0].Key)
		require.Equal(t, []byte(`"ciao"`), p.calls[0][0].Value)
		require.Equal(t, "some-topic", p.calls[0][0].Topic)
		require.InDelta(t, time.Now().Unix(), p.calls[0][0].Timestamp.Unix(), 1)
	})

	t.Run("ok", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		kafkaStats.publishTime = getMockedTimer(t, ctrl, false)
		kafkaStats.prepareBatchTime = getMockedTimer(t, ctrl, false)
		kafkaStats.batchSize = mock_stats.NewMockMeasurement(ctrl)
		kafkaStats.batchSize.(*mock_stats.MockMeasurement).EXPECT().Observe(1.0).Times(1)

		p := &pMockErr{error: nil}
		pm := &ProducerManager{p: p}
		sc, res, err := sendBatchedMessage(
			context.Background(),
			json.RawMessage(`[{"message":"ciao","userId":"123","topic":"some-topic"}]`),
			pm,
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

	t.Run("default topic test", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		kafkaStats.publishTime = getMockedTimer(t, ctrl, false)
		kafkaStats.prepareBatchTime = getMockedTimer(t, ctrl, false)
		kafkaStats.batchSize = mock_stats.NewMockMeasurement(ctrl)
		kafkaStats.batchSize.(*mock_stats.MockMeasurement).EXPECT().Observe(1.0).Times(1)

		p := &pMockErr{error: nil}
		pm := &ProducerManager{p: p}
		sc, res, err := sendBatchedMessage(
			context.Background(),
			json.RawMessage(`[{"message":"ciao","userId":"123"}]`),
			pm,
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

	t.Run("schemaId not available", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		kafkaStats.prepareBatchTime = getMockedTimer(t, ctrl, false)
		kafkaStats.avroSerializationErr = getMockedCounter(t, ctrl)

		codec, codecErr := goavro.NewCodec(`{
			"namespace" : "kafkaAvroTest",
			"name": "myRecord",
			"type" :  "record",
			"fields" : [
			   {"name": "uid", "type": "int"},
			   {"name": "someField", "type": "string"}
			]
		}`)
		require.NoError(t, codecErr)
		pm := &pmMockErr{
			codecs: map[string]*goavro.Codec{
				"schemaId001": codec,
			},
		}
		sc, res, err := sendBatchedMessage(
			context.Background(),
			json.RawMessage(`[{"message":{"messageId":"message001","data":"ciao"},"userId":"123","topic":"some-topic"}]`),
			pm,
			"some-topic",
		)
		require.Equal(t, 400, sc)
		require.Equal(t, "Failure", res)
		require.Equal(t, "Error while preparing batched message: unable to process any of the event in the batch: "+
			"some errors are: [schemaId is not available for the event at index 0]", err)
	})

	t.Run("wrong codec", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		kafkaStats.prepareBatchTime = getMockedTimer(t, ctrl, false)
		kafkaStats.avroSerializationErr = getMockedCounter(t, ctrl)
		codec, codecErr := goavro.NewCodec(`{
			"namespace" : "kafkaAvroTest",
			"name": "myRecord",
			"type" :  "record",
			"fields" : [
			   {"name": "uid", "type": "int"},
			   {"name": "someField", "type": "string"}
			]
		}`)
		require.NoError(t, codecErr)
		pm := &pmMockErr{
			codecs: map[string]*goavro.Codec{
				"schemaId001": codec,
			},
		}
		sc, res, err := sendBatchedMessage(
			context.Background(),
			json.RawMessage(`[{"message":{"messageId":"message001","data":"ciao"},"userId":"123","schemaId":"schemaId001","topic":"some-topic"}]`),
			pm,
			"some-topic",
		)
		require.Equal(t, 400, sc)
		require.Equal(t, "Failure", res)
		require.Equal(t, "Error while preparing batched message: unable to process any of the event in the batch: "+
			"some errors are: [unable to serialize the event with schemaId \"schemaId001\" at index 0: unable convert "+
			"the event to native from textual, with error: cannot decode textual record \"kafkaAvroTest.myRecord\": "+
			"cannot decode textual map: cannot determine codec: \"data\"]", err)
	})

	t.Run("unavailable schemaId", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		kafkaStats.prepareBatchTime = getMockedTimer(t, ctrl, false)
		kafkaStats.avroSerializationErr = getMockedCounter(t, ctrl)
		codec, codecErr := goavro.NewCodec(`{
			"namespace" : "kafkaAvroTest",
			"name": "myRecord",
			"type" :  "record",
			"fields" : [
			   {"name": "uid", "type": "int"},
			   {"name": "someField", "type": "string"}
			]
		}`)
		require.NoError(t, codecErr)
		pm := &pmMockErr{
			codecs: map[string]*goavro.Codec{
				"schemaId001": codec,
			},
		}
		sc, res, err := sendBatchedMessage(
			context.Background(),
			json.RawMessage(`[{"message":{"messageId":"message001","data":"ciao"},"userId":"123","schemaId":"schemaId002","topic":"some-topic"}]`),
			pm,
			"some-topic",
		)
		require.Equal(t, 400, sc)
		require.Equal(t, "Failure", res)
		require.Equal(t, "Error while preparing batched message: unable to process any of the event in the batch: "+
			"some errors are: [unable to find schema with schemaId \"schemaId002\"]", err)
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
		kafkaStats.publishTime = getMockedTimer(t, gomock.NewController(t), false)

		p := &pMockErr{error: nil}
		pm := &ProducerManager{p: p}
		sc, res, err := sendMessage(context.Background(), json.RawMessage(`{"message":"ciao", "topic":"some-topic"}`), pm, "some-topic")
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
		kafkaStats.publishTime = getMockedTimer(t, gomock.NewController(t), false)

		p := &pMockErr{error: fmt.Errorf("something bad")}
		pm := &ProducerManager{p: p}
		sc, res, err := sendMessage(
			context.Background(),
			json.RawMessage(`{"message":"ciao","userId":"123","topic":"some-topic"}`),
			pm,
			"some-topic",
		)
		require.Equal(t, 400, sc)
		require.Contains(t, res, "something bad error occurred.")
		require.Contains(t, err, "something bad")
		require.Len(t, p.calls, 1)
		require.Len(t, p.calls[0], 1)
		require.Equal(t, []byte("123"), p.calls[0][0].Key)
		require.Equal(t, []byte(`"ciao"`), p.calls[0][0].Value)
		require.Equal(t, "some-topic", p.calls[0][0].Topic)
		require.InDelta(t, time.Now().Unix(), p.calls[0][0].Timestamp.Unix(), 1)
	})

	t.Run("schemaId not available", func(t *testing.T) {
		codec, _ := goavro.NewCodec(`{
			"namespace" : "kafkaAvroTest",
			"name": "myrecord",
			"type" :  "record",
			"fields" : [
			   {"name": "uid", "type": "int"},
			   {"name": "somefield", "type": "string"}
			]
		  }`)
		pm := &pmMockErr{
			codecs: map[string]*goavro.Codec{
				"schemaId001": codec,
			},
		}
		sc, res, err := sendMessage(
			context.Background(),
			json.RawMessage(`{"message":{"messageId":"message001","data":"ciao"},"userId":"123","topic":"some-topic"}`),
			pm,
			"some-topic",
		)
		require.Equal(t, 400, sc)
		require.Equal(t, "schemaId is not available for event with messageId: message001 error occurred.", res)
		require.Equal(t, "schemaId is not available for event with messageId: message001", err)
	})

	t.Run("wrong codec", func(t *testing.T) {
		codec, _ := goavro.NewCodec(`{
			"namespace" : "kafkaAvroTest",
			"name": "myrecord",
			"type" :  "record",
			"fields" : [
			   {"name": "uid", "type": "int"},
			   {"name": "somefield", "type": "string"}
			]
		  }`)
		pm := &pmMockErr{
			codecs: map[string]*goavro.Codec{
				"schemaId001": codec,
			},
		}
		sc, res, err := sendMessage(
			context.Background(),
			json.RawMessage(`{"message":{"messageId":"message001","data":"ciao"},"userId":"123","schemaId":"schemaId001","topic":"some-topic"}`),
			pm,
			"some-topic",
		)
		require.Equal(t, 400, sc)
		require.Equal(t, `unable to serialize event with schemaId "schemaId001" and messageId message001: unable convert the event to native from textual, with error: cannot decode textual record "kafkaAvroTest.myrecord": cannot decode textual map: cannot determine codec: "data" error occurred.`, res)
		require.Equal(t, `unable to serialize event with schemaId "schemaId001" and messageId message001: unable convert the event to native from textual, with error: cannot decode textual record "kafkaAvroTest.myrecord": cannot decode textual map: cannot determine codec: "data"`, err)
	})
}

func TestPublish(t *testing.T) {
	t.Run("no topic", func(t *testing.T) {
		kafkaStats.publishTime = getMockedTimer(t, gomock.NewController(t), false)
		p := &client.Producer{}
		pm := &ProducerManager{p: p}
		message := prepareMessage("", "userId", []byte(`{"test": "value"}`), time.Now())
		err := publish(context.Background(), pm, message)
		require.ErrorContains(t, err, "no topic provided for message 0")
	})
}

func TestSSHConfig(t *testing.T) {
	t.Run("not enabled", func(t *testing.T) {
		c := config.New()
		conf, err := getSSHConfig("some id", c)
		require.NoError(t, err)
		require.Nil(t, conf)
	})

	t.Run("enabled for another destination", func(t *testing.T) {
		c := config.New()
		c.Set("ROUTER_KAFKA_SSH_ENABLED", "dest1,dest3")
		conf, err := getSSHConfig("dest2", c)
		require.NoError(t, err)
		require.Nil(t, conf)
	})

	t.Run("no private key", func(t *testing.T) {
		c := config.New()
		c.Set("ROUTER_KAFKA_SSH_ENABLED", "dest2,dest1,dest5")
		conf, err := getSSHConfig("dest1", c)
		require.ErrorContains(t, err, "kafka SSH private key is not set")
		require.Nil(t, conf)
	})

	t.Run("no base64 private key", func(t *testing.T) {
		c := config.New()
		c.Set("ROUTER_KAFKA_SSH_ENABLED", "dest3,dest1,dest7")
		c.Set("ROUTER_KAFKA_SSH_PRIVATE_KEY", "not base64 encoded")
		conf, err := getSSHConfig("dest1", c)
		require.ErrorContains(t, err, "failed to decode base64 private key")
		require.Nil(t, conf)
	})

	t.Run("ok", func(t *testing.T) {
		c := config.New()
		c.Set("ROUTER_KAFKA_SSH_ENABLED", "dest0,dest1,dest6")
		c.Set("ROUTER_KAFKA_SSH_PRIVATE_KEY", "a2V5IGNvbnRlbnQ=")
		c.Set("ROUTER_KAFKA_SSH_USER", "some-user")
		c.Set("ROUTER_KAFKA_SSH_HOST", "1.2.3.4:22")
		c.Set("ROUTER_KAFKA_SSH_ACCEPT_ANY_HOST_KEY", "true")
		conf, err := getSSHConfig("dest1", c)
		require.NoError(t, err)
		require.Equal(t, &client.SSHConfig{
			User:       "some-user",
			Host:       "1.2.3.4:22",
			PrivateKey: "key content",
		}, conf)
	})
}

func TestAvroSchemaRegistry(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	t.Log("Creating Kafka cluster")
	kafkaContainer, err := dockerKafka.Setup(pool, &testCleanup{t},
		dockerKafka.WithBrokers(1),
		dockerKafka.WithSchemaRegistry())
	require.NoError(t, err)

	t.Log("Creating topic")
	var (
		ctx       = context.Background()
		topicName = "test-topic"
		tc        = testutil.New("tcp", kafkaContainer.Brokers[0])
	)
	require.Eventuallyf(t, func() bool {
		err = tc.CreateTopic(ctx, topicName, 1, 1) // partitions = 1, replication factor = 1
		return err == nil
	}, 30*time.Second, 100*time.Millisecond, "failed to create topic %s: %v", topicName, err)

	t.Logf("Creating schema registry client with %s", kafkaContainer.SchemaRegistryURL)
	schemaRegistryClient, err := schemaregistry.NewClient(schemaregistry.NewConfig(kafkaContainer.SchemaRegistryURL))
	require.NoError(t, err)
	cwd, err := os.Getwd()
	require.NoError(t, err)
	path := func(file string) string { return filepath.Join(cwd, "testdata", file) }
	_, schemaID1 := registerSchema(t, "user1", path("user1.avsc"), schemaRegistryClient)
	schema2, schemaID2 := registerSchema(t, "user2", path("user2.avsc"), schemaRegistryClient)
	t.Logf("Schema IDs: %d, %d", schemaID1, schemaID2)

	t.Log("Creating Kafka consumer")
	c, err := kafkaConfluent.NewConsumer(&kafkaConfluent.ConfigMap{
		"bootstrap.servers":  strings.Join(kafkaContainer.Brokers, ","),
		"group.id":           "group-1",
		"session.timeout.ms": 6000,
		"auto.offset.reset":  "earliest",
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	err = c.SubscribeTopics([]string{topicName}, nil)
	require.NoError(t, err)
	kafkaHost, kafkaPort, err := net.SplitHostPort(kafkaContainer.Brokers[0])
	require.NoError(t, err)
	var (
		destinationID = "DEST1"
		destConfig    = map[string]interface{}{
			"topic":         topicName,
			"hostname":      kafkaHost,
			"port":          kafkaPort,
			"convertToAvro": true,
			"avroSchemas": []map[string]interface{}{
				{"schemaId": fmt.Sprintf("%d", schemaID2), "schema": schema2},
			},
		}
		dest       = backendconfig.DestinationT{ID: destinationID, Config: destConfig}
		rawMessage = json.RawMessage(`{
			"userId": "123",
			"messageId": "456",
			"schemaId": "2",
			"topic": "` + topicName + `",
			"message": {
				"first_name": "John",
				"last_name": "Doe"
			}
		}`)
	)

	t.Run("plain avro", func(t *testing.T) {
		t.Cleanup(config.Reset)

		// Setting up mocks
		kafkaStats.produceTime = getMockedTimer(t, gomock.NewController(t), false)
		kafkaStats.publishTime = getMockedTimer(t, gomock.NewController(t), false)
		kafkaStats.creationTime = getMockedTimer(t, gomock.NewController(t), false)

		// Produce message in Avro format with schema 2
		t.Log("Creating Kafka producer")
		config.Set("ROUTER_KAFKA_EMBED_AVRO_SCHEMA_ID_"+destinationID, false)
		p, err := NewProducer(&dest, common.Opts{})
		require.NoError(t, err)
		require.NotNil(t, p)

		statusCode, returnMsg, errMsg := p.Produce(rawMessage, &destConfig)
		require.EqualValuesf(t, http.StatusOK, statusCode, "Produce failed: %s - %s", returnMsg, errMsg)

		// Start consuming
		t.Log("Consuming messages")
		timeout := time.After(5 * time.Second)
		for {
			select {
			case <-timeout:
				t.Fatal("Timed out waiting for expected message")
			default:
				ev := c.Poll(100)
				if ev == nil {
					continue
				}

				switch e := ev.(type) {
				case *kafkaConfluent.Message:
					codec, err := goavro.NewCodec(schema2)
					require.NoError(t, err)
					native, _, err := codec.NativeFromBinary(e.Value)
					require.NoError(t, err)
					require.Equal(t, map[string]interface{}{
						"first_name": "John",
						"last_name":  "Doe",
					}, native)
					return
				case kafkaConfluent.Error:
					t.Logf("Kafka Confluent Error: %v: %v", e.Code(), e)
				default:
					t.Logf("Ignoring consumer entry: %+v", e)
				}
			}
		}
	})

	type User struct {
		FirstName string `json:"first_name"`
		LastName  string `json:"last_name"`
	}

	consumeUserMsg := func(t *testing.T, deser *avro.GenericDeserializer) {
		timeout := time.After(5 * time.Second)
		for {
			select {
			case <-timeout:
				t.Fatal("Timed out waiting for expected message")
			default:
				ev := c.Poll(100)
				if ev == nil {
					continue
				}

				switch e := ev.(type) {
				case *kafkaConfluent.Message:
					value := User{}
					err = deser.DeserializeInto(*e.TopicPartition.Topic, e.Value, &value)
					require.NoErrorf(t, err, "Failed to deserialize payload: %s", err)
					require.Equal(t, User{FirstName: "John", LastName: "Doe"}, value)
					return
				case kafkaConfluent.Error:
					t.Logf("Kafka Confluent Error: %v: %v", e.Code(), e)
				default:
					t.Logf("Ignoring consumer entry: %+v", e)
				}
			}
		}
	}

	t.Run("avro with schema id embedded", func(t *testing.T) {
		t.Cleanup(config.Reset)

		// Setting up mocks
		kafkaStats.produceTime = getMockedTimer(t, gomock.NewController(t), false)
		kafkaStats.publishTime = getMockedTimer(t, gomock.NewController(t), false)
		kafkaStats.creationTime = getMockedTimer(t, gomock.NewController(t), false)

		// Produce message in Avro format with schema 2
		t.Log("Creating Kafka producer")
		config.Set("ROUTER_KAFKA_EMBED_AVRO_SCHEMA_ID_"+destinationID, true)
		p, err := NewProducer(&dest, common.Opts{})
		require.NoError(t, err)
		require.NotNil(t, p)

		statusCode, returnMsg, errMsg := p.Produce(rawMessage, &destConfig)
		require.EqualValuesf(t, http.StatusOK, statusCode, "Produce failed: %s - %s", returnMsg, errMsg)

		// Start consuming
		t.Log("Consuming messages")
		deser, err := avro.NewGenericDeserializer(schemaRegistryClient, serde.ValueSerde, avro.NewDeserializerConfig())
		require.NoError(t, err)

		consumeUserMsg(t, deser)
	})

	t.Run("avro with schema id embedding enabled via config", func(t *testing.T) {
		t.Cleanup(config.Reset)

		// Setting up mocks
		kafkaStats.produceTime = getMockedTimer(t, gomock.NewController(t), false)
		kafkaStats.publishTime = getMockedTimer(t, gomock.NewController(t), false)
		kafkaStats.creationTime = getMockedTimer(t, gomock.NewController(t), false)

		// Produce message in Avro format with schema 2
		t.Log("Creating Kafka producer")
		destConfig["embedAvroSchemaID"] = true
		p, err := NewProducer(&dest, common.Opts{})
		require.NoError(t, err)
		require.NotNil(t, p)

		statusCode, returnMsg, errMsg := p.Produce(rawMessage, &destConfig)
		require.EqualValuesf(t, http.StatusOK, statusCode, "Produce failed: %s - %s", returnMsg, errMsg)

		// Start consuming
		t.Log("Consuming messages")
		deser, err := avro.NewGenericDeserializer(schemaRegistryClient, serde.ValueSerde, avro.NewDeserializerConfig())
		require.NoError(t, err)

		consumeUserMsg(t, deser)
	})
}

func getMockedTimer(t *testing.T, ctrl *gomock.Controller, anyTimes bool) *mock_stats.MockMeasurement {
	t.Helper()
	mockedTimer := mock_stats.NewMockMeasurement(ctrl)
	call := mockedTimer.EXPECT().SendTiming(sinceDuration)
	if anyTimes {
		call.AnyTimes()
	} else {
		call.Times(1)
	}
	return mockedTimer
}

func getMockedCounter(t *testing.T, ctrl *gomock.Controller) *mock_stats.MockMeasurement {
	t.Helper()
	mockedCounter := mock_stats.NewMockMeasurement(ctrl)
	mockedCounter.EXPECT().Increment().Times(1)
	return mockedCounter
}

func registerSchema(t *testing.T, schemaName, schemaPath string, c schemaregistry.Client) (schema string, schemaID int) {
	t.Helper()

	buf, err := os.ReadFile(schemaPath)
	require.NoError(t, err)
	si := schemaregistry.SchemaInfo{Schema: string(buf)}

	require.Eventuallyf(t, func() bool {
		schemaID, err = c.Register(schemaName, si, true)
		return err == nil
	}, 30*time.Second, 100*time.Millisecond, "failed to register schema %s: %v", schemaName, err)

	schema = string(buf)
	return
}

func requireEqualMessage(t testing.TB, topic, key, value string, actual client.Message) {
	t.Helper()
	require.Equal(t, topic, actual.Topic)
	require.Equal(t, key, string(actual.Key))
	require.Equal(t, value, string(actual.Value))
}

// Mocks
type pmMockErr struct {
	codecs map[string]*goavro.Codec
}

func (*pmMockErr) Close() error                                         { return nil }
func (*pmMockErr) Publish(_ context.Context, _ ...client.Message) error { return nil }
func (*pmMockErr) getTimeout() time.Duration                            { return 0 }
func (*pmMockErr) getEmbedAvroSchemaID() bool                           { return false }
func (pm *pmMockErr) getCodecs() map[string]*goavro.Codec {
	return pm.codecs
}

type pMockErr struct {
	error error
	calls [][]client.Message
}

func (p *pMockErr) Close(_ context.Context) error { return p.error }
func (p *pMockErr) Publish(_ context.Context, msgs ...client.Message) error {
	p.calls = append(p.calls, msgs)
	return p.error
}

type nopLogger struct{}

func (*nopLogger) Error(...interface{})          {}
func (*nopLogger) Infof(string, ...interface{})  {}
func (*nopLogger) Errorf(string, ...interface{}) {}

type testCleanup struct{ *testing.T }

func (t *testCleanup) Defer(fn func() error) {
	t.Cleanup(func() {
		if err := fn(); err != nil {
			t.Log(err)
		}
	})
}
