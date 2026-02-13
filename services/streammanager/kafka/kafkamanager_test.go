package kafka

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"maps"
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
	dc "github.com/ory/dockertest/v3/docker"
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
	requiredFields := map[string]any{
		"hostname": "some-hostname",
		"topic":    "some-topic",
		"port":     "1234",
	}
	withRequired := func(m map[string]any) map[string]any {
		maps.Copy(m, requiredFields)
		return m
	}
	buildTest := func(destConfig map[string]any, expectedErr string) func(*testing.T) {
		return func(t *testing.T) {
			kafkaStats.creationTime = getMockedTimer(t, gomock.NewController(t), false)

			dest := backendconfig.DestinationT{Config: destConfig}

			_, err := NewProducer(&dest, common.Opts{})
			require.ErrorContains(t, err, expectedErr)
		}
	}
	t.Run("missing configuration data", func(t *testing.T) {
		t.Run("missing topic", buildTest(
			map[string]any{},
			"invalid configuration: topic cannot be empty"),
		)
		t.Run("missing hostname", buildTest(
			map[string]any{
				"topic": "some-topic",
			},
			"invalid configuration: hostname cannot be empty"),
		)
		t.Run("missing port", buildTest(
			map[string]any{
				"topic":    "some-topic",
				"hostname": "some-hostname",
			},
			"invalid configuration: invalid port"),
		)
		t.Run("invalid port", buildTest(
			map[string]any{
				"topic":    "some-topic",
				"hostname": "some-hostname",
				"port":     "0",
			},
			"invalid configuration: invalid port"),
		)
		t.Run("invalid schema", buildTest(
			withRequired(map[string]any{
				"convertToAvro": true,
				"avroSchemas": []any{
					map[string]string{"schemaId": "schema001"},
					map[string]any{
						"schema": map[string]string{"name": "MyClass"},
					},
				},
			}),
			"Error while unmarshalling destination configuration "+
				"map[avroSchemas:[map[schemaId:schema001] map[schema:map[name:MyClass]]] "+
				"convertToAvro:true hostname:some-hostname port:1234 topic:some-topic], "+
				"got error: "),
		)
		t.Run("invalid ssh config", func(t *testing.T) {
			t.Run("missing ssh host", buildTest(
				withRequired(map[string]any{
					"useSSH": true,
				}),
				"invalid configuration: ssh host cannot be empty",
			))
			t.Run("missing ssh port", buildTest(
				withRequired(map[string]any{
					"useSSH":  true,
					"sshHost": "random-host",
					"sshUser": "johnDoe",
				}),
				"invalid configuration: invalid ssh port",
			))
			t.Run("invalid ssh port", buildTest(
				withRequired(map[string]any{
					"useSSH":  true,
					"sshHost": "random-host",
					"sshUser": "johnDoe",
					"sshPort": "65536",
				}),
				"invalid configuration: invalid ssh port",
			))
			t.Run("missing ssh user", buildTest(
				withRequired(map[string]any{
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

		destConfig := map[string]any{
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
		destConfig := map[string]any{
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
	ctrl := gomock.NewController(t)
	kafkaStats.creationTime = getMockedTimer(t, ctrl, true)
	kafkaStats.produceTime = getMockedTimer(t, ctrl, true)
	kafkaStats.prepareBatchTime = getMockedTimer(t, ctrl, true)
	kafkaStats.publishTime = getMockedTimer(t, ctrl, true)

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	kafkaContainer, err := dockerKafka.Setup(pool, &testCleanup{t}, dockerKafka.WithBrokers(1))
	require.NoError(t, err)
	kafkaHost, kafkaPort, err := net.SplitHostPort(kafkaContainer.Brokers[0])
	require.NoError(t, err)
	destConfig := map[string]any{
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
	for _, message := range []string{"one", "two", "three"} {
		require.Eventually(t, func() bool {
			statusCode, returnMessage, errMessage = p.Produce(json.RawMessage(`{"message":"`+message+`","topic":"foo-bar","userId":"1234"}`), destConfig)
			return statusCode == http.StatusOK
		}, 30*time.Second, 100*time.Millisecond)
		require.Equal(t, "Message delivered to topic: foo-bar", returnMessage)
		require.Equal(t, "Message delivered to topic: foo-bar", errMessage)
	}

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
}

func TestAIOKafka(t *testing.T) {
	config.Default.Set("Router.KAFKA.compression", int(client.CompressionZstd))
	t.Cleanup(func() {
		config.Default.Set("Router.KAFKA.compression", int(client.CompressionNone))
	})
	ctrl := gomock.NewController(t)
	kafkaStats.creationTime = getMockedTimer(t, ctrl, true)
	kafkaStats.produceTime = getMockedTimer(t, ctrl, true)
	kafkaStats.prepareBatchTime = getMockedTimer(t, ctrl, true)
	kafkaStats.publishTime = getMockedTimer(t, ctrl, true)

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

	destConfig := map[string]any{
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
	for _, message := range []string{"one", "two", "three"} {
		require.Eventually(t, func() bool {
			statusCode, returnMessage, errMessage = p.Produce(json.RawMessage(`{"message":"`+message+`","topic":"foo-bar","userId":"1234"}`), destConfig)
			return statusCode == http.StatusOK
		}, 30*time.Second, 100*time.Millisecond)
		require.Equal(t, "Message delivered to topic: foo-bar", returnMessage)
		require.Equal(t, "Message delivered to topic: foo-bar", errMessage)
	}

	// TODO: Remove this workaround once dockertest releases a version that includes
	// https://github.com/ory/dockertest/pull/623 (merged but unreleased as of v3.12.0).
	// Without this, BuildImage sends requests with API version 1.25 (from struct field
	// tags), which Docker daemons 27+ reject (minimum supported API version is 1.44).
	serverVersion, err := pool.Client.Version()
	require.NoError(t, err)
	pool.Client, err = dc.NewVersionedClient(pool.Client.Endpoint(), serverVersion.Get("ApiVersion"))
	require.NoError(t, err)

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

			destConfig := map[string]any{
				"topic": "some-topic",
			}
			dest := backendconfig.DestinationT{Config: destConfig}

			p, err := NewProducerForAzureEventHubs(&dest, common.Opts{})
			require.Nil(t, p)
			require.ErrorContains(t, err, "invalid configuration: bootstrap server cannot be empty")
		})
		t.Run("missing connection string", func(t *testing.T) {
			kafkaStats.creationTimeAzureEventHubs = getMockedTimer(t, gomock.NewController(t), false)

			destConfig := map[string]any{
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

		destConfig := map[string]any{
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

			destConfig := map[string]any{
				"topic": "some-topic",
			}
			dest := backendconfig.DestinationT{Config: destConfig}

			p, err := NewProducerForConfluentCloud(&dest, common.Opts{})
			require.Nil(t, p)
			require.ErrorContains(t, err, "invalid configuration: bootstrap server cannot be empty")
		})
		t.Run("missing api key", func(t *testing.T) {
			kafkaStats.creationTimeConfluentCloud = getMockedTimer(t, gomock.NewController(t), false)

			destConfig := map[string]any{
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

			destConfig := map[string]any{
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

		destConfig := map[string]any{
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
		require.Contains(t, res, "unsupported type")
		require.Contains(t, err, "unsupported type")
	})

	t.Run("empty destination configuration", func(t *testing.T) {
		kafkaStats.produceTime = getMockedTimer(t, gomock.NewController(t), false)

		pm := ProducerManager{p: &client.Producer{}}
		destConfig := map[string]any{"foo": "bar"}
		sc, res, err := pm.Produce(json.RawMessage(""), destConfig)
		require.Equal(t, 400, sc)
		require.Equal(t, "invalid destination configuration: no topic error occurred.", res)
		require.Equal(t, "invalid destination configuration: no topic", err)
	})

	t.Run("invalid message", func(t *testing.T) {
		kafkaStats.produceTime = getMockedTimer(t, gomock.NewController(t), false)

		pm := ProducerManager{p: &client.Producer{}}
		destConfig := map[string]any{"topic": "foo-bar"}
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
		destConfig := map[string]any{"topic": "foo-bar"}
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
		destConfig := map[string]any{"topic": "foo-bar"}
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
		destConfig := map[string]any{"topic": "foo-bar"}
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
		destConfig := map[string]any{"topic": "foo-bar"}
		sc, res, err := pm.Produce(json.RawMessage(`{"message":"ciao","topic": "foo-bar"}`), destConfig)
		require.Equal(t, 200, sc)
		require.Equal(t, "Message delivered to topic: foo-bar", res)
		require.Equal(t, "Message delivered to topic: foo-bar", err)
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
		destConfig    = map[string]any{
			"topic":         topicName,
			"hostname":      kafkaHost,
			"port":          kafkaPort,
			"convertToAvro": true,
			"avroSchemas": []map[string]any{
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
					require.Equal(t, map[string]any{
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

		destConfigCopy := make(map[string]any)
		maps.Copy(destConfigCopy, destConfig)
		destConfigCopy["embedAvroSchemaID"] = true
		destCopy := dest
		destCopy.Config = destConfigCopy

		// Produce message in Avro format with schema 2
		t.Log("Creating Kafka producer")
		p, err := NewProducer(&destCopy, common.Opts{})
		require.NoError(t, err)
		require.NotNil(t, p)

		statusCode, returnMsg, errMsg := p.Produce(rawMessage, &destConfigCopy)
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
	return schema, schemaID
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

func (*nopLogger) Error(...any)          {}
func (*nopLogger) Infof(string, ...any)  {}
func (*nopLogger) Errorf(string, ...any) {}

type testCleanup struct{ *testing.T }

func (t *testCleanup) Defer(fn func() error) {
	t.Cleanup(func() {
		if err := fn(); err != nil {
			t.Log(err)
		}
	})
}
