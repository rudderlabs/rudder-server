// This files implements integration tests for the rudder-server.
// The code is responsible to run all dependencies using docker containers.
// It then runs the service ensuring it is configured to use the dependencies.
// Finally, it sends events and observe the destinations expecting to get the events back.

package docker_test

import (
	"context"
	"database/sql"
	b64 "encoding/base64"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"

	redigo "github.com/gomodule/redigo/redis"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource"
	"github.com/rudderlabs/rudder-go-kit/testhelper/rand"
	"github.com/rudderlabs/rudder-server/runner"
	kafkaClient "github.com/rudderlabs/rudder-server/services/streammanager/kafka/client"
	"github.com/rudderlabs/rudder-server/services/streammanager/kafka/client/testutil"
	"github.com/rudderlabs/rudder-server/testhelper/destination"
	"github.com/rudderlabs/rudder-server/testhelper/destination/kafka"
	"github.com/rudderlabs/rudder-server/testhelper/health"
	whUtil "github.com/rudderlabs/rudder-server/testhelper/webhook"
	"github.com/rudderlabs/rudder-server/testhelper/workspaceConfig"
	"github.com/rudderlabs/rudder-server/utils/httputil"
	"github.com/rudderlabs/rudder-server/utils/types/deployment"
)

var (
	hold                         = true
	db                           *sql.DB
	httpPort                     string
	webhookURL                   string
	disableDestinationWebhookURL string
	webhook                      *whUtil.Recorder
	disableDestinationWebhook    *whUtil.Recorder
	writeKey                     string
	workspaceID                  string
	kafkaContainer               *kafka.Resource
	redisContainer               *resource.RedisResource
	postgresContainer            *resource.PostgresResource
	transformerContainer         *destination.TransformerResource
	minioContainer               *resource.MinioResource
	EventID                      string
	VersionID                    string
)

type event struct {
	anonymousID       string
	userID            string
	count             string
	contextMyUniqueID string
	contextID         string
	contextIP         string
	propKey           string
	myUniqueID        string
	ip                string
}

func TestMainFlow(t *testing.T) {
	if os.Getenv("SLOW") == "0" {
		t.Skip("Skipping tests. Remove 'SLOW=0' env var to run them.")
	}

	hold = os.Getenv("HOLD") == "true"

	var tearDownStart time.Time
	defer func() {
		if tearDownStart == (time.Time{}) {
			t.Log("--- Teardown done (unexpected)")
		} else {
			t.Logf("--- Teardown done (%s)", time.Since(tearDownStart))
		}
	}()

	svcCtx, svcCancel := context.WithCancel(context.Background())
	svcDone := setupMainFlow(svcCtx, t)
	sendEventsToGateway(t)
	t.Run("webhook", func(t *testing.T) {
		require.Eventually(t, func() bool {
			return webhook.RequestsCount() == 10
		}, time.Minute, 300*time.Millisecond)

		i := -1
		require.Eventually(t, func() bool {
			i = i + 1
			req := webhook.Requests()[i]
			body, _ := io.ReadAll(req.Body)
			return gjson.GetBytes(body, "anonymousId").Str == "anonymousId_1"
		}, time.Minute, 100*time.Millisecond)

		req := webhook.Requests()[i]
		body, err := io.ReadAll(req.Body)

		require.NoError(t, err)
		require.Equal(t, "POST", req.Method)
		require.Equal(t, "/", req.URL.Path)
		require.Equal(t, "application/json", req.Header.Get("Content-Type"))
		require.Equal(t, "RudderLabs", req.Header.Get("User-Agent"))

		require.Equal(t, gjson.GetBytes(body, "anonymousId").Str, "anonymousId_1")
		require.Equal(t, gjson.GetBytes(body, "messageId").Str, "messageId_1")
		require.Equal(t, gjson.GetBytes(body, "eventOrderNo").Str, "1")
		require.Equal(t, gjson.GetBytes(body, "userId").Str, "identified_user_id")
		require.Equal(t, gjson.GetBytes(body, "rudderId").Str, "e4cab80e-2f0e-4fa2-87e0-3a4af182634c")
		require.Equal(t, gjson.GetBytes(body, "type").Str, "identify")
		// Verify User Transformation
		require.Equal(t, gjson.GetBytes(body, "myuniqueid").Str, "identified_user_idanonymousId_1")
		require.Equal(t, gjson.GetBytes(body, "context.myuniqueid").Str, "identified_user_idanonymousId_1")
		require.Equal(t, gjson.GetBytes(body, "context.id").Str, "0.0.0.0")
		require.Equal(t, gjson.GetBytes(body, "context.ip").Str, "0.0.0.0")

		// Verify Disabled destination doesn't receive any event.
		require.Equal(t, 0, len(disableDestinationWebhook.Requests()))
	})

	t.Run("postgres", func(t *testing.T) {
		var myEvent event
		require.Eventually(t, func() bool {
			eventSql := "select anonymous_id, user_id from dev_integration_test_1.identifies limit 1"
			_ = db.QueryRow(eventSql).Scan(&myEvent.anonymousID, &myEvent.userID)
			return myEvent.anonymousID == "anonymousId_1"
		}, time.Minute, 10*time.Millisecond)
		eventSql := "select count(*) from dev_integration_test_1.identifies"
		err := db.QueryRow(eventSql).Scan(&myEvent.count)
		require.NoError(t, err)
		require.Equal(t, myEvent.count, "2")

		// Verify User Transformation
		eventSql = "select context_myuniqueid,context_id,context_ip from dev_integration_test_1.identifies"
		err = db.QueryRow(eventSql).Scan(&myEvent.contextMyUniqueID, &myEvent.contextID, &myEvent.contextIP)
		require.NoError(t, err)
		require.Equal(t, myEvent.contextMyUniqueID, "identified_user_idanonymousId_1")
		require.Equal(t, myEvent.contextID, "0.0.0.0")
		require.Equal(t, myEvent.contextIP, "0.0.0.0")

		require.Eventually(t, func() bool {
			eventSql := "select anonymous_id, user_id from dev_integration_test_1.users limit 1"
			_ = db.QueryRow(eventSql).Scan(&myEvent.anonymousID, &myEvent.userID)
			return myEvent.anonymousID == "anonymousId_1"
		}, time.Minute, 10*time.Millisecond)

		require.Eventually(t, func() bool {
			eventSql = "select count(*) from dev_integration_test_1.users"
			_ = db.QueryRow(eventSql).Scan(&myEvent.count)
			return myEvent.count == "1"
		}, time.Minute, 10*time.Millisecond)

		// Verify User Transformation
		eventSql = "select context_myuniqueid,context_id,context_ip from dev_integration_test_1.users "
		err = db.QueryRow(eventSql).Scan(&myEvent.contextMyUniqueID, &myEvent.contextID, &myEvent.contextIP)
		require.NoError(t, err)
		require.Equal(t, myEvent.contextMyUniqueID, "identified_user_idanonymousId_1")
		require.Equal(t, myEvent.contextID, "0.0.0.0")
		require.Equal(t, myEvent.contextIP, "0.0.0.0")

		require.Eventually(t, func() bool {
			eventSql := "select anonymous_id, user_id from dev_integration_test_1.screens limit 1"
			err = db.QueryRow(eventSql).Scan(&myEvent.anonymousID, &myEvent.userID)
			require.NoError(t, err)
			return myEvent.anonymousID == "anonymousId_1"
		}, time.Minute, 10*time.Millisecond)
		require.Eventually(t, func() bool {
			eventSql = "select count(*) from dev_integration_test_1.screens"
			err = db.QueryRow(eventSql).Scan(&myEvent.count)
			require.NoError(t, err)
			return myEvent.count == "1"
		}, time.Minute, 10*time.Millisecond)

		// Verify User Transformation
		require.Eventually(t, func() bool {
			eventSql = "select prop_key,myuniqueid,ip from dev_integration_test_1.screens;"
			err = db.QueryRow(eventSql).Scan(&myEvent.propKey, &myEvent.myUniqueID, &myEvent.ip)
			require.NoError(t, err)
			return myEvent.myUniqueID == "identified_user_idanonymousId_1"
		}, time.Minute, 10*time.Millisecond)

		require.Equal(t, myEvent.propKey, "prop_value_edited")
		require.Equal(t, myEvent.ip, "0.0.0.0")
	})

	t.Run("redis", func(t *testing.T) {
		conn, err := redigo.Dial("tcp", redisContainer.Addr)
		require.NoError(t, err)
		defer func() { _ = conn.Close() }()
		require.Eventually(t, func() bool {
			// Similarly, get the trait1 and convert it to a string.
			event, _ := redigo.String(conn.Do("HGET", "user:identified_user_id", "trait1"))
			return event == "new-val"
		}, time.Minute, 10*time.Millisecond)
	})

	t.Run("kafka", func(t *testing.T) {
		kafkaHost := fmt.Sprintf("localhost:%s", kafkaContainer.Ports[0])

		// Create new consumer
		tc := testutil.New("tcp", kafkaHost)
		topics, err := tc.ListTopics(context.TODO())
		require.NoError(t, err)

		c, err := kafkaClient.New("tcp", []string{kafkaHost, kafkaHost}, kafkaClient.Config{})
		require.NoError(t, err)

		messages, errors := consume(t, c, topics)

		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Interrupt, syscall.SIGTERM) // Get signal for finish

		var (
			msgCount      = 0 // Count how many message processed
			expectedCount = 10
			timeout       = time.After(2 * time.Minute)
		)

	out:
		for {
			select {
			case msg := <-messages:
				msgCount++
				require.Equal(t, "identified_user_id", string(msg.Key))
				require.Contains(t, string(msg.Value), "identified_user_id")

				if msgCount == expectedCount {
					break out
				}
			case consumerError := <-errors:
				msgCount++
				t.Logf("Received consumerError: %v", consumerError)
			case <-timeout:
				t.Fatalf("Timeout waiting on Kafka messages, got %d messages instead of %d", msgCount, expectedCount)
			}
		}

		t.Log("Processed", msgCount, "messages")
	})

	t.Run("beacon-batch", func(t *testing.T) {
		payload := strings.NewReader(`{
			"batch":[
				{
				   "userId": "identified_user_id",
				   "anonymousId":"anonymousId_1",
				   "messageId":"messageId_1"
				}
			]
		}`)
		sendEvent(t, payload, "beacon/v1/batch", writeKey)
	})

	blockOnHold(t)
	svcCancel()
	t.Log("Waiting for service to stop")
	<-svcDone

	tearDownStart = time.Now()
}

func setupMainFlow(svcCtx context.Context, t *testing.T) <-chan struct{} {
	setupStart := time.Now()
	if testing.Verbose() {
		t.Setenv("LOG_LEVEL", "DEBUG")
	}

	config.Reset()
	logger.Reset()

	// uses a sensible default on windows (tcp/http) and linux/osx (socket)
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	containersGroup, containersCtx := errgroup.WithContext(context.TODO())
	containersGroup.Go(func() (err error) {
		kafkaContainer, err = kafka.Setup(pool, t,
			kafka.WithLogger(&testLogger{logger.NewLogger().Child("kafka")}),
			kafka.WithBrokers(1),
		)
		if err != nil {
			return err
		}
		kafkaCtx, kafkaCancel := context.WithTimeout(containersCtx, 3*time.Minute)
		defer kafkaCancel()
		return waitForKafka(kafkaCtx, t, kafkaContainer.Ports[0])
	})
	containersGroup.Go(func() (err error) {
		redisContainer, err = resource.SetupRedis(containersCtx, pool, t)
		return err
	})
	containersGroup.Go(func() (err error) {
		postgresContainer, err = resource.SetupPostgres(pool, t)
		if err != nil {
			return err
		}
		db = postgresContainer.DB
		return nil
	})
	containersGroup.Go(func() (err error) {
		transformerContainer, err = destination.SetupTransformer(pool, t)
		return err
	})
	containersGroup.Go(func() (err error) {
		minioContainer, err = resource.SetupMinio(pool, t)
		return err
	})
	require.NoError(t, containersGroup.Wait())

	if err := godotenv.Load("../../testhelper/.env"); err != nil {
		t.Log("INFO: No .env file found.")
	}

	t.Setenv("JOBS_DB_PORT", postgresContainer.Port)
	t.Setenv("WAREHOUSE_JOBS_DB_PORT", postgresContainer.Port)
	t.Setenv("DEST_TRANSFORM_URL", transformerContainer.TransformURL)
	t.Setenv("DEPLOYMENT_TYPE", string(deployment.DedicatedType))

	httpPortInt, err := kithelper.GetFreePort()
	require.NoError(t, err)

	httpPort = strconv.Itoa(httpPortInt)
	t.Setenv("RSERVER_GATEWAY_WEB_PORT", httpPort)
	httpAdminPort, err := kithelper.GetFreePort()
	require.NoError(t, err)

	t.Setenv("RSERVER_GATEWAY_ADMIN_WEB_PORT", strconv.Itoa(httpAdminPort))
	t.Setenv("RSERVER_ENABLE_STATS", "false")

	webhook = whUtil.NewRecorder()
	t.Cleanup(webhook.Close)
	webhookURL = webhook.Server.URL

	disableDestinationWebhook = whUtil.NewRecorder()
	t.Cleanup(disableDestinationWebhook.Close)
	disableDestinationWebhookURL = disableDestinationWebhook.Server.URL

	writeKey = rand.String(27)
	workspaceID = rand.String(27)
	mapWorkspaceConfig := map[string]any{
		"webhookUrl":                   webhookURL,
		"disableDestinationwebhookUrl": disableDestinationWebhookURL,
		"writeKey":                     writeKey,
		"workspaceId":                  workspaceID,
		"postgresPort":                 postgresContainer.Port,
		"address":                      redisContainer.Addr,
		"minioEndpoint":                minioContainer.Endpoint,
		"minioBucketName":              minioContainer.BucketName,
	}
	mapWorkspaceConfig["kafkaPort"] = kafkaContainer.Ports[0]
	workspaceConfigPath := workspaceConfig.CreateTempFile(t,
		"testdata/workspaceConfigTemplate.json",
		mapWorkspaceConfig,
	)
	if testing.Verbose() {
		data, err := os.ReadFile(workspaceConfigPath)
		require.NoError(t, err)
		t.Logf("Workspace config: %s", string(data))
	}

	t.Log("workspace config path:", workspaceConfigPath)
	t.Setenv("RSERVER_BACKEND_CONFIG_CONFIG_JSONPATH", workspaceConfigPath)

	t.Setenv("RUDDER_TMPDIR", t.TempDir())

	t.Logf("--- Setup done (%s)", time.Since(setupStart))

	svcDone := make(chan struct{})
	go func() {
		r := runner.New(runner.ReleaseInfo{EnterpriseToken: os.Getenv("ENTERPRISE_TOKEN")})
		_ = r.Run(svcCtx, []string{"docker-test-rudder-server"})
		close(svcDone)
	}()

	serviceHealthEndpoint := fmt.Sprintf("http://localhost:%s/health", httpPort)
	t.Log("serviceHealthEndpoint", serviceHealthEndpoint)
	health.WaitUntilReady(
		context.Background(), t,
		serviceHealthEndpoint,
		time.Minute,
		time.Second,
		"serviceHealthEndpoint",
	)

	return svcDone
}

func sendEventsToGateway(t *testing.T) {
	require.Empty(t, webhook.Requests(), "webhook should have no request before sending the event")
	payload1 := strings.NewReader(`{
		"userId": "identified_user_id",
		"anonymousId":"anonymousId_1",
		"messageId":"messageId_1",
		"type": "identify",
		"eventOrderNo":"1",
		"context": {
		  "traits": {
			 "trait1": "new-val"
		  },
		  "ip": "14.5.67.21",
		  "library": {
			  "name": "http"
		  }
		},
		"timestamp": "2020-02-02T00:23:09.544Z"
	}`)
	sendEvent(t, payload1, "identify", writeKey)
	payload2 := strings.NewReader(`{
		"userId": "identified_user_id",
		"anonymousId":"anonymousId_1",
		"messageId":"messageId_1",
		"type": "identify",
		"eventOrderNo":"2",
		"context": {
		  "traits": {
			 "trait1": "new-val"
		  },
		  "ip": "14.5.67.21",
		  "library": {
			  "name": "http"
		  }
		},
		"timestamp": "2020-02-02T00:23:09.544Z"
	}`)
	sendEvent(t, payload2, "identify", writeKey) // sending duplicate event to check dedup

	// Sending Batch event
	payloadBatch := strings.NewReader(`{
		"batch":
		[
			{
				"userId": "identified_user_id",
				"anonymousId": "anonymousId_1",
				"type": "identify",
				"context":
				{
					"traits":
					{
						"trait1": "new-val"
					},
					"ip": "14.5.67.21",
					"library":
					{
						"name": "http"
					}
				},
				"timestamp": "2020-02-02T00:23:09.544Z"
			}
		]
	}`)
	sendEvent(t, payloadBatch, "batch", writeKey)

	// Sending track event
	payloadTrack := strings.NewReader(`{
		"userId": "identified_user_id",
		"anonymousId":"anonymousId_1",
		"messageId":"messageId_1",
		"type": "track",
		"event": "Product Reviewed",
		"properties": {
		  "review_id": "12345",
		  "product_id" : "123",
		  "rating" : 3.0,
		  "review_body" : "Average product, expected much more."
		}
	}`)
	sendEvent(t, payloadTrack, "track", writeKey)

	// Sending page event
	payloadPage := strings.NewReader(`{
		"userId": "identified_user_id",
		"anonymousId":"anonymousId_1",
		"messageId":"messageId_1",
		"type": "page",
		"name": "Home",
		"properties": {
		  "title": "Home | RudderStack",
		  "url": "http://www.rudderstack.com"
		}
	}`)
	sendEvent(t, payloadPage, "page", writeKey)

	// Sending screen event
	payloadScreen := strings.NewReader(`{
		"userId": "identified_user_id",
		"anonymousId":"anonymousId_1",
		"messageId":"messageId_1",
		"type": "screen",
		"name": "Main",
		"properties": {
		  "prop_key": "prop_value"
		}
	}`)
	sendEvent(t, payloadScreen, "screen", writeKey)

	// Sending alias event
	payloadAlias := strings.NewReader(`{
		"userId": "identified_user_id",
		"anonymousId":"anonymousId_1",
		"messageId":"messageId_1",
		"type": "alias",
		"previousId": "name@surname.com",
		"userId": "12345"
	}`)
	sendEvent(t, payloadAlias, "alias", writeKey)

	// Sending group event
	payloadGroup := strings.NewReader(`{
		"userId": "identified_user_id",
		"anonymousId":"anonymousId_1",
		"messageId":"messageId_1",
		"type": "group",
		"groupId": "12345",
		"traits": {
		  "name": "MyGroup",
		  "industry": "IT",
		  "employees": 450,
		  "plan": "basic"
		}
	}`)
	sendEvent(t, payloadGroup, "group", writeKey)
	sendPixelEvents(t, writeKey)
}

func blockOnHold(t *testing.T) {
	t.Helper()
	if !hold {
		return
	}

	t.Log("Test on hold, before cleanup")
	t.Log("Press Ctrl+C to exit")

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	<-c
}

func getEvent(url, method string) (string, error) {
	httpClient := &http.Client{}
	req, err := http.NewRequest(method, url, http.NoBody)
	if err != nil {
		return "", err
	}
	req.Header.Add("Authorization", "Basic cnVkZGVyOnBhc3N3b3Jk")
	res, err := httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer func() { httputil.CloseResponse(res) }()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return "", err
	}
	return string(body), err
}

func sendPixelEvents(t *testing.T, writeKey string) {
	t.Helper()

	// Send pixel/v1/page
	url := fmt.Sprintf(
		"http://localhost:%s/pixel/v1/page?writeKey=%s&anonymousId=identified_user_id", httpPort, writeKey,
	)
	method := "GET"
	resBody, err := getEvent(url, method)
	if err != nil {
		t.Logf("sendPixelEvents error: %v", err)
		t.Logf("sendPixelEvents body: %s", resBody)
		return

	}
	// Send pixel/v1/track
	t.Log("Sending pixel/v1/track Event")
	url = fmt.Sprintf(
		"http://localhost:%s/pixel/v1/track?writeKey=%s&anonymousId=identified_user_id&event=product_reviewed_again",
		httpPort, writeKey,
	)
	method = "GET"
	resBody, err = getEvent(url, method)
	if err != nil {
		t.Logf("sendPixelEvents error: %v", err)
		t.Logf("sendPixelEvents body: %s", resBody)
	}
}

func sendEvent(t *testing.T, payload *strings.Reader, callType, writeKey string) {
	t.Helper()
	t.Logf("Sending %s Event", callType)

	var (
		httpClient = &http.Client{}
		method     = "POST"
		url        = fmt.Sprintf("http://localhost:%s/v1/%s", httpPort, callType)
	)

	req, err := http.NewRequest(method, url, payload)
	if err != nil {
		t.Logf("sendEvent error: %v", err)
		return
	}

	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Authorization", fmt.Sprintf("Basic %s", b64.StdEncoding.EncodeToString(
		[]byte(fmt.Sprintf("%s:", writeKey)),
	)))

	res, err := httpClient.Do(req)
	if err != nil {
		t.Logf("sendEvent error: %v", err)
		return
	}
	defer func() { httputil.CloseResponse(res) }()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		t.Logf("sendEvent error: %v", err)
		return
	}
	if res.Status != "200 OK" {
		return
	}

	t.Logf("Event Sent Successfully: (%s)", body)
}

func consume(t *testing.T, client *kafkaClient.Client, topics []testutil.TopicPartition) (<-chan kafkaClient.Message, <-chan error) {
	t.Helper()
	errors := make(chan error)
	messages := make(chan kafkaClient.Message)

	for _, topic := range topics {
		consumer := client.NewConsumer(topic.Topic, kafkaClient.ConsumerConfig{
			Partition:   topic.Partition,
			StartOffset: kafkaClient.FirstOffset,
		})

		t.Logf("Start consuming topic %s:%d", topic.Topic, topic.Partition)
		go func(consumer *kafkaClient.Consumer) {
			for {
				msg, err := consumer.Receive(context.TODO())
				if err != nil {
					errors <- err
				} else {
					messages <- msg
				}
			}
		}(consumer)
	}

	return messages, errors
}

func waitForKafka(ctx context.Context, t *testing.T, port string) error {
	kafkaHost := "localhost:" + port
	ticker := time.NewTicker(250 * time.Millisecond)
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("kafka not ready within context: %v", ctx.Err())
		case <-ticker.C:
			kc, err := kafkaClient.New("tcp", []string{kafkaHost}, kafkaClient.Config{})
			if err != nil {
				t.Log(fmt.Errorf("could not create Kafka client: %v", err))
				continue
			}
			if err := kc.Ping(ctx); err != nil {
				t.Log(fmt.Errorf("could not ping Kafka: %v", err))
				continue
			}
			tc := testutil.New("tcp", kafkaHost)
			if err := tc.CreateTopic(ctx, "dumb-topic", 1, 1); err != nil {
				t.Log(fmt.Errorf("could not create Kafka topic (dumb-topic): %v", err))
				continue
			}
			if topics, err := tc.ListTopics(ctx); err != nil {
				t.Log(fmt.Errorf("could not list Kafka topics: %v", err))
				continue
			} else if len(topics) == 0 {
				t.Log(fmt.Errorf("kafka topic was not created (dumb-topic missing)"))
				continue
			}
			t.Log("Kafka is ready!")
			return nil
		}
	}
}

type testLogger struct{ logger.Logger }

func (t *testLogger) Log(args ...interface{}) { t.Info(args...) }
