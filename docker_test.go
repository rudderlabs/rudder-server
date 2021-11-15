// This files implements integration tests for the rudder-server.
// The code is responsible to run all dependencies using docker containers.
// It then runs the service ensuring it is configurated to use the dependencies.
// Finally, it sends events and observe the destinations expecting to get the events back.

package main_test

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	b64 "encoding/base64"
	_ "encoding/json"
	"flag"
	"fmt"
	"html/template"
	"io"
	"log"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/go-redis/redis"
	redigo "github.com/gomodule/redigo/redis"
	_ "github.com/lib/pq"
	"github.com/minio/minio-go"
	"github.com/ory/dockertest"
	dc "github.com/ory/dockertest/docker"

	"github.com/phayes/freeport"
	main "github.com/rudderlabs/rudder-server"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

var (
	pool             *dockertest.Pool
	err              error
	z                *dockertest.Resource
	resourceKafka    *dockertest.Resource
	resourceRedis	 *dockertest.Resource
	transformerRes   *dockertest.Resource
	resource         *dockertest.Resource
	network          *dc.Network
	resourcePostgres *dockertest.Resource
	transformURL     string
	minioEndpoint    string
	minioBucketName  string
	hold             bool = true
	db               *sql.DB
	redisClient      *redis.Client
	DB_DSN           = "root@tcp(127.0.0.1:3306)/service"
	httpPort         string
	httpKafkaPort    string
	dbHandle         *sql.DB
	sourceJSON       backendconfig.ConfigT
	webhookurl       string
	webhookDestinationurl string
	webhook          *WebhookRecorder
	webhookDestination *WebhookRecorder
	address          string
	runIntegration   bool
	writeKey         string
	webhookEventWriteKey string
	workspaceID      string
	redisAddress     string
	brokerPort       string
	localhostPort    string
	localhostPortInt int
)

type WebhookRecorder struct {
	Server       *httptest.Server
	requestsMu   sync.RWMutex
	requestDumps [][]byte
}

type User struct {
	trait1 string `redis:"name"`
}

func NewWebhook() *WebhookRecorder {
	whr := WebhookRecorder{}
	whr.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		dump, err := httputil.DumpRequest(r, true)
		if err != nil {
			http.Error(w, fmt.Sprint(err), http.StatusInternalServerError)
			return
		}
		whr.requestsMu.Lock()
		whr.requestDumps = append(whr.requestDumps, dump)
		whr.requestsMu.Unlock()

		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	}))

	return &whr
}

func (whr *WebhookRecorder) Requests() []*http.Request {
	whr.requestsMu.RLock()
	defer whr.requestsMu.RUnlock()

	requests := make([]*http.Request, len(whr.requestDumps))
	for i, d := range whr.requestDumps {
		requests[i], _ = http.ReadRequest(bufio.NewReader(bytes.NewReader(d)))
	}
	return requests
}

func (whr *WebhookRecorder) Close() {
	whr.Server.Close()
}
func randString(n int) string {
	var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

	s := make([]rune, n)
	for i := range s {
		s[i] = letters[rand.Intn(len(letters))]
	}
	return string(s)
}

type Event struct {
	anonymous_id string
	user_id      string
	count        string
}

type Author struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
}

func getWorkspaceConfig() backendconfig.ConfigT {
	backendConfig := new(backendconfig.WorkspaceConfig)
	sourceJSON, _ := backendConfig.Get()
	return sourceJSON
}

func createWorkspaceConfig(templatePath string, values map[string]string) string {
	t, err := template.ParseFiles(templatePath)
	if err != nil {
		panic(err)
	}

	f, err := os.CreateTemp("", "workspaceConfig.*.json")
	if err != nil {
		panic(err)
	}

	err = t.Execute(f, values)
	if err != nil {
		panic(err)
	}

	f.Close()

	return f.Name()
}

func waitUntilReady(ctx context.Context, endpoint string, atMost, interval time.Duration) {
	probe := time.NewTicker(interval)
	timeout := time.After(atMost)
	for {
		select {
		case <-ctx.Done():
			return
		case <-timeout:
			log.Panicf("application was not ready after %s\n", atMost)
		case <-probe.C:
			resp, err := http.Get(endpoint)
			if err != nil {
				continue
			}
			if resp.StatusCode == http.StatusOK {
				log.Println("application ready")
				return
			}
		}
	}
}

func blockOnHold() {
	if !hold {
		return
	}

	log.Println("Test on hold, before cleanup")
	log.Println("Press Ctrl+C to exit")

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	<-c
}

func SendEvent(payload *strings.Reader, call_type string) (string, error){
	log.Println(fmt.Sprintf("Sending %s Event", call_type))
	url := fmt.Sprintf("http://localhost:%s/v1/%s", httpPort, call_type)
	method := "POST"
	client := &http.Client{}
	req, err := http.NewRequest(method, url, payload)

	if err != nil {
		log.Println(err)
		return "" , err

	}

	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Authorization",
		fmt.Sprintf("Basic %s", b64.StdEncoding.EncodeToString(
			[]byte(fmt.Sprintf("%s:", writeKey)),
		)),
	)

	res, err := client.Do(req)
	if err != nil {
		log.Println(err)
		return  "" , err
	}
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		log.Println(err)
		return "" , err
	}
	log.Println(string(body))
	log.Println("Event Sent Successfully")
	return string(body) ,err
}

func SendWebhookEvent() {
	log.Println("Sending Webhook Event")
	url := fmt.Sprintf("http://localhost:%s/v1/webhook?writeKey=%s", httpPort, webhookEventWriteKey)
	method := "POST"
  
	payload := strings.NewReader(`{
	"data": {
	  "customer_id": "abcd-1234"  
	},
	"object_type": "email"
 	 }`)
  
	client := &http.Client {
	}
	req, err := http.NewRequest(method, url, payload)
  
	if err != nil {
	  log.Println(err)
	  return
	}
	req.Header.Add("Content-Type", "application/json")
  
	res, err := client.Do(req)
	if err != nil {
	  log.Println(err)
	  return
	}
	defer res.Body.Close()
  
	body, err := io.ReadAll(res.Body)
	if err != nil {
	  log.Println(err)
	  return
	}
	log.Println(string(body))
	log.Println("Webhook Event Sent Successfully")
}

func TestMain(m *testing.M) {
	flag.BoolVar(&hold, "hold", false, "hold environment clean-up after test execution until Ctrl+C is provided")
	flag.BoolVar(&runIntegration, "integration", false, "run integration level tests")
	flag.Parse()

	if !runIntegration {
		log.Println("Skipping integration test. Use `-integration` to run them.")
		return
	}

	// hack to make defer work, without being affected by the os.Exit in TestMain
	exitCode, err := run(m)
	if err != nil {
		log.Fatal(err)
	}
	os.Exit(exitCode)
}

func run(m *testing.M) (int, error) {
	setupStart := time.Now()

	var tearDownStart time.Time
	defer func() {
		if tearDownStart == (time.Time{}) {
			fmt.Printf("--- Teardown done (unexpected)\n")
		} else {
			fmt.Printf("--- Teardown done (%s)\n", time.Since(tearDownStart))
		}
	}()
	// uses a sensible default on windows (tcp/http) and linux/osx (socket)
	pool, err = dockertest.NewPool("")
	if err != nil {
		return 0, fmt.Errorf("could not connect to docker: %w", err)
	}
	SetZookeeper()
	defer func() {
		if err := pool.Purge(z); err != nil {
			log.Printf("Could not purge resource: %s \n", err)
		}
	}()
	SetKafka()
	defer func() {
		if err := pool.Purge(resourceKafka); err != nil {
			log.Printf("Could not purge resource: %s \n", err)
		}
	}()
	SetRedis()
	defer func() {
		if err := pool.Purge(resourceRedis); err != nil {
			log.Printf("Could not purge resource: %s \n", err)
		}
	}()
	SetJobsDB()
	defer func() {
	if err := pool.Purge(resourcePostgres); err != nil {
		log.Printf("Could not purge resource: %s \n", err)
	}
	}()
	SetTransformer()
	defer func() {
	if err := pool.Purge(transformerRes); err != nil {
		log.Printf("Could not purge resource: %s \n", err)
	}
	}()
	SetMINIO()
	defer func() {
	if err := pool.Purge(resource); err != nil {
		log.Printf("Could not purge resource: %s \n", err)
	}
	}()

	os.Setenv("JOBS_DB_HOST", "localhost")
	os.Setenv("JOBS_DB_NAME", "jobsdb")
	os.Setenv("JOBS_DB_DB_NAME", "jobsdb")
	os.Setenv("JOBS_DB_USER", "rudder")
	os.Setenv("JOBS_DB_PASSWORD", "password")
	os.Setenv("JOBS_DB_PORT", resourcePostgres.GetPort("5432/tcp"))
	os.Setenv("JOBS_DB_SSL_MODE", "disable")
	os.Setenv("WAREHOUSE_JOBS_DB_HOST", "localhost")
	os.Setenv("WAREHOUSE_JOBS_DB_NAME", "jobsdb")
	os.Setenv("WAREHOUSE_JOBS_DB_DB_NAME", "jobsdb")
	os.Setenv("WAREHOUSE_JOBS_DB_USER", "rudder")
	os.Setenv("WAREHOUSE_JOBS_DB_PASSWORD", "password")
	os.Setenv("WAREHOUSE_JOBS_DB_PORT", resourcePostgres.GetPort("5432/tcp"))
	os.Setenv("DEST_TRANSFORM_URL", transformURL)
	os.Setenv("RUDDER_ADMIN_PASSWORD", "password")
	os.Setenv("RSERVER_BACKEND_CONFIG_CONFIG_FROM_FILE", "true")
	os.Setenv("WORKSPACE_TOKEN", "1vLbwltztKUgpuFxmJlSe1esX8c")
	os.Setenv("CONFIG_BACKEND_URL", "https://api.dev.rudderlabs.com")

	httpPortInt, err := freeport.GetFreePort()
	if err != nil {
		return 0, fmt.Errorf("Could not get free port for http: %w", err)
	}
	httpPort = strconv.Itoa(httpPortInt)
	os.Setenv("RSERVER_GATEWAY_WEB_PORT", httpPort)
	httpAdminPort, err := freeport.GetFreePort()
	if err != nil {
		return 0, fmt.Errorf("Could not get free port for http admin: %w", err)
	}
	os.Setenv("RSERVER_GATEWAY_ADMIN_WEB_PORT", strconv.Itoa(httpAdminPort))

	os.Setenv("RSERVER_ENABLE_STATS", "false")

	webhook = NewWebhook()
	defer webhook.Close()
	webhookurl = webhook.Server.URL
	log.Println("webhookurl", webhookurl)

	webhookDestination = NewWebhook()
	defer webhookDestination.Close()
	webhookDestinationurl = webhookDestination.Server.URL
	log.Println("webhookurl", webhookDestinationurl)

	writeKey = randString(27)
	workspaceID = randString(27)
	webhookEventWriteKey = randString(27)

	workspaceConfigPath := createWorkspaceConfig(
		"testdata/workspaceConfigTemplate.json",
		map[string]string{
			"webhookUrl":      webhookurl,
			"webhookDestinationurl":webhookDestinationurl,
			"writeKey":        writeKey,
			"webhookEventWriteKey": webhookEventWriteKey,
			"workspaceId":     workspaceID,
			"postgresPort":    resourcePostgres.GetPort("5432/tcp"),
			"address":         redisAddress,
			"minioEndpoint":   minioEndpoint,
			"minioBucketName": minioBucketName,
			"kafkaPort":       strconv.Itoa(localhostPortInt),
		},
	)
	defer func() {
		err := os.Remove(workspaceConfigPath)
		log.Println(err)
	}()
	log.Println("workspace config path:", workspaceConfigPath)
	os.Setenv("RSERVER_BACKEND_CONFIG_CONFIG_JSONPATH", workspaceConfigPath)

	rudderTmpDir, err := os.MkdirTemp("", "rudder_server_test")
	if err != nil {
		return 0, err
	}
	defer os.RemoveAll(rudderTmpDir)
	os.Setenv("RUDDER_TMPDIR", rudderTmpDir)
	os.Setenv("CONFIG_PATH", "./config/config.yaml")
	os.Setenv("TEST_SINK_URL", "http://localhost:8181")
	os.Setenv("GO_ENV", "production")
	os.Setenv("LOG_LEVEL", "INFO")
	os.Setenv("INSTANCE_ID", "1")
	os.Setenv("WAREHOUSE_STAGING_BUCKET_FOLDER_NAME", "rudder-warehouse-staging-logs")
	os.Setenv("WAREHOUSE_BUCKET_LOAD_OBJECTS_FOLDER_NAME", "rudder-warehouse-load-objects")
	os.Setenv("DESTINATION_BUCKET_FOLDER_NAME", "rudder-logs")
	os.Setenv("ALERT_PROVIDER", "pagerduty")
	os.Setenv("JOBS_BACKUP_STORAGE_PROVIDER", "MINIO")
	os.Setenv("JOBS_BACKUP_BUCKET", "devintegrationtest")
	os.Setenv("JOBS_BACKUP_PREFIX", "test")
	os.Setenv("MINIO_ENDPOINT", "localhost:9000")
	os.Setenv("MINIO_ACCESS_KEY_ID", "MYACCESSKEY")
	os.Setenv("MINIO_SECRET_ACCESS_KEY", "MYSECRETKEY")
	os.Setenv("MINIO_SSL", "false")
	os.Setenv("WAREHOUSE_URL", "http://localhost:8082")
	os.Setenv("CP_ROUTER_USE_TLS", "true")
	os.Setenv("RSERVER_WAREHOUSE_UPLOAD_FREQ_IN_S", "10s")

	fmt.Printf("--- Setup done (%s)\n", time.Since(setupStart))

	svcCtx, svcCancel := context.WithCancel(context.Background())
	svcDone := make(chan struct{})
	go func() {
		main.Run(svcCtx)
		close(svcDone)
	}()

	serviceHealthEndpoint := fmt.Sprintf("http://localhost:%s/health", httpPort)
	log.Println("serviceHealthEndpoint", serviceHealthEndpoint)
	waitUntilReady(
		context.Background(),
		serviceHealthEndpoint,
		time.Minute,
		time.Second,
	)

	code := m.Run()
	blockOnHold()

	svcCancel()
	fmt.Println("waiting for service to stop")
	<-svcDone

	tearDownStart = time.Now()
	
	return code, nil
}

func TestWebhook(t *testing.T) {
	var err error
	psqlInfo := jobsdb.GetConnectionString()
	dbHandle, err = sql.Open("postgres", psqlInfo)
	if err != nil {
		panic(err)
	}
	// Pulling config form workspaceConfig.json
	sourceJSON = getWorkspaceConfig()

	require.Empty(t, webhook.Requests(), "webhook should have no request before sending the event")
	payload_1 := strings.NewReader(`{
		"userId": "identified user id",
		"anonymousId":"anonymousId_1",
		"messageId":"messageId_1",
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
	SendEvent(payload_1, "identify")
	payload_2 := strings.NewReader(`{
		"userId": "identified user id",
		"anonymousId":"anonymousId_1",
		"messageId":"messageId_1",
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
	SendEvent(payload_2, "identify") //sending duplicate event to check dedup
	// Sending Batch event
	payload_Batch := strings.NewReader(`{
		"batch":
		[
			{
				"userId": "identified user id",
			   "anonymousId":"anonymousId_1",
			   "messageId":"messageId_1"
			}
		]
	}`)
	SendEvent(payload_Batch, "batch")

	// Sending track event
	payload_track := strings.NewReader(`{
		"userId": "identified user id",
		"anonymousId":"anonymousId_1",
		"messageId":"messageId_1"
	  }`)
	SendEvent(payload_track, "track")

	// Sending page event
	payload_page := strings.NewReader(`{
		"userId": "identified user id",
		"anonymousId":"anonymousId_1",
		"messageId":"messageId_1"
	  }`)
	SendEvent(payload_page, "page")

	// Sending screen event
	payload_screen := strings.NewReader(`{
		"userId": "identified user id",
		"anonymousId":"anonymousId_1",
		"messageId":"messageId_1"
	  }`)
	SendEvent(payload_screen, "screen")

	// Sending alias event
	payload_alias := strings.NewReader(`{
		"userId": "identified user id",
		"anonymousId":"anonymousId_1",
		"messageId":"messageId_1"
	}`)
	SendEvent(payload_alias, "alias")

	// Sending group event
	payload_group := strings.NewReader(`{
		"userId": "identified user id",
		"anonymousId":"anonymousId_1",
		"messageId":"messageId_1"
	}`)
	SendEvent(payload_group, "group")
	SendWebhookEvent()

	require.Eventually(t, func() bool {
		return len(webhook.Requests()) == 8
	}, time.Minute, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		return len(webhookDestination.Requests()) == 1
	}, time.Minute, 10*time.Millisecond)

	req := webhook.Requests()[0]
	body, err := io.ReadAll(req.Body)

	require.NoError(t, err)
	require.Equal(t, "POST", req.Method)
	require.Equal(t, "/", req.URL.Path)
	require.Equal(t, "application/json", req.Header.Get("Content-Type"))
	require.Equal(t, "RudderLabs", req.Header.Get("User-Agent"))

	require.Equal(t, gjson.GetBytes(body, "anonymousId").Str, "anonymousId_1")
	require.Equal(t, gjson.GetBytes(body, "messageId").Str, "messageId_1")
	require.Equal(t, gjson.GetBytes(body, "eventOrderNo").Str, "1")
	require.Equal(t, gjson.GetBytes(body, "userId").Str, "identified user id")
	require.Equal(t, gjson.GetBytes(body, "rudderId").Str, "bcba8f05-49ff-4953-a4ee-9228d2f89f31")
	require.Equal(t, gjson.GetBytes(body, "type").Str, "identify")


	req = webhookDestination.Requests()[0]
	body, err = io.ReadAll(req.Body)

	require.NoError(t, err)
	require.Equal(t, "POST", req.Method)
	require.Equal(t, "/", req.URL.Path)
	require.Equal(t, "application/json", req.Header.Get("Content-Type"))
	require.Equal(t, "RudderLabs", req.Header.Get("User-Agent"))

	require.Equal(t, gjson.GetBytes(body, "anonymousId").Str, "")
	require.NotEqual(t, gjson.GetBytes(body, "messageId").Str, "")
}

// Verify Event in POSTGRES
func TestPostgres(t *testing.T) {
	var myEvent Event
	require.Eventually(t, func() bool {
		eventSql := "select anonymous_id, user_id from dev_integration_test_1.identifies limit 1"
		db.QueryRow(eventSql).Scan(&myEvent.anonymous_id, &myEvent.user_id)
		return myEvent.anonymous_id == "anonymousId_1"
	}, time.Minute, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		eventSql := "select count(*) from dev_integration_test_1.aliases"
		db.QueryRow(eventSql).Scan(&myEvent.count)
		return myEvent.count == "1"
	}, time.Minute, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		eventSql := "select count(*) from dev_integration_test_1.groups"
		db.QueryRow(eventSql).Scan(&myEvent.count)
		return myEvent.count == "1"
	}, time.Minute, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		eventSql := "select count(*) from dev_integration_test_1.identifies"
		db.QueryRow(eventSql).Scan(&myEvent.count)
		return myEvent.count == "1"
	}, time.Minute, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		eventSql := "select count(*) from dev_integration_test_1.pages"
		db.QueryRow(eventSql).Scan(&myEvent.count)
		return myEvent.count == "1"
	}, time.Minute, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		eventSql := "select count(*) from dev_integration_test_1.screens"
		db.QueryRow(eventSql).Scan(&myEvent.count)
		return myEvent.count == "1"
	}, time.Minute, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		eventSql := "select count(*) from dev_integration_test_1.tracks"
		db.QueryRow(eventSql).Scan(&myEvent.count)
		return myEvent.count == "1"
	}, time.Minute, 10*time.Millisecond)
}

// Verify Audience List EndPoint 
func TestAudiencelist(t *testing.T) {
	payload := strings.NewReader(`{
		"type": "audiencelist",
		"properties": {
			"listData": {
			"add": [
				{
				"EMAIL": "xyz@rudderstack.com",
				"MM": "02"
				}
			],
			"remove": [
				{
				"EMAIL": "abc@rudderstack.com",
				"MM": "02"
				}
			]
			}
		},
		"userId": "user123"
		}`)
	resbody, _ :=SendEvent(payload, "audiencelist")
	require.Equal(t, resbody, "OK")
}

// Verify Event in Redis
func TestRedis(t *testing.T) {
	conn, err := redigo.Dial("tcp", redisAddress)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	require.Eventually(t, func() bool {
		// Similarly, get the trait1 and convert it to a string.
		event, _ := redigo.String(conn.Do("HGET", "user:identified user id", "trait1"))
		return event == "new-val"
	}, time.Minute, 10*time.Millisecond)

}
// Verify Event in Kafka
func TestKafka(t *testing.T) {

	config := sarama.NewConfig()
	config.ClientID = "go-kafka-consumer"
	config.Consumer.Return.Errors = true

	kafkaEndpoint := fmt.Sprintf("localhost:%s", strconv.Itoa(localhostPortInt))
	brokers := []string{kafkaEndpoint}

	// Create new consumer
	master, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		panic(err)
	}
	topics, _ := master.Topics()

	consumer, errors := consume(topics, master)
	defer func() {
		if err := master.Close(); err != nil {
			panic(err)
		}
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// Count how many message processed
	msgCount := 0
	// Get signnal for finish
	expectedCount := 8
out:
	for {
		select {
		case msg := <-consumer:
			msgCount++
			t.Log("Received messages", string(msg.Key), string(msg.Value))
			require.Equal(t, "identified user id", string(msg.Key))
			// require.Contains(t, string(msg.Value), "new-val")
			require.Contains(t, string(msg.Value), "identified user id")
			if msgCount == expectedCount {
				break out
			}
		case consumerError := <-errors:
			msgCount++
			log.Println("Received consumerError ", string(consumerError.Topic), string(consumerError.Partition), consumerError.Err)
			// Required
		case <-time.After(time.Minute):
			panic("timeout waiting on kafka message")
		}
	}
	log.Println("Processed", msgCount, "messages")

}
func consume(topics []string, master sarama.Consumer) (chan *sarama.ConsumerMessage, chan *sarama.ConsumerError) {
	consumers := make(chan *sarama.ConsumerMessage)
	errors := make(chan *sarama.ConsumerError)
	for _, topic := range topics {
		if strings.Contains(topic, "__consumer_offsets") {
			continue
		}
		partitions, _ := master.Partitions(topic)
		// this only consumes partition no 1, you would probably want to consume all partitions
		consumer, err := master.ConsumePartition(topic, partitions[0], sarama.OffsetOldest)
		if nil != err {
			panic(err)
		}
		log.Println(" Start consuming topic ", topic)
		go func(topic string, consumer sarama.PartitionConsumer) {
			for {
				select {
				case consumerError := <-consumer.Errors():
					errors <- consumerError
					log.Println("consumerError: ", consumerError.Err)

				case msg := <-consumer.Messages():
					consumers <- msg
					log.Println("Got message on topic ", topic, msg.Value)
				}
			}
		}(topic, consumer)
	}

	return consumers, errors
}

func SetZookeeper() {
	network, err = pool.Client.CreateNetwork(dc.CreateNetworkOptions{Name: "kafka_network"})
	if err != nil {
		log.Println("Could not create docker network: %s", err)
	}
	zookeeperPortInt, err := freeport.GetFreePort()
	if err != nil {
		log.Println(err)
	}
	zookeeperPort := fmt.Sprintf("%s/tcp", strconv.Itoa(zookeeperPortInt))
	zookeeperclientPort := fmt.Sprintf("ZOOKEEPER_CLIENT_PORT=%s", strconv.Itoa(zookeeperPortInt))
	log.Println("zookeeper Port:", zookeeperPort)
	log.Println("zookeeper client Port :", zookeeperclientPort)

	z, err = pool.RunWithOptions(&dockertest.RunOptions{
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
		log.Println(err)
	}
	
}

func SetKafka() {
	// Set Kafka: pulls an image, creates a container based on it and runs it
	KAFKA_ZOOKEEPER_CONNECT := fmt.Sprintf("KAFKA_ZOOKEEPER_CONNECT= zookeeper:%s", z.GetPort("2181/tcp"))
	log.Println("KAFKA_ZOOKEEPER_CONNECT:", KAFKA_ZOOKEEPER_CONNECT)

	brokerPortInt, err := freeport.GetFreePort()
	if err != nil {
		log.Println(err)
	}
	brokerPort = fmt.Sprintf("%s/tcp", strconv.Itoa(brokerPortInt))
	log.Println("broker Port:", brokerPort)

	localhostPortInt, err = freeport.GetFreePort()
	if err != nil {
		log.Println(err)
	}
	localhostPort = fmt.Sprintf("%s/tcp", strconv.Itoa(localhostPortInt))
	log.Println("localhost Port:", localhostPort)

	KAFKA_ADVERTISED_LISTENERS := fmt.Sprintf("KAFKA_ADVERTISED_LISTENERS=INTERNAL://broker:9090,EXTERNAL://localhost:%s", strconv.Itoa(localhostPortInt))
	KAFKA_LISTENERS := "KAFKA_LISTENERS=INTERNAL://broker:9090,EXTERNAL://:9092"

	log.Println("KAFKA_ADVERTISED_LISTENERS", KAFKA_ADVERTISED_LISTENERS)

	resourceKafka, err = pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "confluentinc/cp-kafka",
		Tag:        "latest",
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
		log.Println(err)
	}
	log.Println("Kafka PORT:- ", resourceKafka.GetPort("9092/tcp"))
}

func SetRedis() {
	// pulls an redis image, creates a container based on it and runs it
	resourceRedis, err = pool.Run("redis", "alpine3.14", []string{"requirepass=secret"})
	if err != nil {
		log.Println("Could not start resource: %s", err)
	}
	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	redisAddress = fmt.Sprintf("localhost:%s", resourceRedis.GetPort("6379/tcp"))
	if err := pool.Retry(func() error {
		redisClient = redis.NewClient(&redis.Options{
			Addr:     redisAddress,
			Password: "",
			DB:       0,
		})

		_, err := redisClient.Ping().Result()
		return err
	}); err != nil {
		log.Println("Could not connect to docker: %s", err)
	}
}

func SetJobsDB() {
	database := "jobsdb"
	// pulls an image, creates a container based on it and runs it
	resourcePostgres, err = pool.Run("postgres", "11-alpine", []string{
		"POSTGRES_PASSWORD=password",
		"POSTGRES_DB=" + database,
		"POSTGRES_USER=rudder",
	})
	if err != nil {
		log.Println("Could not start resource Postgres: %w", err)
	}
	DB_DSN = fmt.Sprintf("postgres://rudder:password@localhost:%s/%s?sslmode=disable", resourcePostgres.GetPort("5432/tcp"), database)
	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	if err := pool.Retry(func() error {
		var err error
		db, err = sql.Open("postgres", DB_DSN)
		if err != nil {
			return err
		}
		return db.Ping()
	}); err != nil {
		log.Println("Could not connect to postgres", DB_DSN, err)
	}
	fmt.Println("DB_DSN:", DB_DSN)
}

func SetTransformer() {
	// Set Rudder Transformer
	// pulls an image, creates a container based on it and runs it
	transformerRes, err = pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "rudderlabs/rudder-transformer",
		Tag:          "latest",
		ExposedPorts: []string{"9090"},
		Env: []string{
			"CONFIG_BACKEND_URL=https://api.dev.rudderlabs.com",
		},
	})
	if err != nil {
		log.Println("Could not start resource transformer: %w", err)
	}

	transformURL = fmt.Sprintf("http://localhost:%s", transformerRes.GetPort("9090/tcp"))
	waitUntilReady(
		context.Background(),
		fmt.Sprintf("%s/health", transformURL),
		time.Minute,
		time.Second,
	)
}

func SetMINIO() {
	minioPortInt, err := freeport.GetFreePort()
	if err != nil {
		log.Println(err)
	}
	minioPort := fmt.Sprintf("%s/tcp", strconv.Itoa(minioPortInt))
	log.Println("minioPort:", minioPort)
	// Setup MINIO
	var minioClient *minio.Client

	options := &dockertest.RunOptions{
		Repository: "minio/minio",
		Tag:        "latest",
		Cmd:        []string{"server", "/data"},
		PortBindings: map[dc.Port][]dc.PortBinding{
			"9000/tcp": []dc.PortBinding{{HostPort: strconv.Itoa(minioPortInt)}},
		},
		Env: []string{"MINIO_ACCESS_KEY=MYACCESSKEY", "MINIO_SECRET_KEY=MYSECRETKEY"},
	}

	resource, err = pool.RunWithOptions(options)
	if err != nil {
		log.Println("Could not start resource:", err)
	}

	minioEndpoint = fmt.Sprintf("localhost:%s", resource.GetPort("9000/tcp"))

	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	// the minio client does not do service discovery for you (i.e. it does not check if connection can be established), so we have to use the health check
	if err := pool.Retry(func() error {
		url := fmt.Sprintf("http://%s/minio/health/live", minioEndpoint)
		resp, err := http.Get(url)
		if err != nil {
			return err
		}
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("status code not OK")
		}
		return nil
	}); err != nil {
		log.Println("Could not connect to docker: %s", err)
	}
	// now we can instantiate minio client
	minioClient, err = minio.New(minioEndpoint, "MYACCESSKEY", "MYSECRETKEY", false)
	if err != nil {
		log.Println("Failed to create minio client:", err)
		panic(err)
	}
	log.Printf("%#v\n", minioClient) // minioClient is now set up

	// Create bucket for MINIO
	// Create a bucket at region 'us-east-1' with object locking enabled.
	minioBucketName = "devintegrationtest"
	err = minioClient.MakeBucket(minioBucketName, "us-east-1")
	if err != nil {
		log.Println(err)
		panic(err)
	}
}

// TODO: Verify in Live Events API
