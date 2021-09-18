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
	_ "encoding/json"
	"fmt"
	"html/template"
	"io/ioutil"
	"log"
	b64 "encoding/base64"
	"flag"
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
	"github.com/ory/dockertest"
	dc "github.com/ory/dockertest/docker"

	_ "github.com/lib/pq"
	"github.com/phayes/freeport"
	main "github.com/rudderlabs/rudder-server"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

var (
	hold        bool = true
	db          *sql.DB
	redisClient *redis.Client
	DB_DSN      = "root@tcp(127.0.0.1:3306)/service"
	httpPort    string
	httpKafkaPort string
	dbHandle    *sql.DB
	sourceJSON  backendconfig.ConfigT
	webhookurl  string
	webhook     *WebhookRecorder
	address 	string
	runIntegration bool
	writeKey       string
	workspaceID    string
)

type WebhookRecorder struct {
	Server *httptest.Server

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
	anonymous_id       string
	user_id    string
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

	f, err := ioutil.TempFile("", "workspaceConfig.*.json")
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

func initializeWarehouseConfig(src string, des string) map[string][]warehouseutils.WarehouseT {
	var warehouses = make(map[string][]warehouseutils.WarehouseT)
	for _, source := range sourceJSON.Sources {
		if source.Name == src {
			if len(source.Destinations) > 0 {
				for _, destination := range source.Destinations {
					if destination.Name == des {
						warehouses[destination.DestinationDefinition.Name] = append(warehouses[destination.DestinationDefinition.Name],
							warehouseutils.WarehouseT{Source: source, Destination: destination})
						return warehouses
					}
				}
			}
		}
	}
	return warehouses
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

	fmt.Println("Test on hold, before cleanup")
	fmt.Println("Press Ctrl+C to exit")

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	<-c
}

func CreateSCHEMAPostgres() {
	// TODO: Need to configure with workspace json
	_, err := db.Exec("CREATE SCHEMA example")
	if err != nil {
		panic(err)
	}
}

func SendEvent() {
	fmt.Println("Sending Track Event")
	url := fmt.Sprintf("http://localhost:%s/v1/identify", httpPort)
	method := "POST"

	payload := strings.NewReader(`{
	"userId": "identified user id",
	"anonymousId":"anon-id-new",
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

	client := &http.Client{}
	req, err := http.NewRequest(method, url, payload)

	if err != nil {
		fmt.Println(err)
		return

	}

	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Authorization",
		fmt.Sprintf("Basic %s", b64.StdEncoding.EncodeToString(
			[]byte(fmt.Sprintf("%s:", writeKey)),
		)),
	)

	res, err := client.Do(req)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer res.Body.Close()

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(string(body))
	fmt.Println("Event Sent Successfully")
}

func TestMain(m *testing.M) {
	flag.BoolVar(&hold, "hold", false, "hold environment clean-up after test execution until Ctrl+C is provided")
	flag.BoolVar(&runIntegration, "integration", false, "run integration level tests")
	flag.Parse()

	if !runIntegration {
		fmt.Println("Skipping integration test. Use `-integration` to run them.")
		return
	}

	// hack to make defer work, without being affected by the os.Exit in TestMain
	os.Exit(run(m))
}

func run(m *testing.M) int {
	// uses a sensible default on windows (tcp/http) and linux/osx (socket)
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	// pulls an redis image, creates a container based on it and runs it
	resourceRedis, err := pool.Run("redis", "alpine3.14", []string{"requirepass=secret"})
	if err != nil {
		log.Fatalf("Could not start resource: %s", err)
	}
	defer func() {
		if err := pool.Purge(resourceRedis); err != nil {
			log.Printf("Could not purge resource: %s \n", err)
		}
	}()
	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	address = fmt.Sprintf("localhost:%s", resourceRedis.GetPort("6379/tcp"))
	if err := pool.Retry(func() error {
		redisClient = redis.NewClient(&redis.Options{
			Addr:     address,
			Password: "",
			DB:       0,
		})

		pong, err := redisClient.Ping().Result()
		fmt.Println(pong, err)
		return err
	}); err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}
	network, err := pool.Client.CreateNetwork(dc.CreateNetworkOptions{Name: "coolest_network_ever"})
	fmt.Println(err)
	z, _ := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "confluentinc/cp-zookeeper",
		Tag:        "latest",
		NetworkID:  network.ID,
		Hostname:   "zookeeper",
		PortBindings: map[dc.Port][]dc.PortBinding{
		   "2181/tcp": {{HostIP: "zookeeper", HostPort: "2181/tcp"}},
		},
		Env: []string{"ZOOKEEPER_CLIENT_PORT=2181"},
	 })
	// Set Kafka: pulls an image, creates a container based on it and runs it
	KAFKA_ZOOKEEPER_CONNECT:=fmt.Sprintf("KAFKA_ZOOKEEPER_CONNECT= zookeeper:%s", z.GetPort("2181/tcp"))
	fmt.Println(KAFKA_ZOOKEEPER_CONNECT)
	resourceKafka, _ := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "confluentinc/cp-kafka",
		Tag:        "latest",
		NetworkID:  network.ID,
		Hostname:   "broker",
		PortBindings: map[dc.Port][]dc.PortBinding{
		   "29092/tcp": {{HostIP: "broker", HostPort: "29092/tcp"}},
		   "9092/tcp":  {{HostIP: "localhost", HostPort: "9092/tcp"}},
		},
		Env: []string{
		   "KAFKA_BROKER_ID=1",
		   "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT",
		   "KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://broker:29092,PLAINTEXT_HOST://localhost:9092",
		   "KAFKA_ZOOKEEPER_CONNECT= zookeeper:2181",
		},
	 })
	 fmt.Println(resourceKafka)

	database := "jobsdb"
	// pulls an image, creates a container based on it and runs it
	resourcePostgres, err := pool.Run("postgres", "11-alpine", []string{
		"POSTGRES_PASSWORD=password",
		"POSTGRES_DB=" + database,
		"POSTGRES_USER=rudder",
	})
	if err != nil {
		log.Fatalf("Could not start resource: %s", err)
	}
	defer func() {
		if err := pool.Purge(resourcePostgres); err != nil {
			log.Printf("Could not purge resource: %s \n", err)
		}
	}()

	DB_DSN = fmt.Sprintf("postgres://rudder:password@localhost:%s/%s?sslmode=disable", resourcePostgres.GetPort("5432/tcp"), database)

	os.Setenv("JOBS_DB_HOST", "localhost")
	os.Setenv("JOBS_DB_NAME", "jobsdb")
	os.Setenv("JOBS_DB_USER", "rudder")
	os.Setenv("JOBS_DB_PASSWORD", "password")
	os.Setenv("JOBS_DB_PORT", resourcePostgres.GetPort("5432/tcp"))

	os.Setenv("WAREHOUSE_JOBS_DB_PORT", resourcePostgres.GetPort("5432/tcp"))
	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	if err := pool.Retry(func() error {
		var err error
		db, err = sql.Open("postgres", DB_DSN)
		if err != nil {
			return err
		}
		return db.Ping()
	}); err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	// ----------
	// Set Rudder Transformer
	// pulls an image, creates a container based on it and runs it
	transformerRes, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "rudderlabs/rudder-transformer",
		Tag:          "latest",
		ExposedPorts: []string{"9090"},
		Env: []string{
			"CONFIG_BACKEND_URL=https://api.dev.rudderlabs.com",
		},
	})
	defer func() {
		if err := pool.Purge(transformerRes); err != nil {
			log.Printf("Could not purge resource: %s \n", err)
		}
	}()

	transformURL := fmt.Sprintf("http://localhost:%s", transformerRes.GetPort("9090/tcp"))
	waitUntilReady(
		context.Background(),
		fmt.Sprintf("%s/health", transformURL),
		time.Minute,
		time.Second,
	)
	os.Setenv("DEST_TRANSFORM_URL", transformURL)

	os.Setenv("RUDDER_ADMIN_PASSWORD", "password")

	os.Setenv("RSERVER_BACKEND_CONFIG_CONFIG_FROM_FILE", "true")

	os.Setenv("WORKSPACE_TOKEN", "1vLbwltztKUgpuFxmJlSe1esX8c")

	os.Setenv("CONFIG_BACKEND_URL", "https://api.dev.rudderlabs.com")

	httpPortInt, err := freeport.GetFreePort()
	if err != nil {
		log.Panic(err)
	}
	httpPort = strconv.Itoa(httpPortInt)
	os.Setenv("RSERVER_GATEWAY_WEB_PORT", httpPort)
	httpAdminPort, err := freeport.GetFreePort()
	if err != nil {
		log.Panic(err)
	}
	os.Setenv("RSERVER_GATEWAY_ADMIN_WEB_PORT", strconv.Itoa(httpAdminPort))

	os.Setenv("RSERVER_ENABLE_STATS", "false")

	webhook = NewWebhook()
	defer webhook.Close()
	webhookurl = webhook.Server.URL
	fmt.Println("webhookurl", webhookurl)

	writeKey = randString(27)
	workspaceID = randString(27)

	workspaceConfigPath := createWorkspaceConfig(
		"testdata/workspaceConfigTemplate.json",
		map[string]string{
			"webhookUrl":  webhookurl,
			"writeKey":    writeKey,
			"workspaceId": workspaceID,
			"postgresPort": resourcePostgres.GetPort("5432/tcp"),
			"address": address,
		},
	)
	defer func() {
		err := os.Remove(workspaceConfigPath)
		fmt.Println(err)
	}()
	fmt.Println("workspace config path:", workspaceConfigPath)
	os.Setenv("RSERVER_BACKEND_CONFIG_CONFIG_JSONPATH", workspaceConfigPath)

	svcCtx, svcCancel := context.WithCancel(context.Background())
	go main.Run(svcCtx)

	serviceHealthEndpoint := fmt.Sprintf("http://localhost:%s/health", httpPort)
	fmt.Println("serviceHealthEndpoint", serviceHealthEndpoint)
	waitUntilReady(
		context.Background(),
		serviceHealthEndpoint,
		time.Minute,
		time.Second,
	)
	code := m.Run()
	blockOnHold()

	_ = svcCancel
	// TODO: svcCancel() - don't cancel service until graceful termination is implemented
	fmt.Println("test done, ignore errors bellow:")

	// // wait for the service to be stopped
	// pool.Retry(func() error {
	// 	_, err := http.Get(serviceHealthEndpoint)
	// 	if err != nil {
	// 		return nil
	// 	}
	// 	return fmt.Errorf("still working")
	// })

	return code
}

func TestWebhook(t *testing.T) {
	//Testing postgres Client
	CreateSCHEMAPostgres()

	//
	var err error
	psqlInfo := jobsdb.GetConnectionString()
	dbHandle, err = sql.Open("postgres", psqlInfo)
	if err != nil {
		panic(err)
	}
	// Pulling config form workspaceConfig.json
	sourceJSON = getWorkspaceConfig()

	require.Empty(t, webhook.Requests(), "webhook should have no request before sending the event")
	SendEvent()

	require.Eventually(t, func() bool {
		return 1 == len(webhook.Requests())
	}, time.Minute, 10*time.Millisecond)

	req := webhook.Requests()[0]

	body, err := ioutil.ReadAll(req.Body)

	require.Equal(t, "POST", req.Method)
	require.Equal(t, "/", req.URL.Path)
	require.Equal(t, "application/json", req.Header.Get("Content-Type"))
	require.Equal(t, "RudderLabs", req.Header.Get("User-Agent"))

	require.Equal(t, gjson.GetBytes(body, "anonymousId").Str, "anon-id-new")
	require.Equal(t, gjson.GetBytes(body, "userId").Str, "identified user id")
	require.Equal(t, gjson.GetBytes(body, "rudderId").Str, "daf823fb-e8d3-413a-8313-d34cd756f968")
	require.Equal(t, gjson.GetBytes(body, "type").Str, "identify")
	
}
// Verify Event in POSTGRES
func TestPostgres(t *testing.T) {
	var myEvent Event
	require.Eventually(t, func() bool {
		eventSql:= "select anonymous_id, user_id from example.identifies limit 1"
		db.QueryRow(eventSql).Scan(&myEvent.anonymous_id, &myEvent.user_id)
		return myEvent.anonymous_id == "anon-id-new"
	}, time.Minute, 10*time.Millisecond)
	require.Equal(t, "identified user id", myEvent.user_id)
	
	}
// Verify Event in Redis	
func TestRedis(t *testing.T) {
	fmt.Println(address)
	conn, err := redigo.Dial("tcp", address)
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
func TestKafka(t *testing.T) {
	config := sarama.NewConfig()
	config.ClientID = "go-kafka-consumer"
	config.Consumer.Return.Errors = true

	brokers := []string{"localhost:9092"}

	// Create new consumer
	master, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		panic(err)
	}
	// defer func() {
	// 	if err := master.Close(); err != nil {
	// 		panic(err)
	// 	}
	// }()
	topics, _ := master.Topics()

	consumer, errors := consume(topics, master)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// Count how many message processed
	msgCount := 0
	// Get signnal for finish
	doneCh := make(chan struct{})
	
	go func() {for {
		select {
		case msg := <-consumer:
			msgCount++
			fmt.Println("Received messages", string(msg.Key), string(msg.Value))
			require.Equal(t, "identified user id", string(msg.Key))
			require.Contains(t, string(msg.Value), "new-val")
			require.Contains(t, string(msg.Value), "identified user id")
			master.Close()
		case consumerError := <-errors:
			msgCount++
			fmt.Println("Received consumerError ", string(consumerError.Topic), string(consumerError.Partition), consumerError.Err)
			doneCh <- struct{}{}
		case <-signals:
			fmt.Println("Interrupt is detected")
			doneCh <- struct{}{}
		}
		
	}}()
	<-doneCh
	fmt.Println("Processed", msgCount, "messages")

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
			fmt.Printf("Topic %v Partitions: %v", topic, partitions)
			panic(err)
		}
		fmt.Println(" Start consuming topic ", topic)
		go func(topic string, consumer sarama.PartitionConsumer) {
			for {
				select {
				case consumerError := <-consumer.Errors():
					errors <- consumerError
					fmt.Println("consumerError: ", consumerError.Err)

				case msg := <-consumer.Messages():
					consumers <- msg
					fmt.Println("Got message on topic ", topic, msg.Value)
				}
			}
		}(topic, consumer)
	}

	return consumers, errors
}
// TODO: Verify in Live Evets API
