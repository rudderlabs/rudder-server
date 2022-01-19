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

	"github.com/gofrs/uuid"
	redigo "github.com/gomodule/redigo/redis"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/warehouse/clickhouse"
	"github.com/rudderlabs/rudder-server/warehouse/mssql"
	"github.com/rudderlabs/rudder-server/warehouse/postgres"
	"github.com/tidwall/gjson"

	"github.com/Shopify/sarama"
	"github.com/go-redis/redis"
	_ "github.com/lib/pq"
	"github.com/minio/minio-go"
	"github.com/ory/dockertest"
	dc "github.com/ory/dockertest/docker"

	"github.com/phayes/freeport"
	main "github.com/rudderlabs/rudder-server"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/stretchr/testify/require"
)

type WHEventsCountMap map[string]int

type PostgresTest struct {
	resource    *dockertest.Resource
	credentials *postgres.CredentialsT
	db          *sql.DB
	eventsMap   WHEventsCountMap
	writeKey    string
}

type ClickHouseTest struct {
	resource    *dockertest.Resource
	credentials *clickhouse.CredentialsT
	db          *sql.DB
	eventsMap   WHEventsCountMap
	writeKey    string
}

type ClickHouseClusterTest struct {
	network      *dc.Network
	zookeeper    *dockertest.Resource
	clickhouse01 *dockertest.Resource
	clickhouse02 *dockertest.Resource
	clickhouse03 *dockertest.Resource
	clickhouse04 *dockertest.Resource
	credentials  *clickhouse.CredentialsT
	db           *sql.DB
	eventsMap    WHEventsCountMap
	writeKey     string
}

type MSSQLTest struct {
	resource    *dockertest.Resource
	credentials *mssql.CredentialsT
	db          *sql.DB
	eventsMap   WHEventsCountMap
	writeKey    string
}

type WareHouseDestinationTest struct {
	db               *sql.DB
	whEventsCountMap WHEventsCountMap
	writeKey         string
	userId           string
	schema           string
}

type WareHouseTest struct {
	pgTest                 *PostgresTest
	chTest                 *ClickHouseTest
	chClusterTest          *ClickHouseClusterTest
	mssqlTest              *MSSQLTest
	gwJobsSqlFunction      string
	batchRtJobsSqlFunction string
}

var (
	hold             bool = true
	db               *sql.DB
	redisClient      *redis.Client
	DB_DSN           = "root@tcp(127.0.0.1:3306)/service"
	httpPort         string
	httpKafkaPort    string
	dbHandle         *sql.DB
	sourceJSON       backendconfig.ConfigT
	webhookurl       string
	disableDestinationwebhookurl     string
	webhook          *WebhookRecorder
	disableDestinationwebhook		 *WebhookRecorder
	address          string
	runIntegration   bool
	writeKey         string
	workspaceID      string
	redisAddress     string
	brokerPort       string
	localhostPort    string
	localhostPortInt int
	whTest           *WareHouseTest
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
	anonymous_id		 string
	user_id    			 string
	count      		 	 string
	context_myuniqueid	 string
	context_id 			 string
	context_ip 			 string
	prop_key			 string
	myuniqueid	 		 string
	ip 					 string
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
func GetEvent(url string, method string) (string, error) {
	client := &http.Client{}
	req, err := http.NewRequest(method, url, nil)

	if err != nil {
		return "", err
	}
	// req.Header.Add("Authorization", "Basic cnVkZGVyOnBhc3N3b3Jk")
	res, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return "", err
	}
	return string(body), err
}

func SendPixelEvents(writeKey string) {
	// Send pixel/v1/page
	url := fmt.Sprintf("http://localhost:%s/pixel/v1/page?writeKey=%s&anonymousId=identified_user_id", httpPort, writeKey)
	method := "GET"
	resBody, err := GetEvent(url, method)
	if err != nil {
		log.Println(err)
		log.Println(resBody)
		return

	}
	// Send pixel/v1/track
	log.Println("Sending pixel/v1/track Event")
	url = fmt.Sprintf("http://localhost:%s/pixel/v1/track?writeKey=%s&anonymousId=identified_user_id&event=product_reviewed_again", httpPort, writeKey)
	method = "GET"
	resBody, _ = GetEvent(url, method)
	if err != nil {
		log.Println(err)
		log.Println(resBody)
		return

	}
}

func SendEvent(payload *strings.Reader, callType string, writeKey string) {
	log.Println(fmt.Sprintf("Sending %s Event", callType))
	url := fmt.Sprintf("http://localhost:%s/v1/%s", httpPort, callType)
	method := "POST"
	client := &http.Client{}
	req, err := http.NewRequest(method, url, payload)

	if err != nil {
		log.Println(err)
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
	log.Println("Event Sent Successfully")
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
	pool, err := dockertest.NewPool("")
	if err != nil {
		return 0, fmt.Errorf("could not connect to docker: %w", err)
	}

	network, err := pool.Client.CreateNetwork(dc.CreateNetworkOptions{Name: "kafka_network"})
	if err != nil {
		log.Fatalf("Could not create docker network: %s", err)
	}
	zookeeperPortInt, err := freeport.GetFreePort()
	if err != nil {
		log.Panic(err)
	}
	defer func() {
		if network == nil {
		    return
		}
	        log.Printf("Purging kafka network resource: %s \n", err)
	        if err := pool.Client.RemoveNetwork(network.ID); err != nil {
			log.Printf("Could not purge kafka network resource: %s \n", err)
		}
	}()
	zookeeperPort := fmt.Sprintf("%s/tcp", strconv.Itoa(zookeeperPortInt))
	zookeeperclientPort := fmt.Sprintf("ZOOKEEPER_CLIENT_PORT=%s", strconv.Itoa(zookeeperPortInt))
	log.Println("zookeeper Port:", zookeeperPort)
	log.Println("zookeeper client Port :", zookeeperclientPort)

	z, err := pool.RunWithOptions(&dockertest.RunOptions{
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
		log.Panic(err)
	}
	defer func() {
		if err := pool.Purge(z); err != nil {
			log.Printf("Could not purge resource: %s \n", err)
		}
	}()

	// Set Kafka: pulls an image, creates a container based on it and runs it
	KAFKA_ZOOKEEPER_CONNECT := fmt.Sprintf("KAFKA_ZOOKEEPER_CONNECT= zookeeper:%s", z.GetPort("2181/tcp"))
	log.Println("KAFKA_ZOOKEEPER_CONNECT:", KAFKA_ZOOKEEPER_CONNECT)

	brokerPortInt, err := freeport.GetFreePort()
	if err != nil {
		log.Panic(err)
	}
	brokerPort = fmt.Sprintf("%s/tcp", strconv.Itoa(brokerPortInt))
	log.Println("broker Port:", brokerPort)

	localhostPortInt, err = freeport.GetFreePort()
	if err != nil {
		log.Panic(err)
	}
	localhostPort = fmt.Sprintf("%s/tcp", strconv.Itoa(localhostPortInt))
	log.Println("localhost Port:", localhostPort)

	KAFKA_ADVERTISED_LISTENERS := fmt.Sprintf("KAFKA_ADVERTISED_LISTENERS=INTERNAL://broker:9090,EXTERNAL://localhost:%s", strconv.Itoa(localhostPortInt))
	KAFKA_LISTENERS := "KAFKA_LISTENERS=INTERNAL://broker:9090,EXTERNAL://:9092"
	log.Println("KAFKA_ADVERTISED_LISTENERS", KAFKA_ADVERTISED_LISTENERS)

	resourceKafka, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "confluentinc/cp-kafka",
		Tag:        "7.0.0",
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
		log.Panic(err)
	}
	log.Println("Kafka PORT:- ", resourceKafka.GetPort("9092/tcp"))
	defer func() {
		if err := pool.Purge(resourceKafka); err != nil {
			log.Printf("Could not purge resource: %s \n", err)
		}
	}()

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
		log.Fatalf("Could not connect to docker: %s", err)
	}

	database := "jobsdb"
	// pulls an image, creates a container based on it and runs it
	resourcePostgres, err := pool.Run("postgres", "11-alpine", []string{
		"POSTGRES_PASSWORD=password",
		"POSTGRES_DB=" + database,
		"POSTGRES_USER=rudder",
	})
	if err != nil {
		return 0, fmt.Errorf("Could not start resource Postgres: %w", err)
	}
	defer func() {
		if err := pool.Purge(resourcePostgres); err != nil {
			log.Printf("Could not purge resource: %s \n", err)
		}
	}()

	DB_DSN = fmt.Sprintf("postgres://rudder:password@localhost:%s/%s?sslmode=disable", resourcePostgres.GetPort("5432/tcp"), database)

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
	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	if err := pool.Retry(func() error {
		var err error
		db, err = sql.Open("postgres", DB_DSN)
		if err != nil {
			return err
		}
		return db.Ping()
	}); err != nil {
		return 0, fmt.Errorf("Could not connect to postgres %q: %w", DB_DSN, err)
	}
	fmt.Println("DB_DSN:", DB_DSN)

	initWHConfig()

	defer SetWHPostgresDestination(pool)()
	defer SetWHClickHouseDestination(pool)()
	defer SetWHClickHouseClusterDestination(pool)()
	defer SetWHMssqlDestination(pool)()

	AddWHSpecificSqlFunctionsToJobsDb()

	// ----------
	// Set Rudder Transformer
	// pulls an image, creates a container based on it and runs it
	transformerRes, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "rudderlabs/rudder-transformer",
		Tag:          "latest",
		ExposedPorts: []string{"9090"},
		Env: []string{
			"CONFIG_BACKEND_URL=https://api.rudderlabs.com",
		},
	})
	if err != nil {
		return 0, fmt.Errorf("Could not start resource transformer: %w", err)
	}
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

	os.Setenv("CONFIG_BACKEND_URL", "https://api.rudderlabs.com")

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

	disableDestinationwebhook = NewWebhook()
	defer disableDestinationwebhook.Close()
	disableDestinationwebhookurl = disableDestinationwebhook.Server.URL

	minioPortInt, err := freeport.GetFreePort()
	if err != nil {
		log.Panic(err)
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

	resource, err := pool.RunWithOptions(options)
	if err != nil {
		log.Fatalf("Could not start resource: %s", err)
	}
	defer func() {
		if err := pool.Purge(resource); err != nil {
			log.Printf("Could not purge resource: %s \n", err)
		}
	}()

	minioEndpoint := fmt.Sprintf("localhost:%s", resource.GetPort("9000/tcp"))

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
		log.Fatalf("Could not connect to docker: %s", err)
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
	minioBucketName := "devintegrationtest"
	err = minioClient.MakeBucket(minioBucketName, "us-east-1")
	if err != nil {
		log.Println(err)
		panic(err)
	}

	writeKey = randString(27)
	workspaceID = randString(27)

	workspaceConfigPath := createWorkspaceConfig(
		"testdata/workspaceConfigTemplate.json",
		map[string]string{
			"webhookUrl":                          webhookurl,
			"disableDestinationwebhookUrl":                         disableDestinationwebhookurl,
			"writeKey":                            writeKey,
			"workspaceId":                         workspaceID,
			"postgresPort":                        resourcePostgres.GetPort("5432/tcp"),
			"address":                             redisAddress,
			"minioEndpoint":                       minioEndpoint,
			"minioBucketName":                     minioBucketName,
			"kafkaPort":                           strconv.Itoa(localhostPortInt),
			"postgresEventWriteKey":               whTest.pgTest.writeKey,
			"clickHouseEventWriteKey":             whTest.chTest.writeKey,
			"clickHouseClusterEventWriteKey":      whTest.chClusterTest.writeKey,
			"mssqlEventWriteKey":                  whTest.mssqlTest.writeKey,
			"rwhPostgresDestinationPort":          whTest.pgTest.credentials.Port,
			"rwhClickHouseDestinationPort":        whTest.chTest.credentials.Port,
			"rwhClickHouseClusterDestinationPort": whTest.chClusterTest.credentials.Port,
			"rwhMSSqlDestinationPort":             whTest.mssqlTest.credentials.Port,
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
	fmt.Println("RUDDER_TMPDIR:", rudderTmpDir)

	fmt.Printf("--- Setup done (%s)\n", time.Since(setupStart))

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
	os.Setenv("RSERVER_WAREHOUSE_WAREHOUSE_SYNC_FREQ_IGNORE", "true")
	os.Setenv("RSERVER_WAREHOUSE_UPLOAD_FREQ_IN_S", "10s")
	os.Setenv("RUDDER_GRACEFUL_SHUTDOWN_TIMEOUT_EXIT", "false")

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
	SendEvent(payload_1, "identify", writeKey)
	payload_2 := strings.NewReader(`{
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
	SendEvent(payload_2, "identify", writeKey) //sending duplicate event to check dedup

	// Sending Batch event
	payload_Batch := strings.NewReader(`{
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
	SendEvent(payload_Batch, "batch", writeKey)

	// Sending track event
	payload_track := strings.NewReader(`{
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
	SendEvent(payload_track, "track",writeKey)

	// Sending page event
	payload_page := strings.NewReader(`{
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
	SendEvent(payload_page, "page",writeKey)

	// Sending screen event
	payload_screen := strings.NewReader(`{
		"userId": "identified_user_id",
		"anonymousId":"anonymousId_1",
		"messageId":"messageId_1",
		"type": "screen",
		"name": "Main",
		"properties": {
		  "prop_key": "prop_value"
		}
	  }`)
	SendEvent(payload_screen, "screen",writeKey)

	// Sending alias event
	payload_alias := strings.NewReader(`{
		"userId": "identified_user_id",
		"anonymousId":"anonymousId_1",
		"messageId":"messageId_1",
		"type": "alias",
		"previousId": "name@surname.com",
		"userId": "12345"
	  }`)
	SendEvent(payload_alias, "alias",writeKey)

	// Sending group event
	payload_group := strings.NewReader(`{
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
	SendEvent(payload_group, "group",writeKey)
	SendPixelEvents(writeKey)

	require.Eventually(t, func() bool {
		return len(webhook.Requests()) == 10
	}, time.Minute, 30*time.Millisecond)

	i := -1
	require.Eventually(t, func() bool {
		i = i + 1
		req := webhook.Requests()[i]
		body, _ := io.ReadAll(req.Body)
		return gjson.GetBytes(body, "anonymousId").Str == "anonymousId_1"
	}, time.Minute, 10*time.Millisecond)

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
	require.Equal(t, 0, len(disableDestinationwebhook.Requests()))
}

// Verify Event in POSTGRES
func TestPostgres(t *testing.T) {
	var myEvent Event
	require.Eventually(t, func() bool {
		eventSql := "select anonymous_id, user_id from dev_integration_test_1.identifies limit 1"
		db.QueryRow(eventSql).Scan(&myEvent.anonymous_id, &myEvent.user_id)
		return myEvent.anonymous_id == "anonymousId_1"
	}, time.Minute, 10*time.Millisecond)
	eventSql := "select count(*) from dev_integration_test_1.identifies"
	db.QueryRow(eventSql).Scan(&myEvent.count)
	require.Equal(t, myEvent.count, "2")

	// Verify User Transformation
	eventSql = "select context_myuniqueid,context_id,context_ip from dev_integration_test_1.identifies"
	db.QueryRow(eventSql).Scan(&myEvent.context_myuniqueid, &myEvent.context_id, &myEvent.context_ip)
	require.Equal(t, myEvent.context_myuniqueid, "identified_user_idanonymousId_1")
	require.Equal(t, myEvent.context_id, "0.0.0.0")
	require.Equal(t, myEvent.context_ip, "0.0.0.0")


	require.Eventually(t, func() bool {
		eventSql := "select anonymous_id, user_id from dev_integration_test_1.users limit 1"
		db.QueryRow(eventSql).Scan(&myEvent.anonymous_id, &myEvent.user_id)
		return myEvent.anonymous_id == "anonymousId_1"
	}, time.Minute, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		eventSql = "select count(*) from dev_integration_test_1.users"
		db.QueryRow(eventSql).Scan(&myEvent.count)
		return myEvent.count == "1"
	}, time.Minute, 10*time.Millisecond)

	// Verify User Transformation
	eventSql = "select context_myuniqueid,context_id,context_ip from dev_integration_test_1.users "
	db.QueryRow(eventSql).Scan(&myEvent.context_myuniqueid, &myEvent.context_id, &myEvent.context_ip)
	require.Equal(t, myEvent.context_myuniqueid, "identified_user_idanonymousId_1")
	require.Equal(t, myEvent.context_id, "0.0.0.0")
	require.Equal(t, myEvent.context_ip, "0.0.0.0")


	require.Eventually(t, func() bool {
			eventSql := "select anonymous_id, user_id from dev_integration_test_1.screens limit 1"
			db.QueryRow(eventSql).Scan(&myEvent.anonymous_id, &myEvent.user_id)
		return myEvent.anonymous_id == "anonymousId_1"
	}, time.Minute, 10*time.Millisecond)
	require.Eventually(t, func() bool {
			eventSql = "select count(*) from dev_integration_test_1.screens"
			db.QueryRow(eventSql).Scan(&myEvent.count)
		return  myEvent.count == "1"
	}, time.Minute, 10*time.Millisecond)
	
	// Verify User Transformation
	require.Eventually(t, func() bool {
			eventSql = "select prop_key,myuniqueid,ip from dev_integration_test_1.screens;"
			db.QueryRow(eventSql).Scan(&myEvent.prop_key, &myEvent.myuniqueid, &myEvent.ip)
		return  myEvent.myuniqueid == "identified_user_idanonymousId_1"
	}, time.Minute, 10*time.Millisecond)
	
	require.Equal(t, myEvent.prop_key, "prop_value_edited")
	require.Equal(t, myEvent.ip, "0.0.0.0")
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
		event, _ := redigo.String(conn.Do("HGET", "user:identified_user_id", "trait1"))
		return event == "new-val"
	}, time.Minute, 10*time.Millisecond)
}

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
	expectedCount := 10
out:
	for {
		select {
		case msg := <-consumer:
			msgCount++
			t.Log("Received messages", string(msg.Key), string(msg.Value))
			require.Equal(t, "identified_user_id", string(msg.Key))
			require.Contains(t, string(msg.Value), "identified_user_id")

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
				}
			}
		}(topic, consumer)
	}

	return consumers, errors
}

// initWHConfig Initialize warehouse config
func initWHConfig() {
	whTest = &WareHouseTest{}
	whTest.gwJobsSqlFunction = `CREATE OR REPLACE FUNCTION gw_jobs_for_user_id_and_write_key(user_id varchar, write_key varchar)
								RETURNS TABLE
										(
											job_id varchar
										)
							AS
							$$
							DECLARE
								table_record RECORD;
								batch_record jsonb;
							BEGIN
								FOR table_record IN SELECT * FROM gw_jobs_1 where (event_payload ->> 'writeKey') = write_key
									LOOP
										FOR batch_record IN SELECT * FROM jsonb_array_elements((table_record.event_payload ->> 'batch')::jsonb)
											LOOP
												if batch_record ->> 'userId' != user_id THEN
													CONTINUE;
												END IF;
												job_id := table_record.job_id;
												RETURN NEXT;
												EXIT;
											END LOOP;
									END LOOP;
							END;
							$$ LANGUAGE plpgsql`
	whTest.batchRtJobsSqlFunction = `CREATE OR REPLACE FUNCTION brt_jobs_for_user_id(user_id varchar)
									RETURNS TABLE
											(
												job_id varchar
											)
								AS
								$$
								DECLARE
									table_record  RECORD;
									event_payload jsonb;
								BEGIN
									FOR table_record IN SELECT * FROM batch_rt_jobs_1
										LOOP
											event_payload = (table_record.event_payload ->> 'data')::jsonb;
											if event_payload ->> 'user_id' = user_id Or event_payload ->> 'id' = user_id THEN
												job_id := table_record.job_id;
												RETURN NEXT;
											END IF;
										END LOOP;
								END ;
								$$ LANGUAGE plpgsql`

	config.Load()
	logger.Init()
	postgres.Init()
	clickhouse.Init()
	mssql.Init()
}

// SetWHPostgresDestination setup warehouse postgres destination
func SetWHPostgresDestination(pool *dockertest.Pool) (cleanup func()) {
	whTest.pgTest = &PostgresTest{
		writeKey: randString(27),
		credentials: &postgres.CredentialsT{
			DBName:   "rudderdb",
			Password: "rudder-password",
			User:     "rudder",
			Host:     "localhost",
			SSLMode:  "disable",
		},
		eventsMap: WHEventsCountMap{
			"identifies":    1,
			"users":         1,
			"tracks":        1,
			"product_track": 1,
			"pages":         1,
			"screens":       1,
			"aliases":       1,
			"groups":        1,
			"gateway":       6,
			"batchRT":       8,
		},
	}
	pgTest := whTest.pgTest
	credentials := pgTest.credentials
	cleanup = func() {}

	var err error
	if pgTest.resource, err = pool.Run("postgres", "11-alpine", []string{
		fmt.Sprintf("POSTGRES_DB=%s", credentials.DBName),
		fmt.Sprintf("POSTGRES_PASSWORD=%s", credentials.Password),
		fmt.Sprintf("POSTGRES_USER=%s", credentials.User),
	}); err != nil {
		panic(fmt.Errorf("Could not create WareHouse Postgres: %v\n", err))
	}

	// Getting at which port the postgres container is running
	credentials.Port = pgTest.resource.GetPort("5432/tcp")

	purgeResources := func() {
		if pgTest.resource != nil {
			log.Printf("Purging warehouse postgres resource: %s \n", err)
			if err := pool.Purge(pgTest.resource); err != nil {
				log.Printf("Could not purge warehouse postgres resource: %s \n", err)
			}
		}
	}

	if err = pool.Retry(func() error {
		var err error
		pgTest.db, err = postgres.Connect(*credentials)
		if err != nil {
			return err
		}
		return pgTest.db.Ping()
	}); err != nil {
		defer purgeResources()
		panic(fmt.Errorf("Could not connect to warehouse postgres with error: %w\n", err))
	}
	cleanup = purgeResources
	return
}

// SetWHClickHouseDestination setup warehouse clickhouse destination
func SetWHClickHouseDestination(pool *dockertest.Pool) (cleanup func()) {
	whTest.chTest = &ClickHouseTest{
		writeKey: randString(27),
		credentials: &clickhouse.CredentialsT{
			Host:          "localhost",
			User:          "rudder",
			Password:      "rudder-password",
			DBName:        "rudderdb",
			Secure:        "false",
			SkipVerify:    "true",
			TLSConfigName: "",
		},
		eventsMap: WHEventsCountMap{
			"identifies":    1,
			"users":         1,
			"tracks":        1,
			"product_track": 1,
			"pages":         1,
			"screens":       1,
			"aliases":       1,
			"groups":        1,
			"gateway":       6,
			"batchRT":       8,
		},
	}
	chTest := whTest.chTest
	credentials := chTest.credentials
	cleanup = func() {}

	var err error
	if chTest.resource, err = pool.Run("yandex/clickhouse-server", "21-alpine", []string{
		fmt.Sprintf("CLICKHOUSE_DB=%s", credentials.DBName),
		fmt.Sprintf("CLICKHOUSE_PASSWORD=%s", credentials.Password),
		fmt.Sprintf("CLICKHOUSE_USER=%s", credentials.User),
	}); err != nil {
		panic(fmt.Errorf("Could not create WareHouse ClickHouse: %v\n", err))
	}

	// Getting at which port the clickhouse container is running
	credentials.Port = chTest.resource.GetPort("9000/tcp")

	purgeResources := func() {
		if chTest.resource != nil {
			log.Printf("Purging warehouse clickhouse resource: %s \n", err)
			if err := pool.Purge(chTest.resource); err != nil {
				log.Printf("Could not purge warehouse clickhouse resource: %s \n", err)
			}
		}
	}

	if err = pool.Retry(func() error {
		var err error
		chTest.db, err = clickhouse.Connect(*credentials, true)
		if err != nil {
			return err
		}
		return chTest.db.Ping()
	}); err != nil {
		defer purgeResources()
		panic(fmt.Errorf("Could not connect to warehouse clickhouse with error: %w\n", err))
	}
	cleanup = purgeResources
	return
}

// SetWHClickHouseClusterDestination setup warehouse clickhouse cluster mode destination
func SetWHClickHouseClusterDestination(pool *dockertest.Pool) (cleanup func()) {
	whTest.chClusterTest = &ClickHouseClusterTest{
		writeKey: randString(27),
		credentials: &clickhouse.CredentialsT{
			Host:          "localhost",
			User:          "rudder",
			Password:      "rudder-password",
			DBName:        "rudderdb",
			Secure:        "false",
			SkipVerify:    "true",
			TLSConfigName: "",
		},
		eventsMap: WHEventsCountMap{
			"identifies":    1,
			"users":         1,
			"tracks":        1,
			"product_track": 1,
			"pages":         1,
			"screens":       1,
			"aliases":       1,
			"groups":        1,
			"gateway":       6,
			"batchRT":       8,
		},
	}
	chClusterTest := whTest.chClusterTest
	credentials := chClusterTest.credentials
	cleanup = func() {}

	pwd, err := os.Getwd()
	if err != nil {
		panic(fmt.Errorf("Could not get working directory: %w", err))
	}

	var chSetupError error
	if chClusterTest.network, err = pool.Client.CreateNetwork(dc.CreateNetworkOptions{
		Name: "clickhouse-network",
		IPAM: &dc.IPAMOptions{
			Config: []dc.IPAMConfig{
				{
					Subnet: "172.23.0.0/24",
				},
			},
		},
	}); err != nil {
		chSetupError = err
		log.Println("Could not create clickhouse cluster network: %w", err)
	}

	if chClusterTest.zookeeper, err = pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "zookeeper",
		Tag:        "3.5",
		Hostname:   "clickhouse-zookeeper",
		Name:       "clickhouse-zookeeper",
	}); err != nil {
		chSetupError = err
		log.Println("Could not create clickhouse cluster zookeeper: %w", err)
	}

	if chClusterTest.clickhouse01, err = pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "yandex/clickhouse-server",
		Tag:        "21-alpine",
		Hostname:   "clickhouse01",
		Name:       "clickhouse01",
		PortBindings: map[dc.Port][]dc.PortBinding{
			"8123": {{HostIP: "127.0.0.1", HostPort: "8123"}},
			"9000": {{HostIP: "127.0.0.1", HostPort: "9000"}},
		},
		ExposedPorts: []string{"8123", "9000"},
		Mounts:       []string{fmt.Sprintf(`%s/testdata/warehouse/clickhouse/cluster/clickhouse01:/etc/clickhouse-server`, pwd)},
		Links:        []string{"clickhouse-zookeeper"},
	}); err != nil {
		chSetupError = err
		log.Println("Could not create clickhouse cluster 1: %w", err)
	}
	if chClusterTest.clickhouse02, err = pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "yandex/clickhouse-server",
		Tag:        "21-alpine",
		Hostname:   "clickhouse02",
		Name:       "clickhouse02",
		Mounts:     []string{fmt.Sprintf(`%s/testdata/warehouse/clickhouse/cluster/clickhouse02:/etc/clickhouse-server`, pwd)},
		Links:      []string{"clickhouse-zookeeper"},
	}); err != nil {
		chSetupError = err
		log.Println("Could not create clickhouse cluster 2: %w", err)
	}
	if chClusterTest.clickhouse03, err = pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "yandex/clickhouse-server",
		Tag:        "21-alpine",
		Hostname:   "clickhouse03",
		Name:       "clickhouse03",
		Mounts:     []string{fmt.Sprintf(`%s/testdata/warehouse/clickhouse/cluster/clickhouse03:/etc/clickhouse-server`, pwd)},
		Links:      []string{"clickhouse-zookeeper"},
	}); err != nil {
		chSetupError = err
		log.Println("Could not create clickhouse cluster 3: %w", err)
	}
	if chClusterTest.clickhouse04, err = pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "yandex/clickhouse-server",
		Tag:        "21-alpine",
		Hostname:   "clickhouse04",
		Name:       "clickhouse04",
		Mounts:     []string{fmt.Sprintf(`%s/testdata/warehouse/clickhouse/cluster/clickhouse04:/etc/clickhouse-server`, pwd)},
		Links:      []string{"clickhouse-zookeeper"},
	}); err != nil {
		chSetupError = err
		log.Println("Could not create clickhouse cluster 4: %w", err)
	}

	if chClusterTest.network != nil {
		if chClusterTest.zookeeper != nil {
			if err = pool.Client.ConnectNetwork(chClusterTest.network.ID, dc.NetworkConnectionOptions{
				Container: chClusterTest.zookeeper.Container.Name,
				EndpointConfig: &dc.EndpointConfig{
					IPAddress: "172.23.0.10",
				},
			}); err != nil {
				chSetupError = err
				log.Println("Could not configure clickhouse clutser zookeeper network: %w", err)
			}
		}

		if chClusterTest.clickhouse01 != nil {
			if err = pool.Client.ConnectNetwork(chClusterTest.network.ID, dc.NetworkConnectionOptions{
				Container: chClusterTest.clickhouse01.Container.Name,
				EndpointConfig: &dc.EndpointConfig{
					IPAddress: "172.23.0.11",
				},
			}); err != nil {
				chSetupError = err
				log.Println("Could not configure clickhouse cluster 1 network: %w", err)
			}
		}
		if chClusterTest.clickhouse02 != nil {
			if err = pool.Client.ConnectNetwork(chClusterTest.network.ID, dc.NetworkConnectionOptions{
				Container: chClusterTest.clickhouse02.Container.Name,
				EndpointConfig: &dc.EndpointConfig{
					IPAddress: "172.23.0.12",
				},
			}); err != nil {
				chSetupError = err
				log.Println("Could not configure clickhouse cluster 2 network: %w", err)
			}
		}
		if chClusterTest.clickhouse03 != nil {
			if err = pool.Client.ConnectNetwork(chClusterTest.network.ID, dc.NetworkConnectionOptions{
				Container: chClusterTest.clickhouse03.Container.Name,
				EndpointConfig: &dc.EndpointConfig{
					IPAddress: "172.23.0.13",
				},
			}); err != nil {
				chSetupError = err
				log.Println("Could not configure clickhouse cluster 3 network: %w", err)
			}
		}
		if chClusterTest.clickhouse04 != nil {
			if err = pool.Client.ConnectNetwork(chClusterTest.network.ID, dc.NetworkConnectionOptions{
				Container: chClusterTest.clickhouse04.Container.Name,
				EndpointConfig: &dc.EndpointConfig{
					IPAddress: "172.23.0.14",
				},
			}); err != nil {
				chSetupError = err
				log.Println("Could not configure clickhouse cluster 4 network: %w", err)
			}
		}
	}

	purgeResources := func() {
		if chClusterTest.zookeeper != nil {
			log.Printf("Purging clickhouse cluster zookeeper resource: %s \n", err)
			if err := pool.Purge(chClusterTest.zookeeper); err != nil {
				log.Printf("Could not purge clickhouse cluster zookeeper resource: %s \n", err)
			}
		}
		if chClusterTest.clickhouse01 != nil {
			log.Printf("Purging clickhouse cluster 1 resource: %s \n", err)
			if err := pool.Purge(chClusterTest.clickhouse01); err != nil {
				log.Printf("Could not purge clickhouse cluster 1 resource: %s \n", err)
			}
		}
		if chClusterTest.clickhouse02 != nil {
			log.Printf("Purging clickhouse cluster 2 resource: %s \n", err)
			if err := pool.Purge(chClusterTest.clickhouse02); err != nil {
				log.Printf("Could not purge clickhouse cluster 2 resource: %s \n", err)
			}
		}
		if chClusterTest.clickhouse03 != nil {
			log.Printf("Purging clickhouse cluster 3 resource: %s \n", err)
			if err := pool.Purge(chClusterTest.clickhouse03); err != nil {
				log.Printf("Could not purge clickhouse cluster 3 resource: %s \n", err)
			}
		}
		if chClusterTest.clickhouse04 != nil {
			log.Printf("Purging clickhouse cluster 4 resource: %s \n", err)
			if err := pool.Purge(chClusterTest.clickhouse04); err != nil {
				log.Printf("Could not purge clickhouse cluster 4 resource: %s \n", err)
			}
		}
		if chClusterTest.network != nil {
			log.Printf("Purging clickhouse cluster network resource: %s \n", err)
			if err := pool.Client.RemoveNetwork(chClusterTest.network.ID); err != nil {
				log.Printf("Could not purge clickhouse cluster network resource: %s \n", err)
			}
		}
	}

	if chSetupError != nil {
		defer purgeResources()
		panic(fmt.Errorf("Could not create WareHouse ClickHouse Cluster: %v\n", chSetupError))
	}

	// Getting at which port the container is running
	credentials.Port = chClusterTest.clickhouse01.GetPort("9000/tcp")

	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	if err := pool.Retry(func() error {
		var err error
		chClusterTest.db, err = clickhouse.Connect(*credentials, true)
		if err != nil {
			return err
		}
		return chClusterTest.db.Ping()
	}); err != nil {
		defer purgeResources()
		panic(fmt.Errorf("Could not connect to warehouse clickhouse cluster with error: %w\n", err))
	}
	cleanup = purgeResources
	return
}

// SetWHMssqlDestination setup clickhouse mssql destination
func SetWHMssqlDestination(pool *dockertest.Pool) (cleanup func()) {
	whTest.mssqlTest = &MSSQLTest{
		writeKey: randString(27),
		credentials: &mssql.CredentialsT{
			DBName:   "master",
			Password: "reallyStrongPwd123",
			User:     "SA",
			Host:     "localhost",
			SSLMode:  "disable",
		},
		eventsMap: WHEventsCountMap{
			"identifies":    1,
			"users":         1,
			"tracks":        1,
			"product_track": 1,
			"pages":         1,
			"screens":       1,
			"aliases":       1,
			"groups":        1,
			"gateway":       6,
			"batchRT":       8,
		},
	}
	mssqlTest := whTest.mssqlTest
	credentials := mssqlTest.credentials
	cleanup = func() {}

	var err error
	if mssqlTest.resource, err = pool.Run("mcr.microsoft.com/mssql/server", "2019-CU10-ubuntu-20.04", []string{
		fmt.Sprintf("ACCEPT_EULA=%s", "Y"),
		fmt.Sprintf("SA_PASSWORD=%s", credentials.Password),
		fmt.Sprintf("SA_DB=%s", credentials.DBName),
		fmt.Sprintf("SA_USER=%s", credentials.User),
	}); err != nil {
		panic(fmt.Errorf("Could not create WareHouse Mssql: %v\n", err))
	}

	// Getting at which port the mssql container is running
	credentials.Port = mssqlTest.resource.GetPort("1433/tcp")

	purgeResources := func() {
		if mssqlTest.resource != nil {
			if err := pool.Purge(mssqlTest.resource); err != nil {
				log.Printf("Could not purge warehouse mssql resource: %s \n", err)
			}
		}
	}

	if err = pool.Retry(func() error {
		var err error
		mssqlTest.db, err = mssql.Connect(*credentials)
		if err != nil {
			return err
		}
		return mssqlTest.db.Ping()
	}); err != nil {
		defer purgeResources()
		panic(fmt.Errorf("Could not connect to warehouse mssql with error: %w\n", err))
	}
	cleanup = purgeResources
	return
}

// Adding necessary sql functions which needs to be used during warehouse testing
func AddWHSpecificSqlFunctionsToJobsDb() {
	var err error
	_, err = db.Exec(whTest.gwJobsSqlFunction)
	if err != nil {
		panic(fmt.Errorf("Error occurred with executing gw jobs function for events count with err %v\n", err))
		return
	}

	_, err = db.Exec(whTest.batchRtJobsSqlFunction)
	if err != nil {
		panic(fmt.Errorf("Error occurred with executing brt jobs function for events count with err %v\n", err))
		return
	}
}

// Verify Event in WareHouse Postgres
func TestWHPostgresDestination(t *testing.T) {
	pgTest := whTest.pgTest

	whDestTest := &WareHouseDestinationTest{
		db:               pgTest.db,
		whEventsCountMap: pgTest.eventsMap,
		writeKey:         pgTest.writeKey,
		userId:           "userId_postgres",
		schema:           "postgres_wh_integration",
	}
	sendWHEvents(whDestTest)
	whDestinationTest(t, whDestTest)

	whDestTest.userId = "userId_postgres_1"
	sendUpdatedWHEvents(whDestTest)
	whDestinationTest(t, whDestTest)
}

// Verify Event in WareHouse ClickHouse
func TestWHClickHouseDestination(t *testing.T) {
	chTest := whTest.chTest

	whDestTest := &WareHouseDestinationTest{
		db:               chTest.db,
		whEventsCountMap: chTest.eventsMap,
		writeKey:         chTest.writeKey,
		userId:           "userId_clickhouse",
		schema:           "rudderdb",
	}
	sendWHEvents(whDestTest)
	whDestinationTest(t, whDestTest)

	whDestTest.userId = "userId_clickhouse_1"
	sendUpdatedWHEvents(whDestTest)
	whDestinationTest(t, whDestTest)
}

// Verify Event in WareHouse ClickHouse Cluster
func TestWHClickHouseClusterDestination(t *testing.T) {
	chClusterTest := whTest.chClusterTest

	whDestTest := &WareHouseDestinationTest{
		db:               chClusterTest.db,
		whEventsCountMap: chClusterTest.eventsMap,
		writeKey:         chClusterTest.writeKey,
		userId:           "userId_clickhouse_cluster",
		schema:           "rudderdb",
	}
	sendWHEvents(whDestTest)
	whDestinationTest(t, whDestTest)

	initWHClickHouseClusterModeSetup(t)

	whDestTest.userId = "userId_clickhouse_cluster_1"
	sendWHEvents(whDestTest)

	// Update events count Map
	// This is required as because of the cluster mode setup and distributed view, events are getting duplicated.
	whDestTest.whEventsCountMap = WHEventsCountMap{
		"identifies":    2,
		"users":         2,
		"tracks":        2,
		"product_track": 2,
		"pages":         2,
		"screens":       2,
		"aliases":       2,
		"groups":        2,
		"gateway":       6,
		"batchRT":       8,
	}
	whDestinationTest(t, whDestTest)
}

// Verify Event in WareHouse MSSQL
func TestWHMssqlDestination(t *testing.T) {
	whDestTest := &WareHouseDestinationTest{
		db:               whTest.mssqlTest.db,
		whEventsCountMap: whTest.mssqlTest.eventsMap,
		writeKey:         whTest.mssqlTest.writeKey,
		userId:           "userId_mssql",
		schema:           "mssql_wh_integration",
	}
	sendWHEvents(whDestTest)
	whDestinationTest(t, whDestTest)

	whDestTest.userId = "userId_mssql_1"
	sendUpdatedWHEvents(whDestTest)
	whDestinationTest(t, whDestTest)
}

// sendWHEvents Sending warehouse events
func sendWHEvents(wdt *WareHouseDestinationTest) {
	// Sending identify event
	if identify, exists := wdt.whEventsCountMap["identifies"]; exists {
		for i := 0; i < identify; i++ {
			payloadIdentify := strings.NewReader(fmt.Sprintf(`{
			"userId": "%s",
			"messageId":"%s",
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
		  }`, wdt.userId, uuid.Must(uuid.NewV4()).String()))
			SendEvent(payloadIdentify, "identify", wdt.writeKey)
		}
	}

	// Sending track event
	if track, exists := wdt.whEventsCountMap["tracks"]; exists {
		for i := 0; i < track; i++ {
			payloadTrack := strings.NewReader(fmt.Sprintf(`{
			"userId": "%s",
			"messageId":"%s",
			"type": "track",
			"event": "Product Track",
			"properties": {
			  "review_id": "12345",
			  "product_id" : "123",
			  "rating" : 3.0,
			  "review_body" : "Average product, expected much more."
			}
		  }`, wdt.userId, uuid.Must(uuid.NewV4()).String()))
			SendEvent(payloadTrack, "track", wdt.writeKey)
		}
	}

	// Sending page event
	if page, exists := wdt.whEventsCountMap["pages"]; exists {
		for i := 0; i < page; i++ {
			payloadPage := strings.NewReader(fmt.Sprintf(`{
			"userId": "%s",
			"messageId":"%s",
			"type": "page",
			"name": "Home",
			"properties": {
			  "title": "Home | RudderStack",
			  "url": "http://www.rudderstack.com"
			}
		  }`, wdt.userId, uuid.Must(uuid.NewV4()).String()))
			SendEvent(payloadPage, "page", wdt.writeKey)
		}
	}

	// Sending screen event
	if screen, exists := wdt.whEventsCountMap["screens"]; exists {
		for i := 0; i < screen; i++ {
			payloadScreen := strings.NewReader(fmt.Sprintf(`{
			"userId": "%s",
			"messageId":"%s",
			"type": "screen",
			"name": "Main",
			"properties": {
			  "prop_key": "prop_value"
			}
		  }`, wdt.userId, uuid.Must(uuid.NewV4()).String()))
			SendEvent(payloadScreen, "screen", wdt.writeKey)
		}
	}

	// Sending alias event
	if alias, exists := wdt.whEventsCountMap["aliases"]; exists {
		for i := 0; i < alias; i++ {
			payloadAlias := strings.NewReader(fmt.Sprintf(`{
			"userId": "%s",
			"messageId":"%s",
			"type": "alias",
			"previousId": "name@surname.com"
		  }`, wdt.userId, uuid.Must(uuid.NewV4()).String()))
			SendEvent(payloadAlias, "alias", wdt.writeKey)
		}
	}

	// Sending group event
	if group, exists := wdt.whEventsCountMap["groups"]; exists {
		for i := 0; i < group; i++ {
			payloadGroup := strings.NewReader(fmt.Sprintf(`{
			"userId": "%s",
			"messageId":"%s",
			"type": "group",
			"groupId": "groupId",
			"traits": {
			  "name": "MyGroup",
			  "industry": "IT",
			  "employees": 450,
			  "plan": "basic"
			}
		  }`, wdt.userId, uuid.Must(uuid.NewV4()).String()))
			SendEvent(payloadGroup, "group", wdt.writeKey)
		}
	}
}

// sendUpdatedWHEvents Sending updated warehouse events
func sendUpdatedWHEvents(wdt *WareHouseDestinationTest) {
	// Sending identify event
	if identify, exists := wdt.whEventsCountMap["identifies"]; exists {
		for i := 0; i < identify; i++ {
			payloadIdentify := strings.NewReader(fmt.Sprintf(`{
			"userId": "%s",
			"messageId":"%s",
		    "channel": "web",
			"eventOrderNo":"1",
			"type": "identify",
			"context": {
			  "traits": {
				 "trait1": "new-val",
				 "trait2": "new-val"
			  },
			  "ip": "14.5.67.21",
			  "library": {
				  "name": "http"
			  }
			},
			"traits": {
				"data": [
					{
						"city": "Disney",
						"country": "USA",
						"email": "mickey@disney.com",
						"firstname": "Mickey",
						"version": [
							"1",
							"2",
							"3"
						]
					},
					{
						"city": "Disney",
						"country": "USA",
						"email": "mickey@disney.com",
						"firstname": "Mickey"
					}
				]
			},
			"originalTimestamp": "2020-01-24T06:29:02.362Z",
    		"receivedAt": "2020-01-24T11:59:02.403+05:30",
    		"request_ip": "[::1]:53709",
    		"sentAt": "2020-01-24T06:29:02.363Z",
			"timestamp": "2020-02-02T00:23:09.544Z"
		  }`, wdt.userId, uuid.Must(uuid.NewV4()).String()))
			SendEvent(payloadIdentify, "identify", wdt.writeKey)
		}
	}

	// Sending track event
	if track, exists := wdt.whEventsCountMap["tracks"]; exists {
		for i := 0; i < track; i++ {
			payloadTrack := strings.NewReader(fmt.Sprintf(`{
			"userId": "%s",
			"messageId":"%s",
			"type": "track",
			"event": "Product Track",
			"properties": {
			  "review_id": "12345",
			  "product_id" : "123",
			  "rating" : 3.0,
			  "revenue" : 4.99,
			  "review_body" : "Average product, expected much more."
			},
			"context": {
				"ip": "14.5.67.21"
			}
		  }`, wdt.userId, uuid.Must(uuid.NewV4()).String()))
			SendEvent(payloadTrack, "track", wdt.writeKey)
		}
	}

	// Sending page event
	if page, exists := wdt.whEventsCountMap["pages"]; exists {
		for i := 0; i < page; i++ {
			payloadPage := strings.NewReader(fmt.Sprintf(`{
			"userId": "%s",
			"messageId":"%s",
			"type": "page",
			"name": "Home",
			"properties": {
			  "title": "Home | RudderStack",
			  "url": "http://www.rudderstack.com"
			},
			"context": {
				"ip": "14.5.67.21",
				"library": {
					"name": "http"
				}
			  }
		  }`, wdt.userId, uuid.Must(uuid.NewV4()).String()))
			SendEvent(payloadPage, "page", wdt.writeKey)
		}
	}

	// Sending screen event
	if screen, exists := wdt.whEventsCountMap["screens"]; exists {
		for i := 0; i < screen; i++ {
			payloadScreen := strings.NewReader(fmt.Sprintf(`{
			"userId": "%s",
			"messageId":"%s",
			"type": "screen",
			"name": "Main",
			"properties": {
			  "prop_key": "prop_value"
			},
			"context": {
				"ip": "14.5.67.21",
				"library": {
					"name": "http"
				}
			  }
		  }`, wdt.userId, uuid.Must(uuid.NewV4()).String()))
			SendEvent(payloadScreen, "screen", wdt.writeKey)
		}
	}

	// Sending alias event
	if alias, exists := wdt.whEventsCountMap["aliases"]; exists {
		for i := 0; i < alias; i++ {
			payloadAlias := strings.NewReader(fmt.Sprintf(`{
			"userId": "%s",
			"messageId":"%s",
			"type": "alias",
			"previousId": "name@surname.com",
			"context": {
				"traits": {
				   "trait1": "new-val"
				},
				"ip": "14.5.67.21",
				"library": {
					"name": "http"
				}
            }
		  }`, wdt.userId, uuid.Must(uuid.NewV4()).String()))
			SendEvent(payloadAlias, "alias", wdt.writeKey)
		}
	}

	// Sending group event
	if group, exists := wdt.whEventsCountMap["groups"]; exists {
		for i := 0; i < group; i++ {
			payloadGroup := strings.NewReader(fmt.Sprintf(`{
			"userId": "%s",
			"messageId":"%s",
			"type": "group",
			"groupId": "groupId",
			"traits": {
			  "name": "MyGroup",
			  "industry": "IT",
			  "employees": 450,
			  "plan": "basic"
			},
			"context": {
				"traits": {
					"trait1": "new-val"
				},
				"ip": "14.5.67.21",
				"library": {
					"name": "http"
				}
			}
		  }`, wdt.userId, uuid.Must(uuid.NewV4()).String()))
			SendEvent(payloadGroup, "group", wdt.writeKey)
		}
	}
}

// whDestinationTest Checking warehouse destination
func whDestinationTest(t *testing.T, wdt *WareHouseDestinationTest) {
	whGatewayTest(t, wdt)
	whBatchRouterTest(t, wdt)
	whTablesTest(t, wdt)
}

// whGatewayTest Checking for gateway jobs
func whGatewayTest(t *testing.T, wdt *WareHouseDestinationTest) {
	require.Contains(t, wdt.whEventsCountMap, "gateway")
	gwEvents := wdt.whEventsCountMap["gateway"]

	// Checking for the gateway jobs
	t.Log("Checking for the gateway jobs")
	require.Eventually(t, func() bool {
		var count int64
		jobSqlStatement := fmt.Sprintf(`select count(*) from gw_jobs_for_user_id_and_write_key('%s', '%s') as job_ids`, wdt.userId, wdt.writeKey)
		err := db.QueryRow(jobSqlStatement).Scan(&count)
		require.Equal(t, err, nil)
		return count == int64(gwEvents)
	}, time.Minute, 10*time.Millisecond)

	// Getting gateway job ids
	var jobIds []string
	require.Eventually(t, func() bool {
		jobIds = make([]string, 0)
		jobSqlStatement := fmt.Sprintf(`select * from gw_jobs_for_user_id_and_write_key('%s', '%s') as job_ids`, wdt.userId, wdt.writeKey)
		rows, err := db.Query(jobSqlStatement)
		require.Equal(t, err, nil)

		defer rows.Close()
		for rows.Next() {
			var jobId int64
			err = rows.Scan(&jobId)
			require.Equal(t, err, nil)
			jobIds = append(jobIds, fmt.Sprint(jobId))
		}
		return gwEvents == len(jobIds)
	}, time.Minute, 10*time.Millisecond)

	// Checking for the gateway jobs state
	t.Log("Checking for the gateway jobs state")
	require.Eventually(t, func() bool {
		var count int64
		jobsSqlStatement := fmt.Sprintf("select count(*) from gw_job_status_1 where job_id in (%s) and job_state = 'succeeded'", strings.Join(jobIds, ","))
		err := db.QueryRow(jobsSqlStatement).Scan(&count)
		require.Equal(t, err, nil)
		return count == int64(gwEvents)
	}, time.Minute, 10*time.Millisecond)
}

// whBatchRouterTest Checking for batch router jobs
func whBatchRouterTest(t *testing.T, wdt *WareHouseDestinationTest) {
	require.Contains(t, wdt.whEventsCountMap, "batchRT")
	brtEvents := wdt.whEventsCountMap["batchRT"]

	// Checking for the batch router jobs
	require.Eventually(t, func() bool {
		var count int64
		jobsSqlStatement := fmt.Sprintf(`select count(*) from brt_jobs_for_user_id('%s') as job_ids`, wdt.userId)
		err := db.QueryRow(jobsSqlStatement).Scan(&count)
		require.Equal(t, err, nil)
		return count == int64(brtEvents)
	}, 2*time.Minute, 100*time.Millisecond)

	// Getting batch router job ids
	var jobIds []string
	require.Eventually(t, func() bool {
		jobIds = make([]string, 0)
		jobSqlStatement := fmt.Sprintf(`select * from brt_jobs_for_user_id('%s') as job_ids`, wdt.userId)
		rows, err := db.Query(jobSqlStatement)
		require.Equal(t, err, nil)

		defer rows.Close()
		for rows.Next() {
			var jobId int64
			err = rows.Scan(&jobId)
			require.Equal(t, err, nil)
			jobIds = append(jobIds, fmt.Sprint(jobId))
		}
		return brtEvents == len(jobIds)
	}, 2*time.Minute, 100*time.Millisecond)

	require.Eventually(t, func() bool {
		var count int64
		jobsSqlStatement := fmt.Sprintf("select count(*) from batch_rt_job_status_1 where job_id in (%s) and job_state = 'succeeded'", strings.Join(jobIds, ","))
		err := db.QueryRow(jobsSqlStatement).Scan(&count)
		require.Equal(t, err, nil)
		return count == int64(brtEvents)
	}, 2*time.Minute, 100*time.Millisecond)
}

// whTablesTest Checking warehouse
func whTablesTest(t *testing.T, wdt *WareHouseDestinationTest) {
	tables := []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"}
	primaryKeys := []string{"user_id", "id", "user_id", "user_id", "user_id", "user_id", "user_id", "user_id"}
	for idx, table := range tables {
		require.Contains(t, wdt.whEventsCountMap, table)
		tableCount := wdt.whEventsCountMap[table]
		require.Eventually(t, func() bool {
			var count int64
			sqlStatement := fmt.Sprintf("select count(*) from %s.%s where %s = '%s'", wdt.schema, table, primaryKeys[idx], wdt.userId)
			_ = wdt.db.QueryRow(sqlStatement).Scan(&count)
			return count == int64(tableCount)
		}, 2*time.Minute, 100*time.Millisecond)
	}
}

// initWHClickHouseClusterModeSetup Initialize cluster mode setup
func initWHClickHouseClusterModeSetup(t *testing.T) {
	chClusterTest := whTest.chClusterTest
	tables := []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"}

	// Rename tables to tables_shard
	for _, table := range tables {
		sqlStatement := fmt.Sprintf("RENAME TABLE %[1]s to %[1]s_shard ON CLUSTER rudder_cluster;", table)
		_, err := chClusterTest.db.Exec(sqlStatement)
		require.Equal(t, err, nil)
	}

	// Create distribution views for tables
	for _, table := range tables {
		sqlStatement := fmt.Sprintf("CREATE TABLE rudderdb.%[1]s ON CLUSTER 'rudder_cluster' AS rudderdb.%[1]s_shard ENGINE = Distributed('rudder_cluster', rudderdb, %[1]s_shard, cityHash64(concat(toString(received_at), id)));", table)
		_, err := chClusterTest.db.Exec(sqlStatement)
		require.Equal(t, err, nil)
	}
}