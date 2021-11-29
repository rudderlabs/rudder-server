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
	"encoding/json"
	"flag"
	"fmt"
	"github.com/gofrs/uuid"
	redigo "github.com/gomodule/redigo/redis"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"html/template"
	"io"
	"log"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
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
	_ "github.com/lib/pq"
	"github.com/minio/minio-go"
	"github.com/ory/dockertest"
	dc "github.com/ory/dockertest/docker"

	"github.com/phayes/freeport"
	main "github.com/rudderlabs/rudder-server"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
)

type PostgresDestConfig struct {
	PostgresDb       string
	PostgresPassword string
	PostgresUser     string
	PostgresHost     string
	PostgresSslMode  string
}

type PostgresTestT struct {
	resource   *dockertest.Resource
	destConfig *PostgresDestConfig
	db         *sql.DB
	writeKey   string
	setupDone  bool
}

type ClickHouseDestConfig struct {
	ClickHouseHost           string
	ClickHouseUser           string
	ClickHousePassword       string
	ClickHouseDb             string
	ClickHouseBlockSize      string
	ClickHousePoolSize       string
	ClickHouseQueryDebugLogs string
	ClickHouseReadTimeout    string
	ClickHouseWriteTimeout   string
	ClickHouseCompress       string
	ClickHouseSecure         string
	ClickHouseSkipVerify     string
	ClickHouseTlsConfigName  string
}

type ClickHouseTestT struct {
	resource   *dockertest.Resource
	destConfig *ClickHouseDestConfig
	db         *sql.DB
	writeKey   string
	setupDone  bool
}

type ClickHouseClusterTestT struct {
	network      *dc.Network
	zookeeper    *dockertest.Resource
	clickhouse01 *dockertest.Resource
	clickhouse02 *dockertest.Resource
	clickhouse03 *dockertest.Resource
	clickhouse04 *dockertest.Resource
	db           *sql.DB
	destConfig   *ClickHouseDestConfig
	writeKey     string
	setupDone    bool
}

type MSSqlDestConfig struct {
	MSSqlDb       string
	MSSqlPassword string
	MSSqlUser     string
	MSSqlHost     string
	MSSqlSslMode  string
}

type MSSqlTestT struct {
	resource   *dockertest.Resource
	db         *sql.DB
	destConfig *MSSqlDestConfig
	writeKey   string
	setupDone  bool
}

type WareHouseEventsCount struct {
	identify int64
	track    int64
	page     int64
	screen   int64
	alias    int64
	group    int64
	gateway  int64
	batchRT  int64
}

type WareHouseTestT struct {
	pgTestT                *PostgresTestT
	chTestT                *ClickHouseTestT
	chClusterTestT         *ClickHouseClusterTestT
	mssqlTestT             *MSSqlTestT
	gwJobsSqlFunction      string
	batchRtJobsSqlFunction string
	sqlFunctionsAdded      bool
}

var (
	pool                  *dockertest.Pool
	err                   error
	z                     *dockertest.Resource
	resourceKafka         *dockertest.Resource
	resourceRedis         *dockertest.Resource
	transformerRes        *dockertest.Resource
	resource              *dockertest.Resource
	network               *dc.Network
	resourcePostgres      *dockertest.Resource
	transformURL          string
	minioEndpoint         string
	minioBucketName       string
	hold                  bool = true
	db                    *sql.DB
	redisClient           *redis.Client
	DB_DSN                = "root@tcp(127.0.0.1:3306)/service"
	httpPort              string
	httpKafkaPort         string
	dbHandle              *sql.DB
	sourceJSON            backendconfig.ConfigT
	webhookurl            string
	webhookDestinationurl string
	webhook               *WebhookRecorder
	webhookDestination    *WebhookRecorder
	address               string
	runIntegration        bool
	writeKey              string
	webhookEventWriteKey  string
	workspaceID           string
	redisAddress          string
	brokerPort            string
	localhostPort         string
	localhostPortInt      int
	whTest                *WareHouseTestT
	EventID               string
	VersionID             string
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

func SendEvent(payload *strings.Reader, callType string, writeKey string) (string, error) {
	log.Println(fmt.Sprintf("Sending %s Event", callType))
	url := ""
	if callType != "beacon" {
		url = fmt.Sprintf("http://localhost:%s/v1/%s", httpPort, callType)
	} else {
		url = fmt.Sprintf("http://localhost:%s/beacon/v1/batch?writeKey=%s", httpPort, writeKey)
	}
	method := "POST"
	client := &http.Client{}
	req, err := http.NewRequest(method, url, payload)

	if err != nil {
		log.Println(err)
		return "", err
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
		return "", err
	}
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		log.Println(err)
		return "", err
	}
	log.Println(string(body))
	log.Println("Event Sent Successfully")
	return string(body), err
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

	client := &http.Client{}
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

func SendPixelEvents() {
	// Send pixel/v1/page
	log.Println("Sending pixel/v1/page Event")
	url := fmt.Sprintf("http://localhost:%s/pixel/v1/page?writeKey=%s&anonymousId=identified_user_id", httpPort, writeKey)
	method := "GET"
	resBody, _ := GetEvent(url, method)
	log.Println(resBody)
	log.Println("pixel/v1/page Event Sent Successfully")

	// Send pixel/v1/track
	log.Println("Sending pixel/v1/track Event")
	url = fmt.Sprintf("http://localhost:%s/pixel/v1/track?writeKey=%s&anonymousId=identified_user_id&event=product_reviewed_again", httpPort, writeKey)
	method = "GET"
	resBody, _ = GetEvent(url, method)
	log.Println(resBody)
	log.Println("pixel/v1/track Event Sent Successfully")
}

func GetEvent(url string, method string) (string, error) {
	client := &http.Client{}
	req, err := http.NewRequest(method, url, nil)

	if err != nil {
		fmt.Println(err)
		return "", err
	}
	req.Header.Add("Authorization", "Basic cnVkZGVyOnBhc3N3b3Jk")
	res, err := client.Do(req)
	if err != nil {
		fmt.Println(err)
		return "", err
	}
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		fmt.Println(err)
		return "", err
	}
	return string(body), err
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

	// Initialize warehouse config
	initWhConfig()

	// Setting postgres destination
	SetWhPostgresDestination()
	defer func() {
		pgTestT := whTest.pgTestT
		if pgTestT.resource != nil {
			if err := pool.Purge(pgTestT.resource); err != nil {
				log.Printf("Could not purge warehouse postgres resource: %s \n", err)
			}
		}
	}()

	// Setting clickhouse destination
	SetWhClickHouseDestination()
	defer func() {
		chTestT := whTest.chTestT
		if chTestT.resource != nil {
			if err := pool.Purge(chTestT.resource); err != nil {
				log.Printf("Could not purge resource: %s \n", err)
			}
		}
	}()

	// Setting clickhouse cluster destination
	SetWhClickHouseClusterDestination()
	defer func() {
		chClusterTestT := whTest.chClusterTestT
		if chClusterTestT.zookeeper != nil {
			if err := pool.Purge(chClusterTestT.zookeeper); err != nil {
				log.Printf("Could not purge resource: %s \n", err)
			}
		}
		if chClusterTestT.clickhouse01 != nil {
			if err := pool.Purge(chClusterTestT.clickhouse01); err != nil {
				log.Printf("Could not purge resource: %s \n", err)
			}
		}
		if chClusterTestT.clickhouse02 != nil {
			if err := pool.Purge(chClusterTestT.clickhouse02); err != nil {
				log.Printf("Could not purge resource: %s \n", err)
			}
		}
		if chClusterTestT.clickhouse03 != nil {
			if err := pool.Purge(chClusterTestT.clickhouse03); err != nil {
				log.Printf("Could not purge resource: %s \n", err)
			}
		}
		if chClusterTestT.clickhouse04 != nil {
			if err := pool.Purge(chClusterTestT.clickhouse04); err != nil {
				log.Printf("Could not purge resource: %s \n", err)
			}
		}
		if chClusterTestT.network != nil {
			if err := pool.Client.RemoveNetwork(chClusterTestT.network.ID); err != nil {
				log.Printf("Could not remove network: %s \n", err)
			}
		}
	}()

	// Setting mssql destination
	SetWhMSSqlDestination()
	defer func() {
		mssqlTestT := whTest.mssqlTestT
		if mssqlTestT.resource != nil {
			if err := pool.Purge(mssqlTestT.resource); err != nil {
				log.Printf("Could not purge resource: %s \n", err)
			}
		}
	}()

	// Adding necessary sql functions which needs to be used during warehouse testing
	AddSqlFunctionsToJobsDb()

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
	fmt.Println("writeKey", writeKey)

	workspaceConfigPath := createWorkspaceConfig(
		"testdata/workspaceConfigTemplate.json",
		map[string]string{
			"webhookUrl":                          webhookurl,
			"webhookDestinationurl":               webhookDestinationurl,
			"writeKey":                            writeKey,
			"webhookEventWriteKey":                webhookEventWriteKey,
			"postgresEventWriteKey":               whTest.pgTestT.writeKey,
			"clickHouseEventWriteKey":             whTest.chTestT.writeKey,
			"clickHouseClusterEventWriteKey":      whTest.chClusterTestT.writeKey,
			"mssqlEventWriteKey":                  whTest.mssqlTestT.writeKey,
			"workspaceId":                         workspaceID,
			"postgresPort":                        resourcePostgres.GetPort("5432/tcp"),
			"rwhPostgresDestinationPort":          whTest.pgTestT.resource.GetPort("5432/tcp"),
			"rwhClickHouseDestinationPort":        whTest.chTestT.resource.GetPort("9000/tcp"),
			"rwhClickHouseClusterDestinationPort": whTest.chClusterTestT.clickhouse01.GetPort("9000/tcp"),
			"rwhMSSqlDestinationPort":             whTest.mssqlTestT.resource.GetPort("1433/tcp"),
			"address":                             redisAddress,
			"minioEndpoint":                       minioEndpoint,
			"minioBucketName":                     minioBucketName,
			"kafkaPort":                           strconv.Itoa(localhostPortInt),
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
	os.Setenv("RSERVER_WAREHOUSE_WAREHOUSE_SYNC_FREQ_IGNORE", "true")
	os.Setenv("RSERVER_WAREHOUSE_UPLOAD_FREQ_IN_S", "10")
	os.Setenv("RSERVER_EVENT_SCHEMAS_ENABLE_EVENT_SCHEMAS_FEATURE", "true")
	os.Setenv("RSERVER_EVENT_SCHEMAS_SYNC_INTERVAL", "15")

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
		"userId": "identified_user_id",
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
	SendEvent(payload_1, "identify", writeKey)
	payload_2 := strings.NewReader(`{
		"userId": "identified_user_id",
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
	SendEvent(payload_2, "identify", writeKey) //sending duplicate event to check dedup
	// Sending Batch event
	payload_Batch := strings.NewReader(`{
		"batch":
		[
			{
				"userId": "identified_user_id",
			   "anonymousId":"anonymousId_1",
			   "messageId":"messageId_1"
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
	SendEvent(payload_track, "track", writeKey)

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
	SendEvent(payload_page, "page", writeKey)

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
	SendEvent(payload_screen, "screen", writeKey)

	// Sending alias event
	payload_alias := strings.NewReader(`{
		"userId": "identified_user_id",
		"anonymousId":"anonymousId_1",
		"messageId":"messageId_1",
		"type": "alias",
		"previousId": "name@surname.com",
		"userId": "12345"
	  }`)
	SendEvent(payload_alias, "alias", writeKey)

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
	SendEvent(payload_group, "group", writeKey)
	SendWebhookEvent()
	SendPixelEvents()

	require.Eventually(t, func() bool {
		return len(webhook.Requests()) == 10
	}, time.Minute, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		return len(webhookDestination.Requests()) == 1
	}, time.Minute, 10*time.Millisecond)

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
		return myEvent.count == "2"
	}, time.Minute, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		eventSql := "select count(*) from dev_integration_test_1.screens"
		db.QueryRow(eventSql).Scan(&myEvent.count)
		return myEvent.count == "1"
	}, time.Minute, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		eventSql := "select count(*) from dev_integration_test_1.tracks"
		db.QueryRow(eventSql).Scan(&myEvent.count)
		return myEvent.count == "2"
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
	resBody, _ := SendEvent(payload, "audiencelist", writeKey)
	require.Equal(t, resBody, "OK")
}

type eventSchemasObject struct {
	EventID   string
	EventType string
	VersionID string
}

// Verify Event Models EndPoint
func TestEventModels(t *testing.T) {
	// GET /schemas/event-models
	url := fmt.Sprintf("http://localhost:%s/schemas/event-models", httpPort)
	method := "GET"
	resBody, _ := GetEvent(url, method)
	require.Eventually(t, func() bool {
		// Similarly, pole until the Event Schema Tables are updated
		resBody, _ = GetEvent(url, method)
		return resBody != "[]"
	}, time.Minute, 10*time.Millisecond)
	// log.Println(resBody)
	require.NotEqual(t, resBody, "[]")
	b := []byte(resBody)
	var eventSchemas []eventSchemasObject

	err := json.Unmarshal(b, &eventSchemas)
	if err != nil {
		fmt.Println("error:", err)
	}
	fmt.Println("// loop over array of structs of shipObject")
	for k := range eventSchemas {
		if eventSchemas[k].EventType == "page" {
			EventID = eventSchemas[k].EventID
		}
	}
	if EventID == "" {
		fmt.Println("error: Page type EventID not found")
	}
	log.Println("Test Schemas Event ID", EventID)
}

// Verify Event Versions EndPoint
func TestEventVersions(t *testing.T) {
	// GET /schemas/event-versions
	url := fmt.Sprintf("http://localhost:%s/schemas/event-versions?EventID=%s", httpPort, EventID)
	method := "GET"
	resBody, _ := GetEvent(url, method)
	require.Contains(t, resBody, EventID)

	b := []byte(resBody)
	var eventSchemas []eventSchemasObject

	err := json.Unmarshal(b, &eventSchemas)
	if err != nil {
		fmt.Println("error:", err)
	}
	VersionID = eventSchemas[0].VersionID
	log.Println("Test Schemas Event ID's VersionID: ", VersionID)
}

// Verify schemas/event-model/{EventID}/key-counts EndPoint
func TestEventModelKeyCounts(t *testing.T) {
	// GET schemas/event-model/{EventID}/key-counts
	url := fmt.Sprintf("http://localhost:%s/schemas/event-model/%s/key-counts", httpPort, EventID)
	method := "GET"
	resBody, _ := GetEvent(url, method)
	require.Contains(t, resBody, "messageId")
}

// Verify /schemas/event-model/{EventID}/metadata EndPoint
func TestEventModelMetadata(t *testing.T) {
	// GET /schemas/event-model/{EventID}/metadata
	url := fmt.Sprintf("http://localhost:%s/schemas/event-model/%s/metadata", httpPort, EventID)
	method := "GET"
	resBody, _ := GetEvent(url, method)
	require.Contains(t, resBody, "messageId")
}

// Verify /schemas/event-version/{VersionID}/metadata EndPoint
func TestEventVersionMetadata(t *testing.T) {
	// GET /schemas/event-version/{VersionID}/metadata
	url := fmt.Sprintf("http://localhost:%s/schemas/event-version/%s/metadata", httpPort, VersionID)
	method := "GET"
	resBody, _ := GetEvent(url, method)
	require.Contains(t, resBody, "messageId")
}

// Verify /schemas/event-version/{VersionID}/missing-keys EndPoint
func TestEventVersionMissingKeys(t *testing.T) {
	// GET /schemas/event-version/{VersionID}/metadata
	url := fmt.Sprintf("http://localhost:%s/schemas/event-version/%s/missing-keys", httpPort, VersionID)
	method := "GET"
	resBody, _ := GetEvent(url, method)
	log.Println("EventID", EventID)
	log.Println("VersionID", VersionID)
	log.Println(resBody)
	require.Contains(t, resBody, "originalTimestamp")
	require.Contains(t, resBody, "sentAt")
	require.Contains(t, resBody, "channel")
	require.Contains(t, resBody, "integrations.All")
}

// Verify /schemas/event-models/json-schemas EndPoint
func TestEventModelsJsonSchemas(t *testing.T) {
	// GET /schemas/event-models/json-schemas
	url := fmt.Sprintf("http://localhost:%s/schemas/event-models/json-schemas", httpPort)
	method := "GET"
	resBody, _ := GetEvent(url, method)
	require.Eventually(t, func() bool {
		// Similarly, pole until the Event Schema Tables are updated
		resBody, _ = GetEvent(url, method)
		return resBody != "[]"
	}, time.Minute, 10*time.Millisecond)
	require.NotEqual(t, resBody, "[]")
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
	expectedCount := 10
out:
	for {
		select {
		case msg := <-consumer:
			msgCount++
			// t.Log("Received messages", string(msg.Key), string(msg.Value))
			require.Equal(t, "identified_user_id", string(msg.Key))
			// require.Contains(t, string(msg.Value), "new-val")
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

// Verify beacon  EndPoint
func TestBeaconBatch(t *testing.T) {
	payload := strings.NewReader(`{
		"batch":
		[
			{
			   "userId": "identified_user_id",
			   "anonymousId":"anonymousId_1",
			   "messageId":"messageId_1"
			}
		]
	}`)
	resBody, _ := SendEvent(payload, "beacon", writeKey)
	require.Equal(t, resBody, "OK")
}

// Verify Event in WareHouse Postgres
func TestWhPostgresDestination(t *testing.T) {
	pgTest := whTest.pgTestT
	if !pgTest.setupDone {
		log.Println("Setup not initialized for postgres destination")
		return
	}
	userId := "userId_postgres"

	// Sending warehouse events
	whEventsCount := sendWHEvents(pgTest.writeKey, userId)

	// Checking for gateway jobs
	testGWForWareHouseDestination(t, pgTest.writeKey, userId, whEventsCount.gateway)

	// Checking for batch router jobs
	testBatchRouterForWareHouseDestination(t, userId, whEventsCount.batchRT)

	// Checking for warehouse
	testWareHouseForWareHouseDestination(t, pgTest.db, "postgres_wh_integration", whEventsCount)
}

// Verify Event in WareHouse ClickHouse
func TestWhClickHouseDestination(t *testing.T) {
	chTestT := whTest.chTestT
	if !chTestT.setupDone {
		log.Println("Setup not initialized for clickhouse destination")
		return
	}

	userId := "userId_clickhouse"

	// Sending warehouse events
	whEventsCount := sendWHEvents(chTestT.writeKey, userId)

	// Checking for gateway jobs
	testGWForWareHouseDestination(t, chTestT.writeKey, userId, whEventsCount.gateway)

	// Checking for batch router jobs
	testBatchRouterForWareHouseDestination(t, userId, whEventsCount.batchRT)

	// Checking for warehouse
	testWareHouseForWareHouseDestination(t, chTestT.db, "rudderdb", whEventsCount)
}

// Verify Event in WareHouse ClickHouse Cluster
func TestWhClickHouseClusterDestination(t *testing.T) {
	chClusterTestT := whTest.chClusterTestT
	if !chClusterTestT.setupDone {
		log.Println("Setup not initialized for clickhouse cluster destination")
		return
	}

	userId := "userId_clickhouse_cluster"

	// Sending warehouse events
	whEventsCount := sendWHEvents(chClusterTestT.writeKey, userId)

	// Checking for gateway jobs
	testGWForWareHouseDestination(t, chClusterTestT.writeKey, userId, whEventsCount.gateway)

	// Checking for batch router jobs
	testBatchRouterForWareHouseDestination(t, userId, whEventsCount.batchRT)

	// Checking for warehouse
	testWareHouseForWareHouseDestination(t, chClusterTestT.db, "rudderdb", whEventsCount)

	// Making changes for distribution views.
	var sqlStatement string
	sqlStatement = fmt.Sprint("rename table tracks to tracks_shard on cluster rudder_cluster;")
	_, err = chClusterTestT.db.Exec(sqlStatement)
	require.Equal(t, err, nil)

	sqlStatement = fmt.Sprint("rename table product_track to product_track_shard on cluster rudder_cluster;")
	_, err = chClusterTestT.db.Exec(sqlStatement)
	require.Equal(t, err, nil)

	sqlStatement = fmt.Sprint("CREATE TABLE rudderdb.tracks ON CLUSTER 'rudder_cluster' AS rudderdb.tracks_shard ENGINE = Distributed('rudder_cluster', rudderdb, tracks_shard, cityHash64(id));")
	_, err = chClusterTestT.db.Exec(sqlStatement)
	require.Equal(t, err, nil)

	sqlStatement = fmt.Sprint("CREATE TABLE rudderdb.product_track ON CLUSTER 'rudder_cluster' AS rudderdb.product_track_shard ENGINE = Distributed('rudder_cluster', rudderdb, product_track_shard, cityHash64(id));")
	_, err = chClusterTestT.db.Exec(sqlStatement)
	require.Equal(t, err, nil)

	// Sending warehouse events
	clusterEventCount := int64(6)
	for i := 0; i < int(clusterEventCount); i++ {
		sendWHEvents(chClusterTestT.writeKey, userId)
	}

	// Checking for tracks table
	require.Eventually(t, func() bool {
		var count int64
		sqlStatement := fmt.Sprintf("select count(*) from rudderdb.tracks")
		_ = chClusterTestT.db.QueryRow(sqlStatement).Scan(&count)
		return count == 2*(whEventsCount.track+clusterEventCount)
	}, 2*time.Minute, 100*time.Millisecond)

	// Checking product reviewed table
	require.Eventually(t, func() bool {
		var count int64
		sqlStatement := fmt.Sprintf("select count(*) from rudderdb.product_track")
		_ = chClusterTestT.db.QueryRow(sqlStatement).Scan(&count)
		return count == 2*(whEventsCount.track+clusterEventCount)
	}, 2*time.Minute, 100*time.Millisecond)
}

// Verify Event in WareHouse MSSQL
func TestWhMsSqlDestination(t *testing.T) {
	mssqlTestT := whTest.mssqlTestT
	if !mssqlTestT.setupDone {
		log.Println("Setup not initialized for mssql destination")
		return
	}

	userId := "userId_mssql"

	// Sending warehouse events
	whEventsCount := sendWHEvents(mssqlTestT.writeKey, userId)

	// Checking for gateway jobs
	testGWForWareHouseDestination(t, mssqlTestT.writeKey, userId, whEventsCount.gateway)

	// Checking for batch router jobs
	testBatchRouterForWareHouseDestination(t, userId, whEventsCount.batchRT)

	// Checking for warehouse
	testWareHouseForWareHouseDestination(t, mssqlTestT.db, "mssql_wh_integration", whEventsCount)
}

func sendWHEvents(writeKey string, userId string) *WareHouseEventsCount {
	identifyEventsCount, tracksEventsCount, pageEventsCount, screenEventsCount, aliasEventsCount, groupEventsCount := 1, 1, 1, 1, 1, 1
	gwEventsCount := identifyEventsCount + tracksEventsCount + pageEventsCount + screenEventsCount + aliasEventsCount + groupEventsCount
	batchRtEventsCountEvents := 2*identifyEventsCount + 2*tracksEventsCount + pageEventsCount + screenEventsCount + aliasEventsCount + groupEventsCount

	// Sending identify event
	for i := 0; i < identifyEventsCount; i++ {
		payloadIdentify := strings.NewReader(fmt.Sprintf(`{
			"userId": "%s",
			"messageId":"%s",
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
		  }`, userId, uuid.Must(uuid.NewV4()).String()))
		_, _ = SendEvent(payloadIdentify, "identify", writeKey)
	}

	// Sending track event
	for i := 0; i < tracksEventsCount; i++ {
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
		  }`, userId, uuid.Must(uuid.NewV4()).String()))
		_, _ = SendEvent(payloadTrack, "track", writeKey)
	}

	// Sending page event
	for i := 0; i < pageEventsCount; i++ {
		payloadPage := strings.NewReader(fmt.Sprintf(`{
			"userId": "%s",
			"messageId":"%s",
			"type": "page",
			"name": "Home",
			"properties": {
			  "title": "Home | RudderStack",
			  "url": "http://www.rudderstack.com"
			}
		  }`, userId, uuid.Must(uuid.NewV4()).String()))
		_, _ = SendEvent(payloadPage, "page", writeKey)
	}

	// Sending screen event
	for i := 0; i < screenEventsCount; i++ {
		payloadScreen := strings.NewReader(fmt.Sprintf(`{
			"userId": "%s",
			"messageId":"%s",
			"type": "screen",
			"name": "Main",
			"properties": {
			  "prop_key": "prop_value"
			}
		  }`, userId, uuid.Must(uuid.NewV4()).String()))
		_, _ = SendEvent(payloadScreen, "screen", writeKey)
	}

	// Sending alias event
	for i := 0; i < aliasEventsCount; i++ {
		payloadAlias := strings.NewReader(fmt.Sprintf(`{
			"userId": "%s",
			"messageId":"%s",
			"type": "alias",
			"previousId": "name@surname.com",
			"userId": "12345"
		  }`, userId, uuid.Must(uuid.NewV4()).String()))
		_, _ = SendEvent(payloadAlias, "alias", writeKey)
	}

	// Sending group event
	for i := 0; i < groupEventsCount; i++ {
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
		  }`, userId, uuid.Must(uuid.NewV4()).String()))
		_, _ = SendEvent(payloadGroup, "group", writeKey)
	}

	return &WareHouseEventsCount{
		identify: int64(identifyEventsCount),
		track:    int64(tracksEventsCount),
		page:     int64(pageEventsCount),
		screen:   int64(screenEventsCount),
		alias:    int64(aliasEventsCount),
		group:    int64(groupEventsCount),
		gateway:  int64(gwEventsCount),
		batchRT:  int64(batchRtEventsCountEvents),
	}
}

func testGWForWareHouseDestination(t *testing.T, writeKey string, userId string, eventsCount int64) {
	if !whTest.sqlFunctionsAdded {
		log.Println("WareHouse sqlFunctionsAdded not set")
		return
	}

	// Getting gateway job ids
	var jobIds []string
	require.Eventually(t, func() bool {
		jobIds = make([]string, 0)
		jobSqlStatement := fmt.Sprintf(`select * from gw_jobs('%s', '%s') as job_ids`, writeKey, userId)
		var rows *sql.Rows
		var err error
		rows, err = db.Query(jobSqlStatement)
		if err != nil {
			return false
		}
		defer rows.Close()
		for rows.Next() {
			var jobId int64
			err = rows.Scan(&jobId)
			if err != nil {
				return false
			}
			jobIds = append(jobIds, fmt.Sprint(jobId))
		}
		return int(eventsCount) == len(jobIds)
	}, time.Minute, 10*time.Millisecond)

	// Checking for the gateway jobs state
	require.Eventually(t, func() bool {
		var count int64
		jobsSqlStatement := fmt.Sprintf("select count(*) from gw_job_status_1 where job_id in (%s) and job_state = 'succeeded'", strings.Join(jobIds, ","))
		_ = db.QueryRow(jobsSqlStatement).Scan(&count)
		return count == eventsCount
	}, time.Minute, 10*time.Millisecond)
}

func testBatchRouterForWareHouseDestination(t *testing.T, userId string, eventsCount int64) {
	if !whTest.sqlFunctionsAdded {
		log.Println("WareHouse sqlFunctionsAdded not set")
		return
	}

	// Getting batch router job ids
	var jobIds []string
	require.Eventually(t, func() bool {
		jobIds = make([]string, 0)
		jobSqlStatement := fmt.Sprintf(`select * from batch_rt_jobs('%s') as job_ids`, userId)
		var rows *sql.Rows
		var err error
		rows, err = db.Query(jobSqlStatement)
		if err != nil {
			return false
		}
		defer rows.Close()
		for rows.Next() {
			var jobId int64
			err = rows.Scan(&jobId)
			if err != nil {
				return false
			}
			jobIds = append(jobIds, fmt.Sprint(jobId))
		}
		return int(eventsCount) == len(jobIds)
	}, 2*time.Minute, 100*time.Millisecond)

	require.Eventually(t, func() bool {
		var count int64
		jobsSqlStatement := fmt.Sprintf("select count(*) from batch_rt_job_status_1 where job_id in (%s) and job_state = 'succeeded'", strings.Join(jobIds, ","))
		_ = db.QueryRow(jobsSqlStatement).Scan(&count)
		return count == eventsCount
	}, 2*time.Minute, 100*time.Millisecond)
}

func testWareHouseForWareHouseDestination(t *testing.T, db *sql.DB, schema string, whEventsCount *WareHouseEventsCount) {
	// Checking identify table
	require.Eventually(t, func() bool {
		var count int64
		sqlStatement := fmt.Sprintf("select count(*) from %s.identify", schema)
		_ = db.QueryRow(sqlStatement).Scan(&count)
		return count == whEventsCount.identify
	}, 5*time.Minute, 100*time.Millisecond)

	// Checking for tracks table
	require.Eventually(t, func() bool {
		var count int64
		sqlStatement := fmt.Sprintf("select count(*) from %s.tracks", schema)
		_ = db.QueryRow(sqlStatement).Scan(&count)
		return count == whEventsCount.track
	}, 5*time.Minute, 100*time.Millisecond)
	require.Eventually(t, func() bool {
		var count int64
		sqlStatement := fmt.Sprintf("select count(*) from %s.product_track", schema)
		_ = db.QueryRow(sqlStatement).Scan(&count)
		return count == whEventsCount.track
	}, 5*time.Minute, 100*time.Millisecond)

	// Checking page table
	require.Eventually(t, func() bool {
		var count int64
		sqlStatement := fmt.Sprintf("select count(*) from %s.page", schema)
		_ = db.QueryRow(sqlStatement).Scan(&count)
		return count == whEventsCount.page
	}, 5*time.Minute, 100*time.Millisecond)

	// Checking screen table
	require.Eventually(t, func() bool {
		var count int64
		sqlStatement := fmt.Sprintf("select count(*) from %s.screen", schema)
		_ = db.QueryRow(sqlStatement).Scan(&count)
		return count == whEventsCount.screen
	}, 5*time.Minute, 100*time.Millisecond)

	// Checking alias table
	require.Eventually(t, func() bool {
		var count int64
		sqlStatement := fmt.Sprintf("select count(*) from %s.alias", schema)
		_ = db.QueryRow(sqlStatement).Scan(&count)
		return count == whEventsCount.alias
	}, 5*time.Minute, 100*time.Millisecond)

	// Checking group table
	require.Eventually(t, func() bool {
		var count int64
		sqlStatement := fmt.Sprintf("select count(*) from %s.group", schema)
		_ = db.QueryRow(sqlStatement).Scan(&count)
		return count == whEventsCount.group
	}, 5*time.Minute, 100*time.Millisecond)
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
					// log.Println("Got message on topic ", topic, msg.Value)
				}
			}
		}(topic, consumer)
	}

	return consumers, errors
}

func SetZookeeper() {
	network, err = pool.Client.CreateNetwork(dc.CreateNetworkOptions{Name: "kafka_network"})
	if err != nil {
		log.Printf("Could not create docker network: %s", err)
	}
	zookeeperPortInt, err := freeport.GetFreePort()
	if err != nil {
		fmt.Println(err)
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
		fmt.Println(err)
	}

}

func SetKafka() {
	// Set Kafka: pulls an image, creates a container based on it and runs it
	KAFKA_ZOOKEEPER_CONNECT := fmt.Sprintf("KAFKA_ZOOKEEPER_CONNECT= zookeeper:%s", z.GetPort("2181/tcp"))
	log.Println("KAFKA_ZOOKEEPER_CONNECT:", KAFKA_ZOOKEEPER_CONNECT)

	brokerPortInt, err := freeport.GetFreePort()
	if err != nil {
		fmt.Println(err)
	}
	brokerPort = fmt.Sprintf("%s/tcp", strconv.Itoa(brokerPortInt))
	log.Println("broker Port:", brokerPort)

	localhostPortInt, err = freeport.GetFreePort()
	if err != nil {
		fmt.Println(err)
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
		fmt.Println(err)
	}
	log.Println("Kafka PORT:- ", resourceKafka.GetPort("9092/tcp"))
}

func SetRedis() {
	// pulls an redis image, creates a container based on it and runs it
	resourceRedis, err = pool.Run("redis", "alpine3.14", []string{"requirepass=secret"})
	if err != nil {
		log.Printf("Could not start resource: %s", err)
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
		log.Printf("Could not connect to docker: %s", err)
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
		fmt.Println(err)
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
		log.Printf("Could not connect to docker: %s", err)
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

func initWhConfig() {
	whTest = &WareHouseTestT{}
	whTest.gwJobsSqlFunction = `CREATE OR REPLACE FUNCTION gw_jobs(write_key varchar, user_id varchar)
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
	whTest.batchRtJobsSqlFunction = `CREATE OR REPLACE FUNCTION batch_rt_jobs(user_id varchar)
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

	whTest.pgTestT = &PostgresTestT{
		writeKey: randString(27),
		destConfig: &PostgresDestConfig{
			PostgresDb:       "rudderdb",
			PostgresPassword: "rudder-password",
			PostgresUser:     "rudder",
			PostgresHost:     "localhost",
			PostgresSslMode:  "disable",
		},
	}
	whTest.chTestT = &ClickHouseTestT{
		writeKey: randString(27),
		destConfig: &ClickHouseDestConfig{
			ClickHouseHost:           "localhost",
			ClickHouseUser:           "rudder",
			ClickHousePassword:       "rudder-password",
			ClickHouseDb:             "rudderdb",
			ClickHouseBlockSize:      "1000000",
			ClickHousePoolSize:       "100",
			ClickHouseQueryDebugLogs: "false",
			ClickHouseReadTimeout:    "300",
			ClickHouseWriteTimeout:   "1800",
			ClickHouseCompress:       "true",
			ClickHouseSecure:         "false",
			ClickHouseSkipVerify:     "true",
			ClickHouseTlsConfigName:  "",
		},
	}
	whTest.chClusterTestT = &ClickHouseClusterTestT{
		writeKey: randString(27),
		destConfig: &ClickHouseDestConfig{
			ClickHouseHost:           "localhost",
			ClickHouseUser:           "rudder",
			ClickHousePassword:       "rudder-password",
			ClickHouseDb:             "rudderdb",
			ClickHouseBlockSize:      "1000000",
			ClickHousePoolSize:       "100",
			ClickHouseQueryDebugLogs: "false",
			ClickHouseReadTimeout:    "300",
			ClickHouseWriteTimeout:   "1800",
			ClickHouseCompress:       "true",
			ClickHouseSecure:         "false",
			ClickHouseSkipVerify:     "true",
			ClickHouseTlsConfigName:  "",
		},
	}
	whTest.mssqlTestT = &MSSqlTestT{
		writeKey: randString(27),
		destConfig: &MSSqlDestConfig{
			MSSqlDb:       "master",
			MSSqlPassword: "reallyStrongPwd123",
			MSSqlUser:     "SA",
			MSSqlHost:     "localhost",
			MSSqlSslMode:  "disable",
		},
	}
}

func SetWhPostgresDestination() {
	pgTest := whTest.pgTestT
	destConfig := pgTest.destConfig

	pgTest.resource, err = pool.Run("postgres", "11-alpine", []string{
		fmt.Sprintf("POSTGRES_DB=%s", destConfig.PostgresDb),
		fmt.Sprintf("POSTGRES_PASSWORD=%s", destConfig.PostgresPassword),
		fmt.Sprintf("POSTGRES_USER=%s", destConfig.PostgresUser),
	})
	if err != nil {
		log.Printf("Could not start resource WareHouse Postgres: %v\n", err)
		return
	}

	// Getting at which port the container is running
	PostgresPort := pgTest.resource.GetPort("5432/tcp")

	// Creating url string for connection
	whPostgresUrl := fmt.Sprintf("user=%v password=%v host=%v port=%v dbname=%v sslmode=%v",
		destConfig.PostgresUser,
		destConfig.PostgresPassword,
		destConfig.PostgresHost,
		PostgresPort,
		destConfig.PostgresDb,
		destConfig.PostgresSslMode,
	)
	fmt.Println("whPostgresUrl:", whPostgresUrl)

	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	if err := pool.Retry(func() error {
		var err error
		pgTest.db, err = sql.Open("postgres", whPostgresUrl)
		if err != nil {
			return err
		}
		return pgTest.db.Ping()
	}); err != nil {
		log.Printf("Could not connect to warehouse postgres url: %v, with error: %v\n", whPostgresUrl, err)
	}
	if err != nil {
		return
	}

	pgTest.setupDone = true
}

func SetWhClickHouseDestination() {
	chTestT := whTest.chTestT
	destConfig := chTestT.destConfig

	chTestT.resource, err = pool.Run("yandex/clickhouse-server", "21-alpine", []string{
		fmt.Sprintf("CLICKHOUSE_DB=%s", destConfig.ClickHouseDb),
		fmt.Sprintf("CLICKHOUSE_PASSWORD=%s", destConfig.ClickHousePassword),
		fmt.Sprintf("CLICKHOUSE_USER=%s", destConfig.ClickHouseUser),
	})
	if err != nil {
		log.Println("Could not start resource ClickHouse: %w", err)
		return
	}

	// Getting at which port the container is running
	ClickHousePort := chTestT.resource.GetPort("9000/tcp")

	// Creating url string for connection
	whClickHouseUrl := fmt.Sprintf("tcp://%s:%s?&username=%s&password=%s&database=%s&block_size=%s&pool_size=%s&debug=%s&secure=%s&skip_verify=%s&tls_config=%s&read_timeout=%s&write_timeout=%s&compress=%s",
		destConfig.ClickHouseHost,
		ClickHousePort,
		destConfig.ClickHouseUser,
		destConfig.ClickHousePassword,
		destConfig.ClickHouseDb,
		destConfig.ClickHouseBlockSize,
		destConfig.ClickHousePoolSize,
		destConfig.ClickHouseQueryDebugLogs,
		destConfig.ClickHouseSecure,
		destConfig.ClickHouseSkipVerify,
		destConfig.ClickHouseTlsConfigName,
		destConfig.ClickHouseReadTimeout,
		destConfig.ClickHouseWriteTimeout,
		destConfig.ClickHouseCompress,
	)
	fmt.Println("whClickHouseUrl:", whClickHouseUrl)

	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	if err := pool.Retry(func() error {
		var err error
		chTestT.db, err = sql.Open("clickhouse", whClickHouseUrl)
		if err != nil {
			return err
		}
		return chTestT.db.Ping()
	}); err != nil {
		log.Println("Could not connect to postgres", whClickHouseUrl, err)
	}
	if err != nil {
		return
	}

	chTestT.setupDone = true
}

func SetWhClickHouseClusterDestination() {
	chClusterTestT := whTest.chClusterTestT
	destConfig := chClusterTestT.destConfig

	pwd, err := os.Getwd()
	if err != nil {
		log.Println("Could not get working directory: %w", err)
		return
	}

	if chClusterTestT.network, err = pool.Client.CreateNetwork(dc.CreateNetworkOptions{
		Name: "clickhouse-network",
		IPAM: &dc.IPAMOptions{
			Config: []dc.IPAMConfig{
				{
					Subnet: "172.23.0.0/24",
				},
			},
		},
	}); err != nil {
		log.Println("Could not create clickhouse cluster network: %w", err)
		return
	}

	if chClusterTestT.zookeeper, err = pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "zookeeper",
		Tag:        "3.5",
		Hostname:   "clickhouse-zookeeper",
		Name:       "clickhouse-zookeeper",
	}); err != nil {
		log.Println("Could not create clickhouse cluster zookeeper: %w", err)
		return
	}

	if chClusterTestT.clickhouse01, err = pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "yandex/clickhouse-server",
		Tag:        "21-alpine",
		Hostname:   "clickhouse01",
		Name:       "clickhouse01",
		PortBindings: map[dc.Port][]dc.PortBinding{
			"8123": {{HostIP: "127.0.0.1", HostPort: "8123"}},
			"9000": {{HostIP: "127.0.0.1", HostPort: "9000"}},
		},
		ExposedPorts: []string{"8123", "9000"},
		Mounts:       []string{fmt.Sprintf(`%s/warehouse/clickhouse/cluster/clickhouse01:/etc/clickhouse-server`, pwd)},
		Links:        []string{"clickhouse-zookeeper"},
	}); err != nil {
		log.Println("Could not create clickhouse cluster 1: %w", err)
		return
	}

	if chClusterTestT.clickhouse02, err = pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "yandex/clickhouse-server",
		Tag:        "21-alpine",
		Hostname:   "clickhouse02",
		Name:       "clickhouse02",
		Mounts:     []string{fmt.Sprintf(`%s/warehouse/clickhouse/cluster/clickhouse02:/etc/clickhouse-server`, pwd)},
		Links:      []string{"clickhouse-zookeeper"},
	}); err != nil {
		log.Println("Could not create clickhouse cluster 2: %w", err)
		return
	}

	if chClusterTestT.clickhouse03, err = pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "yandex/clickhouse-server",
		Tag:        "21-alpine",
		Hostname:   "clickhouse03",
		Name:       "clickhouse03",
		Mounts:     []string{fmt.Sprintf(`%s/warehouse/clickhouse/cluster/clickhouse03:/etc/clickhouse-server`, pwd)},
		Links:      []string{"clickhouse-zookeeper"},
	}); err != nil {
		log.Println("Could not create clickhouse cluster 3: %w", err)
		return
	}

	if chClusterTestT.clickhouse04, err = pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "yandex/clickhouse-server",
		Tag:        "21-alpine",
		Hostname:   "clickhouse04",
		Name:       "clickhouse04",
		Mounts:     []string{fmt.Sprintf(`%s/warehouse/clickhouse/cluster/clickhouse04:/etc/clickhouse-server`, pwd)},
		Links:      []string{"clickhouse-zookeeper"},
	}); err != nil {
		log.Println("Could not create clickhouse cluster 4: %w", err)
		return
	}

	if err = pool.Client.ConnectNetwork(chClusterTestT.network.ID, dc.NetworkConnectionOptions{
		Container: chClusterTestT.zookeeper.Container.Name,
		EndpointConfig: &dc.EndpointConfig{
			IPAddress: "172.23.0.10",
		},
	}); err != nil {
		log.Println("Could not configure clickhouse zookeeper network: %w", err)
		return
	}

	if err = pool.Client.ConnectNetwork(chClusterTestT.network.ID, dc.NetworkConnectionOptions{
		Container: chClusterTestT.clickhouse01.Container.Name,
		EndpointConfig: &dc.EndpointConfig{
			IPAddress: "172.23.0.11",
		},
	}); err != nil {
		log.Println("Could not configure clickhouse cluster network 1: %w", err)
		return
	}

	if err = pool.Client.ConnectNetwork(chClusterTestT.network.ID, dc.NetworkConnectionOptions{
		Container: chClusterTestT.clickhouse02.Container.Name,
		EndpointConfig: &dc.EndpointConfig{
			IPAddress: "172.23.0.12",
		},
	}); err != nil {
		log.Println("Could not configure clickhouse cluster network 2: %w", err)
		return
	}

	if err = pool.Client.ConnectNetwork(chClusterTestT.network.ID, dc.NetworkConnectionOptions{
		Container: chClusterTestT.clickhouse03.Container.Name,
		EndpointConfig: &dc.EndpointConfig{
			IPAddress: "172.23.0.13",
		},
	}); err != nil {
		log.Println("Could not configure clickhouse cluster network 3: %w", err)
		return
	}

	if err = pool.Client.ConnectNetwork(chClusterTestT.network.ID, dc.NetworkConnectionOptions{
		Container: chClusterTestT.clickhouse04.Container.Name,
		EndpointConfig: &dc.EndpointConfig{
			IPAddress: "172.23.0.14",
		},
	}); err != nil {
		log.Println("Could not configure clickhouse cluster network 4: %w", err)
		return
	}

	// Getting at which port the container is running
	ClickHousePort := chClusterTestT.clickhouse01.GetPort("9000/tcp")

	// Creating url string for connection
	whClickHouseClusterUrl := fmt.Sprintf("tcp://%s:%s?&username=%s&password=%s&database=%s&block_size=%s&pool_size=%s&debug=%s&secure=%s&skip_verify=%s&tls_config=%s&read_timeout=%s&write_timeout=%s&compress=%s",
		destConfig.ClickHouseHost,
		ClickHousePort,
		destConfig.ClickHouseUser,
		destConfig.ClickHousePassword,
		destConfig.ClickHouseDb,
		destConfig.ClickHouseBlockSize,
		destConfig.ClickHousePoolSize,
		destConfig.ClickHouseQueryDebugLogs,
		destConfig.ClickHouseSecure,
		destConfig.ClickHouseSkipVerify,
		destConfig.ClickHouseTlsConfigName,
		destConfig.ClickHouseReadTimeout,
		destConfig.ClickHouseWriteTimeout,
		destConfig.ClickHouseCompress,
	)
	fmt.Println("whClickHouseClusterUrl:", whClickHouseClusterUrl)

	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	if err := pool.Retry(func() error {
		var err error
		chClusterTestT.db, err = sql.Open("clickhouse", whClickHouseClusterUrl)
		if err != nil {
			return err
		}
		return chClusterTestT.db.Ping()
	}); err != nil {
		log.Println("Could not connect to clickhouse", whClickHouseClusterUrl, err)
	}
	if err != nil {
		return
	}

	chClusterTestT.setupDone = true
}

func SetWhMSSqlDestination() {
	mssqlTestT := whTest.mssqlTestT
	destConfig := mssqlTestT.destConfig

	mssqlTestT.resource, err = pool.Run("mcr.microsoft.com/mssql/server", "2019-latest", []string{
		fmt.Sprintf("ACCEPT_EULA=%s", "Y"),
		fmt.Sprintf("SA_PASSWORD=%s", destConfig.MSSqlPassword),
		fmt.Sprintf("SA_DB=%s", destConfig.MSSqlDb),
		fmt.Sprintf("SA_USER=%s", destConfig.MSSqlUser),
	})
	if err != nil {
		log.Println("Could not start resource ClickHouse: %w", err)
		return
	}

	// Getting at which port the container is running
	MSSqlPort := mssqlTestT.resource.GetPort("1433/tcp")

	query := url.Values{}
	query.Add("database", destConfig.MSSqlDb)
	query.Add("encrypt", destConfig.MSSqlSslMode)
	query.Add("TrustServerCertificate", "true")
	whMSSqlUrl := &url.URL{
		Scheme:   "sqlserver",
		User:     url.UserPassword(destConfig.MSSqlUser, destConfig.MSSqlPassword),
		Host:     fmt.Sprintf("%s:%s", destConfig.MSSqlHost, MSSqlPort),
		RawQuery: query.Encode(),
	}
	fmt.Println("whMSSqlUrl:", whMSSqlUrl.String())

	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	if err := pool.Retry(func() error {
		var err error
		mssqlTestT.db, err = sql.Open("sqlserver", whMSSqlUrl.String())
		if err != nil {
			return err
		}
		return mssqlTestT.db.Ping()
	}); err != nil {
		log.Println("Could not connect to MSSql", whMSSqlUrl, err)
	}
	if err != nil {
		return
	}

	mssqlTestT.setupDone = true
}

func AddSqlFunctionsToJobsDb() {
	if db == nil {
		return
	}

	// Running fetching gateway jobs function
	_, err = db.Exec(whTest.gwJobsSqlFunction)
	if err != nil {
		log.Printf("Error occurred with executing prepared statement for events count with err %v\n", err)
		return
	}

	// Running fetching batch router jobs function
	_, err = db.Exec(whTest.batchRtJobsSqlFunction)
	if err != nil {
		log.Printf("Error occurred with executing prepared statement for events count with err %v\n", err)
		return
	}

	whTest.sqlFunctionsAdded = true
}

// TODO: Verify in Live Events API
