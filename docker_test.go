// This files implements integration tests for the rudder-server.
// The code is responsible to run all dependencies using docker containers.
// It then runs the service ensuring it is configured to use the dependencies.
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
	"io"
	"log"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"text/template"
	"time"

	"github.com/gofrs/uuid"
	redigo "github.com/gomodule/redigo/redis"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
	"github.com/ory/dockertest/v3"
	"github.com/phayes/freeport"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	main "github.com/rudderlabs/rudder-server"
	"github.com/rudderlabs/rudder-server/config"
	kafkaclient "github.com/rudderlabs/rudder-server/services/streammanager/kafka/client"
	"github.com/rudderlabs/rudder-server/services/streammanager/kafka/client/testutil"
	"github.com/rudderlabs/rudder-server/testhelper"
	"github.com/rudderlabs/rudder-server/testhelper/destination"
	wht "github.com/rudderlabs/rudder-server/testhelper/warehouse"
	"github.com/rudderlabs/rudder-server/utils/logger"
	bq "github.com/rudderlabs/rudder-server/warehouse/bigquery"
	"github.com/rudderlabs/rudder-server/warehouse/client"
)

var (
	hold                         = true
	db                           *sql.DB
	httpPort                     string
	webhookURL                   string
	disableDestinationWebhookURL string
	webhook                      *WebhookRecorder
	disableDestinationWebhook    *WebhookRecorder
	runIntegration               bool
	writeKey                     string
	workspaceID                  string
	KafkaContainer               *destination.KafkaResource
	RedisContainer               *destination.RedisResource
	PostgresContainer            *destination.PostgresResource
	TransformerContainer         *destination.TransformerResource
	MINIOContainer               *destination.MINIOResource
	EventID                      string
	VersionID                    string
	runBigQueryTest              bool
)

type WebhookRecorder struct {
	Server       *httptest.Server
	requestsMu   sync.RWMutex
	requestDumps [][]byte
}

type eventSchemasObject struct {
	EventID   string
	EventType string
	VersionID string
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
		_, _ = w.Write([]byte("OK"))
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

func createWorkspaceConfig(templatePath string, values map[string]string) string {
	t, err := template.ParseFiles(templatePath)
	if err != nil {
		panic(err)
	}

	f, err := os.CreateTemp("", "workspaceConfig.*.json")
	if err != nil {
		panic(err)
	}
	defer func() { _ = f.Close() }()

	err = t.Execute(f, values)
	if err != nil {
		panic(err)
	}

	return f.Name()
}

func waitUntilReady(ctx context.Context, endpoint string, atMost, interval time.Duration, caller string) {
	probe := time.NewTicker(interval)
	timeout := time.After(atMost)
	for {
		select {
		case <-ctx.Done():
			return
		case <-timeout:
			log.Panicf("application was not ready after %s, for the end point: %s, caller: %s\n", atMost, endpoint,
				caller)
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
	httpClient := &http.Client{}
	req, err := http.NewRequest(method, url, nil)

	if err != nil {
		return "", err
	}
	req.Header.Add("Authorization", "Basic cnVkZGVyOnBhc3N3b3Jk")
	res, err := httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer func() { _ = res.Body.Close() }()

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
	resBody, err = GetEvent(url, method)
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
	httpClient := &http.Client{}
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

	res, err := httpClient.Do(req)
	if err != nil {
		log.Println(err)
		return
	}
	defer func() { _ = res.Body.Close() }()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		log.Println(err)
		return
	}
	if res.Status != "200 OK" {
		return
	}
	log.Println(string(body))
	log.Println("Event Sent Successfully")
}

func TestMain(m *testing.M) {
	flag.BoolVar(&hold, "hold", false, "hold environment clean-up after test execution until Ctrl+C is provided")
	flag.BoolVar(&runIntegration, "integration", false, "run integration level tests")
	flag.BoolVar(&runBigQueryTest, "bigqueryintegration", false, "run big query test")
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

	config.Load()
	logger.Init()

	// uses a sensible default on windows (tcp/http) and linux/osx (socket)
	pool, err := dockertest.NewPool("")
	if err != nil {
		return 0, fmt.Errorf("could not connect to docker: %w", err)
	}

	cleanup := &testhelper.Cleanup{}
	defer cleanup.Run()

	KafkaContainer, err = destination.SetupKafka(pool, cleanup,
		destination.WithLogger(&testLogger{logger.NewLogger().Child("kafka")}),
		destination.WithBrokers(3),
	)
	if err != nil {
		return 0, fmt.Errorf("setup Kafka Destination container: %w", err)
	}

	RedisContainer, err = destination.SetupRedis(pool, cleanup)
	if err != nil {
		return 0, fmt.Errorf("setup Redis Destination container: %w", err)
	}

	PostgresContainer, err = destination.SetupPostgres(pool, cleanup)
	if err != nil {
		return 0, fmt.Errorf("setup Postgres Destination container: %w", err)
	}
	db = PostgresContainer.DB

	TransformerContainer, err = destination.SetupTransformer(pool, cleanup)
	if err != nil {
		return 0, fmt.Errorf("setup Transformer container : %w", err)
	}

	waitUntilReady(
		context.Background(),
		fmt.Sprintf("%s/health", TransformerContainer.TransformURL),
		time.Minute,
		time.Second,
		"transformer",
	)

	MINIOContainer, err = destination.SetupMINIO(pool, cleanup)
	if err != nil {
		return 0, fmt.Errorf("setup MINIO Container : %w", err)
	}

	if err := godotenv.Load("testhelper/.env"); err != nil {
		fmt.Println("INFO: No .env file found.")
	}

	_ = os.Setenv("JOBS_DB_PORT", PostgresContainer.Port)
	_ = os.Setenv("WAREHOUSE_JOBS_DB_PORT", PostgresContainer.Port)
	_ = os.Setenv("DEST_TRANSFORM_URL", TransformerContainer.TransformURL)

	wht.InitWHConfig()

	defer wht.SetWHPostgresDestination(pool)()
	defer wht.SetWHClickHouseDestination(pool)()
	defer wht.SetWHClickHouseClusterDestination(pool)()
	defer wht.SetWHMssqlDestination(pool)()
	defer wht.SetWHBigQueryDestination()()

	AddWHSpecificSqlFunctionsToJobsDb()

	httpPortInt, err := freeport.GetFreePort()
	if err != nil {
		return 0, fmt.Errorf("could not get free port for http: %w", err)
	}
	httpPort = strconv.Itoa(httpPortInt)
	_ = os.Setenv("RSERVER_GATEWAY_WEB_PORT", httpPort)
	httpAdminPort, err := freeport.GetFreePort()
	if err != nil {
		return 0, fmt.Errorf("could not get free port for http admin: %w", err)
	}

	_ = os.Setenv("RSERVER_GATEWAY_ADMIN_WEB_PORT", strconv.Itoa(httpAdminPort))
	_ = os.Setenv("RSERVER_ENABLE_STATS", "false")

	webhook = NewWebhook()
	defer webhook.Close()
	webhookURL = webhook.Server.URL

	disableDestinationWebhook = NewWebhook()
	defer disableDestinationWebhook.Close()
	disableDestinationWebhookURL = disableDestinationWebhook.Server.URL

	writeKey = randString(27)
	workspaceID = randString(27)
	mapWorkspaceConfig := map[string]string{
		"webhookUrl":                          webhookURL,
		"disableDestinationwebhookUrl":        disableDestinationWebhookURL,
		"writeKey":                            writeKey,
		"workspaceId":                         workspaceID,
		"postgresPort":                        PostgresContainer.Port,
		"address":                             RedisContainer.RedisAddress,
		"minioEndpoint":                       MINIOContainer.MinioEndpoint,
		"minioBucketName":                     MINIOContainer.MinioBucketName,
		"kafkaPort":                           KafkaContainer.Port,
		"postgresEventWriteKey":               wht.Test.PGTest.WriteKey,
		"clickHouseEventWriteKey":             wht.Test.CHTest.WriteKey,
		"clickHouseClusterEventWriteKey":      wht.Test.CHClusterTest.WriteKey,
		"mssqlEventWriteKey":                  wht.Test.MSSQLTest.WriteKey,
		"rwhPostgresDestinationPort":          wht.Test.PGTest.Credentials.Port,
		"rwhClickHouseDestinationPort":        wht.Test.CHTest.Credentials.Port,
		"rwhClickHouseClusterDestinationPort": wht.Test.CHClusterTest.GetResource().Credentials.Port,
		"rwhMSSqlDestinationPort":             wht.Test.MSSQLTest.Credentials.Port,
	}
	if runBigQueryTest && wht.Test.BQTest != nil {
		mapWorkspaceConfig["bqEventWriteKey"] = wht.Test.BQTest.WriteKey
		mapWorkspaceConfig["rwhBQProject"] = wht.Test.BQTest.Credentials.ProjectID
		mapWorkspaceConfig["rwhBQLocation"] = wht.Test.BQTest.Credentials.Location
		mapWorkspaceConfig["rwhBQBucketName"] = wht.Test.BQTest.Credentials.Bucket
		mapWorkspaceConfig["rwhBQCredentials"] = wht.Test.BQTest.Credentials.CredentialsEscaped
	}
	workspaceConfigPath := createWorkspaceConfig(
		"testdata/workspaceConfigTemplate.json",
		mapWorkspaceConfig,
	)
	defer func() {
		err := os.Remove(workspaceConfigPath)
		log.Println(err)
	}()
	log.Println("workspace config path:", workspaceConfigPath)
	_ = os.Setenv("RSERVER_BACKEND_CONFIG_CONFIG_JSONPATH", workspaceConfigPath)

	rudderTmpDir, err := os.MkdirTemp("", "rudder_server_test")
	if err != nil {
		return 0, err
	}
	defer func() { _ = os.RemoveAll(rudderTmpDir) }()
	_ = os.Setenv("RUDDER_TMPDIR", rudderTmpDir)

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
		"serviceHealthEndpoint",
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
	SendEvent(payload1, "identify", writeKey)
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
	SendEvent(payload2, "identify", writeKey) //sending duplicate event to check dedup

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
	SendEvent(payloadBatch, "batch", writeKey)

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
	SendEvent(payloadTrack, "track", writeKey)

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
	SendEvent(payloadPage, "page", writeKey)

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
	SendEvent(payloadScreen, "screen", writeKey)

	// Sending alias event
	payloadAlias := strings.NewReader(`{
		"userId": "identified_user_id",
		"anonymousId":"anonymousId_1",
		"messageId":"messageId_1",
		"type": "alias",
		"previousId": "name@surname.com",
		"userId": "12345"
	  }`)
	SendEvent(payloadAlias, "alias", writeKey)

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
	SendEvent(payloadGroup, "group", writeKey)
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
	require.Equal(t, 0, len(disableDestinationWebhook.Requests()))
}

//Verify Event in POSTGRES
func TestPostgres(t *testing.T) {
	var myEvent Event
	require.Eventually(t, func() bool {
		eventSql := "select anonymous_id, user_id from dev_integration_test_1.identifies limit 1"
		_ = db.QueryRow(eventSql).Scan(&myEvent.anonymousID, &myEvent.userID)
		return myEvent.anonymousID == "anonymousId_1"
	}, time.Minute, 10*time.Millisecond)
	eventSql := "select count(*) from dev_integration_test_1.identifies"
	_ = db.QueryRow(eventSql).Scan(&myEvent.count)
	require.Equal(t, myEvent.count, "2")

	// Verify User Transformation
	eventSql = "select context_myuniqueid,context_id,context_ip from dev_integration_test_1.identifies"
	_ = db.QueryRow(eventSql).Scan(&myEvent.contextMyUniqueID, &myEvent.contextID, &myEvent.contextIP)
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
	_ = db.QueryRow(eventSql).Scan(&myEvent.contextMyUniqueID, &myEvent.contextID, &myEvent.contextIP)
	require.Equal(t, myEvent.contextMyUniqueID, "identified_user_idanonymousId_1")
	require.Equal(t, myEvent.contextID, "0.0.0.0")
	require.Equal(t, myEvent.contextIP, "0.0.0.0")

	require.Eventually(t, func() bool {
		eventSql := "select anonymous_id, user_id from dev_integration_test_1.screens limit 1"
		_ = db.QueryRow(eventSql).Scan(&myEvent.anonymousID, &myEvent.userID)
		return myEvent.anonymousID == "anonymousId_1"
	}, time.Minute, 10*time.Millisecond)
	require.Eventually(t, func() bool {
		eventSql = "select count(*) from dev_integration_test_1.screens"
		_ = db.QueryRow(eventSql).Scan(&myEvent.count)
		return myEvent.count == "1"
	}, time.Minute, 10*time.Millisecond)

	// Verify User Transformation
	require.Eventually(t, func() bool {
		eventSql = "select prop_key,myuniqueid,ip from dev_integration_test_1.screens;"
		_ = db.QueryRow(eventSql).Scan(&myEvent.propKey, &myEvent.myUniqueID, &myEvent.ip)
		return myEvent.myUniqueID == "identified_user_idanonymousId_1"
	}, time.Minute, 10*time.Millisecond)

	require.Equal(t, myEvent.propKey, "prop_value_edited")
	require.Equal(t, myEvent.ip, "0.0.0.0")
}

// Verify Event in Redis
func TestRedis(t *testing.T) {
	conn, err := redigo.Dial("tcp", RedisContainer.RedisAddress)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()
	require.Eventually(t, func() bool {
		// Similarly, get the trait1 and convert it to a string.
		event, _ := redigo.String(conn.Do("HGET", "user:identified_user_id", "trait1"))
		return event == "new-val"
	}, time.Minute, 10*time.Millisecond)
}

func TestKafka(t *testing.T) {
	if runtime.GOARCH == "arm64" {
		t.Skip("arm64 is not supported yet")
	}

	kafkaHost := fmt.Sprintf("localhost:%s", KafkaContainer.Port)

	// Create new consumer
	tc := testutil.New("tcp", kafkaHost)
	topics, err := tc.ListTopics(context.TODO())
	require.NoError(t, err)

	c, err := kafkaclient.New("tcp", kafkaHost, kafkaclient.Config{})
	require.NoError(t, err)

	messages, errors := consume(t, c, topics)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// Count how many message processed
	msgCount := 0
	// Get signal for finish
	expectedCount := 10
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
			t.Log("Received consumerError", consumerError)
		case <-time.After(time.Minute):
			t.Error("timeout waiting on Kafka message")
		}
	}

	t.Log("Processed", msgCount, "messages")
}

func consume(t *testing.T, client *kafkaclient.Client, topics []testutil.TopicPartition) (<-chan kafkaclient.Message, <-chan error) {
	t.Helper()
	errors := make(chan error)
	messages := make(chan kafkaclient.Message)

	for _, topic := range topics {
		consumer := client.NewConsumer(topic.Topic, kafkaclient.ConsumerConfig{
			Partition:   topic.Partition,
			StartOffset: kafkaclient.FirstOffset,
		})

		t.Logf("Start consuming topic %s:%d", topic.Topic, topic.Partition)
		go func(topic testutil.TopicPartition, consumer *kafkaclient.Consumer) {
			for {
				msg, err := consumer.Receive(context.TODO())
				if err != nil {
					errors <- err
				} else {
					messages <- msg
				}
			}
		}(topic, consumer)
	}

	return messages, errors
}

//Verify Event Models EndPoint
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
	require.NotEqual(t, resBody, "[]")
	b := []byte(resBody)
	var eventSchemas []eventSchemasObject

	err := json.Unmarshal(b, &eventSchemas)
	if err != nil {
		log.Println(err)
	}
	for k := range eventSchemas {
		if eventSchemas[k].EventType == "page" {
			EventID = eventSchemas[k].EventID
		}
	}
	require.NotEqual(t, EventID, "")
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
		log.Println(err)
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

// Verify beacon EndPoint
func TestBeaconBatch(_ *testing.T) {
	payload := strings.NewReader(`{
		"batch":[
			{
			   "userId": "identified_user_id",
			   "anonymousId":"anonymousId_1",
			   "messageId":"messageId_1"
			}
		]
	}`)
	SendEvent(payload, "beacon/v1/batch", writeKey)
}

// Adding necessary sql functions which needs to be used during warehouse testing
func AddWHSpecificSqlFunctionsToJobsDb() {
	var err error
	_, err = db.Exec(wht.Test.GatewayJobsSqlFunction)
	if err != nil {
		panic(fmt.Errorf("error occurred with executing gw jobs function for events count with err %v", err.Error()))
	}

	_, err = db.Exec(wht.Test.BatchRouterJobsSqlFunction)
	if err != nil {
		panic(fmt.Errorf("error occurred with executing brt jobs function for events count with err %s", err.Error()))
	}
}

//Verify Event in WareHouse Postgres
func TestWHPostgresDestination(t *testing.T) {
	pgTest := wht.Test.PGTest

	whDestTest := &wht.WareHouseDestinationTest{
		Client: &client.Client{
			SQL:  pgTest.DB,
			Type: client.SQLClient,
		},
		EventsCountMap:     pgTest.EventsMap,
		WriteKey:           pgTest.WriteKey,
		UserId:             "userId_postgres",
		Schema:             "postgres_wh_integration",
		TableTestQueryFreq: pgTest.TableTestQueryFreq,
	}
	sendWHEvents(whDestTest)
	whDestinationTest(t, whDestTest)

	whDestTest.UserId = "userId_postgres_1"
	sendUpdatedWHEvents(whDestTest)
	whDestinationTest(t, whDestTest)
}

//Verify Event in WareHouse ClickHouse
func TestWHClickHouseDestination(t *testing.T) {
	chTest := wht.Test.CHTest

	whDestTest := &wht.WareHouseDestinationTest{
		Client: &client.Client{
			SQL:  chTest.DB,
			Type: client.SQLClient,
		},
		EventsCountMap:     chTest.EventsMap,
		WriteKey:           chTest.WriteKey,
		UserId:             "userId_clickhouse",
		Schema:             "rudderdb",
		TableTestQueryFreq: chTest.TableTestQueryFreq,
	}
	sendWHEvents(whDestTest)
	whDestinationTest(t, whDestTest)

	whDestTest.UserId = "userId_clickhouse_1"
	sendUpdatedWHEvents(whDestTest)
	whDestinationTest(t, whDestTest)
}

//Verify Event in WareHouse ClickHouse Cluster
func TestWHClickHouseClusterDestination(t *testing.T) {
	chClusterTest := wht.Test.CHClusterTest

	whDestTest := &wht.WareHouseDestinationTest{
		Client: &client.Client{
			SQL:  chClusterTest.GetResource().DB,
			Type: client.SQLClient,
		},
		EventsCountMap:     chClusterTest.EventsMap,
		WriteKey:           chClusterTest.WriteKey,
		UserId:             "userId_clickhouse_cluster",
		Schema:             "rudderdb",
		TableTestQueryFreq: chClusterTest.TableTestQueryFreq,
	}
	sendWHEvents(whDestTest)
	whDestinationTest(t, whDestTest)

	initWHClickHouseClusterModeSetup(t)

	whDestTest.UserId = "userId_clickhouse_cluster_1"
	sendUpdatedWHEvents(whDestTest)

	// Update events count Map
	// This is required as because of the cluster mode setup and distributed view, events are getting duplicated.
	whDestTest.EventsCountMap = wht.EventsCountMap{
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

func TestWHBigQuery(t *testing.T) {
	if runBigQueryTest == false {
		t.Skip("Big query integration skipped. use -bigqueryintegration to add this test ")

	}
	if wht.Test.BQTest == nil {
		fmt.Println("Error in ENV variable BIGQUERY_INTEGRATION_TEST_USER_CRED")
		t.FailNow()
	}
	//Disabling big query dedup
	config.SetBool("Warehouse.bigquery.isDedupEnabled", false)
	bq.Init()
	bqTest := wht.Test.BQTest
	randomness := strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", "")

	whDestTest := &wht.WareHouseDestinationTest{
		Client: &client.Client{
			BQ:   bqTest.DB,
			Type: client.BQClient,
		},
		EventsCountMap:     bqTest.EventsMap,
		WriteKey:           bqTest.WriteKey,
		UserId:             fmt.Sprintf("userId_bq_%s", randomness),
		Schema:             "rudderstack_sample_http_source",
		BQContext:          bqTest.Context,
		Tables:             bqTest.Tables,
		PrimaryKeys:        bqTest.PrimaryKeys,
		TableTestQueryFreq: bqTest.TableTestQueryFreq,
	}

	whDestTest.MessageId = uuid.Must(uuid.NewV4()).String()

	whDestTest.EventsCountMap = wht.EventsCountMap{
		"identifies": 2,
		"tracks":     2,
		"pages":      2,
		"screens":    2,
		"aliases":    2,
		"groups":     2,
	}
	sendWHEvents(whDestTest)

	whDestTest.EventsCountMap = wht.EventsCountMap{
		"identifies":    2,
		"users":         1,
		"tracks":        2,
		"product_track": 2,
		"pages":         2,
		"screens":       2,
		"aliases":       2,
		"_groups":       2,
		"gateway":       12,
		"batchRT":       16,
	}

	whDestinationTest(t, whDestTest)
	//Enabling big query dedup
	config.SetBool("Warehouse.bigquery.isDedupEnabled", true)
	bq.Init()

	whDestTest.EventsCountMap = wht.EventsCountMap{
		"identifies": 2,
		"tracks":     2,
		"pages":      2,
		"screens":    2,
		"aliases":    2,
		"groups":     2,
	}

	sendWHEvents(whDestTest)

	whDestTest.EventsCountMap = wht.EventsCountMap{
		"identifies":    2,
		"users":         1,
		"tracks":        2,
		"product_track": 2,
		"pages":         2,
		"screens":       2,
		"aliases":       2,
		"_groups":       2,
		"gateway":       24,
		"batchRT":       32,
	}

	whDestinationTest(t, whDestTest)

}

//Verify Event in WareHouse MSSQL
func TestWHMssqlDestination(t *testing.T) {
	MssqlTest := wht.Test.MSSQLTest

	whDestTest := &wht.WareHouseDestinationTest{
		Client: &client.Client{
			SQL:  MssqlTest.DB,
			Type: client.SQLClient,
		},
		EventsCountMap:     MssqlTest.EventsMap,
		WriteKey:           MssqlTest.WriteKey,
		UserId:             "userId_mssql",
		Schema:             "mssql_wh_integration",
		TableTestQueryFreq: MssqlTest.TableTestQueryFreq,
	}
	sendWHEvents(whDestTest)
	whDestinationTest(t, whDestTest)

	whDestTest.UserId = "userId_mssql_1"
	sendUpdatedWHEvents(whDestTest)
	whDestinationTest(t, whDestTest)
}

func getMessageId(wdt *wht.WareHouseDestinationTest) string {
	if wdt.MessageId == "" {
		return uuid.Must(uuid.NewV4()).String()
	}
	return wdt.MessageId
}

// sendWHEvents Sending warehouse events
func sendWHEvents(wdt *wht.WareHouseDestinationTest) {
	// Sending identify event
	if identify, exists := wdt.EventsCountMap["identifies"]; exists {
		for i := 0; i < identify; i++ {
			payloadIdentify := strings.NewReader(fmt.Sprintf(`{
			"userId": "%s",
			"messageId":"%s",
			"type": "identify",
			"eventOrderNo":"1",
			"context": {
			  "traits": {
				 "trait1": "new-val"
			  }
			},
			"timestamp": "2020-02-02T00:23:09.544Z"
		  }`, wdt.UserId, getMessageId(wdt)))
			SendEvent(payloadIdentify, "identify", wdt.WriteKey)
		}
	}

	// Sending track event
	if track, exists := wdt.EventsCountMap["tracks"]; exists {
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
		  }`, wdt.UserId, getMessageId(wdt)))
			SendEvent(payloadTrack, "track", wdt.WriteKey)
		}
	}

	// Sending page event
	if page, exists := wdt.EventsCountMap["pages"]; exists {
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
		  }`, wdt.UserId, getMessageId(wdt)))
			SendEvent(payloadPage, "page", wdt.WriteKey)
		}
	}

	// Sending screen event
	if screen, exists := wdt.EventsCountMap["screens"]; exists {
		for i := 0; i < screen; i++ {
			payloadScreen := strings.NewReader(fmt.Sprintf(`{
			"userId": "%s",
			"messageId":"%s",
			"type": "screen",
			"name": "Main",
			"properties": {
			  "prop_key": "prop_value"
			}
		  }`, wdt.UserId, getMessageId(wdt)))
			SendEvent(payloadScreen, "screen", wdt.WriteKey)
		}
	}

	// Sending alias event
	if alias, exists := wdt.EventsCountMap["aliases"]; exists {
		for i := 0; i < alias; i++ {
			payloadAlias := strings.NewReader(fmt.Sprintf(`{
			"userId": "%s",
			"messageId":"%s",
			"type": "alias",
			"previousId": "name@surname.com"
		  }`, wdt.UserId, getMessageId(wdt)))
			SendEvent(payloadAlias, "alias", wdt.WriteKey)
		}
	}

	// Sending group event
	if group, exists := wdt.EventsCountMap["groups"]; exists {
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
		  }`, wdt.UserId, getMessageId(wdt)))
			SendEvent(payloadGroup, "group", wdt.WriteKey)
		}
	}
}

// sendUpdatedWHEvents Sending updated warehouse events
func sendUpdatedWHEvents(wdt *wht.WareHouseDestinationTest) {
	// Sending identify event
	if identify, exists := wdt.EventsCountMap["identifies"]; exists {
		for i := 0; i < identify; i++ {
			payloadIdentify := strings.NewReader(fmt.Sprintf(`{
			"userId": "%s",
			"messageId":"%s",
			"type": "identify",
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
		  }`, wdt.UserId, uuid.Must(uuid.NewV4()).String()))
			SendEvent(payloadIdentify, "identify", wdt.WriteKey)
		}
	}

	// Sending track event
	if track, exists := wdt.EventsCountMap["tracks"]; exists {
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
				"ip": "14.5.67.21",
				"library": {
					"name": "http"
				}
			}
		  }`, wdt.UserId, uuid.Must(uuid.NewV4()).String()))
			SendEvent(payloadTrack, "track", wdt.WriteKey)
		}
	}

	// Sending page event
	if page, exists := wdt.EventsCountMap["pages"]; exists {
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
		  }`, wdt.UserId, uuid.Must(uuid.NewV4()).String()))
			SendEvent(payloadPage, "page", wdt.WriteKey)
		}
	}

	// Sending screen event
	if screen, exists := wdt.EventsCountMap["screens"]; exists {
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
		  }`, wdt.UserId, uuid.Must(uuid.NewV4()).String()))
			SendEvent(payloadScreen, "screen", wdt.WriteKey)
		}
	}

	// Sending alias event
	if alias, exists := wdt.EventsCountMap["aliases"]; exists {
		for i := 0; i < alias; i++ {
			payloadAlias := strings.NewReader(fmt.Sprintf(`{
			"userId": "%s",
			"messageId":"%s",
			"type": "alias",
			"previousId": "name@surname.com",
			"context": {
				"ip": "14.5.67.21",
				"library": {
					"name": "http"
				}
            }
		  }`, wdt.UserId, uuid.Must(uuid.NewV4()).String()))
			SendEvent(payloadAlias, "alias", wdt.WriteKey)
		}
	}

	// Sending group event
	if group, exists := wdt.EventsCountMap["groups"]; exists {
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
				"ip": "14.5.67.21",
				"library": {
					"name": "http"
				}
			}
		  }`, wdt.UserId, uuid.Must(uuid.NewV4()).String()))
			SendEvent(payloadGroup, "group", wdt.WriteKey)
		}
	}
}

// whDestinationTest Checking warehouse destination
func whDestinationTest(t *testing.T, wdt *wht.WareHouseDestinationTest) {
	whGatewayTest(t, wdt)
	whBatchRouterTest(t, wdt)
	whTablesTest(t, wdt)
}

// whGatewayTest Checking for gateway jobs
func whGatewayTest(t *testing.T, wdt *wht.WareHouseDestinationTest) {
	require.Contains(t, wdt.EventsCountMap, "gateway")
	gwEvents := wdt.EventsCountMap["gateway"]
	// Checking for the gateway jobs
	t.Log("Checking for the gateway jobs")
	require.Eventually(t, func() bool {
		var count int64
		jobSqlStatement := fmt.Sprintf(`select count(*) from gw_jobs_for_user_id_and_write_key('%s', '%s') as job_ids`, wdt.UserId, wdt.WriteKey)
		err := db.QueryRow(jobSqlStatement).Scan(&count)
		require.Equal(t, err, nil)
		return count == int64(gwEvents)
	}, time.Minute, 10*time.Millisecond)

	// Getting gateway job ids
	t.Log("Getting gateway job ids")
	var jobIds []string
	require.Eventually(t, func() bool {
		jobIds = make([]string, 0)
		jobSqlStatement := fmt.Sprintf(`select * from gw_jobs_for_user_id_and_write_key('%s', '%s') as job_ids`, wdt.UserId, wdt.WriteKey)
		rows, err := db.Query(jobSqlStatement)
		require.Equal(t, err, nil)

		defer func() { _ = rows.Close() }()
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
		require.Equal(t, nil, err)
		return count == int64(gwEvents)
	}, time.Minute, 10*time.Millisecond)
}

// whBatchRouterTest Checking for batch router jobs
func whBatchRouterTest(t *testing.T, wdt *wht.WareHouseDestinationTest) {
	require.Contains(t, wdt.EventsCountMap, "batchRT")
	brtEvents := wdt.EventsCountMap["batchRT"]

	// Checking for the batch router jobs
	t.Log("Checking for the batch router jobs")
	require.Eventually(t, func() bool {
		var count int64
		jobsSqlStatement := fmt.Sprintf(`select count(*) from brt_jobs_for_user_id('%s') as job_ids`, wdt.UserId)
		err := db.QueryRow(jobsSqlStatement).Scan(&count)
		require.Equal(t, err, nil)
		return count == int64(brtEvents)
	}, 2*time.Minute, 100*time.Millisecond)

	// Getting batch router job ids
	t.Log("Getting batch router job ids")
	var jobIds []string
	require.Eventually(t, func() bool {
		jobIds = make([]string, 0)
		jobSqlStatement := fmt.Sprintf(`select * from brt_jobs_for_user_id('%s') as job_ids`, wdt.UserId)
		rows, err := db.Query(jobSqlStatement)
		require.Equal(t, err, nil)

		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var jobId int64
			err = rows.Scan(&jobId)
			require.Equal(t, err, nil)
			jobIds = append(jobIds, fmt.Sprint(jobId))
		}
		return brtEvents == len(jobIds)
	}, 2*time.Minute, 100*time.Millisecond)

	// Checking for the batch router jobs state
	t.Log("Checking for the batch router jobs state")
	require.Eventually(t, func() bool {
		var count int64
		jobsSqlStatement := fmt.Sprintf("select count(*) from batch_rt_job_status_1 where job_id in (%s) and job_state = 'succeeded'", strings.Join(jobIds, ","))
		err := db.QueryRow(jobsSqlStatement).Scan(&count)
		require.Equal(t, err, nil)
		return count == int64(brtEvents)
	}, 2*time.Minute, 100*time.Millisecond)
}

func getQueryCount(cl *client.Client, statement string) (int64, error) {
	result, err := cl.Query(statement, client.Read)
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(result.Values[0][0], 10, 64)
}

// whTablesTest Checking warehouse
func whTablesTest(t *testing.T, wdt *wht.WareHouseDestinationTest) {
	tables := []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"}
	primaryKeys := []string{"user_id", "id", "user_id", "user_id", "user_id", "user_id", "user_id", "user_id"}

	if len(wdt.Tables) != 0 {
		tables = wdt.Tables
	}
	if len(wdt.PrimaryKeys) != 0 {
		primaryKeys = wdt.PrimaryKeys
	}

	for idx, table := range tables {
		require.Contains(t, wdt.EventsCountMap, table)
		tableCount := wdt.EventsCountMap[table]
		require.Eventually(t, func() bool {
			var count int64
			sqlStatement := fmt.Sprintf("select count(*) from %s.%s where %s = '%s'", wdt.Schema, table, primaryKeys[idx], wdt.UserId)
			count, _ = getQueryCount(wdt.Client, sqlStatement)
			return count == int64(tableCount)
		}, 2*time.Minute, wdt.TableTestQueryFreq)
	}
}

// initWHClickHouseClusterModeSetup Initialize cluster mode setup
func initWHClickHouseClusterModeSetup(t *testing.T) {
	type ColumnInfoT struct {
		ColumnName string
		ColumnType string
	}

	chClusterTest := wht.Test.CHClusterTest
	tables := []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"}
	tableColumnInfoMap := map[string][]ColumnInfoT{
		"identifies": {
			{
				ColumnName: "context_passed_ip",
				ColumnType: "Nullable(String)",
			},
			{
				ColumnName: "context_library_name",
				ColumnType: "Nullable(String)",
			},
		},
		"product_track": {
			{
				ColumnName: "revenue",
				ColumnType: "Nullable(Float64)",
			},
			{
				ColumnName: "context_passed_ip",
				ColumnType: "Nullable(String)",
			},
			{
				ColumnName: "context_library_name",
				ColumnType: "Nullable(String)",
			},
		},
		"tracks": {
			{
				ColumnName: "context_passed_ip",
				ColumnType: "Nullable(String)",
			},
			{
				ColumnName: "context_library_name",
				ColumnType: "Nullable(String)",
			},
		},
		"users": {
			{
				ColumnName: "context_passed_ip",
				ColumnType: "Nullable(String)",
			},
			{
				ColumnName: "context_library_name",
				ColumnType: "SimpleAggregateFunction(anyLast, Nullable(String))",
			},
		},
		"pages": {
			{
				ColumnName: "context_passed_ip",
				ColumnType: "Nullable(String)",
			},
			{
				ColumnName: "context_library_name",
				ColumnType: "Nullable(String)",
			},
		},
		"screens": {
			{
				ColumnName: "context_passed_ip",
				ColumnType: "Nullable(String)",
			},
			{
				ColumnName: "context_library_name",
				ColumnType: "Nullable(String)",
			},
		},
		"aliases": {
			{
				ColumnName: "context_passed_ip",
				ColumnType: "Nullable(String)",
			},
			{
				ColumnName: "context_library_name",
				ColumnType: "Nullable(String)",
			},
		},
		"groups": {
			{
				ColumnName: "context_passed_ip",
				ColumnType: "Nullable(String)",
			},
			{
				ColumnName: "context_library_name",
				ColumnType: "Nullable(String)",
			},
		},
	}

	// Rename tables to tables_shard
	for _, table := range tables {
		sqlStatement := fmt.Sprintf("RENAME TABLE %[1]s to %[1]s_shard ON CLUSTER rudder_cluster;", table)
		_, err := chClusterTest.GetResource().DB.Exec(sqlStatement)
		require.Equal(t, err, nil)
	}

	// Create distribution views for tables
	for _, table := range tables {
		sqlStatement := fmt.Sprintf("CREATE TABLE rudderdb.%[1]s ON CLUSTER 'rudder_cluster' AS rudderdb.%[1]s_shard ENGINE = Distributed('rudder_cluster', rudderdb, %[1]s_shard, cityHash64(concat(toString(received_at), id)));", table)
		_, err := chClusterTest.GetResource().DB.Exec(sqlStatement)
		require.Equal(t, err, nil)
	}

	// Alter columns to all the cluster tables
	for _, chResource := range chClusterTest.Resources {
		for tableName, columnInfos := range tableColumnInfoMap {
			for _, columnInfo := range columnInfos {
				sqlStatement := fmt.Sprintf("ALTER TABLE rudderdb.%[1]s_shard ADD COLUMN IF NOT EXISTS %[2]s %[3]s;", tableName, columnInfo.ColumnName, columnInfo.ColumnType)
				_, err := chResource.DB.Exec(sqlStatement)
				require.Equal(t, err, nil)
			}
		}
	}
}

type testLogger struct{ logger.LoggerI }

func (t *testLogger) Log(args ...interface{}) { t.Info(args...) }
