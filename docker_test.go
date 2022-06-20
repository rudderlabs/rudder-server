// This files implements integration tests for the rudder-server.
// The code is responsible to run all dependencies using docker containers.
// It then runs the service ensuring it is configured to use the dependencies.
// Finally, it sends events and observe the destinations expecting to get the events back.

//go:build integration

package main_test

import (
	"context"
	"database/sql"
	b64 "encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
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
	"github.com/tidwall/gjson"

	main "github.com/rudderlabs/rudder-server"
	"github.com/rudderlabs/rudder-server/config"
	kafkaClient "github.com/rudderlabs/rudder-server/services/streammanager/kafka/client"
	"github.com/rudderlabs/rudder-server/services/streammanager/kafka/client/testutil"
	"github.com/rudderlabs/rudder-server/testhelper/destination"
	wht "github.com/rudderlabs/rudder-server/testhelper/warehouse"
	whUtil "github.com/rudderlabs/rudder-server/testhelper/webhook"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/types/deployment"
	bq "github.com/rudderlabs/rudder-server/warehouse/bigquery"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/stretchr/testify/require"
)

var (
	hold                         = true
	db                           *sql.DB
	httpPort                     string
	webhookURL                   string
	disableDestinationWebhookURL string
	webhook                      *whUtil.Recorder
	disableDestinationWebhook    *whUtil.Recorder
	runSlow                      bool
	overrideArm64Check           bool
	writeKey                     string
	workspaceID                  string
	kafkaContainer               *destination.KafkaResource
	redisContainer               *destination.RedisResource
	postgresContainer            *destination.PostgresResource
	transformerContainer         *destination.TransformerResource
	minioContainer               *destination.MINIOResource
	EventID                      string
	VersionID                    string
	runBigQueryTest              bool
)

type eventSchemasObject struct {
	EventID   string
	EventType string
	VersionID string
}

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
	runSlow = config.GetEnvAsBool("SLOW", true)
	if !runSlow {
		t.Skip("Skipping tests. Remove 'SLOW=0' env var to run them.")
	}

	hold = config.GetEnvAsBool("HOLD", false)
	runBigQueryTest = config.GetEnvAsBool("BIGQUERYINTEGRATION", false)
	if os.Getenv("OVERRIDE_ARM64_CHECK") == "1" {
		overrideArm64Check = true
	}

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
	})

	t.Run("redis", func(t *testing.T) {
		conn, err := redigo.Dial("tcp", redisContainer.RedisAddress)
		require.NoError(t, err)
		defer func() { _ = conn.Close() }()
		require.Eventually(t, func() bool {
			// Similarly, get the trait1 and convert it to a string.
			event, _ := redigo.String(conn.Do("HGET", "user:identified_user_id", "trait1"))
			return event == "new-val"
		}, time.Minute, 10*time.Millisecond)
	})

	t.Run("kafka", func(t *testing.T) {
		if runtime.GOARCH == "arm64" && !overrideArm64Check {
			t.Skip("arm64 is not supported yet")
		}

		kafkaHost := fmt.Sprintf("localhost:%s", kafkaContainer.Port)

		// Create new consumer
		tc := testutil.New("tcp", kafkaHost)
		topics, err := tc.ListTopics(context.TODO())
		require.NoError(t, err)

		c, err := kafkaClient.New("tcp", []string{kafkaHost}, kafkaClient.Config{})
		require.NoError(t, err)

		messages, errors := consume(t, c, topics)

		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Interrupt, os.Kill) // Get signal for finish

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

	t.Run("event-models", func(t *testing.T) {
		// GET /schemas/event-models
		url := fmt.Sprintf("http://localhost:%s/schemas/event-models", httpPort)
		method := "GET"
		resBody, _ := getEvent(url, method)
		require.Eventually(t, func() bool {
			// Similarly, pole until the Event Schema Tables are updated
			resBody, _ = getEvent(url, method)
			return resBody != "[]"
		}, time.Minute, 10*time.Millisecond)
		require.NotEqual(t, resBody, "[]")
		b := []byte(resBody)
		var eventSchemas []eventSchemasObject

		err := json.Unmarshal(b, &eventSchemas)
		if err != nil {
			t.Log(err)
		}
		for k := range eventSchemas {
			if eventSchemas[k].EventType == "page" {
				EventID = eventSchemas[k].EventID
			}
		}
		require.NotEqual(t, EventID, "")
	})

	t.Run("event-versions", func(t *testing.T) {
		// GET /schemas/event-versions
		url := fmt.Sprintf("http://localhost:%s/schemas/event-versions?EventID=%s", httpPort, EventID)
		method := "GET"
		resBody, _ := getEvent(url, method)
		require.Contains(t, resBody, EventID)

		b := []byte(resBody)
		var eventSchemas []eventSchemasObject

		err := json.Unmarshal(b, &eventSchemas)
		if err != nil {
			t.Log(err)
		}
		VersionID = eventSchemas[0].VersionID
		t.Log("Test Schemas Event ID's VersionID:", VersionID)
	})

	t.Run("event-model-key-counts", func(t *testing.T) {
		// GET schemas/event-model/{EventID}/key-counts
		url := fmt.Sprintf("http://localhost:%s/schemas/event-model/%s/key-counts", httpPort, EventID)
		method := "GET"
		resBody, _ := getEvent(url, method)
		require.Contains(t, resBody, "messageId")
	})

	t.Run("event-model-metadata", func(t *testing.T) {
		// GET /schemas/event-model/{EventID}/metadata
		url := fmt.Sprintf("http://localhost:%s/schemas/event-model/%s/metadata", httpPort, EventID)
		method := "GET"
		resBody, _ := getEvent(url, method)
		require.Contains(t, resBody, "messageId")
	})

	t.Run("event-version-metadata", func(t *testing.T) {
		// GET /schemas/event-version/{VersionID}/metadata
		url := fmt.Sprintf("http://localhost:%s/schemas/event-version/%s/metadata", httpPort, VersionID)
		method := "GET"
		resBody, _ := getEvent(url, method)
		require.Contains(t, resBody, "messageId")
	})

	t.Run("event-version-missing-keys", func(t *testing.T) {
		// GET /schemas/event-version/{VersionID}/metadata
		url := fmt.Sprintf("http://localhost:%s/schemas/event-version/%s/missing-keys", httpPort, VersionID)
		method := "GET"
		resBody, _ := getEvent(url, method)
		require.Contains(t, resBody, "originalTimestamp")
		require.Contains(t, resBody, "sentAt")
		require.Contains(t, resBody, "channel")
		require.Contains(t, resBody, "integrations.All")
	})

	t.Run("event-models-json-schemas", func(t *testing.T) {
		// GET /schemas/event-models/json-schemas
		url := fmt.Sprintf("http://localhost:%s/schemas/event-models/json-schemas", httpPort)
		method := "GET"
		resBody, _ := getEvent(url, method)
		require.Eventually(t, func() bool {
			// Similarly, pole until the Event Schema Tables are updated
			resBody, _ = getEvent(url, method)
			return resBody != "[]"
		}, time.Minute, 10*time.Millisecond)
		require.NotEqual(t, resBody, "[]")
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

	t.Run("warehouse-postgres-destination", func(t *testing.T) {
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
		sendWHEvents(t, whDestTest)
		whDestinationTest(t, whDestTest)

		whDestTest.UserId = "userId_postgres_1"
		sendUpdatedWHEvents(t, whDestTest)
		whDestinationTest(t, whDestTest)
	})

	t.Run("warehouse-clickhouse-destination", func(t *testing.T) {
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
		sendWHEvents(t, whDestTest)
		whDestinationTest(t, whDestTest)

		whDestTest.UserId = "userId_clickhouse_1"
		sendUpdatedWHEvents(t, whDestTest)
		whDestinationTest(t, whDestTest)
	})

	t.Run("warehouse-clickhouse-cluster-destination", func(t *testing.T) {
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
		sendWHEvents(t, whDestTest)
		whDestinationTest(t, whDestTest)

		initWHClickHouseClusterModeSetup(t)

		whDestTest.UserId = "userId_clickhouse_cluster_1"
		sendUpdatedWHEvents(t, whDestTest)

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
	})

	t.Run("warehouse-bigquery", func(t *testing.T) {
		if runBigQueryTest == false {
			t.Skip("Big query integration skipped. use -bigqueryintegration to add this test ")
		}
		if wht.Test.BQTest == nil {
			t.Log("Error in ENV variable BIGQUERY_INTEGRATION_TEST_USER_CRED")
			t.FailNow()
		}
		// Disabling big query dedup
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
		sendWHEvents(t, whDestTest)

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
		// Enabling big query dedup
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

		sendWHEvents(t, whDestTest)

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
	})

	t.Run("warehouse-mssql-destination", func(t *testing.T) {
		msSqlTest := wht.Test.MSSQLTest

		whDestTest := &wht.WareHouseDestinationTest{
			Client: &client.Client{
				SQL:  msSqlTest.DB,
				Type: client.SQLClient,
			},
			EventsCountMap:     msSqlTest.EventsMap,
			WriteKey:           msSqlTest.WriteKey,
			UserId:             "userId_mssql",
			Schema:             "mssql_wh_integration",
			TableTestQueryFreq: msSqlTest.TableTestQueryFreq,
		}
		sendWHEvents(t, whDestTest)
		whDestinationTest(t, whDestTest)

		whDestTest.UserId = "userId_mssql_1"
		sendUpdatedWHEvents(t, whDestTest)
		whDestinationTest(t, whDestTest)
	})

	blockOnHold(t)
	svcCancel()
	t.Log("Waiting for service to stop")
	<-svcDone

	tearDownStart = time.Now()
}

func setupMainFlow(svcCtx context.Context, t *testing.T) <-chan struct{} {
	setupStart := time.Now()

	config.Load()
	logger.Init()

	// uses a sensible default on windows (tcp/http) and linux/osx (socket)
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	if runtime.GOARCH != "arm64" || overrideArm64Check {
		kafkaContainer, err = destination.SetupKafka(pool, t,
			destination.WithLogger(&testLogger{logger.NewLogger().Child("kafka")}),
			destination.WithBrokers(3),
		)
		require.NoError(t, err)
	}

	redisContainer, err = destination.SetupRedis(pool, t)
	require.NoError(t, err)

	postgresContainer, err = destination.SetupPostgres(pool, t)
	require.NoError(t, err)
	db = postgresContainer.DB

	transformerContainer, err = destination.SetupTransformer(pool, t)
	require.NoError(t, err)

	waitUntilReady(
		context.Background(), t,
		fmt.Sprintf("%s/health", transformerContainer.TransformURL),
		time.Minute,
		time.Second,
		"transformer",
	)

	minioContainer, err = destination.SetupMINIO(pool, t)
	require.NoError(t, err)

	if err := godotenv.Load("testhelper/.env"); err != nil {
		t.Log("INFO: No .env file found.")
	}

	_ = os.Setenv("JOBS_DB_PORT", postgresContainer.Port)
	_ = os.Setenv("WAREHOUSE_JOBS_DB_PORT", postgresContainer.Port)
	_ = os.Setenv("DEST_TRANSFORM_URL", transformerContainer.TransformURL)
	_ = os.Setenv("DEPLOYMENT_TYPE", string(deployment.DedicatedType))

	wht.InitWHConfig()

	t.Cleanup(wht.SetWHPostgresDestination(pool))
	t.Cleanup(wht.SetWHClickHouseDestination(pool))
	t.Cleanup(wht.SetWHClickHouseClusterDestination(pool))
	t.Cleanup(wht.SetWHMssqlDestination(pool))
	t.Cleanup(wht.SetWHBigQueryDestination())

	addWHSpecificSqlFunctionsToJobsDb()

	httpPortInt, err := freeport.GetFreePort()
	require.NoError(t, err)

	httpPort = strconv.Itoa(httpPortInt)
	_ = os.Setenv("RSERVER_GATEWAY_WEB_PORT", httpPort)
	httpAdminPort, err := freeport.GetFreePort()
	require.NoError(t, err)

	_ = os.Setenv("RSERVER_GATEWAY_ADMIN_WEB_PORT", strconv.Itoa(httpAdminPort))
	_ = os.Setenv("RSERVER_ENABLE_STATS", "false")

	webhook = whUtil.NewRecorder()
	t.Cleanup(webhook.Close)
	webhookURL = webhook.Server.URL

	disableDestinationWebhook = whUtil.NewRecorder()
	t.Cleanup(disableDestinationWebhook.Close)
	disableDestinationWebhookURL = disableDestinationWebhook.Server.URL

	writeKey = randString(27)
	workspaceID = randString(27)
	mapWorkspaceConfig := map[string]string{
		"webhookUrl":                          webhookURL,
		"disableDestinationwebhookUrl":        disableDestinationWebhookURL,
		"writeKey":                            writeKey,
		"workspaceId":                         workspaceID,
		"postgresPort":                        postgresContainer.Port,
		"address":                             redisContainer.RedisAddress,
		"minioEndpoint":                       minioContainer.MinioEndpoint,
		"minioBucketName":                     minioContainer.MinioBucketName,
		"postgresEventWriteKey":               wht.Test.PGTest.WriteKey,
		"clickHouseEventWriteKey":             wht.Test.CHTest.WriteKey,
		"clickHouseClusterEventWriteKey":      wht.Test.CHClusterTest.WriteKey,
		"mssqlEventWriteKey":                  wht.Test.MSSQLTest.WriteKey,
		"rwhPostgresDestinationPort":          wht.Test.PGTest.Credentials.Port,
		"rwhClickHouseDestinationPort":        wht.Test.CHTest.Credentials.Port,
		"rwhClickHouseClusterDestinationPort": wht.Test.CHClusterTest.GetResource().Credentials.Port,
		"rwhMSSqlDestinationPort":             wht.Test.MSSQLTest.Credentials.Port,
	}
	if runtime.GOARCH != "arm64" || overrideArm64Check {
		mapWorkspaceConfig["kafkaPort"] = kafkaContainer.Port
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
	t.Cleanup(func() {
		if err := os.Remove(workspaceConfigPath); err != nil {
			t.Logf("Error while removing workspace config path: %v", err)
		}
	})
	t.Log("workspace config path:", workspaceConfigPath)
	_ = os.Setenv("RSERVER_BACKEND_CONFIG_CONFIG_JSONPATH", workspaceConfigPath)

	rudderTmpDir, err := os.MkdirTemp("", "rudder_server_test")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(rudderTmpDir) })
	_ = os.Setenv("RUDDER_TMPDIR", rudderTmpDir)

	t.Logf("--- Setup done (%s)", time.Since(setupStart))

	svcDone := make(chan struct{})
	go func() {
		main.Run(svcCtx)
		close(svcDone)
	}()

	serviceHealthEndpoint := fmt.Sprintf("http://localhost:%s/health", httpPort)
	t.Log("serviceHealthEndpoint", serviceHealthEndpoint)
	waitUntilReady(
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

func randString(n int) string {
	letters := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
	s := make([]rune, n)
	for i := range s {
		s[i] = letters[rand.Intn(len(letters))]
	}
	return string(s)
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

func waitUntilReady(ctx context.Context, t *testing.T, endpoint string, atMost, interval time.Duration, caller string) {
	t.Helper()
	probe := time.NewTicker(interval)
	timeout := time.After(atMost)
	for {
		select {
		case <-ctx.Done():
			return
		case <-timeout:
			t.Fatalf(
				"application was not ready after %s, for the end point: %s, caller: %s", atMost, endpoint, caller,
			)
		case <-probe.C:
			resp, err := http.Get(endpoint)
			if err != nil {
				continue
			}
			if resp.StatusCode == http.StatusOK {
				t.Log("application ready")
				return
			}
		}
	}
}

func blockOnHold(t *testing.T) {
	t.Helper()
	if !hold {
		return
	}

	t.Log("Test on hold, before cleanup")
	t.Log("Press Ctrl+C to exit")

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c
}

func getEvent(url, method string) (string, error) {
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
	defer func() { _ = res.Body.Close() }()

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
		go func(topic testutil.TopicPartition, consumer *kafkaClient.Consumer) {
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

// Adding necessary sql functions which needs to be used during warehouse testing
func addWHSpecificSqlFunctionsToJobsDb() {
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

func getMessageId(wdt *wht.WareHouseDestinationTest) string {
	if wdt.MessageId == "" {
		return uuid.Must(uuid.NewV4()).String()
	}
	return wdt.MessageId
}

// sendWHEvents Sending warehouse events
func sendWHEvents(t *testing.T, wdt *wht.WareHouseDestinationTest) {
	t.Helper()

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
			sendEvent(t, payloadIdentify, "identify", wdt.WriteKey)
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
			sendEvent(t, payloadTrack, "track", wdt.WriteKey)
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
			sendEvent(t, payloadPage, "page", wdt.WriteKey)
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
			sendEvent(t, payloadScreen, "screen", wdt.WriteKey)
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
			sendEvent(t, payloadAlias, "alias", wdt.WriteKey)
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
			sendEvent(t, payloadGroup, "group", wdt.WriteKey)
		}
	}
}

// sendUpdatedWHEvents Sending updated warehouse events
func sendUpdatedWHEvents(t *testing.T, wdt *wht.WareHouseDestinationTest) {
	t.Helper()

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
			sendEvent(t, payloadIdentify, "identify", wdt.WriteKey)
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
			sendEvent(t, payloadTrack, "track", wdt.WriteKey)
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
			sendEvent(t, payloadPage, "page", wdt.WriteKey)
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
			sendEvent(t, payloadScreen, "screen", wdt.WriteKey)
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
			sendEvent(t, payloadAlias, "alias", wdt.WriteKey)
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
			sendEvent(t, payloadGroup, "group", wdt.WriteKey)
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
