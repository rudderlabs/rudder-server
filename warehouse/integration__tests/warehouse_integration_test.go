package integration__tests

import (
	"context"
	b64 "encoding/base64"
	"flag"
	"fmt"
	"github.com/rudderlabs/rudder-server/warehouse/bigquery"
	"github.com/rudderlabs/rudder-server/warehouse/clickhouse"
	"github.com/rudderlabs/rudder-server/warehouse/integration__tests/testhelper"
	"github.com/rudderlabs/rudder-server/warehouse/mssql"
	"github.com/rudderlabs/rudder-server/warehouse/postgres"
	"github.com/rudderlabs/rudder-server/warehouse/snowflake"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/gofrs/uuid"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
	"github.com/phayes/freeport"
	r "github.com/rudderlabs/rudder-server/cmd/run"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/stretchr/testify/require"
)

var (
	hold = true
)

var (
	runIntegration   bool
	runBigQueryTest  bool
	runSnowflakeTest bool
	runRedhshiftTest bool
)

var (
	httpPort      string
	httpAdminPort string
)

var (
	PGTest                     *testhelper.PostgresTest
	BQTest                     *testhelper.BiqQueryTest
	CHTest                     *testhelper.ClickHouseTest
	CHClusterTest              *testhelper.ClickHouseClusterTest
	MSSQLTest                  *testhelper.MSSQLTest
	SFTest                     *testhelper.SnowflakeTest
	RSTest                     *testhelper.RedshiftTest
	JobsDBResource             *testhelper.JobsDBResource
	GatewayJobsSqlFunction     string
	BatchRouterJobsSqlFunction string
)

func TestMain(m *testing.M) {
	flag.BoolVar(&hold, "hold", false, "hold environment clean-up after test execution until Ctrl+C is provided")
	flag.BoolVar(&runIntegration, "integration", false, "run integration level tests")
	flag.BoolVar(&runBigQueryTest, "bigqueryintegration", false, "run big query test")
	flag.BoolVar(&runSnowflakeTest, "snowflakeintegration", false, "run snowflake test")
	flag.BoolVar(&runRedhshiftTest, "redshiftintegration", false, "run redshift test")
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
			log.Printf("--- Teardown done (unexpected)\n")
		} else {
			log.Printf("--- Teardown done (%s)\n", time.Since(tearDownStart))
		}
	}()

	config.Load()
	logger.Init()

	// Setting up JobsDB
	JobsDBResource = SetUpJobsDB()

	// Setting up transformer
	transformerResource := testhelper.SetupTransformer()

	// Setting up Minio
	minioResource := testhelper.SetupMinio()

	// Loading env variables
	if err := godotenv.Load("testhelper/.env"); err != nil {
		log.Println("Error occurred while loading .env file.")
	}

	// Setting up env variables
	_ = os.Setenv("JOBS_DB_PORT", JobsDBResource.Credentials.Port)
	_ = os.Setenv("WAREHOUSE_JOBS_DB_PORT", JobsDBResource.Credentials.Port)
	_ = os.Setenv("DEST_TRANSFORM_URL", transformerResource.Url)

	// Initializing warehouse config
	InitConfig()

	// Setting up warehouse tests
	PGTest = SetUpPostgres()
	CHTest = SetupClickHouse()
	CHClusterTest = SetupClickHouseCluster()
	MSSQLTest = SetupMSSQL()
	BQTest = SetupBigQuery()
	SFTest = SetupSnowflake()
	RSTest = SetupRedshift()

	// Adding sql functions to jobs DB
	addSqlFunctionToJobsDB()

	// Getting free ports for http
	httpPortInt, err := freeport.GetFreePort()
	if err != nil {
		return 0, fmt.Errorf("could not get free port for http: %w", err)
	}
	httpPort = strconv.Itoa(httpPortInt)

	// Getting free ports for http admin
	httpAdminPortInt, err := freeport.GetFreePort()
	if err != nil {
		return 0, fmt.Errorf("could not get free port for http admin: %w", err)
	}
	httpAdminPort = strconv.Itoa(httpAdminPortInt)

	// Setting up workspace config
	workspaceID := testhelper.RandString(27)
	mapWorkspaceConfig := map[string]string{
		"workspaceId":               workspaceID,
		"minioEndpoint":             minioResource.MinioEndpoint,
		"minioBucketName":           minioResource.MinioBucketName,
		"postgresWriteKey":          PGTest.WriteKey,
		"clickHouseWriteKey":        CHTest.WriteKey,
		"clickHouseClusterWriteKey": CHClusterTest.WriteKey,
		"mssqlWriteKey":             MSSQLTest.WriteKey,
		"postgresPort":              PGTest.Credentials.Port,
		"clickHousePort":            CHTest.Credentials.Port,
		"clickHouseClusterPort":     CHClusterTest.GetResource().Credentials.Port,
		"mssqlPort":                 MSSQLTest.Credentials.Port,
	}

	if runBigQueryTest {
		mapWorkspaceConfig["bqEventWriteKey"] = BQTest.WriteKey
		mapWorkspaceConfig["bqProject"] = BQTest.Credentials.ProjectID
		mapWorkspaceConfig["bqLocation"] = BQTest.Credentials.Location
		mapWorkspaceConfig["bqBucketName"] = BQTest.Credentials.Bucket
		mapWorkspaceConfig["bqCredentials"] = BQTest.Credentials.CredentialsEscaped
	}
	if runSnowflakeTest {
		mapWorkspaceConfig["snowflakeEventWriteKey"] = SFTest.WriteKey
		mapWorkspaceConfig["snowflakeAccount"] = SFTest.Credentials.Account
		mapWorkspaceConfig["snowflakeDatabase"] = SFTest.Credentials.Database
		mapWorkspaceConfig["snowflakeWarehouse"] = SFTest.Credentials.Warehouse
		mapWorkspaceConfig["snowflakeUser"] = SFTest.Credentials.User
		mapWorkspaceConfig["snowflakePassword"] = SFTest.Credentials.Password
		mapWorkspaceConfig["snowflakeBucketName"] = SFTest.Credentials.BucketName
		mapWorkspaceConfig["snowflakeAccesskeyID"] = SFTest.Credentials.AccessKeyID
		mapWorkspaceConfig["snowflakeAccesskey"] = SFTest.Credentials.AccessKey
	}
	if runRedhshiftTest {
		mapWorkspaceConfig["redshiftEventWriteKey"] = RSTest.WriteKey
		mapWorkspaceConfig["redshiftHost"] = RSTest.Credentials.Host
		mapWorkspaceConfig["redshiftPort"] = RSTest.Credentials.Port
		mapWorkspaceConfig["redshiftDatabase"] = RSTest.Credentials.Database
		mapWorkspaceConfig["redshiftUser"] = RSTest.Credentials.User
		mapWorkspaceConfig["redshiftPassword"] = RSTest.Credentials.Password
		mapWorkspaceConfig["redshiftBucketName"] = RSTest.Credentials.BucketName
		mapWorkspaceConfig["redshiftAccessKeyID"] = RSTest.Credentials.AccessKeyID
		mapWorkspaceConfig["redshiftAccessKey"] = RSTest.Credentials.AccessKey
	}

	workspaceConfigPath := testhelper.CreateWorkspaceConfig("testdata/workspaceConfig/template.json", mapWorkspaceConfig)
	log.Println("workspace config path:", workspaceConfigPath)
	defer func() {
		err := os.Remove(workspaceConfigPath)
		log.Println(err)
	}()

	// Setting up env variables
	_ = os.Setenv("RSERVER_GATEWAY_WEB_PORT", httpPort)
	_ = os.Setenv("RSERVER_GATEWAY_ADMIN_WEB_PORT", httpAdminPort)
	_ = os.Setenv("RSERVER_ENABLE_STATS", "false")
	_ = os.Setenv("RSERVER_BACKEND_CONFIG_CONFIG_JSONPATH", workspaceConfigPath)

	rudderTmpDir, err := os.MkdirTemp("", "rudder_server_test")
	if err != nil {
		return 0, err
	}
	defer func() { _ = os.RemoveAll(rudderTmpDir) }()
	_ = os.Setenv("RUDDER_TMPDIR", rudderTmpDir)

	log.Printf("--- Setup done (%s)\n", time.Since(setupStart))

	svcCtx, svcCancel := context.WithCancel(context.Background())
	svcDone := make(chan struct{})
	go func() {
		r.Run(svcCtx)
		close(svcDone)
	}()

	// Waiting until health endpoint is ready
	healthEndpoint := fmt.Sprintf("http://localhost:%s/health", httpPort)
	log.Println("healthEndpoint", healthEndpoint)
	testhelper.WaitUntilReady(context.Background(), healthEndpoint, time.Minute, time.Second, "healthEndpoint")

	code := m.Run()
	blockOnHold()

	svcCancel()
	log.Println("waiting for service to stop")
	<-svcDone

	tearDownStart = time.Now()
	return code, nil
}

// blockOnHold block until Ctrl+C is presed
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

// InitConfig Initialize warehouse config
func InitConfig() {
	GatewayJobsSqlFunction = testhelper.GatewayJobsSqlFunction()
	BatchRouterJobsSqlFunction = testhelper.BatchRouterJobsSqlFunction()

	config.Load()
	logger.Init()
	postgres.Init()
	clickhouse.Init()
	mssql.Init()
	bigquery.Init()
	snowflake.Init()
}

// sendEvent send event with corresponding payload and writeKey
func sendEvent(payload *strings.Reader, callType string, writeKey string) {
	log.Println(fmt.Sprintf("Sending %s Event", callType))

	url := fmt.Sprintf("http://localhost:%s/v1/%s", httpPort, callType)
	method := "POST"
	httpClient := &http.Client{}
	req, err := http.NewRequest(method, url, payload)

	if err != nil {
		log.Printf("Error occurred while creating new http request for sending event with error: %s", err.Error())
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
		log.Printf("Error occurred while making http request for sending event with error: %s", err.Error())
		return
	}
	defer func() { _ = res.Body.Close() }()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		log.Printf("Error occurred while reading http response for sending event with error: %s", err.Error())
		return
	}
	if res.Status != "200 OK" {
		return
	}
	log.Println(string(body))
	log.Println("Event Sent Successfully")
}

// addSqlFunctionToJobsDB Adding sql functions to JobsDB
func addSqlFunctionToJobsDB() {
	var err error
	_, err = JobsDBResource.DB.Exec(GatewayJobsSqlFunction)
	if err != nil {
		panic(fmt.Errorf("error occurred with executing gw jobs function for events count with err %v", err.Error()))
	}

	_, err = JobsDBResource.DB.Exec(BatchRouterJobsSqlFunction)
	if err != nil {
		panic(fmt.Errorf("error occurred with executing brt jobs function for events count with err %s", err.Error()))
	}
}

// sendEvents Sending warehouse events
func sendEvents(wdt *testhelper.WareHouseDestinationTest) {
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
		  }`, wdt.UserId, wdt.MsgId()))
			sendEvent(payloadIdentify, "identify", wdt.WriteKey)
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
		  }`, wdt.UserId, wdt.MsgId()))
			sendEvent(payloadTrack, "track", wdt.WriteKey)
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
		  }`, wdt.UserId, wdt.MsgId()))
			sendEvent(payloadPage, "page", wdt.WriteKey)
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
		  }`, wdt.UserId, wdt.MsgId()))
			sendEvent(payloadScreen, "screen", wdt.WriteKey)
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
		  }`, wdt.UserId, wdt.MsgId()))
			sendEvent(payloadAlias, "alias", wdt.WriteKey)
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
		  }`, wdt.UserId, wdt.MsgId()))
			sendEvent(payloadGroup, "group", wdt.WriteKey)
		}
	}
}

// sendUpdatedEvents Sending updated warehouse events
func sendUpdatedEvents(wdt *testhelper.WareHouseDestinationTest) {
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
			sendEvent(payloadIdentify, "identify", wdt.WriteKey)
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
			sendEvent(payloadTrack, "track", wdt.WriteKey)
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
			sendEvent(payloadPage, "page", wdt.WriteKey)
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
			sendEvent(payloadScreen, "screen", wdt.WriteKey)
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
			sendEvent(payloadAlias, "alias", wdt.WriteKey)
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
			sendEvent(payloadGroup, "group", wdt.WriteKey)
		}
	}
}

// destinationTest Checking warehouse destination
func destinationTest(t *testing.T, wdt *testhelper.WareHouseDestinationTest) {
	gatewayTest(t, wdt)
	batchRouterTest(t, wdt)
	tablesTest(t, wdt)
}

// gatewayTest Checking for gateway jobs
func gatewayTest(t *testing.T, wdt *testhelper.WareHouseDestinationTest) {
	require.Contains(t, wdt.EventsCountMap, "gateway")
	gwEvents := wdt.EventsCountMap["gateway"]
	// Checking for the gateway jobs
	t.Log("Checking for the gateway jobs")
	require.Eventually(t, func() bool {
		var count int64
		jobSqlStatement := fmt.Sprintf(`select count(*) from gw_jobs_for_user_id_and_write_key('%s', '%s') as job_ids`, wdt.UserId, wdt.WriteKey)
		err := JobsDBResource.DB.QueryRow(jobSqlStatement).Scan(&count)
		require.Equal(t, err, nil)
		return count == int64(gwEvents)
	}, time.Minute, 10*time.Millisecond)

	// Getting gateway job ids
	t.Log("Getting gateway job ids")
	var jobIds []string
	require.Eventually(t, func() bool {
		jobIds = make([]string, 0)
		jobSqlStatement := fmt.Sprintf(`select * from gw_jobs_for_user_id_and_write_key('%s', '%s') as job_ids`, wdt.UserId, wdt.WriteKey)
		rows, err := JobsDBResource.DB.Query(jobSqlStatement)
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
		err := JobsDBResource.DB.QueryRow(jobsSqlStatement).Scan(&count)
		require.Equal(t, nil, err)
		return count == int64(gwEvents)
	}, time.Minute, 10*time.Millisecond)
}

// batchRouterTest Checking for batch router jobs
func batchRouterTest(t *testing.T, wdt *testhelper.WareHouseDestinationTest) {
	require.Contains(t, wdt.EventsCountMap, "batchRT")
	brtEvents := wdt.EventsCountMap["batchRT"]

	// Checking for the batch router jobs
	t.Log("Checking for the batch router jobs")
	require.Eventually(t, func() bool {
		var count int64
		jobsSqlStatement := fmt.Sprintf(`select count(*) from brt_jobs_for_user_id('%s') as job_ids`, wdt.UserId)
		err := JobsDBResource.DB.QueryRow(jobsSqlStatement).Scan(&count)
		require.Equal(t, err, nil)
		return count == int64(brtEvents)
	}, 2*time.Minute, 100*time.Millisecond)

	// Getting batch router job ids
	t.Log("Getting batch router job ids")
	var jobIds []string
	require.Eventually(t, func() bool {
		jobIds = make([]string, 0)
		jobSqlStatement := fmt.Sprintf(`select * from brt_jobs_for_user_id('%s') as job_ids`, wdt.UserId)
		rows, err := JobsDBResource.DB.Query(jobSqlStatement)
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
		err := JobsDBResource.DB.QueryRow(jobsSqlStatement).Scan(&count)
		require.Equal(t, err, nil)
		return count == int64(brtEvents)
	}, 2*time.Minute, 100*time.Millisecond)
}

// tablesTest Checking warehouse
func tablesTest(t *testing.T, wdt *testhelper.WareHouseDestinationTest) {
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
			count, _ = queryCount(wdt.Client, sqlStatement)
			return count == int64(tableCount)
		}, 2*time.Minute, wdt.TableTestQueryFreq)
	}
}

// queryCount returns count of rows provided by the sql statement
func queryCount(cl *client.Client, statement string) (int64, error) {
	result, err := cl.Query(statement, client.Read)
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(result.Values[0][0], 10, 64)
}
