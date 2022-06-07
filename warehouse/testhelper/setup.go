package testhelper

import (
	"context"
	b64 "encoding/base64"
	"flag"
	"fmt"
	r "github.com/rudderlabs/rudder-server/cmd/run"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/warehouse/bigquery"
	"github.com/rudderlabs/rudder-server/warehouse/clickhouse"
	"github.com/rudderlabs/rudder-server/warehouse/deltalake"
	"github.com/rudderlabs/rudder-server/warehouse/mssql"
	"github.com/rudderlabs/rudder-server/warehouse/postgres"
	"github.com/rudderlabs/rudder-server/warehouse/redshift"
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
	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/stretchr/testify/require"
)

var (
	hold = true
)

var (
	WaitFor2Min           = 2 * time.Minute
	WaitFor5Min           = 5 * time.Minute
	DefaultQueryFrequency = 100 * time.Millisecond
)

var (
	httpPort      string
	httpAdminPort string
)

var (
	jobsDB *JobsDBResource
)

type ISetup interface {
	EnhanceWorkspaceConfig(map[string]string)
	SetUpDestination()
}

func Setup(m *testing.M, setup ISetup) int {
	// Getting hold flag variable
	flag.BoolVar(&hold, "hold", false, "hold environment clean-up after test execution until Ctrl+C is provided")
	flag.Parse()

	var tearDownStart time.Time
	defer func() {
		if tearDownStart == (time.Time{}) {
			log.Println("--- Teardown done (unexpected)")
		} else {
			log.Printf("--- Teardown done (%s)", time.Since(tearDownStart))
		}
	}()

	// Initializing config
	initialize()

	// Setting up jobsDB
	jobsDB = SetUpJobsDB()
	enhanceJobsDBWithSQLFunctions()

	// Setting up transformer
	transformer := SetupTransformer()

	// Setting up minio
	minio := SetupMinio()

	// Loading env variables
	if err := godotenv.Load("../testhelper/.env"); err != nil {
		log.Printf("Error occurred while loading .env with error: %v", err)
	}

	// Setting up destination
	setup.SetUpDestination()

	// Setting up env variables
	_ = os.Setenv("JOBS_DB_PORT", jobsDB.Credentials.Port)
	_ = os.Setenv("WAREHOUSE_JOBS_DB_PORT", jobsDB.Credentials.Port)
	_ = os.Setenv("DEST_TRANSFORM_URL", transformer.Url)
	_ = os.Setenv("DATABRICKS_CONNECTOR_URL", "localhost:54330")

	// Getting free http port for running rudder-server
	httpPortInt, err := freeport.GetFreePort()
	if err != nil {
		log.Panicf("error occurred while getting free port for http: %v", err)
		return 0
	}
	httpPort = strconv.Itoa(httpPortInt)

	// Getting free ports for http admin
	httpAdminPortInt, err := freeport.GetFreePort()
	if err != nil {
		log.Panicf("error occurred while getting free port for http admin: %v", err)
		return 0
	}
	httpAdminPort = strconv.Itoa(httpAdminPortInt)

	// Setting up workspace config
	workspaceID := RandString(27)
	workspaceConfigMap := map[string]string{
		"workspaceId":     workspaceID,
		"minioEndpoint":   minio.MinioEndpoint,
		"minioBucketName": minio.MinioBucketName,
	}
	setup.EnhanceWorkspaceConfig(workspaceConfigMap)

	// Setting up workspace
	workspaceConfigPath := CreateWorkspaceConfig("../testdata/workspaceConfig/template.json", workspaceConfigMap)
	log.Println("workspace config path:", workspaceConfigPath)
	defer func() {
		err := os.Remove(workspaceConfigPath)
		log.Println(err)
	}()

	// Setting up warehouse temp directory
	rudderTmpDir, err := os.MkdirTemp("", "rudder_warehouse_test")
	if err != nil {
		log.Panicf("error occurred while creating temp directory for warehouse test with error: %v", err)
		return 0
	}
	defer func() { _ = os.RemoveAll(rudderTmpDir) }()

	// Setting up env variables
	_ = os.Setenv("RSERVER_GATEWAY_WEB_PORT", httpPort)
	_ = os.Setenv("RSERVER_GATEWAY_ADMIN_WEB_PORT", httpAdminPort)
	_ = os.Setenv("RSERVER_ENABLE_STATS", "false")
	_ = os.Setenv("RSERVER_BACKEND_CONFIG_CONFIG_JSONPATH", workspaceConfigPath)
	_ = os.Setenv("RUDDER_TMPDIR", rudderTmpDir)

	// Starting rudder server and rudder warehouse
	svcCtx, svcCancel := context.WithCancel(context.Background())
	svcDone := make(chan struct{})
	go func() {
		r.Run(svcCtx)
		close(svcDone)
	}()

	// Checking health endpoint for rudder server
	healthEndpoint := fmt.Sprintf("http://localhost:%s/health", httpPort)
	log.Println("healthEndpoint", healthEndpoint)
	WaitUntilReady(context.Background(), healthEndpoint, WaitFor2Min, time.Second, "healthEndpoint")

	// running module tests
	code := m.Run()
	blockOnHold()

	svcCancel()
	log.Println("waiting for service to stop")
	<-svcDone

	tearDownStart = time.Now()
	return code
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

func sendEvent(payload *strings.Reader, eventType, writeKey string) {
	log.Printf("Sending event: %s for writeKey: %s", eventType, writeKey)

	url := fmt.Sprintf("http://localhost:%s/v1/%s", httpPort, eventType)
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

	_, err = io.ReadAll(res.Body)
	if err != nil {
		log.Printf("Error occurred while reading http response for sending event with error: %s", err.Error())
		return
	}
	if res.Status != "200 OK" {
		return
	}

	log.Printf("Send successfully for event: %s and writeKey: %s", eventType, writeKey)
}

func enhanceJobsDBWithSQLFunctions() {
	var err error

	_, err = jobsDB.DB.Exec(GWJobsForUserIdWriteKey())
	if err != nil {
		log.Panicf("error occurred with executing gw jobs function for events count with err %v", err.Error())
	}

	_, err = jobsDB.DB.Exec(BRTJobsForUserId())
	if err != nil {
		log.Panicf("error occurred with executing brt jobs function for events count with err %s", err.Error())
	}
}

func SendEvents(t testing.TB, wdt *WareHouseDestinationTest) {
	if identify, exists := wdt.EventsCountMap["identifies"]; exists {
		t.Logf("Sending identifies events")
		for i := 0; i < identify; i++ {
			payloadIdentify := strings.NewReader(fmt.Sprintf(IdentifyPayload, wdt.UserId, wdt.MsgId()))
			sendEvent(payloadIdentify, "identify", wdt.WriteKey)
		}
	}

	if track, exists := wdt.EventsCountMap["tracks"]; exists {
		t.Logf("Sending tracks events")
		for i := 0; i < track; i++ {
			payloadTrack := strings.NewReader(fmt.Sprintf(TrackPayload, wdt.UserId, wdt.MsgId(), wdt.Event))
			sendEvent(payloadTrack, "track", wdt.WriteKey)
		}
	}

	if page, exists := wdt.EventsCountMap["pages"]; exists {
		t.Logf("Sending pages events")
		for i := 0; i < page; i++ {
			payloadPage := strings.NewReader(fmt.Sprintf(PagePayload, wdt.UserId, wdt.MsgId()))
			sendEvent(payloadPage, "page", wdt.WriteKey)
		}
	}

	if screen, exists := wdt.EventsCountMap["screens"]; exists {
		t.Logf("Sending screens events")
		for i := 0; i < screen; i++ {
			payloadScreen := strings.NewReader(fmt.Sprintf(ScreenPayload, wdt.UserId, wdt.MsgId()))
			sendEvent(payloadScreen, "screen", wdt.WriteKey)
		}
	}

	if alias, exists := wdt.EventsCountMap["aliases"]; exists {
		t.Logf("Sending aliases events")
		for i := 0; i < alias; i++ {
			payloadAlias := strings.NewReader(fmt.Sprintf(AliasPayload, wdt.UserId, wdt.MsgId()))
			sendEvent(payloadAlias, "alias", wdt.WriteKey)
		}
	}

	if group, exists := wdt.EventsCountMap["groups"]; exists {
		t.Logf("Sending groups events")
		for i := 0; i < group; i++ {
			payloadGroup := strings.NewReader(fmt.Sprintf(GroupPayload, wdt.UserId, wdt.MsgId()))
			sendEvent(payloadGroup, "group", wdt.WriteKey)
		}
	}
}

func SendModifiedEvents(t testing.TB, wdt *WareHouseDestinationTest) {
	if identify, exists := wdt.EventsCountMap["identifies"]; exists {
		t.Logf("Sending modified identifies events")
		for i := 0; i < identify; i++ {
			payloadIdentify := strings.NewReader(fmt.Sprintf(ModifiedIdentifyPayload, wdt.UserId, uuid.Must(uuid.NewV4()).String()))
			sendEvent(payloadIdentify, "identify", wdt.WriteKey)
		}
	}

	if track, exists := wdt.EventsCountMap["tracks"]; exists {
		t.Logf("Sending modified tracks events")
		for i := 0; i < track; i++ {
			payloadTrack := strings.NewReader(fmt.Sprintf(ModifiedTrackPayload, wdt.UserId, uuid.Must(uuid.NewV4()).String(), wdt.Event))
			sendEvent(payloadTrack, "track", wdt.WriteKey)
		}
	}

	if page, exists := wdt.EventsCountMap["pages"]; exists {
		t.Logf("Sending modified pages events")
		for i := 0; i < page; i++ {
			payloadPage := strings.NewReader(fmt.Sprintf(ModifiedPagePayload, wdt.UserId, uuid.Must(uuid.NewV4()).String()))
			sendEvent(payloadPage, "page", wdt.WriteKey)
		}
	}

	if screen, exists := wdt.EventsCountMap["screens"]; exists {
		t.Logf("Sending modified screens events")
		for i := 0; i < screen; i++ {
			payloadScreen := strings.NewReader(fmt.Sprintf(ModifiedScreenPayload, wdt.UserId, uuid.Must(uuid.NewV4()).String()))
			sendEvent(payloadScreen, "screen", wdt.WriteKey)
		}
	}

	if alias, exists := wdt.EventsCountMap["aliases"]; exists {
		t.Logf("Sending modified aliases events")
		for i := 0; i < alias; i++ {
			payloadAlias := strings.NewReader(fmt.Sprintf(ModifiedAliasPayload, wdt.UserId, uuid.Must(uuid.NewV4()).String()))
			sendEvent(payloadAlias, "alias", wdt.WriteKey)
		}
	}

	if group, exists := wdt.EventsCountMap["groups"]; exists {
		t.Logf("Sending modified groups events")
		for i := 0; i < group; i++ {
			payloadGroup := strings.NewReader(fmt.Sprintf(ModifiedGroupPayload, wdt.UserId, uuid.Must(uuid.NewV4()).String()))
			sendEvent(payloadGroup, "group", wdt.WriteKey)
		}
	}

}

func VerifyingDestination(t testing.TB, wdt *WareHouseDestinationTest) {
	t.Logf("Started verifying gateway events")
	VerifyingGatewayEvents(t, wdt)
	t.Logf("Completed verifying gateway events")

	t.Logf("Started verifying batch router events")
	VerifyingBatchRouterEvents(t, wdt)
	t.Logf("Completed verifying batch router events")

	t.Logf("Started verifying tables events")
	VerifyingTablesEventCount(t, wdt)
	t.Logf("Completed verifying tables events")
}

func VerifyingGatewayEvents(t testing.TB, wdt *WareHouseDestinationTest) {
	require.Contains(t, wdt.EventsCountMap, "gateway")
	gwEvents := wdt.EventsCountMap["gateway"]

	var count int64

	sqlStatement := fmt.Sprintf(`select count(*) from gw_jobs_for_user_id_and_write_key('%s', '%s') as job_ids`, wdt.UserId, wdt.WriteKey)
	t.Logf("Checking for the gateway jobs for sqlStatement: %s", sqlStatement)
	operation := func() bool {
		err := jobsDB.DB.QueryRow(sqlStatement).Scan(&count)
		require.Equal(t, err, nil)
		return count == int64(gwEvents)
	}
	require.Eventually(t, operation, WaitFor2Min, DefaultQueryFrequency)

	var jobIds []string
	var jobId int64

	sqlStatement = fmt.Sprintf(`select * from gw_jobs_for_user_id_and_write_key('%s', '%s') as job_ids`, wdt.UserId, wdt.WriteKey)
	t.Logf("Checking for gateway job ids for sqlStatement: %s", sqlStatement)
	operation = func() bool {
		jobIds = make([]string, 0)

		rows, err := jobsDB.DB.Query(sqlStatement)
		require.Equal(t, err, nil)

		defer func() { _ = rows.Close() }()

		for rows.Next() {
			err = rows.Scan(&jobId)
			require.Equal(t, err, nil)
			jobIds = append(jobIds, fmt.Sprint(jobId))
		}
		return gwEvents == len(jobIds)
	}
	require.Eventually(t, operation, WaitFor2Min, DefaultQueryFrequency)

	sqlStatement = fmt.Sprintf("select count(*) from gw_job_status_1 where job_id in (%s) and job_state = 'succeeded'", strings.Join(jobIds, ","))
	t.Logf("Checking for gateway jobs state for sqlStatement: %s", sqlStatement)
	operation = func() bool {
		err := jobsDB.DB.QueryRow(sqlStatement).Scan(&count)
		require.Equal(t, nil, err)
		return count == int64(gwEvents)
	}
	require.Eventually(t, operation, WaitFor2Min, DefaultQueryFrequency)
}

func VerifyingBatchRouterEvents(t testing.TB, wdt *WareHouseDestinationTest) {
	require.Contains(t, wdt.EventsCountMap, "batchRT")
	brtEvents := wdt.EventsCountMap["batchRT"]

	var count int64

	sqlStatement := fmt.Sprintf(`select count(*) from brt_jobs_for_user_id('%s') as job_ids`, wdt.UserId)
	t.Logf("Checking for batch router jobs for sqlStatement: %s", sqlStatement)
	operation := func() bool {
		err := jobsDB.DB.QueryRow(sqlStatement).Scan(&count)
		require.Equal(t, err, nil)
		return count == int64(brtEvents)
	}
	require.Eventually(t, operation, WaitFor2Min, DefaultQueryFrequency)

	var jobIds []string
	var jobId int64

	sqlStatement = fmt.Sprintf(`select * from brt_jobs_for_user_id('%s') as job_ids`, wdt.UserId)
	t.Logf("Checking for batch router job ids for sqlStatement: %s", sqlStatement)
	operation = func() bool {
		jobIds = make([]string, 0)

		rows, err := jobsDB.DB.Query(sqlStatement)
		require.Equal(t, err, nil)

		defer func() { _ = rows.Close() }()

		for rows.Next() {
			err = rows.Scan(&jobId)
			require.Equal(t, err, nil)
			jobIds = append(jobIds, fmt.Sprint(jobId))
		}
		return brtEvents == len(jobIds)
	}
	require.Eventually(t, operation, WaitFor2Min, DefaultQueryFrequency)

	// Checking for the batch router jobs state
	sqlStatement = fmt.Sprintf("select count(*) from batch_rt_job_status_1 where job_id in (%s) and job_state = 'succeeded'", strings.Join(jobIds, ","))
	t.Logf("Checking for batch router jobs state for sqlStatement: %s", sqlStatement)
	operation = func() bool {
		err := jobsDB.DB.QueryRow(sqlStatement).Scan(&count)
		require.Equal(t, err, nil)
		return count == int64(brtEvents)
	}
	require.Eventually(t, operation, WaitFor2Min, DefaultQueryFrequency)
}

func VerifyingTablesEventCount(t testing.TB, wdt *WareHouseDestinationTest) {
	var count int64
	for idx, table := range wdt.Tables {
		sqlStatement := fmt.Sprintf("select count(*) from %s.%s where %s = '%s'", wdt.Schema, table, wdt.PrimaryKeys[idx], wdt.UserId)
		t.Logf("Verifying tables event count for sqlStatement: %s", sqlStatement)

		require.Contains(t, wdt.EventsCountMap, table)
		tableCount := wdt.EventsCountMap[table]

		condition := func() bool {
			count, _ = queryCount(wdt.Client, sqlStatement)
			return count == int64(tableCount)
		}
		require.Eventually(t, condition, WaitFor5Min, wdt.VerifyingTablesFrequency)
	}
}

func queryCount(cl *client.Client, statement string) (int64, error) {
	result, err := cl.Query(statement, client.Read)
	if err != nil || result.Values == nil {
		return 0, err
	}
	return strconv.ParseInt(result.Values[0][0], 10, 64)
}

func SetUpJobsDB() (jobsDB *JobsDBResource) {
	pgCredentials := &postgres.CredentialsT{
		DBName:   "jobsdb",
		Password: "password",
		User:     "rudder",
		Host:     "localhost",
		SSLMode:  "disable",
		Port:     "54328",
	}
	jobsDB = &JobsDBResource{}
	jobsDB.Credentials = pgCredentials

	var err error
	if jobsDB.DB, err = postgres.Connect(*pgCredentials); err != nil {
		log.Fatalf("could not connect to jobsDb with error: %s", err.Error())
	}
	if err = jobsDB.DB.Ping(); err != nil {
		log.Fatalf("could not connect to jobsDb while pinging with error: %s", err.Error())
	}
	return
}

func initialize() {
	config.Load()
	logger.Init()
	stats.Init()
	stats.Setup()
	postgres.Init()
	clickhouse.Init()
	mssql.Init()
	bigquery.Init()
	snowflake.Init()
	redshift.Init()
	deltalake.Init()
}
