package testhelper

import (
	"fmt"
	"github.com/joho/godotenv"
	"log"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gofrs/uuid"
	"github.com/iancoleman/strcase"
	azuresynapse "github.com/rudderlabs/rudder-server/warehouse/azure-synapse"
	"github.com/rudderlabs/rudder-server/warehouse/datalake"

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

	_ "github.com/lib/pq"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/stretchr/testify/require"
)

var (
	WaitFor2Minute            = 2 * time.Minute
	WaitFor5Minute            = 5 * time.Minute
	DefaultQueryFrequency     = 100 * time.Millisecond
	LongRunningQueryFrequency = 5000 * time.Millisecond
)

var (
	ConnectBackoffDuration = 1 * time.Second
	ConnectBackoffRetryMax = 5
)

var jobsDB *JobsDBResource

var (
	SnowflakeIntegrationTestUserCred  = "SNOWFLAKE_INTEGRATION_TEST_USER_CRED"
	RedshiftIntegrationTestUserCred   = "REDSHIFT_INTEGRATION_TEST_USER_CRED"
	DatabricksIntegrationTestUserCred = "DATABRICKS_INTEGRATION_TEST_USER_CRED"
)

func (w *WareHouseDestinationTest) MsgId() string {
	if w.MessageId == "" {
		return uuid.Must(uuid.NewV4()).String()
	}
	return w.MessageId
}

func (w *WareHouseDestinationTest) Reset(destType string, randomEvent bool) {
	randomness := strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", "")
	w.UserId = fmt.Sprintf("userId_%s_%s", strings.ToLower(destType), randomness)

	if randomEvent {
		w.Event = fmt.Sprintf("Product Track %s", randomness)
	} else {
		w.Event = "Product Track"
	}
	w.EventsCountMap[strcase.ToSnake(w.Event)] = 1
	w.Tables = []string{"identifies", "users", "tracks", strcase.ToSnake(w.Event), "pages", "screens", "aliases", "groups"}
}

func Run(m *testing.M, setup ISetup) int {
	initialize()

	jobsDB = setUpJobsDB()
	enhanceJobsDBWithSQLFunctions()

	setup.SetUpDestination()

	return m.Run()
}

func loadEnv() {
	if err := godotenv.Load("../testhelper/.env"); err != nil {
		fmt.Printf("Error occurred while loading env for warehouse integration test with error: %s", err.Error())
	}
}

func initialize() {
	loadEnv()

	config.Load()
	logger.Init()

	stats.Init()
	stats.Setup()

	azuresynapse.Init()
	bigquery.Init()
	clickhouse.Init()
	datalake.Init()
	deltalake.Init()
	mssql.Init()
	postgres.Init()
	redshift.Init()
	snowflake.Init()
}

func setUpJobsDB() (jobsDB *JobsDBResource) {
	pgCredentials := &postgres.CredentialsT{
		DBName:   "jobsdb",
		Password: "password",
		User:     "rudder",
		Host:     "jobsDb",
		SSLMode:  "disable",
		Port:     "5432",
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
	require.Eventually(t, operation, WaitFor2Minute, DefaultQueryFrequency)

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
	require.Eventually(t, operation, WaitFor2Minute, DefaultQueryFrequency)

	sqlStatement = fmt.Sprintf("select count(*) from gw_job_status_1 where job_id in (%s) and job_state = 'succeeded'", strings.Join(jobIds, ","))
	t.Logf("Checking for gateway jobs state for sqlStatement: %s", sqlStatement)
	operation = func() bool {
		err := jobsDB.DB.QueryRow(sqlStatement).Scan(&count)
		require.Equal(t, nil, err)
		return count == int64(gwEvents)
	}
	require.Eventually(t, operation, WaitFor2Minute, DefaultQueryFrequency)
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
	require.Eventually(t, operation, WaitFor2Minute, DefaultQueryFrequency)

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
	require.Eventually(t, operation, WaitFor2Minute, DefaultQueryFrequency)

	// Checking for the batch router jobs state
	sqlStatement = fmt.Sprintf("select count(*) from batch_rt_job_status_1 where job_id in (%s) and job_state = 'succeeded'", strings.Join(jobIds, ","))
	t.Logf("Checking for batch router jobs state for sqlStatement: %s", sqlStatement)
	operation = func() bool {
		err := jobsDB.DB.QueryRow(sqlStatement).Scan(&count)
		require.Equal(t, err, nil)
		return count == int64(brtEvents)
	}
	require.Eventually(t, operation, WaitFor2Minute, DefaultQueryFrequency)
}

func VerifyingTablesEventCount(t testing.TB, wdt *WareHouseDestinationTest) {
	var count int64
	for _, table := range wdt.Tables {
		primaryKey := "user_id"
		if table == "users" {
			primaryKey = "id"
		}

		sqlStatement := fmt.Sprintf("select count(*) from %s.%s where %s = '%s'", wdt.Schema, table, primaryKey, wdt.UserId)
		t.Logf("Verifying tables event count for sqlStatement: %s", sqlStatement)

		require.Contains(t, wdt.EventsCountMap, table)
		tableCount := wdt.EventsCountMap[table]

		condition := func() bool {
			count, _ = queryCount(wdt.Client, sqlStatement)
			return count == int64(tableCount)
		}
		require.Eventually(t, condition, WaitFor5Minute, wdt.VerifyingTablesFrequency)
	}
}

func queryCount(cl *client.Client, statement string) (int64, error) {
	result, err := cl.Query(statement, client.Read)
	if err != nil || result.Values == nil {
		return 0, err
	}
	return strconv.ParseInt(result.Values[0][0], 10, 64)
}
