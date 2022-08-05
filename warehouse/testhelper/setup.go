package testhelper

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/rudderlabs/rudder-server/utils/misc"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"

	"github.com/cenkalti/backoff"

	"github.com/joho/godotenv"

	"github.com/gofrs/uuid"
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

type JobsDBResource struct {
	Credentials *postgres.CredentialsT
	DB          *sql.DB
}

type EventsCountMap map[string]int

type WareHouseTest struct {
	Client               *client.Client
	WriteKey             string
	Schema               string
	TablesQueryFrequency time.Duration
	EventsCountMap       EventsCountMap
	UserId               string
	MessageId            string
	Tables               []string
}

type WarehouseTestSetup interface {
	TestConnection() error
}

const (
	WaitFor2Minute            = 2 * time.Minute
	WaitFor10Minute           = 10 * time.Minute
	DefaultQueryFrequency     = 100 * time.Millisecond
	LongRunningQueryFrequency = 10000 * time.Millisecond
)

const (
	ConnectBackoffDuration = 1 * time.Second
	ConnectBackoffRetryMax = 5
)

var jobsDB *JobsDBResource

const (
	SnowflakeIntegrationTestCredentials = "SNOWFLAKE_INTEGRATION_TEST_CREDENTIALS"
	RedshiftIntegrationTestCredentials  = "REDSHIFT_INTEGRATION_TEST_CREDENTIALS"
	DeltalakeIntegrationTestCredentials = "DATABRICKS_INTEGRATION_TEST_CREDENTIALS"
	BigqueryIntegrationTestCredentials  = "BIGQUERY_INTEGRATION_TEST_CREDENTIALS"
	WorkspaceConfigPath                 = "/etc/rudderstack/workspaceConfig.json"
)

func (w *WareHouseTest) MsgId() string {
	if w.MessageId == "" {
		return uuid.Must(uuid.NewV4()).String()
	}
	return w.MessageId
}

func (w *WareHouseTest) SetUserId(destType string) {
	w.UserId = fmt.Sprintf("userId_%s_%s", strings.ToLower(destType), strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", ""))
}

func Run(m *testing.M, setup WarehouseTestSetup) int {
	initialize()
	initJobsDB()
	if err := setup.TestConnection(); err != nil {
		log.Fatalf("Could not complete test connection with err: %s", err.Error())
	}
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

func initJobsDB() {
	jobsDB = setUpJobsDB()
	enhanceJobsDBWithSQLFunctions()
}

func setUpJobsDB() (jobsDB *JobsDBResource) {
	pgCredentials := &postgres.CredentialsT{
		DBName:   "jobsdb",
		Password: "password",
		User:     "rudder",
		Host:     "wh-jobsDb",
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

	txn, err := jobsDB.DB.Begin()
	if err != nil {
		log.Panicf("error occurred with creating transactions for jobs function with err %v", err.Error())
	}

	_, err = txn.Exec(GWJobsForUserIdWriteKey())
	if err != nil {
		log.Panicf("error occurred with executing gw jobs function with err %v", err.Error())
	}

	_, err = txn.Exec(BRTJobsForUserId())
	if err != nil {
		log.Panicf("error occurred with executing brt jobs function with err %s", err.Error())
	}

	err = txn.Commit()
	if err != nil {
		log.Panicf("error occurred with commiting txn with err %s", err.Error())
	}
}

func VerifyingGatewayEvents(t testing.TB, wareHouseTest *WareHouseTest) {
	t.Helper()
	t.Logf("Started verifying gateway events")

	require.Contains(t, wareHouseTest.EventsCountMap, "gateway")
	gwEvents := wareHouseTest.EventsCountMap["gateway"]

	var (
		count        int64
		jobId        int64
		jobIds       []string
		sqlStatement string
		operation    func() bool
		err          error
		rows         *sql.Rows
	)

	sqlStatement = fmt.Sprintf(`
		select
		  count(*)
		from
		  gw_jobs_for_user_id_and_write_key('%s', '%s') as job_ids;`,
		wareHouseTest.UserId,
		wareHouseTest.WriteKey,
	)
	t.Logf("Checking for the gateway jobs for sqlStatement: %s", sqlStatement)
	operation = func() bool {
		err = jobsDB.DB.QueryRow(sqlStatement).Scan(&count)
		require.NoError(t, err)
		return count == int64(gwEvents)
	}
	require.Eventually(t, operation, WaitFor2Minute, DefaultQueryFrequency, fmt.Sprintf("GW events count is %d and GW Jobs count is %d", gwEvents, count))

	sqlStatement = fmt.Sprintf(`
		select
		  *
		from
		  gw_jobs_for_user_id_and_write_key('%s', '%s') as job_ids;`,
		wareHouseTest.UserId,
		wareHouseTest.WriteKey,
	)
	t.Logf("Checking for gateway job ids for sqlStatement: %s", sqlStatement)
	operation = func() bool {
		jobIds = make([]string, 0)

		rows, err = jobsDB.DB.Query(sqlStatement)
		require.NoError(t, err)

		defer func() { _ = rows.Close() }()

		for rows.Next() {
			err = rows.Scan(&jobId)
			require.NoError(t, err)
			jobIds = append(jobIds, fmt.Sprint(jobId))
		}
		return gwEvents == len(jobIds)
	}
	require.Eventually(t, operation, WaitFor2Minute, DefaultQueryFrequency, fmt.Sprintf("GW events count is %d and GW Jobs status count is %d", gwEvents, count))

	sqlStatement = fmt.Sprintf(`
		select
		  count(*)
		from
		  gw_job_status_1
		where
		  job_id in (%s)
		  and job_state = 'succeeded';`,
		strings.Join(jobIds, ","),
	)
	t.Logf("Checking for gateway jobs state for sqlStatement: %s", sqlStatement)
	operation = func() bool {
		err = jobsDB.DB.QueryRow(sqlStatement).Scan(&count)
		require.NoError(t, err)
		return count == int64(gwEvents)
	}
	require.Eventually(t, operation, WaitFor2Minute, DefaultQueryFrequency, fmt.Sprintf("GW events count is %d and GW Jobs succeeded count is %d", gwEvents, count))

	t.Logf("Completed verifying gateway events")
}

func VerifyingBatchRouterEvents(t testing.TB, wareHouseTest *WareHouseTest) {
	t.Helper()
	t.Logf("Started verifying batch router events")

	require.Contains(t, wareHouseTest.EventsCountMap, "batchRT")
	brtEvents := wareHouseTest.EventsCountMap["batchRT"]

	var (
		count        int64
		jobId        int64
		jobIds       []string
		sqlStatement string
		operation    func() bool
		err          error
		rows         *sql.Rows
	)

	sqlStatement = fmt.Sprintf(`
		select
		  count(*)
		from
		  brt_jobs_for_user_id('%s') as job_ids;`,
		wareHouseTest.UserId,
	)
	t.Logf("Checking for batch router jobs for sqlStatement: %s", sqlStatement)
	operation = func() bool {
		err = jobsDB.DB.QueryRow(sqlStatement).Scan(&count)
		require.NoError(t, err)
		return count == int64(brtEvents)
	}
	require.Eventually(t, operation, WaitFor2Minute, DefaultQueryFrequency, fmt.Sprintf("BRT events count is %d and BRT Jobs count is %d", brtEvents, count))

	sqlStatement = fmt.Sprintf(`
		select
		  *
		from
		  brt_jobs_for_user_id('%s') as job_ids;`,
		wareHouseTest.UserId,
	)
	t.Logf("Checking for batch router job ids for sqlStatement: %s", sqlStatement)
	operation = func() bool {
		jobIds = make([]string, 0)

		rows, err = jobsDB.DB.Query(sqlStatement)
		require.NoError(t, err)

		defer func() { _ = rows.Close() }()

		for rows.Next() {
			err = rows.Scan(&jobId)
			require.NoError(t, err)
			jobIds = append(jobIds, fmt.Sprint(jobId))
		}
		return brtEvents == len(jobIds)
	}
	require.Eventually(t, operation, WaitFor2Minute, DefaultQueryFrequency, fmt.Sprintf("BRT events count is %d and BRT Jobs status count is %d", brtEvents, count))

	// Checking for the batch router jobs state
	sqlStatement = fmt.Sprintf(`
		select
		  count(*)
		from
		  batch_rt_job_status_1
		where
		  job_id in (%s)
		  and job_state = 'succeeded';`,
		strings.Join(jobIds, ","),
	)
	t.Logf("Checking for batch router jobs state for sqlStatement: %s", sqlStatement)
	operation = func() bool {
		err = jobsDB.DB.QueryRow(sqlStatement).Scan(&count)
		require.NoError(t, err)
		return count == int64(brtEvents)
	}
	require.Eventually(t, operation, WaitFor2Minute, DefaultQueryFrequency, fmt.Sprintf("BRT events count is %d and BRT Jobs succeeded count is %d", brtEvents, count))

	t.Logf("Completed verifying batch router events")
}

func VerifyingTablesEventCount(t testing.TB, wareHouseTest *WareHouseTest) {
	t.Helper()
	t.Logf("Started verifying tables events")

	primaryKey := func(tableName string) string {
		if tableName == "users" {
			return "id"
		}
		return "user_id"
	}

	var (
		count        int64
		sqlStatement string
		condition    func() bool
	)

	for _, table := range wareHouseTest.Tables {
		sqlStatement = fmt.Sprintf(`
			select
			  count(*)
			from
			  %s.%s
			where
			  %s = '%s';`,
			wareHouseTest.Schema,
			table,
			primaryKey(table),
			wareHouseTest.UserId,
		)
		t.Logf("Verifying tables event count for sqlStatement: %s", sqlStatement)

		require.Contains(t, wareHouseTest.EventsCountMap, table)
		tableCount := wareHouseTest.EventsCountMap[table]

		condition = func() bool {
			count, _ = queryCount(wareHouseTest.Client, sqlStatement)
			return count == int64(tableCount)
		}
		require.Eventually(t, condition, WaitFor10Minute, wareHouseTest.TablesQueryFrequency, fmt.Sprintf("Table %s Count is %d and Events Count is %d", table, tableCount, count))
	}

	t.Logf("Completed verifying tables events")
}

func queryCount(cl *client.Client, statement string) (int64, error) {
	result, err := cl.Query(statement)
	if err != nil || result.Values == nil {
		return 0, err
	}
	return strconv.ParseInt(result.Values[0][0], 10, 64)
}

func ConnectWithBackoff(operation func() error) error {
	backoffWithMaxRetry := backoff.WithMaxRetries(backoff.NewConstantBackOff(ConnectBackoffDuration), uint64(ConnectBackoffRetryMax))
	return backoff.Retry(operation, backoffWithMaxRetry)
}

func GWJobsForUserIdWriteKey() string {
	return `
		CREATE
		OR REPLACE FUNCTION gw_jobs_for_user_id_and_write_key(
		  user_id varchar, write_key varchar
		) RETURNS TABLE (job_id varchar) AS $$ DECLARE table_record RECORD;
		batch_record jsonb;
		BEGIN FOR table_record IN
		SELECT
		  *
		FROM
		  gw_jobs_1
		where
		  (event_payload ->> 'writeKey') = write_key LOOP FOR batch_record IN
		SELECT
		  *
		FROM
		  jsonb_array_elements(
			(
			  table_record.event_payload ->> 'batch'
			):: jsonb
		  ) LOOP if batch_record ->> 'userId' != user_id THEN CONTINUE;
		END IF;
		job_id := table_record.job_id;
		RETURN NEXT;
		EXIT;
		END LOOP;
		END LOOP;
		END;
		$$ LANGUAGE plpgsql;
`
}

func BRTJobsForUserId() string {
	return `
		CREATE
		OR REPLACE FUNCTION brt_jobs_for_user_id(user_id varchar) RETURNS TABLE (job_id varchar) AS $$ DECLARE table_record RECORD;
		event_payload jsonb;
		BEGIN FOR table_record IN
		SELECT
		  *
		FROM
		  batch_rt_jobs_1 LOOP event_payload = (
			table_record.event_payload ->> 'data'
		  ):: jsonb;
		if event_payload ->> 'user_id' = user_id
		Or event_payload ->> 'id' = user_id
		Or event_payload ->> 'USER_ID' = user_id
		Or event_payload ->> 'ID' = user_id THEN job_id := table_record.job_id;
		RETURN NEXT;
		END IF;
		END LOOP;
		END;
		$$ LANGUAGE plpgsql;
`
}

func DefaultEventMap() EventsCountMap {
	return EventsCountMap{
		"identifies":    1,
		"users":         1,
		"tracks":        1,
		"pages":         1,
		"product_track": 1,
		"screens":       1,
		"aliases":       1,
		"groups":        1,
		"_groups":       1,
		"gateway":       6,
		"batchRT":       8,
	}
}

func GetUserId(userType string) string {
	return fmt.Sprintf("userId_%s_%s", strings.ToLower(userType), strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", ""))
}

func SetConfig(kvs []warehouseutils.KeyValue) error {
	payload, err := json.Marshal(&kvs)
	if err != nil {
		return fmt.Errorf("error marshalling while setting config with err: %s", err.Error())
	}

	url := fmt.Sprintf(`%s/v1/setConfig`, misc.GetWarehouseURL())
	_, err = warehouseutils.PostRequestWithTimeout(context.TODO(), url, payload, time.Second*60)
	if err != nil {
		return fmt.Errorf("error while making post request to set config with err: %s", err.Error())
	}
	return nil
}
