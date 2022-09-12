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
	Client                       *client.Client
	WriteKey                     string
	Schema                       string
	TablesQueryFrequency         time.Duration
	EventsCountMap               EventsCountMap
	UserId                       string
	SourceID                     string
	DestinationID                string
	TimestampBeforeSendingEvents time.Time
	MessageId                    string
	Tables                       []string
	Provider                     string
}

type WarehouseTestSetup interface {
	VerifyConnection() error
}

const (
	WaitFor2Minute        = 2 * time.Minute
	WaitFor10Minute       = 10 * time.Minute
	DefaultQueryFrequency = 100 * time.Millisecond
)

const (
	BackoffDuration = 1 * time.Second
	BackoffRetryMax = 5
)

var jobsDB *JobsDBResource

const (
	SnowflakeIntegrationTestCredentials = "SNOWFLAKE_INTEGRATION_TEST_CREDENTIALS"
	RedshiftIntegrationTestCredentials  = "REDSHIFT_INTEGRATION_TEST_CREDENTIALS"
	DeltalakeIntegrationTestCredentials = "DATABRICKS_INTEGRATION_TEST_CREDENTIALS"
	BigqueryIntegrationTestCredentials  = "BIGQUERY_INTEGRATION_TEST_CREDENTIALS"
	SnowflakeIntegrationTestSchema      = "SNOWFLAKE_INTEGRATION_TEST_SCHEMA"
	RedshiftIntegrationTestSchema       = "REDSHIFT_INTEGRATION_TEST_SCHEMA"
	DeltalakeIntegrationTestSchema      = "DATABRICKS_INTEGRATION_TEST_SCHEMA"
	BigqueryIntegrationTestSchema       = "BIGQUERY_INTEGRATION_TEST_SCHEMA"
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
	if err := setup.VerifyConnection(); err != nil {
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

func VerifyingEventsInStagingFiles(t testing.TB, wareHouseTest *WareHouseTest) {
	t.Helper()
	t.Logf("Started verifying events in staging files")

	var (
		count             sql.NullInt64
		stagingFileEvents int
		sqlStatement      string
		operation         func() bool
		err               error
		tableName         = "wh_staging_files"
	)

	require.NotEmpty(t, wareHouseTest.SourceID)
	require.NotEmpty(t, wareHouseTest.DestinationID)
	require.NotEmpty(t, wareHouseTest.EventsCountMap)
	require.NotEmpty(t, wareHouseTest.EventsCountMap[tableName])

	require.NotNil(t, jobsDB.DB)

	stagingFileEvents = wareHouseTest.EventsCountMap[tableName]

	sqlStatement = `
		SELECT
		   COALESCE(SUM(total_events)) AS sum
		FROM
		   wh_staging_files
		WHERE
		   source_id = $1
		   AND destination_id = $2
		   AND created_at > $3;
	`
	t.Logf("Checking events in staging files for sourceID: %s, DestinationID: %s, TimestampBeforeSendingEvents: %s, sqlStatement: %s",
		wareHouseTest.SourceID,
		wareHouseTest.DestinationID,
		wareHouseTest.TimestampBeforeSendingEvents,
		sqlStatement,
	)
	operation = func() bool {
		err = jobsDB.DB.QueryRow(
			sqlStatement,
			wareHouseTest.SourceID,
			wareHouseTest.DestinationID,
			wareHouseTest.TimestampBeforeSendingEvents,
		).Scan(&count)
		require.NoError(t, err)
		return count.Int64 == int64(stagingFileEvents)
	}
	require.Eventually(t, operation, WaitFor2Minute, DefaultQueryFrequency, fmt.Sprintf("Expected staging files events count is %d and Actual staging files events count is %d", stagingFileEvents, count.Int64))

	t.Logf("Completed verifying events in staging files")
}

func VerifyingEventsInLoadFiles(t testing.TB, wareHouseTest *WareHouseTest) {
	t.Helper()
	t.Logf("Started verifying events in load file")

	var (
		count          sql.NullInt64
		loadFileEvents int
		sqlStatement   string
		operation      func() bool
		err            error
	)

	require.NotEmpty(t, wareHouseTest.SourceID)
	require.NotEmpty(t, wareHouseTest.DestinationID)
	require.NotEmpty(t, wareHouseTest.EventsCountMap)

	require.NotNil(t, jobsDB.DB)

	for _, table := range wareHouseTest.Tables {
		require.NotEmpty(t, wareHouseTest.EventsCountMap[table])

		loadFileEvents = wareHouseTest.EventsCountMap[table]

		sqlStatement = `
			SELECT
			   COALESCE(SUM(total_events)) AS sum
			FROM
			   wh_load_files
			WHERE
			   source_id = $1
			   AND destination_id = $2
			   AND created_at > $3
			   AND table_name = $4;
		`
		t.Logf("Checking events in load files for sourceID: %s, DestinationID: %s, TimestampBeforeSendingEvents: %s, table: %s, sqlStatement: %s",
			wareHouseTest.SourceID,
			wareHouseTest.DestinationID,
			wareHouseTest.TimestampBeforeSendingEvents,
			warehouseutils.ToProviderCase(wareHouseTest.Provider, table),
			sqlStatement,
		)
		operation = func() bool {
			err = jobsDB.DB.QueryRow(
				sqlStatement,
				wareHouseTest.SourceID,
				wareHouseTest.DestinationID,
				wareHouseTest.TimestampBeforeSendingEvents,
				warehouseutils.ToProviderCase(wareHouseTest.Provider, table),
			).Scan(&count)
			require.NoError(t, err)
			return count.Int64 == int64(loadFileEvents)
		}
		require.Eventually(t, operation, WaitFor10Minute, DefaultQueryFrequency, fmt.Sprintf("Expected load files events count is %d and Actual load files events count is %d for table %s", loadFileEvents, count.Int64, table))
	}

	t.Logf("Completed verifying events in load files")
}

func VerifyingEventsInTableUploads(t testing.TB, wareHouseTest *WareHouseTest) {
	t.Helper()
	t.Logf("Started verifying events in table uploads")

	var (
		count             sql.NullInt64
		tableUploadEvents int
		sqlStatement      string
		operation         func() bool
		err               error
	)

	require.NotEmpty(t, wareHouseTest.SourceID)
	require.NotEmpty(t, wareHouseTest.DestinationID)
	require.NotEmpty(t, wareHouseTest.EventsCountMap)

	require.NotNil(t, jobsDB.DB)

	for _, table := range wareHouseTest.Tables {
		require.NotEmpty(t, wareHouseTest.EventsCountMap[table])

		tableUploadEvents = wareHouseTest.EventsCountMap[table]

		sqlStatement = `
			SELECT
			   COALESCE(SUM(total_events)) AS sum
			FROM
			   wh_table_uploads
			   LEFT JOIN
				  wh_uploads
				  ON wh_uploads.id = wh_table_uploads.wh_upload_id
			WHERE
			   wh_uploads.source_id = $1
			   AND wh_uploads.destination_id = $2
			   AND wh_uploads.created_at > $3
			   AND wh_table_uploads.table_name = $4
			   AND wh_table_uploads.status = 'exported_data';
		`
		t.Logf("Checking events in table uploads for sourceID: %s, DestinationID: %s, TimestampBeforeSendingEvents: %s, table: %s, sqlStatement: %s",
			wareHouseTest.SourceID,
			wareHouseTest.DestinationID,
			wareHouseTest.TimestampBeforeSendingEvents,
			warehouseutils.ToProviderCase(wareHouseTest.Provider, table),
			sqlStatement,
		)
		operation = func() bool {
			err = jobsDB.DB.QueryRow(
				sqlStatement,
				wareHouseTest.SourceID,
				wareHouseTest.DestinationID,
				wareHouseTest.TimestampBeforeSendingEvents,
				warehouseutils.ToProviderCase(wareHouseTest.Provider, table),
			).Scan(&count)
			require.NoError(t, err)
			return count.Int64 == int64(tableUploadEvents)
		}
		require.Eventually(t, operation, WaitFor10Minute, DefaultQueryFrequency, fmt.Sprintf("Expected table uploads events count is %d and Actual table uploads events count is %d for table %s", tableUploadEvents, count.Int64, table))
	}

	t.Logf("Completed verifying events in table uploads")
}

func VerifyingEventsInWareHouse(t testing.TB, wareHouseTest *WareHouseTest) {
	t.Helper()
	t.Logf("Started verifying events in warehouse")

	require.NotEmpty(t, wareHouseTest.Schema)
	require.NotEmpty(t, wareHouseTest.UserId)

	require.NotNil(t, wareHouseTest.Client)

	primaryKey := func(tableName string) string {
		if tableName == "users" {
			return "id"
		}
		return "user_id"
	}

	var (
		count    int64
		countErr error
	)

	for _, table := range wareHouseTest.Tables {
		require.Contains(t, wareHouseTest.EventsCountMap, table)

		tableCount := wareHouseTest.EventsCountMap[table]

		sqlStatement := fmt.Sprintf(`
			select
			  count(*)
			from
			  %s.%s
			where
			  %s = '%s';`,
			wareHouseTest.Schema,
			warehouseutils.ToProviderCase(wareHouseTest.Provider, table),
			primaryKey(table),
			wareHouseTest.UserId,
		)
		t.Logf("Checking events in warehouse for schema: %s, table: %s, primaryKey: %s, UserId: %s, sqlStatement: %s",
			wareHouseTest.Schema,
			warehouseutils.ToProviderCase(wareHouseTest.Provider, table),
			primaryKey(table),
			wareHouseTest.UserId,
			sqlStatement,
		)

		require.NoError(t, WithConstantBackoff(func() error {
			count, countErr = queryCount(wareHouseTest.Client, sqlStatement)
			if countErr != nil {
				return countErr
			}
			if count != int64(tableCount) {
				return fmt.Errorf("error in counting events in warehouse for schema: %s, table: %s,UserId: %s",
					wareHouseTest.Schema,
					warehouseutils.ToProviderCase(wareHouseTest.Provider, table),
					wareHouseTest.UserId,
				)
			}
			return nil
		}))
	}

	t.Logf("Completed verifying events in warehouse")
}

func queryCount(cl *client.Client, statement string) (int64, error) {
	result, err := cl.Query(statement)
	if err != nil || result.Values == nil {
		return 0, err
	}
	return strconv.ParseInt(result.Values[0][0], 10, 64)
}

func WithConstantBackoff(operation func() error) error {
	backoffWithMaxRetry := backoff.WithMaxRetries(backoff.NewConstantBackOff(BackoffDuration), uint64(BackoffRetryMax))
	return backoff.Retry(operation, backoffWithMaxRetry)
}

func SendEventsMap() EventsCountMap {
	return EventsCountMap{
		"identifies": 1,
		"tracks":     1,
		"pages":      1,
		"screens":    1,
		"aliases":    1,
		"groups":     1,
	}
}

func StagingFilesEventsMap() EventsCountMap {
	return EventsCountMap{
		"wh_staging_files": 32,
	}
}

func LoadFilesEventsMap() EventsCountMap {
	return EventsCountMap{
		"identifies":    4,
		"users":         4,
		"tracks":        4,
		"product_track": 4,
		"pages":         4,
		"screens":       4,
		"aliases":       4,
		"groups":        4,
	}
}

func TableUploadsEventsMap() EventsCountMap {
	return EventsCountMap{
		"identifies":    4,
		"users":         4,
		"tracks":        4,
		"product_track": 4,
		"pages":         4,
		"screens":       4,
		"aliases":       4,
		"groups":        4,
	}
}

func WarehouseEventsMap() EventsCountMap {
	return EventsCountMap{
		"identifies":    4,
		"users":         1,
		"tracks":        4,
		"product_track": 4,
		"pages":         4,
		"screens":       4,
		"aliases":       4,
		"groups":        4,
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

func GetSchema(provider, schemaKey string) string {
	return warehouseutils.ToProviderCase(
		provider,
		warehouseutils.ToSafeNamespace(
			provider,
			config.GetRequiredEnv(schemaKey),
		),
	)
}
