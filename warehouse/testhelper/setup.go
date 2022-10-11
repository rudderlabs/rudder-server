package testhelper

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v6"

	"github.com/rudderlabs/rudder-server/admin"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/services/pgnotifier"
	"github.com/rudderlabs/rudder-server/warehouse"
	"github.com/rudderlabs/rudder-server/warehouse/configuration_testing"
	"github.com/rudderlabs/rudder-server/warehouse/deltalake/databricks"

	"github.com/rudderlabs/rudder-server/utils/misc"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"

	"github.com/cenkalti/backoff"

	"github.com/joho/godotenv"

	"github.com/gofrs/uuid"
	azuresynapse "github.com/rudderlabs/rudder-server/warehouse/azure-synapse"
	"github.com/rudderlabs/rudder-server/warehouse/datalake"

	"github.com/rudderlabs/rudder-server/config"
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
	SourceWriteKey               string
	Schema                       string
	UserId                       string
	MessageId                    string
	Tables                       []string
	Provider                     string
	LatestSourceRunConfig        map[string]string
	SourceID                     string
	DestinationID                string
	TimestampBeforeSendingEvents time.Time
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

const (
	SnowflakeIntegrationTestCredentials = "SNOWFLAKE_INTEGRATION_TEST_CREDENTIALS"
	RedshiftIntegrationTestCredentials  = "REDSHIFT_INTEGRATION_TEST_CREDENTIALS"
	DeltalakeIntegrationTestCredentials = "DATABRICKS_INTEGRATION_TEST_CREDENTIALS"
	BigqueryIntegrationTestCredentials  = "BIGQUERY_INTEGRATION_TEST_CREDENTIALS"
)

const (
	SnowflakeIntegrationTestSchema = "SNOWFLAKE_INTEGRATION_TEST_SCHEMA"
	RedshiftIntegrationTestSchema  = "REDSHIFT_INTEGRATION_TEST_SCHEMA"
	DeltalakeIntegrationTestSchema = "DATABRICKS_INTEGRATION_TEST_SCHEMA"
	BigqueryIntegrationTestSchema  = "BIGQUERY_INTEGRATION_TEST_SCHEMA"
)

const (
	WorkspaceConfigPath   = "/etc/rudderstack/workspaceConfig.json"
	WorkspaceTemplatePath = "warehouse/testdata/workspaceConfig/template.json"
)

var jobsDB *JobsDBResource

func Run(m *testing.M, setup WarehouseTestSetup) int {
	loadEnv()
	initialize()
	initJobsDB()
	initConnection(setup)
	return m.Run()
}

func loadEnv() {
	if err := godotenv.Load("../testhelper/.env"); err != nil {
		fmt.Printf("Error occurred while loading env for warehouse integration test with error: %s", err.Error())
	}
}

func initialize() {
	config.Reset()
	logger.Reset()
	admin.Init()
	misc.Init()

	backendconfig.Init()
	warehouseutils.Init()

	warehouse.Init()
	warehouse.Init2()
	warehouse.Init3()
	warehouse.Init4()
	warehouse.Init5()
	warehouse.Init6()

	pgnotifier.Init()
	configuration_testing.Init()

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

func setUpJobsDB() *JobsDBResource {
	pgCredentials := &postgres.CredentialsT{
		DBName:   "jobsdb",
		Password: "password",
		User:     "rudder",
		Host:     "wh-jobsDb",
		SSLMode:  "disable",
		Port:     "5432",
	}
	jobsDB := &JobsDBResource{}
	jobsDB.Credentials = pgCredentials

	var err error
	if jobsDB.DB, err = postgres.Connect(*pgCredentials); err != nil {
		log.Fatalf("could not connect to jobsDb with error: %s", err.Error())
	}
	if err = jobsDB.DB.Ping(); err != nil {
		log.Fatalf("could not connect to jobsDb while pinging with error: %s", err.Error())
	}
	return jobsDB
}

func initConnection(setup WarehouseTestSetup) {
	if err := setup.VerifyConnection(); err != nil {
		log.Fatalf("Could not complete test connection with err: %s", err.Error())
	}
}

func VerifyEventsInStagingFiles(t testing.TB, wareHouseTest *WareHouseTest, eventsMap EventsCountMap) {
	t.Helper()
	t.Logf("Started verifying events in staging files")

	var (
		tableName         = "wh_staging_files"
		stagingFileEvents int
		sqlStatement      string
		operation         func() bool
		count             sql.NullInt64
		err               error
	)

	require.NotEmpty(t, wareHouseTest.SourceID)
	require.NotEmpty(t, wareHouseTest.DestinationID)
	require.NotEmpty(t, eventsMap)
	require.NotEmpty(t, eventsMap[tableName])
	require.NotNil(t, jobsDB.DB)

	stagingFileEvents = eventsMap[tableName]

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

func VerifyEventsInLoadFiles(t testing.TB, wareHouseTest *WareHouseTest, eventsMap EventsCountMap) {
	t.Helper()
	t.Logf("Started verifying events in load file")

	var (
		loadFileEvents int
		sqlStatement   string
		operation      func() bool
		count          sql.NullInt64
		err            error
	)

	require.NotEmpty(t, wareHouseTest.SourceID)
	require.NotEmpty(t, wareHouseTest.DestinationID)
	require.NotEmpty(t, eventsMap)
	require.NotNil(t, jobsDB.DB)

	for _, table := range wareHouseTest.Tables {
		require.NotEmpty(t, eventsMap[table])

		loadFileEvents = eventsMap[table]

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

func VerifyEventsInTableUploads(t testing.TB, wareHouseTest *WareHouseTest, eventsMap EventsCountMap) {
	t.Helper()
	t.Logf("Started verifying events in table uploads")

	var (
		tableUploadEvents int
		sqlStatement      string
		operation         func() bool
		count             sql.NullInt64
		err               error
	)

	require.NotEmpty(t, wareHouseTest.SourceID)
	require.NotEmpty(t, wareHouseTest.DestinationID)
	require.NotEmpty(t, eventsMap)
	require.NotNil(t, jobsDB.DB)

	for _, table := range wareHouseTest.Tables {
		require.NotEmpty(t, eventsMap[table])

		tableUploadEvents = eventsMap[table]

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

func VerifyEventsInWareHouse(t testing.TB, wareHouseTest *WareHouseTest, eventsMap EventsCountMap) {
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
		require.Contains(t, eventsMap, table)

		tableCount := eventsMap[table]
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

func VerifyingConfigurationTest(t *testing.T, destination backendconfig.DestinationT) {
	t.Helper()
	t.Logf("Started configuration tests for destination type: %s", destination.DestinationDefinition.Name)

	require.NoError(t, WithConstantBackoff(func() error {
		destinationValidator := configuration_testing.NewDestinationValidator()
		req := &configuration_testing.DestinationValidationRequest{Destination: destination}
		response, err := destinationValidator.ValidateCredentials(req)
		if err != nil || response.Error != "" {
			return fmt.Errorf("failed to validate credentials for destination: %s with error: %s", destination.DestinationDefinition.Name, response.Error)
		}
		return nil
	}))

	t.Logf("Completed configuration tests for destination type: %s", destination.DestinationDefinition.Name)
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

func DefaultSourceEventMap() EventsCountMap {
	return EventsCountMap{
		"google_sheet": 1,
	}
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

func WarehouseSourceEventsMap() EventsCountMap {
	return EventsCountMap{
		"google_sheet": 1,
		"tracks":       1,
	}
}

func DefaultSourceRunConfig() map[string]string {
	srcrunconfig := make(map[string]string)
	srcrunconfig["job_run_id"] = ""
	srcrunconfig["task_run_id"] = ""

	return srcrunconfig
}

func GetUserId(userType string) string {
	return fmt.Sprintf("userId_%s_%s", strings.ToLower(userType), strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", ""))
}

func CreateBucketForMinio(t testing.TB, bucketName string) {
	t.Helper()
	t.Logf("Creating bucket for minio: %s", bucketName)

	const (
		endPoint    = "wh-minio:9000"
		accessKeyID = "MYACCESSKEY"
		accessKey   = "MYSECRETKEY"
		secure      = false
	)
	minioClient, err := minio.New(endPoint, accessKeyID, accessKey, secure)
	require.NoError(t, err)

	_ = minioClient.MakeBucket(bucketName, "us-east-1")
}

// TODO: Make it retryable
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

func PopulateTemplateConfigurations() map[string]string {
	configurations := map[string]string{
		"workspaceId": "BpLnfgDsc2WD8F2qNfHK5a84jjJ",

		"postgresWriteKey": "kwzDkh9h2fhfUVuS9jZ8uVbhV3v",
		"postgresHost":     "wh-postgres",
		"postgresDatabase": "rudderdb",
		"postgresUser":     "rudder",
		"postgresPassword": "rudder-password",
		"postgresPort":     "5432",

		"clickHouseWriteKey": "C5AWX39IVUWSP2NcHciWvqZTa2N",
		"clickHouseHost":     "wh-clickhouse",
		"clickHouseDatabase": "rudderdb",
		"clickHouseUser":     "rudder",
		"clickHousePassword": "rudder-password",
		"clickHousePort":     "9000",

		"clickhouseClusterWriteKey": "95RxRTZHWUsaD6HEdz0ThbXfQ6p",
		"clickhouseClusterHost":     "wh-clickhouse01",
		"clickhouseClusterDatabase": "rudderdb",
		"clickhouseClusterCluster":  "rudder_cluster",
		"clickhouseClusterUser":     "rudder",
		"clickhouseClusterPassword": "rudder-password",
		"clickhouseClusterPort":     "9000",

		"mssqlWriteKey": "YSQ3n267l1VQKGNbSuJE9fQbzON",
		"mssqlHost":     "wh-mssql",
		"mssqlDatabase": "master",
		"mssqlUser":     "SA",
		"mssqlPassword": "reallyStrongPwd123",
		"mssqlPort":     "1433",

		"azureDatalakeWriteKey":      "Hf4GTz4OiufmUqR1cq6KIeguOdC",
		"azureDatalakeContainerName": "azure-datalake-test",
		"azureDatalakeAccountName":   "MYACCESSKEY",
		"azureDatalakeAccountKey":    "TVlTRUNSRVRLRVk=",
		"azureDatalakeEndPoint":      "wh-azure:10000",

		"s3DatalakeWriteKey":   "ZapZJHfSxUN96GTIuShnz6bv0zi",
		"s3DatalakeBucketName": "s3-datalake-test",
		"s3DatalakeRegion":     "us-east-1",

		"gcsDatalakeWriteKey": "9zZFfcRqr2LpwerxICilhQmMybn",

		"bigqueryWriteKey":               "J77aX7tLFJ84qYU6UrN8ctecwZt",
		"snowflakeWriteKey":              "2eSJyYtqwcFiUILzXv2fcNIrWO7",
		"snowflakeCaseSensitiveWriteKey": "2eSJyYtqwcFYUILzXv2fcNIrWO7",
		"redshiftWriteKey":               "JAAwdCxmM8BIabKERsUhPNmMmdf",
		"deltalakeWriteKey":              "sToFgoilA0U1WxNeW1gdgUVDsEW",

		"minioBucketName":      "devintegrationtest",
		"minioAccesskeyID":     "MYACCESSKEY",
		"minioSecretAccessKey": "MYSECRETKEY",
		"minioEndpoint":        "wh-minio:9000",
	}

	enhanceWithRedshiftConfigurations(configurations)
	enhanceWithSnowflakeConfigurations(configurations)
	enhanceWithDeltalakeConfigurations(configurations)
	enhanceWithBQConfigurations(configurations)
	return configurations
}

func enhanceWithSnowflakeConfigurations(values map[string]string) {
	for k, v := range credentialsFromKey(SnowflakeIntegrationTestCredentials) {
		values[fmt.Sprintf("snowflake%s", k)] = v
	}

	values["snowflakeCaseSensitiveDBName"] = strings.ToLower(values["snowflakeDBName"])
	values["snowflakeNamespace"] = Schema(warehouseutils.SNOWFLAKE, SnowflakeIntegrationTestSchema)
	values["snowflakeCaseSensitiveNamespace"] = fmt.Sprintf("%s_%s", values["snowflakeNamespace"], "CS")
}

func enhanceWithRedshiftConfigurations(values map[string]string) {
	for k, v := range credentialsFromKey(RedshiftIntegrationTestCredentials) {
		values[fmt.Sprintf("redshift%s", k)] = v
	}

	values["redshiftNamespace"] = Schema(warehouseutils.RS, RedshiftIntegrationTestSchema)
}

func enhanceWithDeltalakeConfigurations(values map[string]string) {
	for k, v := range credentialsFromKey(DeltalakeIntegrationTestCredentials) {
		values[fmt.Sprintf("deltalake%s", k)] = v
	}

	values["deltalakeNamespace"] = Schema(warehouseutils.DELTALAKE, DeltalakeIntegrationTestSchema)
}

func enhanceWithBQConfigurations(values map[string]string) {
	for k, v := range credentialsFromKey(BigqueryIntegrationTestCredentials) {
		values[fmt.Sprintf("bigquery%s", k)] = v
	}

	values["bigqueryNamespace"] = Schema(warehouseutils.BQ, BigqueryIntegrationTestSchema)

	key := "bigqueryCredentials"
	if credentials, exists := values[key]; exists {
		escapedCredentials, err := json.Marshal(credentials)
		if err != nil {
			log.Panicf("error escaping big query JSON credentials while setting up the workspace config with error: %s", err.Error())
		}
		values[key] = strings.Trim(string(escapedCredentials), `"`)
	}
}

func Schema(provider, schemaKey string) string {
	return warehouseutils.ToProviderCase(
		provider,
		warehouseutils.ToSafeNamespace(
			provider,
			config.MustGetString(schemaKey),
		),
	)
}

func credentialsFromKey(key string) (credentials map[string]string) {
	cred, exists := os.LookupEnv(key)
	if !exists {
		log.Print(fmt.Errorf("env %s does not exists while setting up the workspace config", key))
		return
	}

	err := json.Unmarshal([]byte(cred), &credentials)
	if err != nil {
		log.Panicf("error occurred while unmarshalling %s for setting up the workspace config", key)
		return
	}
	return
}

func SnowflakeCredentials() (credentials snowflake.SnowflakeCredentialsT, err error) {
	cred, exists := os.LookupEnv(SnowflakeIntegrationTestCredentials)
	if !exists {
		err = fmt.Errorf("following %s does not exists while running the Snowflake test", SnowflakeIntegrationTestCredentials)
		return
	}

	err = json.Unmarshal([]byte(cred), &credentials)
	if err != nil {
		err = fmt.Errorf("error occurred while unmarshalling snowflake test credentials with err: %s", err.Error())
		return
	}
	return
}

func RedshiftCredentials() (credentials redshift.RedshiftCredentialsT, err error) {
	cred, exists := os.LookupEnv(RedshiftIntegrationTestCredentials)
	if !exists {
		err = fmt.Errorf("following %s does not exists while running the Redshift test", RedshiftIntegrationTestCredentials)
		return
	}

	err = json.Unmarshal([]byte(cred), &credentials)
	if err != nil {
		err = fmt.Errorf("error occurred while unmarshalling redshift test credentials with err: %s", err.Error())
	}
	return
}

func BigqueryCredentials() (credentials bigquery.BQCredentialsT, err error) {
	cred, exists := os.LookupEnv(BigqueryIntegrationTestCredentials)
	if !exists {
		err = fmt.Errorf("following %s does not exists while running the Bigquery test", BigqueryIntegrationTestCredentials)
		return
	}

	err = json.Unmarshal([]byte(cred), &credentials)
	if err != nil {
		err = fmt.Errorf("error occurred while unmarshalling bigquery test credentials with err: %s", err.Error())
		return
	}
	return
}

func DatabricksCredentials() (credentials databricks.CredentialsT, err error) {
	cred, exists := os.LookupEnv(DeltalakeIntegrationTestCredentials)
	if !exists {
		err = fmt.Errorf("following %s does not exists while running the Deltalake test", DeltalakeIntegrationTestCredentials)
		return
	}

	err = json.Unmarshal([]byte(cred), &credentials)
	if err != nil {
		err = fmt.Errorf("error occurred while unmarshalling databricks test credentials with err: %s", err.Error())
		return
	}
	return
}

func (w *WareHouseTest) MsgId() string {
	if w.MessageId == "" {
		return uuid.Must(uuid.NewV4()).String()
	}
	return w.MessageId
}
