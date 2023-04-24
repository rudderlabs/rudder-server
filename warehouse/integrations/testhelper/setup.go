package testhelper

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"text/template"
	"time"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/warehouse/validations"

	"github.com/rudderlabs/rudder-server/utils/httputil"
	"github.com/rudderlabs/rudder-server/utils/misc"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"

	_ "github.com/lib/pq"
	"github.com/rudderlabs/rudder-go-kit/config"
	warehouseclient "github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/postgres"
	"github.com/stretchr/testify/require"
)

const (
	WaitFor2Minute         = 2 * time.Minute
	WaitFor10Minute        = 10 * time.Minute
	DefaultQueryFrequency  = 100 * time.Millisecond
	AsyncJOBQueryFrequency = 1000 * time.Millisecond
)

const (
	SnowflakeIntegrationTestCredentials     = "SNOWFLAKE_INTEGRATION_TEST_CREDENTIALS"
	SnowflakeRBACIntegrationTestCredentials = "SNOWFLAKE_RBAC_INTEGRATION_TEST_CREDENTIALS"
	RedshiftIntegrationTestCredentials      = "REDSHIFT_INTEGRATION_TEST_CREDENTIALS"
	DeltalakeIntegrationTestCredentials     = "DATABRICKS_INTEGRATION_TEST_CREDENTIALS"
	BigqueryIntegrationTestCredentials      = "BIGQUERY_INTEGRATION_TEST_CREDENTIALS"
)

const (
	SnowflakeIntegrationTestSchema = "SNOWFLAKE_INTEGRATION_TEST_SCHEMA"
	RedshiftIntegrationTestSchema  = "REDSHIFT_INTEGRATION_TEST_SCHEMA"
	DeltalakeIntegrationTestSchema = "DATABRICKS_INTEGRATION_TEST_SCHEMA"
	BigqueryIntegrationTestSchema  = "BIGQUERY_INTEGRATION_TEST_SCHEMA"
)

type EventsCountMap map[string]int

type WareHouseTest struct {
	Client                       *warehouseclient.Client
	WriteKey                     string
	Schema                       string
	UserID                       string
	MessageID                    string
	JobRunID                     string
	TaskRunID                    string
	RecordID                     string
	Tables                       []string
	Provider                     string
	SourceID                     string
	DestinationID                string
	TimestampBeforeSendingEvents time.Time
	EventsMap                    EventsCountMap
	StagingFilesEventsMap        EventsCountMap
	LoadFilesEventsMap           EventsCountMap
	TableUploadsEventsMap        EventsCountMap
	WarehouseEventsMap           EventsCountMap
	JobsDB                       *sql.DB
	AsyncJob                     bool
	Prerequisite                 func(t testing.TB)
	StatsToVerify                []string
	SkipWarehouse                bool
	HTTPPort                     int
}

func (w *WareHouseTest) init() {
	w.TimestampBeforeSendingEvents = timeutil.Now()

	if len(w.EventsMap) == 0 {
		w.EventsMap = defaultSendEventsMap()
	}
	if len(w.StagingFilesEventsMap) == 0 {
		w.StagingFilesEventsMap = defaultStagingFilesEventsMap()
	}
	if len(w.LoadFilesEventsMap) == 0 {
		w.LoadFilesEventsMap = defaultLoadFilesEventsMap()
	}
	if len(w.TableUploadsEventsMap) == 0 {
		w.TableUploadsEventsMap = defaultTableUploadsEventsMap()
	}
	if len(w.WarehouseEventsMap) == 0 {
		w.WarehouseEventsMap = defaultWarehouseEventsMap()
	}
}

func (w *WareHouseTest) msgID() string {
	if w.MessageID == "" {
		return misc.FastUUID().String()
	}
	return w.MessageID
}

func (w *WareHouseTest) recordID() string {
	if w.RecordID == "" {
		return misc.FastUUID().String()
	}
	return w.RecordID
}

func (w *WareHouseTest) VerifyEvents(t testing.TB) {
	t.Helper()

	w.init()

	if w.Prerequisite != nil {
		w.Prerequisite(t)
	}

	SendEvents(t, w)
	SendEvents(t, w)
	SendEvents(t, w)
	SendIntegratedEvents(t, w)

	verifyEventsInStagingFiles(t, w)
	verifyEventsInLoadFiles(t, w)
	verifyEventsInTableUploads(t, w)
	if !w.SkipWarehouse {
		verifyEventsInWareHouse(t, w)
	}
}

func (w *WareHouseTest) VerifyModifiedEvents(t testing.TB) {
	t.Helper()

	w.init()

	if w.Prerequisite != nil {
		w.Prerequisite(t)
	}

	SendModifiedEvents(t, w)
	SendModifiedEvents(t, w)
	SendModifiedEvents(t, w)
	SendIntegratedEvents(t, w)

	verifyEventsInStagingFiles(t, w)
	verifyEventsInLoadFiles(t, w)
	verifyEventsInTableUploads(t, w)
	if w.AsyncJob {
		verifyAsyncJob(t, w)
	}
	if !w.SkipWarehouse {
		verifyEventsInWareHouse(t, w)
	}
}

func SetUpJobsDB(t testing.TB) *sql.DB {
	t.Helper()

	pgCredentials := &postgres.Credentials{
		DBName:   "jobsdb",
		Password: "password",
		User:     "rudder",
		Host:     "wh-jobsDb",
		SSLMode:  "disable",
		Port:     "5432",
	}

	db, err := postgres.Connect(*pgCredentials)
	require.NoError(t, err)

	err = db.Ping()
	require.NoError(t, err)

	return db
}

func verifyEventsInStagingFiles(t testing.TB, wareHouseTest *WareHouseTest) {
	t.Helper()
	t.Logf("Started verifying events in staging files")

	var (
		tableName         = "wh_staging_files"
		workspaceID       = "BpLnfgDsc2WD8F2qNfHK5a84jjJ"
		stagingFileEvents int
		sqlStatement      string
		operation         func() bool
		count             sql.NullInt64
		err               error
		db                = wareHouseTest.JobsDB
		eventsMap         = wareHouseTest.StagingFilesEventsMap
	)

	require.NotEmpty(t, wareHouseTest.SourceID)
	require.NotEmpty(t, wareHouseTest.DestinationID)
	require.NotEmpty(t, eventsMap)
	require.NotEmpty(t, eventsMap[tableName])
	require.NotNil(t, db)

	stagingFileEvents = eventsMap[tableName]

	sqlStatement = `
		SELECT
			COALESCE(SUM(total_events)) AS sum
		FROM
			wh_staging_files
		WHERE
		   	workspace_id = $1 AND
		   	source_id = $2 AND
		   	destination_id = $3 AND
		   	created_at > $4;
	`
	t.Logf("Checking events in staging files for workspaceID: %s, sourceID: %s, DestinationID: %s, TimestampBeforeSendingEvents: %s, sqlStatement: %s",
		workspaceID,
		wareHouseTest.SourceID,
		wareHouseTest.DestinationID,
		wareHouseTest.TimestampBeforeSendingEvents,
		sqlStatement,
	)
	operation = func() bool {
		err = db.QueryRow(
			sqlStatement,
			workspaceID,
			wareHouseTest.SourceID,
			wareHouseTest.DestinationID,
			wareHouseTest.TimestampBeforeSendingEvents,
		).Scan(&count)
		require.NoError(t, err)
		return count.Int64 == int64(stagingFileEvents)
	}
	require.Eventually(
		t,
		operation,
		WaitFor2Minute,
		DefaultQueryFrequency,
		fmt.Sprintf("Expected staging files events count is %d and Actual staging files events count is %d",
			stagingFileEvents,
			count.Int64,
		),
	)

	t.Logf("Completed verifying events in staging files")
}

func verifyEventsInLoadFiles(t testing.TB, wareHouseTest *WareHouseTest) {
	t.Helper()
	t.Logf("Started verifying events in load file")

	var (
		loadFileEvents int
		sqlStatement   string
		operation      func() bool
		count          sql.NullInt64
		err            error
		db             = wareHouseTest.JobsDB
		eventsMap      = wareHouseTest.LoadFilesEventsMap
	)

	require.NotEmpty(t, wareHouseTest.SourceID)
	require.NotEmpty(t, wareHouseTest.DestinationID)
	require.NotEmpty(t, eventsMap)
	require.NotNil(t, db)

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
			err = db.QueryRow(
				sqlStatement,
				wareHouseTest.SourceID,
				wareHouseTest.DestinationID,
				wareHouseTest.TimestampBeforeSendingEvents,
				warehouseutils.ToProviderCase(wareHouseTest.Provider, table),
			).Scan(&count)
			require.NoError(t, err)
			return count.Int64 == int64(loadFileEvents)
		}
		require.Eventually(
			t,
			operation,
			WaitFor10Minute,
			DefaultQueryFrequency,
			fmt.Sprintf("Expected load files events count is %d and Actual load files events count is %d for table %s",
				loadFileEvents,
				count.Int64,
				table,
			),
		)
	}

	t.Logf("Completed verifying events in load files")
}

func verifyEventsInTableUploads(t testing.TB, wareHouseTest *WareHouseTest) {
	t.Helper()
	t.Logf("Started verifying events in table uploads")

	var (
		workspaceID       = "BpLnfgDsc2WD8F2qNfHK5a84jjJ"
		tableUploadEvents int
		sqlStatement      string
		operation         func() bool
		count             sql.NullInt64
		err               error
		db                = wareHouseTest.JobsDB
		eventsMap         = wareHouseTest.TableUploadsEventsMap
	)

	require.NotEmpty(t, wareHouseTest.SourceID)
	require.NotEmpty(t, wareHouseTest.DestinationID)
	require.NotEmpty(t, eventsMap)
	require.NotNil(t, db)

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
			   wh_uploads.workspace_id = $1 AND
			   wh_uploads.source_id = $2 AND
			   wh_uploads.destination_id = $3 AND
			   wh_uploads.created_at > $4 AND
			   wh_table_uploads.table_name = $5 AND
			   wh_table_uploads.status = 'exported_data';
		`
		t.Logf("Checking events in table uploads for workspaceID: %s, sourceID: %s, DestinationID: %s, TimestampBeforeSendingEvents: %s, table: %s, sqlStatement: %s",
			workspaceID,
			wareHouseTest.SourceID,
			wareHouseTest.DestinationID,
			wareHouseTest.TimestampBeforeSendingEvents,
			warehouseutils.ToProviderCase(wareHouseTest.Provider, table),
			sqlStatement,
		)
		operation = func() bool {
			err = db.QueryRow(
				sqlStatement,
				workspaceID,
				wareHouseTest.SourceID,
				wareHouseTest.DestinationID,
				wareHouseTest.TimestampBeforeSendingEvents,
				warehouseutils.ToProviderCase(wareHouseTest.Provider, table),
			).Scan(&count)
			require.NoError(t, err)
			return count.Int64 == int64(tableUploadEvents)
		}
		require.Eventually(t,
			operation,
			WaitFor10Minute,
			DefaultQueryFrequency,
			fmt.Sprintf("Expected table uploads events count is %d and Actual table uploads events count is %d for table %s",
				tableUploadEvents,
				count.Int64,
				table,
			),
		)
	}

	t.Logf("Completed verifying events in table uploads")
}

func verifyEventsInWareHouse(t testing.TB, wareHouseTest *WareHouseTest) {
	t.Helper()
	t.Logf("Started verifying events in warehouse")

	eventsMap := wareHouseTest.WarehouseEventsMap

	require.NotEmpty(t, wareHouseTest.Schema)
	require.NotEmpty(t, wareHouseTest.UserID)
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
			wareHouseTest.UserID,
		)
		t.Logf("Checking events in warehouse for schema: %s, table: %s, primaryKey: %s, UserID: %s, sqlStatement: %s",
			wareHouseTest.Schema,
			warehouseutils.ToProviderCase(wareHouseTest.Provider, table),
			primaryKey(table),
			wareHouseTest.UserID,
			sqlStatement,
		)

		require.NoError(t, WithConstantRetries(func() error {
			count, countErr = queryCount(wareHouseTest.Client, sqlStatement)
			if countErr != nil {
				return countErr
			}
			if count != int64(tableCount) {
				return fmt.Errorf("error in counting events in warehouse for schema: %s, table: %s, UserID: %s count: %d, expectedCount: %d",
					wareHouseTest.Schema,
					warehouseutils.ToProviderCase(wareHouseTest.Provider, table),
					wareHouseTest.UserID,
					count,
					tableCount,
				)
			}
			return nil
		}))
	}

	t.Logf("Completed verifying events in warehouse")
}

func verifyAsyncJob(t testing.TB, wareHouseTest *WareHouseTest) {
	t.Helper()
	t.Logf("Started verifying async job")

	workspaceID := "BpLnfgDsc2WD8F2qNfHK5a84jjJ"
	asyncPayload := strings.NewReader(
		fmt.Sprintf(
			AsyncWhPayload,
			wareHouseTest.SourceID,
			wareHouseTest.JobRunID,
			wareHouseTest.TaskRunID,
			wareHouseTest.DestinationID,
			time.Now().UTC().Format("2006-01-02 15:04:05"),
			workspaceID,
		),
	)
	t.Logf("Run async job for sourceID: %s, DestinationID: %s, jobRunID: %s, taskRunID: %s",
		wareHouseTest.SourceID,
		wareHouseTest.DestinationID,
		wareHouseTest.JobRunID,
		wareHouseTest.TaskRunID,
	)

	send(t, asyncPayload, "warehouse/jobs", wareHouseTest.WriteKey, "POST", wareHouseTest.HTTPPort)

	var (
		path = fmt.Sprintf("warehouse/jobs/status?job_run_id=%s&task_run_id=%s&source_id=%s&destination_id=%s&workspace_id=%s",
			wareHouseTest.JobRunID,
			wareHouseTest.TaskRunID,
			wareHouseTest.SourceID,
			wareHouseTest.DestinationID,
			workspaceID,
		)
		url        = fmt.Sprintf("http://localhost:%d/v1/%s", wareHouseTest.HTTPPort, path)
		method     = "GET"
		httpClient = &http.Client{}
		req        *http.Request
		res        *http.Response
		err        error
	)

	type asyncResponse struct {
		Status string `json:"status"`
		Error  string `json:"error"`
	}

	operation := func() bool {
		if req, err = http.NewRequest(method, url, strings.NewReader("")); err != nil {
			return false
		}

		req.Header.Add("Content-Type", "application/json")
		req.Header.Add("Authorization", fmt.Sprintf("Basic %s", base64.StdEncoding.EncodeToString(
			[]byte(fmt.Sprintf("%s:", wareHouseTest.WriteKey)),
		)))

		if res, err = httpClient.Do(req); err != nil {
			return false
		}
		if res.StatusCode != http.StatusOK {
			return false
		}

		defer func() { httputil.CloseResponse(res) }()

		var asyncRes asyncResponse
		if err = json.NewDecoder(res.Body).Decode(&asyncRes); err != nil {
			return false
		}
		return asyncRes.Status == "succeeded"
	}
	require.Eventually(
		t,
		operation,
		WaitFor10Minute,
		AsyncJOBQueryFrequency,
		fmt.Sprintf("Failed to get async job status for job_run_id: %s, task_run_id: %s, source_id: %s, destination_id: %s",
			wareHouseTest.JobRunID,
			wareHouseTest.TaskRunID,
			wareHouseTest.SourceID,
			wareHouseTest.DestinationID,
		),
	)

	t.Logf("Completed verifying async job")
}

func VerifyConfigurationTest(t testing.TB, destination backendconfig.DestinationT) {
	t.Helper()
	t.Logf("Started configuration tests for destination type: %s", destination.DestinationDefinition.Name)

	require.NoError(t, WithConstantRetries(func() error {
		destinationValidator := validations.NewDestinationValidator()
		response := destinationValidator.Validate(&destination)
		if !response.Success {
			return fmt.Errorf("failed to validate credentials for destination: %s with error: %s", destination.DestinationDefinition.Name, response.Error)
		}
		return nil
	}))

	t.Logf("Completed configuration tests for destination type: %s", destination.DestinationDefinition.Name)
}

func queryCount(cl *warehouseclient.Client, statement string) (int64, error) {
	result, err := cl.Query(statement)
	if err != nil || result.Values == nil {
		return 0, err
	}
	return strconv.ParseInt(result.Values[0][0], 10, 64)
}

func WithConstantRetries(operation func() error) error {
	var err error
	for i := 0; i < 5; i++ {
		if err = operation(); err == nil {
			return nil
		}
	}
	return err
}

func defaultSendEventsMap() EventsCountMap {
	return EventsCountMap{
		"identifies": 1,
		"tracks":     1,
		"pages":      1,
		"screens":    1,
		"aliases":    1,
		"groups":     1,
	}
}

func defaultStagingFilesEventsMap() EventsCountMap {
	return EventsCountMap{
		"wh_staging_files": 32,
	}
}

func defaultLoadFilesEventsMap() EventsCountMap {
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

func defaultTableUploadsEventsMap() EventsCountMap {
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

func defaultWarehouseEventsMap() EventsCountMap {
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

func SourcesSendEventsMap() EventsCountMap {
	return EventsCountMap{
		"google_sheet": 1,
	}
}

func SourcesStagingFilesEventsMap() EventsCountMap {
	return EventsCountMap{
		"wh_staging_files": 8,
	}
}

func SourcesLoadFilesEventsMap() EventsCountMap {
	return EventsCountMap{
		"tracks":       4,
		"google_sheet": 4,
	}
}

func SourcesTableUploadsEventsMap() EventsCountMap {
	return EventsCountMap{
		"tracks":       4,
		"google_sheet": 4,
	}
}

func SourcesWarehouseEventsMap() EventsCountMap {
	return EventsCountMap{
		"google_sheet": 4,
		"tracks":       4,
	}
}

func GetUserId(provider string) string {
	return fmt.Sprintf("userId_%s_%s", strings.ToLower(provider), warehouseutils.RandHex())
}

func SetConfig(t testing.TB, kvs []warehouseutils.KeyValue) {
	t.Helper()

	payload, err := json.Marshal(&kvs)
	require.NoError(t, err)

	url := fmt.Sprintf(`%s/v1/setConfig`, misc.GetWarehouseURL())
	_, err = warehouseutils.PostRequestWithTimeout(context.TODO(), url, payload, time.Second*60)
	require.NoError(t, err)
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

func CreateTempFile(t testing.TB, templatePath string, values map[string]string) string {
	t.Helper()

	tpl, err := template.ParseFiles(templatePath)
	require.NoError(t, err)

	tmpFile, err := os.CreateTemp("", "workspaceConfig.*.json")
	require.NoError(t, err)
	defer func() { _ = tmpFile.Close() }()

	require.NoError(t, tpl.Execute(tmpFile, values))
	t.Cleanup(func() {
		if err := os.Remove(tmpFile.Name()); err != nil {
			t.Logf("Error while removing workspace config: %v", err)
		}
	})

	return tmpFile.Name()
}
