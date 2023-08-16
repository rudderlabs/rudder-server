package testhelper

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/utils/httputil"
	warehouseclient "github.com/rudderlabs/rudder-server/warehouse/client"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/rudderlabs/rudder-server/warehouse/validations"
)

func verifyEventsInStagingFiles(t testing.TB, testConfig *TestConfig) {
	t.Helper()

	t.Logf("Started verifying events in staging files")

	sqlStatement := `
		SELECT
			SUM(total_events) AS sum
		FROM
			wh_staging_files
		WHERE
		   	workspace_id = $1 AND
		   	source_id = $2 AND
		   	destination_id = $3 AND
		   	created_at > $4;`
	t.Logf("Checking events in staging files for workspaceID: %s, sourceID: %s, DestinationID: %s, TimestampBeforeSendingEvents: %s, sqlStatement: %s", testConfig.WorkspaceID, testConfig.SourceID, testConfig.DestinationID, testConfig.TimestampBeforeSendingEvents, sqlStatement)

	count := sql.NullInt64{}
	expectedCount := testConfig.StagingFilesEventsMap["wh_staging_files"]

	operation := func() bool {
		err := testConfig.JobsDB.QueryRow(sqlStatement, testConfig.WorkspaceID, testConfig.SourceID, testConfig.DestinationID, testConfig.TimestampBeforeSendingEvents).Scan(&count)
		require.NoError(t, err)

		return count.Int64 == int64(expectedCount)
	}
	require.Eventually(t, operation, WaitFor2Minute, DefaultQueryFrequency,
		fmt.Sprintf("Expected staging files events count is %d and Actual staging files events count is %d",
			expectedCount,
			count.Int64,
		),
	)

	t.Logf("Completed verifying events in staging files")
}

func verifyEventsInLoadFiles(t testing.TB, testConfig *TestConfig) {
	t.Helper()

	t.Logf("Started verifying events in load file")

	for _, table := range testConfig.Tables {
		count := sql.NullInt64{}
		expectedCount := testConfig.LoadFilesEventsMap[table]

		sqlStatement := `
			SELECT
			   SUM(total_events) AS sum
			FROM
			   wh_load_files
			WHERE
			   source_id = $1
			   AND destination_id = $2
			   AND created_at > $3
			   AND table_name = $4;`
		t.Logf("Checking events in load files for sourceID: %s, DestinationID: %s, TimestampBeforeSendingEvents: %s, table: %s, sqlStatement: %s", testConfig.SourceID, testConfig.DestinationID, testConfig.TimestampBeforeSendingEvents, warehouseutils.ToProviderCase(testConfig.DestinationType, table), sqlStatement)

		operation := func() bool {
			err := testConfig.JobsDB.QueryRow(sqlStatement, testConfig.SourceID, testConfig.DestinationID, testConfig.TimestampBeforeSendingEvents, warehouseutils.ToProviderCase(testConfig.DestinationType, table)).Scan(&count)
			require.NoError(t, err)

			return count.Int64 == int64(expectedCount)
		}
		require.Eventually(t, operation, WaitFor10Minute, DefaultQueryFrequency,
			fmt.Sprintf("Expected load files events count is %d and Actual load files events count is %d for table %s",
				expectedCount,
				count.Int64,
				table,
			),
		)
	}

	t.Logf("Completed verifying events in load files")
}

func verifyEventsInTableUploads(t testing.TB, testConfig *TestConfig) {
	t.Helper()

	t.Logf("Started verifying events in table uploads")

	for _, table := range testConfig.Tables {
		count := sql.NullInt64{}
		expectedCount := testConfig.TableUploadsEventsMap[table]

		sqlStatement := `
			SELECT
			   SUM(total_events) AS sum
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
			   wh_table_uploads.status = 'exported_data';`
		t.Logf("Checking events in table uploads for workspaceID: %s, sourceID: %s, DestinationID: %s, TimestampBeforeSendingEvents: %s, table: %s, sqlStatement: %s", testConfig.WorkspaceID, testConfig.SourceID, testConfig.DestinationID, testConfig.TimestampBeforeSendingEvents, warehouseutils.ToProviderCase(testConfig.DestinationType, table), sqlStatement)

		operation := func() bool {
			err := testConfig.JobsDB.QueryRow(sqlStatement, testConfig.WorkspaceID, testConfig.SourceID, testConfig.DestinationID, testConfig.TimestampBeforeSendingEvents, warehouseutils.ToProviderCase(testConfig.DestinationType, table)).Scan(&count)
			require.NoError(t, err)

			return count.Int64 == int64(expectedCount)
		}
		require.Eventually(t, operation, WaitFor10Minute, DefaultQueryFrequency,
			fmt.Sprintf("Expected table uploads events count is %d and Actual table uploads events count is %d for table %s",
				expectedCount,
				count.Int64,
				table,
			),
		)
	}

	t.Logf("Completed verifying events in table uploads")
}

func verifyEventsInWareHouse(t testing.TB, testConfig *TestConfig) {
	t.Helper()

	t.Logf("Started verifying events in warehouse")

	primaryKey := func(tableName string) string {
		if tableName == "users" {
			return "id"
		}
		return "user_id"
	}

	for _, table := range testConfig.Tables {
		expectedCount := testConfig.WarehouseEventsMap[table]

		sqlStatement := fmt.Sprintf(`
			SELECT
			  count(*)
			FROM
			  %s.%s
			WHERE
			  %s = '%s';`,
			testConfig.Schema,
			warehouseutils.ToProviderCase(testConfig.DestinationType, table),
			primaryKey(table),
			testConfig.UserID,
		)
		t.Logf("Checking events in warehouse for schema: %s, table: %s, primaryKey: %s, UserID: %s, sqlStatement: %s", testConfig.Schema, warehouseutils.ToProviderCase(testConfig.DestinationType, table), primaryKey(table), testConfig.UserID, sqlStatement)

		require.NoError(t, WithConstantRetries(func() error {
			count, countErr := queryCount(testConfig.Client, sqlStatement)
			if countErr != nil {
				return countErr
			}
			if count != int64(expectedCount) {
				return fmt.Errorf("error in counting events in warehouse for schema: %s, table: %s, UserID: %s count: %d, expectedCount: %d", testConfig.Schema, warehouseutils.ToProviderCase(testConfig.DestinationType, table), testConfig.UserID, count, expectedCount)
			}
			return nil
		}))
	}

	t.Logf("Completed verifying events in warehouse")
}

func queryCount(cl *warehouseclient.Client, statement string) (int64, error) {
	result, err := cl.Query(statement)
	if err != nil || result.Values == nil {
		return 0, err
	}
	return strconv.ParseInt(result.Values[0][0], 10, 64)
}

func verifyAsyncJob(t testing.TB, testConfig *TestConfig) {
	t.Helper()

	t.Logf("Started verifying async job")

	t.Logf("Creating async job for sourceID: %s, jobRunID: %s, taskRunID: %s, destinationID: %s, workspaceID: %s", testConfig.SourceID, testConfig.JobRunID, testConfig.TaskRunID, testConfig.DestinationID, testConfig.WorkspaceID)

	asyncPayload := fmt.Sprintf(`{"source_id":"%s","job_run_id":"%s","task_run_id":"%s","channel":"sources","async_job_type":"deletebyjobrunid","destination_id":"%s","start_time":"%s","workspace_id":"%s"}	`,
		testConfig.SourceID,
		testConfig.JobRunID,
		testConfig.TaskRunID,
		testConfig.DestinationID,
		time.Now().UTC().Format("2006-01-02 15:04:05"),
		testConfig.WorkspaceID,
	)

	httpClient := &http.Client{}

	url := fmt.Sprintf("http://localhost:%d/v1/warehouse/jobs", testConfig.HTTPPort)
	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(asyncPayload))
	require.NoError(t, err)

	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Authorization",
		fmt.Sprintf("Basic %s", base64.StdEncoding.EncodeToString(
			[]byte(fmt.Sprintf("%s:", testConfig.WriteKey)),
		)),
	)

	res, err := httpClient.Do(req)
	require.NoError(t, err)
	defer func() { httputil.CloseResponse(res) }()

	_, err = io.ReadAll(res.Body)
	require.NoError(t, err)
	require.Equal(t, res.Status, "200 OK")

	t.Logf("Verify async job status for sourceID: %s, jobRunID: %s, taskRunID: %s, destinationID: %s, workspaceID: %s", testConfig.SourceID, testConfig.JobRunID, testConfig.TaskRunID, testConfig.DestinationID, testConfig.WorkspaceID)

	type asyncResponse struct {
		Status string `json:"status"`
		Error  string `json:"error"`
	}

	queryParams := fmt.Sprintf("warehouse/jobs/status?job_run_id=%s&task_run_id=%s&source_id=%s&destination_id=%s&workspace_id=%s", testConfig.JobRunID, testConfig.TaskRunID, testConfig.SourceID, testConfig.DestinationID, testConfig.WorkspaceID)
	url = fmt.Sprintf("http://localhost:%d/v1/%s", testConfig.HTTPPort, queryParams)

	operation := func() bool {
		if req, err = http.NewRequest(http.MethodGet, url, strings.NewReader("")); err != nil {
			return false
		}

		req.Header.Add("Content-Type", "application/json")
		req.Header.Add("Authorization", fmt.Sprintf("Basic %s", base64.StdEncoding.EncodeToString(
			[]byte(fmt.Sprintf("%s:", testConfig.WriteKey)),
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
	require.Eventually(t, operation, WaitFor10Minute, AsyncJOBQueryFrequency,
		fmt.Sprintf("Failed to get async job status for job_run_id: %s, task_run_id: %s, source_id: %s, destination_id: %s",
			testConfig.JobRunID,
			testConfig.TaskRunID,
			testConfig.SourceID,
			testConfig.DestinationID,
		),
	)

	t.Logf("Completed verifying async job")
}

func VerifyConfigurationTest(t testing.TB, destination backendconfig.DestinationT) {
	t.Helper()

	t.Logf("Started configuration tests for destination type: %s", destination.DestinationDefinition.Name)

	require.NoError(t, WithConstantRetries(func() error {
		response := validations.NewDestinationValidator().Validate(context.Background(), &destination)
		if !response.Success {
			return fmt.Errorf("failed to validate credentials for destination: %s with error: %s", destination.DestinationDefinition.Name, response.Error)
		}

		return nil
	}))

	t.Logf("Completed configuration tests for destination type: %s", destination.DestinationDefinition.Name)
}
