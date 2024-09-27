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

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/source"

	"github.com/stretchr/testify/require"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/utils/httputil"
	whclient "github.com/rudderlabs/rudder-server/warehouse/client"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/rudderlabs/rudder-server/warehouse/validations"
)

func verifyEventsInStagingFiles(t testing.TB, testConfig *TestConfig) {
	t.Helper()

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
	t.Logf(
		"Checking events in staging files for workspaceID: %s, srcID: %s, DestID: %s, "+
			"TimestampBeforeSendingEvents: %s, sqlStatement: %s",
		testConfig.WorkspaceID, testConfig.SourceID, testConfig.DestinationID, testConfig.TimestampBeforeSendingEvents,
		sqlStatement,
	)

	var err error
	var count sql.NullInt64
	expectedCount := int64(testConfig.StagingFilesEventsMap["wh_staging_files"])

	operation := func() bool {
		err = testConfig.JobsDB.QueryRow(sqlStatement,
			testConfig.WorkspaceID, testConfig.SourceID, testConfig.DestinationID,
			testConfig.TimestampBeforeSendingEvents,
		).Scan(&count)
		return err == nil && count.Int64 == expectedCount
	}
	require.Eventuallyf(t, operation, WaitFor2Minute, DefaultQueryFrequency,
		"Expected staging files events count is %d, got %d: %v",
		expectedCount, count, err,
	)

	t.Logf("Completed verifying events in staging files")
}

func verifyEventsInTableUploads(t testing.TB, testConfig *TestConfig) {
	t.Helper()

	t.Logf("Started verifying events in table uploads")

	for _, table := range testConfig.Tables {
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
		t.Logf(
			"Checking events in table uploads for workspaceID: %s, SrcID: %s, DestID: %s, "+
				"TimestampBeforeSendingEvents: %s, table: %s, sqlStatement: %s",
			testConfig.WorkspaceID, testConfig.SourceID, testConfig.DestinationID,
			testConfig.TimestampBeforeSendingEvents, whutils.ToProviderCase(testConfig.DestinationType, table),
			sqlStatement,
		)

		var err error
		var count sql.NullInt64
		expectedCount := int64(testConfig.TableUploadsEventsMap[table])
		operation := func() bool {
			err = testConfig.JobsDB.QueryRow(sqlStatement,
				testConfig.WorkspaceID, testConfig.SourceID, testConfig.DestinationID,
				testConfig.TimestampBeforeSendingEvents, whutils.ToProviderCase(testConfig.DestinationType, table),
			).Scan(&count)
			return err == nil && count.Int64 == expectedCount
		}
		require.Eventuallyf(t, operation, WaitFor10Minute, DefaultQueryFrequency,
			"Expected table uploads events count for table %q is %d, got %d: %v",
			table, expectedCount, count, err,
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
		expectedCount := int64(testConfig.WarehouseEventsMap[table])

		sqlStatement := fmt.Sprintf(`
			SELECT
			  count(*)
			FROM
			  %s.%s
			WHERE
			  %s = '%s';`,
			testConfig.Schema,
			whutils.ToProviderCase(testConfig.DestinationType, table),
			primaryKey(table),
			testConfig.UserID,
		)
		t.Logf("Checking events in warehouse for schema: %s, table: %s, primaryKey: %s, UserID: %s, sqlStatement: %s",
			testConfig.Schema, whutils.ToProviderCase(testConfig.DestinationType, table), primaryKey(table),
			testConfig.UserID, sqlStatement,
		)

		var err error
		var count int64
		require.Eventuallyf(t,
			func() bool {
				count, err = queryCount(testConfig.Client, sqlStatement)
				return err == nil && count == expectedCount
			}, WaitFor10Minute, DefaultWarehouseQueryFrequency,
			"Expected %d events in WH (schema: %s, table: %s, userID: %s), got %d: %v",
			expectedCount,
			testConfig.Schema, whutils.ToProviderCase(testConfig.DestinationType, table), testConfig.UserID,
			count, err,
		)
	}

	t.Logf("Completed verifying events in warehouse")
}

func queryCount(cl *whclient.Client, statement string) (int64, error) {
	result, err := cl.Query(statement)
	if err != nil || result.Values == nil {
		return 0, err
	}
	return strconv.ParseInt(result.Values[0][0], 10, 64)
}

func verifySourceJob(t testing.TB, tc *TestConfig) {
	t.Helper()

	t.Logf("Creating source job for sourceID: %s, jobRunID: %s, taskRunID: %s, destinationID: %s, workspaceID: %s",
		tc.SourceID, tc.JobRunID, tc.TaskRunID, tc.DestinationID, tc.WorkspaceID,
	)

	payload := fmt.Sprintf(
		`{
			"source_id":"%s","job_run_id":"%s","task_run_id":"%s","channel":"sources",
			"async_job_type":"deletebyjobrunid","destination_id":"%s","start_time":"%s","workspace_id":"%s"
		}`,
		tc.SourceID,
		tc.JobRunID,
		tc.TaskRunID,
		tc.DestinationID,
		time.Now().UTC().Format(source.CustomTimeLayout),
		tc.WorkspaceID,
	)

	url := fmt.Sprintf("http://localhost:%d/v1/warehouse/jobs", tc.HTTPPort)
	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(payload))
	require.NoError(t, err)

	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Authorization",
		fmt.Sprintf("Basic %s", base64.StdEncoding.EncodeToString(
			[]byte(fmt.Sprintf("%s:", tc.WriteKey)),
		)),
	)

	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { httputil.CloseResponse(res) }()

	_, err = io.ReadAll(res.Body)
	require.NoError(t, err)
	require.Equal(t, res.Status, "200 OK")

	t.Logf("Verify source job status for sourceID: %s, jobRunID: %s, taskRunID: %s, destID: %s, workspaceID: %s",
		tc.SourceID, tc.JobRunID, tc.TaskRunID, tc.DestinationID, tc.WorkspaceID,
	)

	type jobResponse struct {
		Status string `json:"status"`
		Error  string `json:"error"`
	}

	queryParams := fmt.Sprintf(
		"warehouse/jobs/status?job_run_id=%s&task_run_id=%s&source_id=%s&destination_id=%s&workspace_id=%s",
		tc.JobRunID, tc.TaskRunID, tc.SourceID, tc.DestinationID, tc.WorkspaceID,
	)
	url = fmt.Sprintf("http://localhost:%d/v1/%s", tc.HTTPPort, queryParams)

	operation := func() bool {
		if req, err = http.NewRequest(http.MethodGet, url, strings.NewReader("")); err != nil {
			return false
		}

		req.Header.Add("Content-Type", "application/json")
		req.Header.Add("Authorization", fmt.Sprintf("Basic %s", base64.StdEncoding.EncodeToString(
			[]byte(fmt.Sprintf("%s:", tc.WriteKey)),
		)))

		if res, err = http.DefaultClient.Do(req); err != nil {
			return false
		}
		defer func() { httputil.CloseResponse(res) }()

		if res.StatusCode != http.StatusOK {
			return false
		}

		var jr jobResponse
		if err = json.NewDecoder(res.Body).Decode(&jr); err != nil {
			return false
		}
		return jr.Status == model.SourceJobStatusSucceeded.String()
	}
	require.Eventuallyf(t, operation, WaitFor10Minute, SourceJobQueryFrequency,
		"Failed to get source job status for job_run_id: %s, task_run_id: %s, source_id: %s, destination_id: %s: %v",
		tc.JobRunID,
		tc.TaskRunID,
		tc.SourceID,
		tc.DestinationID,
		err,
	)

	t.Logf("Completed verifying source job")
}

func VerifyConfigurationTest(t testing.TB, destination backendconfig.DestinationT) {
	t.Helper()

	t.Logf("Started configuration tests for destination type: %s", destination.DestinationDefinition.Name)

	require.NoError(t, WithConstantRetries(func() error {
		response := validations.NewDestinationValidator().Validate(context.Background(), &destination)
		if !response.Success {
			return fmt.Errorf("failed to validate credentials for destination: %s with error: %s",
				destination.DestinationDefinition.Name, response.Error,
			)
		}
		return nil
	}))

	t.Logf("Completed configuration tests for destination type: %s", destination.DestinationDefinition.Name)
}
