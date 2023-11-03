package http_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource"
	"github.com/rudderlabs/rudder-server/services/rsources"
	rsources_http "github.com/rudderlabs/rudder-server/services/rsources/http"
)

func prepare(
	t *testing.T,
	handlerType func(service rsources.JobService, logger logger.Logger) http.Handler,
) (
	handler http.Handler,
	service rsources.JobService,
	db *sql.DB,
) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	postgresContainer, err := resource.SetupPostgres(pool, t)
	require.NoError(t, err)

	config := rsources.JobServiceConfig{
		LocalHostname: postgresContainer.Host,
		MaxPoolSize:   1,
		LocalConn:     postgresContainer.DBDsn,
		Log:           logger.NOP,
	}
	service, err = rsources.NewJobService(config)
	require.NoError(t, err)
	handler = handlerType(service, logger.NOP)
	db = postgresContainer.DB
	return
}

func addFailedRecords(
	t *testing.T,
	service rsources.JobService,
	db *sql.DB,
	records []json.RawMessage,
) {
	tx, err := db.Begin()
	require.NoError(t, err)
	err = service.AddFailedRecords(context.Background(), tx,
		"jobRunID",
		rsources.JobTargetKey{
			TaskRunID:     "taskRunID",
			SourceID:      "sourceID",
			DestinationID: "destinationID",
		},
		records,
	)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
}

func getFailedRecords(
	t *testing.T,
	handler http.Handler,
	pageSize int,
	pageToken,
	endpoint string,
) *rsources.JobFailedRecords {
	params := url.Values{}
	if pageSize > 0 {
		params.Set("pageSize", strconv.Itoa(pageSize))
		if pageToken != "" {
			params.Set("pageToken", pageToken)
		}
	}
	reqURL, err := url.Parse("http://localhost/" + endpoint)
	require.NoError(t, err)
	reqURL.RawQuery = params.Encode()
	req, err := http.NewRequest("GET", reqURL.String(), nil)
	require.NoError(t, err)
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, req)
	require.Equal(t, http.StatusOK, resp.Code)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	var failedRecords rsources.JobFailedRecords
	err = json.Unmarshal(body, &failedRecords)
	require.NoError(t, err)
	return &failedRecords
}

func TestGetFailedRecordsIntegration(t *testing.T) {
	t.Run("without pagination", func(t *testing.T) {
		handler, service, db := prepare(t, rsources_http.NewV1Handler)
		addFailedRecords(t, service, db, []json.RawMessage{
			[]byte(`"id-1"`),
			[]byte(`"id-2"`),
			[]byte(`"id-3"`),
			[]byte(`"id-4"`),
		})
		pageSize := 0
		pageToken := ""
		failedRecords := getFailedRecords(
			t,
			handler,
			pageSize,
			pageToken,
			"jobRunID/failed-records",
		)
		require.NotNil(t, failedRecords)
		require.Len(t, failedRecords.Tasks, 1)
		require.Len(t, failedRecords.Tasks[0].Sources, 1)
		require.Len(t, failedRecords.Tasks[0].Sources[0].Destinations, 1)
		require.Len(t, failedRecords.Tasks[0].Sources[0].Destinations[0].Records, 4)
		require.Nil(t, failedRecords.Paging, "no paging information should be present")
	})

	t.Run("with pagination", func(t *testing.T) {
		handler, service, db := prepare(t, rsources_http.NewV1Handler)
		addFailedRecords(t, service, db, []json.RawMessage{
			[]byte(`"id-1"`),
			[]byte(`"id-2"`),
			[]byte(`"id-3"`),
			[]byte(`"id-4"`),
		})
		pageSize := 2
		pageToken := ""
		for i := 0; i < 2; i++ { // 2 pages are retrieved with 2 records each and where paging is present
			failedRecords := getFailedRecords(
				t,
				handler,
				pageSize,
				pageToken,
				"jobRunID/failed-records",
			)
			require.NotNil(t, failedRecords)
			require.Len(t, failedRecords.Tasks, 1)
			require.Len(t, failedRecords.Tasks[0].Sources, 1)
			require.Len(t, failedRecords.Tasks[0].Sources[0].Destinations, 1)
			require.Len(t, failedRecords.Tasks[0].Sources[0].Destinations[0].Records, pageSize)
			require.NotNil(t, failedRecords.Paging, "paging information should be present")
			require.Equal(t, pageSize, failedRecords.Paging.Size)
			pageToken = failedRecords.Paging.NextPageToken
		}

		// 3 page is retrieved with 0 records and where paging is not present
		failedRecords := getFailedRecords(
			t,
			handler,
			pageSize,
			pageToken,
			"jobRunID/failed-records",
		)
		require.NotNil(t, failedRecords)
		require.Len(t, failedRecords.Tasks, 0)
		require.Nil(t, failedRecords.Paging, "no paging information should be present")
	})
}

func TestFailedRecordsV2(t *testing.T) {
	t.Run("without pagination", func(t *testing.T) {
		handler, service, db := prepare(t, rsources_http.FailedKeysHandler)
		addFailedRecords(t, service, db, []json.RawMessage{
			[]byte(`"id-1"`),
			[]byte(`"id-2"`),
			[]byte(`"id-3"`),
			[]byte(`"id-4"`),
		})
		pageSize := 0
		pageToken := ""
		failedRecords := getFailedRecords(t, handler, pageSize, pageToken, "jobRunID")
		require.NotNil(t, failedRecords)
		require.Len(t, failedRecords.Tasks, 1)
		require.Len(t, failedRecords.Tasks[0].Sources, 1)
		require.Len(t, failedRecords.Tasks[0].Sources[0].Destinations, 1)
		require.Len(t, failedRecords.Tasks[0].Sources[0].Destinations[0].Records, 4)
		require.Nil(t, failedRecords.Paging, "no paging information should be present")
	})

	t.Run("with pagination", func(t *testing.T) {
		handler, service, db := prepare(t, rsources_http.FailedKeysHandler)
		addFailedRecords(t, service, db, []json.RawMessage{
			[]byte(`"id-1"`),
			[]byte(`"id-2"`),
			[]byte(`"id-3"`),
			[]byte(`"id-4"`),
		})
		pageSize := 2
		pageToken := ""
		for i := 0; i < 2; i++ { // 2 pages are retrieved with 2 records each and where paging is present
			failedRecords := getFailedRecords(t, handler, pageSize, pageToken, "jobRunID")
			require.NotNil(t, failedRecords)
			require.Len(t, failedRecords.Tasks, 1)
			require.Len(t, failedRecords.Tasks[0].Sources, 1)
			require.Len(t, failedRecords.Tasks[0].Sources[0].Destinations, 1)
			require.Len(t, failedRecords.Tasks[0].Sources[0].Destinations[0].Records, pageSize)
			require.NotNil(t, failedRecords.Paging, "paging information should be present")
			require.Equal(t, pageSize, failedRecords.Paging.Size)
			pageToken = failedRecords.Paging.NextPageToken
		}

		// 3 page is retrieved with 0 records and where paging is not present
		failedRecords := getFailedRecords(t, handler, pageSize, pageToken, "jobRunID")
		require.NotNil(t, failedRecords)
		require.Len(t, failedRecords.Tasks, 0)
		require.Nil(t, failedRecords.Paging, "no paging information should be present")
	})
}

func TestDeleteEndpoints(t *testing.T) {
	t.Run("v2 delete endpoints delete only failed-keys", func(t *testing.T) {
		fkHandler, service, db := prepare(t, rsources_http.FailedKeysHandler)
		addFailedRecords(t, service, db, []json.RawMessage{
			[]byte(`"id-1"`),
			[]byte(`"id-2"`),
			[]byte(`"id-3"`),
			[]byte(`"id-4"`),
		})
		jsHandler := rsources_http.JobStatusHandler(service, logger.NOP)
		tx, err := db.Begin()
		require.NoError(t, err)
		require.NoError(
			t,
			service.IncrementStats(
				context.Background(),
				tx,
				"jobRunID",
				rsources.JobTargetKey{
					TaskRunID:     "taskRunID",
					SourceID:      "sourceID",
					DestinationID: "destinationID",
				},
				rsources.Stats{
					In:     15,
					Out:    6,
					Failed: 4,
				},
			),
		)
		require.NoError(t, tx.Commit())
		t.Run("should retrieve failed keys from v2 endpoint", func(t *testing.T) {
			failedRecords := getFailedRecords(t, fkHandler, 10, "", "jobRunID")
			require.NotNil(t, failedRecords)
			require.Len(t, failedRecords.Tasks, 1)
			require.Nil(t, failedRecords.Paging, "no paging information should be present")
		})
		t.Run("calling v2 failed-keys delete should only delete failed keys", func(t *testing.T) {
			req, err := http.NewRequest("DELETE", "http://localhost/jobRunID", nil)
			require.NoError(t, err)
			resp := httptest.NewRecorder()
			fkHandler.ServeHTTP(resp, req)
			require.Equal(t, http.StatusNoContent, resp.Code)
			failedRecords := getFailedRecords(t, fkHandler, 10, "", "jobRunID")
			require.NotNil(t, failedRecords)
			require.Len(t, failedRecords.Tasks, 0)
			require.Nil(t, failedRecords.Paging, "no paging information should be present")

			req, err = http.NewRequest("GET", "http://localhost/jobRunID", nil)
			require.NoError(t, err)
			resp = httptest.NewRecorder()
			jsHandler.ServeHTTP(resp, req)
			require.Equal(t, http.StatusOK, resp.Code)
			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			var jobStatus rsources.JobStatus
			err = json.Unmarshal(body, &jobStatus)
			require.NoError(t, err)
			require.Len(t, jobStatus.TasksStatus, 1)
			require.Len(t, jobStatus.TasksStatus[0].SourcesStatus, 1)
			require.Len(t, jobStatus.TasksStatus[0].SourcesStatus[0].DestinationsStatus, 1)
			require.Equal(t, uint(15), jobStatus.TasksStatus[0].SourcesStatus[0].DestinationsStatus[0].Stats.In)
			require.Equal(t, uint(6), jobStatus.TasksStatus[0].SourcesStatus[0].DestinationsStatus[0].Stats.Out)
			require.Equal(t, uint(4), jobStatus.TasksStatus[0].SourcesStatus[0].DestinationsStatus[0].Stats.Failed)
		})
	})

	t.Run("v2 delete endpoints delete only job-status", func(t *testing.T) {
		fkHandler, service, db := prepare(t, rsources_http.FailedKeysHandler)
		addFailedRecords(t, service, db, []json.RawMessage{
			[]byte(`"id-1"`),
			[]byte(`"id-2"`),
			[]byte(`"id-3"`),
			[]byte(`"id-4"`),
		})
		jsHandler := rsources_http.JobStatusHandler(service, logger.NOP)
		tx, err := db.Begin()
		require.NoError(t, err)
		require.NoError(
			t,
			service.IncrementStats(
				context.Background(),
				tx,
				"jobRunID",
				rsources.JobTargetKey{
					TaskRunID:     "taskRunID",
					SourceID:      "sourceID",
					DestinationID: "destinationID",
				},
				rsources.Stats{
					In:     15,
					Out:    6,
					Failed: 4,
				},
			),
		)
		require.NoError(t, tx.Commit())

		t.Run("calling v2 job-status delete should only delete job-status", func(t *testing.T) {
			req, err := http.NewRequest("GET", "http://localhost/jobRunID", nil)
			require.NoError(t, err)
			resp := httptest.NewRecorder()
			jsHandler.ServeHTTP(resp, req)
			require.Equal(t, http.StatusOK, resp.Code)
			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			var jobStatus rsources.JobStatus
			err = json.Unmarshal(body, &jobStatus)
			require.NoError(t, err)
			require.Len(t, jobStatus.TasksStatus, 1)
			require.Len(t, jobStatus.TasksStatus[0].SourcesStatus, 1)
			require.Len(t, jobStatus.TasksStatus[0].SourcesStatus[0].DestinationsStatus, 1)
			require.Equal(t, uint(15), jobStatus.TasksStatus[0].SourcesStatus[0].DestinationsStatus[0].Stats.In)
			require.Equal(t, uint(6), jobStatus.TasksStatus[0].SourcesStatus[0].DestinationsStatus[0].Stats.Out)
			require.Equal(t, uint(4), jobStatus.TasksStatus[0].SourcesStatus[0].DestinationsStatus[0].Stats.Failed)

			req, err = http.NewRequest("DELETE", "http://localhost/jobRunID", nil)
			require.NoError(t, err)
			resp = httptest.NewRecorder()
			jsHandler.ServeHTTP(resp, req)
			require.Equal(t, http.StatusNoContent, resp.Code)
			failedRecords := getFailedRecords(t, fkHandler, 10, "", "jobRunID")
			require.NotNil(t, failedRecords)

			req, err = http.NewRequest("GET", "http://localhost/jobRunID", nil)
			require.NoError(t, err)
			resp = httptest.NewRecorder()
			jsHandler.ServeHTTP(resp, req)
			require.Equal(t, http.StatusNotFound, resp.Code)

			req, err = http.NewRequest("DELETE", "http://localhost/jobRunID", nil)
			require.NoError(t, err)
			resp = httptest.NewRecorder()
			fkHandler.ServeHTTP(resp, req)
			require.Equal(t, http.StatusNoContent, resp.Code)
			failedRecords = getFailedRecords(t, fkHandler, 10, "", "jobRunID")
			require.NotNil(t, failedRecords)
			require.Len(t, failedRecords.Tasks, 0)
			require.Nil(t, failedRecords.Paging, "no paging information should be present")
		})

	})
}
