package http_test

import (
	"context"
	"database/sql"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"

	"github.com/rudderlabs/rudder-server/services/rsources"
	rsources_http "github.com/rudderlabs/rudder-server/services/rsources/http"
)

func prepare(
	t *testing.T,
	handlerFunc func(service rsources.JobService, logger logger.Logger) http.Handler,
) (
	handler http.Handler,
	service rsources.JobService,
	dbResource *postgres.Resource,
) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	postgresContainer, err := postgres.Setup(pool, t)
	require.NoError(t, err)

	config := rsources.JobServiceConfig{
		LocalHostname:       postgresContainer.Host,
		MaxPoolSize:         1,
		LocalConn:           postgresContainer.DBDsn,
		Log:                 logger.NOP,
		ShouldSetupSharedDB: true,
	}
	sts, err := memstats.New()
	require.NoError(t, err, "should create stats")

	service, err = rsources.NewJobService(context.Background(), config, sts)
	require.NoError(t, err)
	handler = handlerFunc(service, logger.NOP)
	dbResource = postgresContainer
	return handler, service, dbResource
}

func addFailedRecords(
	t *testing.T,
	service rsources.JobService,
	db *sql.DB,
	records []rsources.FailedRecord,
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
	pageToken string,
) *rsources.JobFailedRecordsV2 {
	params := url.Values{}
	if pageSize > 0 {
		params.Set("pageSize", strconv.Itoa(pageSize))
		if pageToken != "" {
			params.Set("pageToken", pageToken)
		}
	}
	reqURL, err := url.Parse("http://localhost/jobRunID/failed-records")
	require.NoError(t, err)
	reqURL.RawQuery = params.Encode()
	req, err := http.NewRequest(http.MethodGet, reqURL.String(), nil)
	require.NoError(t, err)
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, req)
	require.Equal(t, http.StatusOK, resp.Code)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	var failedRecords rsources.JobFailedRecordsV2
	require.NoError(t, jsonrs.Unmarshal(body, &failedRecords))
	return &failedRecords
}

func TestGetFailedRecordsIntegration(t *testing.T) {
	t.Run("without pagination", func(t *testing.T) {
		handler, service, dbResource := prepare(t, rsources_http.NewV2Handler)
		addFailedRecords(t, service, dbResource.DB, []rsources.FailedRecord{
			{Record: []byte(`"id-1"`), Code: 422, Error: "rejected: invalid email"},
			{Record: []byte(`"id-2"`)},
			{Record: []byte(`"id-3"`)},
			{Record: []byte(`"id-4"`)},
		})
		pageSize := 0
		pageToken := ""
		failedRecords := getFailedRecords(t, handler, pageSize, pageToken)
		require.NotNil(t, failedRecords)
		require.Len(t, failedRecords.Tasks, 1)
		require.Len(t, failedRecords.Tasks[0].Sources, 1)
		require.Len(t, failedRecords.Tasks[0].Sources[0].Destinations, 1)
		records := failedRecords.Tasks[0].Sources[0].Destinations[0].Records
		require.Len(t, records, 4)
		require.Nil(t, failedRecords.Paging, "no paging information should be present")

		// The captured message survives the column, the response and the client's
		// decode. The records that carry none are served without the field at all,
		// which is exactly what a new client sees from an older server: an empty
		// message, not a decode failure.
		require.Equal(t, 422, records[0].Code)
		require.Equal(t, "rejected: invalid email", records[0].Error)
		for _, r := range records[1:] {
			require.Empty(t, r.Error, "record %s", r.Record)
		}
	})

	t.Run("with pagination", func(t *testing.T) {
		handler, service, dbResource := prepare(t, rsources_http.NewV2Handler)
		addFailedRecords(t, service, dbResource.DB, []rsources.FailedRecord{
			{Record: []byte(`"id-1"`), Error: "message 1"},
			{Record: []byte(`"id-2"`), Error: "message 2"},
			{Record: []byte(`"id-3"`), Error: "message 3"},
			{Record: []byte(`"id-4"`), Error: "message 4"},
		})
		pageSize := 2
		pageToken := ""
		for page := range 2 { // 2 pages are retrieved with 2 records each and where paging is present
			failedRecords := getFailedRecords(t, handler, pageSize, pageToken)
			require.NotNil(t, failedRecords)
			require.Len(t, failedRecords.Tasks, 1)
			require.Len(t, failedRecords.Tasks[0].Sources, 1)
			require.Len(t, failedRecords.Tasks[0].Sources[0].Destinations, 1)
			records := failedRecords.Tasks[0].Sources[0].Destinations[0].Records
			require.Len(t, records, pageSize)
			require.NotNil(t, failedRecords.Paging, "paging information should be present")
			require.Equal(t, pageSize, failedRecords.Paging.Size)
			pageToken = failedRecords.Paging.NextPageToken

			// The message is carried by every page, and the cursor is still the
			// record id: paging must not drop or reorder it.
			for i, r := range records {
				require.Equalf(t, strconv.Itoa(page*pageSize+i+1),
					strings.TrimSuffix(strings.TrimPrefix(string(r.Record), `"id-`), `"`),
					"page %d record %d is out of order", page, i)
				require.Equalf(t, "message "+strconv.Itoa(page*pageSize+i+1), r.Error,
					"page %d record %d lost its message", page, i)
			}
		}

		// 3 page is retrieved with 0 records and where paging is not present
		failedRecords := getFailedRecords(t, handler, pageSize, pageToken)
		require.NotNil(t, failedRecords)
		require.Len(t, failedRecords.Tasks, 0)
		require.Nil(t, failedRecords.Paging, "no paging information should be present")
	})
}

func TestDeleteEndpoints(t *testing.T) {
	t.Run("delete failed-keys", func(t *testing.T) {
		handler, service, dbResource := prepare(t, rsources_http.NewV2Handler)
		addFailedRecords(t, service, dbResource.DB, []rsources.FailedRecord{
			{Record: []byte(`"id-1"`)},
			{Record: []byte(`"id-2"`)},
			{Record: []byte(`"id-3"`)},
			{Record: []byte(`"id-4"`)},
		})

		tx, err := dbResource.DB.Begin()
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

		t.Run("calling v2 failed-keys delete should only delete failed keys", func(t *testing.T) {
			req, err := http.NewRequest(http.MethodDelete, "http://localhost/jobRunID/failed-records", nil)
			require.NoError(t, err)
			resp := httptest.NewRecorder()
			handler.ServeHTTP(resp, req)
			require.Equal(t, http.StatusNoContent, resp.Code)
			failedRecords := getFailedRecords(t, handler, 10, "")
			require.NotNil(t, failedRecords)
			require.Len(t, failedRecords.Tasks, 0)
			require.Nil(t, failedRecords.Paging, "no paging information should be present")

			req, err = http.NewRequest(http.MethodGet, "http://localhost/jobRunID", nil)
			require.NoError(t, err)
			resp = httptest.NewRecorder()
			handler.ServeHTTP(resp, req)
			require.Equal(t, http.StatusOK, resp.Code)
			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			var jobStatus rsources.JobStatus
			err = jsonrs.Unmarshal(body, &jobStatus)
			require.NoError(t, err)
			require.Len(t, jobStatus.TasksStatus, 1)
			require.Len(t, jobStatus.TasksStatus[0].SourcesStatus, 1)
			require.Len(t, jobStatus.TasksStatus[0].SourcesStatus[0].DestinationsStatus, 1)
			require.Equal(t, uint(15), jobStatus.TasksStatus[0].SourcesStatus[0].DestinationsStatus[0].Stats.In)
			require.Equal(t, uint(6), jobStatus.TasksStatus[0].SourcesStatus[0].DestinationsStatus[0].Stats.Out)
			require.Equal(t, uint(4), jobStatus.TasksStatus[0].SourcesStatus[0].DestinationsStatus[0].Stats.Failed)
		})
	})

	t.Run("delete job-status", func(t *testing.T) {
		handler, service, dbResource := prepare(t, rsources_http.NewV2Handler)
		addFailedRecords(t, service, dbResource.DB, []rsources.FailedRecord{
			{Record: []byte(`"id-1"`)},
			{Record: []byte(`"id-2"`)},
			{Record: []byte(`"id-3"`)},
			{Record: []byte(`"id-4"`)},
		})
		tx, err := dbResource.DB.Begin()
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
					Failed: 9,
				},
			),
		)
		require.NoError(t, tx.Commit())

		t.Run("calling v2 job-status delete should only delete job-status", func(t *testing.T) {
			req, err := http.NewRequest("GET", "http://localhost/jobRunID", nil)
			require.NoError(t, err)
			resp := httptest.NewRecorder()
			handler.ServeHTTP(resp, req)
			require.Equal(t, http.StatusOK, resp.Code)
			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			var jobStatus rsources.JobStatus
			err = jsonrs.Unmarshal(body, &jobStatus)
			require.NoError(t, err)
			require.Len(t, jobStatus.TasksStatus, 1)
			require.Len(t, jobStatus.TasksStatus[0].SourcesStatus, 1)
			require.Len(t, jobStatus.TasksStatus[0].SourcesStatus[0].DestinationsStatus, 1)
			require.Equal(t, uint(15), jobStatus.TasksStatus[0].SourcesStatus[0].DestinationsStatus[0].Stats.In)
			require.Equal(t, uint(6), jobStatus.TasksStatus[0].SourcesStatus[0].DestinationsStatus[0].Stats.Out)
			require.Equal(t, uint(9), jobStatus.TasksStatus[0].SourcesStatus[0].DestinationsStatus[0].Stats.Failed)

			req, err = http.NewRequest("DELETE", "http://localhost/jobRunID", nil)
			require.NoError(t, err)
			resp = httptest.NewRecorder()
			handler.ServeHTTP(resp, req)
			require.Equal(t, http.StatusNoContent, resp.Code)

			req, err = http.NewRequest("GET", "http://localhost/jobRunID", nil)
			require.NoError(t, err)
			resp = httptest.NewRecorder()
			handler.ServeHTTP(resp, req)
			require.Equal(t, http.StatusNotFound, resp.Code)

			failedRecords := getFailedRecords(t, handler, 10, "")
			require.NotNil(t, failedRecords)
			require.Len(t, failedRecords.Tasks, 1)
			require.Nil(t, failedRecords.Paging, "no paging information should be present")
			require.Len(t, failedRecords.Tasks[0].Sources, 1)
			require.Len(t, failedRecords.Tasks[0].Sources[0].Destinations, 1)
			require.Len(t, failedRecords.Tasks[0].Sources[0].Destinations[0].Records, 4)
		})
	})
}
