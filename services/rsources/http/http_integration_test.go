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

func TestGetFailedRecordsIntegration(t *testing.T) {
	prepare := func() (handler http.Handler, service rsources.JobService, db *sql.DB) {
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
		handler = rsources_http.NewHandler(service, logger.NOP)
		db = postgresContainer.DB
		return
	}

	addFailedRecords := func(t *testing.T, service rsources.JobService, db *sql.DB, records []json.RawMessage) {
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

	getFailedRecords := func(t *testing.T, handler http.Handler, pageSize int, pageToken string) *rsources.JobFailedRecords {
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

	t.Run("without pagination", func(t *testing.T) {
		handler, service, db := prepare()
		addFailedRecords(t, service, db, []json.RawMessage{
			[]byte(`"id-1"`),
			[]byte(`"id-2"`),
			[]byte(`"id-3"`),
			[]byte(`"id-4"`),
		})
		pageSize := 0
		pageToken := ""
		failedRecords := getFailedRecords(t, handler, pageSize, pageToken)
		require.NotNil(t, failedRecords)
		require.Len(t, failedRecords.Tasks, 1)
		require.Len(t, failedRecords.Tasks[0].Sources, 1)
		require.Len(t, failedRecords.Tasks[0].Sources[0].Destinations, 1)
		require.Len(t, failedRecords.Tasks[0].Sources[0].Destinations[0].Records, 4)
		require.Nil(t, failedRecords.Paging, "no paging information should be present")
	})

	t.Run("with pagination", func(t *testing.T) {
		handler, service, db := prepare()
		addFailedRecords(t, service, db, []json.RawMessage{
			[]byte(`"id-1"`),
			[]byte(`"id-2"`),
			[]byte(`"id-3"`),
			[]byte(`"id-4"`),
		})
		pageSize := 2
		pageToken := ""
		for i := 0; i < 2; i++ { // 2 pages are retrieved with 2 records each and where paging is present
			failedRecords := getFailedRecords(t, handler, pageSize, pageToken)
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
		failedRecords := getFailedRecords(t, handler, pageSize, pageToken)
		require.NotNil(t, failedRecords)
		require.Len(t, failedRecords.Tasks, 0)
		require.Nil(t, failedRecords.Paging, "no paging information should be present")
	})
}
