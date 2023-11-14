package http_test

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	gomock "github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	mock_logger "github.com/rudderlabs/rudder-server/mocks/utils/logger"
	"github.com/rudderlabs/rudder-server/services/rsources"
	rsources_http "github.com/rudderlabs/rudder-server/services/rsources/http"
)

func TestFailedKeysHandler(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	service := rsources.NewMockJobService(mockCtrl)
	handler := rsources_http.FailedKeysHandler(
		service,
		mock_logger.NewMockLogger(mockCtrl),
	)
	tests := []struct {
		name                 string
		jobRunId             string
		jobFilter            rsources.JobFilter
		endpoint             string
		method               string
		expectedResponseCode int
		serviceReturnError   error
	}{
		{
			name:                 "basic test",
			jobRunId:             "123",
			jobFilter:            rsources.JobFilter{},
			endpoint:             prepURL("/{job_run_id}", "123"),
			method:               http.MethodDelete,
			expectedResponseCode: http.StatusNoContent,
		},
		{
			name:                 "service returns error test",
			jobRunId:             "123",
			jobFilter:            rsources.JobFilter{},
			endpoint:             prepURL("/{job_run_id}", "123"),
			method:               http.MethodDelete,
			expectedResponseCode: http.StatusInternalServerError,
			serviceReturnError:   fmt.Errorf("something when wrong"),
		},
		{
			name:                 "job run id doesn't exist",
			jobRunId:             "invalid",
			jobFilter:            rsources.JobFilter{},
			endpoint:             prepURL("/{job_run_id}", "invalid"),
			method:               http.MethodDelete,
			expectedResponseCode: http.StatusNoContent,
		},
		{
			name:                 "job's source is incomplete",
			jobRunId:             "123",
			jobFilter:            rsources.JobFilter{},
			endpoint:             prepURL("/{job_run_id}", "123"),
			method:               http.MethodDelete,
			expectedResponseCode: http.StatusNoContent,
		},
	}
	toQueryParams := func(jobFilter rsources.JobFilter) string {
		var params []string
		for _, v := range jobFilter.TaskRunID {
			params = append(params, fmt.Sprintf("task_run_id=%s", v))
		}
		for _, v := range jobFilter.SourceID {
			params = append(params, fmt.Sprintf("source_id=%s", v))
		}
		if len(params) == 0 {
			return ""
		}
		return "?" + strings.Join(params, "&")
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			queryParams := toQueryParams(tt.jobFilter)
			t.Log("endpoint tested:", tt.endpoint+queryParams)
			service.EXPECT().DeleteFailedRecords(gomock.Any(), tt.jobRunId, tt.jobFilter).Return(tt.serviceReturnError).Times(1)

			url := fmt.Sprintf("http://localhost:8080%s%s", tt.endpoint, queryParams)
			req, err := http.NewRequest(tt.method, url, http.NoBody)
			require.NoError(t, err)
			req.Header.Set("Content-Type", "application/json")
			resp := httptest.NewRecorder()

			handler.ServeHTTP(resp, req)
			_, err = io.ReadAll(resp.Body)
			require.NoError(t, err)

			require.Equal(t, tt.expectedResponseCode, resp.Code, "required error different than expected")
		})
	}
}

func TestJobStatusHandler(t *testing.T) {}
