package http_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go.uber.org/mock/gomock"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/logger/mock_logger"
	"github.com/rudderlabs/rudder-server/services/rsources"
	rsources_http "github.com/rudderlabs/rudder-server/services/rsources/http"
)

// TODO: delete this file once we remove support for the v1 api

func TestDeleteV1(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	service := rsources.NewMockJobService(mockCtrl)
	handler := rsources_http.NewV1Handler(service, mock_logger.NewMockLogger(mockCtrl))

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
			expectedResponseCode: http.StatusNotFound,
			serviceReturnError:   rsources.ErrStatusNotFound,
		},
		{
			name:                 "job's source is incomplete",
			jobRunId:             "123",
			jobFilter:            rsources.JobFilter{},
			endpoint:             prepURL("/{job_run_id}", "123"),
			method:               http.MethodDelete,
			expectedResponseCode: http.StatusBadRequest,
			serviceReturnError:   rsources.ErrSourceNotCompleted,
		},
	}

	for _, tt := range tests {
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
		t.Run(tt.name, func(t *testing.T) {
			queryParams := toQueryParams(tt.jobFilter)
			t.Log("endpoint tested:", tt.endpoint+queryParams)
			service.EXPECT().Delete(gomock.Any(), tt.jobRunId, tt.jobFilter).Return(tt.serviceReturnError).Times(1)

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

func TestGetFailedRecordsV1(t *testing.T) {
	var noPaging rsources.PagingInfo
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	service := rsources.NewMockJobService(mockCtrl)
	handler := rsources_http.NewV1Handler(service, mock_logger.NewMockLogger(mockCtrl))

	tests := []struct {
		name                 string
		jobID                string
		endpoint             string
		method               string
		expectedResponseCode int
		filter               map[string][]string
		failedRecords        rsources.JobFailedRecordsV1
		failedRecordsError   error
		respBody             string
	}{
		{
			name:                 "basic test - GetFailedRecords succeeds",
			jobID:                "123",
			endpoint:             prepURL("/{job_run_id}/failed-records", "123"),
			method:               "GET",
			expectedResponseCode: http.StatusOK,
			filter: map[string][]string{
				"task_run_id": {"t1", "t2"},
				"source_id":   {"s1"},
			},
			failedRecordsError: nil,
			failedRecords: rsources.JobFailedRecordsV1{
				ID: "123",
				Tasks: []rsources.TaskFailedRecords[json.RawMessage]{{
					ID: "t1",
					Sources: []rsources.SourceFailedRecords[json.RawMessage]{{
						ID: "s1",
						Destinations: []rsources.DestinationFailedRecords[json.RawMessage]{{
							ID: "d1",
							Records: []json.RawMessage{
								json.RawMessage(`{"id":"record_123"}`),
							},
						}},
					}},
				}},
			},
			respBody: `{"id":"123","tasks":[{"id":"t1","sources":[{"id":"s1","records":null,"destinations":[{"id":"d1","records":[{"id":"record_123"}]}]}]}]}`,
		},
		{
			name:                 "get failed records basic test with no failed records",
			jobID:                "123",
			endpoint:             prepURL("/{job_run_id}/failed-records", "123"),
			method:               "GET",
			expectedResponseCode: 200,
			failedRecordsError:   nil,
			filter: map[string][]string{
				"task_run_id": {"t1", "t2"},
				"source_id":   {"s1"},
			},
			failedRecords: rsources.JobFailedRecordsV1{ID: "123"},
			respBody:      `{"id":"123","tasks":null}`,
		},
		{
			name:                 "get failed records basic test - GetFailedRecords fails",
			jobID:                "123",
			endpoint:             prepURL("/{job_run_id}/failed-records", "123"),
			method:               "GET",
			expectedResponseCode: 500,
			filter: map[string][]string{
				"task_run_id": {"t1", "t2"},
				"source_id":   {"s1"},
			},
			failedRecords:      rsources.JobFailedRecordsV1{ID: "123"},
			failedRecordsError: errors.New("failed to get failed records"),
			respBody:           failedRecordsRespBody,
		},
		{
			name:                 "get failed records with invalid pagination token",
			jobID:                "123",
			endpoint:             prepURL("/{job_run_id}/failed-records", "123"),
			method:               "GET",
			expectedResponseCode: http.StatusBadRequest,
			filter: map[string][]string{
				"task_run_id": {"t1", "t2"},
				"source_id":   {"s1"},
			},
			failedRecords:      rsources.JobFailedRecordsV1{ID: "123"},
			failedRecordsError: rsources.ErrInvalidPaginationToken,
			respBody:           rsources.ErrInvalidPaginationToken.Error() + "\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Log("endpoint tested:", tt.endpoint)

			filterArg := getArgumentFilter(tt.filter)
			service.EXPECT().GetFailedRecordsV1(gomock.Any(), tt.jobID, filterArg, noPaging).Return(tt.failedRecords, tt.failedRecordsError).Times(1)

			basicUrl := fmt.Sprintf("http://localhost:8080%s", tt.endpoint)
			url := withFilter(basicUrl, tt.filter)
			req, err := http.NewRequest(tt.method, url, http.NoBody)
			require.NoError(t, err)
			req.Header.Set("Content-Type", "application/json")
			resp := httptest.NewRecorder()

			handler.ServeHTTP(resp, req)
			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)

			require.Equal(t, tt.expectedResponseCode, resp.Code, "actual response code different than expected")
			require.Equal(t, tt.respBody, string(body), "actual response body different than expected")
		})
	}
}

func TestFailedRecordsDisabledV1(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	service := rsources.NewMockJobService(mockCtrl)
	handler := rsources_http.NewV1Handler(service, mock_logger.NewMockLogger(mockCtrl))

	service.EXPECT().GetFailedRecordsV1(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(rsources.JobFailedRecordsV1{}, rsources.ErrOperationNotSupported).Times(1)

	url := fmt.Sprintf("http://localhost:8080%s", prepURL("/{job_run_id}/failed-records", "123"))
	req, err := http.NewRequest("GET", url, http.NoBody)
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()

	handler.ServeHTTP(resp, req)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	require.Equal(t, 400, resp.Code, "actual response code different than expected")
	require.Equal(t, "rsources: operation not supported\n", string(body), "actual response body different than expected")
}
