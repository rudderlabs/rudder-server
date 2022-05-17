package api_test

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"regexp"
	"testing"

	gomock "github.com/golang/mock/gomock"
	"github.com/rudderlabs/rudder-server/gateway/rudder-sources/api"
	"github.com/rudderlabs/rudder-server/gateway/rudder-sources/model"
	"github.com/stretchr/testify/require"
)

func TestDelete(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mockSVC := api.NewMockSourcesService(mockCtrl)
	sAPI := api.NewSourcesSvc(mockSVC)

	var tests = []struct {
		name                 string
		jobID                string
		endpoint             string
		method               string
		expectedResponseCode int
		serviceReturnError   error
	}{
		{
			name:                 "basic test",
			jobID:                "123",
			endpoint:             prepURL("/v1/job-status/{job_id}", "123"),
			method:               "DELETE",
			expectedResponseCode: 204,
		},
		{
			name:                 "service returns error test",
			jobID:                "123",
			endpoint:             prepURL("/v1/job-status/{job_id}", "123"),
			method:               "DELETE",
			expectedResponseCode: 500,
			serviceReturnError:   fmt.Errorf("something when wrong"),
		},
	}

	for _, tt := range tests {

		t.Run(tt.name, func(t *testing.T) {
			t.Log("endpoint tested:", tt.endpoint)
			mockSVC.EXPECT().Delete(gomock.Any(), tt.jobID).Return(tt.serviceReturnError).Times(1)

			url := fmt.Sprintf("http://localhost:8080%s", tt.endpoint)
			req, err := http.NewRequest(tt.method, url, nil)
			require.NoError(t, err)
			req.Header.Set("Content-Type", "application/json")
			resp := httptest.NewRecorder()

			h := sAPI.Handler()
			h.ServeHTTP(resp, req)
			_, err = ioutil.ReadAll(resp.Body)
			require.NoError(t, err)

			require.Equal(t, tt.expectedResponseCode, resp.Code, "required error different than expected")
		})
	}
}

func TestGetStatus(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mockSVC := api.NewMockSourcesService(mockCtrl)
	sAPI := api.NewSourcesSvc(mockSVC)

	var tests = []struct {
		name                 string
		jobID                string
		endpoint             string
		method               string
		expectedResponseCode int
		filter               map[string][]string
		jobStatus            model.JobStatus
		respBody             string
	}{
		{
			name:                 "basic test",
			jobID:                "123",
			endpoint:             prepURL("/v1/job-status/{job_id}", "123"),
			method:               "GET",
			expectedResponseCode: 200,
			filter: map[string][]string{
				"task_id": {"t1", "t2"},

				"source_id": {"s1"},
			},
			jobStatus: model.JobStatus{
				ID: "123",
				TasksStatus: []model.TaskStatus{
					{
						ID: "t1",
						SourcesStatus: []model.SourceStatus{
							{
								ID:        "s1",
								Completed: false,
								Stats: model.Stats{
									In:     1,
									Out:    1,
									Failed: 0,
								},
								DestinationsStatus: []model.DestinationStatus{
									{
										ID:        "d1",
										Completed: false,
										Stats: model.Stats{
											In:     1,
											Out:    1,
											Failed: 0,
										},
									},
								},
							},
						},
					},
					{
						ID: "t2",
						SourcesStatus: []model.SourceStatus{
							{
								ID:        "s1",
								Completed: false,
								Stats: model.Stats{
									In:     1,
									Out:    1,
									Failed: 0,
								},
								DestinationsStatus: []model.DestinationStatus{
									{
										ID:        "d2",
										Completed: false,
										Stats: model.Stats{
											In:     1,
											Out:    1,
											Failed: 0,
										},
									},
								},
							},
						},
					},
				},
			},
			respBody: `{"id":"123","tasks":[{"id":"t1","sources":[{"id":"s1","completed":false,"stats":{"in":1,"out":1,"failed":0},"destinations":[{"id":"d1","completed":false,"stats":{"in":1,"out":1,"failed":0}}]}]},{"id":"t2","sources":[{"id":"s1","completed":false,"stats":{"in":1,"out":1,"failed":0},"destinations":[{"id":"d2","completed":false,"stats":{"in":1,"out":1,"failed":0}}]}]}]}`,
		},
	}

	for _, tt := range tests {

		t.Run(tt.name, func(t *testing.T) {
			t.Log("endpoint tested:", tt.endpoint)

			filterArg := getArgumentFilter(tt.filter)
			mockSVC.EXPECT().GetStatus(gomock.Any(), tt.jobID, filterArg).Return(tt.jobStatus, nil).Times(1)

			basicUrl := fmt.Sprintf("http://localhost:8080%s", tt.endpoint)
			url := withFilter(basicUrl, tt.filter)
			req, err := http.NewRequest(tt.method, url, nil)
			require.NoError(t, err)
			req.Header.Set("Content-Type", "application/json")
			resp := httptest.NewRecorder()

			h := sAPI.Handler()
			h.ServeHTTP(resp, req)
			body, err := ioutil.ReadAll(resp.Body)
			require.NoError(t, err)

			require.Equal(t, tt.expectedResponseCode, resp.Code, "actual response code different than expected")
			require.Equal(t, tt.respBody, string(body), "actual response body different than expected")
		})
	}
}

func getArgumentFilter(filter map[string][]string) model.JobFilter {
	var filterArg model.JobFilter

	if len(filter["task_id"]) != 0 {
		tID := filter["task_id"]
		filterArg.TaskRunId = &tID
	}
	if len(filter["source_id"]) != 0 {
		sID := filter["source_id"]
		filterArg.SourceId = &sID
	}

	return filterArg
}

func withFilter(basicUrl string, filters map[string][]string) string {
	if len(filters) == 0 {
		return basicUrl
	}

	newURL := basicUrl + "?"
	for key, values := range filters {
		for _, val := range values {
			newURL = newURL + key + "=" + val + "&"
		}
	}
	return newURL[:len(newURL)-1]
}

func prepURL(url string, params ...string) string {
	var re = regexp.MustCompile(`{.*?}`)
	i := 0
	return string(re.ReplaceAllFunc([]byte(url), func(matched []byte) []byte {
		if i >= len(params) {
			panic(fmt.Sprintf("value for %q not provided", matched))
		}
		v := params[i]
		i++
		return []byte(v)
	}))
}
