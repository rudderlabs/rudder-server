package client_test

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/rudderlabs/rudder-server/regulation-worker/internal/client"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
	"github.com/stretchr/testify/require"
)

func TestGet(t *testing.T) {
	var tests = []struct {
		name                      string
		workspaceID               string
		respBody                  string
		respCode                  int
		expectedErr               error
		acutalErr                 error
		expectedUsrAttributeCount int
	}{
		{
			name:                      "Get request to get job: successful",
			workspaceID:               "1001",
			respBody:                  `{"jobId":"1","destinationId":"23","userAttributes":[{"userId":"1","phone":"555-555-5555"},{"userId":"2","email":"john@example.com"}]}`,
			respCode:                  200,
			expectedUsrAttributeCount: 2,
		},
		{
			name:        "Get request to get job: NoRunnableJob found",
			workspaceID: "1001",
			respCode:    404,
			expectedErr: model.ErrNoRunnableJob,
		},
		{
			name:        "Get request to get job: random error",
			workspaceID: "1001",
			respCode:    429,
			expectedErr: fmt.Errorf("error while getting job:{429  <nil>}"),
		},
	}
	for _, tt := range tests {

		t.Run(tt.name, func(t *testing.T) {
			svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tt.respCode)
				fmt.Fprintf(w, tt.respBody)
			}))
			defer svr.Close()

			c := client.JobAPI{
				WorkspaceID: tt.workspaceID,
				URLPrefix:   svr.URL,
			}
			job, err := c.Get(context.Background())
			fmt.Println(job)
			require.Equal(t, tt.expectedErr, err)
			require.Equal(t, tt.expectedUsrAttributeCount, len(job.UserAttributes), "no of users different than expected")
			t.Log("actual job:", job)

		})
	}
}

func TestUpdateStatus(t *testing.T) {
	var tests = []struct {
		name            string
		workspaceID     string
		status          model.JobStatus
		jobID           int
		expectedReqBody string
		respCode        int
		expectedErr     error
	}{
		{
			name:            "update status request: successful",
			workspaceID:     "1001",
			status:          model.JobStatusComplete,
			jobID:           1,
			expectedReqBody: `{"status":"complete"}`,
			respCode:        201,
		},
		{
			name:            "update status request: returns error",
			workspaceID:     "1001",
			status:          model.JobStatusComplete,
			jobID:           1,
			expectedReqBody: `{"status":"complete"}`,
			respCode:        429,
			expectedErr:     fmt.Errorf("update status failed with status code: 429"),
		},
	}
	for _, tt := range tests {

		t.Run(tt.name, func(t *testing.T) {
			var body []byte
			svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tt.respCode)
				body, _ = ioutil.ReadAll(r.Body)
			}))
			defer svr.Close()

			c := client.JobAPI{
				URLPrefix:   svr.URL,
				WorkspaceID: tt.workspaceID,
			}
			err := c.UpdateStatus(context.Background(), tt.status, tt.jobID)
			require.Equal(t, tt.expectedErr, err)
			require.Equal(t, tt.expectedReqBody, string(body), "actual request body different than expected")

		})
	}
}
