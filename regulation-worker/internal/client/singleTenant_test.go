package client_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/rudderlabs/rudder-server/regulation-worker/internal/client"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/initialize"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
	"github.com/stretchr/testify/require"
)

func TestGet(t *testing.T) {
	initialize.Init()
	tests := []struct {
		name                      string
		workspaceID               string
		namespaceID               string
		respBody                  string
		respCode                  int
		expectedErr               error
		acutalErr                 error
		expectedUsrAttributeCount int
		serverDelay               int
	}{
		{
			name:                      "Get request to get job: successful",
			workspaceID:               "1001",
			namespaceID:               "1001",
			respBody:                  `{"jobId":"1","destinationId":"23","userAttributes":[{"userId":"1","phone":"555-555-5555"},{"userId":"2","email":"john@example.com"},{"userId":"3","randomKey":"randomValue"}]}`,
			respCode:                  200,
			expectedUsrAttributeCount: 3,
		},
		{
			name:        "Get request to get job: NoRunnableJob found",
			workspaceID: "1001",
			namespaceID: "1001",
			respCode:    204,
			expectedErr: model.ErrNoRunnableJob,
		},
		{
			name:        "Get request to get job: random error",
			workspaceID: "1001",
			namespaceID: "1001",
			respCode:    429,
			expectedErr: fmt.Errorf("unexpected response code: 429"),
		},
		{
			name:        "Get request to get model.ErrRequestTimeout",
			workspaceID: "1001",
			namespaceID: "1001",
			expectedErr: model.ErrRequestTimeout,
			serverDelay: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tt.respCode != 0 {
					w.WriteHeader(tt.respCode)
				}
				time.Sleep(time.Duration(tt.serverDelay) * time.Millisecond)
				fmt.Fprintf(w, tt.respBody)
			}))
			defer svr.Close()
			httpClient := &http.Client{}
			if tt.serverDelay > 0 {
				httpClient = &http.Client{
					Timeout: time.Duration(tt.serverDelay) * time.Microsecond,
				}
			}
			singleTenantClient := client.SingleTenantJobAPI{
				Client:      httpClient,
				WorkspaceID: tt.workspaceID,
				URLPrefix:   svr.URL,
			}
			job1, err := singleTenantClient.Get(context.Background())
			require.Equal(t, tt.expectedErr, err)
			require.Equal(t, tt.expectedUsrAttributeCount, len(job1.Users), "no of users different than expected")
			t.Log("actual job:", job1)

			multiTenantClient := client.MultiTenantJobAPI{
				Client:      httpClient,
				NamespaceID: tt.workspaceID,
				URLPrefix:   svr.URL,
			}
			job2, err := multiTenantClient.Get(context.Background())
			require.Equal(t, tt.expectedErr, err)
			require.Equal(t, tt.expectedUsrAttributeCount, len(job2.Users), "no of users different than expected")
			t.Log("actual job:", job2)
		})
	}
}

func TestUpdateStatus(t *testing.T) {
	initialize.Init()
	tests := []struct {
		name            string
		workspaceID     string
		namespaceID     string
		status          model.JobStatus
		jobID           int
		expectedReqBody string
		respCode        int
		expectedErr     error
	}{
		{
			name:            "update status request: successful",
			workspaceID:     "1001",
			namespaceID:     "1001",
			status:          model.JobStatusComplete,
			jobID:           1,
			expectedReqBody: `{"status":"complete"}`,
			respCode:        201,
		},
		{
			name:            "update status request: returns error",
			workspaceID:     "1001",
			namespaceID:     "1001",
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
				body, _ = io.ReadAll(r.Body)
			}))
			defer svr.Close()

			singleTenantClient := client.SingleTenantJobAPI{
				Client:      &http.Client{},
				URLPrefix:   svr.URL,
				WorkspaceID: tt.workspaceID,
			}
			err := singleTenantClient.UpdateStatus(context.Background(), tt.status, tt.jobID)
			require.Equal(t, tt.expectedErr, err)
			require.Equal(t, tt.expectedReqBody, string(body), "actual request body different than expected")

			multiTenantClient := client.MultiTenantJobAPI{
				Client:      &http.Client{},
				URLPrefix:   svr.URL,
				NamespaceID: tt.namespaceID,
			}
			err = multiTenantClient.UpdateStatus(context.Background(), tt.status, tt.jobID)
			require.Equal(t, tt.expectedErr, err)
			require.Equal(t, tt.expectedReqBody, string(body), "actual request body different than expected")
		})
	}
}
