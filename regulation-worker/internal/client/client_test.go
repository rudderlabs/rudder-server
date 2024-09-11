package client_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/regulation-worker/internal/client"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
	"github.com/rudderlabs/rudder-server/services/controlplane/identity"
	"github.com/rudderlabs/rudder-server/utils/types/deployment"
)

func TestGet(t *testing.T) {
	tests := []struct {
		name                      string
		ID                        string
		respBody                  string
		respCode                  int
		expectedErr               error
		acutalErr                 error
		expectedUsrAttributeCount int
		serverDelay               int
		expectedPath              string
		mode                      deployment.Type
	}{
		{
			name:                      "DEDICATED MODE: Get request to get job: successful",
			ID:                        "1001",
			respBody:                  `{"jobId":"1","workspaceId":"abc","destinationId":"23","userAttributes":[{"userId":"1","phone":"555-555-5555"},{"userId":"2","email":"john@example.com"},{"userId":"3","randomKey":"randomValue"}]}`,
			respCode:                  200,
			expectedUsrAttributeCount: 3,
			mode:                      deployment.DedicatedType,
			expectedPath:              "/dataplane/workspaces/1001/regulations/workerJobs",
		},
		{
			name:         "DEDICATED MODE: Get request to get job: NoRunnableJob found",
			ID:           "1001",
			respCode:     204,
			expectedErr:  model.ErrNoRunnableJob,
			mode:         deployment.DedicatedType,
			expectedPath: "/dataplane/workspaces/1001/regulations/workerJobs",
		},
		{
			name:         "DEDICATED MODE: Get request to get job: random error",
			ID:           "1001",
			respCode:     429,
			expectedErr:  fmt.Errorf("unexpected response code: 429"),
			mode:         deployment.DedicatedType,
			expectedPath: "/dataplane/workspaces/1001/regulations/workerJobs",
		},
		{
			name:         "DEDICATED MODE: Get request to get model.ErrRequestTimeout",
			ID:           "1001",
			expectedErr:  model.ErrRequestTimeout,
			serverDelay:  1,
			mode:         deployment.DedicatedType,
			expectedPath: "/dataplane/workspaces/1001/regulations/workerJobs",
		},
		{
			name:                      "MULTITENANT MODE: Get request to get job: successful",
			ID:                        "1001",
			respBody:                  `{"jobId":"1","workspaceId":"abc","destinationId":"23","userAttributes":[{"userId":"1","phone":"555-555-5555"},{"userId":"2","email":"john@example.com"},{"userId":"3","randomKey":"randomValue"}]}`,
			respCode:                  200,
			expectedUsrAttributeCount: 3,
			mode:                      deployment.MultiTenantType,
			expectedPath:              "/dataplane/namespaces/1001/regulations/workerJobs",
		},
		{
			name:         "MULTITENANT MODE: Get request to get job: NoRunnableJob found",
			ID:           "1001",
			respCode:     204,
			expectedErr:  model.ErrNoRunnableJob,
			mode:         deployment.MultiTenantType,
			expectedPath: "/dataplane/namespaces/1001/regulations/workerJobs",
		},
		{
			name:         "MULTITENANT MODE: Get request to get job: random error",
			ID:           "1001",
			respCode:     429,
			expectedErr:  fmt.Errorf("unexpected response code: 429"),
			mode:         deployment.MultiTenantType,
			expectedPath: "/dataplane/namespaces/1001/regulations/workerJobs",
		},
		{
			name:         "MULTITENANT MODE: Get request to get model.ErrRequestTimeout",
			ID:           "1001",
			expectedErr:  model.ErrRequestTimeout,
			serverDelay:  1,
			mode:         deployment.MultiTenantType,
			expectedPath: "/dataplane/namespaces/1001/regulations/workerJobs",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tt.respCode != 0 {
					w.WriteHeader(tt.respCode)
				}
				path := r.URL.Path
				require.Equal(t, tt.expectedPath, path)
				time.Sleep(time.Duration(tt.serverDelay) * time.Millisecond)
				_, _ = w.Write([]byte(tt.respBody))
			}))
			defer svr.Close()
			httpClient := &http.Client{}
			if tt.serverDelay > 0 {
				httpClient = &http.Client{
					Timeout: time.Duration(tt.serverDelay) * time.Microsecond,
				}
			}
			var identifier identity.Identifier = &identity.Workspace{WorkspaceID: "1001"}
			if tt.mode == deployment.MultiTenantType {
				identifier = &identity.Namespace{Namespace: "1001"}
			}
			c := client.JobAPI{
				Client:    httpClient,
				Identity:  identifier,
				URLPrefix: svr.URL,
			}
			job, err := c.Get(context.Background())
			require.Equal(t, tt.expectedErr, err)
			require.Equal(t, tt.expectedUsrAttributeCount, len(job.Users), "no of users different than expected")
			t.Log("actual job:", job)
		})
	}
}

func TestUpdateStatus(t *testing.T) {
	tests := []struct {
		name            string
		workspaceID     string
		status          model.JobStatus
		jobID           int
		expectedReqBody string
		respCode        int
		expectedErr     error
		mode            deployment.Type
		expectedPath    string
	}{
		{
			name:            "DEDICATED MODE: update status request: successful",
			workspaceID:     "1001",
			status:          model.JobStatus{Status: model.JobStatusComplete},
			jobID:           1,
			expectedReqBody: `{"status":"complete","reason":""}`,
			respCode:        201,
			mode:            deployment.DedicatedType,
			expectedPath:    "/dataplane/workspaces/1001/regulations/workerJobs/1",
		},
		{
			name:            "DEDICATED MODE: update status request: returns error",
			workspaceID:     "1001",
			status:          model.JobStatus{Status: model.JobStatusComplete},
			jobID:           1,
			expectedReqBody: `{"status":"complete","reason":""}`,
			respCode:        429,
			expectedErr:     fmt.Errorf("update status failed with status code: 429"),
			mode:            deployment.DedicatedType,
			expectedPath:    "/dataplane/workspaces/1001/regulations/workerJobs/1",
		},
		{
			name:            "MULTITENANT MODE: update status request: successful",
			workspaceID:     "1001",
			status:          model.JobStatus{Status: model.JobStatusComplete},
			jobID:           1,
			expectedReqBody: `{"status":"complete","reason":""}`,
			respCode:        201,
			mode:            deployment.MultiTenantType,
			expectedPath:    "/dataplane/namespaces/1001/regulations/workerJobs/1",
		},
		{
			name:            "MULTITENANT MODE: update status request: returns error",
			workspaceID:     "1001",
			status:          model.JobStatus{Status: model.JobStatusComplete},
			jobID:           1,
			expectedReqBody: `{"status":"complete","reason":""}`,
			respCode:        429,
			expectedErr:     fmt.Errorf("update status failed with status code: 429"),
			mode:            deployment.MultiTenantType,
			expectedPath:    "/dataplane/namespaces/1001/regulations/workerJobs/1",
		},
		{
			name:            "MULTITENANT MODE: update status request: returns error",
			workspaceID:     "1001",
			status:          model.JobStatus{Status: model.JobStatusFailed, Error: fmt.Errorf("some reason")},
			jobID:           1,
			expectedReqBody: `{"status":"failed","reason":"some reason"}`,
			respCode:        429,
			expectedErr:     fmt.Errorf("update status failed with status code: 429"),
			mode:            deployment.MultiTenantType,
			expectedPath:    "/dataplane/namespaces/1001/regulations/workerJobs/1",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var body []byte
			svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tt.respCode)
				body, _ = io.ReadAll(r.Body)
				path := r.URL.Path
				require.Equal(t, tt.expectedPath, path)
			}))
			defer svr.Close()
			var identifier identity.Identifier = &identity.Workspace{WorkspaceID: "1001"}
			if tt.mode == deployment.MultiTenantType {
				identifier = &identity.Namespace{Namespace: "1001"}
			}
			c := client.JobAPI{
				Client:    &http.Client{},
				URLPrefix: svr.URL,
				Identity:  identifier,
			}
			err := c.UpdateStatus(context.Background(), tt.status, tt.jobID)
			require.Equal(t, tt.expectedErr, err)
			require.Equal(t, tt.expectedReqBody, string(body), "actual request body different than expected")
		})
	}
}
