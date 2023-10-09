package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/golang/mock/gomock"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	mocksBackendConfig "github.com/rudderlabs/rudder-server/mocks/backend-config"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/delete/api"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
	"github.com/rudderlabs/rudder-server/services/oauth"

	"github.com/stretchr/testify/require"
)

func (d *deleteAPI) handler() http.Handler {
	srvMux := chi.NewMux()
	srvMux.Post("/deleteUsers", d.deleteMockServer)
	return srvMux
}

func TestDelete(t *testing.T) {
	tests := []struct {
		name                 string
		job                  model.Job
		destConfig           map[string]interface{}
		destName             string
		respCode             int
		respBodyErr          error
		expectedDeleteStatus model.JobStatus
		expectedPayload      string
	}{
		{
			name: "test deleter API client with expected status complete",
			job: model.Job{
				ID:            1,
				WorkspaceID:   "1001",
				DestinationID: "1234",
				Status:        model.JobStatus{Status: model.JobStatusPending},
				Users: []model.User{
					{
						ID: "Jermaine1473336609491897794707338",
						Attributes: map[string]string{
							"phone":     "6463633841",
							"email":     "dorowane8n285680461479465450293436@gmail.com",
							"randomKey": "randomValue",
						},
					},
					{
						ID: "Mercie8221821544021583104106123",
						Attributes: map[string]string{
							"email": "dshirilad8536019424659691213279980@gmail.com",
						},
					},
					{
						ID: "Claiborn443446989226249191822329",
						Attributes: map[string]string{
							"phone": "8782905113",
						},
					},
				},
			},
			destConfig: map[string]interface{}{
				"bucketName":  "regulation-test-data",
				"accessKeyID": "abc",
				"accessKey":   "xyz",
				"enableSSE":   false,
				"prefix":      "reg-original",
			},
			destName:             "amplitude",
			respCode:             200,
			expectedDeleteStatus: model.JobStatus{Status: model.JobStatusComplete},
			expectedPayload:      `[{"jobId":"1","destType":"amplitude","config":{"accessKey":"xyz","accessKeyID":"abc","bucketName":"regulation-test-data","enableSSE":false,"prefix":"reg-original"},"userAttributes":[{"email":"dorowane8n285680461479465450293436@gmail.com","phone":"6463633841","randomKey":"randomValue","userId":"Jermaine1473336609491897794707338"},{"email":"dshirilad8536019424659691213279980@gmail.com","userId":"Mercie8221821544021583104106123"},{"phone":"8782905113","userId":"Claiborn443446989226249191822329"}]}]`,
		},
		{
			name:                 "test deleter API client with expected status failed: error returned 429",
			destName:             "amplitude",
			respCode:             429,
			expectedDeleteStatus: model.JobStatus{Status: model.JobStatusFailed, Error: fmt.Errorf("error: code: 429, body: [{  }]")},
			expectedPayload:      `[{"jobId":"0","destType":"amplitude","config":null,"userAttributes":[]}]`,
		},
		{
			name:                 "test deleter API client with expected status failed-error returned 408",
			destName:             "amplitude",
			respCode:             408,
			expectedDeleteStatus: model.JobStatus{Status: model.JobStatusFailed, Error: fmt.Errorf("error: code: 408, body: [{  }]")},
			expectedPayload:      `[{"jobId":"0","destType":"amplitude","config":null,"userAttributes":[]}]`,
		},
		{
			name:                 "test deleter API client with expected status failed: error returned 504",
			destName:             "amplitude",
			respCode:             504,
			expectedDeleteStatus: model.JobStatus{Status: model.JobStatusFailed, Error: fmt.Errorf("error: code: 504, body: [{  }]")},
			expectedPayload:      `[{"jobId":"0","destType":"amplitude","config":null,"userAttributes":[]}]`,
		},
		{
			name:                 "test deleter API client with expected status failed: error returned 400",
			destName:             "amplitude",
			respCode:             400,
			expectedDeleteStatus: model.JobStatus{Status: model.JobStatusFailed, Error: fmt.Errorf("error: code: 400, body: [{  }]")},
			expectedPayload:      `[{"jobId":"0","destType":"amplitude","config":null,"userAttributes":[]}]`,
		},
		{
			name:                 "test deleter API client with expected status failed: error returned 401",
			destName:             "amplitude",
			respCode:             401,
			expectedDeleteStatus: model.JobStatus{Status: model.JobStatusFailed, Error: fmt.Errorf("error: code: 401, body: [{  }]")},
			expectedPayload:      `[{"jobId":"0","destType":"amplitude","config":null,"userAttributes":[]}]`,
		},
		{
			name:                 "test deleter API client with expected status failed: error returned 405",
			destName:             "amplitude",
			respCode:             405,
			expectedDeleteStatus: model.JobStatus{Status: model.JobStatusAborted, Error: fmt.Errorf("destination not supported by transformer")},
			expectedPayload:      `[{"jobId":"0","destType":"amplitude","config":null,"userAttributes":[]}]`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := deleteAPI{
				respStatusCode: tt.respCode,
			}
			ctx := context.Background()
			svr := httptest.NewServer(d.handler())

			defer svr.Close()
			t.Setenv("DEST_TRANSFORM_URL", svr.URL)
			api := api.APIManager{
				Client:           &http.Client{},
				DestTransformURL: svr.URL,
			}
			dest := model.Destination{
				Config: tt.destConfig,
				Name:   tt.destName,
			}
			status := api.Delete(ctx, tt.job, dest)
			fmt.Println("status", status)
			require.Equal(t, tt.expectedDeleteStatus, status)
			require.Equal(t, tt.expectedPayload, d.payload)
		})
	}
}

type deleteAPI struct {
	payload         string
	respStatusCode  int
	respBodyStatus  model.Status
	respBodyErr     error
	authErrCategory string
}

func (d *deleteAPI) deleteMockServer(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	buf := new(bytes.Buffer)
	_, err := buf.ReadFrom(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	d.payload = buf.String()

	w.WriteHeader(d.respStatusCode)

	var resp api.JobRespSchema
	resp.Status = string(d.respBodyStatus)
	if d.respBodyErr != nil {
		resp.Error = d.respBodyErr.Error()
	}
	if d.authErrCategory != "" {
		resp.AuthErrorCategory = d.authErrCategory
	}

	body, err := json.Marshal([]api.JobRespSchema{resp})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	_, err = w.Write(body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func TestOAuth(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	mockBackendConfig := mocksBackendConfig.NewMockBackendConfig(mockCtrl)
	mockBackendConfig.EXPECT().AccessToken().AnyTimes()

	tests := []struct {
		name                   string
		job                    model.Job
		dest                   model.Destination
		destConfig             map[string]interface{}
		destName               string
		respBodyErr            error
		cpResponses            []cpResponseParams
		deleteResponses        []deleteResponseParams
		oauthHttpClientTimeout time.Duration
		expectedDeleteStatus   model.JobStatus
		expectedPayload        string
	}{
		{
			name: "test with a valid token and successful response",
			job: model.Job{
				ID:            1,
				WorkspaceID:   "1001",
				DestinationID: "1234",
				Status:        model.JobStatus{Status: model.JobStatusPending},
				Users: []model.User{
					{
						ID: "Jermaine1473336609491897794707338",
						Attributes: map[string]string{
							"phone":     "6463633841",
							"email":     "dorowane8n285680461479465450293437@gmail.com",
							"randomKey": "randomValue",
						},
					},
					{
						ID: "Mercie8221821544021583104106123",
						Attributes: map[string]string{
							"email": "dshirilad853601942465969121327991@gmail.com",
						},
					},
					{
						ID: "Claiborn443446989226249191822329",
						Attributes: map[string]string{
							"phone": "8782905113",
						},
					},
				},
			},
			dest: model.Destination{
				DestinationID: "1234",
				Config: map[string]interface{}{
					"rudderDeleteAccountId": "xyz",
				},
				Name: "GA",
				DestDefConfig: map[string]interface{}{
					"auth": map[string]interface{}{
						"type": "OAuth",
					},
				},
			},
			deleteResponses: []deleteResponseParams{
				{
					status:      200,
					jobResponse: `[{"status":"successful"}]`,
				},
			},
			cpResponses: []cpResponseParams{
				{
					code:     200,
					response: `{"secret": {"access_token": "valid_access_token","refresh_token":"valid_refresh_token"}}`,
				},
			},
			expectedDeleteStatus: model.JobStatus{Status: model.JobStatusComplete},
			expectedPayload:      `[{"jobId":"1","destType":"ga","config":{"rudderDeleteAccountId":"xyz"},"userAttributes":[{"email":"dorowane8n285680461479465450293437@gmail.com","phone":"6463633841","randomKey":"randomValue","userId":"Jermaine1473336609491897794707338"},{"email":"dshirilad853601942465969121327991@gmail.com","userId":"Mercie8221821544021583104106123"},{"phone":"8782905113","userId":"Claiborn443446989226249191822329"}]}]`,
		},
		{
			name: "when 1st time fails with expired token after refresh, immediate retry of job should pass the job",
			job: model.Job{
				ID:            2,
				WorkspaceID:   "1001",
				DestinationID: "1234",
				Status:        model.JobStatus{Status: model.JobStatusPending},
				Users: []model.User{
					{
						ID: "Jermaine1473336609491897794707338",
						Attributes: map[string]string{
							"phone":     "6463633841",
							"email":     "dorowane8n285680461479465450293438@gmail.com",
							"randomKey": "randomValue",
						},
					},
					{
						ID: "Mercie8221821544021583104106123",
						Attributes: map[string]string{
							"email": "dshirilad8536019424659691213279982@gmail.com",
						},
					},
				},
			},
			dest: model.Destination{
				DestinationID: "1234",
				Config: map[string]interface{}{
					"rudderDeleteAccountId": "xyz",
				},
				Name: "GA",
				DestDefConfig: map[string]interface{}{
					"auth": map[string]interface{}{
						"type": "OAuth",
					},
				},
			},
			deleteResponses: []deleteResponseParams{
				{
					status:      500,
					jobResponse: `[{"status":"failed","authErrorCategory":"REFRESH_TOKEN", "error": "[GA] invalid credentials"}]`,
				},
				{
					status:      200,
					jobResponse: `[{"status":"successful"}]`,
				},
			},
			cpResponses: []cpResponseParams{
				{
					code:     200,
					response: `{"secret": {"access_token": "expired_access_token","refresh_token":"valid_refresh_token"}}`,
				},
				{
					code:     200,
					response: `{"secret": {"access_token": "refreshed_access_token","refresh_token":"valid_refresh_token"}}`,
				},
			},
			expectedDeleteStatus: model.JobStatus{Status: model.JobStatusComplete},
			expectedPayload:      `[{"jobId":"2","destType":"ga","config":{"rudderDeleteAccountId":"xyz"},"userAttributes":[{"email":"dorowane8n285680461479465450293438@gmail.com","phone":"6463633841","randomKey":"randomValue","userId":"Jermaine1473336609491897794707338"},{"email":"dshirilad8536019424659691213279982@gmail.com","userId":"Mercie8221821544021583104106123"}]}]`,
		},
		{
			name: "test when fetch token fails(with 500) to respond properly fail the job",
			job: model.Job{
				ID:            3,
				WorkspaceID:   "1001",
				DestinationID: "1234",
				Status:        model.JobStatus{Status: model.JobStatusPending},
				Users: []model.User{
					{
						ID: "Jermaine1473336609491897794707338",
						Attributes: map[string]string{
							"phone":     "6463633841",
							"email":     "dorowane8n285680461479465450293448@gmail.com",
							"randomKey": "randomValue",
						},
					},
					{
						ID: "Mercie8221821544021583104106123",
						Attributes: map[string]string{
							"email": "dshirilad8536019424659691213279983@gmail.com",
						},
					},
				},
			},
			dest: model.Destination{
				DestinationID: "1234",
				Config: map[string]interface{}{
					"rudderDeleteAccountId": "xyz",
				},
				Name: "GA",
				DestDefConfig: map[string]interface{}{
					"auth": map[string]interface{}{
						"type": "OAuth",
					},
				},
			},
			cpResponses: []cpResponseParams{
				{
					code:     500,
					response: `Internal Server Error`,
				},
			},
			deleteResponses:      []deleteResponseParams{{}},
			expectedDeleteStatus: model.JobStatus{Status: model.JobStatusFailed, Error: fmt.Errorf("[GA][FetchToken] Error in Token Fetch statusCode: 500\t error: Unmarshal of response unsuccessful: Internal Server Error")},
			expectedPayload:      "", // since request has not gone to transformer at all!
		},
		{
			name: "test when fetch token request times out fail the job",
			job: model.Job{
				ID:            3,
				WorkspaceID:   "1001",
				DestinationID: "1234",
				Status:        model.JobStatus{Status: model.JobStatusPending},
				Users: []model.User{
					{
						ID: "Jermaine1473336609491897794707338",
						Attributes: map[string]string{
							"phone":     "6463633841",
							"email":     "dorowane8n285680461479465450293448@gmail.com",
							"randomKey": "randomValue",
						},
					},
					{
						ID: "Mercie8221821544021583104106123",
						Attributes: map[string]string{
							"email": "dshirilad8536019424659691213279983@gmail.com",
						},
					},
				},
			},
			dest: model.Destination{
				DestinationID: "1234",
				Config: map[string]interface{}{
					"rudderDeleteAccountId": "xyz",
				},
				Name: "GA",
				DestDefConfig: map[string]interface{}{
					"auth": map[string]interface{}{
						"type": "OAuth",
					},
				},
			},
			cpResponses: []cpResponseParams{
				{
					code:     500,
					response: `Internal Server Error`,
					timeout:  2 * time.Second,
				},
			},
			deleteResponses:        []deleteResponseParams{{}},
			oauthHttpClientTimeout: 1 * time.Second,
			expectedDeleteStatus:   model.JobStatus{Status: model.JobStatusFailed, Error: fmt.Errorf("Client.Timeout exceeded while awaiting headers")},
			expectedPayload:        "", // since request has not gone to transformer at all!
		},
		{
			// In this case the request will not even reach transformer, as OAuth is required but we don't have "rudderDeleteAccountId"
			name: "when rudderDeleteAccountId is present but is empty string in destination config fail the job",
			job: model.Job{
				ID:            1,
				WorkspaceID:   "1001",
				DestinationID: "1234",
				Status:        model.JobStatus{Status: model.JobStatusPending},
				Users: []model.User{
					{
						ID: "Jermaine1473336609491897794707338",
						Attributes: map[string]string{
							"phone":     "6463633841",
							"email":     "dorowane8n285680461479465450293437@gmail.com",
							"randomKey": "randomValue",
						},
					},
					{
						ID: "Mercie8221821544021583104106123",
						Attributes: map[string]string{
							"email": "dshirilad853601942465969121327991@gmail.com",
						},
					},
					{
						ID: "Claiborn443446989226249191822329",
						Attributes: map[string]string{
							"phone": "8782905113",
						},
					},
				},
			},
			dest: model.Destination{
				DestinationID: "1234",
				Config: map[string]interface{}{
					"rudderDeleteAccountId": "",
				},
				Name: "GA",
				DestDefConfig: map[string]interface{}{
					"auth": map[string]interface{}{
						"type": "OAuth",
					},
				},
			},
			cpResponses:          []cpResponseParams{},
			deleteResponses:      []deleteResponseParams{{}},
			expectedDeleteStatus: model.JobStatus{Status: model.JobStatusFailed, Error: fmt.Errorf("[GA] Delete account ID key (rudderDeleteAccountId) is not present for destination: 1234")},
			expectedPayload:      "",
		},
		{
			// In this case the request will not even reach transformer, as OAuth is required but we don't have "rudderDeleteAccountId"
			name: "when rudderDeleteAccountId field is not present in destination config fail the job",
			job: model.Job{
				ID:            1,
				WorkspaceID:   "1001",
				DestinationID: "1234",
				Status:        model.JobStatus{Status: model.JobStatusPending},
				Users: []model.User{
					{
						ID: "Jermaine1473336609491897794707338",
						Attributes: map[string]string{
							"phone":     "6463633841",
							"email":     "dorowane8n285680461479465450293437@gmail.com",
							"randomKey": "randomValue",
						},
					},
					{
						ID: "Mercie8221821544021583104106123",
						Attributes: map[string]string{
							"email": "dshirilad853601942465969121327991@gmail.com",
						},
					},
					{
						ID: "Claiborn443446989226249191822329",
						Attributes: map[string]string{
							"phone": "8782905113",
						},
					},
				},
			},
			dest: model.Destination{
				DestinationID: "1234",
				Config:        map[string]interface{}{},
				Name:          "GA",
				DestDefConfig: map[string]interface{}{
					"auth": map[string]interface{}{
						"type": "OAuth",
					},
				},
			},
			cpResponses:          []cpResponseParams{},
			deleteResponses:      []deleteResponseParams{{}},
			expectedDeleteStatus: model.JobStatus{Status: model.JobStatusFailed, Error: fmt.Errorf("[GA] Delete account ID key (rudderDeleteAccountId) is not present for destination: 1234")},
			expectedPayload:      "",
		},
		{
			name: "test when refresh token request times out, retry once and pass if cfg-be server is up",
			job: model.Job{
				ID:            9,
				WorkspaceID:   "1001",
				DestinationID: "1234",
				Status:        model.JobStatus{Status: model.JobStatusPending},
				Users: []model.User{
					{
						ID: "Jermaine9",
						Attributes: map[string]string{
							"phone":     "6463633841",
							"email":     "dorowane9@gmail.com",
							"randomKey": "randomValue",
						},
					},
					{
						ID: "Mercie9",
						Attributes: map[string]string{
							"email": "dshirilad9@gmail.com",
						},
					},
				},
			},
			dest: model.Destination{
				DestinationID: "1234",
				Config: map[string]interface{}{
					"rudderDeleteAccountId": "xyz",
				},
				Name: "GA",
				DestDefConfig: map[string]interface{}{
					"auth": map[string]interface{}{
						"type": "OAuth",
					},
				},
			},

			oauthHttpClientTimeout: 1 * time.Second,
			cpResponses: []cpResponseParams{
				{
					code:     200,
					response: `{"secret": {"access_token": "expired_access_token","refresh_token":"valid_refresh_token"}}`,
				},
				{
					code:     500,
					response: `Internal Server Error`,
					timeout:  2 * time.Second,
				},
			},
			deleteResponses: []deleteResponseParams{
				{
					status:      500,
					jobResponse: `[{"status":"failed","authErrorCategory":"REFRESH_TOKEN","error":"[GA] invalid credentials"}]`,
				},
			},
			expectedDeleteStatus: model.JobStatus{Status: model.JobStatusFailed, Error: fmt.Errorf("[GA] Failed to refresh token for destination in workspace(1001) & account(xyz) with Unmarshal of response unsuccessful: Post \"__cfgBE_server__/destination/workspaces/1001/accounts/xyz/token\": context deadline exceeded (Client.Timeout exceeded while awaiting headers)")},
			expectedPayload:      `[{"jobId":"9","destType":"ga","config":{"rudderDeleteAccountId":"xyz"},"userAttributes":[{"email":"dorowane9@gmail.com","phone":"6463633841","randomKey":"randomValue","userId":"Jermaine9"},{"email":"dshirilad9@gmail.com","userId":"Mercie9"}]}]`,
		},

		{
			name: "when AUTH_STATUS_INACTIVE error happens & authStatus/toggle success, fail the job with Failed status",
			job: model.Job{
				ID:            15,
				WorkspaceID:   "1001",
				DestinationID: "1234",
				Status:        model.JobStatus{Status: model.JobStatusPending},
				Users: []model.User{
					{
						ID: "203984798475",
						Attributes: map[string]string{
							"phone": "7463633841",
							"email": "dreymore@gmail.com",
						},
					},
				},
			},
			dest: model.Destination{
				DestinationID: "1234",
				Config: map[string]interface{}{
					"rudderDeleteAccountId": "xyz",
					"authStatus":            "active",
				},
				Name: "GA",
				DestDefConfig: map[string]interface{}{
					"auth": map[string]interface{}{
						"type": "OAuth",
					},
				},
			},
			deleteResponses: []deleteResponseParams{
				{
					status:      400,
					jobResponse: fmt.Sprintf(`[{"status":"failed","authErrorCategory": "%v", "error": "User does not have sufficient permissions"}]`, oauth.AUTH_STATUS_INACTIVE),
				},
			},
			cpResponses: []cpResponseParams{
				// fetch token http request
				{
					code:     200,
					response: `{"secret": {"access_token": "invalid_grant_access_token","refresh_token":"invalid_grant_refresh_token"}}`,
				},
				// authStatus inactive http request
				{
					code: 200,
				},
			},
			expectedDeleteStatus: model.JobStatus{Status: model.JobStatusAborted, Error: fmt.Errorf("Problem with user permission or access/refresh token have been revoked")},
			expectedPayload:      `[{"jobId":"15","destType":"ga","config":{"authStatus":"active","rudderDeleteAccountId":"xyz"},"userAttributes":[{"email":"dreymore@gmail.com","phone":"7463633841","userId":"203984798475"}]}]`,
		},
		{
			name: "when AUTH_STATUS_INACTIVE error happens but authStatus/toggle failed, fail the job with Failed status",
			job: model.Job{
				ID:            16,
				WorkspaceID:   "1001",
				DestinationID: "1234",
				Status:        model.JobStatus{Status: model.JobStatusPending},
				Users: []model.User{
					{
						ID: "203984798476",
						Attributes: map[string]string{
							"phone": "8463633841",
							"email": "greymore@gmail.com",
						},
					},
				},
			},
			dest: model.Destination{
				DestinationID: "1234",
				Config: map[string]interface{}{
					"rudderDeleteAccountId": "xyz",
					"authStatus":            "active",
				},
				Name: "GA",
				DestDefConfig: map[string]interface{}{
					"auth": map[string]interface{}{
						"type": "OAuth",
					},
				},
			},
			deleteResponses: []deleteResponseParams{
				{
					status:      400,
					jobResponse: fmt.Sprintf(`[{"status":"failed","authErrorCategory": "%v", "error": "User does not have sufficient permissions"}]`, oauth.AUTH_STATUS_INACTIVE),
				},
			},
			cpResponses: []cpResponseParams{
				// fetch token http request
				{
					code:     200,
					response: `{"secret": {"access_token": "invalid_grant_access_token","refresh_token":"invalid_grant_refresh_token"}}`,
				},
				// authStatus inactive http request
				{
					code:     400,
					response: `{"message": "AuthStatus toggle skipped as already request in-progress: (1234, 1001)"}`,
				},
			},
			expectedDeleteStatus: model.JobStatus{Status: model.JobStatusAborted, Error: fmt.Errorf("Problem with user permission or access/refresh token have been revoked")},
			expectedPayload:      `[{"jobId":"16","destType":"ga","config":{"authStatus":"active","rudderDeleteAccountId":"xyz"},"userAttributes":[{"email":"greymore@gmail.com","phone":"8463633841","userId":"203984798476"}]}]`,
		},

		{
			name: "when REFRESH_TOKEN error happens but refreshing token fails due to token revocation, fail the job with Failed status",
			job: model.Job{
				ID:            17,
				WorkspaceID:   "1001",
				DestinationID: "1234",
				Status:        model.JobStatus{Status: model.JobStatusPending},
				Users: []model.User{
					{
						ID: "203984798477",
						Attributes: map[string]string{
							"phone": "8463633841",
							"email": "greymore@gmail.com",
						},
					},
				},
			},
			dest: model.Destination{
				DestinationID: "1234",
				Config: map[string]interface{}{
					"rudderDeleteAccountId": "xyz",
					"authStatus":            "active",
				},
				Name: "GA",
				DestDefConfig: map[string]interface{}{
					"auth": map[string]interface{}{
						"type": "OAuth",
					},
				},
			},
			deleteResponses: []deleteResponseParams{
				{
					status:      500,
					jobResponse: `[{"status":"failed","authErrorCategory":"REFRESH_TOKEN", "error": "[GA] invalid credentials"}]`,
				},
			},

			cpResponses: []cpResponseParams{
				// fetch token http request
				{
					code:     200,
					response: `{"secret": {"access_token": "invalid_grant_access_token","refresh_token":"invalid_grant_refresh_token"}}`,
				},
				// refresh token http request
				{
					code:     403,
					response: `{"status":403,"body":{"message":"[google_analytics] \"invalid_grant\" error, refresh token has been revoked","status":403,"code":"ref_token_invalid_grant"},"code":"ref_token_invalid_grant","access_token":"invalid_grant_access_token","refresh_token":"invalid_grant_refresh_token","developer_token":"dev_token"}`,
				},
				// authStatus inactive http request
				{
					code: 200,
				},
			},

			expectedDeleteStatus: model.JobStatus{Status: model.JobStatusFailed, Error: fmt.Errorf("Problem with user permission or access/refresh token have been revoked")},
			expectedPayload:      `[{"jobId":"17","destType":"ga","config":{"authStatus":"active","rudderDeleteAccountId":"xyz"},"userAttributes":[{"email":"greymore@gmail.com","phone":"8463633841","userId":"203984798477"}]}]`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()

			cpRespProducer := &cpResponseProducer{
				responses: tt.cpResponses,
			}
			deleteRespProducer := &deleteResponseProducer{
				responses: tt.deleteResponses,
			}
			cfgBeSrv := httptest.NewServer(cpRespProducer.mockCpRequests())
			svr := httptest.NewServer(deleteRespProducer.mockDeleteRequests())

			defer svr.Close()
			defer cfgBeSrv.Close()

			t.Setenv("DEST_TRANSFORM_URL", svr.URL)
			t.Setenv("CONFIG_BACKEND_URL", cfgBeSrv.URL)
			t.Setenv("CONFIG_BACKEND_TOKEN", "config_backend_token")

			backendconfig.Init()
			oauth.Init()
			OAuth := oauth.NewOAuthErrorHandler(mockBackendConfig, oauth.WithRudderFlow(oauth.RudderFlow_Delete), oauth.WithOAuthClientTimeout(tt.oauthHttpClientTimeout))
			api := api.APIManager{
				Client:                       &http.Client{},
				DestTransformURL:             svr.URL,
				OAuth:                        OAuth,
				MaxOAuthRefreshRetryAttempts: 1,
			}

			status := api.Delete(ctx, tt.job, tt.dest)
			require.Equal(t, tt.expectedDeleteStatus.Status, status.Status)
			if tt.expectedDeleteStatus.Status != model.JobStatusComplete {
				jobError := strings.Replace(tt.expectedDeleteStatus.Error.Error(), "__cfgBE_server__", cfgBeSrv.URL, 1)
				require.Contains(t, status.Error.Error(), jobError)
			}
			// require.Equal(t, tt.expectedDeleteStatus, status)
			// TODO: Compare input payload for all "/deleteUsers" requests
			require.Equal(t, tt.expectedPayload, deleteRespProducer.GetCurrent().actualPayload)
		})
	}
}

type cpResponseParams struct {
	timeout  time.Duration
	code     int
	response string
}
type cpResponseProducer struct {
	responses []cpResponseParams
	callCount int
}

func (s *cpResponseProducer) GetNext() cpResponseParams {
	if s.callCount >= len(s.responses) {
		panic("ran out of responses")
	}
	cpResp := s.responses[s.callCount]
	s.callCount++
	return cpResp
}

func (cpRespProducer *cpResponseProducer) mockCpRequests() *chi.Mux {
	srvMux := chi.NewMux()
	srvMux.HandleFunc("/destination/workspaces/{workspaceId}/accounts/{accountId}/token", func(w http.ResponseWriter, req *http.Request) {
		// iterating over request parameters
		for _, reqParam := range []string{"workspaceId", "accountId"} {
			param := chi.URLParam(req, reqParam)
			if param == "" {
				// This case wouldn't occur I guess
				http.Error(w, fmt.Sprintf("Wrong url being sent: %v", reqParam), http.StatusBadRequest)
				return
			}
		}

		cpResp := cpRespProducer.GetNext()
		// sleep is being used to mimic the waiting in actual transformer response
		if cpResp.timeout > 0 {
			time.Sleep(cpResp.timeout)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(cpResp.code)
		// Lint error fix
		_, err := w.Write([]byte(cpResp.response))
		if err != nil {
			http.Error(w, fmt.Sprintf("Provided response is faulty, please check it. Err: %v", err.Error()), http.StatusInternalServerError)
			return
		}
	})

	srvMux.HandleFunc("/workspaces/{workspaceId}/destinations/{destinationId}/authStatus/toggle", func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPut {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		// iterating over request parameters
		for _, reqParam := range []string{"workspaceId", "destinationId"} {
			param := chi.URLParam(req, reqParam)
			if param == "" {
				// This case wouldn't occur I guess
				http.Error(w, fmt.Sprintf("Wrong url being sent: %v", reqParam), http.StatusNotFound)
				return
			}
		}

		cpResp := cpRespProducer.GetNext()
		// sleep is being used to mimic the waiting in actual transformer response
		if cpResp.timeout > 0 {
			time.Sleep(cpResp.timeout)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(cpResp.code)
		// Lint error fix
		_, err := w.Write([]byte(cpResp.response))
		if err != nil {
			http.Error(w, fmt.Sprintf("Provided response is faulty, please check it. Err: %v", err.Error()), http.StatusInternalServerError)
			return
		}
	})
	return srvMux
}

// This part is to support multiple responses from deleteMockServer as we have retry mechanism embedded for OAuth
type deleteResponseParams struct {
	status        int
	timeout       time.Duration
	jobResponse   string // should be in structure of []api.JobRespSchema
	actualPayload string
}
type deleteResponseProducer struct {
	responses []deleteResponseParams
	callCount int
}

func (s *deleteResponseProducer) GetCurrent() *deleteResponseParams {
	if s.callCount == 0 {
		return &s.responses[s.callCount]
	}
	return &s.responses[s.callCount-1]
}

func (s *deleteResponseProducer) GetNext() *deleteResponseParams {
	if s.callCount >= len(s.responses) {
		panic("ran out of responses")
	}
	deleteResp := &s.responses[s.callCount]
	s.callCount++
	return deleteResp
}

func (delRespProducer *deleteResponseProducer) mockDeleteRequests() *chi.Mux {
	srvMux := chi.NewRouter()

	srvMux.Post("/deleteUsers", func(w http.ResponseWriter, req *http.Request) {
		buf := new(bytes.Buffer)
		_, bufErr := buf.ReadFrom(req.Body)
		if bufErr != nil {
			http.Error(w, bufErr.Error(), http.StatusBadRequest)
			return
		}
		delResp := delRespProducer.GetNext()

		// useful in validating the payload(sent in request body to transformer)
		delRespProducer.GetCurrent().actualPayload = buf.String()
		// sleep is being used to mimic the waiting in actual transformer response
		if delResp.timeout > 0 {
			time.Sleep(delResp.timeout)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(delResp.status)
		// Lint error fix
		_, err := w.Write([]byte(delResp.jobResponse))
		if err != nil {
			http.Error(w, fmt.Sprintf("Provided response is faulty, please check it. Err: %v", err.Error()), http.StatusInternalServerError)
			return
		}
	})

	return srvMux
}
