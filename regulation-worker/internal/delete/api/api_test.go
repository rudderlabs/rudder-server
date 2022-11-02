package api_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/gorilla/mux"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	mocksBackendConfig "github.com/rudderlabs/rudder-server/mocks/config/backend-config"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/delete/api"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/initialize"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
	oauth "github.com/rudderlabs/rudder-server/router/oauthResponseHandler"

	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"
)

func (d *deleteAPI) handler() http.Handler {
	srvMux := mux.NewRouter()
	srvMux.HandleFunc("/deleteUsers", d.deleteMockServer).Methods("POST")

	return srvMux
}

func TestDelete(t *testing.T) {
	initialize.Init()
	tests := []struct {
		name                 string
		job                  model.Job
		destConfig           map[string]interface{}
		destName             string
		respCode             int
		respBodyStatus       model.JobStatus
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
				Status:        model.JobStatusPending,
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
			respBodyStatus:       "complete",
			expectedDeleteStatus: model.JobStatusComplete,
			expectedPayload:      `[{"jobId":"1","destType":"amplitude","config":{"accessKey":"xyz","accessKeyID":"abc","bucketName":"regulation-test-data","enableSSE":false,"prefix":"reg-original"},"userAttributes":[{"email":"dorowane8n285680461479465450293436@gmail.com","phone":"6463633841","randomKey":"randomValue","userId":"Jermaine1473336609491897794707338"},{"email":"dshirilad8536019424659691213279980@gmail.com","userId":"Mercie8221821544021583104106123"},{"phone":"8782905113","userId":"Claiborn443446989226249191822329"}]}]`,
		},
		{
			name:                 "test deleter API client with expected status failed: error returned 429",
			destName:             "amplitude",
			respCode:             429,
			respBodyStatus:       "failed",
			expectedDeleteStatus: model.JobStatusFailed,
			expectedPayload:      `[{"jobId":"0","destType":"amplitude","config":null,"userAttributes":[]}]`,
		},
		{
			name:                 "test deleter API client with expected status failed-error returned 408",
			destName:             "amplitude",
			respCode:             408,
			respBodyStatus:       "failed",
			expectedDeleteStatus: model.JobStatusFailed,
			expectedPayload:      `[{"jobId":"0","destType":"amplitude","config":null,"userAttributes":[]}]`,
		},
		{
			name:                 "test deleter API client with expected status failed: error returned 504",
			destName:             "amplitude",
			respCode:             504,
			respBodyStatus:       "failed",
			expectedDeleteStatus: model.JobStatusFailed,
			expectedPayload:      `[{"jobId":"0","destType":"amplitude","config":null,"userAttributes":[]}]`,
		},
		{
			name:                 "test deleter API client with expected status failed: error returned 400",
			destName:             "amplitude",
			respCode:             400,
			respBodyStatus:       "failed",
			expectedDeleteStatus: model.JobStatusAborted,
			expectedPayload:      `[{"jobId":"0","destType":"amplitude","config":null,"userAttributes":[]}]`,
		},
		{
			name:                 "test deleter API client with expected status failed: error returned 401",
			destName:             "amplitude",
			respCode:             401,
			respBodyStatus:       "failed",
			expectedDeleteStatus: model.JobStatusAborted,
			expectedPayload:      `[{"jobId":"0","destType":"amplitude","config":null,"userAttributes":[]}]`,
		},
		{
			name:                 "test deleter API client with expected status failed: error returned 405",
			destName:             "amplitude",
			respCode:             405,
			respBodyStatus:       "failed",
			expectedDeleteStatus: model.JobStatusNotSupported,
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
			status := api.Delete(ctx, tt.job, tt.destConfig, tt.destName)
			require.Equal(t, tt.expectedDeleteStatus, status)
			require.Equal(t, tt.expectedPayload, d.payload)
		})
	}
}

type deleteAPI struct {
	payload         string
	respStatusCode  int
	respBodyStatus  model.JobStatus
	respBodyErr     error
	authErrCategory string
}

func (d *deleteAPI) deleteMockServer(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	buf := new(bytes.Buffer)
	_, err := buf.ReadFrom(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
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
	}
}

func TestOAuth(t *testing.T) {
	initialize.Init()

	mockCtrl := gomock.NewController(t)
	mockBackendConfig := mocksBackendConfig.NewMockBackendConfig(mockCtrl)
	mockBackendConfig.EXPECT().AccessToken().AnyTimes()
	backendconfig.Init()

	oauth.Init()

	tests := []struct {
		name                 string
		job                  model.Job
		destConfig           map[string]interface{}
		destName             string
		respCode             int
		respBodyStatus       model.JobStatus
		authErrorCategory    string
		respBodyErr          error
		fetchStCode          int
		fetchResponse        string
		refreshStCode        int
		refreshResponse      string
		expectedDeleteStatus model.JobStatus
		expectedPayload      string
	}{
		{
			name: "test with a valid token and successful response",
			job: model.Job{
				ID:            1,
				WorkspaceID:   "1001",
				DestinationID: "1234",
				Status:        model.JobStatusPending,
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
				"rudderUserDeleteAccountId": "xyz",
			},
			destName:             "GA",
			respCode:             200,
			respBodyStatus:       "complete",
			fetchStCode:          200,
			fetchResponse:        `{"secret": {"access_token": "valid_access_token","refresh_token":"valid_refresh_token"}}`,
			expectedDeleteStatus: model.JobStatusComplete,
			expectedPayload:      `[{"jobId":"1","destType":"ga","config":{"rudderUserDeleteAccountId":"xyz"},"userAttributes":[{"email":"dorowane8n285680461479465450293436@gmail.com","phone":"6463633841","randomKey":"randomValue","userId":"Jermaine1473336609491897794707338"},{"email":"dshirilad8536019424659691213279980@gmail.com","userId":"Mercie8221821544021583104106123"},{"phone":"8782905113","userId":"Claiborn443446989226249191822329"}]}]`,
		},
		{
			name: "test with an expired token and validate if token is getting changed",
			job: model.Job{
				ID:            2,
				WorkspaceID:   "1001",
				DestinationID: "1234",
				Status:        model.JobStatusPending,
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
				},
			},
			destConfig: map[string]interface{}{
				"rudderUserDeleteAccountId": "xyz",
			},
			destName:          "GA",
			respCode:          500,
			respBodyStatus:    "failed",
			authErrorCategory: oauth.REFRESH_TOKEN,

			fetchStCode:   200,
			fetchResponse: `{"secret": {"access_token": "expired_access_token","refresh_token":"valid_refresh_token"}}`,

			refreshStCode:   200,
			refreshResponse: `{"secret": {"access_token": "refreshed_access_token","refresh_token":"valid_refresh_token"}}`,

			expectedDeleteStatus: model.JobStatusFailed,
			expectedPayload:      `[{"jobId":"2","destType":"ga","config":{"rudderUserDeleteAccountId":"xyz"},"userAttributes":[{"email":"dorowane8n285680461479465450293436@gmail.com","phone":"6463633841","randomKey":"randomValue","userId":"Jermaine1473336609491897794707338"},{"email":"dshirilad8536019424659691213279980@gmail.com","userId":"Mercie8221821544021583104106123"}]}]`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			OAuth := oauth.NewOAuthErrorHandler(mockBackendConfig)
			OAuth.Setup()
			d := deleteAPI{
				respStatusCode:  tt.respCode,
				authErrCategory: tt.authErrorCategory,
			}
			ctx := context.Background()
			svr := httptest.NewServer(d.handler())
			cfgBeSrv := httptest.NewServer(mockCpRequests(2*time.Second, tt.fetchStCode, tt.fetchResponse))

			defer svr.Close()
			if tt.refreshStCode == 0 {
				defer cfgBeSrv.Close()
			}

			t.Setenv("DEST_TRANSFORM_URL", svr.URL)
			t.Setenv("CONFIG_BACKEND_URL", cfgBeSrv.URL)
			t.Setenv("CONFIG_BACKEND_TOKEN", "config_backend_token")
			api := api.APIManager{
				Client:           &http.Client{},
				DestTransformURL: svr.URL,
				OAuth:            OAuth,
			}

			status := api.Delete(ctx, tt.job, tt.destConfig, tt.destName)

			// to indicate if refresh token would happen
			if tt.refreshStCode > 0 {
				rr := httptest.NewRecorder()
				if rr.Result().StatusCode > 0 {
					// close the earlier server
					cfgBeSrv.Close()

					cfgBeSrv = httptest.NewServer(mockCpRequests(2*time.Second, tt.refreshStCode, tt.refreshResponse))
					t.Setenv("CONFIG_BACKEND_URL", cfgBeSrv.URL)
					defer cfgBeSrv.Close()
				}
			}
			require.Equal(t, tt.expectedDeleteStatus, status)
			require.Equal(t, tt.expectedPayload, d.payload)
		})
	}
}

func mockCpRequests(timeout time.Duration, code int, response string) *mux.Router {
	srvMux := mux.NewRouter()
	srvMux.HandleFunc("/destination/workspaces/{workspaceId}/accounts/{accountId}/token", func(w http.ResponseWriter, req *http.Request) {
		vars := mux.Vars(req)
		// iterating over request parameters
		for _, reqParam := range []string{"workspaceId", "accountId"} {
			_, ok := vars[reqParam]
			if !ok {
				// This case wouldn't occur I guess
				http.Error(w, fmt.Sprintf("Wrong url being sent: %v", reqParam), http.StatusInternalServerError)
				return
			}
		}

		// sleep is being used to mimic the waiting in actual transformer response
		if timeout > 0 {
			time.Sleep(timeout)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		// Lint error fix
		_, err := w.Write([]byte(response))
		if err != nil {
			fmt.Printf("I'm here!!!! Some shitty response!!")
			http.Error(w, fmt.Sprintf("Provided response is faulty, please check it. Err: %v", err.Error()), http.StatusInternalServerError)
		}
	})
	return srvMux
}
