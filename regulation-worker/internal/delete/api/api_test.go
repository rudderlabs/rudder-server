package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/config"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	mocksBackendConfig "github.com/rudderlabs/rudder-server/mocks/backend-config"

	mock_features "github.com/rudderlabs/rudder-server/mocks/services/transformer"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/delete/api"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
	"github.com/rudderlabs/rudder-server/services/oauth"
	"github.com/rudderlabs/rudder-server/services/transformer"
	testutils "github.com/rudderlabs/rudder-server/utils/tests"
	"github.com/rudderlabs/rudder-server/utils/types/deployment"

	rudderSync "github.com/rudderlabs/rudder-go-kit/sync"
	oauthV2 "github.com/rudderlabs/rudder-server/services/oauth/v2"
	"github.com/rudderlabs/rudder-server/services/oauth/v2/common"
	"github.com/rudderlabs/rudder-server/services/oauth/v2/extensions"
	oauthv2_http "github.com/rudderlabs/rudder-server/services/oauth/v2/http"
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
				Client:                     &http.Client{},
				DestTransformURL:           svr.URL,
				TransformerFeaturesService: transformer.NewNoOpService(),
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

func TestGetSupportedDestinations(t *testing.T) {
	tests := []struct {
		name                 string
		fromFeatures         []string
		expectedDestinations []string
	}{
		{
			name:                 "test get supported destinations when there is no transformer features",
			expectedDestinations: api.SupportedDestinations,
			fromFeatures:         []string{},
		},
		{
			name:                 "test get supported destinations when there is transformer features",
			expectedDestinations: []string{"AM", "GA4"},
			fromFeatures:         []string{"AM", "GA4"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockTransformerFeaturesService := mock_features.NewMockFeaturesService(gomock.NewController(t))
			mockTransformerFeaturesService.EXPECT().Wait().Return(testutils.GetClosedEmptyChannel()).AnyTimes()
			mockTransformerFeaturesService.EXPECT().Regulations().Return(tt.fromFeatures)
			api := api.APIManager{
				TransformerFeaturesService: mockTransformerFeaturesService,
			}
			destinations := api.GetSupportedDestinations()
			require.Equal(t, tt.expectedDestinations, destinations)
		})
	}
}

func TestGetSupportedDestinationsShouldBlockUntilFeaturesAreAvailable(t *testing.T) {
	mockTransformerFeaturesService := mock_features.NewMockFeaturesService(gomock.NewController(t))
	mockTransformerFeaturesService.EXPECT().Wait().Return(make(chan struct{})).AnyTimes()
	mockTransformerFeaturesService.EXPECT().Regulations().Return([]string{"AM"}).AnyTimes()
	api := api.APIManager{
		TransformerFeaturesService: mockTransformerFeaturesService,
	}
	resultsChan := make(chan []string)

	go func() {
		resultsChan <- api.GetSupportedDestinations()
	}()
	select {
	case <-time.After(10 * time.Millisecond):
		close(resultsChan)
	case result := <-resultsChan:
		require.Fail(t, "GetSupportedDestinations should block until features are available, but returned %v", result)
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

type oauthTestCases struct {
	name                         string
	job                          model.Job
	dest                         model.Destination
	cpResponses                  []testutils.CpResponseParams
	deleteResponses              []deleteResponseParams
	oauthHttpClientTimeout       time.Duration
	expectedDeleteStatus         model.JobStatus
	expectedDeleteStatus_OAuthV2 model.JobStatus
	expectedPayload              string
	isOAuthV2Enabled             bool
}

var defaultDestDefConfig = map[string]interface{}{
	"auth": map[string]interface{}{
		"type":         "OAuth",
		"rudderScopes": []interface{}{"delete"},
	},
}

var oauthTests = []oauthTestCases{
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
			Name:          "GA",
			DestDefConfig: defaultDestDefConfig,
		},
		deleteResponses: []deleteResponseParams{
			{
				status:      200,
				jobResponse: `[{"status":"successful"}]`,
			},
		},
		cpResponses: []testutils.CpResponseParams{
			{
				Code:     200,
				Response: `{"secret": {"access_token": "valid_access_token","refresh_token":"valid_refresh_token"}}`,
			},
		},
		expectedDeleteStatus:         model.JobStatus{Status: model.JobStatusComplete},
		expectedDeleteStatus_OAuthV2: model.JobStatus{Status: model.JobStatusComplete},
		expectedPayload:              `[{"jobId":"1","destType":"ga","config":{"rudderDeleteAccountId":"xyz"},"userAttributes":[{"email":"dorowane8n285680461479465450293437@gmail.com","phone":"6463633841","randomKey":"randomValue","userId":"Jermaine1473336609491897794707338"},{"email":"dshirilad853601942465969121327991@gmail.com","userId":"Mercie8221821544021583104106123"},{"phone":"8782905113","userId":"Claiborn443446989226249191822329"}]}]`,
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
			Name:          "GA",
			DestDefConfig: defaultDestDefConfig,
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
		cpResponses: []testutils.CpResponseParams{
			{
				Code:     200,
				Response: `{"secret": {"access_token": "expired_access_token","refresh_token":"valid_refresh_token"}}`,
			},
			{
				Code:     200,
				Response: `{"secret": {"access_token": "refreshed_access_token","refresh_token":"valid_refresh_token"}}`,
			},
		},
		expectedDeleteStatus:         model.JobStatus{Status: model.JobStatusComplete},
		expectedDeleteStatus_OAuthV2: model.JobStatus{Status: model.JobStatusComplete},
		expectedPayload:              `[{"jobId":"2","destType":"ga","config":{"rudderDeleteAccountId":"xyz"},"userAttributes":[{"email":"dorowane8n285680461479465450293438@gmail.com","phone":"6463633841","randomKey":"randomValue","userId":"Jermaine1473336609491897794707338"},{"email":"dshirilad8536019424659691213279982@gmail.com","userId":"Mercie8221821544021583104106123"}]}]`,
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
			Name:          "GA",
			DestDefConfig: defaultDestDefConfig,
		},
		cpResponses: []testutils.CpResponseParams{
			{
				Code:     500,
				Response: `Internal Server Error`,
			},
		},
		deleteResponses:              []deleteResponseParams{{}},
		expectedDeleteStatus:         model.JobStatus{Status: model.JobStatusFailed, Error: fmt.Errorf("[GA][FetchToken] Error in Token Fetch statusCode: 500\t error: Unmarshal of response unsuccessful: Internal Server Error")},
		expectedDeleteStatus_OAuthV2: model.JobStatus{Status: model.JobStatusFailed, Error: fmt.Errorf("failed to parse authErrorCategory from response: error occurred while fetching/refreshing account info from CP: Unmarshal of response unsuccessful: Internal Server Error")},
		expectedPayload:              "", // since request has not gone to transformer at all!
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
			Name:          "GA",
			DestDefConfig: defaultDestDefConfig,
		},
		cpResponses: []testutils.CpResponseParams{
			{
				Code:     500,
				Response: `Internal Server Error`,
				Timeout:  2 * time.Second,
			},
		},
		deleteResponses:              []deleteResponseParams{{}},
		oauthHttpClientTimeout:       1 * time.Second,
		expectedDeleteStatus:         model.JobStatus{Status: model.JobStatusFailed, Error: fmt.Errorf("Client.Timeout exceeded while awaiting headers")},
		expectedDeleteStatus_OAuthV2: model.JobStatus{Status: model.JobStatusFailed, Error: fmt.Errorf("failed to parse authErrorCategory from response: error occurred while fetching/refreshing account info from CP: Post \"__cfgBE_server__/destination/workspaces/1001/accounts/xyz/token\": context deadline exceeded (Client.Timeout exceeded while awaiting headers)")},
		expectedPayload:              "", // since request has not gone to transformer at all!
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
			Name:          "GA",
			DestDefConfig: defaultDestDefConfig,
		},
		cpResponses:                  []testutils.CpResponseParams{},
		deleteResponses:              []deleteResponseParams{{}},
		expectedDeleteStatus:         model.JobStatus{Status: model.JobStatusFailed, Error: fmt.Errorf("[GA] Delete account ID key (rudderDeleteAccountId) is not present for destination: 1234")},
		expectedDeleteStatus_OAuthV2: model.JobStatus{Status: model.JobStatusFailed, Error: fmt.Errorf("failed to parse authErrorCategory from response: accountId is empty for destination(%s) in %s flow", "1234", common.RudderFlowDelete)},
		expectedPayload:              "",
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
			DestDefConfig: defaultDestDefConfig,
		},
		cpResponses:                  []testutils.CpResponseParams{},
		deleteResponses:              []deleteResponseParams{{}},
		expectedDeleteStatus:         model.JobStatus{Status: model.JobStatusFailed, Error: fmt.Errorf("[GA] Delete account ID key (rudderDeleteAccountId) is not present for destination: 1234")},
		expectedDeleteStatus_OAuthV2: model.JobStatus{Status: model.JobStatusFailed, Error: fmt.Errorf("accountId not found for destination(%s) in %s flow", "1234", common.RudderFlowDelete)},
		expectedPayload:              "",
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
			Name:          "GA",
			DestDefConfig: defaultDestDefConfig,
		},

		oauthHttpClientTimeout: 1 * time.Second,
		cpResponses: []testutils.CpResponseParams{
			{
				Code:     200,
				Response: `{"secret": {"access_token": "expired_access_token","refresh_token":"valid_refresh_token"}}`,
			},
			{
				Code:     500,
				Response: `Internal Server Error`,
				Timeout:  2 * time.Second,
			},
		},
		deleteResponses: []deleteResponseParams{
			{
				status:      500,
				jobResponse: `[{"status":"failed","authErrorCategory":"REFRESH_TOKEN","error":"[GA] invalid credentials"}]`,
			},
		},
		expectedDeleteStatus:         model.JobStatus{Status: model.JobStatusFailed, Error: fmt.Errorf("[GA] Failed to refresh token for destination in workspace(1001) & account(xyz) with Unmarshal of response unsuccessful: Post \"__cfgBE_server__/destination/workspaces/1001/accounts/xyz/token\": context deadline exceeded (Client.Timeout exceeded while awaiting headers)")},
		expectedDeleteStatus_OAuthV2: model.JobStatus{Status: model.JobStatusFailed, Error: fmt.Errorf("Post \"__cfgBE_server__/destination/workspaces/1001/accounts/xyz/token\": context deadline exceeded (Client.Timeout exceeded while awaiting headers)")},
		expectedPayload:              `[{"jobId":"9","destType":"ga","config":{"rudderDeleteAccountId":"xyz"},"userAttributes":[{"email":"dorowane9@gmail.com","phone":"6463633841","randomKey":"randomValue","userId":"Jermaine9"},{"email":"dshirilad9@gmail.com","userId":"Mercie9"}]}]`,
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
			Name:          "GA",
			DestDefConfig: defaultDestDefConfig,
		},
		deleteResponses: []deleteResponseParams{
			{
				status:      400,
				jobResponse: fmt.Sprintf(`[{"status":"failed","authErrorCategory": "%v", "error": "User does not have sufficient permissions"}]`, oauth.AUTH_STATUS_INACTIVE),
			},
		},
		cpResponses: []testutils.CpResponseParams{
			// fetch token http request
			{
				Code:     200,
				Response: `{"secret": {"access_token": "invalid_grant_access_token","refresh_token":"invalid_grant_refresh_token"}}`,
			},
			// authStatus inactive http request
			{
				Code: 200,
			},
		},
		expectedDeleteStatus:         model.JobStatus{Status: model.JobStatusAborted, Error: fmt.Errorf("problem with user permission or access/refresh token have been revoked")},
		expectedDeleteStatus_OAuthV2: model.JobStatus{Status: model.JobStatusAborted, Error: fmt.Errorf("[{\"status\":\"failed\",\"authErrorCategory\": \"AUTH_STATUS_INACTIVE\", \"error\": \"User does not have sufficient permissions\"}]")},
		expectedPayload:              `[{"jobId":"15","destType":"ga","config":{"authStatus":"active","rudderDeleteAccountId":"xyz"},"userAttributes":[{"email":"dreymore@gmail.com","phone":"7463633841","userId":"203984798475"}]}]`,
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
			Name:          "GA",
			DestDefConfig: defaultDestDefConfig,
		},
		deleteResponses: []deleteResponseParams{
			{
				status:      400,
				jobResponse: fmt.Sprintf(`[{"status":"failed","authErrorCategory": "%v", "error": "User does not have sufficient permissions"}]`, oauth.AUTH_STATUS_INACTIVE),
			},
		},
		cpResponses: []testutils.CpResponseParams{
			// fetch token http request
			{
				Code:     200,
				Response: `{"secret": {"access_token": "invalid_grant_access_token","refresh_token":"invalid_grant_refresh_token"}}`,
			},
			// authStatus inactive http request
			{
				Code:     400,
				Response: `{"message": "AuthStatus toggle skipped as already request in-progress: (1234, 1001)"}`,
			},
		},
		expectedDeleteStatus:         model.JobStatus{Status: model.JobStatusAborted, Error: errors.New("problem with user permission or access/refresh token have been revoked")},
		expectedDeleteStatus_OAuthV2: model.JobStatus{Status: model.JobStatusAborted, Error: fmt.Errorf(`[{"status":"failed","authErrorCategory": "%v", "error": "User does not have sufficient permissions"}]`, common.CategoryAuthStatusInactive)},
		expectedPayload:              `[{"jobId":"16","destType":"ga","config":{"authStatus":"active","rudderDeleteAccountId":"xyz"},"userAttributes":[{"email":"greymore@gmail.com","phone":"8463633841","userId":"203984798476"}]}]`,
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
			Name:          "GA",
			DestDefConfig: defaultDestDefConfig,
		},
		deleteResponses: []deleteResponseParams{
			{
				status:      500,
				jobResponse: `[{"status":"failed","authErrorCategory":"REFRESH_TOKEN", "error": "[GA] invalid credentials"}]`,
			},
		},

		cpResponses: []testutils.CpResponseParams{
			// fetch token http request
			{
				Code:     200,
				Response: `{"secret": {"access_token": "invalid_grant_access_token","refresh_token":"invalid_grant_refresh_token"}}`,
			},
			// refresh token http request
			{
				Code:     403,
				Response: `{"status":403,"body":{"message":"[google_analytics] \"invalid_grant\" error, refresh token has been revoked","status":403,"code":"ref_token_invalid_grant"},"code":"ref_token_invalid_grant","access_token":"invalid_grant_access_token","refresh_token":"invalid_grant_refresh_token","developer_token":"dev_token"}`,
			},
			// authStatus inactive http request
			{
				Code: 200,
			},
		},

		expectedDeleteStatus:         model.JobStatus{Status: model.JobStatusFailed, Error: fmt.Errorf("[google_analytics] \"invalid_grant\" error, refresh token has been revoked")},
		expectedDeleteStatus_OAuthV2: model.JobStatus{Status: model.JobStatusFailed, Error: fmt.Errorf("[google_analytics] \"invalid_grant\" error, refresh token has been revoked")},
		expectedPayload:              `[{"jobId":"17","destType":"ga","config":{"authStatus":"active","rudderDeleteAccountId":"xyz"},"userAttributes":[{"email":"greymore@gmail.com","phone":"8463633841","userId":"203984798477"}]}]`,
	},
}

type mockIdentifier struct {
	key   string
	token string
}

func (m *mockIdentifier) ID() string                  { return m.key }
func (m *mockIdentifier) BasicAuth() (string, string) { return m.token, "" }
func (*mockIdentifier) Type() deployment.Type         { return "mockType" }

func TestOAuth(t *testing.T) {
	for _, tc := range oauthTests {
		tc.name = fmt.Sprintf("[OAuthV2] %s", tc.name)
		tc.isOAuthV2Enabled = true
		oauthTests = append(oauthTests, tc)
		// oauthTests[i] = tc
	}
	mockCtrl := gomock.NewController(t)
	mockBackendConfig := mocksBackendConfig.NewMockBackendConfig(mockCtrl)

	mockBackendConfig.EXPECT().AccessToken().AnyTimes()
	mockBackendConfig.EXPECT().Identity().AnyTimes().Return(&mockIdentifier{})

	for _, tt := range oauthTests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()

			cpRespProducer := &testutils.CpResponseProducer{
				Responses: tt.cpResponses,
			}
			deleteRespProducer := &deleteResponseProducer{
				responses: tt.deleteResponses,
			}
			cfgBeSrv := httptest.NewServer(cpRespProducer.MockCpRequests())
			svr := httptest.NewServer(deleteRespProducer.mockDeleteRequests())

			defer svr.Close()
			defer cfgBeSrv.Close()

			t.Setenv("DEST_TRANSFORM_URL", svr.URL)
			t.Setenv("CONFIG_BACKEND_URL", cfgBeSrv.URL)
			t.Setenv("CONFIG_BACKEND_TOKEN", "config_backend_token")

			backendconfig.Init()
			cli := &http.Client{
				Transport: &http.Transport{
					IdleConnTimeout: 300 * time.Second,
				},
			}

			oauth.Init()
			OAuth := oauth.NewOAuthErrorHandler(mockBackendConfig, oauth.WithRudderFlow(oauth.RudderFlow_Delete), oauth.WithOAuthClientTimeout(tt.oauthHttpClientTimeout))
			if tt.isOAuthV2Enabled {
				cache := oauthV2.NewCache()
				oauthLock := rudderSync.NewPartitionRWLocker()

				if tt.oauthHttpClientTimeout.Seconds() > 0 {
					config.Set("HttpClient.oauth.timeout", tt.oauthHttpClientTimeout.Seconds())
				}
				optionalArgs := oauthv2_http.HttpClientOptionalArgs{
					Augmenter: extensions.HeaderAugmenter,
					Locker:    oauthLock,
				}
				cli = oauthv2_http.NewOAuthHttpClient(
					cli, common.RudderFlow(oauth.RudderFlow_Delete),
					&cache, mockBackendConfig,
					api.GetAuthErrorCategoryFromResponse, &optionalArgs,
				)
			}

			api := api.APIManager{
				Client:                       cli,
				DestTransformURL:             svr.URL,
				OAuth:                        OAuth,
				MaxOAuthRefreshRetryAttempts: 1,
				IsOAuthV2Enabled:             tt.isOAuthV2Enabled,
			}

			status := api.Delete(ctx, tt.job, tt.dest)
			require.Equal(t, tt.expectedDeleteStatus.Status, status.Status)
			if tt.expectedDeleteStatus.Status != model.JobStatusComplete {
				exp := tt.expectedDeleteStatus.Error.Error()
				if tt.isOAuthV2Enabled {
					exp = tt.expectedDeleteStatus_OAuthV2.Error.Error()
				}
				jobError := strings.Replace(exp, "__cfgBE_server__", cfgBeSrv.URL, 1)

				require.Contains(t, status.Error.Error(), jobError)
			}
			// require.Equal(t, tt.expectedDeleteStatus, status)
			// TODO: Compare input payload for all "/deleteUsers" requests
			require.Equal(t, tt.expectedPayload, deleteRespProducer.GetCurrent().actualPayload)
		})
	}
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
