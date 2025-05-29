package oauth_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/samber/lo"
	"go.uber.org/mock/gomock"

	"github.com/stretchr/testify/require"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	mocksBackendConfig "github.com/rudderlabs/rudder-server/mocks/backend-config"
	"github.com/rudderlabs/rudder-server/services/oauth"
)

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

func TestIsOAuthDestination(t *testing.T) {
	testCases := []struct {
		name     string
		config   map[string]interface{}
		expected bool
	}{
		{
			name:     "should return true if destination is OAuth",
			config:   map[string]interface{}{"auth": map[string]interface{}{"type": "OAuth"}},
			expected: true,
		},
		{
			name:     "should return false if destination is not OAuth",
			config:   map[string]interface{}{},
			expected: false,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, oauth.IsOAuthDestination(tc.config))
		})
	}
}

func TestFetchAccountInfoFromCp(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	mockBackendConfig := mocksBackendConfig.NewMockBackendConfig(mockCtrl)
	mockBackendConfig.EXPECT().AccessToken().AnyTimes()

	testCases := []struct {
		name                 string
		cpStatusCode         int
		cpResponse           string
		expectedStatusCode   int
		expectedErrorType    string
		expectedErrorMessage string
		shouldHaveSecret     bool
	}{
		{
			name:               "successful token fetch",
			cpStatusCode:       200,
			cpResponse:         `{"secret":{"access_token":"test_token","refresh_token":"test_refresh"},"expirationDate":"2024-12-31T23:59:59Z"}`,
			expectedStatusCode: http.StatusOK,
			shouldHaveSecret:   true,
		},
		{
			name:                 "control plane returns 400 (internal service error)",
			cpStatusCode:         400,
			cpResponse:           `{"error":"invalid request"}`,
			expectedStatusCode:   http.StatusInternalServerError,
			expectedErrorType:    "cp_status_400",
			expectedErrorMessage: "Control plane returned status 400: {\"error\":\"invalid request\"}",
		},
		{
			name:                 "control plane returns 401 (internal service error)",
			cpStatusCode:         401,
			cpResponse:           `{"error":"unauthorized"}`,
			expectedStatusCode:   http.StatusInternalServerError,
			expectedErrorType:    "cp_status_401",
			expectedErrorMessage: "Control plane returned status 401: {\"error\":\"unauthorized\"}",
		},
		{
			name:                 "control plane returns 404 (internal service error)",
			cpStatusCode:         404,
			cpResponse:           `{"error":"account not found"}`,
			expectedStatusCode:   http.StatusInternalServerError,
			expectedErrorType:    "cp_status_404",
			expectedErrorMessage: "Control plane returned status 404: {\"error\":\"account not found\"}",
		},
		{
			name:                 "control plane returns 500",
			cpStatusCode:         500,
			cpResponse:           `{"error":"internal server error"}`,
			expectedStatusCode:   http.StatusInternalServerError,
			expectedErrorType:    "cp_status_500",
			expectedErrorMessage: "Control plane returned status 500: {\"error\":\"internal server error\"}",
		},
		{
			name:                 "control plane returns 502",
			cpStatusCode:         502,
			cpResponse:           `{"error":"bad gateway"}`,
			expectedStatusCode:   http.StatusInternalServerError,
			expectedErrorType:    "cp_status_502",
			expectedErrorMessage: "Control plane returned status 502: {\"error\":\"bad gateway\"}",
		},
		{
			name:               "empty response with 200 status",
			cpStatusCode:       200,
			cpResponse:         "",
			expectedStatusCode: http.StatusInternalServerError,
			expectedErrorType:  "Empty secret",
		},
		{
			name:                 "invalid grant error",
			cpStatusCode:         200,
			cpResponse:           `{"body":{"code":"ref_token_invalid_grant","message":"Token has been revoked"}}`,
			expectedStatusCode:   http.StatusBadRequest,
			expectedErrorType:    "ref_token_invalid_grant",
			expectedErrorMessage: "Token has been revoked",
		},
		{
			name:               "unmarshallable response",
			cpStatusCode:       200,
			cpResponse:         `invalid json`,
			expectedStatusCode: http.StatusInternalServerError,
			expectedErrorType:  "unmarshallableResponse",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cpRespProducer := &cpResponseProducer{
				responses: []cpResponseParams{
					{
						code:     tc.cpStatusCode,
						response: tc.cpResponse,
					},
				},
			}
			cfgBeSrv := httptest.NewServer(cpRespProducer.mockCpRequests())
			defer cfgBeSrv.Close()

			t.Setenv("CONFIG_BACKEND_URL", cfgBeSrv.URL)
			t.Setenv("CONFIG_BACKEND_TOKEN", "config_backend_token")

			backendconfig.Init()
			oauth.Init()
			oauthHandler := oauth.NewOAuthErrorHandler(mockBackendConfig)

			refTokenParams := &oauth.RefreshTokenParams{
				AccountId:   "test_account",
				WorkspaceId: "test_workspace",
				DestDefName: "test_dest",
				WorkerId:    1,
			}

			statusCode, authResponse := oauthHandler.FetchToken(refTokenParams)

			require.Equal(t, tc.expectedStatusCode, statusCode)

			if tc.shouldHaveSecret {
				require.NotNil(t, authResponse)
				require.NotEmpty(t, authResponse.Account.Secret)
				require.Empty(t, authResponse.Err)
			} else {
				require.NotNil(t, authResponse)
				if tc.expectedErrorType != "" {
					require.Equal(t, tc.expectedErrorType, authResponse.Err)
				}
				if tc.expectedErrorMessage != "" {
					require.Equal(t, tc.expectedErrorMessage, authResponse.ErrorMessage)
				}
			}
		})
	}
}

func TestMultipleRequestsForOAuth(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	mockBackendConfig := mocksBackendConfig.NewMockBackendConfig(mockCtrl)
	mockBackendConfig.EXPECT().AccessToken().AnyTimes()

	t.Run("multiple authStatusInactive requests", func(t *testing.T) {
		cpRespProducer := &cpResponseProducer{
			responses: []cpResponseParams{
				{
					timeout: 1 * time.Second,
					code:    200,
				},
			},
		}
		cfgBeSrv := httptest.NewServer(cpRespProducer.mockCpRequests())

		defer cfgBeSrv.Close()

		t.Setenv("CONFIG_BACKEND_URL", cfgBeSrv.URL)
		t.Setenv("CONFIG_BACKEND_TOKEN", "config_backend_token")

		backendconfig.Init()
		oauth.Init()
		OAuth := oauth.NewOAuthErrorHandler(mockBackendConfig, oauth.WithRudderFlow(oauth.RudderFlow_Delete))

		totalGoRoutines := 5
		var wg sync.WaitGroup
		var allJobStatus []int
		var allJobStatusMu sync.Mutex

		dest := &backendconfig.DestinationT{
			ID: "dId",
			Config: map[string]interface{}{
				"rudderDeleteAccountId": "accountId",
			},
			DestinationDefinition: backendconfig.DestinationDefinitionT{
				Name: "GA",
				Config: map[string]interface{}{
					"auth": map[string]interface{}{
						"type": "OAuth",
					},
				},
			},
		}

		for i := 0; i < totalGoRoutines; i++ {
			wg.Add(1)
			go func() {
				status, _ := OAuth.AuthStatusToggle(&oauth.AuthStatusToggleParams{
					Destination:     dest,
					WorkspaceId:     "wspId",
					RudderAccountId: "accountId",
					AuthStatus:      oauth.AuthStatusInactive,
				})
				allJobStatusMu.Lock()
				allJobStatus = append(allJobStatus, status)
				allJobStatusMu.Unlock()
				wg.Done()
			}()
		}
		wg.Wait()
		countMap := lo.CountValues(allJobStatus)

		require.Equal(t, countMap[http.StatusConflict], totalGoRoutines-1, countMap)
		require.Equal(t, countMap[http.StatusBadRequest], 1, countMap)
	})
}
