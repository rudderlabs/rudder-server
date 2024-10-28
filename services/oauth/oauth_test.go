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
