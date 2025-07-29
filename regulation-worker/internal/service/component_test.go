package service_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	kitsync "github.com/rudderlabs/rudder-go-kit/sync"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	mocksBackendConfig "github.com/rudderlabs/rudder-server/mocks/backend-config"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/client"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/delete"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/delete/api"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/service"
	oauthv2 "github.com/rudderlabs/rudder-server/services/oauth/v2"
	"github.com/rudderlabs/rudder-server/services/oauth/v2/common"
	"github.com/rudderlabs/rudder-server/services/oauth/v2/extensions"
	oauthv2_http "github.com/rudderlabs/rudder-server/services/oauth/v2/http"
	"github.com/rudderlabs/rudder-server/utils/types/deployment"
)

func runIntegrationTest(
	t *testing.T,
	testName string,
	workspaceID, jobID, destinationID string,
	destConfig *mockDestinationConfig,
	configBackendServerFactory func(workspaceID, jobID, destinationID string) *httptest.Server,
) {
	t.Helper()

	configBackendServer := configBackendServerFactory(workspaceID, jobID, destinationID)
	transformerServer := createTransformerServer()
	defer configBackendServer.Close()
	defer transformerServer.Close()

	t.Setenv("DEST_TRANSFORM_URL", transformerServer.URL)
	t.Setenv("CONFIG_BACKEND_URL", configBackendServer.URL)
	t.Setenv("CONFIG_BACKEND_TOKEN", "test-token")

	backendconfig.Init()

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mockBackendConfig := mocksBackendConfig.NewMockBackendConfig(mockCtrl)
	mockBackendConfig.EXPECT().AccessToken().AnyTimes().Return("test-token")
	mockBackendConfig.EXPECT().Identity().AnyTimes().Return(&mockIdentifier{workspaceID: workspaceID})

	httpClient := &http.Client{
		Transport: &http.Transport{
			IdleConnTimeout: 300 * time.Second,
		},
	}
	cache := oauthv2.NewCache()
	oauthLock := kitsync.NewPartitionRWLocker()
	optionalArgs := oauthv2_http.HttpClientOptionalArgs{
		Augmenter: extensions.HeaderAugmenter,
		Locker:    oauthLock,
	}
	oauthClient := oauthv2_http.NewOAuthHttpClient(
		httpClient, common.RudderFlowDelete,
		&cache, mockBackendConfig,
		api.GetAuthErrorCategoryFromResponse, &optionalArgs,
	)

	apiManager := &api.APIManager{
		Client:                       oauthClient,
		DestTransformURL:             transformerServer.URL,
		MaxOAuthRefreshRetryAttempts: 1,
		TransformerFeaturesService:   &mockTransformerFeaturesService{},
	}

	router := delete.NewRouter(apiManager)

	jobSvc := &service.JobSvc{
		API: &client.JobAPI{
			Client:    &http.Client{},
			URLPrefix: configBackendServer.URL,
			Identity:  &mockIdentifier{workspaceID: workspaceID},
		},
		Deleter:           router,
		DestDetail:        destConfig,
		MaxFailedAttempts: 3,
	}

	ctx := context.Background()
	err := jobSvc.JobSvc(ctx)

	require.NoError(t, err, "%s should complete successfully", testName)
}

func TestOAuthV2Integration(t *testing.T) {
	workspaceID := "test-workspace"
	jobID := "1"
	destinationID := "dest-123"
	accountID := "account-123"
	destConfig := &mockDestinationConfig{
		destinations: map[string]model.Destination{
			destinationID: {
				DestinationID: destinationID,
				Name:          "GA",
				Config: map[string]interface{}{
					"rudderDeleteAccountId": accountID,
				},
				DestDefConfig: map[string]interface{}{
					"auth": map[string]interface{}{
						"type":         "OAuth",
						"rudderScopes": []interface{}{"delete"},
					},
				},
			},
		},
	}
	runIntegrationTest(
		t,
		"OAuthV2Integration",
		workspaceID, jobID, destinationID,
		destConfig,
		createConfigBackendServer,
	)
}

func TestNonOAuthIntegration(t *testing.T) {
	workspaceID := "test-workspace"
	jobID := "2"
	destinationID := "dest-456"
	destConfig := &mockDestinationConfig{
		destinations: map[string]model.Destination{
			destinationID: {
				DestinationID: destinationID,
				Name:          "GA",
				Config:        map[string]interface{}{},
				DestDefConfig: map[string]interface{}{
					"auth": map[string]interface{}{
						"type": "apiKey", // Not OAuth
					},
				},
			},
		},
	}
	runIntegrationTest(
		t,
		"NonOAuthIntegration",
		workspaceID, jobID, destinationID,
		destConfig,
		createConfigBackendServerNonOAuth,
	)
}

// Helper function to create config backend server
func createConfigBackendServer(workspaceID, jobID, destinationID string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Printf("Config Backend received request: %s %s\n", r.Method, r.URL.Path)

		switch r.Method {
		case "GET":
			if r.URL.Path == fmt.Sprintf("/dataplane/workspaces/%s/regulations/workerJobs", workspaceID) {
				jobResponse := fmt.Sprintf(`{
					"jobId": "%s",
					"workspaceId": "%s",
					"destinationId": "%s",
					"userAttributes": [
						{
							"userId": "user-1",
							"email": "test@example.com"
						}
					]
				}`, jobID, workspaceID, destinationID)

				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(jobResponse))
				return
			}
		case "PATCH":
			if r.URL.Path == fmt.Sprintf("/dataplane/workspaces/%s/regulations/workerJobs/%s", workspaceID, jobID) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				return
			}
		case "POST":
			// Handle OAuth token requests
			if r.URL.Path == fmt.Sprintf("/destination/workspaces/%s/accounts/account-123/token", workspaceID) {
				tokenResponse := `{
					"secret": {
						"access_token": "test_access_token",
						"refresh_token": "test_refresh_token",
						"expires_in": 3600
					}
				}`

				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(tokenResponse))
				return
			}
		}

		// Default response for unmatched requests
		w.WriteHeader(http.StatusNotFound)
	}))
}

// Helper function to create config backend server for non-OAuth test
func createConfigBackendServerNonOAuth(workspaceID, jobID, destinationID string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Printf("Config Backend (non-OAuth) received request: %s %s\n", r.Method, r.URL.Path)

		switch r.Method {
		case "GET":
			if r.URL.Path == fmt.Sprintf("/dataplane/workspaces/%s/regulations/workerJobs", workspaceID) {
				jobResponse := fmt.Sprintf(`{
					"jobId": "%s",
					"workspaceId": "%s",
					"destinationId": "%s",
					"userAttributes": [
						{
							"userId": "user-2",
							"email": "non-oauth@example.com"
						}
					]
				}`, jobID, workspaceID, destinationID)

				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(jobResponse))
				return
			}
		case "PATCH":
			if r.URL.Path == fmt.Sprintf("/dataplane/workspaces/%s/regulations/workerJobs/%s", workspaceID, jobID) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				return
			}
		}

		w.WriteHeader(http.StatusNotFound)
	}))
}

// Helper function to create transformer server
func createTransformerServer() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Printf("Transformer received request: %s %s\n", r.Method, r.URL.Path)

		if r.Method == "POST" && r.URL.Path == "/deleteUsers" {
			// Read and log the request body for debugging
			body := make([]byte, 1024)
			n, _ := r.Body.Read(body)
			fmt.Printf("Transformer request body: %s\n", string(body[:n]))

			response := `[{"status":"successful"}]`
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(response))
			return
		}

		// Default response for unmatched requests
		w.WriteHeader(http.StatusNotFound)
	}))
}

// Mock identifier for testing
type mockIdentifier struct {
	workspaceID string
}

func (m *mockIdentifier) ID() string                  { return m.workspaceID }
func (m *mockIdentifier) BasicAuth() (string, string) { return "test-token", "" }
func (m *mockIdentifier) Type() deployment.Type       { return deployment.DedicatedType }

// Mock destination config for testing
type mockDestinationConfig struct {
	destinations map[string]model.Destination
}

func (m *mockDestinationConfig) GetDestDetails(destID string) (model.Destination, error) {
	destination, ok := m.destinations[destID]
	if !ok {
		return model.Destination{}, model.ErrInvalidDestination
	}
	return destination, nil
}

// Mock transformer features service
type mockTransformerFeaturesService struct{}

func (m *mockTransformerFeaturesService) Wait() chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}

func (m *mockTransformerFeaturesService) Regulations() []string {
	return []string{"BRAZE", "AM", "INTERCOM", "CLEVERTAP", "AF", "MP", "GA", "ITERABLE", "ENGAGE", "CUSTIFY", "SENDGRID", "SPRIG"}
}

func (m *mockTransformerFeaturesService) SourceTransformerVersion() string {
	return "v2"
}

func (m *mockTransformerFeaturesService) RouterTransform(destType string) bool {
	return false
}

func (m *mockTransformerFeaturesService) TransformerProxyVersion() string {
	return "v0"
}

func (m *mockTransformerFeaturesService) SupportDestTransformCompactedPayloadV1() bool {
	return false
}
