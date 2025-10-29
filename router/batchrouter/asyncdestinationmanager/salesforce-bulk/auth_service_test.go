package salesforcebulk

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/logger"
	oauthv2 "github.com/rudderlabs/rudder-server/services/oauth/v2"
)

func TestSalesforceAuthService_GetAccessToken(t *testing.T) {
	t.Run("returns cached token if still valid", func(t *testing.T) {
		service := &SalesforceAuthService{
			logger:      logger.NOP,
			accessToken: "cached-token",
			instanceURL: "https://test.salesforce.com",
			tokenExpiry: time.Now().Unix() + 3600, // Expires in 1 hour
		}

		token, err := service.GetAccessToken()

		require.NoError(t, err)
		require.Equal(t, "cached-token", token)
	})

	t.Run("returns cached token instance URL", func(t *testing.T) {
		service := &SalesforceAuthService{
			accessToken: "test-token",
			instanceURL: "https://test.salesforce.com",
		}

		url, err := service.GetInstanceURL()
		require.NoError(t, err)
		require.Equal(t, "https://test.salesforce.com", url)
	})

	t.Run("handles OAuth error and clears token", func(t *testing.T) {
		callCount := 0
		mockClient := &mockOAuthClient{
			fetchTokenFunc: func(params *oauthv2.OAuthTokenParams) (json.RawMessage, oauthv2.StatusCodeError) {
				callCount++
				if callCount == 1 {
					// First call returns a token that will be "expired" (simulating 401)
					return json.RawMessage(`{
						"access_token": "expired-token",
						"instance_url": "https://test.salesforce.com",
						"expires_in": 3600
					}`), nil
				}
				// Second call after clearToken returns fresh token
				return json.RawMessage(`{
					"access_token": "fresh-token",
					"instance_url": "https://test.salesforce.com",
					"expires_in": 3600
				}`), nil
			},
		}

		service := &SalesforceAuthService{
			logger:      logger.NOP,
			oauthClient: mockClient,
			workspaceID: "test-workspace",
			accountID:   "test-account",
			destID:      "test-dest",
			tokenExpiry: 0,
		}

		// Get initial token
		token1, err := service.GetAccessToken()
		require.NoError(t, err)
		require.Equal(t, "expired-token", token1)

		// Clear token (simulating 401 error handler)
		service.clearToken()

		// Get fresh token - should call OAuth again
		token2, err := service.GetAccessToken()
		require.NoError(t, err)
		require.Equal(t, "fresh-token", token2)
		require.Equal(t, 2, callCount, "Should have called FetchToken twice")
	})
}

type mockOAuthClient struct {
	fetchTokenFunc func(*oauthv2.OAuthTokenParams) (json.RawMessage, oauthv2.StatusCodeError)
}

func (m *mockOAuthClient) FetchToken(params *oauthv2.OAuthTokenParams) (json.RawMessage, oauthv2.StatusCodeError) {
	if m.fetchTokenFunc != nil {
		return m.fetchTokenFunc(params)
	}
	return json.RawMessage(`{
		"access_token": "new-token",
		"instance_url": "https://instance.salesforce.com",
		"expires_in": 7200
	}`), nil
}

func (m *mockOAuthClient) RefreshToken(params *oauthv2.OAuthTokenParams, previousSecret json.RawMessage) (json.RawMessage, oauthv2.StatusCodeError) {
	return nil, oauthv2.NewStatusCodeError(500, fmt.Errorf("not implemented"))
}

func (m *mockOAuthClient) AuthStatusToggle(params *oauthv2.StatusRequestParams) oauthv2.StatusCodeError {
	return oauthv2.NewStatusCodeError(500, fmt.Errorf("not implemented"))
}

func TestSalesforceAuthService_GetInstanceURL_ColdStart(t *testing.T) {
	t.Run("GetInstanceURL should return instance URL for endpoint construction", func(t *testing.T) {
		mockClient := &mockOAuthClient{
			fetchTokenFunc: func(params *oauthv2.OAuthTokenParams) (json.RawMessage, oauthv2.StatusCodeError) {
				return json.RawMessage(`{
					"access_token": "test-token-123",
					"instance_url": "https://na123.salesforce.com",
					"expires_in": 3600
				}`), nil
			},
		}

		service := &SalesforceAuthService{
			logger:      logger.NOP,
			oauthClient: mockClient,
			workspaceID: "test-workspace",
			accountID:   "test-account",
			destID:      "test-dest",
		}

		instanceURL, err := service.GetInstanceURL()

		require.NoError(t, err)
		require.NotEmpty(t, instanceURL)
		require.Equal(t, "https://na123.salesforce.com", instanceURL)
	})
}

func TestSalesforceAuthService_FetchNewToken(t *testing.T) {
	t.Run("successfully fetches new token", func(t *testing.T) {
		mockClient := &mockOAuthClient{
			fetchTokenFunc: func(params *oauthv2.OAuthTokenParams) (json.RawMessage, oauthv2.StatusCodeError) {
				require.Equal(t, "test-workspace", params.WorkspaceID)
				require.Equal(t, "test-account", params.AccountID)
				require.Equal(t, destName, params.DestType)
				require.Equal(t, "test-dest", params.DestinationID)

				return json.RawMessage(`{
					"access_token": "fresh-token",
					"instance_url": "https://fresh.salesforce.com",
					"expires_in": 3600
				}`), nil
			},
		}

		service := &SalesforceAuthService{
			logger:      logger.NOP,
			oauthClient: mockClient,
			workspaceID: "test-workspace",
			accountID:   "test-account",
			destID:      "test-dest",
			tokenExpiry: time.Now().Unix() - 100, // Expired
		}

		token, err := service.GetAccessToken()

		require.NoError(t, err)
		require.Equal(t, "fresh-token", token)
		require.Equal(t, "https://fresh.salesforce.com", service.instanceURL)
	})

	t.Run("handles missing access token in response", func(t *testing.T) {
		mockClient := &mockOAuthClient{
			fetchTokenFunc: func(params *oauthv2.OAuthTokenParams) (json.RawMessage, oauthv2.StatusCodeError) {
				return json.RawMessage(`{
					"access_token": "",
					"instance_url": "https://test.salesforce.com"
				}`), nil
			},
		}

		service := &SalesforceAuthService{
			logger:      logger.NOP,
			oauthClient: mockClient,
			workspaceID: "test-workspace",
			accountID:   "test-account",
			tokenExpiry: 0,
		}

		_, err := service.GetAccessToken()

		require.Error(t, err)
		require.Contains(t, err.Error(), "access token is empty")
	})

	t.Run("handles missing instance URL in response", func(t *testing.T) {
		mockClient := &mockOAuthClient{
			fetchTokenFunc: func(params *oauthv2.OAuthTokenParams) (json.RawMessage, oauthv2.StatusCodeError) {
				return json.RawMessage(`{
					"access_token": "test-token",
					"instance_url": ""
				}`), nil
			},
		}

		service := &SalesforceAuthService{
			logger:      logger.NOP,
			oauthClient: mockClient,
			workspaceID: "test-workspace",
			accountID:   "test-account",
			tokenExpiry: 0,
		}

		_, err := service.GetAccessToken()

		require.Error(t, err)
		require.Contains(t, err.Error(), "instance URL is empty")
	})
}
