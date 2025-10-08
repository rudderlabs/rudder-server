package salesforcebulk

import (
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

		url := service.GetInstanceURL()
		require.Equal(t, "https://test.salesforce.com", url)
	})
}

type mockOAuthClient struct {
	fetchTokenFunc func(*oauthv2.RefreshTokenParams) (int, *oauthv2.AuthResponse, error)
}

func (m *mockOAuthClient) FetchToken(params *oauthv2.RefreshTokenParams) (int, *oauthv2.AuthResponse, error) {
	if m.fetchTokenFunc != nil {
		return m.fetchTokenFunc(params)
	}
	return 200, &oauthv2.AuthResponse{
		Account: oauthv2.AccountSecret{
			Secret: []byte(`{
				"access_token": "new-token",
				"instance_url": "https://instance.salesforce.com",
				"expires_in": 7200
			}`),
		},
	}, nil
}

func (m *mockOAuthClient) RefreshToken(params *oauthv2.RefreshTokenParams) (int, *oauthv2.AuthResponse, error) {
	return 0, nil, fmt.Errorf("not implemented")
}

func (m *mockOAuthClient) AuthStatusToggle(params *oauthv2.AuthStatusToggleParams) (int, string) {
	return 0, "not implemented"
}

func TestSalesforceAuthService_FetchNewToken(t *testing.T) {
	t.Run("successfully fetches new token", func(t *testing.T) {
		mockClient := &mockOAuthClient{
			fetchTokenFunc: func(params *oauthv2.RefreshTokenParams) (int, *oauthv2.AuthResponse, error) {
				require.Equal(t, "test-workspace", params.WorkspaceID)
				require.Equal(t, "test-account", params.AccountID)

				return 200, &oauthv2.AuthResponse{
					Account: oauthv2.AccountSecret{
						Secret: []byte(`{
							"access_token": "fresh-token",
							"instance_url": "https://fresh.salesforce.com",
							"expires_in": 3600
						}`),
					},
				}, nil
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
			fetchTokenFunc: func(params *oauthv2.RefreshTokenParams) (int, *oauthv2.AuthResponse, error) {
				return 200, &oauthv2.AuthResponse{
					Account: oauthv2.AccountSecret{
						Secret: []byte(`{
							"access_token": "",
							"instance_url": "https://test.salesforce.com"
						}`),
					},
				}, nil
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
			fetchTokenFunc: func(params *oauthv2.RefreshTokenParams) (int, *oauthv2.AuthResponse, error) {
				return 200, &oauthv2.AuthResponse{
					Account: oauthv2.AccountSecret{
						Secret: []byte(`{
							"access_token": "test-token",
							"instance_url": ""
						}`),
					},
				}, nil
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

