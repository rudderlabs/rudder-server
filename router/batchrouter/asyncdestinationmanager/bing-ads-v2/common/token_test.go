package common_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	MockAuthorizer "github.com/rudderlabs/rudder-server/mocks/services/oauthV2"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/bing-ads-v2/common"
	v2 "github.com/rudderlabs/rudder-server/services/oauth/v2"
)

func TestTokenSource_GenerateTokenV2(t *testing.T) {
	t.Parallel()

	t.Run("successfully_generates_token_with_valid_access_and_refresh_tokens", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		oauthClient := MockAuthorizer.NewMockAuthorizer(ctrl)

		tokenSource := common.TokenSource{
			DestinationDefName: "Test",
			DestinationID:      "1234",
			AccountID:          "1234",
			OauthClientV2:      oauthClient,
			CurrentTime: func() time.Time {
				mockTime := time.Date(2025, time.March, 12, 12, 0, 0, 0, time.UTC)
				return mockTime
			},
		}

		oauthClient.EXPECT().FetchToken(gomock.Any()).Return(200,
			&v2.AuthResponse{
				Account: v2.AccountSecret{
					ExpirationDate: "",
					Secret:         []byte(`{"accessToken":"validAccessToken","refreshToken":"validRefreshToken","developer_token":"dev","expirationDate":"2025-03-12T12:34:47.758Z"}`),
				},
			}, nil)

		token, err := tokenSource.GenerateTokenV2()
		require.NoError(t, err)
		require.Equal(t, "validAccessToken", token.AccessToken)
		require.Equal(t, "validRefreshToken", token.RefreshToken)
		require.Equal(t, "dev", token.DeveloperToken)
		require.Equal(t, "2025-03-12T12:34:47.758Z", token.ExpirationDate)
	})

	t.Run("successfully_refreshes_expired_token", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		oauthClient := MockAuthorizer.NewMockAuthorizer(ctrl)

		tokenSource := common.TokenSource{
			DestinationDefName: "Test",
			DestinationID:      "1234",
			AccountID:          "1234",
			OauthClientV2:      oauthClient,
			CurrentTime: func() time.Time {
				mockTime := time.Date(2025, time.March, 13, 12, 0, 0, 0, time.UTC)
				return mockTime
			},
		}

		oauthClient.EXPECT().FetchToken(gomock.Any()).Return(200,
			&v2.AuthResponse{
				Account: v2.AccountSecret{
					ExpirationDate: "",
					Secret:         []byte(`{"accessToken":"expiredAccessToken","refreshToken":"validRefreshToken","developer_token":"dev","expirationDate":"2025-03-12T12:34:47.758Z"}`),
				},
			}, nil)

		oauthClient.EXPECT().RefreshToken(gomock.Any()).Return(200,
			&v2.AuthResponse{
				Account: v2.AccountSecret{
					ExpirationDate: "",
					Secret:         []byte(`{"accessToken":"validAccessToken","refreshToken":"validRefreshToken","developer_token":"dev","expirationDate":"2025-03-13T13:34:47.758Z"}`),
				},
			}, nil)

		token, err := tokenSource.GenerateTokenV2()
		require.NoError(t, err)
		require.Equal(t, "validAccessToken", token.AccessToken)
		require.Equal(t, "validRefreshToken", token.RefreshToken)
		require.Equal(t, "dev", token.DeveloperToken)
		require.Equal(t, "2025-03-13T13:34:47.758Z", token.ExpirationDate)
	})

	t.Run("handles_error_when_fetch_token_returns_error_with_response", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		oauthClient := MockAuthorizer.NewMockAuthorizer(ctrl)

		tokenSource := common.TokenSource{
			DestinationDefName: "Test",
			DestinationID:      "1234",
			AccountID:          "1234",
			OauthClientV2:      oauthClient,
			CurrentTime: func() time.Time {
				mockTime := time.Date(2025, time.March, 12, 12, 0, 0, 0, time.UTC)
				return mockTime
			},
		}

		oauthClient.EXPECT().FetchToken(gomock.Any()).Return(400,
			&v2.AuthResponse{
				Account: v2.AccountSecret{
					ExpirationDate: "",
					Secret:         []byte(`{"accessToken":"","refreshToken":"","developer_token":"","expirationDate":""}`),
				},
				Err: "invalid_request",
			}, fmt.Errorf("invalid request"))

		_, err := tokenSource.GenerateTokenV2()
		require.Error(t, err)
		require.Contains(t, err.Error(), "fetching access token: invalid_request, 400")
	})

	t.Run("handles_error_when_fetch_token_returns_error_without_response", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		oauthClient := MockAuthorizer.NewMockAuthorizer(ctrl)

		tokenSource := common.TokenSource{
			DestinationDefName: "Test",
			DestinationID:      "1234",
			AccountID:          "1234",
			OauthClientV2:      oauthClient,
			CurrentTime: func() time.Time {
				mockTime := time.Date(2025, time.March, 12, 12, 0, 0, 0, time.UTC)
				return mockTime
			},
		}

		oauthClient.EXPECT().FetchToken(gomock.Any()).Return(500, nil, fmt.Errorf("server error"))

		_, err := tokenSource.GenerateTokenV2()
		require.Error(t, err)
		require.Contains(t, err.Error(), "fetching access token resulted in an error: server error,500")
	})

	t.Run("handles_error_when_unmarshalling_secret_fails", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		oauthClient := MockAuthorizer.NewMockAuthorizer(ctrl)

		tokenSource := common.TokenSource{
			DestinationDefName: "Test",
			DestinationID:      "1234",
			AccountID:          "1234",
			OauthClientV2:      oauthClient,
			CurrentTime: func() time.Time {
				mockTime := time.Date(2025, time.March, 12, 12, 0, 0, 0, time.UTC)
				return mockTime
			},
		}

		oauthClient.EXPECT().FetchToken(gomock.Any()).Return(200,
			&v2.AuthResponse{
				Account: v2.AccountSecret{
					ExpirationDate: "",
					Secret:         []byte(`invalid json`),
				},
			}, nil)

		_, err := tokenSource.GenerateTokenV2()
		require.Error(t, err)
		require.Contains(t, err.Error(), "error in unmarshalling secret")
	})

	t.Run("handles_error_when_refresh_token_returns_error", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		oauthClient := MockAuthorizer.NewMockAuthorizer(ctrl)

		tokenSource := common.TokenSource{
			DestinationDefName: "Test",
			DestinationID:      "1234",
			AccountID:          "1234",
			OauthClientV2:      oauthClient,
			CurrentTime: func() time.Time {
				mockTime := time.Date(2025, time.March, 13, 12, 0, 0, 0, time.UTC)
				return mockTime
			},
		}

		oauthClient.EXPECT().FetchToken(gomock.Any()).Return(200,
			&v2.AuthResponse{
				Account: v2.AccountSecret{
					ExpirationDate: "",
					Secret:         []byte(`{"accessToken":"expiredAccessToken","refreshToken":"validRefreshToken","developer_token":"dev","expirationDate":"2025-03-12T12:34:47.758Z"}`),
				},
			}, nil)

		oauthClient.EXPECT().RefreshToken(gomock.Any()).Return(400, nil, fmt.Errorf("error refreshing token"))

		_, err := tokenSource.GenerateTokenV2()
		require.Error(t, err)
		expectedError := fmt.Errorf("error in refreshing access token with this error: %w. StatusCode: %d", fmt.Errorf("error refreshing token"), 400)
		require.Equal(t, expectedError.Error(), err.Error())
	})

	t.Run("handles_error_when_refresh_token_unmarshalling_fails", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		oauthClient := MockAuthorizer.NewMockAuthorizer(ctrl)

		tokenSource := common.TokenSource{
			DestinationDefName: "Test",
			DestinationID:      "1234",
			AccountID:          "1234",
			OauthClientV2:      oauthClient,
			CurrentTime: func() time.Time {
				mockTime := time.Date(2025, time.March, 13, 12, 0, 0, 0, time.UTC)
				return mockTime
			},
		}

		oauthClient.EXPECT().FetchToken(gomock.Any()).Return(200,
			&v2.AuthResponse{
				Account: v2.AccountSecret{
					ExpirationDate: "",
					Secret:         []byte(`{"accessToken":"expiredAccessToken","refreshToken":"validRefreshToken","developer_token":"dev","expirationDate":"2025-03-12T12:34:47.758Z"}`),
				},
			}, nil)

		oauthClient.EXPECT().RefreshToken(gomock.Any()).Return(200,
			&v2.AuthResponse{
				Account: v2.AccountSecret{
					ExpirationDate: "",
					Secret:         []byte(`invalid json after refresh`),
				},
			}, nil)

		_, err := tokenSource.GenerateTokenV2()
		require.Error(t, err)
		require.Contains(t, err.Error(), "error in unmarshalling secret")
	})
}

func TestTokenSource_GenerateToken(t *testing.T) {
	t.Parallel()

	t.Run("successfully_generates_token_with_valid_access_and_refresh_tokens", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		oauthClient := MockAuthorizer.NewMockAuthorizer(ctrl)

		tokenSource := common.TokenSource{
			DestinationDefName: "Test",
			DestinationID:      "1234",
			AccountID:          "1234",
			OauthClientV2:      oauthClient,
			CurrentTime: func() time.Time {
				mockTime := time.Date(2025, time.March, 12, 12, 0, 0, 0, time.UTC)
				return mockTime
			},
		}

		oauthClient.EXPECT().FetchToken(gomock.Any()).Return(200,
			&v2.AuthResponse{
				Account: v2.AccountSecret{
					ExpirationDate: "",
					Secret:         []byte(`{"accessToken":"validAccessToken","refreshToken":"validRefreshToken","developer_token":"dev","expirationDate":"2025-03-12T12:34:47.758Z"}`),
				},
			}, nil)

		token, err := tokenSource.Token()
		require.NoError(t, err)
		require.Equal(t, "validAccessToken", token.AccessToken)
		require.Equal(t, "validRefreshToken", token.RefreshToken)
		require.WithinDuration(t, time.Date(2025, time.March, 12, 12, 34, 47, 758000000, time.UTC), token.Expiry, 1*time.Second)
	})

	t.Run("successfully_refreshes_expired_token", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		oauthClient := MockAuthorizer.NewMockAuthorizer(ctrl)

		tokenSource := common.TokenSource{
			DestinationDefName: "Test",
			DestinationID:      "1234",
			AccountID:          "1234",
			OauthClientV2:      oauthClient,
			CurrentTime: func() time.Time {
				mockTime := time.Date(2025, time.March, 13, 12, 0, 0, 0, time.UTC)
				return mockTime
			},
		}

		oauthClient.EXPECT().FetchToken(gomock.Any()).Return(200,
			&v2.AuthResponse{
				Account: v2.AccountSecret{
					ExpirationDate: "",
					Secret:         []byte(`{"accessToken":"expiredAccessToken","refreshToken":"validRefreshToken","developer_token":"dev","expirationDate":"2025-03-12T12:34:47.758Z"}`),
				},
			}, nil)

		oauthClient.EXPECT().RefreshToken(gomock.Any()).Return(200,
			&v2.AuthResponse{
				Account: v2.AccountSecret{
					ExpirationDate: "",
					Secret:         []byte(`{"accessToken":"validAccessToken","refreshToken":"validRefreshToken","developer_token":"dev","expirationDate":"2025-03-13T13:34:47.758Z"}`),
				},
			}, nil)

		token, err := tokenSource.Token()
		require.NoError(t, err)
		require.Equal(t, "validAccessToken", token.AccessToken)
		require.Equal(t, "validRefreshToken", token.RefreshToken)
		require.WithinDuration(t, time.Date(2025, time.March, 13, 13, 34, 47, 758000000, time.UTC), token.Expiry, 1*time.Second)
	})

	t.Run("handles_error_when_refresh_token_returns_error", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		oauthClient := MockAuthorizer.NewMockAuthorizer(ctrl)

		tokenSource := common.TokenSource{
			DestinationDefName: "Test",
			DestinationID:      "1234",
			AccountID:          "1234",
			OauthClientV2:      oauthClient,
			CurrentTime: func() time.Time {
				mockTime := time.Date(2025, time.March, 13, 12, 0, 0, 0, time.UTC)
				return mockTime
			},
		}

		oauthClient.EXPECT().FetchToken(gomock.Any()).Return(200,
			&v2.AuthResponse{
				Account: v2.AccountSecret{
					ExpirationDate: "",
					Secret:         []byte(`{"accessToken":"expiredAccessToken","refreshToken":"validRefreshToken","developer_token":"dev","expirationDate":"2025-03-12T12:34:47.758Z"}`),
				},
			}, nil)

		oauthClient.EXPECT().RefreshToken(gomock.Any()).Return(400, nil, fmt.Errorf("error refreshing token"))

		_, err := tokenSource.Token()
		require.Error(t, err)
		expectedError := fmt.Errorf("generating the accessToken: %w", fmt.Errorf("error in refreshing access token with this error: %w. StatusCode: %d", fmt.Errorf("error refreshing token"), 400))
		require.Equal(t, expectedError.Error(), err.Error())
	})

	t.Run("handles_error_when_parsing_expiration_date", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		oauthClient := MockAuthorizer.NewMockAuthorizer(ctrl)

		tokenSource := common.TokenSource{
			DestinationDefName: "Test",
			DestinationID:      "1234",
			AccountID:          "1234",
			OauthClientV2:      oauthClient,
			CurrentTime: func() time.Time {
				mockTime := time.Date(2025, time.March, 13, 12, 0, 0, 0, time.UTC)
				return mockTime
			},
		}

		oauthClient.EXPECT().FetchToken(gomock.Any()).Return(200,
			&v2.AuthResponse{
				Account: v2.AccountSecret{
					ExpirationDate: "",
					Secret:         []byte(`{"accessToken":"expiredAccessToken","refreshToken":"validRefreshToken","developer_token":"dev","expirationDate":"invalid-date"}`),
				},
			}, nil)

		_, err := tokenSource.Token()
		require.Error(t, err)
		require.Contains(t, err.Error(), "error in parsing expirationDate")
		require.Contains(t, err.Error(), "invalid-date")
	})
}
