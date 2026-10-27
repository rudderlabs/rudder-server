package v2

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/logger"
)

var mockLogger = logger.NewLogger().Child("MockOAuthHandler")

func TestOAuthUtils(t *testing.T) {
	t.Run("Test GetRefreshTokenErrResp function", func(t *testing.T) {
		t.Run("Call GetRefreshTokenErrResp with empty response", func(t *testing.T) {
			_, refreshError := getRefreshTokenFromResponse(``, mockLogger)
			require.Equal(t, "unmarshallableResponse", refreshError.errType)
			require.Equal(t, "Unmarshal of response unsuccessful: ", refreshError.message)
		})

		t.Run("Call GetRefreshTokenErrResp with marshallable(into AccountSecret) response", func(t *testing.T) {
			oauthToken, refreshError := getRefreshTokenFromResponse(`{"options":{},"id":"2BFzzzID8kITtU7AxxWtrn9KQQf","createdAt":"2022-06-29T15:34:47.758Z","updatedAt":"2024-02-12T12:18:35.213Z","workspaceId":"1oVajb9QqG50undaAcokNlYyJQa","name":"dummy user","role":"google_adwords_enhanced_conversions_v1","userId":"1oVadeaoGXN2pataEEoeIaXS3bO","metadata":{"userId":"115538421777182389816","displayName":"dummy user","email":"dummy@testmail.com"},"secretVersion":50,"rudderCategory":"destination","secret":{"access_token":"newaccesstoken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken"}}`, mockLogger)
			require.Nil(t, refreshError)
			require.Empty(t, oauthToken.ExpirationDate)
			require.JSONEq(t, `{"access_token":"newaccesstoken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken"}`, string(oauthToken.Secret))
		})

		t.Run("Call GetRefreshTokenErrResp with invalid_grant response from control-plane", func(t *testing.T) {
			_, refreshError := getRefreshTokenFromResponse(`{"body":{"code":"ref_token_invalid_grant","message":"[criteo_audience] \"invalid_grant\" error, refresh token has expired or revoked"}}`, mockLogger)
			require.Equal(t, "ref_token_invalid_grant", refreshError.errType)
			require.Equal(t, "[criteo_audience] \"invalid_grant\" error, refresh token has expired or revoked", refreshError.message)
		})

		t.Run("Call GetRefreshTokenErrResp with invalid_grant response and empty message", func(t *testing.T) {
			_, refreshError := getRefreshTokenFromResponse(`{"body":{"code":"ref_token_invalid_grant"}}`, mockLogger)
			require.Equal(t, "ref_token_invalid_grant", refreshError.errType)
			require.Equal(t, "problem with user permission or access/refresh token have been revoked", refreshError.message)
		})

		t.Run("Call GetRefreshTokenErrResp with invalid_refresh_response code", func(t *testing.T) {
			_, refreshError := getRefreshTokenFromResponse(`{"body":{"code":"INVALID_REFRESH_RESPONSE","message":"Custom invalid refresh message"}}`, mockLogger)
			require.Equal(t, "INVALID_REFRESH_RESPONSE", refreshError.errType)
			require.Equal(t, "Custom invalid refresh message", refreshError.message)
		})

		t.Run("Call GetRefreshTokenErrResp with invalid_refresh_response code and empty message", func(t *testing.T) {
			_, refreshError := getRefreshTokenFromResponse(`{"body":{"code":"INVALID_REFRESH_RESPONSE"}}`, mockLogger)
			require.Equal(t, "INVALID_REFRESH_RESPONSE", refreshError.errType)
			require.Equal(t, "invalid auth token refresh response", refreshError.message)
		})

		t.Run("Call GetRefreshTokenErrResp with unknown error code and custom message", func(t *testing.T) {
			_, refreshError := getRefreshTokenFromResponse(`{"body":{"code":"httpError","message":"Connection timeout occurred"}}`, mockLogger)
			require.Equal(t, "httpError", refreshError.errType)
			require.Equal(t, "Connection timeout occurred", refreshError.message)
		})

		t.Run("Call GetRefreshTokenErrResp with unknown error code and empty message (default case)", func(t *testing.T) {
			_, refreshError := getRefreshTokenFromResponse(`{"body":{"code":"httpError"}}`, mockLogger)
			require.Equal(t, "httpError", refreshError.errType)
			require.Equal(t, "Internal service error: httpError", refreshError.message)
		})
	})
}
