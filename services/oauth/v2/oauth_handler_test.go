package v2_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"os"
	"runtime"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	kitsync "github.com/rudderlabs/rudder-go-kit/sync"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	mockhttpclient "github.com/rudderlabs/rudder-server/mocks/services/oauth/v2/http"
	mock_oauthV2 "github.com/rudderlabs/rudder-server/mocks/services/oauthV2"
	v2 "github.com/rudderlabs/rudder-server/services/oauth/v2"
	"github.com/rudderlabs/rudder-server/services/oauth/v2/common"
	"github.com/rudderlabs/rudder-server/services/oauth/v2/controlplane"
	testutils "github.com/rudderlabs/rudder-server/utils/tests"
)

var Destination = &backendconfig.DestinationT{
	ID:          "Destination123",
	WorkspaceID: "456",
	Config:      map[string]any{},
	DestinationDefinition: backendconfig.DestinationDefinitionT{
		Name:   "testDest",
		Config: map[string]any{},
	},
}

func TestOauth(t *testing.T) {
	timeoutError := "connection timed out"
	if runtime.GOOS == "darwin" {
		timeoutError = "operation timed out"
	}
	t.Run("Test FetchToken function", func(t *testing.T) {
		t.Run("fetch token function call when cache is empty", func(t *testing.T) {
			fetchTokenParams := &v2.OAuthTokenParams{
				AccountID:     "123",
				WorkspaceID:   "456",
				DestType:      "testDest",
				DestinationID: Destination.ID,
			}

			ctrl := gomock.NewController(t)
			mockCpConnector := mock_oauthV2.NewMockConnector(ctrl)
			mockCpConnector.EXPECT().CpApiCall(gomock.Any()).Return(http.StatusOK, `{"options":{},"id":"2BFzzzID8kITtU7AxxWtrn9KQQf","createdAt":"2022-06-29T15:34:47.758Z","updatedAt":"2024-02-12T12:18:35.213Z","workspaceId":"1oVajb9QqG50undaAcokNlYyJQa","name":"dummy user","role":"google_adwords_enhanced_conversions_v1","userId":"1oVadeaoGXN2pataEEoeIaXS3bO","metadata":{"userId":"115538421777182389816","displayName":"dummy user","email":"dummy@testmail.com"},"secretVersion":50,"rudderCategory":"destination","secret":{"access_token":"newaccesstoken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken"}}`)
			mockAuthIdentityProvider := mock_oauthV2.NewMockAuthIdentityProvider(ctrl)
			mockAuthIdentityProvider.EXPECT().Identity().Return(nil)
			expectedResponse := []byte(`{"access_token":"newaccesstoken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken"}`)

			cache := v2.NewOauthTokenCache()
			// Invoke code under test
			oauthHandler := v2.NewOAuthHandler(mockAuthIdentityProvider,
				v2.WithCache(cache),
				v2.WithLocker(kitsync.NewPartitionRWLocker()),
				v2.WithStats(stats.Default),
				v2.WithLogger(logger.NewLogger().Child("MockOAuthHandler")),
				v2.WithCpClient(mockCpConnector),
			)
			response, err := oauthHandler.FetchToken(fetchTokenParams)
			// Assertions
			require.NoError(t, err)
			require.JSONEq(t, string(expectedResponse), string(response))
			token, _ := cache.Load(fetchTokenParams.AccountID)
			require.JSONEq(t, string(expectedResponse), string(token.Secret))
		})

		t.Run("fetch token function call when cache is not empty and token is not expired", func(t *testing.T) {
			fetchTokenParams := &v2.OAuthTokenParams{
				AccountID:     "123",
				WorkspaceID:   "456",
				DestType:      "testDest",
				DestinationID: Destination.ID,
			}
			expectedResponse := []byte(`{"access_token":"StoredDummyaccesstoken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken"}`)

			cache := v2.NewOauthTokenCache()
			// Invoke code under test
			oauthHandler := v2.NewOAuthHandler(nil,
				v2.WithCache(cache),
				v2.WithLocker(kitsync.NewPartitionRWLocker()),
				v2.WithLogger(logger.NewLogger().Child("MockOAuthHandler")),
				v2.WithStats(stats.Default),
			)
			storedAuthToken := v2.OAuthToken{
				Secret: []byte(`{"access_token":"StoredDummyaccesstoken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken"}`),
			}
			cache.Store(fetchTokenParams.AccountID, storedAuthToken)
			response, err := oauthHandler.FetchToken(fetchTokenParams)
			// Assertions
			require.NoError(t, err)
			require.JSONEq(t, string(expectedResponse), string(response))
			token, _ := cache.Load(fetchTokenParams.AccountID)
			// We are checking if the token is updated in the cache or not
			require.Equal(t, storedAuthToken, token)
		})

		t.Run("fetch token function call when cache is not empty and token is expired", func(t *testing.T) {
			fetchTokenParams := &v2.OAuthTokenParams{
				AccountID:     "123",
				WorkspaceID:   "456",
				DestType:      "testDest",
				DestinationID: Destination.ID,
			}

			ctrl := gomock.NewController(t)
			mockCpConnector := mock_oauthV2.NewMockConnector(ctrl)
			mockCpConnector.EXPECT().CpApiCall(gomock.Any()).Return(http.StatusOK, `{"options":{},"id":"2BFzzzID8kITtU7AxxWtrn9KQQf","createdAt":"2022-06-29T15:34:47.758Z","updatedAt":"2024-02-12T12:18:35.213Z","workspaceId":"1oVajb9QqG50undaAcokNlYyJQa","name":"dummy user","role":"google_adwords_enhanced_conversions_v1","userId":"1oVadeaoGXN2pataEEoeIaXS3bO","metadata":{"userId":"115538421777182389816","displayName":"dummy user","email":"dummy@testmail.com"},"secretVersion":50,"rudderCategory":"destination","secret":{"access_token":"NewDummyaccesstoken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken"}}`)
			mockAuthIdentityProvider := mock_oauthV2.NewMockAuthIdentityProvider(ctrl)
			mockAuthIdentityProvider.EXPECT().Identity().Return(nil)
			expectedResponse := []byte(`{"access_token":"NewDummyaccesstoken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken"}`)
			cache := v2.NewOauthTokenCache()
			oauthHandler := v2.NewOAuthHandler(mockAuthIdentityProvider,
				v2.WithCache(cache),
				v2.WithLocker(kitsync.NewPartitionRWLocker()),
				v2.WithStats(stats.Default),
				v2.WithLogger(logger.NewLogger().Child("MockOAuthHandler")),
				v2.WithCpClient(mockCpConnector),
			)
			storedAuthToken := v2.OAuthToken{
				Secret:         []byte(`{"access_token":"StoredDummyaccesstoken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken","expirationDate":"2022-06-29T15:34:47.758Z"}`),
				ExpirationDate: "2022-06-29T15:34:47.758Z",
			}
			cache.Store(fetchTokenParams.AccountID, storedAuthToken)
			response, err := oauthHandler.FetchToken(fetchTokenParams)
			require.NoError(t, err)
			require.JSONEq(t, string(expectedResponse), string(response))
			token, _ := cache.Load(fetchTokenParams.AccountID)
			// We are checking if the token is updated in the cache or not
			require.NotEqual(t, storedAuthToken, token)
		})

		t.Run("fetch token function call is successful and token is returned with 'expirationDate', should contain ExpirationDate information in AccountSecret{}", func(t *testing.T) {
			fetchTokenParams := &v2.OAuthTokenParams{
				AccountID:     "123",
				WorkspaceID:   "456",
				DestType:      "testDest",
				DestinationID: Destination.ID,
			}

			ctrl := gomock.NewController(t)
			mockCpConnector := mock_oauthV2.NewMockConnector(ctrl)
			mockCpConnector.EXPECT().CpApiCall(gomock.Any()).Return(http.StatusOK, `{"options":{},"id":"2BFzzzID8kITtU7AxxWtrn9KQQf","createdAt":"2022-06-29T15:34:47.758Z","updatedAt":"2024-02-12T12:18:35.213Z","workspaceId":"1oVajb9QqG50undaAcokNlYyJQa","name":"dummy user","role":"google_adwords_enhanced_conversions_v1","userId":"1oVadeaoGXN2pataEEoeIaXS3bO","metadata":{"userId":"115538421777182389816","displayName":"dummy user","email":"dummy@testmail.com"},"secretVersion":50,"rudderCategory":"destination","secret":{"access_token":"NewDummyaccesstoken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken","expirationDate":"2022-06-29T16:34:47.758Z"}}`)
			mockAuthIdentityProvider := mock_oauthV2.NewMockAuthIdentityProvider(ctrl)
			mockAuthIdentityProvider.EXPECT().Identity().Return(nil)
			expectedResponse := []byte(`{"access_token":"NewDummyaccesstoken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken","expirationDate":"2022-06-29T16:34:47.758Z"}`)
			expectedExpirationDate := "2022-06-29T16:34:47.758Z"
			cache := v2.NewOauthTokenCache()
			oauthHandler := v2.NewOAuthHandler(mockAuthIdentityProvider,
				v2.WithCache(cache),
				v2.WithLocker(kitsync.NewPartitionRWLocker()),
				v2.WithStats(stats.Default),
				v2.WithLogger(logger.NewLogger().Child("MockOAuthHandler")),
				v2.WithCpClient(mockCpConnector),
			)

			_, err := oauthHandler.FetchToken(fetchTokenParams)
			require.NoError(t, err)
			oauthToken, _ := cache.Load(fetchTokenParams.AccountID)
			// We are checking if the token is updated in the cache or not
			require.Equal(t, expectedExpirationDate, oauthToken.ExpirationDate)
			require.JSONEq(t, string(expectedResponse), string(oauthToken.Secret))
		})

		t.Run("fetch token function call when cache is empty and cpApiCall returns empty response", func(t *testing.T) {
			fetchTokenParams := &v2.OAuthTokenParams{
				AccountID:     "123",
				WorkspaceID:   "456",
				DestType:      "testDest",
				DestinationID: Destination.ID,
			}

			ctrl := gomock.NewController(t)
			mockCpConnector := mock_oauthV2.NewMockConnector(ctrl)
			mockCpConnector.EXPECT().CpApiCall(gomock.Any()).Return(http.StatusNoContent, ``)
			mockAuthIdentityProvider := mock_oauthV2.NewMockAuthIdentityProvider(ctrl)
			mockAuthIdentityProvider.EXPECT().Identity().Return(nil)
			oauthHandler := v2.NewOAuthHandler(mockAuthIdentityProvider,
				v2.WithCache(v2.NewOauthTokenCache()),
				v2.WithLocker(kitsync.NewPartitionRWLocker()),
				v2.WithStats(stats.Default),
				v2.WithLogger(logger.NewLogger().Child("MockOAuthHandler")),
				v2.WithCpClient(mockCpConnector),
			)
			response, err := oauthHandler.FetchToken(fetchTokenParams)
			require.Error(t, err)
			require.Equal(t, http.StatusInternalServerError, err.StatusCode())
			require.Nil(t, response)
			requireErrorInChain(t, err, "empty secret in response from control plane")
		})

		t.Run("fetch token function call when cache is empty and cpApiCall returns a failed response", func(t *testing.T) {
			fetchTokenParams := &v2.OAuthTokenParams{
				AccountID:     "123",
				WorkspaceID:   "456",
				DestType:      "testDest",
				DestinationID: Destination.ID,
			}

			ctrl := gomock.NewController(t)
			mockCpConnector := mock_oauthV2.NewMockConnector(ctrl)
			mockCpConnector.EXPECT().CpApiCall(gomock.Any()).Return(http.StatusNoContent, `{
				"body":{
				  "code":"ref_token_invalid_grant",
				  "message":"invalid_grant error, refresh token has expired or revoked"
				}
			  }`)
			mockAuthIdentityProvider := mock_oauthV2.NewMockAuthIdentityProvider(ctrl)
			mockAuthIdentityProvider.EXPECT().Identity().Return(nil)
			oauthHandler := v2.NewOAuthHandler(mockAuthIdentityProvider,
				v2.WithCache(v2.NewOauthTokenCache()),
				v2.WithLocker(kitsync.NewPartitionRWLocker()),
				v2.WithStats(stats.Default),
				v2.WithLogger(logger.NewLogger().Child("MockOAuthHandler")),
				v2.WithCpClient(mockCpConnector),
			)
			response, err := oauthHandler.FetchToken(fetchTokenParams)
			require.Error(t, err)
			require.Nil(t, response)
			require.Equal(t, http.StatusBadRequest, err.StatusCode())
			require.ErrorIs(t, err, common.ErrInvalidGrant)
		})
		t.Run("fetch token function call when cache is empty and cpApiCall returns a failed response due to config backend service is not available", func(t *testing.T) {
			fetchTokenParams := &v2.OAuthTokenParams{
				AccountID:     "123",
				WorkspaceID:   "456",
				DestType:      "testDest",
				DestinationID: Destination.ID,
			}

			ctrl := gomock.NewController(t)
			mockHttpClient := mockhttpclient.NewMockHttpClient(ctrl)
			mockHttpClient.EXPECT().Do(gomock.Any()).Return(&http.Response{}, &net.OpError{
				Op:     "mock",
				Net:    "mock",
				Source: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 1234},
				Addr:   &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 12340},
				Err:    &os.SyscallError{Syscall: "read", Err: syscall.ECONNREFUSED},
			})
			cpConnector := controlplane.NewConnector(
				config.Default,
				controlplane.WithClient(mockHttpClient),
				controlplane.WithStats(stats.Default),
			)

			mockAuthIdentityProvider := mock_oauthV2.NewMockAuthIdentityProvider(ctrl)
			mockAuthIdentityProvider.EXPECT().Identity().Return(&testutils.BasicAuthMock{})
			oauthHandler := v2.NewOAuthHandler(mockAuthIdentityProvider,
				v2.WithCache(v2.NewOauthTokenCache()),
				v2.WithLocker(kitsync.NewPartitionRWLocker()),
				v2.WithStats(stats.Default),
				v2.WithLogger(logger.NewLogger().Child("MockOAuthHandler")),
				v2.WithCpClient(cpConnector),
			)
			response, err := oauthHandler.FetchToken(fetchTokenParams)
			require.Error(t, err)
			require.Nil(t, response)
			require.Equal(t, http.StatusInternalServerError, err.StatusCode())
			var target *v2.TypeMessageError
			require.ErrorAs(t, err, &target)
			require.Equal(t, &v2.TypeMessageError{Type: "econnrefused", Message: "mock mock 127.0.0.1:1234->127.0.0.1:12340: read: connection refused"}, target)
			require.NotErrorIs(t, err, common.ErrInvalidGrant)
		})
		t.Run("fetch token function call when cache is empty and cpApiCall returns a failed response due to config backend call failed due to timeout error", func(t *testing.T) {
			fetchTokenParams := &v2.OAuthTokenParams{
				AccountID:     "123",
				WorkspaceID:   "456",
				DestType:      "testDest",
				DestinationID: Destination.ID,
			}

			ctrl := gomock.NewController(t)
			mockHttpClient := mockhttpclient.NewMockHttpClient(ctrl)
			mockHttpClient.EXPECT().Do(gomock.Any()).Return(&http.Response{}, &net.OpError{
				Op:     "mock",
				Net:    "mock",
				Source: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 1234},
				Addr:   &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 12340},
				Err:    &os.SyscallError{Syscall: "read", Err: syscall.ETIMEDOUT},
			})
			cpConnector := controlplane.NewConnector(
				config.Default,
				controlplane.WithClient(mockHttpClient),
				controlplane.WithStats(stats.Default),
			)

			mockAuthIdentityProvider := mock_oauthV2.NewMockAuthIdentityProvider(ctrl)
			mockAuthIdentityProvider.EXPECT().Identity().Return(&testutils.BasicAuthMock{})
			oauthHandler := v2.NewOAuthHandler(mockAuthIdentityProvider,
				v2.WithCache(v2.NewOauthTokenCache()),
				v2.WithLocker(kitsync.NewPartitionRWLocker()),
				v2.WithStats(stats.Default),
				v2.WithLogger(logger.NewLogger().Child("MockOAuthHandler")),
				v2.WithCpClient(cpConnector),
			)
			response, err := oauthHandler.FetchToken(fetchTokenParams)
			require.Error(t, err)
			require.Nil(t, response)
			require.Equal(t, http.StatusInternalServerError, err.StatusCode())
			var target *v2.TypeMessageError
			require.ErrorAs(t, err, &target)
			require.Equal(t, &v2.TypeMessageError{Type: "timeout", Message: "mock mock 127.0.0.1:1234->127.0.0.1:12340: read: " + timeoutError}, target)
			require.NotErrorIs(t, err, common.ErrInvalidGrant)
		})

		t.Run("fetch token function call when cache is empty and cpApiCall returns empty secret", func(t *testing.T) {
			fetchTokenParams := &v2.OAuthTokenParams{
				AccountID:     "123",
				WorkspaceID:   "456",
				DestType:      "testDest",
				DestinationID: Destination.ID,
			}

			ctrl := gomock.NewController(t)
			mockCpConnector := mock_oauthV2.NewMockConnector(ctrl)
			mockCpConnector.EXPECT().CpApiCall(gomock.Any()).Return(http.StatusOK, `{"secret":{}}`)
			mockAuthIdentityProvider := mock_oauthV2.NewMockAuthIdentityProvider(ctrl)
			mockAuthIdentityProvider.EXPECT().Identity().Return(nil)
			oauthHandler := v2.NewOAuthHandler(mockAuthIdentityProvider,
				v2.WithCache(v2.NewOauthTokenCache()),
				v2.WithLocker(kitsync.NewPartitionRWLocker()),
				v2.WithStats(stats.Default),
				v2.WithLogger(logger.NewLogger().Child("MockOAuthHandler")),
				v2.WithCpClient(mockCpConnector),
			)
			response, err := oauthHandler.FetchToken(fetchTokenParams)
			require.Error(t, err)
			require.Nil(t, response)
			require.Equal(t, http.StatusInternalServerError, err.StatusCode())
			requireErrorInChain(t, err, "empty secret received from CP")
		})

		t.Run("fetch token function call when cache is empty and cpApiCall returns empty string", func(t *testing.T) {
			fetchTokenParams := &v2.OAuthTokenParams{
				AccountID:     "123",
				WorkspaceID:   "456",
				DestType:      "testDest",
				DestinationID: Destination.ID,
			}

			ctrl := gomock.NewController(t)
			mockCpConnector := mock_oauthV2.NewMockConnector(ctrl)
			mockCpConnector.EXPECT().CpApiCall(gomock.Any()).Return(http.StatusOK, `{"secret":""}`)
			mockAuthIdentityProvider := mock_oauthV2.NewMockAuthIdentityProvider(ctrl)
			mockAuthIdentityProvider.EXPECT().Identity().Return(nil)
			oauthHandler := v2.NewOAuthHandler(mockAuthIdentityProvider,
				v2.WithCache(v2.NewOauthTokenCache()),
				v2.WithLocker(kitsync.NewPartitionRWLocker()),
				v2.WithStats(stats.Default),
				v2.WithLogger(logger.NewLogger().Child("MockOAuthHandler")),
				v2.WithCpClient(mockCpConnector),
			)
			response, err := oauthHandler.FetchToken(fetchTokenParams)
			require.Error(t, err)
			require.Nil(t, response)
			require.Equal(t, http.StatusInternalServerError, err.StatusCode())
			requireErrorInChain(t, err, "empty secret received from CP")
		})

		t.Run("fetch token function call when cache is empty and cpApiCall returns nil secret", func(t *testing.T) {
			fetchTokenParams := &v2.OAuthTokenParams{
				AccountID:     "123",
				WorkspaceID:   "456",
				DestType:      "testDest",
				DestinationID: Destination.ID,
			}

			ctrl := gomock.NewController(t)
			mockCpConnector := mock_oauthV2.NewMockConnector(ctrl)
			mockCpConnector.EXPECT().CpApiCall(gomock.Any()).Return(http.StatusOK, `{"secret": null}`)
			mockAuthIdentityProvider := mock_oauthV2.NewMockAuthIdentityProvider(ctrl)
			mockAuthIdentityProvider.EXPECT().Identity().Return(nil)
			oauthHandler := v2.NewOAuthHandler(mockAuthIdentityProvider,
				v2.WithCache(v2.NewOauthTokenCache()),
				v2.WithLocker(kitsync.NewPartitionRWLocker()),
				v2.WithStats(stats.Default),
				v2.WithLogger(logger.NewLogger().Child("MockOAuthHandler")),
				v2.WithCpClient(mockCpConnector),
			)
			response, err := oauthHandler.FetchToken(fetchTokenParams)
			require.Error(t, err)
			require.Nil(t, response)
			requireErrorInChain(t, err, "empty secret received from CP")
		})
	})

	t.Run("Test RefreshToken function", func(t *testing.T) {
		t.Run("refreshToken function call when stored cache is same as provided secret", func(t *testing.T) {
			previousSecret := []byte(`{"access_token":"storedAccessToken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken"}`)
			tokenParams := &v2.OAuthTokenParams{
				AccountID:     "123",
				WorkspaceID:   "456",
				DestType:      "testDest",
				DestinationID: Destination.ID,
			}

			ctrl := gomock.NewController(t)
			mockCpConnector := mock_oauthV2.NewMockConnector(ctrl)
			mockCpConnector.EXPECT().CpApiCall(gomock.Any()).Return(http.StatusOK, `{"options":{},"id":"2BFzzzID8kITtU7AxxWtrn9KQQf","createdAt":"2022-06-29T15:34:47.758Z","updatedAt":"2024-02-12T12:18:35.213Z","workspaceId":"1oVajb9QqG50undaAcokNlYyJQa","name":"dummy user","role":"google_adwords_enhanced_conversions_v1","userId":"1oVadeaoGXN2pataEEoeIaXS3bO","metadata":{"userId":"115538421777182389816","displayName":"dummy user","email":"dummy@testmail.com"},"secretVersion":50,"rudderCategory":"destination","secret":{"access_token":"newAccessToken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken"}}`)

			mockAuthIdentityProvider := mock_oauthV2.NewMockAuthIdentityProvider(ctrl)
			mockAuthIdentityProvider.EXPECT().Identity().Return(nil)

			// Invoke code under test
			cache := v2.NewOauthTokenCache()
			oauthHandler := v2.NewOAuthHandler(mockAuthIdentityProvider,
				v2.WithCache(cache),
				v2.WithLocker(kitsync.NewPartitionRWLocker()),
				v2.WithStats(stats.Default),
				v2.WithLogger(logger.NewLogger().Child("MockOAuthHandler")),
				v2.WithCpClient(mockCpConnector),
			)
			storedAuthToken := v2.OAuthToken{
				Secret: previousSecret,
			}
			cache.Store(tokenParams.AccountID, storedAuthToken)
			_, err := oauthHandler.RefreshToken(tokenParams, previousSecret)
			// Assertions
			require.NoError(t, err)
			token, _ := cache.Load(tokenParams.AccountID)
			expectedResponse := v2.OAuthToken{
				Secret:        []byte(`{"access_token":"newAccessToken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken"}`),
				SecretVersion: 50,
			}
			require.Equal(t, expectedResponse, token)
		})

		t.Run("refreshToken function call when stored cache is different as provided secret", func(t *testing.T) {
			previousSecret := []byte(`{"access_token":"providedAccessToken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken"}`)
			refreshTokenParams := &v2.OAuthTokenParams{
				AccountID:     "123",
				WorkspaceID:   "456",
				DestType:      "testDest",
				DestinationID: Destination.ID,
			}

			// Invoke code under test
			cache := v2.NewOauthTokenCache()
			oauthHandler := v2.NewOAuthHandler(nil,
				v2.WithCache(cache),
				v2.WithLocker(kitsync.NewPartitionRWLocker()),
				v2.WithStats(stats.Default),
				v2.WithLogger(logger.NewLogger().Child("MockOAuthHandler")),
			)
			storedAuthToken := v2.OAuthToken{
				Secret: []byte(`{"access_token":"storedAccessToken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken"}`),
			}
			cache.Store(refreshTokenParams.AccountID, storedAuthToken)
			expectedResponse := v2.OAuthToken{
				Secret: []byte(`{"access_token":"storedAccessToken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken"}`),
			}
			_, err := oauthHandler.RefreshToken(refreshTokenParams, previousSecret)
			token, _ := cache.Load(refreshTokenParams.AccountID)
			require.NoError(t, err)
			require.Equal(t, expectedResponse, token)
		})

		t.Run("refreshToken function call when stored cache is same as provided secret but the cpApiCall failed with ref_token_invalid_grant error", func(t *testing.T) {
			previousSecret := []byte(`{"access_token":"storedAccessToken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken"}`)
			refreshTokenParams := &v2.OAuthTokenParams{
				AccountID:     "123",
				WorkspaceID:   "456",
				DestType:      "testDest",
				DestinationID: Destination.ID,
			}

			ctrl := gomock.NewController(t)
			mockCpConnector := mock_oauthV2.NewMockConnector(ctrl)

			mockCpConnector.EXPECT().CpApiCall(gomock.Any()).Return(http.StatusBadRequest, `{
				"body":{
				  "code":"ref_token_invalid_grant",
				  "message":"invalid_grant error, refresh token has expired or revoked"
				}
			  }`)

			mockAuthIdentityProvider := mock_oauthV2.NewMockAuthIdentityProvider(ctrl)
			mockAuthIdentityProvider.EXPECT().Identity().Return(nil)

			// Invoke code under test
			cache := v2.NewOauthTokenCache()
			oauthHandler := v2.NewOAuthHandler(mockAuthIdentityProvider,
				v2.WithCache(cache),
				v2.WithLocker(kitsync.NewPartitionRWLocker()),
				v2.WithStats(stats.Default),
				v2.WithLogger(logger.NewLogger().Child("MockOAuthHandler")),
				v2.WithCpClient(mockCpConnector),
			)
			storedAuthToken := v2.OAuthToken{
				Secret: []byte(`{"access_token":"storedAccessToken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken"}`),
			}
			cache.Store(refreshTokenParams.AccountID, storedAuthToken)
			response, err := oauthHandler.RefreshToken(refreshTokenParams, previousSecret)
			require.Error(t, err)
			require.Nil(t, response)
			require.Equal(t, http.StatusBadRequest, err.StatusCode())
			require.ErrorIs(t, err, common.ErrInvalidGrant)
		})
		t.Run("refreshToken function call when stored cache is same as provided secret and cpApiCall returns a failed response due to config backend service is not available", func(t *testing.T) {
			previousSecret := []byte(`{"access_token":"storedAccessToken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken"}`)
			refreshTokenParams := &v2.OAuthTokenParams{
				AccountID:     "123",
				WorkspaceID:   "456",
				DestType:      "testDest",
				DestinationID: Destination.ID,
			}

			ctrl := gomock.NewController(t)
			mockHttpClient := mockhttpclient.NewMockHttpClient(ctrl)
			mockHttpClient.EXPECT().Do(gomock.Any()).Return(&http.Response{}, &net.OpError{
				Op:     "mock",
				Net:    "mock",
				Source: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 1234},
				Addr:   &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 12340},
				Err:    &os.SyscallError{Syscall: "read", Err: syscall.ECONNREFUSED},
			})
			cpConnector := controlplane.NewConnector(
				config.Default,
				controlplane.WithClient(mockHttpClient),
				controlplane.WithStats(stats.Default),
			)

			mockAuthIdentityProvider := mock_oauthV2.NewMockAuthIdentityProvider(ctrl)
			mockAuthIdentityProvider.EXPECT().Identity().Return(&testutils.BasicAuthMock{})
			oauthHandler := v2.NewOAuthHandler(mockAuthIdentityProvider,
				v2.WithCache(v2.NewOauthTokenCache()),
				v2.WithLocker(kitsync.NewPartitionRWLocker()),
				v2.WithStats(stats.Default),
				v2.WithLogger(logger.NewLogger().Child("MockOAuthHandler")),
				v2.WithCpClient(cpConnector),
			)
			response, err := oauthHandler.RefreshToken(refreshTokenParams, previousSecret)
			require.Error(t, err)
			require.Nil(t, response)
			require.Equal(t, http.StatusInternalServerError, err.StatusCode())
			var target *v2.TypeMessageError
			require.ErrorAs(t, err, &target)
			require.Equal(t, &v2.TypeMessageError{Type: "econnrefused", Message: "mock mock 127.0.0.1:1234->127.0.0.1:12340: read: connection refused"}, target)
			require.NotErrorIs(t, err, common.ErrInvalidGrant)
		})

		t.Run("refreshToken function call when stored cache is same as provided secret and cpApiCall returns a failed response due to config backend service is failed due to timeout", func(t *testing.T) {
			previousSecret := []byte(`{"access_token":"storedAccessToken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken"}`)
			refreshTokenParams := &v2.OAuthTokenParams{
				AccountID:     "123",
				WorkspaceID:   "456",
				DestType:      "testDest",
				DestinationID: Destination.ID,
			}

			ctrl := gomock.NewController(t)
			mockHttpClient := mockhttpclient.NewMockHttpClient(ctrl)
			mockHttpClient.EXPECT().Do(gomock.Any()).Return(&http.Response{}, &net.OpError{
				Op:     "mock",
				Net:    "mock",
				Source: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 1234},
				Addr:   &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 12340},
				Err:    &os.SyscallError{Syscall: "read", Err: syscall.ETIMEDOUT},
			})
			cpConnector := controlplane.NewConnector(
				config.Default,
				controlplane.WithClient(mockHttpClient),
				controlplane.WithStats(stats.Default),
			)

			mockAuthIdentityProvider := mock_oauthV2.NewMockAuthIdentityProvider(ctrl)
			mockAuthIdentityProvider.EXPECT().Identity().Return(&testutils.BasicAuthMock{})
			oauthHandler := v2.NewOAuthHandler(mockAuthIdentityProvider,
				v2.WithCache(v2.NewOauthTokenCache()),
				v2.WithLocker(kitsync.NewPartitionRWLocker()),
				v2.WithStats(stats.Default),
				v2.WithLogger(logger.NewLogger().Child("MockOAuthHandler")),
				v2.WithCpClient(cpConnector),
			)
			response, err := oauthHandler.RefreshToken(refreshTokenParams, previousSecret)
			require.Error(t, err)
			require.Nil(t, response)
			require.Equal(t, http.StatusInternalServerError, err.StatusCode())
			var target *v2.TypeMessageError
			require.ErrorAs(t, err, &target)
			require.Equal(t, &v2.TypeMessageError{Type: "timeout", Message: "mock mock 127.0.0.1:1234->127.0.0.1:12340: read: " + timeoutError}, target)
			require.NotErrorIs(t, err, common.ErrInvalidGrant)
		})

		t.Run("refreshToken function call when stored cache is same as provided secret and cpApiCall returns a failed response because of faulty implementation in some downstream service", func(t *testing.T) {
			previousSecret := []byte(`{"access_token":"storedAccessToken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken"}`)
			refreshTokenParams := &v2.OAuthTokenParams{
				AccountID:     "123",
				WorkspaceID:   "456",
				DestType:      "testDest",
				DestinationID: Destination.ID,
			}

			ctrl := gomock.NewController(t)
			mockHttpClient := mockhttpclient.NewMockHttpClient(ctrl)
			cpResponseString := `{
				"status": 500,
				"code": "INVALID_REFRESH_RESPONSE",
				"body": {
					"code": "INVALID_REFRESH_RESPONSE",
					"message": "Missing required token fields in refresh response"
				},
				"access_token":"storedAccessToken",
				"refresh_token":"dummyRefreshToken",
				"developer_token":"dummyDeveloperToken"
			}`
			mockHttpClient.EXPECT().Do(gomock.Any()).Return(&http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(bytes.NewBufferString(cpResponseString)),
			}, nil)
			cpConnector := controlplane.NewConnector(
				config.Default,
				controlplane.WithClient(mockHttpClient),
				controlplane.WithStats(stats.Default),
			)

			mockAuthIdentityProvider := mock_oauthV2.NewMockAuthIdentityProvider(ctrl)
			mockAuthIdentityProvider.EXPECT().Identity().Return(&testutils.BasicAuthMock{})
			oauthHandler := v2.NewOAuthHandler(mockAuthIdentityProvider,
				v2.WithCache(v2.NewOauthTokenCache()),
				v2.WithLocker(kitsync.NewPartitionRWLocker()),
				v2.WithStats(stats.Default),
				v2.WithLogger(logger.NewLogger().Child("MockOAuthHandler")),
				v2.WithCpClient(cpConnector),
			)
			response, err := oauthHandler.RefreshToken(refreshTokenParams, previousSecret)
			require.Error(t, err)
			require.Nil(t, response)
			require.Equal(t, http.StatusInternalServerError, err.StatusCode())
			var target *v2.TypeMessageError
			require.ErrorAs(t, err, &target)
			require.Equal(t, &v2.TypeMessageError{Type: "INVALID_REFRESH_RESPONSE", Message: "Missing required token fields in refresh response"}, target)
			require.NotErrorIs(t, err, common.ErrInvalidGrant)
		})

		t.Run("refreshToken function call when stored cache is same as provided secret and cpApiCall returns a failed response because of invalid refresh response without message", func(t *testing.T) {
			previousSecret := []byte(`{"access_token":"storedAccessToken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken"}`)
			refreshTokenParams := &v2.OAuthTokenParams{
				AccountID:     "123",
				WorkspaceID:   "456",
				DestType:      "testDest",
				DestinationID: Destination.ID,
			}

			ctrl := gomock.NewController(t)
			mockHttpClient := mockhttpclient.NewMockHttpClient(ctrl)
			cpResponseString := `{
				"status": 500,
				"code": "INVALID_REFRESH_RESPONSE",
				"body": {
					"code": "INVALID_REFRESH_RESPONSE"
				},
				"access_token":"storedAccessToken",
				"refresh_token":"dummyRefreshToken",
				"developer_token":"dummyDeveloperToken"
			}`
			mockHttpClient.EXPECT().Do(gomock.Any()).Return(&http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(bytes.NewBufferString(cpResponseString)),
			}, nil)
			cpConnector := controlplane.NewConnector(
				config.Default,
				controlplane.WithClient(mockHttpClient),
				controlplane.WithStats(stats.Default),
			)

			mockAuthIdentityProvider := mock_oauthV2.NewMockAuthIdentityProvider(ctrl)
			mockAuthIdentityProvider.EXPECT().Identity().Return(&testutils.BasicAuthMock{})
			oauthHandler := v2.NewOAuthHandler(mockAuthIdentityProvider,
				v2.WithCache(v2.NewOauthTokenCache()),
				v2.WithLocker(kitsync.NewPartitionRWLocker()),
				v2.WithStats(stats.Default),
				v2.WithLogger(logger.NewLogger().Child("MockOAuthHandler")),
				v2.WithCpClient(cpConnector),
			)
			response, err := oauthHandler.RefreshToken(refreshTokenParams, previousSecret)
			require.Error(t, err)
			require.Nil(t, response)
			require.Equal(t, http.StatusInternalServerError, err.StatusCode())
			var target *v2.TypeMessageError
			require.ErrorAs(t, err, &target)
			require.Equal(t, &v2.TypeMessageError{Type: "INVALID_REFRESH_RESPONSE", Message: "invalid auth token refresh response"}, target)
			require.NotErrorIs(t, err, common.ErrInvalidGrant)
		})

		t.Run("refreshToken function call when stored cache is same as provided secret and cpApiCall returns a failed response because of invalid refresh response without message", func(t *testing.T) {
			previousSecret := []byte(`{"access_token":"storedAccessToken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken"}`)
			refreshTokenParams := &v2.OAuthTokenParams{
				AccountID:     "123",
				WorkspaceID:   "456",
				DestType:      "testDest",
				DestinationID: Destination.ID,
			}

			ctrl := gomock.NewController(t)
			mockHttpClient := mockhttpclient.NewMockHttpClient(ctrl)
			cpResponseString := fmt.Sprintf(`{
				"status": 500,
				"code": "%[1]s",
				"body": {
					"code": "%[1]s"
				},
				"access_token":"storedAccessToken",
				"refresh_token":"dummyRefreshToken",
				"developer_token":"dummyDeveloperToken"
			}`, common.RefTokenInvalidGrant)
			mockHttpClient.EXPECT().Do(gomock.Any()).Return(&http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(bytes.NewBufferString(cpResponseString)),
			}, nil)
			cpConnector := controlplane.NewConnector(
				config.Default,
				controlplane.WithClient(mockHttpClient),
				controlplane.WithStats(stats.Default),
			)

			mockAuthIdentityProvider := mock_oauthV2.NewMockAuthIdentityProvider(ctrl)
			mockAuthIdentityProvider.EXPECT().Identity().Return(&testutils.BasicAuthMock{})
			oauthHandler := v2.NewOAuthHandler(mockAuthIdentityProvider,
				v2.WithCache(v2.NewOauthTokenCache()),
				v2.WithLocker(kitsync.NewPartitionRWLocker()),
				v2.WithStats(stats.Default),
				v2.WithLogger(logger.NewLogger().Child("MockOAuthHandler")),
				v2.WithCpClient(cpConnector),
			)
			response, err := oauthHandler.RefreshToken(refreshTokenParams, previousSecret)
			require.Error(t, err)
			require.Nil(t, response)
			require.Equal(t, http.StatusBadRequest, err.StatusCode())
			require.ErrorIs(t, err, common.ErrInvalidGrant)
		})
	})

	t.Run("Test FetchToken with multiple go routines", func(t *testing.T) {
		t.Run("fetch token function call when cache is not empty", func(t *testing.T) {
			fetchTokenParams := &v2.OAuthTokenParams{
				AccountID:     "123",
				WorkspaceID:   "456",
				DestType:      "testDest",
				DestinationID: Destination.ID,
			}
			// Invoke code under test
			expectedResponse := []byte(`{"access_token":"new acceess token","refresh_token":"dummyAccessToken","developer_token":"dummydeveloperToken"}`)

			ctrl := gomock.NewController(t)
			mockCpConnector := mock_oauthV2.NewMockConnector(ctrl)
			mockCpConnector.EXPECT().CpApiCall(gomock.Any()).Times(0)

			cache := v2.NewOauthTokenCache()
			oauthHandler := v2.NewOAuthHandler(nil,
				v2.WithCache(cache),
				v2.WithLocker(kitsync.NewPartitionRWLocker()),
				v2.WithLogger(logger.NewLogger().Child("MockOAuthHandler")),
				v2.WithCpClient(mockCpConnector),
			)
			storedAuthToken := v2.OAuthToken{
				Secret: []byte(`{"access_token":"new acceess token","refresh_token":"dummyAccessToken","developer_token":"dummydeveloperToken"}`),
			}
			cache.Store(fetchTokenParams.AccountID, storedAuthToken)
			wg := sync.WaitGroup{}
			for range 20 {
				wg.Go(func() {
					time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
					response, err := oauthHandler.FetchToken(fetchTokenParams)
					// Assertions
					assert.NoError(t, err)
					assert.JSONEq(t, string(expectedResponse), string(response))
					token, _ := cache.Load(fetchTokenParams.AccountID)
					assert.JSONEq(t, string(expectedResponse), string(token.Secret))
				})
			}
			wg.Wait()

			// Invoke code under test
		})

		t.Run("fetch token function call when cache is empty", func(t *testing.T) {
			fetchTokenParams := &v2.OAuthTokenParams{
				AccountID:     "123",
				WorkspaceID:   "456",
				DestType:      "testDest",
				DestinationID: Destination.ID,
			}
			// Invoke code under test
			expectedResponse := []byte(`{"access_token":"new 1234 acceess token","refresh_token":"dummyAccessToken","developer_token":"dummydeveloperToken"}`)
			ctrl := gomock.NewController(t)
			mockCpConnector := mock_oauthV2.NewMockConnector(ctrl)
			mockCpConnector.EXPECT().CpApiCall(gomock.Any()).Return(http.StatusOK, `{"options":{},"id":"2BFzzzID8kITtU7AxxWtrn9KQQf","createdAt":"2022-06-29T15:34:47.758Z","updatedAt":"2024-02-12T12:18:35.213Z","workspaceId":"1oVajb9QqG50undaAcokNlYyJQa","name":"dummy user","role":"google_adwords_enhanced_conversions_v1","userId":"1oVadeaoGXN2pataEEoeIaXS3bO","metadata":{"userId":"115538421777182389816","displayName":"dummy user","email":""},"secretVersion":50,"rudderCategory":"destination","secret":{"access_token":"new 1234 acceess token","refresh_token":"dummyAccessToken","developer_token":"dummydeveloperToken"}}`).Times(1)
			mockAuthIdentityProvider := mock_oauthV2.NewMockAuthIdentityProvider(ctrl)
			mockAuthIdentityProvider.EXPECT().Identity().Return(nil)
			cache := v2.NewOauthTokenCache()
			oauthHandler := v2.NewOAuthHandler(mockAuthIdentityProvider,
				v2.WithCache(cache),
				v2.WithLocker(kitsync.NewPartitionRWLocker()),
				v2.WithStats(stats.Default),
				v2.WithLogger(logger.NewLogger().Child("MockOAuthHandler")),
				v2.WithCpClient(mockCpConnector),
			)
			wg := sync.WaitGroup{}
			for range 200 {
				wg.Go(func() {
					response, err := oauthHandler.FetchToken(fetchTokenParams)
					// Assertions
					assert.NoError(t, err)
					assert.JSONEq(t, string(expectedResponse), string(response))
					token, _ := cache.Load(fetchTokenParams.AccountID)
					assert.JSONEq(t, string(expectedResponse), string(token.Secret))
					assert.Zero(t, token.ExpirationDate)
				})
			}
			wg.Wait()

			// Invoke code under test
		})
	})
	t.Run("Test RefreshToken with multiple go routines", func(t *testing.T) {
		t.Run("refresh token function call when stored cache is same as provided secret", func(t *testing.T) {
			previousSecret := []byte(`{"access_token":"storedAccessToken","refresh_token":"dummyAccessToken","developer_token":"dummyDeveloperToken"}`)
			refreshTokenParams := &v2.OAuthTokenParams{
				AccountID:     "123",
				WorkspaceID:   "456",
				DestType:      "testDest",
				DestinationID: Destination.ID,
			}
			// Invoke code under test
			expectedResponse := []byte(`{"access_token":"new acceess token","refresh_token":"dummyAccessToken","developer_token":"dummydeveloperToken"}`)
			ctrl := gomock.NewController(t)
			mockCpConnector := mock_oauthV2.NewMockConnector(ctrl)
			mockCpConnector.EXPECT().CpApiCall(gomock.Any()).Return(http.StatusOK, `{"options":{},"id":"2BFzzzID8kITtU7AxxWtrn9KQQf","createdAt":"2022-06-29T15:34:47.758Z","updatedAt":"2024-02-12T12:18:35.213Z","workspaceId":"1oVajb9QqG50undaAcokNlYyJQa","name":"dummy user","role":"google_adwords_enhanced_conversions_v1","userId":"1oVadeaoGXN2pataEEoeIaXS3bO","metadata":{"userId":"115538421777182389816","displayName":"dummy user","email":""},"secretVersion":50,"rudderCategory":"destination","secret":{"access_token":"new acceess token","refresh_token":"dummyAccessToken","developer_token":"dummydeveloperToken"}}`).Times(1)
			mockAuthIdentityProvider := mock_oauthV2.NewMockAuthIdentityProvider(ctrl)
			mockAuthIdentityProvider.EXPECT().Identity().Return(nil)
			cache := v2.NewOauthTokenCache()
			oauthHandler := v2.NewOAuthHandler(mockAuthIdentityProvider,
				v2.WithCache(cache),
				v2.WithLocker(kitsync.NewPartitionRWLocker()),
				v2.WithStats(stats.Default),
				v2.WithLogger(logger.NewLogger().Child("MockOAuthHandler")),
				v2.WithCpClient(mockCpConnector),
			)

			storedAuthToken := v2.OAuthToken{
				Secret: []byte(`{"access_token":"storedAccessToken","refresh_token":"dummyAccessToken","developer_token":"dummyDeveloperToken"}`),
			}
			cache.Store(refreshTokenParams.AccountID, storedAuthToken)
			wg := sync.WaitGroup{}
			for range 20 {
				wg.Go(func() {
					response, err := oauthHandler.RefreshToken(refreshTokenParams, previousSecret)
					// Assertions
					assert.NoError(t, err)
					assert.JSONEq(t, string(expectedResponse), string(response))
					token, _ := cache.Load(refreshTokenParams.AccountID)
					assert.JSONEq(t, string(expectedResponse), string(token.Secret))
					assert.Zero(t, token.ExpirationDate)
				})
			}
			wg.Wait()
		})

		t.Run("refresh token function call when stored cache is same as provided secret with sequential and concurrent access", func(t *testing.T) {
			previousSecret := []byte(`{"access_token":"storedAccessToken","refresh_token":"dummyAccessToken","developer_token":"dummyDeveloperToken"}`)
			refreshTokenParams := &v2.OAuthTokenParams{
				AccountID:     "123",
				WorkspaceID:   "456",
				DestType:      "testDest",
				DestinationID: Destination.ID,
			}
			// Invoke code under test
			expectedResponse := []byte(`{"access_token":"new acceess token","refresh_token":"dummyAccessToken","developer_token":"dummydeveloperToken"}`)
			ctrl := gomock.NewController(t)
			mockCpConnector := mock_oauthV2.NewMockConnector(ctrl)
			mockCpConnector.EXPECT().CpApiCall(gomock.Any()).Return(http.StatusOK, `{"options":{},"id":"2BFzzzID8kITtU7AxxWtrn9KQQf","createdAt":"2022-06-29T15:34:47.758Z","updatedAt":"2024-02-12T12:18:35.213Z","workspaceId":"1oVajb9QqG50undaAcokNlYyJQa","name":"dummy user","role":"google_adwords_enhanced_conversions_v1","userId":"1oVadeaoGXN2pataEEoeIaXS3bO","metadata":{"userId":"115538421777182389816","displayName":"dummy user","email":""},"secretVersion":50,"rudderCategory":"destination","secret":{"access_token":"new acceess token","refresh_token":"dummyAccessToken","developer_token":"dummydeveloperToken"}}`).Times(1)
			mockAuthIdentityProvider := mock_oauthV2.NewMockAuthIdentityProvider(ctrl)
			mockAuthIdentityProvider.EXPECT().Identity().Return(nil)
			cache := v2.NewOauthTokenCache()
			oauthHandler := v2.NewOAuthHandler(mockAuthIdentityProvider,
				v2.WithCache(cache),
				v2.WithLocker(kitsync.NewPartitionRWLocker()),
				v2.WithStats(stats.Default),
				v2.WithLogger(logger.NewLogger().Child("MockOAuthHandler")),
				v2.WithCpClient(mockCpConnector),
			)

			storedAuthToken := v2.OAuthToken{
				Secret: []byte(`{"access_token":"storedAccessToken","refresh_token":"dummyAccessToken","developer_token":"dummyDeveloperToken"}`),
			}
			cache.Store(refreshTokenParams.AccountID, storedAuthToken)
			wg := sync.WaitGroup{}

			for range 20 {
				response, err := oauthHandler.RefreshToken(refreshTokenParams, previousSecret)
				// Assertions
				require.NoError(t, err)
				require.JSONEq(t, string(expectedResponse), string(response))
				token, _ := cache.Load(refreshTokenParams.AccountID)
				require.JSONEq(t, string(expectedResponse), string(token.Secret))
				require.Zero(t, token.ExpirationDate)
			}

			for range 20 {
				wg.Go(func() {
					response, err := oauthHandler.RefreshToken(refreshTokenParams, previousSecret)
					// Assertions
					assert.NoError(t, err)
					assert.JSONEq(t, string(expectedResponse), string(response))
					token, _ := cache.Load(refreshTokenParams.AccountID)
					assert.JSONEq(t, string(expectedResponse), string(token.Secret))
					assert.Zero(t, token.ExpirationDate)
				})
			}
			wg.Wait()
		})
	})
}

// requireErrorInChain requires an error in err's chain whose message is exactly msg.
func requireErrorInChain(t *testing.T, err error, msg string) {
	t.Helper()
	for e := err; e != nil; e = errors.Unwrap(e) {
		if e.Error() == msg {
			return
		}
	}
	require.Failf(t, "no error in the chain has the expected message", "want %q in the chain of %q", msg, err)
}
