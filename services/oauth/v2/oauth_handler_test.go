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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	kitsync "github.com/rudderlabs/rudder-go-kit/sync"
	mockhttpclient "github.com/rudderlabs/rudder-server/mocks/services/oauth/v2/http"
	mock_oauthV2 "github.com/rudderlabs/rudder-server/mocks/services/oauthV2"
	v2 "github.com/rudderlabs/rudder-server/services/oauth/v2"
	"github.com/rudderlabs/rudder-server/services/oauth/v2/common"
	"github.com/rudderlabs/rudder-server/services/oauth/v2/controlplane"
	testutils "github.com/rudderlabs/rudder-server/utils/tests"
)

var Destination = &v2.DestinationInfo{
	DestType:         "testDest",
	ID:               "Destination123",
	WorkspaceID:      "456",
	DefinitionConfig: map[string]interface{}{},
	Config:           map[string]interface{}{},
}

var _ = Describe("Oauth", func() {
	timeoutError := "connection timed out"
	if runtime.GOOS == "darwin" {
		timeoutError = "operation timed out"
	}
	Describe("Test FetchToken function", func() {
		It("fetch token function call when cache is empty", func() {
			fetchTokenParams := &v2.OAuthTokenParams{
				AccountID:     "123",
				WorkspaceID:   "456",
				DestType:      "testDest",
				DestinationID: Destination.ID,
			}

			ctrl := gomock.NewController(GinkgoT())
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
			Expect(err).To(BeNil())
			Expect(response).To(MatchJSON(expectedResponse))
			token, _ := cache.Load(fetchTokenParams.AccountID)
			Expect(token.Secret).To(MatchJSON(expectedResponse))
		})

		It("fetch token function call when cache is not empty and token is not expired", func() {
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
			Expect(err).To(BeNil())
			Expect(response).To(MatchJSON(expectedResponse))
			token, _ := cache.Load(fetchTokenParams.AccountID)
			// We are checking if the token is updated in the cache or not
			Expect(token).To(Equal(storedAuthToken))
		})

		It("fetch token function call when cache is not empty and token is expired", func() {
			fetchTokenParams := &v2.OAuthTokenParams{
				AccountID:     "123",
				WorkspaceID:   "456",
				DestType:      "testDest",
				DestinationID: Destination.ID,
			}

			ctrl := gomock.NewController(GinkgoT())
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
			Expect(err).To(BeNil())
			Expect(response).To(MatchJSON(expectedResponse))
			token, _ := cache.Load(fetchTokenParams.AccountID)
			// We are checking if the token is updated in the cache or not
			Expect(token).NotTo(Equal(storedAuthToken))
		})

		It("fetch token function call is successful and token is returned with 'expirationDate', should contain ExpirationDate information in AccountSecret{}", func() {
			fetchTokenParams := &v2.OAuthTokenParams{
				AccountID:     "123",
				WorkspaceID:   "456",
				DestType:      "testDest",
				DestinationID: Destination.ID,
			}

			ctrl := gomock.NewController(GinkgoT())
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
			Expect(err).To(BeNil())
			oauthToken, _ := cache.Load(fetchTokenParams.AccountID)
			// We are checking if the token is updated in the cache or not
			Expect(oauthToken.ExpirationDate).To(Equal(expectedExpirationDate))
			Expect(oauthToken.Secret).To(MatchJSON(expectedResponse))
		})

		It("fetch token function call when cache is empty and cpApiCall returns empty response", func() {
			fetchTokenParams := &v2.OAuthTokenParams{
				AccountID:     "123",
				WorkspaceID:   "456",
				DestType:      "testDest",
				DestinationID: Destination.ID,
			}

			ctrl := gomock.NewController(GinkgoT())
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
			Expect(err).ToNot(BeNil())
			Expect(err.StatusCode()).To(Equal(http.StatusInternalServerError))
			Expect(response).To(BeNil())
			Expect(err).To(MatchError(errors.New("empty secret in response from control plane")))
		})

		It("fetch token function call when cache is empty and cpApiCall returns a failed response", func() {
			fetchTokenParams := &v2.OAuthTokenParams{
				AccountID:     "123",
				WorkspaceID:   "456",
				DestType:      "testDest",
				DestinationID: Destination.ID,
			}

			ctrl := gomock.NewController(GinkgoT())
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
			Expect(err).ToNot(BeNil())
			Expect(response).To(BeNil())
			Expect(err.StatusCode()).To(Equal(http.StatusBadRequest))
			Expect(err).To(MatchError(common.ErrInvalidGrant))
		})
		It("fetch token function call when cache is empty and cpApiCall returns a failed response due to config backend service is not available", func() {
			fetchTokenParams := &v2.OAuthTokenParams{
				AccountID:     "123",
				WorkspaceID:   "456",
				DestType:      "testDest",
				DestinationID: Destination.ID,
			}

			ctrl := gomock.NewController(GinkgoT())
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
			Expect(err).ToNot(BeNil())
			Expect(response).To(BeNil())
			Expect(err.StatusCode()).To(Equal(http.StatusInternalServerError))
			Expect(err).To(MatchError(&v2.TypeMessageError{Type: "econnrefused", Message: "mock mock 127.0.0.1:1234->127.0.0.1:12340: read: connection refused"}))
		})
		It("fetch token function call when cache is empty and cpApiCall returns a failed response due to config backend call failed due to timeout error", func() {
			fetchTokenParams := &v2.OAuthTokenParams{
				AccountID:     "123",
				WorkspaceID:   "456",
				DestType:      "testDest",
				DestinationID: Destination.ID,
			}

			ctrl := gomock.NewController(GinkgoT())
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
			Expect(err).ToNot(BeNil())
			Expect(response).To(BeNil())
			Expect(err.StatusCode()).To(Equal(http.StatusInternalServerError))
			Expect(err).To(MatchError(&v2.TypeMessageError{Type: "timeout", Message: "mock mock 127.0.0.1:1234->127.0.0.1:12340: read: " + timeoutError}))
		})

		It("fetch token function call when cache is empty and cpApiCall returns empty secret", func() {
			fetchTokenParams := &v2.OAuthTokenParams{
				AccountID:     "123",
				WorkspaceID:   "456",
				DestType:      "testDest",
				DestinationID: Destination.ID,
			}

			ctrl := gomock.NewController(GinkgoT())
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
			Expect(err).ToNot(BeNil())
			Expect(response).To(BeNil())
			Expect(err.StatusCode()).To(Equal(http.StatusInternalServerError))
			Expect(err).To(MatchError(errors.New("empty secret received from CP")))
		})

		It("fetch token function call when cache is empty and cpApiCall returns empty string", func() {
			fetchTokenParams := &v2.OAuthTokenParams{
				AccountID:     "123",
				WorkspaceID:   "456",
				DestType:      "testDest",
				DestinationID: Destination.ID,
			}

			ctrl := gomock.NewController(GinkgoT())
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
			Expect(err).ToNot(BeNil())
			Expect(response).To(BeNil())
			Expect(err.StatusCode()).To(Equal(http.StatusInternalServerError))
			Expect(err).To(MatchError(errors.New("empty secret received from CP")))
		})

		It("fetch token function call when cache is empty and cpApiCall returns nil secret", func() {
			fetchTokenParams := &v2.OAuthTokenParams{
				AccountID:     "123",
				WorkspaceID:   "456",
				DestType:      "testDest",
				DestinationID: Destination.ID,
			}

			ctrl := gomock.NewController(GinkgoT())
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
			Expect(err).ToNot(BeNil())
			Expect(response).To(BeNil())
			Expect(err).To(MatchError(errors.New("empty secret received from CP")))
		})
	})

	Describe("Test RefreshToken function", func() {
		It("refreshToken function call when stored cache is same as provided secret", func() {
			previousSecret := []byte(`{"access_token":"storedAccessToken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken"}`)
			tokenParams := &v2.OAuthTokenParams{
				AccountID:     "123",
				WorkspaceID:   "456",
				DestType:      "testDest",
				DestinationID: Destination.ID,
			}

			ctrl := gomock.NewController(GinkgoT())
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
			Expect(err).To(BeNil())
			token, _ := cache.Load(tokenParams.AccountID)
			expectedResponse := v2.OAuthToken{
				Secret: []byte(`{"access_token":"newAccessToken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken"}`),
			}
			Expect(token).To(Equal(expectedResponse))
		})

		It("refreshToken function call when stored cache is different as provided secret", func() {
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
			Expect(err).To(BeNil())
			Expect(token).To(Equal(expectedResponse))
		})

		It("refreshToken function call when stored cache is same as provided secret but the cpApiCall failed with ref_token_invalid_grant error", func() {
			previousSecret := []byte(`{"access_token":"storedAccessToken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken"}`)
			refreshTokenParams := &v2.OAuthTokenParams{
				AccountID:     "123",
				WorkspaceID:   "456",
				DestType:      "testDest",
				DestinationID: Destination.ID,
			}

			ctrl := gomock.NewController(GinkgoT())
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
			Expect(err).ToNot(BeNil())
			Expect(response).To(BeNil())
			Expect(err.StatusCode()).To(Equal(http.StatusBadRequest))
			Expect(err).To(MatchError(common.ErrInvalidGrant))
		})
		It("refreshToken function call when stored cache is same as provided secret and cpApiCall returns a failed response due to config backend service is not available", func() {
			previousSecret := []byte(`{"access_token":"storedAccessToken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken"}`)
			refreshTokenParams := &v2.OAuthTokenParams{
				AccountID:     "123",
				WorkspaceID:   "456",
				DestType:      "testDest",
				DestinationID: Destination.ID,
			}

			ctrl := gomock.NewController(GinkgoT())
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
			Expect(err).ToNot(BeNil())
			Expect(response).To(BeNil())
			Expect(err.StatusCode()).To(Equal(http.StatusInternalServerError))
			Expect(err).To(MatchError(&v2.TypeMessageError{Type: "econnrefused", Message: "mock mock 127.0.0.1:1234->127.0.0.1:12340: read: connection refused"}))
		})

		It("refreshToken function call when stored cache is same as provided secret and cpApiCall returns a failed response due to config backend service is failed due to timeout", func() {
			previousSecret := []byte(`{"access_token":"storedAccessToken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken"}`)
			refreshTokenParams := &v2.OAuthTokenParams{
				AccountID:     "123",
				WorkspaceID:   "456",
				DestType:      "testDest",
				DestinationID: Destination.ID,
			}

			ctrl := gomock.NewController(GinkgoT())
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
			Expect(err).ToNot(BeNil())
			Expect(response).To(BeNil())
			Expect(err.StatusCode()).To(Equal(http.StatusInternalServerError))
			Expect(err).To(MatchError(&v2.TypeMessageError{Type: "timeout", Message: "mock mock 127.0.0.1:1234->127.0.0.1:12340: read: " + timeoutError}))
		})

		It("refreshToken function call when stored cache is same as provided secret and cpApiCall returns a failed response because of faulty implementation in some downstream service", func() {
			previousSecret := []byte(`{"access_token":"storedAccessToken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken"}`)
			refreshTokenParams := &v2.OAuthTokenParams{
				AccountID:     "123",
				WorkspaceID:   "456",
				DestType:      "testDest",
				DestinationID: Destination.ID,
			}

			ctrl := gomock.NewController(GinkgoT())
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
			Expect(err).ToNot(BeNil())
			Expect(response).To(BeNil())
			Expect(err.StatusCode()).To(Equal(http.StatusInternalServerError))
			Expect(err).To(MatchError(&v2.TypeMessageError{Type: "INVALID_REFRESH_RESPONSE", Message: "Missing required token fields in refresh response"}))
		})

		It("refreshToken function call when stored cache is same as provided secret and cpApiCall returns a failed response because of invalid refresh response without message", func() {
			previousSecret := []byte(`{"access_token":"storedAccessToken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken"}`)
			refreshTokenParams := &v2.OAuthTokenParams{
				AccountID:     "123",
				WorkspaceID:   "456",
				DestType:      "testDest",
				DestinationID: Destination.ID,
			}

			ctrl := gomock.NewController(GinkgoT())
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
			Expect(err).ToNot(BeNil())
			Expect(response).To(BeNil())
			Expect(err.StatusCode()).To(Equal(http.StatusInternalServerError))
			Expect(err).To(MatchError(&v2.TypeMessageError{Type: "INVALID_REFRESH_RESPONSE", Message: "invalid refresh response"}))
		})

		It("refreshToken function call when stored cache is same as provided secret and cpApiCall returns a failed response because of invalid refresh response without message", func() {
			previousSecret := []byte(`{"access_token":"storedAccessToken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken"}`)
			refreshTokenParams := &v2.OAuthTokenParams{
				AccountID:     "123",
				WorkspaceID:   "456",
				DestType:      "testDest",
				DestinationID: Destination.ID,
			}

			ctrl := gomock.NewController(GinkgoT())
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
			Expect(err).ToNot(BeNil())
			Expect(response).To(BeNil())
			Expect(err.StatusCode()).To(Equal(http.StatusBadRequest))
			Expect(err).To(MatchError(common.ErrInvalidGrant))
		})
	})

	Describe("Test FetchToken with multiple go routines", func() {
		It("fetch token function call when cache is not empty", func() {
			fetchTokenParams := &v2.OAuthTokenParams{
				AccountID:     "123",
				WorkspaceID:   "456",
				DestType:      "testDest",
				DestinationID: Destination.ID,
			}
			// Invoke code under test
			expectedResponse := []byte(`{"access_token":"new acceess token","refresh_token":"dummyAccessToken","developer_token":"dummydeveloperToken"}`)

			ctrl := gomock.NewController(GinkgoT())
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
				wg.Add(1)
				go func() {
					defer wg.Done()
					time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
					response, err := oauthHandler.FetchToken(fetchTokenParams)
					// Assertions
					Expect(err).To(BeNil())
					Expect(response).To(MatchJSON(expectedResponse))
					token, _ := cache.Load(fetchTokenParams.AccountID)
					Expect(token.Secret).To(MatchJSON(expectedResponse))
				}()
			}
			wg.Wait()

			// Invoke code under test
		})

		It("fetch token function call when cache is empty", func() {
			fetchTokenParams := &v2.OAuthTokenParams{
				AccountID:     "123",
				WorkspaceID:   "456",
				DestType:      "testDest",
				DestinationID: Destination.ID,
			}
			// Invoke code under test
			expectedResponse := []byte(`{"access_token":"new 1234 acceess token","refresh_token":"dummyAccessToken","developer_token":"dummydeveloperToken"}`)
			ctrl := gomock.NewController(GinkgoT())
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
				wg.Add(1)
				go func() {
					defer GinkgoRecover()
					defer wg.Done()
					response, err := oauthHandler.FetchToken(fetchTokenParams)
					// Assertions
					Expect(err).To(BeNil())
					Expect(response).To(MatchJSON(expectedResponse))
					token, _ := cache.Load(fetchTokenParams.AccountID)
					Expect(token.Secret).To(MatchJSON(expectedResponse))
					Expect(token.ExpirationDate).To(BeZero())
				}()
			}
			wg.Wait()

			// Invoke code under test
		})
	})
	Describe("Test RefreshToken with multiple go routines", func() {
		It("refresh token function call when stored cache is same as provided secret", func() {
			previousSecret := []byte(`{"access_token":"storedAccessToken","refresh_token":"dummyAccessToken","developer_token":"dummyDeveloperToken"}`)
			refreshTokenParams := &v2.OAuthTokenParams{
				AccountID:     "123",
				WorkspaceID:   "456",
				DestType:      "testDest",
				DestinationID: Destination.ID,
			}
			// Invoke code under test
			expectedResponse := []byte(`{"access_token":"new acceess token","refresh_token":"dummyAccessToken","developer_token":"dummydeveloperToken"}`)
			ctrl := gomock.NewController(GinkgoT())
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
			for i := 0; i < 20; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					response, err := oauthHandler.RefreshToken(refreshTokenParams, previousSecret)
					// Assertions
					Expect(err).To(BeNil())
					Expect(response).To(MatchJSON(expectedResponse))
					token, _ := cache.Load(refreshTokenParams.AccountID)
					Expect(token.Secret).To(MatchJSON(expectedResponse))
					Expect(token.ExpirationDate).To(BeZero())
				}()
			}
			wg.Wait()
		})

		It("refresh token function call when stored cache is same as provided secret with sequential and concurrent access", func() {
			previousSecret := []byte(`{"access_token":"storedAccessToken","refresh_token":"dummyAccessToken","developer_token":"dummyDeveloperToken"}`)
			refreshTokenParams := &v2.OAuthTokenParams{
				AccountID:     "123",
				WorkspaceID:   "456",
				DestType:      "testDest",
				DestinationID: Destination.ID,
			}
			// Invoke code under test
			expectedResponse := []byte(`{"access_token":"new acceess token","refresh_token":"dummyAccessToken","developer_token":"dummydeveloperToken"}`)
			ctrl := gomock.NewController(GinkgoT())
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

			for i := 0; i < 20; i++ {
				response, err := oauthHandler.RefreshToken(refreshTokenParams, previousSecret)
				// Assertions
				Expect(err).To(BeNil())
				Expect(response).To(MatchJSON(expectedResponse))
				token, _ := cache.Load(refreshTokenParams.AccountID)
				Expect(token.Secret).To(MatchJSON(expectedResponse))
				Expect(token.ExpirationDate).To(BeZero())
			}

			for i := 0; i < 20; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					response, err := oauthHandler.RefreshToken(refreshTokenParams, previousSecret)
					// Assertions
					Expect(err).To(BeNil())
					Expect(response).To(MatchJSON(expectedResponse))
					token, _ := cache.Load(refreshTokenParams.AccountID)
					Expect(token.Secret).To(MatchJSON(expectedResponse))
					Expect(token.ExpirationDate).To(BeZero())
				}()
			}
			wg.Wait()
		})
	})
})
