package v2_test

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"os"
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
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	mockhttpclient "github.com/rudderlabs/rudder-server/mocks/services/oauth/v2/http"
	mock_oauthV2 "github.com/rudderlabs/rudder-server/mocks/services/oauthV2"
	v2 "github.com/rudderlabs/rudder-server/services/oauth/v2"
	"github.com/rudderlabs/rudder-server/services/oauth/v2/common"
	"github.com/rudderlabs/rudder-server/services/oauth/v2/controlplane"
	testutils "github.com/rudderlabs/rudder-server/utils/tests"
)

var Destination = &v2.DestinationInfo{
	DefinitionName:   "testDest",
	ID:               "Destination123",
	WorkspaceID:      "456",
	DefinitionConfig: map[string]interface{}{},
	Config:           map[string]interface{}{},
}

var _ = Describe("Oauth", func() {
	Describe("Test NewOAuthHandler function", func() {
		It("Returns a pointer to OAuthHandler with default values", func() {
			testOauthHandler := v2.NewOAuthHandler(backendconfig.DefaultBackendConfig)
			Expect(testOauthHandler).To(Not(BeNil()))
			Expect(testOauthHandler.CpConn).To(Not(BeNil()))
			Expect(testOauthHandler.Cache).To(Not(BeNil()))
			Expect(testOauthHandler.CacheMutex).To(Not(BeNil()))
			Expect(testOauthHandler.AuthStatusUpdateActiveMap).To(Not(BeNil()))
			Expect(testOauthHandler.ExpirationTimeDiff).To(Not(BeNil()))
			Expect(testOauthHandler.ExpirationTimeDiff).To(Equal(1 * time.Minute))
		})
	})
	Describe("Test FetchToken function", func() {
		It("fetch token function call when cache is empty", func() {
			fetchTokenParams := &v2.RefreshTokenParams{
				AccountID:   "123",
				WorkspaceID: "456",
				DestDefName: "testDest",
				Destination: Destination,
			}

			ctrl := gomock.NewController(GinkgoT())
			mockCpConnector := mock_oauthV2.NewMockConnector(ctrl)
			mockCpConnector.EXPECT().CpApiCall(gomock.Any()).Return(http.StatusOK, `{"options":{},"id":"2BFzzzID8kITtU7AxxWtrn9KQQf","createdAt":"2022-06-29T15:34:47.758Z","updatedAt":"2024-02-12T12:18:35.213Z","workspaceId":"1oVajb9QqG50undaAcokNlYyJQa","name":"dummy user","role":"google_adwords_enhanced_conversions_v1","userId":"1oVadeaoGXN2pataEEoeIaXS3bO","metadata":{"userId":"115538421777182389816","displayName":"dummy user","email":"dummy@testmail.com"},"secretVersion":50,"rudderCategory":"destination","secret":{"access_token":"newaccesstoken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken"}}`)
			mockTokenProvider := mock_oauthV2.NewMockTokenProvider(ctrl)
			mockTokenProvider.EXPECT().Identity().Return(nil)
			expectedResponse := &v2.AuthResponse{
				Account: v2.AccountSecret{
					Secret: []byte(`{"access_token":"newaccesstoken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken"}`),
				}, Err: "",
				ErrorMessage: "",
			}

			// Invoke code under test
			oauthHandler := v2.NewOAuthHandler(mockTokenProvider,
				v2.WithCache(v2.NewCache()),
				v2.WithLocker(kitsync.NewPartitionRWLocker()),
				v2.WithStats(stats.Default),
				v2.WithLogger(logger.NewLogger().Child("MockOAuthHandler")),
				v2.WithCpConnector(mockCpConnector),
			)
			statusCode, response, err := oauthHandler.FetchToken(fetchTokenParams)
			// Assertions
			Expect(statusCode).To(Equal(http.StatusOK))
			Expect(response).To(Equal(expectedResponse))
			Expect(err).To(BeNil())
			token, _ := oauthHandler.Cache.Load(fetchTokenParams.AccountID)
			Expect(token.(*v2.AuthResponse)).To(Equal(expectedResponse))
		})

		It("fetch token function call when cache is not empty and token is not expired", func() {
			fetchTokenParams := &v2.RefreshTokenParams{
				AccountID:   "123",
				WorkspaceID: "456",
				DestDefName: "testDest",
				Destination: Destination,
			}

			expectedResponse := &v2.AuthResponse{
				Account: v2.AccountSecret{
					Secret: []byte(`{"access_token":"StoredDummyaccesstoken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken"}`),
				},
				Err:          "",
				ErrorMessage: "",
			}

			// Invoke code under test
			oauthHandler := v2.NewOAuthHandler(nil,
				v2.WithCache(v2.NewCache()),
				v2.WithLocker(kitsync.NewPartitionRWLocker()),
				v2.WithLogger(logger.NewLogger().Child("MockOAuthHandler")),
				v2.WithStats(stats.Default),
			)
			storedAuthResponse := &v2.AuthResponse{
				Account: v2.AccountSecret{
					Secret: []byte(`{"access_token":"StoredDummyaccesstoken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken"}`),
				},
				Err:          "",
				ErrorMessage: "",
			}
			oauthHandler.Cache.Store(fetchTokenParams.AccountID, storedAuthResponse)
			statusCode, response, err := oauthHandler.FetchToken(fetchTokenParams)
			// Assertions
			Expect(statusCode).To(Equal(http.StatusOK))
			Expect(response).To(Equal(expectedResponse))
			Expect(err).To(BeNil())
			token, _ := oauthHandler.Cache.Load(fetchTokenParams.AccountID)
			// We are checking if the token is updated in the cache or not
			Expect(token.(*v2.AuthResponse)).To(Equal(storedAuthResponse))
		})

		It("fetch token function call when cache is not empty and token is expired", func() {
			fetchTokenParams := &v2.RefreshTokenParams{
				AccountID:   "123",
				WorkspaceID: "456",
				DestDefName: "testDest",
				Destination: Destination,
			}

			ctrl := gomock.NewController(GinkgoT())
			mockCpConnector := mock_oauthV2.NewMockConnector(ctrl)
			mockCpConnector.EXPECT().CpApiCall(gomock.Any()).Return(http.StatusOK, `{"options":{},"id":"2BFzzzID8kITtU7AxxWtrn9KQQf","createdAt":"2022-06-29T15:34:47.758Z","updatedAt":"2024-02-12T12:18:35.213Z","workspaceId":"1oVajb9QqG50undaAcokNlYyJQa","name":"dummy user","role":"google_adwords_enhanced_conversions_v1","userId":"1oVadeaoGXN2pataEEoeIaXS3bO","metadata":{"userId":"115538421777182389816","displayName":"dummy user","email":"dummy@testmail.com"},"secretVersion":50,"rudderCategory":"destination","secret":{"access_token":"NewDummyaccesstoken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken"}}`)
			mockTokenProvider := mock_oauthV2.NewMockTokenProvider(ctrl)
			mockTokenProvider.EXPECT().Identity().Return(nil)
			expectedResponse := &v2.AuthResponse{
				Account: v2.AccountSecret{
					Secret: []byte(`{"access_token":"NewDummyaccesstoken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken"}`),
				},
				Err:          "",
				ErrorMessage: "",
			}
			oauthHandler := v2.NewOAuthHandler(mockTokenProvider,
				v2.WithCache(v2.NewCache()),
				v2.WithLocker(kitsync.NewPartitionRWLocker()),
				v2.WithStats(stats.Default),
				v2.WithLogger(logger.NewLogger().Child("MockOAuthHandler")),
				v2.WithCpConnector(mockCpConnector),
			)
			storedAuthResponse := &v2.AuthResponse{
				Account: v2.AccountSecret{
					Secret:         []byte(`{"access_token":"StoredDummyaccesstoken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken","expirationDate":"2022-06-29T15:34:47.758Z"}`),
					ExpirationDate: "2022-06-29T15:34:47.758Z",
				},
				Err:          "",
				ErrorMessage: "",
			}
			oauthHandler.Cache.Store(fetchTokenParams.AccountID, storedAuthResponse)
			statusCode, response, err := oauthHandler.FetchToken(fetchTokenParams)
			Expect(statusCode).To(Equal(http.StatusOK))
			Expect(response).To(Equal(expectedResponse))
			Expect(err).To(BeNil())
			token, _ := oauthHandler.Cache.Load(fetchTokenParams.AccountID)
			// We are checking if the token is updated in the cache or not
			Expect(token.(*v2.AuthResponse)).NotTo(Equal(storedAuthResponse))
		})

		It("fetch token function call is successful and token is returned with 'expirationDate', should contain ExpirationDate information in AccountSecret{}", func() {
			fetchTokenParams := &v2.RefreshTokenParams{
				AccountID:   "123",
				WorkspaceID: "456",
				DestDefName: "testDest",
				Destination: Destination,
			}

			ctrl := gomock.NewController(GinkgoT())
			mockCpConnector := mock_oauthV2.NewMockConnector(ctrl)
			mockCpConnector.EXPECT().CpApiCall(gomock.Any()).Return(http.StatusOK, `{"options":{},"id":"2BFzzzID8kITtU7AxxWtrn9KQQf","createdAt":"2022-06-29T15:34:47.758Z","updatedAt":"2024-02-12T12:18:35.213Z","workspaceId":"1oVajb9QqG50undaAcokNlYyJQa","name":"dummy user","role":"google_adwords_enhanced_conversions_v1","userId":"1oVadeaoGXN2pataEEoeIaXS3bO","metadata":{"userId":"115538421777182389816","displayName":"dummy user","email":"dummy@testmail.com"},"secretVersion":50,"rudderCategory":"destination","secret":{"access_token":"NewDummyaccesstoken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken","expirationDate":"2022-06-29T16:34:47.758Z"}}`)
			mockTokenProvider := mock_oauthV2.NewMockTokenProvider(ctrl)
			mockTokenProvider.EXPECT().Identity().Return(nil)
			expectedResponse := &v2.AuthResponse{
				Account: v2.AccountSecret{
					Secret:         []byte(`{"access_token":"NewDummyaccesstoken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken","expirationDate":"2022-06-29T16:34:47.758Z"}`),
					ExpirationDate: "2022-06-29T16:34:47.758Z",
				},
				Err:          "",
				ErrorMessage: "",
			}
			oauthHandler := v2.NewOAuthHandler(mockTokenProvider,
				v2.WithCache(v2.NewCache()),
				v2.WithLocker(kitsync.NewPartitionRWLocker()),
				v2.WithStats(stats.Default),
				v2.WithLogger(logger.NewLogger().Child("MockOAuthHandler")),
				v2.WithCpConnector(mockCpConnector),
			)

			statusCode, _, err := oauthHandler.FetchToken(fetchTokenParams)
			Expect(statusCode).To(Equal(http.StatusOK))
			Expect(err).To(BeNil())
			token, _ := oauthHandler.Cache.Load(fetchTokenParams.AccountID)
			// We are checking if the token is updated in the cache or not
			Expect(token).To(BeAssignableToTypeOf(&v2.AuthResponse{}))
			accountInfo, ok := token.(*v2.AuthResponse)
			Expect(ok).To(BeTrue())
			Expect(accountInfo.Account.ExpirationDate).To(Equal(expectedResponse.Account.ExpirationDate))
			Expect(accountInfo.Account.Secret).To(Equal(expectedResponse.Account.Secret))
		})

		It("fetch token function call when cache is empty and cpApiCall returns empty response", func() {
			fetchTokenParams := &v2.RefreshTokenParams{
				AccountID:   "123",
				WorkspaceID: "456",
				DestDefName: "testDest",
				Destination: Destination,
			}

			ctrl := gomock.NewController(GinkgoT())
			mockCpConnector := mock_oauthV2.NewMockConnector(ctrl)
			mockCpConnector.EXPECT().CpApiCall(gomock.Any()).Return(http.StatusNoContent, ``)
			mockTokenProvider := mock_oauthV2.NewMockTokenProvider(ctrl)
			mockTokenProvider.EXPECT().Identity().Return(nil)
			oauthHandler := v2.NewOAuthHandler(mockTokenProvider,
				v2.WithCache(v2.NewCache()),
				v2.WithLocker(kitsync.NewPartitionRWLocker()),
				v2.WithStats(stats.Default),
				v2.WithLogger(logger.NewLogger().Child("MockOAuthHandler")),
				v2.WithCpConnector(mockCpConnector),
			)
			statusCode, response, err := oauthHandler.FetchToken(fetchTokenParams)
			Expect(statusCode).To(Equal(http.StatusInternalServerError))
			var expectedResponse *v2.AuthResponse
			Expect(response).To(Equal(expectedResponse))

			Expect(err).To(MatchError(fmt.Errorf("empty secret")))
		})

		It("fetch token function call when cache is empty and cpApiCall returns a failed response", func() {
			fetchTokenParams := &v2.RefreshTokenParams{
				AccountID:   "123",
				WorkspaceID: "456",
				DestDefName: "testDest",
				Destination: Destination,
			}

			ctrl := gomock.NewController(GinkgoT())
			mockCpConnector := mock_oauthV2.NewMockConnector(ctrl)
			mockCpConnector.EXPECT().CpApiCall(gomock.Any()).Return(http.StatusNoContent, `{
				"body":{
				  "code":"ref_token_invalid_grant",
				  "message":"invalid_grant error, refresh token has expired or revoked"
				}
			  }`)
			mockTokenProvider := mock_oauthV2.NewMockTokenProvider(ctrl)
			mockTokenProvider.EXPECT().Identity().Return(nil)
			oauthHandler := v2.NewOAuthHandler(mockTokenProvider,
				v2.WithCache(v2.NewCache()),
				v2.WithLocker(kitsync.NewPartitionRWLocker()),
				v2.WithStats(stats.Default),
				v2.WithLogger(logger.NewLogger().Child("MockOAuthHandler")),
				v2.WithCpConnector(mockCpConnector),
			)
			statusCode, response, err := oauthHandler.FetchToken(fetchTokenParams)
			Expect(statusCode).To(Equal(http.StatusBadRequest))
			expectedResponse := &v2.AuthResponse{
				Err:          "ref_token_invalid_grant",
				ErrorMessage: "invalid_grant error, refresh token has expired or revoked",
			}
			Expect(response).To(Equal(expectedResponse))
			Expect(err).To(MatchError(fmt.Errorf("invalid grant")))
		})
		It("fetch token function call when cache is empty and cpApiCall returns a failed response due to config backend service is not available", func() {
			fetchTokenParams := &v2.RefreshTokenParams{
				AccountID:   "123",
				WorkspaceID: "456",
				DestDefName: "testDest",
				Destination: Destination,
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

			mockTokenProvider := mock_oauthV2.NewMockTokenProvider(ctrl)
			mockTokenProvider.EXPECT().Identity().Return(&testutils.BasicAuthMock{})
			oauthHandler := v2.NewOAuthHandler(mockTokenProvider,
				v2.WithCache(v2.NewCache()),
				v2.WithLocker(kitsync.NewPartitionRWLocker()),
				v2.WithStats(stats.Default),
				v2.WithLogger(logger.NewLogger().Child("MockOAuthHandler")),
				v2.WithCpConnector(cpConnector),
			)
			statusCode, response, err := oauthHandler.FetchToken(fetchTokenParams)
			Expect(statusCode).To(Equal(http.StatusInternalServerError))
			expectedResponse := &v2.AuthResponse{
				Err:          "econnrefused",
				ErrorMessage: "mock mock 127.0.0.1:1234->127.0.0.1:12340: read: connection refused",
			}
			Expect(response).To(Equal(expectedResponse))
			Expect(err).To(MatchError(fmt.Errorf("error occurred while fetching/refreshing account info from CP: mock mock 127.0.0.1:1234->127.0.0.1:12340: read: connection refused")))
		})
		It("fetch token function call when cache is empty and cpApiCall returns a failed response due to config backend call failed due to timeout error", func() {
			fetchTokenParams := &v2.RefreshTokenParams{
				AccountID:   "123",
				WorkspaceID: "456",
				DestDefName: "testDest",
				Destination: Destination,
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

			mockTokenProvider := mock_oauthV2.NewMockTokenProvider(ctrl)
			mockTokenProvider.EXPECT().Identity().Return(&testutils.BasicAuthMock{})
			oauthHandler := v2.NewOAuthHandler(mockTokenProvider,
				v2.WithCache(v2.NewCache()),
				v2.WithLocker(kitsync.NewPartitionRWLocker()),
				v2.WithStats(stats.Default),
				v2.WithLogger(logger.NewLogger().Child("MockOAuthHandler")),
				v2.WithCpConnector(cpConnector),
			)
			statusCode, response, err := oauthHandler.FetchToken(fetchTokenParams)
			Expect(statusCode).To(Equal(http.StatusInternalServerError))
			expectedResponse := &v2.AuthResponse{
				Err:          "timeout",
				ErrorMessage: "mock mock 127.0.0.1:1234->127.0.0.1:12340: read: connection timed out",
			}
			Expect(response).To(Equal(expectedResponse))
			Expect(err).To(MatchError(fmt.Errorf("error occurred while fetching/refreshing account info from CP: mock mock 127.0.0.1:1234->127.0.0.1:12340: read: connection timed out")))
		})
	})

	Describe("Test RefreshToken function", func() {
		It("refreshToken function call when stored cache is same as provided secret", func() {
			refreshTokenParams := &v2.RefreshTokenParams{
				AccountID:   "123",
				WorkspaceID: "456",
				DestDefName: "testDest",
				Destination: Destination,
				Secret:      []byte(`{"access_token":"storedAccessToken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken"}`),
			}

			ctrl := gomock.NewController(GinkgoT())
			mockCpConnector := mock_oauthV2.NewMockConnector(ctrl)
			mockCpConnector.EXPECT().CpApiCall(gomock.Any()).Return(http.StatusOK, `{"options":{},"id":"2BFzzzID8kITtU7AxxWtrn9KQQf","createdAt":"2022-06-29T15:34:47.758Z","updatedAt":"2024-02-12T12:18:35.213Z","workspaceId":"1oVajb9QqG50undaAcokNlYyJQa","name":"dummy user","role":"google_adwords_enhanced_conversions_v1","userId":"1oVadeaoGXN2pataEEoeIaXS3bO","metadata":{"userId":"115538421777182389816","displayName":"dummy user","email":"dummy@testmail.com"},"secretVersion":50,"rudderCategory":"destination","secret":{"access_token":"newAccessToken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken"}}`)

			mockTokenProvider := mock_oauthV2.NewMockTokenProvider(ctrl)
			mockTokenProvider.EXPECT().Identity().Return(nil)

			// Invoke code under test
			oauthHandler := v2.NewOAuthHandler(mockTokenProvider,
				v2.WithCache(v2.NewCache()),
				v2.WithLocker(kitsync.NewPartitionRWLocker()),
				v2.WithStats(stats.Default),
				v2.WithLogger(logger.NewLogger().Child("MockOAuthHandler")),
				v2.WithCpConnector(mockCpConnector),
			)
			storedAuthResponse := &v2.AuthResponse{
				Account: v2.AccountSecret{
					Secret: []byte(`{"access_token":"storedAccessToken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken"}`),
				}, Err: "",
				ErrorMessage: "",
			}
			oauthHandler.Cache.Store(refreshTokenParams.AccountID, storedAuthResponse)
			statusCode, _, err := oauthHandler.RefreshToken(refreshTokenParams)
			// Assertions
			Expect(statusCode).To(Equal(http.StatusOK))
			Expect(err).To(BeNil())
			token, _ := oauthHandler.Cache.Load(refreshTokenParams.AccountID)
			expectedResponse := &v2.AuthResponse{
				Account: v2.AccountSecret{
					Secret: []byte(`{"access_token":"newAccessToken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken"}`),
				}, Err: "",
				ErrorMessage: "",
			}
			Expect(token.(*v2.AuthResponse)).To(Equal(expectedResponse))
		})

		It("refreshToken function call when stored cache is different as provided secret", func() {
			refreshTokenParams := &v2.RefreshTokenParams{
				AccountID:   "123",
				WorkspaceID: "456",
				DestDefName: "testDest",
				Destination: Destination,
				Secret:      []byte(`{"access_token":"providedAccessToken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken"}`),
			}

			// Invoke code under test
			oauthHandler := v2.NewOAuthHandler(nil,
				v2.WithCache(v2.NewCache()),
				v2.WithLocker(kitsync.NewPartitionRWLocker()),
				v2.WithStats(stats.Default),
				v2.WithLogger(logger.NewLogger().Child("MockOAuthHandler")),
			)
			storedAuthResponse := &v2.AuthResponse{
				Account: v2.AccountSecret{
					Secret: []byte(`{"access_token":"storedAccessToken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken"}`),
				}, Err: "",
				ErrorMessage: "",
			}
			oauthHandler.Cache.Store(refreshTokenParams.AccountID, storedAuthResponse)
			token, _ := oauthHandler.Cache.Load(refreshTokenParams.AccountID)
			expectedResponse := &v2.AuthResponse{
				Account: v2.AccountSecret{
					Secret: []byte(`{"access_token":"storedAccessToken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken"}`),
				}, Err: "",
				ErrorMessage: "",
			}
			statusCode, _, err := oauthHandler.RefreshToken(refreshTokenParams)

			Expect(statusCode).To(Equal(http.StatusOK))
			Expect(err).To(BeNil())
			Expect(token.(*v2.AuthResponse)).To(Equal(expectedResponse))
		})

		It("refreshToken function call when stored cache is same as provided secret but the cpApiCall failed with ref_token_invalid_grant error", func() {
			refreshTokenParams := &v2.RefreshTokenParams{
				AccountID:   "123",
				WorkspaceID: "456",
				DestDefName: "testDest",
				Destination: Destination,
				Secret:      []byte(`{"access_token":"storedAccessToken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken"}`),
			}

			ctrl := gomock.NewController(GinkgoT())
			mockCpConnector := mock_oauthV2.NewMockConnector(ctrl)

			mockCpConnector.EXPECT().CpApiCall(gomock.Any()).Return(http.StatusBadRequest, `{
				"body":{
				  "code":"ref_token_invalid_grant",
				  "message":"invalid_grant error, refresh token has expired or revoked"
				}
			  }`)

			mockTokenProvider := mock_oauthV2.NewMockTokenProvider(ctrl)
			mockTokenProvider.EXPECT().Identity().Return(nil)

			// Invoke code under test
			oauthHandler := v2.NewOAuthHandler(mockTokenProvider,
				v2.WithCache(v2.NewCache()),
				v2.WithLocker(kitsync.NewPartitionRWLocker()),
				v2.WithStats(stats.Default),
				v2.WithLogger(logger.NewLogger().Child("MockOAuthHandler")),
				v2.WithCpConnector(mockCpConnector),
			)
			storedAuthResponse := &v2.AuthResponse{
				Account: v2.AccountSecret{
					Secret: []byte(`{"access_token":"storedAccessToken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken"}`),
				}, Err: "",
				ErrorMessage: "",
			}
			oauthHandler.Cache.Store(refreshTokenParams.AccountID, storedAuthResponse)
			expectedResponse := &v2.AuthResponse{
				Account: v2.AccountSecret{
					Secret: nil,
				}, Err: "ref_token_invalid_grant",
				ErrorMessage: "invalid_grant error, refresh token has expired or revoked",
			}
			statusCode, response, err := oauthHandler.RefreshToken(refreshTokenParams)

			Expect(statusCode).To(Equal(http.StatusBadRequest))
			Expect(err).To(MatchError(fmt.Errorf("invalid grant")))
			Expect(response).To(Equal(expectedResponse))
		})
		It("refreshToken function call when stored cache is same as provided secret and cpApiCall returns a failed response due to config backend service is not available", func() {
			refreshTokenParams := &v2.RefreshTokenParams{
				AccountID:   "123",
				WorkspaceID: "456",
				DestDefName: "testDest",
				Destination: Destination,
				Secret:      []byte(`{"access_token":"storedAccessToken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken"}`),
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

			mockTokenProvider := mock_oauthV2.NewMockTokenProvider(ctrl)
			mockTokenProvider.EXPECT().Identity().Return(&testutils.BasicAuthMock{})
			oauthHandler := v2.NewOAuthHandler(mockTokenProvider,
				v2.WithCache(v2.NewCache()),
				v2.WithLocker(kitsync.NewPartitionRWLocker()),
				v2.WithStats(stats.Default),
				v2.WithLogger(logger.NewLogger().Child("MockOAuthHandler")),
				v2.WithCpConnector(cpConnector),
			)
			statusCode, response, err := oauthHandler.RefreshToken(refreshTokenParams)
			Expect(statusCode).To(Equal(http.StatusInternalServerError))
			expectedResponse := &v2.AuthResponse{
				Err:          "econnrefused",
				ErrorMessage: "mock mock 127.0.0.1:1234->127.0.0.1:12340: read: connection refused",
			}
			Expect(response).To(Equal(expectedResponse))
			Expect(err).To(MatchError(fmt.Errorf("error occurred while fetching/refreshing account info from CP: mock mock 127.0.0.1:1234->127.0.0.1:12340: read: connection refused")))
		})

		It("refreshToken function call when stored cache is same as provided secret and cpApiCall returns a failed response due to config backend service is failed due to timeout", func() {
			refreshTokenParams := &v2.RefreshTokenParams{
				AccountID:   "123",
				WorkspaceID: "456",
				DestDefName: "testDest",
				Destination: Destination,
				Secret:      []byte(`{"access_token":"storedAccessToken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken"}`),
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

			mockTokenProvider := mock_oauthV2.NewMockTokenProvider(ctrl)
			mockTokenProvider.EXPECT().Identity().Return(&testutils.BasicAuthMock{})
			oauthHandler := v2.NewOAuthHandler(mockTokenProvider,
				v2.WithCache(v2.NewCache()),
				v2.WithLocker(kitsync.NewPartitionRWLocker()),
				v2.WithStats(stats.Default),
				v2.WithLogger(logger.NewLogger().Child("MockOAuthHandler")),
				v2.WithCpConnector(cpConnector),
			)
			statusCode, response, err := oauthHandler.RefreshToken(refreshTokenParams)
			Expect(statusCode).To(Equal(http.StatusInternalServerError))
			expectedResponse := &v2.AuthResponse{
				Err:          "timeout",
				ErrorMessage: "mock mock 127.0.0.1:1234->127.0.0.1:12340: read: connection timed out",
			}
			Expect(response).To(Equal(expectedResponse))
			Expect(err).To(MatchError(fmt.Errorf("error occurred while fetching/refreshing account info from CP: mock mock 127.0.0.1:1234->127.0.0.1:12340: read: connection timed out")))
		})

		It("refreshToken function call when stored cache is same as provided secret and cpApiCall returns a failed response because of faulty implementation in some downstream service", func() {
			refreshTokenParams := &v2.RefreshTokenParams{
				AccountID:   "123",
				WorkspaceID: "456",
				DestDefName: "testDest",
				Destination: Destination,
				Secret:      []byte(`{"access_token":"storedAccessToken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken"}`),
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

			mockTokenProvider := mock_oauthV2.NewMockTokenProvider(ctrl)
			mockTokenProvider.EXPECT().Identity().Return(&testutils.BasicAuthMock{})
			oauthHandler := v2.NewOAuthHandler(mockTokenProvider,
				v2.WithCache(v2.NewCache()),
				v2.WithLocker(kitsync.NewPartitionRWLocker()),
				v2.WithStats(stats.Default),
				v2.WithLogger(logger.NewLogger().Child("MockOAuthHandler")),
				v2.WithCpConnector(cpConnector),
			)
			statusCode, response, err := oauthHandler.RefreshToken(refreshTokenParams)
			Expect(statusCode).To(Equal(http.StatusInternalServerError))
			expectedResponse := &v2.AuthResponse{
				Err:          "INVALID_REFRESH_RESPONSE",
				ErrorMessage: "Missing required token fields in refresh response",
			}
			Expect(response).To(Equal(expectedResponse))
			Expect(err).To(MatchError(fmt.Errorf("error occurred while fetching/refreshing account info from CP: Missing required token fields in refresh response")))
		})

		It("refreshToken function call when stored cache is same as provided secret and cpApiCall returns a failed response because of invalid refresh response without message", func() {
			refreshTokenParams := &v2.RefreshTokenParams{
				AccountID:   "123",
				WorkspaceID: "456",
				DestDefName: "testDest",
				Destination: Destination,
				Secret:      []byte(`{"access_token":"storedAccessToken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken"}`),
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

			mockTokenProvider := mock_oauthV2.NewMockTokenProvider(ctrl)
			mockTokenProvider.EXPECT().Identity().Return(&testutils.BasicAuthMock{})
			oauthHandler := v2.NewOAuthHandler(mockTokenProvider,
				v2.WithCache(v2.NewCache()),
				v2.WithLocker(kitsync.NewPartitionRWLocker()),
				v2.WithStats(stats.Default),
				v2.WithLogger(logger.NewLogger().Child("MockOAuthHandler")),
				v2.WithCpConnector(cpConnector),
			)
			statusCode, response, err := oauthHandler.RefreshToken(refreshTokenParams)
			Expect(statusCode).To(Equal(http.StatusInternalServerError))
			expectedResponse := &v2.AuthResponse{
				Err:          "INVALID_REFRESH_RESPONSE",
				ErrorMessage: "invalid refresh response",
			}
			Expect(response).To(Equal(expectedResponse))
			Expect(err).To(MatchError(fmt.Errorf("error occurred while fetching/refreshing account info from CP: invalid refresh response")))
		})

		It("refreshToken function call when stored cache is same as provided secret and cpApiCall returns a failed response because of invalid refresh response without message", func() {
			refreshTokenParams := &v2.RefreshTokenParams{
				AccountID:   "123",
				WorkspaceID: "456",
				DestDefName: "testDest",
				Destination: Destination,
				Secret:      []byte(`{"access_token":"storedAccessToken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken"}`),
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

			mockTokenProvider := mock_oauthV2.NewMockTokenProvider(ctrl)
			mockTokenProvider.EXPECT().Identity().Return(&testutils.BasicAuthMock{})
			oauthHandler := v2.NewOAuthHandler(mockTokenProvider,
				v2.WithCache(v2.NewCache()),
				v2.WithLocker(kitsync.NewPartitionRWLocker()),
				v2.WithStats(stats.Default),
				v2.WithLogger(logger.NewLogger().Child("MockOAuthHandler")),
				v2.WithCpConnector(cpConnector),
			)
			statusCode, response, err := oauthHandler.RefreshToken(refreshTokenParams)
			Expect(statusCode).To(Equal(http.StatusBadRequest))
			expectedResponse := &v2.AuthResponse{
				Err:          common.RefTokenInvalidGrant,
				ErrorMessage: v2.ErrPermissionOrTokenRevoked.Error(),
			}
			Expect(response).To(Equal(expectedResponse))
			Expect(err).To(MatchError(fmt.Errorf("invalid grant")))
		})
	})

	Describe("Test AuthStatusToggle function", func() {
		It("authStatusToggle function call when config backend api call failed", func() {
			ctrl := gomock.NewController(GinkgoT())
			mockTokenProvider := mock_oauthV2.NewMockTokenProvider(ctrl)
			mockTokenProvider.EXPECT().Identity().Return(nil)
			mockCpConnector := mock_oauthV2.NewMockConnector(ctrl)
			oauthHandler := v2.NewOAuthHandler(mockTokenProvider,
				v2.WithCache(v2.NewCache()),
				v2.WithLocker(kitsync.NewPartitionRWLocker()),
				v2.WithStats(stats.Default),
				v2.WithLogger(logger.NewLogger().Child("MockOAuthHandler")),
				v2.WithCpConnector(mockCpConnector),
			)
			mockCpConnector.EXPECT().CpApiCall(gomock.Any()).Return(http.StatusBadRequest, `{
				  "message":"unable to update the auth status for the destination"
			  }`)
			statusCode, response := oauthHandler.AuthStatusToggle(&v2.AuthStatusToggleParams{
				Destination:     Destination,
				WorkspaceID:     "workspaceID",
				RudderAccountID: "rudderAccountId",
				StatPrefix:      "AuthStatusInactive",
				AuthStatus:      common.CategoryAuthStatusInactive,
			})
			Expect(statusCode).To(Equal(http.StatusBadRequest))
			Expect(response).To(Equal("problem with user permission or access/refresh token have been revoked"))
		})
		It("authStatusToggle function call when config backend api call succeeded", func() {
			ctrl := gomock.NewController(GinkgoT())
			mockTokenProvider := mock_oauthV2.NewMockTokenProvider(ctrl)
			mockTokenProvider.EXPECT().Identity().Return(nil)
			mockCpConnector := mock_oauthV2.NewMockConnector(ctrl)
			oauthHandler := v2.NewOAuthHandler(mockTokenProvider,
				v2.WithCache(v2.NewCache()),
				v2.WithLocker(kitsync.NewPartitionRWLocker()),
				v2.WithStats(stats.Default),
				v2.WithLogger(logger.NewLogger().Child("MockOAuthHandler")),
				v2.WithCpConnector(mockCpConnector),
			)
			mockCpConnector.EXPECT().CpApiCall(gomock.Any()).Return(http.StatusOK, ``)
			statusCode, response := oauthHandler.AuthStatusToggle(&v2.AuthStatusToggleParams{
				Destination:     Destination,
				WorkspaceID:     "workspaceID",
				RudderAccountID: "rudderAccountId",
				StatPrefix:      "AuthStatusInactive",
				AuthStatus:      common.CategoryAuthStatusInactive,
			})
			Expect(statusCode).To(Equal(http.StatusBadRequest))
			Expect(response).To(Equal("problem with user permission or access/refresh token have been revoked"))
		})
		It("authStatusToggle function call when config backend api call failed due to config backend service is failed due to timeout", func() {
			ctrl := gomock.NewController(GinkgoT())
			mockHttpClient := mockhttpclient.NewMockHttpClient(ctrl)
			mockHttpClient.EXPECT().Do(gomock.Any()).Return(&http.Response{
				StatusCode: http.StatusRequestTimeout,
			}, &net.OpError{
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

			mockTokenProvider := mock_oauthV2.NewMockTokenProvider(ctrl)
			mockTokenProvider.EXPECT().Identity().Return(&testutils.BasicAuthMock{})
			oauthHandler := v2.NewOAuthHandler(mockTokenProvider,
				v2.WithCache(v2.NewCache()),
				v2.WithLocker(kitsync.NewPartitionRWLocker()),
				v2.WithStats(stats.Default),
				v2.WithLogger(logger.NewLogger().Child("MockOAuthHandler")),
				v2.WithCpConnector(cpConnector),
			)
			statusCode, response := oauthHandler.AuthStatusToggle(&v2.AuthStatusToggleParams{
				Destination:     Destination,
				WorkspaceID:     "workspaceID",
				RudderAccountID: "rudderAccountId",
				StatPrefix:      "AuthStatusInactive",
				AuthStatus:      common.CategoryAuthStatusInactive,
			})
			Expect(statusCode).To(Equal(http.StatusBadRequest))
			Expect(response).To(Equal("problem with user permission or access/refresh token have been revoked"))
		})
	})

	Describe("Test FetchToken with multiple go routines", func() {
		It("fetch token function call when cache is not empty", func() {
			fetchTokenParams := &v2.RefreshTokenParams{
				AccountID:   "123",
				WorkspaceID: "456",
				DestDefName: "testDest",
				Destination: Destination,
			}
			// Invoke code under test
			expectedResponse := &v2.AuthResponse{
				Account: v2.AccountSecret{
					Secret: []byte(`{"access_token":"new acceess token","refresh_token":"dummyAccessToken","developer_token":"dummydeveloperToken"}`),
				}, Err: "",
				ErrorMessage: "",
			}

			ctrl := gomock.NewController(GinkgoT())
			mockCpConnector := mock_oauthV2.NewMockConnector(ctrl)
			mockCpConnector.EXPECT().CpApiCall(gomock.Any()).Times(0)

			oauthHandler := v2.NewOAuthHandler(nil,
				v2.WithCache(v2.NewCache()),
				v2.WithLocker(kitsync.NewPartitionRWLocker()),
				v2.WithLogger(logger.NewLogger().Child("MockOAuthHandler")),
				v2.WithCpConnector(mockCpConnector),
			)
			storedAuthResponse := &v2.AuthResponse{
				Account: v2.AccountSecret{
					Secret: []byte(`{"access_token":"new acceess token","refresh_token":"dummyAccessToken","developer_token":"dummydeveloperToken"}`),
				}, Err: "",
				ErrorMessage: "",
			}
			oauthHandler.Cache.Store(fetchTokenParams.AccountID, storedAuthResponse)
			wg := sync.WaitGroup{}
			for i := 0; i < 20; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
					statusCode, response, err := oauthHandler.FetchToken(fetchTokenParams)
					// Assertions
					Expect(statusCode).To(Equal(http.StatusOK))
					Expect(response).To(Equal(expectedResponse))
					Expect(err).To(BeNil())
					token, _ := oauthHandler.Cache.Load(fetchTokenParams.AccountID)
					Expect(token.(*v2.AuthResponse)).To(Equal(expectedResponse))
				}()
			}
			wg.Wait()

			// Invoke code under test
		})

		It("fetch token function call when cache is empty", func() {
			fetchTokenParams := &v2.RefreshTokenParams{
				AccountID:   "123",
				WorkspaceID: "456",
				DestDefName: "testDest",
				Destination: Destination,
			}
			// Invoke code under test
			expectedResponse := &v2.AuthResponse{
				Account: v2.AccountSecret{
					Secret: []byte(`{"access_token":"new 1234 acceess token","refresh_token":"dummyAccessToken","developer_token":"dummydeveloperToken"}`),
				}, Err: "",
				ErrorMessage: "",
			}
			ctrl := gomock.NewController(GinkgoT())
			mockCpConnector := mock_oauthV2.NewMockConnector(ctrl)
			mockCpConnector.EXPECT().CpApiCall(gomock.Any()).Return(http.StatusOK, `{"options":{},"id":"2BFzzzID8kITtU7AxxWtrn9KQQf","createdAt":"2022-06-29T15:34:47.758Z","updatedAt":"2024-02-12T12:18:35.213Z","workspaceId":"1oVajb9QqG50undaAcokNlYyJQa","name":"dummy user","role":"google_adwords_enhanced_conversions_v1","userId":"1oVadeaoGXN2pataEEoeIaXS3bO","metadata":{"userId":"115538421777182389816","displayName":"dummy user","email":""},"secretVersion":50,"rudderCategory":"destination","secret":{"access_token":"new 1234 acceess token","refresh_token":"dummyAccessToken","developer_token":"dummydeveloperToken"}}`).Times(1)
			mockTokenProvider := mock_oauthV2.NewMockTokenProvider(ctrl)
			mockTokenProvider.EXPECT().Identity().Return(nil)
			oauthHandler := v2.NewOAuthHandler(mockTokenProvider,
				v2.WithCache(v2.NewCache()),
				v2.WithLocker(kitsync.NewPartitionRWLocker()),
				v2.WithStats(stats.Default),
				v2.WithLogger(logger.NewLogger().Child("MockOAuthHandler")),
				v2.WithCpConnector(mockCpConnector),
			)
			wg := sync.WaitGroup{}
			for i := 0; i < 200; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					statusCode, response, err := oauthHandler.FetchToken(fetchTokenParams)
					// Assertions
					Expect(statusCode).To(Equal(http.StatusOK))
					Expect(response).To(Equal(expectedResponse))
					Expect(err).To(BeNil())
					token, _ := oauthHandler.Cache.Load(fetchTokenParams.AccountID)
					Expect(token.(*v2.AuthResponse)).To(Equal(expectedResponse))
				}()
			}
			wg.Wait()

			// Invoke code under test
		})
	})
	Describe("Test RefreshToken with multiple go routines", func() {
		It("refresh token function call when stored cache is same as provided secret", func() {
			refreshTokenParams := &v2.RefreshTokenParams{
				AccountID:   "123",
				WorkspaceID: "456",
				DestDefName: "testDest",
				Destination: Destination,
				Secret:      []byte(`{"access_token":"storedAccessToken","refresh_token":"dummyAccessToken","developer_token":"dummyDeveloperToken"}`),
			}
			// Invoke code under test
			expectedResponse := &v2.AuthResponse{
				Account: v2.AccountSecret{
					Secret: []byte(`{"access_token":"new acceess token","refresh_token":"dummyAccessToken","developer_token":"dummydeveloperToken"}`),
				}, Err: "",
				ErrorMessage: "",
			}
			ctrl := gomock.NewController(GinkgoT())
			mockCpConnector := mock_oauthV2.NewMockConnector(ctrl)
			mockCpConnector.EXPECT().CpApiCall(gomock.Any()).Return(http.StatusOK, `{"options":{},"id":"2BFzzzID8kITtU7AxxWtrn9KQQf","createdAt":"2022-06-29T15:34:47.758Z","updatedAt":"2024-02-12T12:18:35.213Z","workspaceId":"1oVajb9QqG50undaAcokNlYyJQa","name":"dummy user","role":"google_adwords_enhanced_conversions_v1","userId":"1oVadeaoGXN2pataEEoeIaXS3bO","metadata":{"userId":"115538421777182389816","displayName":"dummy user","email":""},"secretVersion":50,"rudderCategory":"destination","secret":{"access_token":"new acceess token","refresh_token":"dummyAccessToken","developer_token":"dummydeveloperToken"}}`).Times(1)
			mockTokenProvider := mock_oauthV2.NewMockTokenProvider(ctrl)
			mockTokenProvider.EXPECT().Identity().Return(nil)
			oauthHandler := v2.NewOAuthHandler(mockTokenProvider,
				v2.WithCache(v2.NewCache()),
				v2.WithLocker(kitsync.NewPartitionRWLocker()),
				v2.WithStats(stats.Default),
				v2.WithLogger(logger.NewLogger().Child("MockOAuthHandler")),
				v2.WithCpConnector(mockCpConnector),
			)

			storedAuthResponse := &v2.AuthResponse{
				Account: v2.AccountSecret{
					Secret: []byte(`{"access_token":"storedAccessToken","refresh_token":"dummyAccessToken","developer_token":"dummyDeveloperToken"}`),
				}, Err: "",
				ErrorMessage: "",
			}
			oauthHandler.Cache.Store(refreshTokenParams.AccountID, storedAuthResponse)
			wg := sync.WaitGroup{}
			for i := 0; i < 20; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					statusCode, response, err := oauthHandler.RefreshToken(refreshTokenParams)
					// Assertions
					Expect(statusCode).To(Equal(http.StatusOK))
					Expect(response).To(Equal(expectedResponse))
					Expect(err).To(BeNil())
					token, _ := oauthHandler.Cache.Load(refreshTokenParams.AccountID)
					Expect(token.(*v2.AuthResponse)).To(Equal(expectedResponse))
				}()
			}
			wg.Wait()
		})

		It("refresh token function call when stored cache is same as provided secret with sequential and concurrent access", func() {
			refreshTokenParams := &v2.RefreshTokenParams{
				AccountID:   "123",
				WorkspaceID: "456",
				DestDefName: "testDest",
				Destination: Destination,
				Secret:      []byte(`{"access_token":"storedAccessToken","refresh_token":"dummyAccessToken","developer_token":"dummyDeveloperToken"}`),
			}
			// Invoke code under test
			expectedResponse := &v2.AuthResponse{
				Account: v2.AccountSecret{
					Secret: []byte(`{"access_token":"new acceess token","refresh_token":"dummyAccessToken","developer_token":"dummydeveloperToken"}`),
				}, Err: "",
				ErrorMessage: "",
			}
			ctrl := gomock.NewController(GinkgoT())
			mockCpConnector := mock_oauthV2.NewMockConnector(ctrl)
			mockCpConnector.EXPECT().CpApiCall(gomock.Any()).Return(http.StatusOK, `{"options":{},"id":"2BFzzzID8kITtU7AxxWtrn9KQQf","createdAt":"2022-06-29T15:34:47.758Z","updatedAt":"2024-02-12T12:18:35.213Z","workspaceId":"1oVajb9QqG50undaAcokNlYyJQa","name":"dummy user","role":"google_adwords_enhanced_conversions_v1","userId":"1oVadeaoGXN2pataEEoeIaXS3bO","metadata":{"userId":"115538421777182389816","displayName":"dummy user","email":""},"secretVersion":50,"rudderCategory":"destination","secret":{"access_token":"new acceess token","refresh_token":"dummyAccessToken","developer_token":"dummydeveloperToken"}}`).Times(1)
			mockTokenProvider := mock_oauthV2.NewMockTokenProvider(ctrl)
			mockTokenProvider.EXPECT().Identity().Return(nil)
			oauthHandler := v2.NewOAuthHandler(mockTokenProvider,
				v2.WithCache(v2.NewCache()),
				v2.WithLocker(kitsync.NewPartitionRWLocker()),
				v2.WithStats(stats.Default),
				v2.WithLogger(logger.NewLogger().Child("MockOAuthHandler")),
				v2.WithCpConnector(mockCpConnector),
			)

			storedAuthResponse := &v2.AuthResponse{
				Account: v2.AccountSecret{
					Secret: []byte(`{"access_token":"storedAccessToken","refresh_token":"dummyAccessToken","developer_token":"dummyDeveloperToken"}`),
				}, Err: "",
				ErrorMessage: "",
			}
			oauthHandler.Cache.Store(refreshTokenParams.AccountID, storedAuthResponse)
			wg := sync.WaitGroup{}

			for i := 0; i < 20; i++ {
				statusCode, response, err := oauthHandler.RefreshToken(refreshTokenParams)
				// Assertions
				Expect(statusCode).To(Equal(http.StatusOK))
				Expect(response).To(Equal(expectedResponse))
				Expect(err).To(BeNil())
				token, _ := oauthHandler.Cache.Load(refreshTokenParams.AccountID)
				Expect(token.(*v2.AuthResponse)).To(Equal(expectedResponse))
			}

			for i := 0; i < 20; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					statusCode, response, err := oauthHandler.RefreshToken(refreshTokenParams)
					// Assertions
					Expect(statusCode).To(Equal(http.StatusOK))
					Expect(response).To(Equal(expectedResponse))
					Expect(err).To(BeNil())
					token, _ := oauthHandler.Cache.Load(refreshTokenParams.AccountID)
					Expect(token.(*v2.AuthResponse)).To(Equal(expectedResponse))
				}()
			}
			wg.Wait()
		})
	})
})
