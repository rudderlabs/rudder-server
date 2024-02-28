package v2_test

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/rudderlabs/rudder-go-kit/logger"
	rudderSync "github.com/rudderlabs/rudder-go-kit/sync"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	mock_oauthV2 "github.com/rudderlabs/rudder-server/mocks/services/oauthV2"
	v2 "github.com/rudderlabs/rudder-server/services/oauth/v2"
)

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
				AccountId:   "123",
				WorkspaceId: "456",
				DestDefName: "testDest",
			}

			ctrl := gomock.NewController(GinkgoT())
			mockCpConnector := mock_oauthV2.NewMockControlPlaneConnectorI(ctrl)
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
			oauthHandler := &v2.OAuthHandler{
				CacheMutex:    rudderSync.NewPartitionRWLocker(),
				Cache:         v2.NewCache(),
				CpConn:        mockCpConnector,
				TokenProvider: mockTokenProvider,
				Logger:        logger.NewLogger().Child("MockOAuthHandler"),
			}
			statusCode, response, err := oauthHandler.FetchToken(fetchTokenParams)
			// Assertions
			Expect(statusCode).To(Equal(http.StatusOK))
			Expect(response).To(Equal(expectedResponse))
			Expect(err).To(BeNil())
			token, _ := oauthHandler.Cache.Get(fetchTokenParams.AccountId)
			Expect(token.(*v2.AuthResponse)).To(Equal(expectedResponse))
		})

		It("fetch token function call when cache is not empty and token is not expired", func() {
			fetchTokenParams := &v2.RefreshTokenParams{
				AccountId:   "123",
				WorkspaceId: "456",
				DestDefName: "testDest",
			}

			expectedResponse := &v2.AuthResponse{
				Account: v2.AccountSecret{
					Secret: []byte(`{"access_token":"StoredDummyaccesstoken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken"}`),
				},
				Err:          "",
				ErrorMessage: "",
			}

			// Invoke code under test
			oauthHandler := &v2.OAuthHandler{
				CacheMutex: rudderSync.NewPartitionRWLocker(),
				Cache:      v2.NewCache(),
				Logger:     logger.NewLogger().Child("MockOAuthHandler"),
			}
			storedAuthResponse := &v2.AuthResponse{
				Account: v2.AccountSecret{
					Secret: []byte(`{"access_token":"StoredDummyaccesstoken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken"}`),
				},
				Err:          "",
				ErrorMessage: "",
			}
			oauthHandler.Cache.Set(fetchTokenParams.AccountId, storedAuthResponse)
			statusCode, response, err := oauthHandler.FetchToken(fetchTokenParams)
			// Assertions
			Expect(statusCode).To(Equal(http.StatusOK))
			Expect(response).To(Equal(expectedResponse))
			Expect(err).To(BeNil())
			token, _ := oauthHandler.Cache.Get(fetchTokenParams.AccountId)
			// We are checking if the token is updated in the cache or not
			Expect(token.(*v2.AuthResponse)).To(Equal(storedAuthResponse))
		})

		It("fetch token function call when cache is not empty and token is expired", func() {
			fetchTokenParams := &v2.RefreshTokenParams{
				AccountId:   "123",
				WorkspaceId: "456",
				DestDefName: "testDest",
			}

			ctrl := gomock.NewController(GinkgoT())
			mockCpConnector := mock_oauthV2.NewMockControlPlaneConnectorI(ctrl)
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
			oauthHandler := &v2.OAuthHandler{
				CacheMutex:    rudderSync.NewPartitionRWLocker(),
				Cache:         v2.NewCache(),
				CpConn:        mockCpConnector,
				TokenProvider: mockTokenProvider,
				Logger:        logger.NewLogger().Child("MockOAuthHandler"),
			}
			storedAuthResponse := &v2.AuthResponse{
				Account: v2.AccountSecret{
					Secret:         []byte(`{"access_token":"StoredDummyaccesstoken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken","expirationDate":"2022-06-29T15:34:47.758Z"}`),
					ExpirationDate: "",
				},
				Err:          "",
				ErrorMessage: "",
			}
			oauthHandler.Cache.Set(fetchTokenParams.AccountId, storedAuthResponse)
			statusCode, response, err := oauthHandler.FetchToken(fetchTokenParams)
			Expect(statusCode).To(Equal(http.StatusOK))
			Expect(response).To(Equal(expectedResponse))
			Expect(err).To(BeNil())
			token, _ := oauthHandler.Cache.Get(fetchTokenParams.AccountId)
			// We are checking if the token is updated in the cache or not
			Expect(token.(*v2.AuthResponse)).NotTo(Equal(storedAuthResponse))
		})

		It("fetch token function call when cache is empty and cpApiCall returns empty response", func() {
			fetchTokenParams := &v2.RefreshTokenParams{
				AccountId:   "123",
				WorkspaceId: "456",
				DestDefName: "testDest",
			}

			ctrl := gomock.NewController(GinkgoT())
			mockCpConnector := mock_oauthV2.NewMockControlPlaneConnectorI(ctrl)
			mockCpConnector.EXPECT().CpApiCall(gomock.Any()).Return(http.StatusNoContent, ``)
			mockTokenProvider := mock_oauthV2.NewMockTokenProvider(ctrl)
			mockTokenProvider.EXPECT().Identity().Return(nil)
			oauthHandler := &v2.OAuthHandler{
				CacheMutex:    rudderSync.NewPartitionRWLocker(),
				Cache:         v2.NewCache(),
				CpConn:        mockCpConnector,
				TokenProvider: mockTokenProvider,
				Logger:        logger.NewLogger().Child("MockOAuthHandler"),
			}
			statusCode, response, err := oauthHandler.FetchToken(fetchTokenParams)
			Expect(statusCode).To(Equal(http.StatusInternalServerError))
			var expectedResponse *v2.AuthResponse
			Expect(response).To(Equal(expectedResponse))

			Expect(err).To(MatchError(fmt.Errorf("empty secret")))
		})

		It("fetch token function call when cache is empty and cpApiCall returns a failed response", func() {
			fetchTokenParams := &v2.RefreshTokenParams{
				AccountId:   "123",
				WorkspaceId: "456",
				DestDefName: "testDest",
			}

			ctrl := gomock.NewController(GinkgoT())
			mockCpConnector := mock_oauthV2.NewMockControlPlaneConnectorI(ctrl)
			mockCpConnector.EXPECT().CpApiCall(gomock.Any()).Return(http.StatusNoContent, `{
				"body":{
				  "code":"ref_token_invalid_grant",
				  "message":"invalid_grant error, refresh token has expired or revoked"
				}
			  }`)
			mockTokenProvider := mock_oauthV2.NewMockTokenProvider(ctrl)
			mockTokenProvider.EXPECT().Identity().Return(nil)
			oauthHandler := &v2.OAuthHandler{
				CacheMutex:    rudderSync.NewPartitionRWLocker(),
				Cache:         v2.NewCache(),
				CpConn:        mockCpConnector,
				TokenProvider: mockTokenProvider,
				Logger:        logger.NewLogger().Child("MockOAuthHandler"),
			}
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
				AccountId:   "123",
				WorkspaceId: "456",
				DestDefName: "testDest",
			}

			ctrl := gomock.NewController(GinkgoT())
			mockHttpClient := mock_oauthV2.NewMockHttpClient(ctrl)
			mockHttpClient.EXPECT().Do(gomock.Any()).Return(&http.Response{}, &net.OpError{
				Op:     "mock",
				Net:    "mock",
				Source: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 1234},
				Addr:   &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 12340},
				Err:    &os.SyscallError{Syscall: "read", Err: syscall.ECONNREFUSED},
			})
			cpConnector := v2.NewControlPlaneConnector(
				v2.WithClient(mockHttpClient),
				v2.WithParentLogger(logger.NewLogger().Child("ControlPlaneConnector")),
			)

			mockTokenProvider := mock_oauthV2.NewMockTokenProvider(ctrl)
			mockTokenProvider.EXPECT().Identity().Return(&mock_oauthV2.BasicAuthMock{})
			oauthHandler := &v2.OAuthHandler{
				CacheMutex:    rudderSync.NewPartitionRWLocker(),
				Cache:         v2.NewCache(),
				CpConn:        cpConnector,
				TokenProvider: mockTokenProvider,
				Logger:        logger.NewLogger().Child("MockOAuthHandler"),
			}
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
				AccountId:   "123",
				WorkspaceId: "456",
				DestDefName: "testDest",
			}

			ctrl := gomock.NewController(GinkgoT())
			mockHttpClient := mock_oauthV2.NewMockHttpClient(ctrl)
			mockHttpClient.EXPECT().Do(gomock.Any()).Return(&http.Response{}, &net.OpError{
				Op:     "mock",
				Net:    "mock",
				Source: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 1234},
				Addr:   &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 12340},
				Err:    &os.SyscallError{Syscall: "read", Err: syscall.ETIMEDOUT},
			})
			cpConnector := v2.NewControlPlaneConnector(
				v2.WithClient(mockHttpClient),
				v2.WithParentLogger(logger.NewLogger().Child("ControlPlaneConnector")),
			)

			mockTokenProvider := mock_oauthV2.NewMockTokenProvider(ctrl)
			mockTokenProvider.EXPECT().Identity().Return(&mock_oauthV2.BasicAuthMock{})
			oauthHandler := &v2.OAuthHandler{
				CacheMutex:    rudderSync.NewPartitionRWLocker(),
				Cache:         v2.NewCache(),
				CpConn:        cpConnector,
				TokenProvider: mockTokenProvider,
				Logger:        logger.NewLogger().Child("MockOAuthHandler"),
			}
			statusCode, response, err := oauthHandler.FetchToken(fetchTokenParams)
			Expect(statusCode).To(Equal(http.StatusInternalServerError))
			expectedResponse := &v2.AuthResponse{
				Err:          "timeout",
				ErrorMessage: "mock mock 127.0.0.1:1234->127.0.0.1:12340: read: operation timed out",
			}
			Expect(response).To(Equal(expectedResponse))
			Expect(err).To(MatchError(fmt.Errorf("error occurred while fetching/refreshing account info from CP: mock mock 127.0.0.1:1234->127.0.0.1:12340: read: operation timed out")))
		})
	})

	Describe("Test RefreshToken function", func() {
		It("refreshToken function call when stored cache is same as provided secret", func() {
			refreshTokenParams := &v2.RefreshTokenParams{
				AccountId:   "123",
				WorkspaceId: "456",
				DestDefName: "testDest",
				Secret:      []byte(`{"access_token":"storedAccessToken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken"}`),
			}

			ctrl := gomock.NewController(GinkgoT())
			mockCpConnector := mock_oauthV2.NewMockControlPlaneConnectorI(ctrl)
			mockCpConnector.EXPECT().CpApiCall(gomock.Any()).Return(http.StatusOK, `{"options":{},"id":"2BFzzzID8kITtU7AxxWtrn9KQQf","createdAt":"2022-06-29T15:34:47.758Z","updatedAt":"2024-02-12T12:18:35.213Z","workspaceId":"1oVajb9QqG50undaAcokNlYyJQa","name":"dummy user","role":"google_adwords_enhanced_conversions_v1","userId":"1oVadeaoGXN2pataEEoeIaXS3bO","metadata":{"userId":"115538421777182389816","displayName":"dummy user","email":"dummy@testmail.com"},"secretVersion":50,"rudderCategory":"destination","secret":{"access_token":"newAccessToken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken"}}`)

			mockTokenProvider := mock_oauthV2.NewMockTokenProvider(ctrl)
			mockTokenProvider.EXPECT().Identity().Return(nil)

			// Invoke code under test
			oauthHandler := &v2.OAuthHandler{
				CacheMutex:    rudderSync.NewPartitionRWLocker(),
				Cache:         v2.NewCache(),
				CpConn:        mockCpConnector,
				TokenProvider: mockTokenProvider,
				Logger:        logger.NewLogger().Child("MockOAuthHandler"),
			}
			storedAuthResponse := &v2.AuthResponse{
				Account: v2.AccountSecret{
					Secret: []byte(`{"access_token":"storedAccessToken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken"}`),
				}, Err: "",
				ErrorMessage: "",
			}
			oauthHandler.Cache.Set(refreshTokenParams.AccountId, storedAuthResponse)
			statusCode, _, err := oauthHandler.RefreshToken(refreshTokenParams)
			// Assertions
			Expect(statusCode).To(Equal(http.StatusOK))
			Expect(err).To(BeNil())
			token, _ := oauthHandler.Cache.Get(refreshTokenParams.AccountId)
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
				AccountId:   "123",
				WorkspaceId: "456",
				DestDefName: "testDest",
				Secret:      []byte(`{"access_token":"providedAccessToken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken"}`),
			}

			// Invoke code under test
			oauthHandler := &v2.OAuthHandler{
				CacheMutex: rudderSync.NewPartitionRWLocker(),
				Cache:      v2.NewCache(),
				Logger:     logger.NewLogger().Child("MockOAuthHandler"),
			}
			storedAuthResponse := &v2.AuthResponse{
				Account: v2.AccountSecret{
					Secret: []byte(`{"access_token":"storedAccessToken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken"}`),
				}, Err: "",
				ErrorMessage: "",
			}
			oauthHandler.Cache.Set(refreshTokenParams.AccountId, storedAuthResponse)
			token, _ := oauthHandler.Cache.Get(refreshTokenParams.AccountId)
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
				AccountId:   "123",
				WorkspaceId: "456",
				DestDefName: "testDest",
				Secret:      []byte(`{"access_token":"storedAccessToken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken"}`),
				Destination: &v2.DestinationInfo{
					DestDefName:   "testDest",
					DestinationId: "Destination123",
				},
			}

			ctrl := gomock.NewController(GinkgoT())
			mockCpConnector := mock_oauthV2.NewMockControlPlaneConnectorI(ctrl)

			mockCpConnector.EXPECT().CpApiCall(gomock.Any()).Return(http.StatusBadRequest, `{
				"body":{
				  "code":"ref_token_invalid_grant",
				  "message":"invalid_grant error, refresh token has expired or revoked"
				}
			  }`)

			mockTokenProvider := mock_oauthV2.NewMockTokenProvider(ctrl)
			mockTokenProvider.EXPECT().Identity().Return(nil)

			// Invoke code under test
			oauthHandler := &v2.OAuthHandler{
				CacheMutex:    rudderSync.NewPartitionRWLocker(),
				Cache:         v2.NewCache(),
				CpConn:        mockCpConnector,
				TokenProvider: mockTokenProvider,
				Logger:        logger.NewLogger().Child("MockOAuthHandler"),
			}
			storedAuthResponse := &v2.AuthResponse{
				Account: v2.AccountSecret{
					Secret: []byte(`{"access_token":"storedAccessToken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken"}`),
				}, Err: "",
				ErrorMessage: "",
			}
			oauthHandler.Cache.Set(refreshTokenParams.AccountId, storedAuthResponse)
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
				AccountId:   "123",
				WorkspaceId: "456",
				DestDefName: "testDest",
				Secret:      []byte(`{"access_token":"storedAccessToken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken"}`),
				Destination: &v2.DestinationInfo{
					DestDefName:   "testDest",
					DestinationId: "Destination123",
				},
			}

			ctrl := gomock.NewController(GinkgoT())
			mockHttpClient := mock_oauthV2.NewMockHttpClient(ctrl)
			mockHttpClient.EXPECT().Do(gomock.Any()).Return(&http.Response{}, &net.OpError{
				Op:     "mock",
				Net:    "mock",
				Source: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 1234},
				Addr:   &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 12340},
				Err:    &os.SyscallError{Syscall: "read", Err: syscall.ECONNREFUSED},
			})
			cpConnector := v2.NewControlPlaneConnector(
				v2.WithClient(mockHttpClient),
				v2.WithParentLogger(logger.NewLogger().Child("ControlPlaneConnector")),
			)

			mockTokenProvider := mock_oauthV2.NewMockTokenProvider(ctrl)
			mockTokenProvider.EXPECT().Identity().Return(&mock_oauthV2.BasicAuthMock{})
			oauthHandler := &v2.OAuthHandler{
				CacheMutex:    rudderSync.NewPartitionRWLocker(),
				Cache:         v2.NewCache(),
				CpConn:        cpConnector,
				TokenProvider: mockTokenProvider,
				Logger:        logger.NewLogger().Child("MockOAuthHandler"),
			}
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
				AccountId:   "123",
				WorkspaceId: "456",
				DestDefName: "testDest",
				Secret:      []byte(`{"access_token":"storedAccessToken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken"}`),
				Destination: &v2.DestinationInfo{
					DestDefName:   "testDest",
					DestinationId: "Destination123",
				},
			}

			ctrl := gomock.NewController(GinkgoT())
			mockHttpClient := mock_oauthV2.NewMockHttpClient(ctrl)
			mockHttpClient.EXPECT().Do(gomock.Any()).Return(&http.Response{}, &net.OpError{
				Op:     "mock",
				Net:    "mock",
				Source: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 1234},
				Addr:   &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 12340},
				Err:    &os.SyscallError{Syscall: "read", Err: syscall.ETIMEDOUT},
			})
			cpConnector := v2.NewControlPlaneConnector(
				v2.WithClient(mockHttpClient),
				v2.WithParentLogger(logger.NewLogger().Child("ControlPlaneConnector")),
			)

			mockTokenProvider := mock_oauthV2.NewMockTokenProvider(ctrl)
			mockTokenProvider.EXPECT().Identity().Return(&mock_oauthV2.BasicAuthMock{})
			oauthHandler := &v2.OAuthHandler{
				CacheMutex:    rudderSync.NewPartitionRWLocker(),
				Cache:         v2.NewCache(),
				CpConn:        cpConnector,
				TokenProvider: mockTokenProvider,
				Logger:        logger.NewLogger().Child("MockOAuthHandler"),
			}
			statusCode, response, err := oauthHandler.RefreshToken(refreshTokenParams)
			Expect(statusCode).To(Equal(http.StatusInternalServerError))
			expectedResponse := &v2.AuthResponse{
				Err:          "timeout",
				ErrorMessage: "mock mock 127.0.0.1:1234->127.0.0.1:12340: read: operation timed out",
			}
			Expect(response).To(Equal(expectedResponse))
			Expect(err).To(MatchError(fmt.Errorf("error occurred while fetching/refreshing account info from CP: mock mock 127.0.0.1:1234->127.0.0.1:12340: read: operation timed out")))
		})
	})

	Describe("Test AuthStatusToggle function", func() {
		It("authStatusToggle function call when config backend api call failed", func() {
			ctrl := gomock.NewController(GinkgoT())
			mockTokenProvider := mock_oauthV2.NewMockTokenProvider(ctrl)
			mockTokenProvider.EXPECT().Identity().Return(nil)
			mockCpConnector := mock_oauthV2.NewMockControlPlaneConnectorI(ctrl)
			oauthHandler := &v2.OAuthHandler{
				CacheMutex:                rudderSync.NewPartitionRWLocker(),
				Cache:                     v2.NewCache(),
				Logger:                    logger.NewLogger().Child("MockOAuthHandler"),
				AuthStatusUpdateActiveMap: make(map[string]bool),
				TokenProvider:             mockTokenProvider,
				CpConn:                    mockCpConnector,
			}
			mockCpConnector.EXPECT().CpApiCall(gomock.Any()).Return(http.StatusBadRequest, `{
				  "message":"unable to update the auth status for the destination"
			  }`)
			statusCode, response := oauthHandler.AuthStatusToggle(&v2.AuthStatusToggleParams{
				Destination: &v2.DestinationInfo{
					DestDefName:   "testDest",
					DestinationId: "Destination123",
				},
				WorkspaceId:     "workspaceID",
				RudderAccountId: "rudderAccountId",
				StatPrefix:      "AuthStatusInactive",
				AuthStatus:      v2.CategoryAuthStatusInactive,
			})
			Expect(statusCode).To(Equal(http.StatusBadRequest))
			Expect(response).To(Equal("Problem with user permission or access/refresh token have been revoked"))
		})
		It("authStatusToggle function call when config backend api call succeeded", func() {
			ctrl := gomock.NewController(GinkgoT())
			mockTokenProvider := mock_oauthV2.NewMockTokenProvider(ctrl)
			mockTokenProvider.EXPECT().Identity().Return(nil)
			mockCpConnector := mock_oauthV2.NewMockControlPlaneConnectorI(ctrl)
			oauthHandler := &v2.OAuthHandler{
				CacheMutex:                rudderSync.NewPartitionRWLocker(),
				Cache:                     v2.NewCache(),
				Logger:                    logger.NewLogger().Child("MockOAuthHandler"),
				AuthStatusUpdateActiveMap: make(map[string]bool),
				TokenProvider:             mockTokenProvider,
				CpConn:                    mockCpConnector,
			}
			mockCpConnector.EXPECT().CpApiCall(gomock.Any()).Return(http.StatusOK, ``)
			statusCode, response := oauthHandler.AuthStatusToggle(&v2.AuthStatusToggleParams{
				Destination: &v2.DestinationInfo{
					DestDefName:   "testDest",
					DestinationId: "Destination123",
				},
				WorkspaceId:     "workspaceID",
				RudderAccountId: "rudderAccountId",
				StatPrefix:      "AuthStatusInactive",
				AuthStatus:      v2.CategoryAuthStatusInactive,
			})
			Expect(statusCode).To(Equal(http.StatusBadRequest))
			Expect(response).To(Equal("Problem with user permission or access/refresh token have been revoked"))
		})
		It("authStatusToggle function call when config backend api call failed due to config backend service is failed due to timeout", func() {
			ctrl := gomock.NewController(GinkgoT())
			mockHttpClient := mock_oauthV2.NewMockHttpClient(ctrl)
			mockHttpClient.EXPECT().Do(gomock.Any()).Return(&http.Response{
				StatusCode: http.StatusRequestTimeout,
			}, &net.OpError{
				Op:     "mock",
				Net:    "mock",
				Source: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 1234},
				Addr:   &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 12340},
				Err:    &os.SyscallError{Syscall: "read", Err: syscall.ETIMEDOUT},
			})
			cpConnector := v2.NewControlPlaneConnector(
				v2.WithClient(mockHttpClient),
				v2.WithParentLogger(logger.NewLogger().Child("ControlPlaneConnector")),
			)

			mockTokenProvider := mock_oauthV2.NewMockTokenProvider(ctrl)
			mockTokenProvider.EXPECT().Identity().Return(&mock_oauthV2.BasicAuthMock{})
			oauthHandler := &v2.OAuthHandler{
				CacheMutex:                rudderSync.NewPartitionRWLocker(),
				Cache:                     v2.NewCache(),
				CpConn:                    cpConnector,
				TokenProvider:             mockTokenProvider,
				Logger:                    logger.NewLogger().Child("MockOAuthHandler"),
				AuthStatusUpdateActiveMap: make(map[string]bool),
			}
			statusCode, response := oauthHandler.AuthStatusToggle(&v2.AuthStatusToggleParams{
				Destination: &v2.DestinationInfo{
					DestDefName:   "testDest",
					DestinationId: "Destination123",
				},
				WorkspaceId:     "workspaceID",
				RudderAccountId: "rudderAccountId",
				StatPrefix:      "AuthStatusInactive",
				AuthStatus:      v2.CategoryAuthStatusInactive,
			})
			Expect(statusCode).To(Equal(http.StatusBadRequest))
			Expect(response).To(Equal("Problem with user permission or access/refresh token have been revoked"))
		})
	})

	Describe("Test FetchToken with multiple go routines", func() {
		It("fetch token function call when cache is not empty", func() {
			fetchTokenParams := &v2.RefreshTokenParams{
				AccountId:   "123",
				WorkspaceId: "456",
				DestDefName: "testDest",
			}
			// Invoke code under test
			expectedResponse := &v2.AuthResponse{
				Account: v2.AccountSecret{
					Secret: []byte(`{"access_token":"new acceess token","refresh_token":"dummyAccessToken","developer_token":"dummydeveloperToken"}`),
				}, Err: "",
				ErrorMessage: "",
			}

			ctrl := gomock.NewController(GinkgoT())
			mockCpConnector := mock_oauthV2.NewMockControlPlaneConnectorI(ctrl)
			mockCpConnector.EXPECT().CpApiCall(gomock.Any()).Times(0)

			oauthHandler := &v2.OAuthHandler{
				CacheMutex: rudderSync.NewPartitionRWLocker(),
				Cache:      v2.NewCache(),
				Logger:     logger.NewLogger().Child("MockOAuthHandler"),
				CpConn:     mockCpConnector,
			}
			storedAuthResponse := &v2.AuthResponse{
				Account: v2.AccountSecret{
					Secret: []byte(`{"access_token":"new acceess token","refresh_token":"dummyAccessToken","developer_token":"dummydeveloperToken"}`),
				}, Err: "",
				ErrorMessage: "",
			}
			oauthHandler.Cache.Set(fetchTokenParams.AccountId, storedAuthResponse)
			wg := sync.WaitGroup{}
			for i := 0; i < 20; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					statusCode, response, err := oauthHandler.FetchToken(fetchTokenParams)
					// Assertions
					Expect(statusCode).To(Equal(http.StatusOK))
					Expect(response).To(Equal(expectedResponse))
					Expect(err).To(BeNil())
					token, _ := oauthHandler.Cache.Get(fetchTokenParams.AccountId)
					Expect(token.(*v2.AuthResponse)).To(Equal(expectedResponse))
				}()
			}
			wg.Wait()

			// Invoke code under test
		})

		It("fetch token function call when cache is empty", func() {
			fetchTokenParams := &v2.RefreshTokenParams{
				AccountId:   "123",
				WorkspaceId: "456",
				DestDefName: "testDest",
			}
			// Invoke code under test
			expectedResponse := &v2.AuthResponse{
				Account: v2.AccountSecret{
					Secret: []byte(`{"access_token":"new 1234 acceess token","refresh_token":"dummyAccessToken","developer_token":"dummydeveloperToken"}`),
				}, Err: "",
				ErrorMessage: "",
			}
			ctrl := gomock.NewController(GinkgoT())
			mockCpConnector := mock_oauthV2.NewMockControlPlaneConnectorI(ctrl)
			mockCpConnector.EXPECT().CpApiCall(gomock.Any()).Return(http.StatusOK, `{"options":{},"id":"2BFzzzID8kITtU7AxxWtrn9KQQf","createdAt":"2022-06-29T15:34:47.758Z","updatedAt":"2024-02-12T12:18:35.213Z","workspaceId":"1oVajb9QqG50undaAcokNlYyJQa","name":"dummy user","role":"google_adwords_enhanced_conversions_v1","userId":"1oVadeaoGXN2pataEEoeIaXS3bO","metadata":{"userId":"115538421777182389816","displayName":"dummy user","email":""},"secretVersion":50,"rudderCategory":"destination","secret":{"access_token":"new 1234 acceess token","refresh_token":"dummyAccessToken","developer_token":"dummydeveloperToken"}}`).Times(1)
			mockTokenProvider := mock_oauthV2.NewMockTokenProvider(ctrl)
			mockTokenProvider.EXPECT().Identity().Return(nil)
			oauthHandler := &v2.OAuthHandler{
				CacheMutex:    rudderSync.NewPartitionRWLocker(),
				Cache:         v2.NewCache(),
				Logger:        logger.NewLogger().Child("MockOAuthHandler"),
				CpConn:        mockCpConnector,
				TokenProvider: mockTokenProvider,
			}
			wg := sync.WaitGroup{}
			for i := 0; i < 20; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					statusCode, response, err := oauthHandler.FetchToken(fetchTokenParams)
					// Assertions
					Expect(statusCode).To(Equal(http.StatusOK))
					Expect(response).To(Equal(expectedResponse))
					Expect(err).To(BeNil())
					token, _ := oauthHandler.Cache.Get(fetchTokenParams.AccountId)
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
				AccountId:   "123",
				WorkspaceId: "456",
				DestDefName: "testDest",
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
			mockCpConnector := mock_oauthV2.NewMockControlPlaneConnectorI(ctrl)
			mockCpConnector.EXPECT().CpApiCall(gomock.Any()).Return(http.StatusOK, `{"options":{},"id":"2BFzzzID8kITtU7AxxWtrn9KQQf","createdAt":"2022-06-29T15:34:47.758Z","updatedAt":"2024-02-12T12:18:35.213Z","workspaceId":"1oVajb9QqG50undaAcokNlYyJQa","name":"dummy user","role":"google_adwords_enhanced_conversions_v1","userId":"1oVadeaoGXN2pataEEoeIaXS3bO","metadata":{"userId":"115538421777182389816","displayName":"dummy user","email":""},"secretVersion":50,"rudderCategory":"destination","secret":{"access_token":"new acceess token","refresh_token":"dummyAccessToken","developer_token":"dummydeveloperToken"}}`).Times(1)
			mockTokenProvider := mock_oauthV2.NewMockTokenProvider(ctrl)
			mockTokenProvider.EXPECT().Identity().Return(nil)
			oauthHandler := &v2.OAuthHandler{
				CacheMutex:    rudderSync.NewPartitionRWLocker(),
				Cache:         v2.NewCache(),
				Logger:        logger.NewLogger().Child("MockOAuthHandler"),
				CpConn:        mockCpConnector,
				TokenProvider: mockTokenProvider,
			}

			storedAuthResponse := &v2.AuthResponse{
				Account: v2.AccountSecret{
					Secret: []byte(`{"access_token":"storedAccessToken","refresh_token":"dummyAccessToken","developer_token":"dummyDeveloperToken"}`),
				}, Err: "",
				ErrorMessage: "",
			}
			oauthHandler.Cache.Set(refreshTokenParams.AccountId, storedAuthResponse)
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
					token, _ := oauthHandler.Cache.Get(refreshTokenParams.AccountId)
					Expect(token.(*v2.AuthResponse)).To(Equal(expectedResponse))
				}()
			}
			wg.Wait()
		})
	})
})
