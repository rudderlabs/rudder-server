package v2_test

import (
	"net/http"

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
		})
	})
	Describe("Test FetchToken function", func() {
		/**
		1. CpApiCall returns a plain string(non-empty)
		2. CpApiCall returns empty response
		3. CpApiCall returns a new token when the token is expired
		4. CpApiCall returns an error when the token is not found
		*/
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
					Secret:         []byte(`{"access_token":"StoredDummyaccesstoken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken"}`),
					ExpirationDate: "2022-06-29T15:34:47.758Z",
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
			Expect(err).To(MatchError("empty secret"))
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
			Expect(err).To(MatchError("invalid grant"))
		})
	})
})
