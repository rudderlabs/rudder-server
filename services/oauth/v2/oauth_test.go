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
		It("returns an error when the token is not found", func() {
			fetchTokenParams := &v2.RefreshTokenParams{
				AccountId:   "123",
				WorkspaceId: "456",
				DestDefName: "testDest",
				Secret:      []byte("testSecret"),
			}

			ctrl := gomock.NewController(GinkgoT())
			mockCpConnector := mock_oauthV2.NewMockControlPlaneConnectorI(ctrl)
			mockCpConnector.EXPECT().CpApiCall(gomock.Any()).Return(http.StatusOK, `{"options":{},"id":"2BFzzzID8kITtU7AxxWtrn9KQQf","createdAt":"2022-06-29T15:34:47.758Z","updatedAt":"2024-02-12T12:18:35.213Z","workspaceId":"1oVajb9QqG50undaAcokNlYyJQa","name":"Sudip Paul","role":"google_adwords_enhanced_conversions_v1","userId":"1oVadeaoGXN2pataEEoeIaXS3bO","metadata":{"userId":"115538421777182389816","displayName":"Sudip Paul","email":"sudip@rudderstack.com"},"secretVersion":50,"rudderCategory":"destination","secret":{"access_token":"ya29.a0AfB_byCCnrOczk_aBO9V-0tL1WvvjPN_YyHXnJ3dYFDMnmpS0PeviEVg2oFRPhZ_WD3QZA5qoPGCSQ5tBkezj6ivgAqeGwIMKpLGFU-gKc9Umv-6DMMTR9EmKYVL6uJ7C-2dB1lrFAqrqWsgzUur4UpqWEZQK-0oqNEiT5gaCgYKAScSARMSFQHGX2MioCSrtGiFOoe38-j8hFB0Bg0174","refresh_token":"1//0dKRUQivYgldBCgYIARAAGA0SNwF-L9IroOezXhK9la3zXXY68ek0eiJv3xpV-BPAzdRLqYGEbDRPc2ePA2sLOAOUVWIkoTMvaq4","developer_token":"UZkP2EcIdKrapfR29hqIUg"}}`)
			mockCpConnector.EXPECT().CpApiCall(gomock.Any()).Return(http.StatusOK, `{"options":{},"id":"2BFzzzID8kITtU7AxxWtrn9KQQf","createdAt":"2022-06-29T15:34:47.758Z","updatedAt":"2024-02-12T12:18:35.213Z","workspaceId":"1oVajb9QqG50undaAcokNlYyJQa","name":"Sudip Paul","role":"google_adwords_enhanced_conversions_v1","userId":"1oVadeaoGXN2pataEEoeIaXS3bO","metadata":{"userId":"115538421777182389816","displayName":"Sudip Paul","email":"sudip@rudderstack.com"},"secretVersion":50,"rudderCategory":"destination","secret":{"access_token":"ya29.a0AfB_byCCnrOczk_aBO9V-0tL1WvvjPN_YyHXnJ3dYFDMnmpS0PeviEVg2oFRPhZ_WD3QZA5qoPGCSQ5tBkezj6ivgAqeGwIMKpLGFU-gKc9Umv-6DMMTR9EmKYVL6uJ7C-2dB1lrFAqrqWsgzUur4UpqWEZQK-0oqNEiT5gaCgYKAScSARMSFQHGX2MioCSrtGiFOoe38-j8hFB0Bg0174","refresh_token":"1//0dKRUQivYgldBCgYIARAAGA0SNwF-L9IroOezXhK9la3zXXY68ek0eiJv3xpV-BPAzdRLqYGEbDRPc2ePA2sLOAOUVWIkoTMvaq4","developer_token":"UZkP2EcIdKrapfR29hqIUg"}}`)
			mockTokenProvider := mock_oauthV2.NewMockTokenProvider(ctrl)
			mockTokenProvider.EXPECT().Identity().Return(nil)
			mockTokenProvider.EXPECT().Identity().Return(nil)

			expectedResponse := &v2.AuthResponse{
				Account: v2.AccountSecret{
					Secret: []byte(`{"access_token":"ya29.a0AfB_byCCnrOczk_aBO9V-0tL1WvvjPN_YyHXnJ3dYFDMnmpS0PeviEVg2oFRPhZ_WD3QZA5qoPGCSQ5tBkezj6ivgAqeGwIMKpLGFU-gKc9Umv-6DMMTR9EmKYVL6uJ7C-2dB1lrFAqrqWsgzUur4UpqWEZQK-0oqNEiT5gaCgYKAScSARMSFQHGX2MioCSrtGiFOoe38-j8hFB0Bg0174","refresh_token":"1//0dKRUQivYgldBCgYIARAAGA0SNwF-L9IroOezXhK9la3zXXY68ek0eiJv3xpV-BPAzdRLqYGEbDRPc2ePA2sLOAOUVWIkoTMvaq4","developer_token":"UZkP2EcIdKrapfR29hqIUg"}`),
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
			Expect(statusCode).To(Equal(http.StatusInternalServerError))
			Expect(response).To(Equal(expectedResponse))
			Expect(err).To(BeNil())
			token, _ := oauthHandler.Cache.Get(fetchTokenParams.AccountId)

			Expect(token.(*v2.AuthResponse)).To(Equal(expectedResponse))
		})
	})
})
