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
		It("returns a pointer to OAuthHandler with default values", func() {
			testOauthHandler := v2.NewOAuthHandler(backendconfig.DefaultBackendConfig)
			Expect(testOauthHandler).To(Not(BeNil()))
			Expect(testOauthHandler.CpConn).To(Not(BeNil()))
			// Expect(testOauthHandler.Logger).To(Not(BeNil()))
			Expect(testOauthHandler.Cache).To(Not(BeNil()))
			Expect(testOauthHandler.Lock).To(Not(BeNil()))
			Expect(testOauthHandler.AuthStatusUpdateActiveMap).To(Not(BeNil()))
		})
	})
	// func GetTokenInfo(i I) {
	// 	i.MethodA(0)
	// }
	Describe("Test FetchToken function", func() {
		It("returns an error when the token is not found", func() {
			fetchTokenParams := &v2.RefreshTokenParams{
				AccountId:   "123",
				WorkspaceId: "456",
				DestDefName: "testDest",
				Secret:      []byte("testSecret"),
			}
			// authResponse := &v2.AuthResponse{
			// 	Account: v2.AccountSecret{
			// 		Secret: []byte("newSecret"),
			// 	},
			// }

			ctrl := gomock.NewController(GinkgoT())
			mockCpConnector := mock_oauthV2.NewMockControlPlaneConnectorI(ctrl)
			mockCpConnector.EXPECT().CpApiCall(gomock.Any()).Return(http.StatusOK, "testResponse")
			mockTokenProvider := mock_oauthV2.NewMockTokenProvider(ctrl)
			mockTokenProvider.EXPECT().Identity().Return(nil)

			// Invoke code under test
			oauthHandler := &v2.OAuthHandler{
				Lock:          rudderSync.NewPartitionRWLocker(),
				Cache:         v2.NewCache(),
				CpConn:        mockCpConnector,
				TokenProvider: mockTokenProvider,
				Logger:        logger.NewLogger().Child("MockOAuthHandler"),
			}
			statusCode, _, _ := oauthHandler.FetchToken(fetchTokenParams)

			// Assertions
			Expect(statusCode).To(Equal(http.StatusInternalServerError))
			// Expect(response).To(Equal(nil))
			// Expect(err).NotTo(BeNil())
			// assert.Equal(t, http.StatusOK, statusCode)
			// assert.Equal(t, authResponse, response)
			// assert.Nil(t, err)
		})
	})
})
