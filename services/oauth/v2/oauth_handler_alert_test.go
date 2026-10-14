package v2_test

import (
	"net/http"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
	kitsync "github.com/rudderlabs/rudder-go-kit/sync"

	mock_oauthV2 "github.com/rudderlabs/rudder-server/mocks/services/oauthV2"
	v2 "github.com/rudderlabs/rudder-server/services/oauth/v2"
	"github.com/rudderlabs/rudder-server/services/oauth/v2/common"
)

var _ = Describe("oauth_action_request tags (auth alert)", func() {
	It("tags oauth_action_request with the destinationId on a ref_token_invalid_grant refresh failure", func() {
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

		statsStore, statsErr := memstats.New()
		Expect(statsErr).To(BeNil())

		cache := v2.NewOauthTokenCache()
		oauthHandler := v2.NewOAuthHandler(mockAuthIdentityProvider,
			v2.WithCache(cache),
			v2.WithLocker(kitsync.NewPartitionRWLocker()),
			v2.WithStats(statsStore),
			v2.WithLogger(logger.NewLogger().Child("MockOAuthHandler")),
			v2.WithCpClient(mockCpConnector),
		)
		cache.Store(refreshTokenParams.AccountID, v2.OAuthToken{
			Secret: []byte(`{"access_token":"storedAccessToken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken"}`),
		})

		_, scErr := oauthHandler.RefreshToken(refreshTokenParams, previousSecret)
		Expect(scErr).ToNot(BeNil())

		metrics := statsStore.GetByName("oauth_action_request")
		Expect(metrics).To(HaveLen(1))
		Expect(metrics[0].Tags["errorMessage"]).To(Equal(common.RefTokenInvalidGrant))
		Expect(metrics[0].Tags["destinationId"]).To(Equal(Destination.ID))
		Expect(metrics[0].Tags["id"]).To(Equal("123"))
		Expect(metrics[0].Tags["workspaceId"]).To(Equal("456"))
		Expect(metrics[0].Value).To(BeNumerically("==", 1))
	})
})
