package common_test

import (
	"encoding/json"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/mock/gomock"

	MockAuthorizer "github.com/rudderlabs/rudder-server/mocks/services/oauthV2"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/bing-ads/common"
	v2 "github.com/rudderlabs/rudder-server/services/oauth/v2"
)

var _ = Describe("Token", func() {
	It("Successfully generates a token with valid access and refresh tokens", func() {
		ctrl := gomock.NewController(GinkgoT())
		oauthClient := MockAuthorizer.NewMockOAuthHandler(ctrl)

		tokenSource := common.TokenSource{
			DestinationDefName: "Test",
			DestinationID:      "1234",
			AccountID:          "1234",
			OauthHandler:       oauthClient,
			CurrentTime: func() time.Time {
				mockTime := time.Date(2025, time.March, 12, 12, 0, 0, 0, time.UTC)
				return mockTime
			},
		}

		oauthClient.EXPECT().FetchToken(gomock.Any()).Return(
			json.RawMessage(`{"accessToken":"validAccessToken","refreshToken":"validRefreshToken","developer_token":"dev","expirationDate":"2025-03-12T12:34:47.758Z"}`),
			nil,
		)
		token, _ := tokenSource.Token()
		Expect(token.AccessToken).To(Equal("validAccessToken"))
		Expect(token.RefreshToken).To(Equal("validRefreshToken"))
		Expect(token.Expiry).To(BeTemporally("~", time.Date(2025, time.March, 12, 12, 34, 47, 758000000, time.UTC), 1*time.Second))
	})

	It("Handle error when FetchToken returns an error", func() {
		ctrl := gomock.NewController(GinkgoT())
		oauthClient := MockAuthorizer.NewMockOAuthHandler(ctrl)

		tokenSource := common.TokenSource{
			DestinationDefName: "Test",
			DestinationID:      "1234",
			AccountID:          "1234",
			OauthHandler:       oauthClient,
			CurrentTime: func() time.Time {
				mockTime := time.Date(2025, time.March, 13, 12, 0, 0, 0, time.UTC)
				return mockTime
			},
		}

		oauthClient.EXPECT().FetchToken(gomock.Any()).Return(nil, v2.NewStatusCodeError(400, fmt.Errorf("error fetching token")))
		_, err := tokenSource.Token()
		Expect(err).To(Equal(
			fmt.Errorf("generating the accessToken: %w", fmt.Errorf("fetching access token: %w", v2.NewStatusCodeError(400, fmt.Errorf("error fetching token"))))),
		)
	})
})
