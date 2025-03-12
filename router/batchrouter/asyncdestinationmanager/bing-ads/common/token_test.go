package common_test

import (
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
		oauthClient := MockAuthorizer.NewMockAuthorizer(ctrl)

		tokenSource := common.TokenSource{
			DestinationDefName: "Test",
			DestinationID:      "1234",
			AccountID:          "1234",
			OauthClientV2:      oauthClient,
			CurrentTime: func() time.Time {
				mockTime := time.Date(2025, time.March, 12, 12, 0, 0, 0, time.UTC)
				return mockTime
			},
		}

		oauthClient.EXPECT().FetchToken(gomock.Any()).Return(200,

			&v2.AuthResponse{
				Account: v2.AccountSecret{
					ExpirationDate: "",
					Secret:         []byte(`{"accessToken":"validAccessToken","refreshToken":"validRefreshToken","developer_token":"dev","expirationDate":"2025-03-12T12:34:47.758Z"}`),
				},
			}, nil)
		token, _ := tokenSource.Token()
		Expect(token.AccessToken).To(Equal("validAccessToken"))
		Expect(token.RefreshToken).To(Equal("validRefreshToken"))
		Expect(token.Expiry).To(BeTemporally("~", time.Date(2025, time.March, 12, 12, 34, 47, 758000000, time.UTC), 1*time.Second))
	})

	It("Successfully refresh an expired token", func() {
		ctrl := gomock.NewController(GinkgoT())
		oauthClient := MockAuthorizer.NewMockAuthorizer(ctrl)

		tokenSource := common.TokenSource{
			DestinationDefName: "Test",
			DestinationID:      "1234",
			AccountID:          "1234",
			OauthClientV2:      oauthClient,
			CurrentTime: func() time.Time {
				mockTime := time.Date(2025, time.March, 13, 12, 0, 0, 0, time.UTC)
				return mockTime
			},
		}

		oauthClient.EXPECT().FetchToken(gomock.Any()).Return(200,

			&v2.AuthResponse{
				Account: v2.AccountSecret{
					ExpirationDate: "",
					Secret:         []byte(`{"accessToken":"expiredAccessToken","refreshToken":"validRefreshToken","developer_token":"dev","expirationDate":"2025-03-12T12:34:47.758Z"}`),
				},
			}, nil)
		oauthClient.EXPECT().RefreshToken(gomock.Any()).Return(200,

			&v2.AuthResponse{
				Account: v2.AccountSecret{
					ExpirationDate: "",
					Secret:         []byte(`{"accessToken":"validAccessToken","refreshToken":"validRefreshToken","developer_token":"dev","expirationDate":"2025-03-13T13:34:47.758Z"}`),
				},
			}, nil)
		token, _ := tokenSource.Token()
		Expect(token.AccessToken).To(Equal("validAccessToken"))
		Expect(token.RefreshToken).To(Equal("validRefreshToken"))
		Expect(token.Expiry).To(BeTemporally("~", time.Date(2025, time.March, 13, 13, 34, 47, 758000000, time.UTC), 1*time.Second))
	})

	It("Handle error when RefreshToken returns an error", func() {
		ctrl := gomock.NewController(GinkgoT())
		oauthClient := MockAuthorizer.NewMockAuthorizer(ctrl)

		tokenSource := common.TokenSource{
			DestinationDefName: "Test",
			DestinationID:      "1234",
			AccountID:          "1234",
			OauthClientV2:      oauthClient,
			CurrentTime: func() time.Time {
				mockTime := time.Date(2025, time.March, 13, 12, 0, 0, 0, time.UTC)
				return mockTime
			},
		}

		oauthClient.EXPECT().FetchToken(gomock.Any()).Return(200,

			&v2.AuthResponse{
				Account: v2.AccountSecret{
					ExpirationDate: "",
					Secret:         []byte(`{"accessToken":"expiredAccessToken","refreshToken":"validRefreshToken","developer_token":"dev","expirationDate":"2025-03-12T12:34:47.758Z"}`),
				},
			}, nil)
		oauthClient.EXPECT().RefreshToken(gomock.Any()).Return(400, nil, fmt.Errorf("error refreshing token"))
		_, err := tokenSource.Token()
		Expect(err).To(Equal(
			fmt.Errorf("generating the accessToken: %w", fmt.Errorf("error in refreshing access token with this error: %w. StatusCode: %d", fmt.Errorf("error refreshing token"), 400))),
		)
	})

	It("Handle error when parsing the expiration date", func() {
		ctrl := gomock.NewController(GinkgoT())
		oauthClient := MockAuthorizer.NewMockAuthorizer(ctrl)

		tokenSource := common.TokenSource{
			DestinationDefName: "Test",
			DestinationID:      "1234",
			AccountID:          "1234",
			OauthClientV2:      oauthClient,
			CurrentTime: func() time.Time {
				mockTime := time.Date(2025, time.March, 13, 12, 0, 0, 0, time.UTC)
				return mockTime
			},
		}

		oauthClient.EXPECT().FetchToken(gomock.Any()).Return(200,

			&v2.AuthResponse{
				Account: v2.AccountSecret{
					ExpirationDate: "",
					Secret:         []byte(`{"accessToken":"expiredAccessToken","refreshToken":"validRefreshToken","developer_token":"dev","expirationDate":"invalid-date"}`),
				},
			}, nil)

		_, err := tokenSource.Token()
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("error in parsing expirationDate"))
		Expect(err.Error()).To(ContainSubstring("invalid-date"))
	})
})
