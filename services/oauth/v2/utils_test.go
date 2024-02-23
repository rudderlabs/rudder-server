package v2_test

import (
	"encoding/json"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/rudderlabs/rudder-go-kit/logger"
	rudderSync "github.com/rudderlabs/rudder-go-kit/sync"
	v2 "github.com/rudderlabs/rudder-server/services/oauth/v2"
)

var _ = Describe("Utils", func() {
	Describe("Test GetOAuthActionStatName function", func() {
		It("returns the correct stat name", func() {
			Expect(v2.GetOAuthActionStatName("test")).To(Equal("oauth_action_test"))
		})
	})

	Describe("Test GetRefreshTokenErrResp function", func() {
		It("Call GetRefreshTokenErrResp with empty response", func() {
			oauthHandler := &v2.OAuthHandler{
				CacheMutex: rudderSync.NewPartitionRWLocker(),
				Cache:      v2.NewCache(),
				Logger:     logger.NewLogger().Child("MockOAuthHandler"),
			}
			accountSecret := &v2.AccountSecret{
				ExpirationDate: "",
				Secret:         nil,
			}
			errType, message := oauthHandler.GetRefreshTokenErrResp(``, accountSecret)
			Expect(errType).To(Equal("unmarshallableResponse"))
			Expect(message).To(Equal("Unmarshal of response unsuccessful: "))
		})

		It("Call GetRefreshTokenErrResp with marshallable(into AccountSecret) response", func() {
			oauthHandler := &v2.OAuthHandler{
				CacheMutex: rudderSync.NewPartitionRWLocker(),
				Cache:      v2.NewCache(),
				Logger:     logger.NewLogger().Child("MockOAuthHandler"),
			}
			accountSecret := &v2.AccountSecret{
				ExpirationDate: "",
				Secret:         nil,
			}
			errType, message := oauthHandler.GetRefreshTokenErrResp(`{"options":{},"id":"2BFzzzID8kITtU7AxxWtrn9KQQf","createdAt":"2022-06-29T15:34:47.758Z","updatedAt":"2024-02-12T12:18:35.213Z","workspaceId":"1oVajb9QqG50undaAcokNlYyJQa","name":"dummy user","role":"google_adwords_enhanced_conversions_v1","userId":"1oVadeaoGXN2pataEEoeIaXS3bO","metadata":{"userId":"115538421777182389816","displayName":"dummy user","email":"dummy@testmail.com"},"secretVersion":50,"rudderCategory":"destination","secret":{"access_token":"newaccesstoken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken"}}`, accountSecret)
			Expect(errType).To(Equal(""))
			Expect(message).To(Equal(""))
			Expect(accountSecret.ExpirationDate).To(Equal(""))
			Expect(accountSecret.Secret).To(Equal(json.RawMessage([]byte(`{"access_token":"newaccesstoken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken"}`))))
		})

		It("Call GetRefreshTokenErrResp with invalid_grant response from control-plane", func() {
			oauthHandler := &v2.OAuthHandler{
				CacheMutex: rudderSync.NewPartitionRWLocker(),
				Cache:      v2.NewCache(),
				Logger:     logger.NewLogger().Child("MockOAuthHandler"),
			}
			accountSecret := &v2.AccountSecret{
				ExpirationDate: "",
				Secret:         nil,
			}
			errType, message := oauthHandler.GetRefreshTokenErrResp(`{"body":{"code":"ref_token_invalid_grant","message":"[criteo_audience] \"invalid_grant\" error, refresh token has expired or revoked"}}`, accountSecret)
			Expect(errType).To(Equal("ref_token_invalid_grant"))
			Expect(message).To(Equal("[criteo_audience] \"invalid_grant\" error, refresh token has expired or revoked"))
		})
	})
})
