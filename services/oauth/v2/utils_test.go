package v2

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/rudderlabs/rudder-go-kit/logger"
)

var (
	mockLogger = logger.NewLogger().Child("MockOAuthHandler")
	_          = Describe("Utils", func() {
		Describe("Test GetRefreshTokenErrResp function", func() {
			It("Call GetRefreshTokenErrResp with empty response", func() {
				_, refreshError := getRefreshTokenFromResponse(``, mockLogger)
				Expect(refreshError.errType).To(Equal("unmarshallableResponse"))
				Expect(refreshError.message).To(Equal("Unmarshal of response unsuccessful: "))
			})

			It("Call GetRefreshTokenErrResp with marshallable(into AccountSecret) response", func() {
				oauthToken, refreshError := getRefreshTokenFromResponse(`{"options":{},"id":"2BFzzzID8kITtU7AxxWtrn9KQQf","createdAt":"2022-06-29T15:34:47.758Z","updatedAt":"2024-02-12T12:18:35.213Z","workspaceId":"1oVajb9QqG50undaAcokNlYyJQa","name":"dummy user","role":"google_adwords_enhanced_conversions_v1","userId":"1oVadeaoGXN2pataEEoeIaXS3bO","metadata":{"userId":"115538421777182389816","displayName":"dummy user","email":"dummy@testmail.com"},"secretVersion":50,"rudderCategory":"destination","secret":{"access_token":"newaccesstoken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken"}}`, mockLogger)
				Expect(refreshError).To(BeNil())
				Expect(oauthToken.ExpirationDate).To(BeEmpty())
				Expect(`{"access_token":"newaccesstoken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken"}`).To(MatchJSON(oauthToken.Secret))
			})

			It("Call GetRefreshTokenErrResp with invalid_grant response from control-plane", func() {
				_, refreshError := getRefreshTokenFromResponse(`{"body":{"code":"ref_token_invalid_grant","message":"[criteo_audience] \"invalid_grant\" error, refresh token has expired or revoked"}}`, mockLogger)
				Expect(refreshError.errType).To(Equal("ref_token_invalid_grant"))
				Expect(refreshError.message).To(Equal("[criteo_audience] \"invalid_grant\" error, refresh token has expired or revoked"))
			})

			It("Call GetRefreshTokenErrResp with invalid_grant response and empty message", func() {
				_, refreshError := getRefreshTokenFromResponse(`{"body":{"code":"ref_token_invalid_grant"}}`, mockLogger)
				Expect(refreshError.errType).To(Equal("ref_token_invalid_grant"))
				Expect(refreshError.message).To(Equal("problem with user permission or access/refresh token have been revoked"))
			})

			It("Call GetRefreshTokenErrResp with invalid_refresh_response code", func() {
				_, refreshError := getRefreshTokenFromResponse(`{"body":{"code":"INVALID_REFRESH_RESPONSE","message":"Custom invalid refresh message"}}`, mockLogger)
				Expect(refreshError.errType).To(Equal("INVALID_REFRESH_RESPONSE"))
				Expect(refreshError.message).To(Equal("Custom invalid refresh message"))
			})

			It("Call GetRefreshTokenErrResp with invalid_refresh_response code and empty message", func() {
				_, refreshError := getRefreshTokenFromResponse(`{"body":{"code":"INVALID_REFRESH_RESPONSE"}}`, mockLogger)
				Expect(refreshError.errType).To(Equal("INVALID_REFRESH_RESPONSE"))
				Expect(refreshError.message).To(Equal("invalid auth token refresh response"))
			})

			It("Call GetRefreshTokenErrResp with unknown error code and custom message", func() {
				_, refreshError := getRefreshTokenFromResponse(`{"body":{"code":"httpError","message":"Connection timeout occurred"}}`, mockLogger)
				Expect(refreshError.errType).To(Equal("httpError"))
				Expect(refreshError.message).To(Equal("Connection timeout occurred"))
			})

			It("Call GetRefreshTokenErrResp with unknown error code and empty message (default case)", func() {
				_, refreshError := getRefreshTokenFromResponse(`{"body":{"code":"httpError"}}`, mockLogger)
				Expect(refreshError.errType).To(Equal("httpError"))
				Expect(refreshError.message).To(Equal("Internal service error: httpError"))
			})
		})
	})
)
