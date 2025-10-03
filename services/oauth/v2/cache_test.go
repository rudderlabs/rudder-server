package v2_test

import (
	"encoding/json"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	v2 "github.com/rudderlabs/rudder-server/services/oauth/v2"
)

var _ = Describe("Cache", func() {
	It("Test Cache", func() {
		cache := v2.NewOauthTokenCache()
		Expect(cache).NotTo(BeNil())
	})
	It("Test Get and Set", func() {
		cache := v2.NewOauthTokenCache()
		authToken := v2.OAuthToken{
			ExpirationDate: "2022-06-29T15:34:47.758Z",
			Secret:         json.RawMessage([]byte(`{"access_token":"validAccessToken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken"}`)),
		}
		cache.Store("test", authToken)
		value, ok := cache.Load("test")
		Expect(ok).To(BeTrue())
		Expect(value).To(Equal(authToken))
	})
	It("Test Delete", func() {
		cache := v2.NewOauthTokenCache()
		authToken := v2.OAuthToken{
			ExpirationDate: "2022-06-29T15:34:47.758Z",
			Secret:         json.RawMessage([]byte(`{"access_token":"validAccessToken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken"}`)),
		}
		cache.Store("test", authToken)
		value, ok := cache.Load("test")
		Expect(ok).To(BeTrue())
		Expect(value).To(Equal(authToken))
		cache.Delete("test")
		value, ok = cache.Load("test")
		Expect(ok).To(BeFalse())
		var zero v2.OAuthToken
		Expect(value).To(Equal(zero))
	})
})
