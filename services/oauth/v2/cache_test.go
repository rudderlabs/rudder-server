package v2_test

import (
	"encoding/json"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	v2 "github.com/rudderlabs/rudder-server/services/oauth/v2"
)

var _ = Describe("Cache", func() {
	It("Test Cache", func() {
		cache := v2.NewCache()
		Expect(cache).NotTo(BeNil())
	})
	It("Test Get and Set", func() {
		cache := v2.NewCache()
		authResponse := &v2.AuthResponse{
			Account: v2.AccountSecret{
				ExpirationDate: "2022-06-29T15:34:47.758Z",
				Secret:         json.RawMessage([]byte(`{"access_token":"validAccessToken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken"}`)),
			},
		}
		cache.Set("test", authResponse)
		value, ok := cache.Get("test")
		Expect(ok).To(BeTrue())
		Expect(value).To(Equal(authResponse))
	})
	It("Test Delete", func() {
		cache := v2.NewCache()
		authResponse := &v2.AuthResponse{
			Account: v2.AccountSecret{
				ExpirationDate: "2022-06-29T15:34:47.758Z",
				Secret:         json.RawMessage([]byte(`{"access_token":"validAccessToken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken"}`)),
			},
		}
		cache.Set("test", authResponse)
		value, ok := cache.Get("test")
		Expect(ok).To(BeTrue())
		Expect(value).To(Equal(authResponse))
		cache.Delete("test")
		value, ok = cache.Get("test")
		Expect(ok).To(BeFalse())
		Expect(value).To(BeNil())
	})
})
