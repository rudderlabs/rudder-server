package v2

import (
	"encoding/json"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Cache", func() {
	It("Test Cache", func() {
		cache := NewCache()
		Expect(cache).NotTo(BeNil())
	})
	It("Test Get and Set", func() {
		cache := NewCache()
		authResponse := &AuthResponse{
			Account: AccountSecret{
				ExpirationDate: "2022-06-29T15:34:47.758Z",
				Secret:         json.RawMessage([]byte(`{"access_token":"validAccessToken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken"}`)),
			},
		}
		cache.Store("test", authResponse)
		value, ok := cache.Load("test")
		Expect(ok).To(BeTrue())
		Expect(value).To(Equal(authResponse))
	})
	It("Test Delete", func() {
		cache := NewCache()
		authResponse := &AuthResponse{
			Account: AccountSecret{
				ExpirationDate: "2022-06-29T15:34:47.758Z",
				Secret:         json.RawMessage([]byte(`{"access_token":"validAccessToken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken"}`)),
			},
		}
		cache.Store("test", authResponse)
		value, ok := cache.Load("test")
		Expect(ok).To(BeTrue())
		Expect(value).To(Equal(authResponse))
		cache.Delete("test")
		value, ok = cache.Load("test")
		Expect(ok).To(BeFalse())
		Expect(value).To(BeNil())
	})
	It("Test for any type of data", func() {
		cache := NewCache()
		authResponse := map[string]interface{}{
			"test":  "test",
			"test2": "test2",
		}
		cache.Store("test", authResponse)
		value, ok := cache.Load("test")
		Expect(ok).To(BeTrue())
		Expect(value).To(Equal(authResponse))
		value, ok = cache.Load("test")
		Expect(ok).To(BeTrue())
		Expect(value).To(Equal(authResponse))
	})
})
