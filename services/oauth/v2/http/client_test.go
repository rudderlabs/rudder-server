package v2_test

import (
	"net/http"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	oauth "github.com/rudderlabs/rudder-server/services/oauth/v2"
	extensions "github.com/rudderlabs/rudder-server/services/oauth/v2/extensions"
	httpClient "github.com/rudderlabs/rudder-server/services/oauth/v2/http"
)

var _ = Describe("Http/Client", func() {
	Describe("OAuthHttpClient", func() {
		It("should return an http client that will add the appropriate authorization information to oauth requests", func() {
			httpClient := httpClient.OAuthHttpClient(&http.Client{}, extensions.BodyAugmenter, oauth.RudderFlow_Delivery, nil, nil, backendconfig.DefaultBackendConfig, oauth.GetAuthErrorCategoryFromTransformResponse)
			Expect(httpClient).ToNot(BeNil())
		})
	})
})
