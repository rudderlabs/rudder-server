package v2

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
)

var _ = Describe("Secret version parsing", func() {
	It("A7e: preserves the token response version and existing expiration handling", func() {
		const response = `{"secret":{"access_token":"t","expirationDate":"2099-01-01T00:00:00.000Z"},"secretVersion":8}`
		token, err := getRefreshTokenFromResponse(response, logger.NOP)
		Expect(err).To(BeNil())
		Expect(token.SecretVersion).To(Equal(8))
		Expect(token.Secret).To(MatchJSON(`{"access_token":"t","expirationDate":"2099-01-01T00:00:00.000Z"}`))
		// getTokenFromAPI copies secret.expirationDate into the cached token after parsing.
		Expect(token.ExpirationDate).To(BeEmpty())

		var account backendconfig.Account
		Expect(jsonrs.Unmarshal([]byte(response), &account)).To(Succeed())
		Expect(account.SecretVersion).To(Equal(8))
	})
})
