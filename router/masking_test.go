package router

import (
	"encoding/json"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/tidwall/gjson"
)

var _ = Describe("Delivery payload masking", func() {
	It("masks paths across request fields and composes replacements", func() {
		payload := json.RawMessage(`{"endpoint":"https://example.test","headers":{"Authorization":"Bearer secret"},"params":{"api_key":"param-secret"},"body":{"token":"body-secret"},"custom":"top-secret"}`)
		masked, reason := maskSecrets(payload, []string{"headers.Authorization", "params.api_key", "body.token", "custom"}, true)

		Expect(reason).To(Equal("listed"))
		Expect(gjson.GetBytes(masked, "headers.Authorization").String()).To(Equal(maskedValue))
		Expect(gjson.GetBytes(masked, "params.api_key").String()).To(Equal(maskedValue))
		Expect(gjson.GetBytes(masked, "body.token").String()).To(Equal(maskedValue))
		Expect(gjson.GetBytes(masked, "custom").String()).To(Equal(maskedValue))
		Expect(gjson.GetBytes(masked, "endpoint").String()).To(Equal("https://example.test"))
	})

	It("supports escaped key characters, array members, scalar array members, and literal bracketed keys", func() {
		payload := json.RawMessage(`{"body":{"literal.key":"dot","star*key":"star","question?key":"question","tokens":["one","two"],"JSON":{"messages":[{"from":"one"},{"from":"two"}]}},"params":{"bd[0]":"bracket"}}`)
		masked, reason := maskSecrets(payload, []string{`body.literal\.key`, `body.star\*key`, `body.question\?key`, "body.JSON.messages.#.from", "body.tokens.#", "params.bd[0]"}, true)

		Expect(reason).To(Equal("listed"))
		Expect(gjson.GetBytes(masked, `body.literal\.key`).String()).To(Equal(maskedValue))
		Expect(gjson.GetBytes(masked, `body.star\*key`).String()).To(Equal(maskedValue))
		Expect(gjson.GetBytes(masked, `body.question\?key`).String()).To(Equal(maskedValue))
		Expect(gjson.GetBytes(masked, "body.JSON.messages.#.from").Array()).To(HaveEach(HaveField("Str", maskedValue)))
		Expect(gjson.GetBytes(masked, "body.tokens.0").String()).To(Equal(maskedValue))
		Expect(gjson.GetBytes(masked, "body.tokens.1").String()).To(Equal(maskedValue))
		Expect(gjson.GetBytes(masked, "params.bd[0]").String()).To(Equal(maskedValue))
	})

	DescribeTable("detects only an unescaped terminal array wildcard",
		func(path, wantParent string, wantOK bool) {
			parent, ok := terminalArrayWildcardParent(path)
			Expect(ok).To(Equal(wantOK))
			Expect(parent).To(Equal(wantParent))
		},
		Entry("bare wildcard", "#", "", true),
		Entry("nested wildcard", "body.tokens.#", "body.tokens", true),
		Entry("escaped dot before #", `body.tokens\.#`, "", false),
		Entry("escaped backslash then separator", `body.tokens\\.#`, `body.tokens\\`, true),
		Entry("escaped #", `body.tokens.\#`, "", false),
		Entry("non-terminal wildcard", "body.#.from", "", false),
	)

	It("never masks endpoint even when it is listed", func() {
		payload := json.RawMessage(`{"endpoint":"https://example.test/path?token=visible","headers":{"Authorization":"secret"}}`)
		masked, reason := maskSecrets(payload, []string{"endpoint", "endpoint.token", "headers.Authorization"}, true)
		Expect(reason).To(Equal("listed"))
		Expect(gjson.GetBytes(masked, "endpoint").String()).To(Equal("https://example.test/path?token=visible"))
		Expect(gjson.GetBytes(masked, "headers.Authorization").String()).To(Equal(maskedValue))
	})

	It("does not fabricate missing paths", func() {
		payload := json.RawMessage(`{"headers":{"Authorization":"secret"}}`)
		masked, reason := maskSecrets(payload, []string{"params.api_key"}, true)
		Expect(reason).To(Equal("listed_noop"))
		Expect(masked).To(MatchJSON(payload))
		Expect(gjson.GetBytes(masked, "params").Exists()).To(BeFalse())
	})

	It("masks a one-request array while preserving the Live Events payload shape", func() {
		payload := json.RawMessage(`[{"endpoint":"https://zendesk.test/users","headers":{"Authorization":"Bearer secret"},"body":{"name":"Ada"}}]`)
		masked, reason := maskForDelivery(payload, []string{"headers.Authorization"}, true)

		Expect(reason).To(Equal("listed"))
		Expect(masked).To(MatchJSON(`[{"endpoint":"https://zendesk.test/users","headers":{"Authorization":"******"},"body":{"name":"Ada"}}]`))
	})

	It("masks every element of a multi-request array independently", func() {
		payload := json.RawMessage(`[
			{"endpoint":"https://sfmc.test/contacts","headers":{"Authorization":"Bearer first"},"body":{"token":"first-token"}},
			{"endpoint":"https://sfmc.test/data","headers":{"Authorization":"Bearer second"},"body":{"token":"second-token"}}
		]`)
		masked, reason := maskForDelivery(payload, []string{"headers.Authorization", "body.token"}, true)

		Expect(reason).To(Equal("listed"))
		Expect(masked).To(MatchJSON(`[
			{"endpoint":"https://sfmc.test/contacts","headers":{"Authorization":"******"},"body":{"token":"******"}},
			{"endpoint":"https://sfmc.test/data","headers":{"Authorization":"******"},"body":{"token":"******"}}
		]`))
	})

	It("preserves an empty array as a successful no-op", func() {
		payload := json.RawMessage(`[ ]`)
		masked, reason := maskForDelivery(payload, []string{"headers.Authorization"}, true)

		Expect(reason).To(Equal("listed_noop"))
		Expect(masked).To(Equal(payload))
	})

	It("preserves the bytes of a non-empty array when no listed path matches", func() {
		payload := json.RawMessage(`[ {"endpoint":"first","headers":{"Authorization":"secret"}} , {"endpoint":"second"} ]`)
		masked, reason := maskForDelivery(payload, []string{"params.missing"}, true)

		Expect(reason).To(Equal("listed_noop"))
		Expect(masked).To(Equal(payload))
	})

	It("preserves bytes for an empty listed-path result", func() {
		payload := json.RawMessage("{ \"body\" : { \"token\" : \"secret\" } }")
		masked, reason := maskSecrets(payload, []string{}, true)
		Expect(reason).To(Equal("listed_noop"))
		Expect(masked).To(Equal(payload))
	})

	It("masks all top-level fields except endpoint", func() {
		payload := json.RawMessage(`{"endpoint":"https://example.test","headers":{"Authorization":"secret"},"params":{"key":"secret"},"body":{"token":"secret"},"custom":"secret"}`)
		masked, maskErr := maskAll(payload)

		Expect(maskErr).To(BeFalse())
		Expect(gjson.GetBytes(masked, "endpoint").String()).To(Equal("https://example.test"))
		for _, key := range []string{"headers", "params", "body", "custom"} {
			Expect(gjson.GetBytes(masked, key).String()).To(Equal(maskedValue))
		}
	})

	It("handles escaped and operator-like top-level keys in fail-closed mode", func() {
		payload := json.RawMessage(`{"endpoint":"visible","secret.key":"dot","secret*key":"star","secret?key":"question","secret\\key":"slash","@this":"operator","a|b":"pipe"}`)
		masked, maskErr := maskAll(payload)
		Expect(maskErr).To(BeFalse())
		Expect(gjson.GetBytes(masked, `secret\.key`).String()).To(Equal(maskedValue))
		Expect(gjson.GetBytes(masked, `secret\*key`).String()).To(Equal(maskedValue))
		Expect(gjson.GetBytes(masked, `secret\?key`).String()).To(Equal(maskedValue))
		Expect(gjson.GetBytes(masked, `secret\\key`).String()).To(Equal(maskedValue))
		Expect(gjson.GetBytes(masked, `\@this`).String()).To(Equal(maskedValue))
		Expect(gjson.GetBytes(masked, `a\|b`).String()).To(Equal(maskedValue))
		Expect(gjson.GetBytes(masked, "endpoint").String()).To(Equal("visible"))
	})

	It("masks duplicate non-endpoint top-level keys in fail-closed mode", func() {
		masked, maskErr := maskAll(json.RawMessage(`{"token":"first","endpoint":"visible","token":"second"}`))
		Expect(maskErr).To(BeFalse())
		Expect(string(masked)).To(ContainSubstring(`"endpoint":"visible"`))
		Expect(string(masked)).ToNot(ContainSubstring("first"))
		Expect(string(masked)).ToNot(ContainSubstring("second"))
	})

	DescribeTable("replaces malformed or non-object payloads with a masked object",
		func(payload string) {
			masked, maskErr := maskAll(json.RawMessage(payload))
			Expect(maskErr).To(BeTrue())
			Expect(masked).To(MatchJSON(`{"masked":"******"}`))
		},
		Entry("invalid JSON", `{"headers":"secret"`),
		Entry("array", `["secret"]`),
		Entry("scalar", `"secret"`),
	)

	DescribeTable("listed masking fails closed with a masked object for unsupported payloads",
		func(payload string) {
			masked, reason := maskForDelivery(json.RawMessage(payload), []string{"headers.Authorization"}, true)
			Expect(reason).To(Equal("mask_error"))
			Expect(masked).To(MatchJSON(`{"masked":"******"}`))
		},
		Entry("scalar", `"secret-token"`),
		Entry("invalid JSON", `{"headers":{"Authorization":"secret"}`),
	)

	DescribeTable("delivery masking fails closed for unsupported payloads even when no paths are listed",
		func(payload string) {
			masked, reason := maskForDelivery(json.RawMessage(payload), []string{}, true)
			Expect(reason).To(Equal("mask_error"))
			Expect(masked).To(MatchJSON(`{"masked":"******"}`))
		},
		Entry("scalar", `"secret-token"`),
		Entry("invalid JSON", `{"headers":{"Authorization":"secret"}`),
		Entry("array with a non-object request", `[{"endpoint":"first"},"secret-token"]`),
	)

	It("fails closed for the whole array when applying a listed path fails", func() {
		payload := json.RawMessage(`[
			{"endpoint":"first","headers":{"Authorization":"secret"}},
			{"endpoint":"second","headers":{"Authorization":"other-secret"}}
		]`)
		masked, reason := maskForDelivery(payload, []string{"headers.Authorization|@reverse"}, true)

		Expect(reason).To(Equal("mask_error"))
		Expect(masked).To(MatchJSON(`{"masked":"******"}`))
	})

	It("falls back to fail-closed masking for an invalid listed path", func() {
		payload := json.RawMessage(`{"endpoint":"visible","headers":{"Authorization":"secret"}}`)
		masked, reason := maskForDelivery(payload, []string{""}, true)
		Expect(reason).To(Equal("mask_error"))
		Expect(gjson.GetBytes(masked, "endpoint").String()).To(Equal("visible"))
		Expect(gjson.GetBytes(masked, "headers").String()).To(Equal(maskedValue))
		Expect(string(masked)).ToNot(ContainSubstring("secret"))
	})

	It("falls back to fail-closed masking when sjson leaves an existing listed path unchanged", func() {
		payload := json.RawMessage(`{"endpoint":"visible","headers":{"Authorization":"secret"},"body":{"token":"secret"}}`)
		masked, reason := maskForDelivery(payload, []string{"headers.Authorization|@reverse"}, true)
		Expect(reason).To(Equal("mask_error"))
		Expect(gjson.GetBytes(masked, "endpoint").String()).To(Equal("visible"))
		Expect(gjson.GetBytes(masked, "headers").String()).To(Equal(maskedValue))
		Expect(gjson.GetBytes(masked, "body").String()).To(Equal(maskedValue))
		Expect(string(masked)).ToNot(ContainSubstring("secret"))
	})
})
