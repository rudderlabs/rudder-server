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
		masked, applied, maskErr := maskListedPaths(payload, []string{"headers.Authorization", "params.api_key", "body.token", "custom"})

		Expect(maskErr).To(BeFalse())
		Expect(applied).To(Equal(4))
		Expect(gjson.GetBytes(masked, "headers.Authorization").String()).To(Equal(maskedValue))
		Expect(gjson.GetBytes(masked, "params.api_key").String()).To(Equal(maskedValue))
		Expect(gjson.GetBytes(masked, "body.token").String()).To(Equal(maskedValue))
		Expect(gjson.GetBytes(masked, "custom").String()).To(Equal(maskedValue))
		Expect(gjson.GetBytes(masked, "endpoint").String()).To(Equal("https://example.test"))
	})

	It("supports escaped key characters, array members, scalar array members, and literal bracketed keys", func() {
		payload := json.RawMessage(`{"body":{"literal.key":"dot","star*key":"star","question?key":"question","tokens":["one","two"],"JSON":{"messages":[{"from":"one"},{"from":"two"}]}},"params":{"bd[0]":"bracket"}}`)
		masked, applied, maskErr := maskListedPaths(payload, []string{`body.literal\.key`, `body.star\*key`, `body.question\?key`, "body.JSON.messages.#.from", "body.tokens.#", "params.bd[0]"})

		Expect(maskErr).To(BeFalse())
		Expect(applied).To(Equal(6))
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
		masked, applied, maskErr := maskListedPaths(payload, []string{"endpoint", "endpoint.token", "headers.Authorization"})
		Expect(maskErr).To(BeFalse())
		Expect(applied).To(Equal(1))
		Expect(gjson.GetBytes(masked, "endpoint").String()).To(Equal("https://example.test/path?token=visible"))
		Expect(gjson.GetBytes(masked, "headers.Authorization").String()).To(Equal(maskedValue))
	})

	It("does not fabricate missing paths", func() {
		payload := json.RawMessage(`{"headers":{"Authorization":"secret"}}`)
		masked, applied, maskErr := maskListedPaths(payload, []string{"params.api_key"})
		Expect(maskErr).To(BeFalse())
		Expect(applied).To(BeZero())
		Expect(masked).To(MatchJSON(payload))
		Expect(gjson.GetBytes(masked, "params").Exists()).To(BeFalse())
	})

	It("preserves bytes for an empty listed-path result", func() {
		payload := json.RawMessage("{ \"body\" : { \"token\" : \"secret\" } }")
		masked, applied, maskErr := maskListedPaths(payload, []string{})
		Expect(maskErr).To(BeFalse())
		Expect(applied).To(BeZero())
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

	DescribeTable("replaces malformed or non-object payloads wholesale",
		func(payload string) {
			masked, maskErr := maskAll(json.RawMessage(payload))
			Expect(maskErr).To(BeTrue())
			Expect(string(masked)).To(Equal(`"******"`))
		},
		Entry("invalid JSON", `{"headers":`),
		Entry("array", `["secret"]`),
		Entry("scalar", `"secret"`),
	)

	It("falls back to fail-closed masking for an invalid listed path", func() {
		payload := json.RawMessage(`{"endpoint":"visible","headers":{"Authorization":"secret"}}`)
		masked, applied, maskErr := maskListedPaths(payload, []string{""})
		Expect(maskErr).To(BeTrue())
		Expect(applied).To(BeZero())
		Expect(gjson.GetBytes(masked, "endpoint").String()).To(Equal("visible"))
		Expect(gjson.GetBytes(masked, "headers").String()).To(Equal(maskedValue))
		Expect(string(masked)).ToNot(ContainSubstring("secret"))
	})

	It("falls back to fail-closed masking when sjson leaves an existing listed path unchanged", func() {
		payload := json.RawMessage(`{"endpoint":"visible","headers":{"Authorization":"secret"},"body":{"token":"secret"}}`)
		masked, applied, maskErr := maskListedPaths(payload, []string{"headers.Authorization|@reverse"})
		Expect(maskErr).To(BeTrue())
		Expect(applied).To(BeZero())
		Expect(gjson.GetBytes(masked, "endpoint").String()).To(Equal("visible"))
		Expect(gjson.GetBytes(masked, "headers").String()).To(Equal(maskedValue))
		Expect(gjson.GetBytes(masked, "body").String()).To(Equal(maskedValue))
		Expect(string(masked)).ToNot(ContainSubstring("secret"))
	})
})
