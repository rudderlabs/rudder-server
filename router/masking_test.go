package router

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestDeliveryPayloadMasking(t *testing.T) {
	t.Run("masks paths across request fields and composes replacements", func(t *testing.T) {
		payload := json.RawMessage(`{"endpoint":"https://example.test","headers":{"Authorization":"Bearer secret"},"params":{"api_key":"param-secret"},"body":{"token":"body-secret"},"custom":"top-secret"}`)
		masked, reason := maskSecrets(payload, []string{"headers.Authorization", "params.api_key", "body.token", "custom"}, true)

		require.Equal(t, "listed", reason)
		require.Equal(t, maskedValue, gjson.GetBytes(masked, "headers.Authorization").String())
		require.Equal(t, maskedValue, gjson.GetBytes(masked, "params.api_key").String())
		require.Equal(t, maskedValue, gjson.GetBytes(masked, "body.token").String())
		require.Equal(t, maskedValue, gjson.GetBytes(masked, "custom").String())
		require.Equal(t, "https://example.test", gjson.GetBytes(masked, "endpoint").String())
	})

	t.Run("supports escaped key characters, array members, scalar array members, and literal bracketed keys", func(t *testing.T) {
		payload := json.RawMessage(`{"body":{"literal.key":"dot","star*key":"star","question?key":"question","tokens":["one","two"],"JSON":{"messages":[{"from":"one"},{"from":"two"}]}},"params":{"bd[0]":"bracket"}}`)
		masked, reason := maskSecrets(payload, []string{`body.literal\.key`, `body.star\*key`, `body.question\?key`, "body.JSON.messages.#.from", "body.tokens.#", "params.bd[0]"}, true)

		require.Equal(t, "listed", reason)
		require.Equal(t, maskedValue, gjson.GetBytes(masked, `body.literal\.key`).String())
		require.Equal(t, maskedValue, gjson.GetBytes(masked, `body.star\*key`).String())
		require.Equal(t, maskedValue, gjson.GetBytes(masked, `body.question\?key`).String())
		froms := gjson.GetBytes(masked, "body.JSON.messages.#.from").Array()
		require.NotEmpty(t, froms)
		for _, from := range froms {
			require.Equal(t, maskedValue, from.Str)
		}
		require.Equal(t, maskedValue, gjson.GetBytes(masked, "body.tokens.0").String())
		require.Equal(t, maskedValue, gjson.GetBytes(masked, "body.tokens.1").String())
		require.Equal(t, maskedValue, gjson.GetBytes(masked, "params.bd[0]").String())
	})

	t.Run("detects only an unescaped terminal array wildcard", func(t *testing.T) {
		testCases := []struct {
			name       string
			path       string
			wantParent string
			wantOK     bool
		}{
			{name: "bare wildcard", path: "#", wantParent: "", wantOK: true},
			{name: "nested wildcard", path: "body.tokens.#", wantParent: "body.tokens", wantOK: true},
			{name: "escaped dot before #", path: `body.tokens\.#`, wantParent: "", wantOK: false},
			{name: "escaped backslash then separator", path: `body.tokens\\.#`, wantParent: `body.tokens\\`, wantOK: true},
			{name: "escaped #", path: `body.tokens.\#`, wantParent: "", wantOK: false},
			{name: "non-terminal wildcard", path: "body.#.from", wantParent: "", wantOK: false},
		}
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				parent, ok := terminalArrayWildcardParent(tc.path)
				require.Equal(t, tc.wantOK, ok)
				require.Equal(t, tc.wantParent, parent)
			})
		}
	})

	t.Run("never masks endpoint even when it is listed", func(t *testing.T) {
		payload := json.RawMessage(`{"endpoint":"https://example.test/path?token=visible","headers":{"Authorization":"secret"}}`)
		masked, reason := maskSecrets(payload, []string{"endpoint", "endpoint.token", "headers.Authorization"}, true)
		require.Equal(t, "listed", reason)
		require.Equal(t, "https://example.test/path?token=visible", gjson.GetBytes(masked, "endpoint").String())
		require.Equal(t, maskedValue, gjson.GetBytes(masked, "headers.Authorization").String())
	})

	t.Run("does not fabricate missing paths", func(t *testing.T) {
		payload := json.RawMessage(`{"headers":{"Authorization":"secret"}}`)
		masked, reason := maskSecrets(payload, []string{"params.api_key"}, true)
		require.Equal(t, "listed_noop", reason)
		require.JSONEq(t, string(payload), string(masked))
		require.False(t, gjson.GetBytes(masked, "params").Exists())
	})

	t.Run("masks a one-request array while preserving the Live Events payload shape", func(t *testing.T) {
		payload := json.RawMessage(`[{"endpoint":"https://zendesk.test/users","headers":{"Authorization":"Bearer secret"},"body":{"name":"Ada"}}]`)
		masked, reason := maskForDelivery(payload, []string{"headers.Authorization"}, true)

		require.Equal(t, "listed", reason)
		require.JSONEq(t, `[{"endpoint":"https://zendesk.test/users","headers":{"Authorization":"******"},"body":{"name":"Ada"}}]`, string(masked))
	})

	t.Run("masks every element of a multi-request array independently", func(t *testing.T) {
		payload := json.RawMessage(`[
			{"endpoint":"https://sfmc.test/contacts","headers":{"Authorization":"Bearer first"},"body":{"token":"first-token"}},
			{"endpoint":"https://sfmc.test/data","headers":{"Authorization":"Bearer second"},"body":{"token":"second-token"}}
		]`)
		masked, reason := maskForDelivery(payload, []string{"headers.Authorization", "body.token"}, true)

		require.Equal(t, "listed", reason)
		require.JSONEq(t, `[
			{"endpoint":"https://sfmc.test/contacts","headers":{"Authorization":"******"},"body":{"token":"******"}},
			{"endpoint":"https://sfmc.test/data","headers":{"Authorization":"******"},"body":{"token":"******"}}
		]`, string(masked))
	})

	t.Run("preserves an empty array as a successful no-op", func(t *testing.T) {
		payload := json.RawMessage(`[ ]`)
		masked, reason := maskForDelivery(payload, []string{"headers.Authorization"}, true)

		require.Equal(t, "listed_noop", reason)
		require.Equal(t, payload, masked)
	})

	t.Run("preserves the bytes of a non-empty array when no listed path matches", func(t *testing.T) {
		payload := json.RawMessage(`[ {"endpoint":"first","headers":{"Authorization":"secret"}} , {"endpoint":"second"} ]`)
		masked, reason := maskForDelivery(payload, []string{"params.missing"}, true)

		require.Equal(t, "listed_noop", reason)
		require.Equal(t, payload, masked)
	})

	t.Run("preserves bytes for an empty listed-path result", func(t *testing.T) {
		payload := json.RawMessage("{ \"body\" : { \"token\" : \"secret\" } }")
		masked, reason := maskSecrets(payload, []string{}, true)
		require.Equal(t, "listed_noop", reason)
		require.Equal(t, payload, masked)
	})

	t.Run("masks all top-level fields except endpoint", func(t *testing.T) {
		payload := json.RawMessage(`{"endpoint":"https://example.test","headers":{"Authorization":"secret"},"params":{"key":"secret"},"body":{"token":"secret"},"custom":"secret"}`)
		masked, maskErr := maskAll(payload)

		require.False(t, maskErr)
		require.Equal(t, "https://example.test", gjson.GetBytes(masked, "endpoint").String())
		for _, key := range []string{"headers", "params", "body", "custom"} {
			require.Equal(t, maskedValue, gjson.GetBytes(masked, key).String())
		}
	})

	t.Run("handles escaped and operator-like top-level keys in fail-closed mode", func(t *testing.T) {
		payload := json.RawMessage(`{"endpoint":"visible","secret.key":"dot","secret*key":"star","secret?key":"question","secret\\key":"slash","@this":"operator","a|b":"pipe"}`)
		masked, maskErr := maskAll(payload)
		require.False(t, maskErr)
		require.Equal(t, maskedValue, gjson.GetBytes(masked, `secret\.key`).String())
		require.Equal(t, maskedValue, gjson.GetBytes(masked, `secret\*key`).String())
		require.Equal(t, maskedValue, gjson.GetBytes(masked, `secret\?key`).String())
		require.Equal(t, maskedValue, gjson.GetBytes(masked, `secret\\key`).String())
		require.Equal(t, maskedValue, gjson.GetBytes(masked, `\@this`).String())
		require.Equal(t, maskedValue, gjson.GetBytes(masked, `a\|b`).String())
		require.Equal(t, "visible", gjson.GetBytes(masked, "endpoint").String())
	})

	t.Run("masks duplicate non-endpoint top-level keys in fail-closed mode", func(t *testing.T) {
		masked, maskErr := maskAll(json.RawMessage(`{"token":"first","endpoint":"visible","token":"second"}`))
		require.False(t, maskErr)
		require.Contains(t, string(masked), `"endpoint":"visible"`)
		require.NotContains(t, string(masked), "first")
		require.NotContains(t, string(masked), "second")
	})

	t.Run("replaces malformed or non-object payloads with a masked object", func(t *testing.T) {
		testCases := []struct {
			name    string
			payload string
		}{
			{name: "invalid JSON", payload: `{"headers":"secret"`},
			{name: "array", payload: `["secret"]`},
			{name: "scalar", payload: `"secret"`},
		}
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				masked, maskErr := maskAll(json.RawMessage(tc.payload))
				require.True(t, maskErr)
				require.JSONEq(t, `{"masked":"******"}`, string(masked))
			})
		}
	})

	t.Run("listed masking fails closed with a masked object for unsupported payloads", func(t *testing.T) {
		testCases := []struct {
			name    string
			payload string
		}{
			{name: "scalar", payload: `"secret-token"`},
			{name: "invalid JSON", payload: `{"headers":{"Authorization":"secret"}`},
		}
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				masked, reason := maskForDelivery(json.RawMessage(tc.payload), []string{"headers.Authorization"}, true)
				require.Equal(t, "mask_error", reason)
				require.JSONEq(t, `{"masked":"******"}`, string(masked))
			})
		}
	})

	t.Run("delivery masking fails closed for unsupported payloads even when no paths are listed", func(t *testing.T) {
		testCases := []struct {
			name    string
			payload string
		}{
			{name: "scalar", payload: `"secret-token"`},
			{name: "invalid JSON", payload: `{"headers":{"Authorization":"secret"}`},
			{name: "array with a non-object request", payload: `[{"endpoint":"first"},"secret-token"]`},
		}
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				masked, reason := maskForDelivery(json.RawMessage(tc.payload), []string{}, true)
				require.Equal(t, "mask_error", reason)
				require.JSONEq(t, `{"masked":"******"}`, string(masked))
			})
		}
	})

	t.Run("fails closed for the whole array when applying a listed path fails", func(t *testing.T) {
		payload := json.RawMessage(`[
			{"endpoint":"first","headers":{"Authorization":"secret"}},
			{"endpoint":"second","headers":{"Authorization":"other-secret"}}
		]`)
		masked, reason := maskForDelivery(payload, []string{"headers.Authorization|@reverse"}, true)

		require.Equal(t, "mask_error", reason)
		require.JSONEq(t, `{"masked":"******"}`, string(masked))
	})

	t.Run("falls back to fail-closed masking for an invalid listed path", func(t *testing.T) {
		payload := json.RawMessage(`{"endpoint":"visible","headers":{"Authorization":"secret"}}`)
		masked, reason := maskForDelivery(payload, []string{""}, true)
		require.Equal(t, "mask_error", reason)
		require.Equal(t, "visible", gjson.GetBytes(masked, "endpoint").String())
		require.Equal(t, maskedValue, gjson.GetBytes(masked, "headers").String())
		require.NotContains(t, string(masked), "secret")
	})

	t.Run("falls back to fail-closed masking when sjson leaves an existing listed path unchanged", func(t *testing.T) {
		payload := json.RawMessage(`{"endpoint":"visible","headers":{"Authorization":"secret"},"body":{"token":"secret"}}`)
		masked, reason := maskForDelivery(payload, []string{"headers.Authorization|@reverse"}, true)
		require.Equal(t, "mask_error", reason)
		require.Equal(t, "visible", gjson.GetBytes(masked, "endpoint").String())
		require.Equal(t, maskedValue, gjson.GetBytes(masked, "headers").String())
		require.Equal(t, maskedValue, gjson.GetBytes(masked, "body").String())
		require.NotContains(t, string(masked), "secret")
	})
}
