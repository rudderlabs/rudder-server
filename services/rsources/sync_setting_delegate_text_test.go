package rsources

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/require"
)

// The extraction mechanics of sync_setting_delegate_text.go: unwrap -> clip ->
// sanitise. Everything here is pure; the policy that decides whether these run at all
// lives in sync_setting_delegate_test.go.

// TestSyncSettingDelegateUnwrapErrorResponse pins the five verified envelope shapes a job status'
// ErrorResponse can carry (test plan A1.1-A1.5) plus the precedence between them.
func TestSyncSettingDelegateUnwrapErrorResponse(t *testing.T) {
	t.Run("A1.1 router http delivery wraps the destination body under response", func(t *testing.T) {
		// router/worker.go: EnhanceJSON(EmptyPayload, "response", trimmedResponse)
		require.Equal(t,
			`{"message":"invalid property"}`,
			unwrapErrorResponse([]byte(`{"response":"{\"message\":\"invalid property\"}"}`)),
		)
	})

	t.Run("A1.2 drain/abort wraps our internal reason under reason", func(t *testing.T) {
		// router/worker.go + batchrouter/worker.go: EnhanceJSON(..., "reason", abortReason)
		require.Equal(t, "source_not_found", unwrapErrorResponse([]byte(`{"reason":"source_not_found"}`)))
	})

	t.Run("A1.3 batchrouter wraps the pipeline error under a capital Error", func(t *testing.T) {
		// batchrouter: jsonrs.Marshal(ErrorResponse{Error: errOccurred.Error()})
		require.Equal(t, "upload failed", unwrapErrorResponse([]byte(`{"Error":"upload failed"}`)))
	})

	t.Run("A1.4 async poll wraps the poll error under a lowercase error", func(t *testing.T) {
		// batchrouter/handle_async.go: UpdateJSONWithNewKeyVal(EmptyPayload, "error", pollResp.Error)
		require.Equal(t, "poll failed", unwrapErrorResponse([]byte(`{"error":"poll failed"}`)))
	})

	t.Run("A1.5 processor dropped jobs carry no error response", func(t *testing.T) {
		require.Equal(t, "", unwrapErrorResponse(nil))
		require.Equal(t, "", unwrapErrorResponse([]byte{}))
		require.Equal(t, "", unwrapErrorResponse([]byte(`{}`)))
	})

	t.Run("unknown envelope keys yield no message rather than the raw wrapper", func(t *testing.T) {
		require.Equal(t, "", unwrapErrorResponse([]byte(`{"success":"OK"}`)))
		require.Equal(t, "", unwrapErrorResponse([]byte(`{"firstAttemptedAt":"2026-08-20T00:00:00.000Z"}`)))
	})

	t.Run("wrapper fields alongside the message are ignored", func(t *testing.T) {
		require.Equal(t,
			"boom",
			unwrapErrorResponse([]byte(`{"firstAttemptedAt":"2026-08-20T00:00:00.000Z","response":"boom","dontBatch":true}`)),
		)
	})

	t.Run("reason wins over a stale response from an earlier delivery attempt", func(t *testing.T) {
		// router/worker.go drains by enhancing the PREVIOUS status' ErrorResponse with
		// "reason", so a drained job carries both. ErrorCode is 410 for every RS abort
		// and the raw text is the only thing that distinguishes them, so the abort
		// reason - not the stale destination body - has to win.
		require.Equal(t,
			"job expired",
			unwrapErrorResponse([]byte(`{"response":"429 too many requests","reason":"job expired"}`)),
		)
	})

	t.Run("the envelope key match is case sensitive", func(t *testing.T) {
		// gjson path lookups fall back to a case-insensitive match, which would
		// conflate Error/error; the delegate switches on the exact key instead.
		require.Equal(t, "capital", unwrapErrorResponse([]byte(`{"Error":"capital"}`)))
		require.Equal(t, "lower", unwrapErrorResponse([]byte(`{"error":"lower"}`)))
		require.Equal(t, "capital", unwrapErrorResponse([]byte(`{"error":"lower","Error":"capital"}`)),
			"Error outranks error in the resolution order")
		require.Equal(t, "", unwrapErrorResponse([]byte(`{"REASON":"shouty"}`)))
	})

	t.Run("malformed or non-object envelopes do not panic", func(t *testing.T) {
		require.Equal(t, "", unwrapErrorResponse([]byte(`not json at all`)))
		require.Equal(t, "", unwrapErrorResponse([]byte(`"a bare string"`)))
		require.Equal(t, "", unwrapErrorResponse([]byte(`[1,2,3]`)))
		require.Equal(t, "", unwrapErrorResponse([]byte(`{"response":`)))
	})

	t.Run("a structured message is kept as raw json", func(t *testing.T) {
		require.Equal(t, `{"code":"E42"}`, unwrapErrorResponse([]byte(`{"error":{"code":"E42"}}`)))
	})
}

// TestSyncSettingDelegateCaptureErrorText pins the cap and the sanitisation applied before storage
// (test plan A1.6-A1.8).
func TestSyncSettingDelegateCaptureErrorText(t *testing.T) {
	t.Run("A1.6 a message longer than the cap is clipped with no truncation marker", func(t *testing.T) {
		body := strings.Repeat("a", 5000)
		text, clipped := captureErrorText([]byte(`{"response":"`+body+`"}`), 2048)
		require.True(t, clipped)
		require.Len(t, text, 2048)
		require.Equal(t, strings.Repeat("a", 2048), text)
	})

	t.Run("A1.6 a message at or below the cap is untouched", func(t *testing.T) {
		text, clipped := captureErrorText([]byte(`{"response":"`+strings.Repeat("a", 2048)+`"}`), 2048)
		require.False(t, clipped)
		require.Len(t, text, 2048)

		text, clipped = captureErrorText([]byte(`{"response":"short"}`), 2048)
		require.False(t, clipped)
		require.Equal(t, "short", text)
	})

	t.Run("A1.7 invalid utf8 is sanitised after the cap, keeping the result within the cap", func(t *testing.T) {
		// An unpaired surrogate: well formed JSON bytes, invalid utf8.
		invalid := "\xed\xa0\x80"
		text, _ := captureErrorText(rawErrorResponse("response", "bad "+invalid+" tail"), 2048)
		require.True(t, utf8.ValidString(text), "captured text must be valid utf8: %q", text)
		require.Contains(t, text, "bad ")
		require.Contains(t, text, " tail")

		// Sanitisation must be length preserving so it cannot push the value back
		// over the cap.
		text, clipped := captureErrorText(rawErrorResponse("response", strings.Repeat(invalid, 2000)), 2048)
		require.True(t, clipped)
		require.LessOrEqual(t, len(text), 2048)
		require.True(t, utf8.ValidString(text))
	})

	t.Run("an empty envelope yields an empty message and is not reported as clipped", func(t *testing.T) {
		text, clipped := captureErrorText(nil, 2048)
		require.Equal(t, "", text)
		require.False(t, clipped)
	})

	t.Run("a non positive cap fails closed instead of storing unbounded text", func(t *testing.T) {
		for _, maxBytes := range []int{0, -1, -2048} {
			text, clipped := captureErrorText([]byte(`{"response":"boom"}`), maxBytes)
			require.Equal(t, "", text, "cap %d", maxBytes)
			require.True(t, clipped, "cap %d must be reported as clipped so the operator sees it", maxBytes)
		}
	})
}

// TestSyncSettingDelegateClipToBytes pins the rune-boundary walk in isolation, including the bound that
// stops malformed input from eating the whole value.
func TestSyncSettingDelegateClipToBytes(t *testing.T) {
	tests := []struct {
		name     string
		in       string
		maxBytes int
		want     string
		clipped  bool
	}{
		{name: "shorter than the cap", in: "abc", maxBytes: 8, want: "abc"},
		{name: "exactly at the cap", in: "abcd", maxBytes: 4, want: "abcd"},
		{name: "one byte over", in: "abcde", maxBytes: 4, want: "abcd", clipped: true},
		{name: "cut lands mid rune", in: "aé", maxBytes: 2, want: "a", clipped: true},
		{name: "cut lands on a rune start", in: "aéb", maxBytes: 3, want: "aé", clipped: true},
		{name: "4 byte rune straddling the cut", in: "a𝄞", maxBytes: 3, want: "a", clipped: true},
		{name: "empty input", in: "", maxBytes: 4, want: ""},
		{
			// A run of continuation bytes with no rune start: the walk is bounded to
			// UTFMax-1 steps, so at most 3 bytes are given back, never the whole value.
			name: "malformed continuation bytes only", in: "\x80\x80\x80\x80\x80", maxBytes: 4,
			want: "\x80", clipped: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, clipped := clipToBytes(tc.in, tc.maxBytes)
			require.Equal(t, tc.want, got)
			require.Equal(t, tc.clipped, clipped)
			require.LessOrEqual(t, len(got), max(tc.maxBytes, 0))
		})
	}
}

// TestSyncSettingDelegateSanitizeErrorText pins the two things postgres cannot take: NUL bytes and
// invalid utf8.
func TestSyncSettingDelegateSanitizeErrorText(t *testing.T) {
	t.Run("valid utf8 without NULs is returned untouched", func(t *testing.T) {
		const s = "plain ascii, é, 𝄞, and a tab\t"
		require.Equal(t, s, sanitizeErrorText(s))
	})

	t.Run("NUL bytes are stripped, not replaced", func(t *testing.T) {
		require.Equal(t, "ab", sanitizeErrorText("a\x00b"))
		require.Equal(t, "", sanitizeErrorText("\x00\x00"))
	})

	t.Run("invalid utf8 is replaced in place, preserving the byte length", func(t *testing.T) {
		in := "head\xed\xa0\x80tail"
		out := sanitizeErrorText(in)
		require.True(t, utf8.ValidString(out), "%q", out)
		require.Len(t, out, len(in), "the replacement must be length preserving")
		require.True(t, strings.HasPrefix(out, "head"))
		require.True(t, strings.HasSuffix(out, "tail"))
	})

	t.Run("a NUL alongside invalid utf8 is handled in one pass", func(t *testing.T) {
		out := sanitizeErrorText("a\x00b\xed\xa0\x80c")
		require.True(t, utf8.ValidString(out), "%q", out)
		require.NotContains(t, out, "\x00")
	})
}

// rawErrorResponse builds an ErrorResponse envelope whose value carries bytes that
// cannot be written as a JSON string literal in source (raw NUL, invalid utf8).
func rawErrorResponse(key, value string) []byte {
	const hex = "0123456789abcdef"
	var b strings.Builder
	b.WriteString(`{"`)
	b.WriteString(key)
	b.WriteString(`":"`)
	for _, c := range []byte(value) {
		switch {
		case c == '"' || c == '\\':
			b.WriteByte('\\')
			b.WriteByte(c)
		case c < 0x20:
			b.WriteString(`\u00`)
			b.WriteByte(hex[c>>4])
			b.WriteByte(hex[c&0xf])
		default:
			b.WriteByte(c)
		}
	}
	b.WriteString(`"}`)
	return []byte(b.String())
}
