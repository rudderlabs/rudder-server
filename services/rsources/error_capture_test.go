package rsources

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
)

// TestUnwrapErrorResponse pins the five verified envelope shapes a job status'
// ErrorResponse can carry (test plan A1.1-A1.5) plus the precedence between them.
func TestUnwrapErrorResponse(t *testing.T) {
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

// TestCaptureErrorText pins the cap and the sanitisation applied before storage
// (test plan A1.6-A1.8).
func TestCaptureErrorText(t *testing.T) {
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

	t.Run("A1.6 the cap never splits a multi-byte rune", func(t *testing.T) {
		// 'e-acute' is 2 bytes, so a 5 byte cap lands mid-rune on the third character.
		text, clipped := captureErrorText([]byte(`{"response":"ééé"}`), 5)
		require.True(t, clipped)
		require.True(t, utf8.ValidString(text), "clipped text must remain valid utf8")
		require.Equal(t, "éé", text)
		require.LessOrEqual(t, len(text), 5)
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

	t.Run("A1.8 embedded NUL bytes are stripped", func(t *testing.T) {
		text, _ := captureErrorText(rawErrorResponse("response", "before\x00after"), 2048)
		require.Equal(t, "beforeafter", text)

		text, _ = captureErrorText(rawErrorResponse("reason", "a\x00b"), 2048)
		require.Equal(t, "ab", text)
	})

	t.Run("an empty envelope yields an empty message and is not reported as clipped", func(t *testing.T) {
		text, clipped := captureErrorText(nil, 2048)
		require.Equal(t, "", text)
		require.False(t, clipped)
	})

	t.Run("a non positive cap fails closed instead of storing unbounded text", func(t *testing.T) {
		text, clipped := captureErrorText([]byte(`{"response":"boom"}`), 0)
		require.Equal(t, "", text)
		require.True(t, clipped)
	})
}

// TestErrorCaptureGateBlocked pins the operator storm blacklist lookup.
func TestErrorCaptureGateBlocked(t *testing.T) {
	t.Run("the connection key is the server side sourceID:destinationID", func(t *testing.T) {
		g := errorCaptureGate{blockedConnections: []string{"src-1:dst-1"}}

		reason, blocked := g.blocked("src-1", "dst-1", "ws-1")
		require.True(t, blocked)
		require.Equal(t, blockedScopeConnection, reason)

		_, blocked = g.blocked("src-1", "dst-2", "ws-1")
		require.False(t, blocked)
		_, blocked = g.blocked("src-2", "dst-1", "ws-1")
		require.False(t, blocked)
	})

	t.Run("workspaces are blocked on their own key", func(t *testing.T) {
		g := errorCaptureGate{blockedWorkspaces: []string{"ws-1"}}

		reason, blocked := g.blocked("src-1", "dst-1", "ws-1")
		require.True(t, blocked)
		require.Equal(t, blockedScopeWorkspace, reason)

		_, blocked = g.blocked("src-1", "dst-1", "ws-2")
		require.False(t, blocked)
	})

	t.Run("an empty workspace id never matches an empty blacklist entry", func(t *testing.T) {
		g := errorCaptureGate{blockedWorkspaces: []string{""}}
		_, blocked := g.blocked("src-1", "dst-1", "")
		require.False(t, blocked)
	})

	t.Run("nothing is blocked by default", func(t *testing.T) {
		var g errorCaptureGate
		_, blocked := g.blocked("src-1", "dst-1", "ws-1")
		require.False(t, blocked)
	})
}

// TestErrorCaptureSettings pins the config keys, their defaults, that they are hot
// reloadable, and that the shared registration is idempotent (NewStatsCollector runs
// once per status-update batch on the router hot path).
func TestErrorCaptureSettings(t *testing.T) {
	conf := config.New()
	s := newErrorCaptureSettings(conf, nil)

	require.False(t, s.enabled.Load(), "capture must be off by default")
	require.Equal(t, defaultMaxErrorLength, s.maxErrorLength.Load())
	require.Empty(t, s.blockedConnections.Load())
	require.Empty(t, s.blockedWorkspaces.Load())

	conf.Set("Rsources.failedKeys.captureErrorDetail", true)
	conf.Set("Rsources.failedKeys.maxErrorLength", 64)
	conf.Set("Rsources.failedKeys.blockedConnections", []string{"src-1:dst-1"})
	conf.Set("Rsources.failedKeys.blockedWorkspaces", []string{"ws-1"})

	require.True(t, s.enabled.Load(), "the global switch must be hot reloadable")
	require.Equal(t, 64, s.maxErrorLength.Load())
	require.Equal(t, []string{"src-1:dst-1"}, s.blockedConnections.Load())
	require.Equal(t, []string{"ws-1"}, s.blockedWorkspaces.Load())

	require.NotPanics(t, func() {
		for range 3 {
			_ = sharedErrorCaptureSettings()
		}
	}, "repeated registration of the shared settings must not panic or grow the registry")

	// The settings must follow config.Default across a Reset, otherwise capture
	// silently stays off for anything that resets the config at startup.
	before := sharedErrorCaptureSettings()
	config.Reset()
	after := sharedErrorCaptureSettings()
	require.NotSame(t, before.enabled, after.enabled,
		"settings must rebind to the new default config after a reset")
	config.Default.Set(captureErrorDetailKey, true)
	require.True(t, sharedErrorCaptureSettings().enabled.Load())
	config.Default.Set(captureErrorDetailKey, false)
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
