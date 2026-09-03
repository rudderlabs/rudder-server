package rsources

import (
	"encoding/json"
	"strings"
	"unicode/utf8"

	"github.com/tidwall/gjson"

	kitutf8 "github.com/rudderlabs/rudder-go-kit/utf8"
)

// The extraction mechanics of the sync setting delegate: given a job status'
// ErrorResponse envelope, produce the text stored in `error_response`.

// errorResponseEnvelopeKeys lists, in resolution order, the keys under which the
// pipeline wraps the final recorded error on a job status' ErrorResponse:
//
//	reason   - drain/abort and retry exhaustion (router/worker.go,
//	           batchrouter/worker.go, batchrouter/handle.go); our own reason, always
//	           paired with error code 410
//	response - router HTTP delivery (router/worker.go); the destination's body,
//	           already trimmed to 10KB upstream
//	Error    - batchrouter object-storage upload and async upload
//	error    - batchrouter async poll
//
// `reason` outranks `response` because a drain enhances the PREVIOUS status'
// ErrorResponse, so a drained job carries both: the stale destination body from the
// last delivery attempt and the reason we finally gave up. Every RS abort reports
// code 410 and the raw text is the only thing that distinguishes them, so the reason
// is the "final error RudderStack recorded".
var errorResponseEnvelopeKeys = [...]string{"reason", "response", "Error", "error"}

// unwrapErrorResponse extracts the error text out of the job status' ErrorResponse
// envelope. An envelope we do not recognise, or one that is not a JSON object,
// yields an empty string rather than the raw wrapper - storing wrapper bookkeeping
// (firstAttemptedAt, dontBatch, ...) as if it were the customer's error would be
// worse than storing nothing.
func unwrapErrorResponse(errorResponse json.RawMessage) string {
	parsed := gjson.ParseBytes(errorResponse)
	if !parsed.IsObject() {
		return ""
	}
	// gjson path lookups are case-sensitive, so `Error` and `error` stay distinct
	// keys; precedence between them is the order of errorResponseEnvelopeKeys.
	for _, key := range errorResponseEnvelopeKeys {
		if v := parsed.Get(key).String(); v != "" {
			return v
		}
	}
	return ""
}

// captureErrorText produces the value stored in `error_response` for one failed
// record: unwrap, then cap, then sanitise.
//
// The cap runs before sanitisation on purpose. SanitizeJSON no longer replaces
// invalid utf8 (only NUL), so validation has to happen here, and it has to happen on
// the already-clipped value: clipping first and validating after guarantees the
// stored bytes are valid utf8 no matter where the cut landed. Sanitisation is
// length-preserving or shrinking, so it can never push the value back over the cap.
//
// No truncation marker is added - the clip is silent by design.
func captureErrorText(errorResponse json.RawMessage, maxBytes int) (text string, clipped bool) {
	text = unwrapErrorResponse(errorResponse)
	if text == "" {
		return "", false
	}
	if maxBytes <= 0 {
		// Misconfigured cap: fail closed rather than storing unbounded text.
		return "", true
	}
	text, clipped = clipToBytes(text, maxBytes)
	return sanitizeErrorText(text), clipped
}

// clipToBytes truncates s to at most maxBytes, cutting on a rune boundary so that a
// multi-byte rune is never split in half.
func clipToBytes(s string, maxBytes int) (string, bool) {
	if len(s) <= maxBytes {
		return s, false
	}
	end := maxBytes
	// Walk back to the start of the rune straddling the cut. A rune is at most
	// utf8.UTFMax bytes, so at most UTFMax-1 steps are ever needed; bounding the
	// walk stops malformed input from eating the whole value.
	for i := 0; i < utf8.UTFMax-1 && end > 0 && !utf8.RuneStart(s[end]); i++ {
		end--
	}
	return s[:end], true
}

// sanitizeErrorText makes the captured text safe to store in a postgres text column:
// NUL bytes are stripped (postgres rejects them) and invalid utf8 byte sequences are
// replaced in place with a single-byte replacement character.
func sanitizeErrorText(s string) string {
	if strings.IndexByte(s, 0) >= 0 {
		s = strings.ReplaceAll(s, "\x00", "")
	}
	if utf8.ValidString(s) {
		return s
	}
	b := []byte(s)
	kitutf8.Sanitize(b)
	return string(b)
}
