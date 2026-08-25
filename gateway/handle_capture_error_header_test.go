package gateway

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"

	gwtypes "github.com/rudderlabs/rudder-server/gateway/types"
)

// captureErrorDetailHeader is the opt-in header rudder-sources sends on the internal rETL
// endpoint, beside X-Rudder-Job-Run-Id / X-Rudder-Task-Run-Id.
const captureErrorDetailHeader = "X-Rudder-Capture-Error-Detail"

// authContextFromCaptureErrorHeaders runs the real middleware helper over a request carrying the
// given headers, so these tests exercise header -> auth context -> params end to end instead of
// hand-setting CaptureErrorDetail. Headers are the only intake for the opt-in, so this is the only
// way a request can legitimately switch capture on.
func authContextFromCaptureErrorHeaders(t *testing.T, headers map[string]string) *gwtypes.AuthRequestContext {
	t.Helper()

	r := httptest.NewRequest(http.MethodPost, "/internal/v1/retl", http.NoBody)
	for k, v := range headers {
		r.Header.Set(k, v)
	}

	arctx := captureErrorAuthContext()
	augmentAuthRequestContext(arctx, r)
	return arctx
}

// forgedCaptureErrorEvent is what a client sends if it tries to turn error capture on the old
// (pre-header) way, or if a mapped rETL column happens to be named capture_error. Nothing reads
// it, and nothing strips it either.
func forgedCaptureErrorEvent() map[string]any {
	return map[string]any{
		"type":   "track",
		"userId": "user-forged",
		"context": map[string]any{
			"sources": map[string]any{
				"job_run_id":    "run-forged",
				"capture_error": true,
			},
			"library": map[string]any{"name": "sdk", "version": "1.0.0"},
		},
	}
}

// rETL traffic that simply does not opt in: the job run id header is present, the opt-in header is
// not. The key must be omitted from params entirely rather than written as false, so the params a
// non-opted-in connection produces stay byte-identical to a server without this feature.
func TestGetJobDataFromRequest_CaptureError_OmittedWhenHeaderAbsent(t *testing.T) {
	gw := newCaptureErrorTestGateway(t)

	authCtx := authContextFromCaptureErrorHeaders(t, map[string]string{
		"X-Rudder-Job-Run-Id": "run-1",
	})
	require.False(t, authCtx.CaptureErrorDetail, "test assumes the opt-in header was not sent")

	req := buildCaptureErrorRequest(t, authCtx, []map[string]any{
		{"type": "track", "userId": "user-1"},
	})

	jobData, err := gw.getJobDataFromRequest(req)
	require.NoError(t, err)
	require.Len(t, jobData.jobs, 1)

	require.False(t, gjson.GetBytes(jobData.jobs[0].Parameters, "capture_error").Exists(),
		"capture_error must be absent from params when the opt-in header was not sent")
	require.Equal(t, "run-1", gjson.GetBytes(jobData.jobs[0].Parameters, "source_job_run_id").String(),
		"the sibling run id must still be carried, proving this was rETL traffic")
}

// Both signals present: the opt-in header AND a forged context.sources.capture_error in the
// payload. The header decides, and the payload is still forwarded verbatim - capture being ON must
// not start a strip. Complements the header-absent case in handle_capture_error_test.go (A3.2).
func TestGetJobDataFromRequest_CaptureError_HeaderDecidesAndPayloadSurvives(t *testing.T) {
	gw := newCaptureErrorTestGateway(t)

	tests := []struct {
		name    string
		headers map[string]string
		want    bool
	}{
		{
			name:    "forged payload, no opt-in header",
			headers: map[string]string{"X-Rudder-Job-Run-Id": "run-1"},
			want:    false,
		},
		{
			name: "forged payload and opt-in header",
			headers: map[string]string{
				"X-Rudder-Job-Run-Id":    "run-1",
				captureErrorDetailHeader: "true",
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event := forgedCaptureErrorEvent()
			// Marshalled up front so the comparison below is against exactly what was sent,
			// independent of any mutation getJobDataFromRequest makes to the shared map.
			sentContext, err := jsonrs.Marshal(event["context"])
			require.NoError(t, err)

			req := buildCaptureErrorRequest(t, authContextFromCaptureErrorHeaders(t, tt.headers), []map[string]any{event})

			jobData, err := gw.getJobDataFromRequest(req)
			require.NoError(t, err)
			require.Len(t, jobData.jobs, 1)

			require.Equal(t, tt.want, gjson.GetBytes(jobData.jobs[0].Parameters, "capture_error").Exists(),
				"params.capture_error must be decided by the header alone")

			// The gateway re-marshals the payload and injects rudderId, messageId, receivedAt and
			// request_ip at the event level, so whole-payload byte equality is not a well defined
			// property. The context subtree - where the opt-in used to be read and stripped - is,
			// and it must round-trip unchanged, forged key included.
			storedContext := gjson.GetBytes(jobData.jobs[0].EventPayload, "batch.0.context")
			require.True(t, storedContext.Exists(), "the event context must survive")
			require.JSONEq(t, string(sentContext), storedContext.Raw,
				"context must be forwarded verbatim: nothing is added to or removed from the payload")
			require.True(t, gjson.GetBytes(jobData.jobs[0].EventPayload, "batch.0.context.sources.capture_error").Bool(),
				"a forged context.sources.capture_error is inert, not removed")
		})
	}
}

// Extends the strictness table in handle_capture_error_test.go with the values that pin the two
// properties most likely to be loosened by a well meaning refactor: case sensitivity and the
// absence of any trimming. Matching mirrors the run id headers beside it, which are taken
// verbatim - Go canonicalises the header NAME, never the value.
func TestAugmentAuthRequestContext_CaptureErrorDetail_ExactValueOnly(t *testing.T) {
	tests := []struct {
		name   string
		header string
	}{
		{name: "capitalised True", header: "True"},
		{name: "mixed case TrUe", header: "TrUe"},
		{name: "leading space", header: " true"},
		{name: "trailing space", header: "true "},
		{name: "explicit false", header: "false"},
		{name: "json boolean-ish", header: `"true"`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPost, "/internal/v1/retl", http.NoBody)
			r.Header.Set("X-Rudder-Job-Run-Id", "run-1")
			r.Header.Set(captureErrorDetailHeader, tt.header)

			arctx := &gwtypes.AuthRequestContext{}
			augmentAuthRequestContext(arctx, r)

			require.Falsef(t, arctx.CaptureErrorDetail, "header value %q must not opt in", tt.header)
			require.Equal(t, "run-1", arctx.SourceJobRunID, "job run id seeding must be unaffected")
		})
	}
}

// The payload lookups the opt-in used to share (context.sources.job_run_id / task_run_id) are
// still live, so odd context shapes must keep parsing without panicking - and must never produce a
// capture_error parameter now that the payload is not an intake at all.
func TestGetJobDataFromRequest_CaptureError_MalformedContextShapes(t *testing.T) {
	gw := newCaptureErrorTestGateway(t)

	tests := []struct {
		name  string
		event map[string]any
	}{
		{
			name:  "no context key at all",
			event: map[string]any{"type": "track", "userId": "user-2"},
		},
		{
			name: "context present without sources",
			event: map[string]any{
				"type":    "track",
				"userId":  "user-3",
				"context": map[string]any{"library": map[string]any{"name": "sdk", "version": "1.0.0"}},
			},
		},
		{
			name: "context.sources is a string, not a map",
			event: map[string]any{
				"type":    "track",
				"userId":  "user-4",
				"context": map[string]any{"sources": "not-a-map"},
			},
		},
		{
			name: "context.sources.capture_error is a string",
			event: map[string]any{
				"type":   "track",
				"userId": "user-5",
				"context": map[string]any{
					"sources": map[string]any{"job_run_id": "run-1", "capture_error": "true"},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// No opt-in header: whatever the payload says, the key must stay out of params.
			req := buildCaptureErrorRequest(t, authContextFromCaptureErrorHeaders(t, map[string]string{
				"X-Rudder-Job-Run-Id": "run-1",
			}), []map[string]any{tt.event})

			jobData, err := gw.getJobDataFromRequest(req)
			require.NoError(t, err)
			require.Len(t, jobData.jobs, 1)
			require.False(t, gjson.GetBytes(jobData.jobs[0].Parameters, "capture_error").Exists())
		})
	}
}
