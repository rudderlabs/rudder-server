package pytransformer_contract

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-server/processor/usertransformer"
)

// TestPyTransformerTestEndpoints pins the wire contract of the four
// control-plane endpoints for the Python transformation-test flow:
//
//	POST /test
//	POST /testRun
//	POST /test-library
//	POST /extract-libs
//
// Both sides go through usertransformer.Client.Test/TestRun/TestLibrary/
// ExtractLibs — the same methods cpservice.Forward uses in production — so the
// test covers the exact client → pyt path. Only the target base URL differs:
// the baseline release on one side, the candidate on the other, which is the
// role cpservice.Forward plays in production.
//
// Responses are compared byte-for-byte. Several assertions below carry comments
// about wording that "deliberately differs" from openfaas's fn-ast; those record
// why pyt's wording is what it is, and are kept as documentation of the intended
// message. They no longer describe a difference between the two sides.
func TestPyTransformerTestEndpoints(t *testing.T) {
	env := newTestEndpointsEnv(t)

	t.Run("test endpoint", func(t *testing.T) {
		t.Run("should run inline code and return transformed events with logs", func(t *testing.T) {
			payload := map[string]any{
				"trRevCode": map[string]any{
					"code":        "def transformEvent(event, metadata):\n    log('hello from test')\n    event['foo'] = 'bar'\n    return event",
					"codeVersion": "1",
					"language":    "pythonfaas",
				},
				"events": []map[string]any{
					{"message": map[string]any{"messageId": "m1"}, "metadata": map[string]any{"messageId": "m1"}},
					{"message": map[string]any{"messageId": "m2"}, "metadata": map[string]any{"messageId": "m2"}},
				},
			}
			baselineStatus, baselineBody := env.callBaseline(t, env.client.Test, payload)
			candidateStatus, candidateBody := env.callCandidate(t, env.client.Test, payload)

			require.Equal(t, http.StatusOK, candidateStatus, "body: %s", candidateBody)
			require.Equal(t, baselineStatus, candidateStatus, "old status %d, old body: %s", baselineStatus, baselineBody)
			resp := decodeFlow(t, candidateBody)
			require.Len(t, resp.TransformedEvents, 2)
			for _, ev := range resp.TransformedEvents {
				require.Equal(t, "bar", ev["foo"])
			}
			compareTestFlowBodies(t, baselineBody, candidateBody)
		})

		t.Run("should fetch libraries from the config backend", func(t *testing.T) {
			payload := map[string]any{
				"trRevCode": map[string]any{
					"code":        "import mathhelper\ndef transformEvent(event, metadata):\n    event['doubled'] = mathhelper.double(event['value'])\n    return event",
					"codeVersion": "1",
					"language":    "pythonfaas",
				},
				"events": []map[string]any{
					{"message": map[string]any{"messageId": "m1", "value": 21}, "metadata": map[string]any{"messageId": "m1"}},
				},
				"libraryVersionIDs": []string{libVersionID},
			}
			baselineStatus, baselineBody := env.callBaseline(t, env.client.Test, payload)
			candidateStatus, candidateBody := env.callCandidate(t, env.client.Test, payload)

			require.Equal(t, http.StatusOK, candidateStatus, "body: %s", candidateBody)
			require.Equal(t, baselineStatus, candidateStatus, "old body: %s", baselineBody)
			resp := decodeFlow(t, candidateBody)
			require.Len(t, resp.TransformedEvents, 1)
			require.EqualValues(t, 42, resp.TransformedEvents[0]["doubled"])
			compareTestFlowBodies(t, baselineBody, candidateBody)
		})

		t.Run("should expose request credentials via getCredential", func(t *testing.T) {
			payload := map[string]any{
				"trRevCode": map[string]any{
					"code":        "def transformEvent(event, metadata):\n    event['secret'] = getCredential('API_KEY')\n    return event",
					"codeVersion": "1",
					"language":    "pythonfaas",
				},
				"events": []map[string]any{
					{"message": map[string]any{"messageId": "m1"}, "metadata": map[string]any{"messageId": "m1"}},
				},
				"credentials": []map[string]any{{"key": "API_KEY", "value": "secret123"}},
			}
			baselineStatus, baselineBody := env.callBaseline(t, env.client.Test, payload)
			candidateStatus, candidateBody := env.callCandidate(t, env.client.Test, payload)

			require.Equal(t, http.StatusOK, candidateStatus, "body: %s", candidateBody)
			require.Equal(t, baselineStatus, candidateStatus, "old body: %s", baselineBody)
			resp := decodeFlow(t, candidateBody)
			require.Len(t, resp.TransformedEvents, 1)
			require.Equal(t, "secret123", resp.TransformedEvents[0]["secret"])
			compareTestFlowBodies(t, baselineBody, candidateBody)
		})

		t.Run("should keep a single event's error inline with HTTP 200", func(t *testing.T) {
			payload := map[string]any{
				"trRevCode": map[string]any{
					"code":        "def transformEvent(event, metadata):\n    if event['n'] == 1:\n        raise ValueError('boom')\n    return event",
					"codeVersion": "1",
					"language":    "pythonfaas",
				},
				"events": []map[string]any{
					{"message": map[string]any{"messageId": "m0", "n": 0}, "metadata": map[string]any{"messageId": "m0"}},
					{"message": map[string]any{"messageId": "m1", "n": 1}, "metadata": map[string]any{"messageId": "m1"}},
				},
			}
			baselineStatus, baselineBody := env.callBaseline(t, env.client.Test, payload)
			candidateStatus, candidateBody := env.callCandidate(t, env.client.Test, payload)

			require.Equal(t, http.StatusOK, candidateStatus, "body: %s", candidateBody)
			require.Equal(t, baselineStatus, candidateStatus, "old body: %s", baselineBody)

			resp := decodeFlow(t, candidateBody)
			require.Len(t, resp.TransformedEvents, 2)
			var errored []map[string]any
			for _, ev := range resp.TransformedEvents {
				if errMsg, ok := ev["error"].(string); ok && errMsg != "" {
					errored = append(errored, ev)
				}
			}
			require.Len(t, errored, 1)
			require.Contains(t, errored[0]["error"], "boom")
			require.NotContains(t, errored[0], "metadata",
				"errored /test elements are {error} only, matching rudder-transformer")

			compareTestFlowBodies(t, baselineBody, candidateBody)
		})

		// Both engines key per-event metadata by the message body's messageId,
		// so two events sharing one id must still both be transformed.
		t.Run("should transform both events when messageIds are duplicated", func(t *testing.T) {
			payload := map[string]any{
				"trRevCode": map[string]any{
					"code":        "def transformEvent(event, metadata):\n    event['doubled'] = event['n'] * 2\n    return event",
					"codeVersion": "1",
					"language":    "pythonfaas",
				},
				"events": []map[string]any{
					{"message": map[string]any{"messageId": "dup-1", "n": 1}, "metadata": map[string]any{"messageId": "dup-1", "sourceId": "s1"}},
					{"message": map[string]any{"messageId": "dup-1", "n": 2}, "metadata": map[string]any{"messageId": "dup-1", "sourceId": "s2"}},
				},
			}
			baselineStatus, baselineBody := env.callBaseline(t, env.client.Test, payload)
			candidateStatus, candidateBody := env.callCandidate(t, env.client.Test, payload)

			require.Equal(t, http.StatusOK, candidateStatus, "body: %s", candidateBody)
			require.Equal(t, baselineStatus, candidateStatus, "old body: %s", baselineBody)
			resp := decodeFlow(t, candidateBody)
			require.Len(t, resp.TransformedEvents, 2)
			require.EqualValues(t, 2, resp.TransformedEvents[0]["doubled"])
			require.EqualValues(t, 4, resp.TransformedEvents[1]["doubled"])
			compareTestFlowBodies(t, baselineBody, candidateBody)
		})

		// Metadata without a messageId is echoed as-is on errored elements —
		// neither engine injects one (metadata is keyed by the body's messageId).
		t.Run("should accept metadata without messageId", func(t *testing.T) {
			payload := map[string]any{
				"trRevCode": map[string]any{
					"code":        "def transformEvent(event, metadata):\n    if event['n'] == 1:\n        raise ValueError('no meta id')\n    return event",
					"codeVersion": "1",
					"language":    "pythonfaas",
				},
				"events": []map[string]any{
					{"message": map[string]any{"messageId": "m0", "n": 0}, "metadata": map[string]any{"sourceId": "s0"}},
					{"message": map[string]any{"messageId": "m1", "n": 1}, "metadata": map[string]any{"sourceId": "s1"}},
				},
			}
			baselineStatus, baselineBody := env.callBaseline(t, env.client.Test, payload)
			candidateStatus, candidateBody := env.callCandidate(t, env.client.Test, payload)

			require.Equal(t, http.StatusOK, candidateStatus, "body: %s", candidateBody)
			require.Equal(t, baselineStatus, candidateStatus, "old body: %s", baselineBody)
			resp := decodeFlow(t, candidateBody)
			require.Len(t, resp.TransformedEvents, 2)
			errored := resp.TransformedEvents[1]
			require.Contains(t, errored["error"], "no meta id")
			require.NotContains(t, errored, "metadata",
				"errored /test elements are {error} only, matching rudder-transformer")
			compareTestFlowBodies(t, baselineBody, candidateBody)
		})

		t.Run("should return HTTP 400 with a top-level error for a compile error", func(t *testing.T) {
			payload := map[string]any{
				"trRevCode": map[string]any{
					// Missing colon: whole-execution compile failure.
					"code":        "def transformEvent(event, metadata)\n    return event",
					"codeVersion": "1",
					"language":    "pythonfaas",
				},
				"events": []map[string]any{
					{"message": map[string]any{"messageId": "m1"}, "metadata": map[string]any{"messageId": "m1"}},
				},
			}
			baselineStatus, baselineBody := env.callBaseline(t, env.client.Test, payload)
			candidateStatus, candidateBody := env.callCandidate(t, env.client.Test, payload)

			require.Equal(t, http.StatusBadRequest, candidateStatus, "body: %s", candidateBody)
			require.Equal(t, baselineStatus, candidateStatus, "old body: %s", baselineBody)
			// The wording deliberately differs: the baseline surfaces fn-ast's
			// BadCodeError (its import extraction parses the code before the
			// function is even deployed), pyt surfaces its runtime compiler's
			// message. Both must carry Python's syntax diagnosis.
			oldErr := decodeError(t, baselineBody)
			newErr := decodeError(t, candidateBody)
			t.Logf("compile error — old: %q new: %q", oldErr, newErr)
			require.Contains(t, oldErr, "expected ':'")
			require.Contains(t, newErr, "expected ':'")
		})

		t.Run("should return the verbatim missing-code error", func(t *testing.T) {
			payload := map[string]any{
				"trRevCode": map[string]any{"codeVersion": "1", "language": "pythonfaas"},
				"events": []map[string]any{
					{"message": map[string]any{"messageId": "m1"}, "metadata": map[string]any{"messageId": "m1"}},
				},
			}
			baselineStatus, baselineBody := env.callBaseline(t, env.client.Test, payload)
			candidateStatus, candidateBody := env.callCandidate(t, env.client.Test, payload)

			require.Equal(t, http.StatusBadRequest, candidateStatus, "body: %s", candidateBody)
			require.Equal(t, baselineStatus, candidateStatus)
			require.Equal(t, "Error: Invalid Request. Missing parameters in transformation code block", decodeError(t, candidateBody))
			require.Equal(t, decodeError(t, baselineBody), decodeError(t, candidateBody))
		})

		t.Run("should return the verbatim missing-events error", func(t *testing.T) {
			payload := map[string]any{
				"trRevCode": map[string]any{
					"code":        "def transformEvent(event, metadata):\n    return event",
					"codeVersion": "1",
					"language":    "pythonfaas",
				},
				"events": []map[string]any{},
			}
			baselineStatus, baselineBody := env.callBaseline(t, env.client.Test, payload)
			candidateStatus, candidateBody := env.callCandidate(t, env.client.Test, payload)

			require.Equal(t, http.StatusBadRequest, candidateStatus, "body: %s", candidateBody)
			require.Equal(t, baselineStatus, candidateStatus)
			require.Equal(t, "Error: Invalid request. Missing events", decodeError(t, candidateBody))
			require.Equal(t, decodeError(t, baselineBody), decodeError(t, candidateBody))
		})

		// Security property, not a parity check: pyt compiles with
		// RestrictedPython 8, whose transformer blocks generator/frame
		// introspection attributes (INSPECT_ATTRIBUTES — the CVE-2023-37271
		// sandbox-escape fix: gi_frame, f_back, cr_frame, ...). openfaas ran
		// RestrictedPython 6.1, which predates the fix, and this case existed to
		// pin that tightening. Both versions must now reject it — a release that
		// loosened it back would be a security regression.
		t.Run("should reject frame-introspection code", func(t *testing.T) {
			payload := map[string]any{
				"trRevCode": map[string]any{
					"code":        "def transformEvent(event, metadata):\n    gen = (x for x in [1])\n    event['has_frame'] = gen.gi_frame is None\n    return event",
					"codeVersion": "1",
					"language":    "pythonfaas",
				},
				"events": []map[string]any{
					{"message": map[string]any{"messageId": "m1"}, "metadata": map[string]any{"messageId": "m1"}},
				},
			}
			baselineStatus, baselineBody := env.callBaseline(t, env.client.Test, payload)
			candidateStatus, candidateBody := env.callCandidate(t, env.client.Test, payload)

			require.Equal(t, http.StatusBadRequest, candidateStatus, "candidate must reject at compile, body: %s", candidateBody)
			require.Contains(t, decodeError(t, candidateBody), "gi_frame")
			require.Contains(t, decodeError(t, candidateBody), "restricted name")

			require.Equal(t, baselineStatus, candidateStatus, "baseline body: %s", baselineBody)
			require.Equal(t, decodeError(t, baselineBody), decodeError(t, candidateBody))
		})

		t.Run("should return HTTP 400 when a library cannot be fetched", func(t *testing.T) {
			payload := map[string]any{
				"trRevCode": map[string]any{
					"code":        "def transformEvent(event, metadata):\n    return event",
					"codeVersion": "1",
					"language":    "pythonfaas",
				},
				"events": []map[string]any{
					{"message": map[string]any{"messageId": "m1"}, "metadata": map[string]any{"messageId": "m1"}},
				},
				"libraryVersionIDs": []string{"unknown-library-version"},
			}
			baselineStatus, baselineBody := env.callBaseline(t, env.client.Test, payload)
			candidateStatus, candidateBody := env.callCandidate(t, env.client.Test, payload)

			require.Equal(t, http.StatusBadRequest, candidateStatus, "body: %s", candidateBody)
			require.NotEmpty(t, decodeError(t, candidateBody))
			require.Equal(t, baselineStatus, candidateStatus, "baseline body: %s", baselineBody)
			require.NotEmpty(t, decodeError(t, baselineBody))
		})
	})

	t.Run("testRun endpoint", func(t *testing.T) {
		t.Run("should echo each input event's metadata with the transformed event", func(t *testing.T) {
			payload := map[string]any{
				"codeRevision": map[string]any{
					"code":        "def transformEvent(event, metadata):\n    event['foo'] = 'bar'\n    return event",
					"language":    "pythonfaas",
					"codeVersion": "1",
				},
				"input": []map[string]any{
					{"message": map[string]any{"messageId": "m1"}, "metadata": map[string]any{"messageId": "m1", "sourceId": "s1"}},
				},
			}
			baselineStatus, baselineBody := env.callBaseline(t, env.client.TestRun, payload)
			candidateStatus, candidateBody := env.callCandidate(t, env.client.TestRun, payload)

			require.Equal(t, http.StatusOK, candidateStatus, "body: %s", candidateBody)
			require.Equal(t, baselineStatus, candidateStatus, "old body: %s", baselineBody)
			resp := decodeFlow(t, candidateBody)
			require.Len(t, resp.TransformedEvents, 1)
			el := resp.TransformedEvents[0]
			transformed, ok := el["transformedEvent"].(map[string]any)
			require.True(t, ok, "element carries the transformed event under the transformedEvent key, matching rudder-transformer")
			require.Equal(t, "bar", transformed["foo"])
			require.NotContains(t, el, "statusCode", "no statusCode, matching rudder-transformer")
			compareTestFlowBodies(t, baselineBody, candidateBody)
		})

		// Test-case metadata is user-authored JSON: neither engine validates its
		// field types (an int rudderId must not 400) and unknown keys round-trip.
		t.Run("should not validate user-authored metadata types or keys", func(t *testing.T) {
			payload := map[string]any{
				"codeRevision": map[string]any{
					"code":        "def transformEvent(event, metadata):\n    return event",
					"language":    "pythonfaas",
					"codeVersion": "1",
				},
				"input": []map[string]any{
					{
						"message": map[string]any{"messageId": "m1"},
						"metadata": map[string]any{
							"messageId": "m1",
							"rudderId":  123,
							"jobId":     "not-an-int",
							"customKey": map[string]any{"nested": true},
						},
					},
				},
			}
			baselineStatus, baselineBody := env.callBaseline(t, env.client.TestRun, payload)
			candidateStatus, candidateBody := env.callCandidate(t, env.client.TestRun, payload)

			require.Equal(t, http.StatusOK, candidateStatus, "body: %s", candidateBody)
			require.Equal(t, baselineStatus, candidateStatus, "old body: %s", baselineBody)
			resp := decodeFlow(t, candidateBody)
			require.Len(t, resp.TransformedEvents, 1)
			meta, ok := resp.TransformedEvents[0]["metadata"].(map[string]any)
			require.True(t, ok)
			require.EqualValues(t, 123, meta["rudderId"], "int rudderId is echoed back, not rejected")
			require.Contains(t, meta, "customKey", "unknown metadata keys are echoed back, not dropped")
			compareTestFlowBodies(t, baselineBody, candidateBody)
		})

		t.Run("should resolve dependencies libraries and credentials", func(t *testing.T) {
			payload := map[string]any{
				"codeRevision": map[string]any{
					"code":        "import mathhelper\ndef transformEvent(event, metadata):\n    event['doubled'] = mathhelper.double(event['value'])\n    event['secret'] = getCredential('API_KEY')\n    return event",
					"language":    "pythonfaas",
					"codeVersion": "1",
				},
				"input": []map[string]any{
					{"message": map[string]any{"messageId": "m1", "value": 21}, "metadata": map[string]any{"messageId": "m1"}},
				},
				"dependencies": map[string]any{
					"libraries":   []map[string]any{{"versionId": libVersionID}},
					"credentials": []map[string]any{{"key": "API_KEY", "value": "secret123"}},
				},
			}
			baselineStatus, baselineBody := env.callBaseline(t, env.client.TestRun, payload)
			candidateStatus, candidateBody := env.callCandidate(t, env.client.TestRun, payload)

			require.Equal(t, http.StatusOK, candidateStatus, "body: %s", candidateBody)
			require.Equal(t, baselineStatus, candidateStatus, "old body: %s", baselineBody)
			resp := decodeFlow(t, candidateBody)
			require.Len(t, resp.TransformedEvents, 1)
			transformed, ok := resp.TransformedEvents[0]["transformedEvent"].(map[string]any)
			require.True(t, ok)
			require.EqualValues(t, 42, transformed["doubled"])
			require.Equal(t, "secret123", transformed["secret"])
			compareTestFlowBodies(t, baselineBody, candidateBody)
		})

		t.Run("should keep a per-event error inline with its metadata", func(t *testing.T) {
			payload := map[string]any{
				"codeRevision": map[string]any{
					"code":        "def transformEvent(event, metadata):\n    raise ValueError('boom')",
					"language":    "pythonfaas",
					"codeVersion": "1",
				},
				"input": []map[string]any{
					{"message": map[string]any{"messageId": "m1"}, "metadata": map[string]any{"messageId": "m1"}},
				},
			}
			baselineStatus, baselineBody := env.callBaseline(t, env.client.TestRun, payload)
			candidateStatus, candidateBody := env.callCandidate(t, env.client.TestRun, payload)

			require.Equal(t, http.StatusOK, candidateStatus, "body: %s", candidateBody)
			require.Equal(t, baselineStatus, candidateStatus, "old body: %s", baselineBody)
			resp := decodeFlow(t, candidateBody)
			require.Len(t, resp.TransformedEvents, 1)
			el := resp.TransformedEvents[0]
			require.Contains(t, el["error"], "boom")
			meta, ok := el["metadata"].(map[string]any)
			require.True(t, ok, "errored /testRun elements keep the input event's metadata")
			require.Equal(t, "m1", meta["messageId"])
			require.NotContains(t, el, "statusCode", "no statusCode, matching rudder-transformer")
			compareTestFlowBodies(t, baselineBody, candidateBody)
		})

		// Both engines key echoed metadata by the message body's messageId, so
		// with duplicated ids the LAST event's metadata wins for every element
		// sharing the id — identical (if surprising) behaviour on both sides.
		t.Run("should echo the last event's metadata for duplicated messageIds", func(t *testing.T) {
			payload := map[string]any{
				"codeRevision": map[string]any{
					"code":        "def transformEvent(event, metadata):\n    event['doubled'] = event['n'] * 2\n    return event",
					"language":    "pythonfaas",
					"codeVersion": "1",
				},
				"input": []map[string]any{
					{"message": map[string]any{"messageId": "dup-1", "n": 1}, "metadata": map[string]any{"messageId": "dup-1", "sourceId": "s1"}},
					{"message": map[string]any{"messageId": "dup-1", "n": 2}, "metadata": map[string]any{"messageId": "dup-1", "sourceId": "s2"}},
				},
			}
			baselineStatus, baselineBody := env.callBaseline(t, env.client.TestRun, payload)
			candidateStatus, candidateBody := env.callCandidate(t, env.client.TestRun, payload)

			require.Equal(t, http.StatusOK, candidateStatus, "body: %s", candidateBody)
			require.Equal(t, baselineStatus, candidateStatus, "old body: %s", baselineBody)
			resp := decodeFlow(t, candidateBody)
			require.Len(t, resp.TransformedEvents, 2)
			for i, el := range resp.TransformedEvents {
				transformed, ok := el["transformedEvent"].(map[string]any)
				require.True(t, ok)
				require.EqualValues(t, (i+1)*2, transformed["doubled"])
				meta, ok := el["metadata"].(map[string]any)
				require.True(t, ok)
				require.Equal(t, "s2", meta["sourceId"], "last event's metadata wins for element %d", i)
			}
			compareTestFlowBodies(t, baselineBody, candidateBody)
		})

		t.Run("should echo metadata without messageId as-is", func(t *testing.T) {
			payload := map[string]any{
				"codeRevision": map[string]any{
					"code":        "def transformEvent(event, metadata):\n    event['foo'] = 'bar'\n    return event",
					"language":    "pythonfaas",
					"codeVersion": "1",
				},
				"input": []map[string]any{
					{"message": map[string]any{"messageId": "m1"}, "metadata": map[string]any{"sourceId": "s1"}},
				},
			}
			baselineStatus, baselineBody := env.callBaseline(t, env.client.TestRun, payload)
			candidateStatus, candidateBody := env.callCandidate(t, env.client.TestRun, payload)

			require.Equal(t, http.StatusOK, candidateStatus, "body: %s", candidateBody)
			require.Equal(t, baselineStatus, candidateStatus, "old body: %s", baselineBody)
			resp := decodeFlow(t, candidateBody)
			require.Len(t, resp.TransformedEvents, 1)
			require.Equal(t, map[string]any{"sourceId": "s1"}, resp.TransformedEvents[0]["metadata"],
				"metadata is echoed as-is, without injecting a messageId")
			compareTestFlowBodies(t, baselineBody, candidateBody)
		})

		t.Run("should return HTTP 400 with a top-level error for a compile error", func(t *testing.T) {
			payload := map[string]any{
				"codeRevision": map[string]any{
					// Missing colon: whole-execution compile failure.
					"code":        "def transformEvent(event, metadata)\n    return event",
					"language":    "pythonfaas",
					"codeVersion": "1",
				},
				"input": []map[string]any{
					{"message": map[string]any{"messageId": "m1"}, "metadata": map[string]any{"messageId": "m1"}},
				},
			}
			baselineStatus, baselineBody := env.callBaseline(t, env.client.TestRun, payload)
			candidateStatus, candidateBody := env.callCandidate(t, env.client.TestRun, payload)

			require.Equal(t, http.StatusBadRequest, candidateStatus, "body: %s", candidateBody)
			require.Equal(t, baselineStatus, candidateStatus, "old body: %s", baselineBody)
			// Wording deliberately differs (fn-ast BadCodeError vs pyt's runtime
			// compiler); both must carry Python's syntax diagnosis.
			oldErr := decodeError(t, baselineBody)
			newErr := decodeError(t, candidateBody)
			t.Logf("compile error — old: %q new: %q", oldErr, newErr)
			require.Contains(t, oldErr, "expected ':'")
			require.Contains(t, newErr, "expected ':'")
		})

		t.Run("should return the verbatim missing-code error", func(t *testing.T) {
			payload := map[string]any{
				"codeRevision": map[string]any{"codeVersion": "1", "language": "pythonfaas"},
				"input": []map[string]any{
					{"message": map[string]any{"messageId": "m1"}, "metadata": map[string]any{"messageId": "m1"}},
				},
			}
			baselineStatus, baselineBody := env.callBaseline(t, env.client.TestRun, payload)
			candidateStatus, candidateBody := env.callCandidate(t, env.client.TestRun, payload)

			require.Equal(t, http.StatusBadRequest, candidateStatus, "body: %s", candidateBody)
			require.Equal(t, baselineStatus, candidateStatus)
			require.Equal(t, "Error: Invalid Request. Missing parameters in transformation code block", decodeError(t, candidateBody))
			require.Equal(t, decodeError(t, baselineBody), decodeError(t, candidateBody))
		})

		t.Run("should return the verbatim missing-events error", func(t *testing.T) {
			payload := map[string]any{
				"codeRevision": map[string]any{
					"code":        "def transformEvent(event, metadata):\n    return event",
					"language":    "pythonfaas",
					"codeVersion": "1",
				},
				"input": []map[string]any{},
			}
			baselineStatus, baselineBody := env.callBaseline(t, env.client.TestRun, payload)
			candidateStatus, candidateBody := env.callCandidate(t, env.client.TestRun, payload)

			require.Equal(t, http.StatusBadRequest, candidateStatus, "body: %s", candidateBody)
			require.Equal(t, baselineStatus, candidateStatus)
			require.Equal(t, "Error: Invalid request. Missing events", decodeError(t, candidateBody))
			require.Equal(t, decodeError(t, baselineBody), decodeError(t, candidateBody))
		})
	})

	t.Run("test-library endpoint", func(t *testing.T) {
		t.Run("should return the import map for valid library code", func(t *testing.T) {
			payload := map[string]any{
				"code":     "import json\nimport datetime\ndef double(x):\n    return x * 2",
				"language": "pythonfaas",
			}
			baselineStatus, baselineBody := env.callBaseline(t, env.client.TestLibrary, payload)
			candidateStatus, candidateBody := env.callCandidate(t, env.client.TestLibrary, payload)

			require.Equal(t, http.StatusOK, candidateStatus, "body: %s", candidateBody)
			require.Equal(t, baselineStatus, candidateStatus, "old body: %s", baselineBody)
			require.Equal(t, map[string]any{"json": []any{}, "datetime": []any{}}, decodeImportMap(t, candidateBody))
			require.Equal(t, decodeImportMap(t, baselineBody), decodeImportMap(t, candidateBody))
		})

		t.Run("should reject a non-whitelisted import with the runtime's message", func(t *testing.T) {
			payload := map[string]any{
				"code":     "import os\ndef double(x):\n    return x * 2",
				"language": "pythonfaas",
			}
			baselineStatus, baselineBody := env.callBaseline(t, env.client.TestLibrary, payload)
			candidateStatus, candidateBody := env.callCandidate(t, env.client.TestLibrary, payload)

			require.Equal(t, http.StatusBadRequest, candidateStatus, "body: %s", candidateBody)
			require.Equal(t, baselineStatus, candidateStatus, "old body: %s", baselineBody)
			// The wording deliberately differs: fn-ast says "Unpermitted
			// import(s). ...", pyt surfaces the runtime's message so static
			// validation and runtime can't drift.
			require.NotEmpty(t, decodeError(t, baselineBody))
			require.Contains(t, decodeError(t, candidateBody), "Import of 'os' is not allowed.")
		})

		t.Run("should key the import map by the module path as written", func(t *testing.T) {
			payload := map[string]any{
				"code":     "import urllib.parse\nfrom dateutil.parser import parse",
				"language": "pythonfaas",
			}
			baselineStatus, baselineBody := env.callBaseline(t, env.client.TestLibrary, payload)
			candidateStatus, candidateBody := env.callCandidate(t, env.client.TestLibrary, payload)

			require.Equal(t, http.StatusOK, candidateStatus, "body: %s", candidateBody)
			require.Equal(t, baselineStatus, candidateStatus, "old body: %s", baselineBody)
			require.Equal(t, map[string]any{"urllib.parse": []any{}, "dateutil.parser": []any{}}, decodeImportMap(t, candidateBody))
			require.Equal(t, decodeImportMap(t, baselineBody), decodeImportMap(t, candidateBody))
		})

		// Security property, not a parity check: pyt's runtime blocks
		// urllib.request (raw HTTP would bypass the requests wrappers) and its
		// validator agrees with its runtime. openfaas's fn-ast accepted it (only
		// the top-level "urllib" was whitelist-checked there), and this case
		// existed to pin that tightening. Both versions must now reject it.
		t.Run("should reject urllib.request", func(t *testing.T) {
			payload := map[string]any{
				"code":     "import urllib.request",
				"language": "pythonfaas",
			}
			baselineStatus, baselineBody := env.callBaseline(t, env.client.TestLibrary, payload)
			candidateStatus, candidateBody := env.callCandidate(t, env.client.TestLibrary, payload)

			require.Equal(t, http.StatusBadRequest, candidateStatus, "candidate must reject, body: %s", candidateBody)
			require.Contains(t, decodeError(t, candidateBody), "Import of 'urllib.request' is not allowed.")

			require.Equal(t, baselineStatus, candidateStatus, "baseline body: %s", baselineBody)
			require.Equal(t, decodeError(t, baselineBody), decodeError(t, candidateBody))
		})

		// Both reject relative imports; the wording deliberately differs —
		// fn-ast surfaces an incidental crash trace ("'NoneType' object has no
		// attribute 'split'"), pyt a clean message.
		t.Run("should reject relative imports", func(t *testing.T) {
			payload := map[string]any{
				"code":     "from . import helper",
				"language": "pythonfaas",
			}
			baselineStatus, baselineBody := env.callBaseline(t, env.client.TestLibrary, payload)
			candidateStatus, candidateBody := env.callCandidate(t, env.client.TestLibrary, payload)

			require.Equal(t, http.StatusBadRequest, candidateStatus, "body: %s", candidateBody)
			require.Equal(t, baselineStatus, candidateStatus, "old body: %s", baselineBody)
			require.NotEmpty(t, decodeError(t, baselineBody))
			require.Equal(t, "Relative imports are not allowed.", decodeError(t, candidateBody))
		})

		t.Run("should return HTTP 400 for a syntax error", func(t *testing.T) {
			payload := map[string]any{
				"code":     "def double(x)\n    return x * 2",
				"language": "pythonfaas",
			}
			baselineStatus, baselineBody := env.callBaseline(t, env.client.TestLibrary, payload)
			candidateStatus, candidateBody := env.callCandidate(t, env.client.TestLibrary, payload)

			require.Equal(t, http.StatusBadRequest, candidateStatus, "body: %s", candidateBody)
			require.Equal(t, baselineStatus, candidateStatus, "old body: %s", baselineBody)
			// Wording deliberately differs (fn-ast BadCodeError vs pyt's runtime
			// compiler); both must carry Python's syntax diagnosis.
			require.Contains(t, decodeError(t, baselineBody), "expected ':'")
			require.Contains(t, decodeError(t, candidateBody), "expected ':'")
		})

		t.Run("should return the verbatim missing-code error", func(t *testing.T) {
			payload := map[string]any{"language": "pythonfaas"}
			baselineStatus, baselineBody := env.callBaseline(t, env.client.TestLibrary, payload)
			candidateStatus, candidateBody := env.callCandidate(t, env.client.TestLibrary, payload)

			require.Equal(t, http.StatusBadRequest, candidateStatus, "body: %s", candidateBody)
			require.Equal(t, baselineStatus, candidateStatus)
			require.Equal(t, "Invalid request. Missing code", decodeError(t, candidateBody))
			require.Equal(t, decodeError(t, baselineBody), decodeError(t, candidateBody))
		})
	})

	t.Run("extract-libs endpoint", func(t *testing.T) {
		t.Run("should extract non-whitelisted imports when validation is off", func(t *testing.T) {
			payload := map[string]any{
				"code":            "import os\nimport json",
				"language":        "pythonfaas",
				"validateImports": false,
			}
			baselineStatus, baselineBody := env.callBaseline(t, env.client.ExtractLibs, payload)
			candidateStatus, candidateBody := env.callCandidate(t, env.client.ExtractLibs, payload)

			require.Equal(t, http.StatusOK, candidateStatus, "body: %s", candidateBody)
			require.Equal(t, baselineStatus, candidateStatus, "old body: %s", baselineBody)
			require.Equal(t, map[string]any{"os": []any{}, "json": []any{}}, decodeImportMap(t, candidateBody))
			require.Equal(t, decodeImportMap(t, baselineBody), decodeImportMap(t, candidateBody))
		})

		t.Run("should allow additional libraries under validation", func(t *testing.T) {
			payload := map[string]any{
				"code":                "import mylib\nimport json",
				"language":            "pythonfaas",
				"validateImports":     true,
				"additionalLibraries": []string{"mylib"},
			}
			baselineStatus, baselineBody := env.callBaseline(t, env.client.ExtractLibs, payload)
			candidateStatus, candidateBody := env.callCandidate(t, env.client.ExtractLibs, payload)

			require.Equal(t, http.StatusOK, candidateStatus, "body: %s", candidateBody)
			require.Equal(t, baselineStatus, candidateStatus, "old body: %s", baselineBody)
			require.Equal(t, map[string]any{"mylib": []any{}, "json": []any{}}, decodeImportMap(t, candidateBody))
			require.Equal(t, decodeImportMap(t, baselineBody), decodeImportMap(t, candidateBody))
		})

		t.Run("should extract dotted imports with their full path when validation is off", func(t *testing.T) {
			payload := map[string]any{
				"code":            "import os.path",
				"language":        "pythonfaas",
				"validateImports": false,
			}
			baselineStatus, baselineBody := env.callBaseline(t, env.client.ExtractLibs, payload)
			candidateStatus, candidateBody := env.callCandidate(t, env.client.ExtractLibs, payload)

			require.Equal(t, http.StatusOK, candidateStatus, "body: %s", candidateBody)
			require.Equal(t, baselineStatus, candidateStatus, "old body: %s", baselineBody)
			require.Equal(t, map[string]any{"os.path": []any{}}, decodeImportMap(t, candidateBody))
			require.Equal(t, decodeImportMap(t, baselineBody), decodeImportMap(t, candidateBody))
		})

		// Both reject relative imports (regardless of validateImports); the
		// wording deliberately differs — fn-ast surfaces an incidental crash
		// trace ("'NoneType' object has no attribute 'split'"), pyt a clean
		// message.
		t.Run("should reject relative imports", func(t *testing.T) {
			payload := map[string]any{
				"code":            "from . import helper",
				"language":        "pythonfaas",
				"validateImports": false,
			}
			baselineStatus, baselineBody := env.callBaseline(t, env.client.ExtractLibs, payload)
			candidateStatus, candidateBody := env.callCandidate(t, env.client.ExtractLibs, payload)

			require.Equal(t, http.StatusBadRequest, candidateStatus, "body: %s", candidateBody)
			require.Equal(t, baselineStatus, candidateStatus, "old body: %s", baselineBody)
			require.NotEmpty(t, decodeError(t, baselineBody))
			require.Equal(t, "Relative imports are not allowed.", decodeError(t, candidateBody))
		})

		t.Run("should reject a non-whitelisted import when validation is on", func(t *testing.T) {
			payload := map[string]any{
				"code":            "import os",
				"language":        "pythonfaas",
				"validateImports": true,
			}
			baselineStatus, baselineBody := env.callBaseline(t, env.client.ExtractLibs, payload)
			candidateStatus, candidateBody := env.callCandidate(t, env.client.ExtractLibs, payload)

			require.Equal(t, http.StatusBadRequest, candidateStatus, "body: %s", candidateBody)
			require.Equal(t, baselineStatus, candidateStatus, "old body: %s", baselineBody)
			// Deliberate wording difference — see the test-library subtest above.
			require.NotEmpty(t, decodeError(t, baselineBody))
			require.Contains(t, decodeError(t, candidateBody), "Import of 'os' is not allowed.")
		})

		t.Run("should return the verbatim missing-code error", func(t *testing.T) {
			payload := map[string]any{
				"language":        "pythonfaas",
				"validateImports": true,
			}
			baselineStatus, baselineBody := env.callBaseline(t, env.client.ExtractLibs, payload)
			candidateStatus, candidateBody := env.callCandidate(t, env.client.ExtractLibs, payload)

			require.Equal(t, http.StatusBadRequest, candidateStatus, "body: %s", candidateBody)
			require.Equal(t, baselineStatus, candidateStatus)
			require.Equal(t, "Invalid request. Code is missing", decodeError(t, candidateBody))
			require.Equal(t, decodeError(t, baselineBody), decodeError(t, candidateBody))
		})
	})
}

const (
	// workspaceID is the workspace the client calls are made as.
	workspaceID = "ws-test-endpoints"
	// libVersionID is the only library version the mock config backend serves.
	libVersionID = "lib-mathhelper-v1"
)

// testEndpointsEnv is everything TestPyTransformerTestEndpoints' subtests need:
// the two rudder-pytransformer versions under comparison, both reached through
// the production client.
type testEndpointsEnv struct {
	baselineURL  string
	candidateURL string
	client       *usertransformer.Client
}

// newTestEndpointsEnv brings up both rudder-pytransformer versions against a
// shared mock config backend, with the production client pointed at them.
func newTestEndpointsEnv(t *testing.T) *testEndpointsEnv {
	t.Helper()

	libraryCode := "def double(x):\n    return x * 2\n"

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	// Pool.Retry lazily initializes MaxWait on first use, which is a data race
	// under the concurrent container startups below — set it up front.
	pool.MaxWait = time.Minute

	// The inline test endpoints never fetch transformation code (it arrives in
	// the request body). Libraries are fetched by pyt (fetch_library:
	// importName/code). The extra name/handleName fields are harmless and keep
	// the body shape identical to what the real config backend returns.
	configBackendHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/transformationLibrary/getByVersionId" {
			t.Logf("ConfigBackend: unexpected path %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		versionID := r.URL.Query().Get("versionId")
		if versionID != libVersionID {
			t.Logf("ConfigBackend: unknown library versionId %q", versionID)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = jsonrs.NewEncoder(w).Encode(map[string]any{
			"versionId":  versionID,
			"name":       "mathhelper",
			"handleName": "mathhelper",
			"importName": "mathhelper",
			"code":       libraryCode,
			"language":   "pythonfaas",
		})
	})
	// Listen on all interfaces on Linux so the server stays reachable from a
	// container in its own network namespace (host.docker.internal resolves to
	// the docker bridge IP there, which httptest's default 127.0.0.1 binding
	// does not answer). macOS Docker Desktop forwards host.docker.internal to
	// the host loopback, so the default binding is fine there.
	configBackend := httptest.NewUnstartedServer(configBackendHandler)
	if runtime.GOOS != "darwin" {
		ln, lerr := net.Listen("tcp", "0.0.0.0:0") //nolint:gosec // deliberate: must be reachable from the docker bridge
		require.NoError(t, lerr)
		_ = configBackend.Listener.Close()
		configBackend.Listener = ln
	}
	configBackend.Start()
	t.Cleanup(configBackend.Close)
	// Host-networked containers (rudder-transformer, pytransformer) and the test
	// process reach the backend on the loopback; a wildcard listener reports
	// 0.0.0.0, so pin the host explicitly. toContainerURL later rewrites this to
	// host.docker.internal for the containers that need it.
	_, configBackendPort, err := net.SplitHostPort(configBackend.Listener.Addr().String())
	require.NoError(t, err)
	configBackendURL := "http://127.0.0.1:" + configBackendPort

	var (
		wg                        sync.WaitGroup
		baselineURL, candidateURL string
	)
	wg.Go(func() {
		baselineURL = startBaselinePytransformer(t, pool, configBackendURL)
	})
	wg.Go(func() {
		candidateURL = startRudderPytransformer(t, pool, configBackendURL)
	})
	wg.Wait()

	// This mirrors production: cpservice.Forward resolves the target base URL
	// (an ephemeral deployment, the prod pyt, or the static AST deployment) and
	// passes it to the client per call — the client itself needs no pyt config.
	// One client serves both versions; only the base URL differs per call.
	return &testEndpointsEnv{
		baselineURL:  baselineURL,
		candidateURL: candidateURL,
		client:       usertransformer.New(config.New(), logger.NOP, stats.NOP),
	}
}

// testEndpointMethod is the shape of the client's four test-flow entry points.
type testEndpointMethod func(ctx context.Context, baseURL, workspaceID string, payload []byte) (int, []byte, error)

// callBaseline sends payload through the given client method against the baseline
// pytransformer container.
func (env *testEndpointsEnv) callBaseline(t *testing.T, method testEndpointMethod, payload map[string]any) (int, []byte) {
	t.Helper()
	return env.call(t, method, env.baselineURL, payload)
}

// callCandidate sends payload through the given client method against the candidate
// pytransformer container.
func (env *testEndpointsEnv) callCandidate(t *testing.T, method testEndpointMethod, payload map[string]any) (int, []byte) {
	t.Helper()
	return env.call(t, method, env.candidateURL, payload)
}

// call marshals payload and sends it through the given client method against
// one container's base URL (the role cpservice.Forward plays in production),
// returning the pyt HTTP status code and response body unchanged.
func (env *testEndpointsEnv) call(
	t *testing.T,
	method testEndpointMethod,
	baseURL string,
	payload map[string]any,
) (int, []byte) {
	t.Helper()
	body, err := jsonrs.Marshal(payload)
	require.NoError(t, err)
	statusCode, respBody, err := method(context.Background(), baseURL, workspaceID, body)
	require.NoError(t, err)
	return statusCode, respBody
}

// testFlowResponse is the success envelope shared by /test and /testRun.
type testFlowResponse struct {
	TransformedEvents []map[string]any `json:"transformedEvents"`
	Logs              []string         `json:"logs"`
}

// testFlowError is the whole-execution failure body shared by all four endpoints.
type testFlowError struct {
	Error string `json:"error"`
}

func decodeFlow(t *testing.T, body []byte) testFlowResponse {
	t.Helper()
	var resp testFlowResponse
	require.NoError(t, jsonrs.Unmarshal(body, &resp), "body: %s", body)
	return resp
}

func decodeError(t *testing.T, body []byte) string {
	t.Helper()
	var resp testFlowError
	require.NoError(t, jsonrs.Unmarshal(body, &resp), "body: %s", body)
	return resp.Error
}

func decodeImportMap(t *testing.T, body []byte) map[string]any {
	t.Helper()
	var resp map[string]any
	require.NoError(t, jsonrs.Unmarshal(body, &resp), "body: %s", body)
	return resp
}

// compareTestFlowBodies compares baseline and candidate /test and /testRun bodies
// element-by-element — byte-identical on both sides. /test elements are bare
// transformed events on success and {error} on per-event failure; /testRun
// elements are {transformedEvent|error, metadata}.
func compareTestFlowBodies(t *testing.T, baselineBody, candidateBody []byte) {
	t.Helper()
	baselineResp, candidateResp := decodeFlow(t, baselineBody), decodeFlow(t, candidateBody)
	require.Len(t, candidateResp.TransformedEvents, len(baselineResp.TransformedEvents),
		"old and candidate must return the same number of transformed events\nold: %s\nnew: %s", baselineBody, candidateBody)
	for i, oldEl := range baselineResp.TransformedEvents {
		require.Equal(t, oldEl, candidateResp.TransformedEvents[i], "transformed event %d", i)
	}
	require.Equal(t, baselineResp.Logs, candidateResp.Logs, "logs must match")
}
