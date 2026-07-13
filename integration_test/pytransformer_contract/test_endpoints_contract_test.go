package pytransformer_contract

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	dockertesthelper "github.com/rudderlabs/rudder-go-kit/testhelper/docker"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/registry"

	"github.com/rudderlabs/rudder-server/processor/usertransformer"
)

// TestPyTransformerTestEndpoints pins the wire contract of the four
// control-plane endpoints introduced for the Python transformation-test flow,
// comparing the new architecture against the old one:
//
//	pyt POST /test          ~ rudder-transformer POST /transformation/test
//	pyt POST /testRun       ~ rudder-transformer POST /transformation/testRun
//	pyt POST /test-library  ~ rudder-transformer POST /transformationLibrary/test
//	pyt POST /extract-libs  ~ rudder-transformer POST /extractLibs
//
// Old architecture: rudder-transformer (TRANSFORMER_TEST_MODE=true) deploys a
// per-request OpenFaaS function whose fprocess carries the inline code, invokes
// it, and deletes it; the AST routes invoke the long-lived fn-ast function. The
// dynamic mock gateway backs each deployment with a real openfaas-flask-base
// container so the inline code actually runs.
//
// New architecture: requests go through usertransformer.Client.Test/TestRun/
// TestLibrary/ExtractLibs — the same methods cpservice.Forward uses in
// production — so the test covers the exact client → pyt path.
//
// Responses are compared byte-for-byte except where rudder-pytransformer
// deliberately (and documentedly, see its README) diverges:
//   - AST import-violation messages: pyt surfaces the runtime's wording
//     ("Import of 'os' is not allowed. ...") instead of fn-ast's
//     ("Unpermitted import(s). ...") — the runtime is the source of truth.
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
			oldStatus, oldBody := env.callOld(t, "/transformation/test", payload)
			newStatus, newBody := env.callNew(t, env.client.Test, payload)

			require.Equal(t, http.StatusOK, newStatus, "body: %s", newBody)
			require.Equal(t, oldStatus, newStatus, "old status %d, old body: %s", oldStatus, oldBody)
			resp := decodeFlow(t, newBody)
			require.Len(t, resp.TransformedEvents, 2)
			for _, ev := range resp.TransformedEvents {
				require.Equal(t, "bar", ev["foo"])
			}
			compareTestFlowBodies(t, oldBody, newBody)
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
			oldStatus, oldBody := env.callOld(t, "/transformation/test", payload)
			newStatus, newBody := env.callNew(t, env.client.Test, payload)

			require.Equal(t, http.StatusOK, newStatus, "body: %s", newBody)
			require.Equal(t, oldStatus, newStatus, "old body: %s", oldBody)
			resp := decodeFlow(t, newBody)
			require.Len(t, resp.TransformedEvents, 1)
			require.EqualValues(t, 42, resp.TransformedEvents[0]["doubled"])
			compareTestFlowBodies(t, oldBody, newBody)
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
			oldStatus, oldBody := env.callOld(t, "/transformation/test", payload)
			newStatus, newBody := env.callNew(t, env.client.Test, payload)

			require.Equal(t, http.StatusOK, newStatus, "body: %s", newBody)
			require.Equal(t, oldStatus, newStatus, "old body: %s", oldBody)
			resp := decodeFlow(t, newBody)
			require.Len(t, resp.TransformedEvents, 1)
			require.Equal(t, "secret123", resp.TransformedEvents[0]["secret"])
			compareTestFlowBodies(t, oldBody, newBody)
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
			oldStatus, oldBody := env.callOld(t, "/transformation/test", payload)
			newStatus, newBody := env.callNew(t, env.client.Test, payload)

			require.Equal(t, http.StatusOK, newStatus, "body: %s", newBody)
			require.Equal(t, oldStatus, newStatus, "old body: %s", oldBody)

			resp := decodeFlow(t, newBody)
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

			compareTestFlowBodies(t, oldBody, newBody)
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
			oldStatus, oldBody := env.callOld(t, "/transformation/test", payload)
			newStatus, newBody := env.callNew(t, env.client.Test, payload)

			require.Equal(t, http.StatusOK, newStatus, "body: %s", newBody)
			require.Equal(t, oldStatus, newStatus, "old body: %s", oldBody)
			resp := decodeFlow(t, newBody)
			require.Len(t, resp.TransformedEvents, 2)
			require.EqualValues(t, 2, resp.TransformedEvents[0]["doubled"])
			require.EqualValues(t, 4, resp.TransformedEvents[1]["doubled"])
			compareTestFlowBodies(t, oldBody, newBody)
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
			oldStatus, oldBody := env.callOld(t, "/transformation/test", payload)
			newStatus, newBody := env.callNew(t, env.client.Test, payload)

			require.Equal(t, http.StatusOK, newStatus, "body: %s", newBody)
			require.Equal(t, oldStatus, newStatus, "old body: %s", oldBody)
			resp := decodeFlow(t, newBody)
			require.Len(t, resp.TransformedEvents, 2)
			errored := resp.TransformedEvents[1]
			require.Contains(t, errored["error"], "no meta id")
			require.NotContains(t, errored, "metadata",
				"errored /test elements are {error} only, matching rudder-transformer")
			compareTestFlowBodies(t, oldBody, newBody)
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
			oldStatus, oldBody := env.callOld(t, "/transformation/test", payload)
			newStatus, newBody := env.callNew(t, env.client.Test, payload)

			require.Equal(t, http.StatusBadRequest, newStatus, "body: %s", newBody)
			require.Equal(t, oldStatus, newStatus, "old body: %s", oldBody)
			// The wording deliberately differs: the old arch surfaces fn-ast's
			// BadCodeError (its import extraction parses the code before the
			// function is even deployed), pyt surfaces its runtime compiler's
			// message. Both must carry Python's syntax diagnosis.
			oldErr := decodeError(t, oldBody)
			newErr := decodeError(t, newBody)
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
			oldStatus, oldBody := env.callOld(t, "/transformation/test", payload)
			newStatus, newBody := env.callNew(t, env.client.Test, payload)

			require.Equal(t, http.StatusBadRequest, newStatus, "body: %s", newBody)
			require.Equal(t, oldStatus, newStatus)
			require.Equal(t, "Error: Invalid Request. Missing parameters in transformation code block", decodeError(t, newBody))
			require.Equal(t, decodeError(t, oldBody), decodeError(t, newBody))
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
			oldStatus, oldBody := env.callOld(t, "/transformation/test", payload)
			newStatus, newBody := env.callNew(t, env.client.Test, payload)

			require.Equal(t, http.StatusBadRequest, newStatus, "body: %s", newBody)
			require.Equal(t, oldStatus, newStatus)
			require.Equal(t, "Error: Invalid request. Missing events", decodeError(t, newBody))
			require.Equal(t, decodeError(t, oldBody), decodeError(t, newBody))
		})

		// Known security tightening: pyt compiles with RestrictedPython 8, whose
		// transformer blocks generator/frame introspection attributes
		// (INSPECT_ATTRIBUTES — the CVE-2023-37271 sandbox-escape fix: gi_frame,
		// f_back, cr_frame, ...). The old architecture runs RestrictedPython 6.1,
		// which predates the fix and only guards underscore-prefixed names, so
		// the same code compiles AND executes there.
		t.Run("should reject frame-introspection code the old architecture compiles and runs", func(t *testing.T) {
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
			oldStatus, oldBody := env.callOld(t, "/transformation/test", payload)
			newStatus, newBody := env.callNew(t, env.client.Test, payload)

			require.Equal(t, http.StatusOK, oldStatus, "old architecture accepts, body: %s", oldBody)
			oldResp := decodeFlow(t, oldBody)
			require.Len(t, oldResp.TransformedEvents, 1)
			require.Equal(t, false, oldResp.TransformedEvents[0]["has_frame"],
				"old architecture executed the gi_frame access")

			require.Equal(t, http.StatusBadRequest, newStatus, "pyt rejects at compile, body: %s", newBody)
			require.Contains(t, decodeError(t, newBody), "gi_frame")
			require.Contains(t, decodeError(t, newBody), "restricted name")
		})

		// pyt-only: in the old architecture an unknown library version crashes
		// the deployed flask container at startup (its --lvids fetch fails), so
		// the request degenerates into a readiness-timeout/retry loop rather
		// than a comparable 400.
		t.Run("should return HTTP 400 when a library cannot be fetched", func(t *testing.T) {
			newStatus, newBody := env.callNew(t, env.client.Test, map[string]any{
				"trRevCode": map[string]any{
					"code":        "def transformEvent(event, metadata):\n    return event",
					"codeVersion": "1",
					"language":    "pythonfaas",
				},
				"events": []map[string]any{
					{"message": map[string]any{"messageId": "m1"}, "metadata": map[string]any{"messageId": "m1"}},
				},
				"libraryVersionIDs": []string{"unknown-library-version"},
			})
			require.Equal(t, http.StatusBadRequest, newStatus, "body: %s", newBody)
			require.NotEmpty(t, decodeError(t, newBody))
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
			oldStatus, oldBody := env.callOld(t, "/transformation/testRun", payload)
			newStatus, newBody := env.callNew(t, env.client.TestRun, payload)

			require.Equal(t, http.StatusOK, newStatus, "body: %s", newBody)
			require.Equal(t, oldStatus, newStatus, "old body: %s", oldBody)
			resp := decodeFlow(t, newBody)
			require.Len(t, resp.TransformedEvents, 1)
			el := resp.TransformedEvents[0]
			transformed, ok := el["transformedEvent"].(map[string]any)
			require.True(t, ok, "element carries the transformed event under the transformedEvent key, matching rudder-transformer")
			require.Equal(t, "bar", transformed["foo"])
			require.NotContains(t, el, "statusCode", "no statusCode, matching rudder-transformer")
			compareTestFlowBodies(t, oldBody, newBody)
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
			oldStatus, oldBody := env.callOld(t, "/transformation/testRun", payload)
			newStatus, newBody := env.callNew(t, env.client.TestRun, payload)

			require.Equal(t, http.StatusOK, newStatus, "body: %s", newBody)
			require.Equal(t, oldStatus, newStatus, "old body: %s", oldBody)
			resp := decodeFlow(t, newBody)
			require.Len(t, resp.TransformedEvents, 1)
			meta, ok := resp.TransformedEvents[0]["metadata"].(map[string]any)
			require.True(t, ok)
			require.EqualValues(t, 123, meta["rudderId"], "int rudderId is echoed back, not rejected")
			require.Contains(t, meta, "customKey", "unknown metadata keys are echoed back, not dropped")
			compareTestFlowBodies(t, oldBody, newBody)
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
			oldStatus, oldBody := env.callOld(t, "/transformation/testRun", payload)
			newStatus, newBody := env.callNew(t, env.client.TestRun, payload)

			require.Equal(t, http.StatusOK, newStatus, "body: %s", newBody)
			require.Equal(t, oldStatus, newStatus, "old body: %s", oldBody)
			resp := decodeFlow(t, newBody)
			require.Len(t, resp.TransformedEvents, 1)
			transformed, ok := resp.TransformedEvents[0]["transformedEvent"].(map[string]any)
			require.True(t, ok)
			require.EqualValues(t, 42, transformed["doubled"])
			require.Equal(t, "secret123", transformed["secret"])
			compareTestFlowBodies(t, oldBody, newBody)
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
			oldStatus, oldBody := env.callOld(t, "/transformation/testRun", payload)
			newStatus, newBody := env.callNew(t, env.client.TestRun, payload)

			require.Equal(t, http.StatusOK, newStatus, "body: %s", newBody)
			require.Equal(t, oldStatus, newStatus, "old body: %s", oldBody)
			resp := decodeFlow(t, newBody)
			require.Len(t, resp.TransformedEvents, 1)
			el := resp.TransformedEvents[0]
			require.Contains(t, el["error"], "boom")
			meta, ok := el["metadata"].(map[string]any)
			require.True(t, ok, "errored /testRun elements keep the input event's metadata")
			require.Equal(t, "m1", meta["messageId"])
			require.NotContains(t, el, "statusCode", "no statusCode, matching rudder-transformer")
			compareTestFlowBodies(t, oldBody, newBody)
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
			oldStatus, oldBody := env.callOld(t, "/transformation/testRun", payload)
			newStatus, newBody := env.callNew(t, env.client.TestRun, payload)

			require.Equal(t, http.StatusOK, newStatus, "body: %s", newBody)
			require.Equal(t, oldStatus, newStatus, "old body: %s", oldBody)
			resp := decodeFlow(t, newBody)
			require.Len(t, resp.TransformedEvents, 2)
			for i, el := range resp.TransformedEvents {
				transformed, ok := el["transformedEvent"].(map[string]any)
				require.True(t, ok)
				require.EqualValues(t, (i+1)*2, transformed["doubled"])
				meta, ok := el["metadata"].(map[string]any)
				require.True(t, ok)
				require.Equal(t, "s2", meta["sourceId"], "last event's metadata wins for element %d", i)
			}
			compareTestFlowBodies(t, oldBody, newBody)
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
			oldStatus, oldBody := env.callOld(t, "/transformation/testRun", payload)
			newStatus, newBody := env.callNew(t, env.client.TestRun, payload)

			require.Equal(t, http.StatusOK, newStatus, "body: %s", newBody)
			require.Equal(t, oldStatus, newStatus, "old body: %s", oldBody)
			resp := decodeFlow(t, newBody)
			require.Len(t, resp.TransformedEvents, 1)
			require.Equal(t, map[string]any{"sourceId": "s1"}, resp.TransformedEvents[0]["metadata"],
				"metadata is echoed as-is, without injecting a messageId")
			compareTestFlowBodies(t, oldBody, newBody)
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
			oldStatus, oldBody := env.callOld(t, "/transformation/testRun", payload)
			newStatus, newBody := env.callNew(t, env.client.TestRun, payload)

			require.Equal(t, http.StatusBadRequest, newStatus, "body: %s", newBody)
			require.Equal(t, oldStatus, newStatus, "old body: %s", oldBody)
			// Wording deliberately differs (fn-ast BadCodeError vs pyt's runtime
			// compiler); both must carry Python's syntax diagnosis.
			oldErr := decodeError(t, oldBody)
			newErr := decodeError(t, newBody)
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
			oldStatus, oldBody := env.callOld(t, "/transformation/testRun", payload)
			newStatus, newBody := env.callNew(t, env.client.TestRun, payload)

			require.Equal(t, http.StatusBadRequest, newStatus, "body: %s", newBody)
			require.Equal(t, oldStatus, newStatus)
			require.Equal(t, "Error: Invalid Request. Missing parameters in transformation code block", decodeError(t, newBody))
			require.Equal(t, decodeError(t, oldBody), decodeError(t, newBody))
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
			oldStatus, oldBody := env.callOld(t, "/transformation/testRun", payload)
			newStatus, newBody := env.callNew(t, env.client.TestRun, payload)

			require.Equal(t, http.StatusBadRequest, newStatus, "body: %s", newBody)
			require.Equal(t, oldStatus, newStatus)
			require.Equal(t, "Error: Invalid request. Missing events", decodeError(t, newBody))
			require.Equal(t, decodeError(t, oldBody), decodeError(t, newBody))
		})
	})

	t.Run("test-library endpoint", func(t *testing.T) {
		t.Run("should return the import map for valid library code", func(t *testing.T) {
			payload := map[string]any{
				"code":     "import json\nimport datetime\ndef double(x):\n    return x * 2",
				"language": "pythonfaas",
			}
			oldStatus, oldBody := env.callOld(t, "/transformationLibrary/test", payload)
			newStatus, newBody := env.callNew(t, env.client.TestLibrary, payload)

			require.Equal(t, http.StatusOK, newStatus, "body: %s", newBody)
			require.Equal(t, oldStatus, newStatus, "old body: %s", oldBody)
			require.Equal(t, map[string]any{"json": []any{}, "datetime": []any{}}, decodeImportMap(t, newBody))
			require.Equal(t, decodeImportMap(t, oldBody), decodeImportMap(t, newBody))
		})

		t.Run("should reject a non-whitelisted import with the runtime's message", func(t *testing.T) {
			payload := map[string]any{
				"code":     "import os\ndef double(x):\n    return x * 2",
				"language": "pythonfaas",
			}
			oldStatus, oldBody := env.callOld(t, "/transformationLibrary/test", payload)
			newStatus, newBody := env.callNew(t, env.client.TestLibrary, payload)

			require.Equal(t, http.StatusBadRequest, newStatus, "body: %s", newBody)
			require.Equal(t, oldStatus, newStatus, "old body: %s", oldBody)
			// The wording deliberately differs: fn-ast says "Unpermitted
			// import(s). ...", pyt surfaces the runtime's message so static
			// validation and runtime can't drift.
			require.NotEmpty(t, decodeError(t, oldBody))
			require.Contains(t, decodeError(t, newBody), "Import of 'os' is not allowed.")
		})

		t.Run("should key the import map by the module path as written", func(t *testing.T) {
			payload := map[string]any{
				"code":     "import urllib.parse\nfrom dateutil.parser import parse",
				"language": "pythonfaas",
			}
			oldStatus, oldBody := env.callOld(t, "/transformationLibrary/test", payload)
			newStatus, newBody := env.callNew(t, env.client.TestLibrary, payload)

			require.Equal(t, http.StatusOK, newStatus, "body: %s", newBody)
			require.Equal(t, oldStatus, newStatus, "old body: %s", oldBody)
			require.Equal(t, map[string]any{"urllib.parse": []any{}, "dateutil.parser": []any{}}, decodeImportMap(t, newBody))
			require.Equal(t, decodeImportMap(t, oldBody), decodeImportMap(t, newBody))
		})

		// Known security tightening: pyt's runtime blocks urllib.request (raw
		// HTTP would bypass the requests wrappers), and its validator agrees
		// with its runtime. fn-ast accepted it (only the top-level "urllib" is
		// whitelisted-checked there).
		t.Run("should reject urllib.request that the old architecture accepts", func(t *testing.T) {
			payload := map[string]any{
				"code":     "import urllib.request",
				"language": "pythonfaas",
			}
			oldStatus, oldBody := env.callOld(t, "/transformationLibrary/test", payload)
			newStatus, newBody := env.callNew(t, env.client.TestLibrary, payload)

			require.Equal(t, http.StatusOK, oldStatus, "old architecture accepts, body: %s", oldBody)
			require.Equal(t, map[string]any{"urllib.request": []any{}}, decodeImportMap(t, oldBody))

			require.Equal(t, http.StatusBadRequest, newStatus, "pyt rejects, body: %s", newBody)
			require.Contains(t, decodeError(t, newBody), "Import of 'urllib.request' is not allowed.")
		})

		// Both reject relative imports; the wording deliberately differs —
		// fn-ast surfaces an incidental crash trace ("'NoneType' object has no
		// attribute 'split'"), pyt a clean message.
		t.Run("should reject relative imports like the old architecture", func(t *testing.T) {
			payload := map[string]any{
				"code":     "from . import helper",
				"language": "pythonfaas",
			}
			oldStatus, oldBody := env.callOld(t, "/transformationLibrary/test", payload)
			newStatus, newBody := env.callNew(t, env.client.TestLibrary, payload)

			require.Equal(t, http.StatusBadRequest, newStatus, "body: %s", newBody)
			require.Equal(t, oldStatus, newStatus, "old body: %s", oldBody)
			require.NotEmpty(t, decodeError(t, oldBody))
			require.Equal(t, "Relative imports are not allowed.", decodeError(t, newBody))
		})

		t.Run("should return HTTP 400 for a syntax error", func(t *testing.T) {
			payload := map[string]any{
				"code":     "def double(x)\n    return x * 2",
				"language": "pythonfaas",
			}
			oldStatus, oldBody := env.callOld(t, "/transformationLibrary/test", payload)
			newStatus, newBody := env.callNew(t, env.client.TestLibrary, payload)

			require.Equal(t, http.StatusBadRequest, newStatus, "body: %s", newBody)
			require.Equal(t, oldStatus, newStatus, "old body: %s", oldBody)
			// Wording deliberately differs (fn-ast BadCodeError vs pyt's runtime
			// compiler); both must carry Python's syntax diagnosis.
			require.Contains(t, decodeError(t, oldBody), "expected ':'")
			require.Contains(t, decodeError(t, newBody), "expected ':'")
		})

		t.Run("should return the verbatim missing-code error", func(t *testing.T) {
			payload := map[string]any{"language": "pythonfaas"}
			oldStatus, oldBody := env.callOld(t, "/transformationLibrary/test", payload)
			newStatus, newBody := env.callNew(t, env.client.TestLibrary, payload)

			require.Equal(t, http.StatusBadRequest, newStatus, "body: %s", newBody)
			require.Equal(t, oldStatus, newStatus)
			require.Equal(t, "Invalid request. Missing code", decodeError(t, newBody))
			require.Equal(t, decodeError(t, oldBody), decodeError(t, newBody))
		})
	})

	t.Run("extract-libs endpoint", func(t *testing.T) {
		t.Run("should extract non-whitelisted imports when validation is off", func(t *testing.T) {
			payload := map[string]any{
				"code":            "import os\nimport json",
				"language":        "pythonfaas",
				"validateImports": false,
			}
			oldStatus, oldBody := env.callOld(t, "/extractLibs", payload)
			newStatus, newBody := env.callNew(t, env.client.ExtractLibs, payload)

			require.Equal(t, http.StatusOK, newStatus, "body: %s", newBody)
			require.Equal(t, oldStatus, newStatus, "old body: %s", oldBody)
			require.Equal(t, map[string]any{"os": []any{}, "json": []any{}}, decodeImportMap(t, newBody))
			require.Equal(t, decodeImportMap(t, oldBody), decodeImportMap(t, newBody))
		})

		t.Run("should allow additional libraries under validation", func(t *testing.T) {
			payload := map[string]any{
				"code":                "import mylib\nimport json",
				"language":            "pythonfaas",
				"validateImports":     true,
				"additionalLibraries": []string{"mylib"},
			}
			oldStatus, oldBody := env.callOld(t, "/extractLibs", payload)
			newStatus, newBody := env.callNew(t, env.client.ExtractLibs, payload)

			require.Equal(t, http.StatusOK, newStatus, "body: %s", newBody)
			require.Equal(t, oldStatus, newStatus, "old body: %s", oldBody)
			require.Equal(t, map[string]any{"mylib": []any{}, "json": []any{}}, decodeImportMap(t, newBody))
			require.Equal(t, decodeImportMap(t, oldBody), decodeImportMap(t, newBody))
		})

		t.Run("should extract dotted imports with their full path when validation is off", func(t *testing.T) {
			payload := map[string]any{
				"code":            "import os.path",
				"language":        "pythonfaas",
				"validateImports": false,
			}
			oldStatus, oldBody := env.callOld(t, "/extractLibs", payload)
			newStatus, newBody := env.callNew(t, env.client.ExtractLibs, payload)

			require.Equal(t, http.StatusOK, newStatus, "body: %s", newBody)
			require.Equal(t, oldStatus, newStatus, "old body: %s", oldBody)
			require.Equal(t, map[string]any{"os.path": []any{}}, decodeImportMap(t, newBody))
			require.Equal(t, decodeImportMap(t, oldBody), decodeImportMap(t, newBody))
		})

		// Both reject relative imports (regardless of validateImports); the
		// wording deliberately differs — fn-ast surfaces an incidental crash
		// trace ("'NoneType' object has no attribute 'split'"), pyt a clean
		// message.
		t.Run("should reject relative imports like the old architecture", func(t *testing.T) {
			payload := map[string]any{
				"code":            "from . import helper",
				"language":        "pythonfaas",
				"validateImports": false,
			}
			oldStatus, oldBody := env.callOld(t, "/extractLibs", payload)
			newStatus, newBody := env.callNew(t, env.client.ExtractLibs, payload)

			require.Equal(t, http.StatusBadRequest, newStatus, "body: %s", newBody)
			require.Equal(t, oldStatus, newStatus, "old body: %s", oldBody)
			require.NotEmpty(t, decodeError(t, oldBody))
			require.Equal(t, "Relative imports are not allowed.", decodeError(t, newBody))
		})

		t.Run("should reject a non-whitelisted import when validation is on", func(t *testing.T) {
			payload := map[string]any{
				"code":            "import os",
				"language":        "pythonfaas",
				"validateImports": true,
			}
			oldStatus, oldBody := env.callOld(t, "/extractLibs", payload)
			newStatus, newBody := env.callNew(t, env.client.ExtractLibs, payload)

			require.Equal(t, http.StatusBadRequest, newStatus, "body: %s", newBody)
			require.Equal(t, oldStatus, newStatus, "old body: %s", oldBody)
			// Deliberate wording difference — see the test-library subtest above.
			require.NotEmpty(t, decodeError(t, oldBody))
			require.Contains(t, decodeError(t, newBody), "Import of 'os' is not allowed.")
		})

		t.Run("should return the verbatim missing-code error", func(t *testing.T) {
			payload := map[string]any{
				"language":        "pythonfaas",
				"validateImports": true,
			}
			oldStatus, oldBody := env.callOld(t, "/extractLibs", payload)
			newStatus, newBody := env.callNew(t, env.client.ExtractLibs, payload)

			require.Equal(t, http.StatusBadRequest, newStatus, "body: %s", newBody)
			require.Equal(t, oldStatus, newStatus)
			require.Equal(t, "Invalid request. Code is missing", decodeError(t, newBody))
			require.Equal(t, decodeError(t, oldBody), decodeError(t, newBody))
		})
	})
}

const (
	// workspaceID is the workspace the new-architecture client calls are made as.
	workspaceID = "ws-test-endpoints"
	// libVersionID is the only library version the mock config backend serves.
	libVersionID = "lib-mathhelper-v1"
)

// testEndpointsEnv is everything TestPyTransformerTestEndpoints' subtests need:
// the old architecture reachable over plain HTTP (transformerURL) and the new
// one through the production client.
type testEndpointsEnv struct {
	transformerURL   string
	pyTransformerURL string
	client           *usertransformer.Client
}

// newTestEndpointsEnv brings up both architectures against a shared mock config
// backend: rudder-transformer in test mode backed by the dynamic OpenFaaS
// gateway (plus the long-lived fn-ast function it routes AST requests to), and
// rudder-pytransformer with the production client pointed at it.
func newTestEndpointsEnv(t *testing.T) *testEndpointsEnv {
	t.Helper()

	libraryCode := "def double(x):\n    return x * 2\n"

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	// Pool.Retry lazily initializes MaxWait on first use, which is a data race
	// under the concurrent container startups below — set it up front.
	pool.MaxWait = time.Minute

	// The inline test endpoints never fetch transformation code (it arrives in
	// the request body). Libraries are fetched by rudder-transformer
	// (getLibraryCodeV1: name/handleName), openfaas-flask-base (--lvids at
	// startup: importName/code) and pyt (fetch_library: importName/code) — one
	// response body serves all three.
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
	// The flask containers run in isolated bridge namespaces and reach this
	// server through host.docker.internal. On Linux that resolves to the docker
	// bridge IP, so the server must listen on all interfaces — httptest's
	// default 127.0.0.1 binding is unreachable from inside the container. macOS
	// Docker Desktop forwards host.docker.internal to the host loopback, so the
	// default binding is fine there.
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

	gateway := newDynamicFaasGateway(t, pool)

	var (
		wg                               sync.WaitGroup
		transformerURL, pyTransformerURL string
	)
	wg.Go(func() {
		transformerURL = startRudderTransformer(t, pool, configBackendURL, gateway.server.URL,
			"TRANSFORMER_TEST_MODE=true")
	})
	wg.Go(func() {
		pyTransformerURL = startRudderPytransformer(t, pool, configBackendURL)
	})
	wg.Go(func() {
		// The AST routes invoke the long-lived fn-ast function (versionId
		// "ast" makes flask-base load its built-in AST parser instead of
		// fetching code).
		astURL, astContainer, err := startOpenFaasFlaskFprocess(t, pool, "python index.py --vid ast")
		require.NoError(t, err, "failed to start fn-ast container")
		waitForOpenFaasFlask(t, pool, astURL)
		gateway.register("fn-ast", astURL, astContainer)
	})
	wg.Wait()

	// This mirrors production: cpservice.Forward resolves the target base URL
	// (an ephemeral deployment, the prod pyt, or the static AST deployment) and
	// passes it to the client per call — the client itself needs no pyt config.
	// Here the single pyt container serves as the target for every call.
	return &testEndpointsEnv{
		transformerURL:   transformerURL,
		pyTransformerURL: pyTransformerURL,
		client:           usertransformer.New(config.New(), logger.NOP, stats.NOP),
	}
}

// callOld POSTs payload to a rudder-transformer test route and returns the
// HTTP status and body. These routes were only ever called by the control
// plane (never rudder-server), so a plain HTTP call is the faithful client.
func (env *testEndpointsEnv) callOld(t *testing.T, path string, payload map[string]any) (int, []byte) {
	t.Helper()
	body, err := jsonrs.Marshal(payload)
	require.NoError(t, err)
	resp, err := http.Post(env.transformerURL+path, "application/json", bytes.NewReader(body))
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	respBody, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, respBody
}

// callNew marshals payload and sends it through the given client method
// against the pyt container's base URL (the role cpservice.Forward plays in
// production), returning the pyt HTTP status code and response body unchanged.
func (env *testEndpointsEnv) callNew(
	t *testing.T,
	method func(ctx context.Context, baseURL, workspaceID string, payload []byte) (int, []byte, error),
	payload map[string]any,
) (int, []byte) {
	t.Helper()
	body, err := jsonrs.Marshal(payload)
	require.NoError(t, err)
	statusCode, respBody, err := method(context.Background(), env.pyTransformerURL, workspaceID, body)
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

// compareTestFlowBodies compares old- and new-arch /test and /testRun bodies
// element-by-element — byte-identical on both sides. /test elements are bare
// transformed events on success and {error} on per-event failure; /testRun
// elements are {transformedEvent|error, metadata}.
func compareTestFlowBodies(t *testing.T, oldBody, newBody []byte) {
	t.Helper()
	oldResp, newResp := decodeFlow(t, oldBody), decodeFlow(t, newBody)
	require.Len(t, newResp.TransformedEvents, len(oldResp.TransformedEvents),
		"old and new arch must return the same number of transformed events\nold: %s\nnew: %s", oldBody, newBody)
	for i, oldEl := range oldResp.TransformedEvents {
		require.Equal(t, oldEl, newResp.TransformedEvents[i], "transformed event %d", i)
	}
	require.Equal(t, oldResp.Logs, newResp.Logs, "logs must match")
}

// faasDeployRequest is the subset of the OpenFaaS deployment payload
// (buildOpenfaasFn in rudder-transformer) the mock gateway needs.
type faasDeployRequest struct {
	Service    string `json:"service"`
	Name       string `json:"name"`
	EnvProcess string `json:"envProcess"`
}

// dynamicFaasGateway mocks the OpenFaaS gateway for the control-plane test flow.
// Unlike newMockOpenFaaSGateway (fixed proxy target), it honours function
// deployments: POST /system/functions starts a real openfaas-flask-base
// container whose fprocess is the deployed envProcess — which is how test-mode
// inline code reaches the old architecture (`python index.py --code "..."`).
// Health checks and invocations are routed to the per-function container, and
// DELETE purges it (rudder-transformer deletes test functions after each run).
type dynamicFaasGateway struct {
	t          *testing.T
	pool       *dockertest.Pool
	server     *httptest.Server
	mu         sync.Mutex
	fnURLs     map[string]string
	containers map[string]*dockertest.Resource
}

func newDynamicFaasGateway(t *testing.T, pool *dockertest.Pool) *dynamicFaasGateway {
	t.Helper()
	g := &dynamicFaasGateway{
		t:          t,
		pool:       pool,
		fnURLs:     map[string]string{},
		containers: map[string]*dockertest.Resource{},
	}
	g.server = httptest.NewServer(http.HandlerFunc(g.handle))
	t.Cleanup(g.server.Close)
	return g
}

// register makes the gateway route /function/{name} traffic to the function's
// container at url.
func (g *dynamicFaasGateway) register(name, url string, container *dockertest.Resource) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.fnURLs[name] = url
	g.containers[name] = container
}

func (g *dynamicFaasGateway) lookup(name string) (string, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	url, ok := g.fnURLs[name]
	return url, ok
}

func (g *dynamicFaasGateway) lookupContainer(name string) *dockertest.Resource {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.containers[name]
}

func (g *dynamicFaasGateway) handle(w http.ResponseWriter, r *http.Request) {
	switch {
	// Deploy function: start a flask-base container with the deployed envProcess.
	case r.Method == http.MethodPost && r.URL.Path == "/system/functions":
		var req faasDeployRequest
		if err := jsonrs.NewDecoder(r.Body).Decode(&req); err != nil {
			g.t.Errorf("DynamicFaasGateway: decoding deploy request: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		name := req.Service
		if name == "" {
			name = req.Name
		}
		// rudder-transformer builds the fprocess with the config-backend URL as
		// the host sees it (127.0.0.1 on Linux). The flask container runs in its
		// own bridge namespace, so rewrite host-local addresses to
		// host.docker.internal so the function can reach the host. On macOS the
		// URL is already host.docker.internal, so this is a no-op there.
		envProcess := dockertesthelper.ToInternalDockerHost(req.EnvProcess)
		g.t.Logf("DynamicFaasGateway: deploying %q with fprocess %q", name, envProcess)
		url, container, err := startOpenFaasFlaskFprocess(g.t, g.pool, envProcess)
		if err != nil {
			g.t.Errorf("DynamicFaasGateway: starting flask container for %q: %v", name, err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		// Wait for the container to actually accept connections before returning
		// 200. On Linux host networking the mapped port is unreachable
		// (connection refused) until fwatchdog binds it — without this gate the
		// caller invokes prematurely and the deploy appears to have failed.
		if err := pollOpenFaasFlaskHealthy(g.pool, url); err != nil {
			g.t.Errorf("DynamicFaasGateway: %q did not become healthy: %v", name, err)
			dumpContainerLogs(g.t, g.pool, container, name)
			_ = g.pool.Purge(container)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		g.register(name, url, container)
		w.WriteHeader(http.StatusOK)

	// Update function
	case r.Method == http.MethodPut && r.URL.Path == "/system/functions":
		w.WriteHeader(http.StatusOK)

	// Delete function: purge its container.
	case r.Method == http.MethodDelete && r.URL.Path == "/system/functions":
		var req struct {
			FunctionName string `json:"functionName"`
		}
		if err := jsonrs.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		g.mu.Lock()
		container := g.containers[req.FunctionName]
		delete(g.containers, req.FunctionName)
		delete(g.fnURLs, req.FunctionName)
		g.mu.Unlock()
		if container != nil {
			if err := g.pool.Purge(container); err != nil {
				g.t.Logf("DynamicFaasGateway: purging %q: %v", req.FunctionName, err)
			}
		}
		w.WriteHeader(http.StatusOK)

	// List functions
	case r.Method == http.MethodGet && r.URL.Path == "/system/functions":
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("[]"))

	// Get function info
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/system/function/"):
		name := strings.TrimPrefix(r.URL.Path, "/system/function/")
		if _, ok := g.lookup(name); !ok {
			w.WriteHeader(http.StatusNotFound)
			_, _ = fmt.Fprintf(w, "error finding function %s", name)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = jsonrs.NewEncoder(w).Encode(map[string]any{"name": name, "replicas": 1})

	// Health check (GET) or invoke (POST) — proxied to the function's container.
	case strings.HasPrefix(r.URL.Path, "/function/"):
		name := strings.TrimPrefix(r.URL.Path, "/function/")
		target, ok := g.lookup(name)
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			_, _ = fmt.Fprintf(w, "error finding function %s", name)
			return
		}
		switch r.Method {
		case http.MethodGet:
			// fwatchdog health check; 503 until the container is up.
			req, err := http.NewRequest(http.MethodGet, target+"/", nil)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			req.Header.Set("X-REQUEST-TYPE", "HEALTH-CHECK")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			defer func() { _ = resp.Body.Close() }()
			w.WriteHeader(resp.StatusCode)
			_, _ = io.Copy(w, resp.Body)
		case http.MethodPost:
			body, err := io.ReadAll(r.Body)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			resp, err := http.Post(target+"/", "application/json", bytes.NewReader(body))
			if err != nil {
				g.t.Logf("DynamicFaasGateway: invoking %q: %v", name, err)
				w.WriteHeader(http.StatusBadGateway)
				return
			}
			defer func() { _ = resp.Body.Close() }()
			respBody, _ := io.ReadAll(resp.Body)
			if resp.StatusCode < 200 || resp.StatusCode >= 300 {
				g.t.Logf("DynamicFaasGateway: invoke %q returned %d: %s", name, resp.StatusCode, respBody)
				if c := g.lookupContainer(name); c != nil {
					dumpContainerLogs(g.t, g.pool, c, name)
				}
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(resp.StatusCode)
			_, _ = w.Write(respBody)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}

	default:
		g.t.Logf("DynamicFaasGateway: unhandled %s %s", r.Method, r.URL.Path)
		w.WriteHeader(http.StatusNotFound)
	}
}

// startOpenFaasFlaskFprocess starts an openfaas-flask-base container running
// the given fprocess verbatim (as rudder-transformer deploys it). It does not
// wait for readiness — callers gate on pollOpenFaasFlaskHealthy (the deploy
// handler) or waitForOpenFaasFlask (fn-ast startup) before use. Returns an
// error instead of failing the test because it is called from the gateway's
// HTTP handler goroutine.
//
// Each flask container runs in its own bridge-network namespace — never host
// networking, even on Linux — because of-watchdog binds a HARD-CODED Prometheus
// metrics port 8081 that is not configurable via env. Under host networking two
// flask containers (the long-lived fn-ast and a per-request fn-test) share the
// host namespace and collide on 8081: of-watchdog panics with "bind: address
// already in use", the container exits, and the invoke port never comes up
// ("connection refused"). A per-container namespace keeps each 8081 private.
func startOpenFaasFlaskFprocess(
	t *testing.T, pool *dockertest.Pool, fprocess string,
) (string, *dockertest.Resource, error) {
	t.Helper()
	const containerPort = "8080"
	cfg := newContainerConfig(t, containerPort, withBridgeNetworking())
	container, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "422074288268.dkr.ecr.us-east-1.amazonaws.com/rudderstack/openfaas-flask",
		// Pinned to match the production version
		Tag:  "1.13.2",
		Auth: registry.AuthConfiguration(),
		Env: []string{
			"fprocess=" + fprocess,
			"port=" + cfg.portStr(containerPort),
		},
		// The image does not EXPOSE any port, and Docker ignores port bindings
		// for unexposed ports.
		ExposedPorts: []string{containerPort + "/tcp"},
		ExtraHosts:   cfg.ExtraHosts,
		PortBindings: cfg.PortBindings,
	}, cfg.hostConfigFn)
	if err != nil {
		return "", nil, err
	}
	t.Cleanup(func() {
		// Best effort: functions deleted by rudder-transformer are already gone.
		_ = pool.Purge(container)
	})
	return cfg.url(container, containerPort), container, nil
}
