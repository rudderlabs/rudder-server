package processor

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/processor/types"
)

// TestCaptureErrorSingularEventMetadata verifies that the rETL capture_error
// opt-in flag survives the EventParams -> Metadata carrier step performed by
// singularEventMetadata, the same way SourceJobRunID/RecordID already do.
func TestCaptureErrorSingularEventMetadata(t *testing.T) {
	proc := &Handle{}
	source := &backendconfig.SourceT{
		WorkspaceID: "test",
		SourceDefinition: backendconfig.SourceDefinitionT{
			Name:     "test_def",
			Category: "eventStream",
			ID:       "testDefId",
		},
	}

	t.Run("true is carried into metadata", func(t *testing.T) {
		metadata := proc.singularEventMetadata(
			dummySingularEvent,
			dummyBatchEvent.UserID,
			dummyBatchEvent.PartitionID,
			dummyBatchEvent.JobID,
			time.Now(),
			source,
			types.EventParams{
				SourceJobRunId: "job_run_id",
				CaptureError:   true,
			},
		)
		require.True(t, metadata.CaptureError)
	})

	t.Run("false is carried into metadata as false", func(t *testing.T) {
		metadata := proc.singularEventMetadata(
			dummySingularEvent,
			dummyBatchEvent.UserID,
			dummyBatchEvent.PartitionID,
			dummyBatchEvent.JobID,
			time.Now(),
			source,
			types.EventParams{
				SourceJobRunId: "job_run_id",
				CaptureError:   false,
			},
		)
		require.False(t, metadata.CaptureError)
	})
}

// TestCaptureErrorEventParamsUnmarshal pins the observed behaviour of jsonrs
// (default backend: github.com/rudderlabs/sonnet, see
// rudder-go-kit/jsonrs/json.go DefaultLib) when unmarshalling the
// gateway-supplied capture_error param.
func TestCaptureErrorEventParamsUnmarshal(t *testing.T) {
	t.Run("bool true unmarshals into the field", func(t *testing.T) {
		var eventParams types.EventParams
		err := jsonrs.Unmarshal([]byte(`{"source_job_run_id":"jr","capture_error":true}`), &eventParams)
		require.NoError(t, err)
		require.True(t, eventParams.CaptureError)
	})

	t.Run("string value is a hard unmarshal error, not a silent coercion", func(t *testing.T) {
		// Observed by running this exact case against jsonrs.Unmarshal: the
		// sonnet-backed implementation rejects the type mismatch outright
		// (does not coerce "true" to true, and does not silently leave it
		// false while swallowing the error) - error message:
		//   sonnet: cannot unmarshal string into Go struct field
		//   EventParams.capture_error of type bool
		var eventParams types.EventParams
		err := jsonrs.Unmarshal([]byte(`{"capture_error":"true"}`), &eventParams)
		require.Error(t, err)
		require.Contains(t, err.Error(), "capture_error")
		require.False(t, eventParams.CaptureError)
	})
}

// TestCaptureErrorMarshalOmitempty is a load-bearing guard: router job
// Parameters (and the Metadata exchanged with the transformer) must stay
// byte-identical to today for every job that did not opt in, which requires
// `omitempty` to actually suppress the field when false.
func TestCaptureErrorMarshalOmitempty(t *testing.T) {
	t.Run("ParametersT omits capture_error when false", func(t *testing.T) {
		b, err := jsonrs.Marshal(ParametersT{})
		require.NoError(t, err)
		require.False(t, gjson.GetBytes(b, "capture_error").Exists(), "capture_error must be absent, got: %s", b)
	})

	t.Run("ParametersT emits capture_error as a JSON bool when true", func(t *testing.T) {
		b, err := jsonrs.Marshal(ParametersT{CaptureError: true})
		require.NoError(t, err)
		result := gjson.GetBytes(b, "capture_error")
		require.True(t, result.Exists())
		require.Equal(t, "true", result.Raw, "capture_error must serialize as a JSON bool, not a string")
	})

	t.Run("Metadata omits captureError when false", func(t *testing.T) {
		b, err := jsonrs.Marshal(types.Metadata{})
		require.NoError(t, err)
		require.False(t, gjson.GetBytes(b, "captureError").Exists(), "captureError must be absent, got: %s", b)
	})

	t.Run("Metadata emits captureError as a JSON bool when true", func(t *testing.T) {
		b, err := jsonrs.Marshal(types.Metadata{CaptureError: true})
		require.NoError(t, err)
		result := gjson.GetBytes(b, "captureError")
		require.True(t, result.Exists())
		require.Equal(t, "true", result.Raw, "captureError must serialize as a JSON bool, not a string")
	})
}
