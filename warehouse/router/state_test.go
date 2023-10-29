package router

import (
	"testing"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"

	"github.com/stretchr/testify/require"
)

func TestState(t *testing.T) {
	t.Run("inProgressState", func(t *testing.T) {
		t.Run("panic in case of unknown state", func(t *testing.T) {
			require.Panics(t, func() {
				inProgressState("unknown")
			})
		})
		t.Run("return in progress state", func(t *testing.T) {
			require.Equal(t, "generating_upload_schema", inProgressState(model.GeneratedUploadSchema))
		})
	})
	t.Run("nextState", func(t *testing.T) {
		testCases := []struct {
			name    string
			current string
			next    *state
		}{
			{
				name:    "unknown state",
				current: "unknown",
				next:    nil,
			},
			{
				name:    "in progress state",
				current: "generating_upload_schema",
				next:    stateTransitions[model.GeneratedUploadSchema],
			},
			{
				name:    "failed state",
				current: "generating_upload_schema_failed",
				next:    stateTransitions[model.GeneratedUploadSchema],
			},
			{
				name:    "completed state",
				current: "generated_upload_schema",
				next:    stateTransitions[model.CreatedTableUploads],
			},
		}
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				require.Equal(t, tc.next, nextState(tc.current))
			})
		}
	})
}
