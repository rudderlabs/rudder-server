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
				name:    "waiting state",
				current: model.Waiting,
				next:    stateTransitions[model.GeneratedUploadSchema],
			},
			{
				name:    "generating_upload_schema",
				current: model.GeneratedUploadSchema,
				next:    stateTransitions[model.CreatedTableUploads],
			},
			{
				name:    "creating_table_uploads",
				current: model.CreatedTableUploads,
				next:    stateTransitions[model.GeneratedLoadFiles],
			},
			{
				name:    "generating_load_files",
				current: model.GeneratedLoadFiles,
				next:    stateTransitions[model.UpdatedTableUploadsCounts],
			},
			{
				name:    "updating_table_uploads_counts",
				current: model.UpdatedTableUploadsCounts,
				next:    stateTransitions[model.CreatedRemoteSchema],
			},
			{
				name:    "creating_remote_schema",
				current: model.CreatedRemoteSchema,
				next:    stateTransitions[model.ExportedData],
			},
			{
				name:    "exporting_data",
				current: model.ExportedData,
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
		}
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				require.Equal(t, tc.next, nextState(tc.current))
			})
		}
	})
}
