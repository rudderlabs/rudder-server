package router

import (
	"testing"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"

	"github.com/stretchr/testify/require"
)

func TestState(t *testing.T) {
	t.Run("inProgressState", func(t *testing.T) {
		t.Run("invalid state", func(t *testing.T) {
			require.Panics(t, func() {
				inProgressState("unknown")
			})
		})
		t.Run("valid state", func(t *testing.T) {
			testcases := []struct {
				current         string
				inProgressState string
			}{
				{current: model.GeneratedUploadSchema, inProgressState: "generating_upload_schema"},
				{current: model.CreatedTableUploads, inProgressState: "creating_table_uploads"},
				{current: model.GeneratedLoadFiles, inProgressState: "generating_load_files"},
				{current: model.UpdatedTableUploadsCounts, inProgressState: "updating_table_uploads_counts"},
				{current: model.CreatedRemoteSchema, inProgressState: "creating_remote_schema"},
				{current: model.ExportedData, inProgressState: "exporting_data"},
			}

			for index, tc := range testcases {
				require.Equal(t, tc.inProgressState, inProgressState(tc.current), "test case %d", index)
			}
		})
	})
	t.Run("nextState", func(t *testing.T) {
		testCases := []struct {
			current string
			next    *state
		}{
			{current: "unknown", next: nil},

			// completed states
			{current: model.Waiting, next: stateTransitions[model.GeneratedUploadSchema]},
			{current: model.GeneratedUploadSchema, next: stateTransitions[model.CreatedTableUploads]},
			{current: model.CreatedTableUploads, next: stateTransitions[model.GeneratedLoadFiles]},
			{current: model.GeneratedLoadFiles, next: stateTransitions[model.UpdatedTableUploadsCounts]},
			{current: model.UpdatedTableUploadsCounts, next: stateTransitions[model.CreatedRemoteSchema]},
			{current: model.CreatedRemoteSchema, next: stateTransitions[model.ExportedData]},
			{current: model.ExportedData, next: nil},
			{current: model.Aborted, next: nil},

			// in progress states
			{current: "generating_upload_schema", next: stateTransitions[model.GeneratedUploadSchema]},
			{current: "creating_table_uploads", next: stateTransitions[model.CreatedTableUploads]},
			{current: "generating_load_files", next: stateTransitions[model.GeneratedLoadFiles]},
			{current: "updating_table_uploads_counts", next: stateTransitions[model.UpdatedTableUploadsCounts]},
			{current: "creating_remote_schema", next: stateTransitions[model.CreatedRemoteSchema]},
			{current: "exporting_data", next: stateTransitions[model.ExportedData]},

			// failed states
			{current: "generating_upload_schema_failed", next: stateTransitions[model.GeneratedUploadSchema]},
			{current: "creating_table_uploads_failed", next: stateTransitions[model.CreatedTableUploads]},
			{current: "generating_load_files_failed", next: stateTransitions[model.GeneratedLoadFiles]},
			{current: "updating_table_uploads_counts_failed", next: stateTransitions[model.UpdatedTableUploadsCounts]},
			{current: "creating_remote_schema_failed", next: stateTransitions[model.CreatedRemoteSchema]},
			{current: "exporting_data_failed", next: stateTransitions[model.ExportedData]},
		}
		for index, tc := range testCases {
			require.Equal(t, tc.next, nextState(tc.current), "test case %d", index)
		}
	})
}
