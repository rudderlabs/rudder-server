package model_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
)

func TestGetLastFailedStatus(t *testing.T) {
	inputs := []struct {
		timingsRaw string
		status     string
	}{
		{
			timingsRaw: "[{\"generating_upload_schema\":\"2022-07-04T16:09:03.001Z\"},{\"generated_upload_schema\":\"2022-07-04T16:09:04.141Z\"},{\"creating_table_uploads\":\"2022-07-04T16:09:04.144Z\"},{\"created_table_uploads\":\"2022-07-04T16:09:04.164Z\"},{\"generating_load_files\":\"2022-07-04T16:09:04.169Z\"},{\"generated_load_files\":\"2022-07-04T16:09:40.957Z\"},{\"updating_table_uploads_counts\":\"2022-07-04T16:09:40.959Z\"},{\"updated_table_uploads_counts\":\"2022-07-04T16:09:41.916Z\"},{\"creating_remote_schema\":\"2022-07-04T16:09:41.918Z\"},{\"created_remote_schema\":\"2022-07-04T16:09:41.920Z\"},{\"exporting_data\":\"2022-07-04T16:09:41.922Z\"},{\"exporting_data_failed\":\"2022-07-04T17:14:24.424Z\"}]",
			status:     "exporting_data_failed",
		},
		{
			timingsRaw: "[]",
			status:     "",
		},
		{
			timingsRaw: "[{\"generating_upload_schema\":\"2022-07-04T16:09:03.001Z\"},{\"generated_upload_schema\":\"2022-07-04T16:09:04.141Z\"},{\"creating_table_uploads\":\"2022-07-04T16:09:04.144Z\"},{\"created_table_uploads\":\"2022-07-04T16:09:04.164Z\"}]",
			status:     "",
		},
	}
	for _, input := range inputs {
		var timing model.Timings
		require.NoError(t, json.Unmarshal([]byte(input.timingsRaw), &timing))

		status := model.GetLastFailedStatus(timing)
		require.Equal(t, status, input.status)
	}
}

func TestGetUserFriendlyJobErrorCategory(t *testing.T) {
	testCases := []struct {
		name             string
		errorType        model.JobErrorType
		expectedCategory string
	}{
		{
			name:             "Uncategorized error",
			errorType:        model.UncategorizedError,
			expectedCategory: "Uncategorized error",
		},
		{
			name:             "Permission error",
			errorType:        model.PermissionError,
			expectedCategory: "Permission error",
		},
		{
			name:             "Some other error",
			errorType:        "some_other_error",
			expectedCategory: "Uncategorized error",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expectedCategory, model.GetUserFriendlyJobErrorCategory(tc.errorType))
		})
	}
}
