package model_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/stretchr/testify/require"
)

func TestGetLoadFileGenTime(t *testing.T) {
	inputs := []struct {
		timingsRaw        string
		loadFilesEpochStr string
	}{
		{
			timingsRaw:        "[{\"generating_upload_schema\":\"2022-07-04T16:09:03.001Z\"},{\"generated_upload_schema\":\"2022-07-04T16:09:04.141Z\"},{\"creating_table_uploads\":\"2022-07-04T16:09:04.144Z\"},{\"created_table_uploads\":\"2022-07-04T16:09:04.164Z\"},{\"generating_load_files\":\"2022-07-04T16:09:04.169Z\"},{\"generated_load_files\":\"2022-07-04T16:09:40.957Z\"},{\"updating_table_uploads_counts\":\"2022-07-04T16:09:40.959Z\"},{\"updated_table_uploads_counts\":\"2022-07-04T16:09:41.916Z\"},{\"creating_remote_schema\":\"2022-07-04T16:09:41.918Z\"},{\"created_remote_schema\":\"2022-07-04T16:09:41.920Z\"},{\"exporting_data\":\"2022-07-04T16:09:41.922Z\"},{\"exporting_data_failed\":\"2022-07-04T17:14:24.424Z\"}]",
			loadFilesEpochStr: "2022-07-04T16:09:04.169Z",
		},
		{
			timingsRaw:        "[]",
			loadFilesEpochStr: "0001-01-01T00:00:00.000Z",
		},
		{
			timingsRaw:        "[{\"generating_upload_schema\":\"2022-07-04T16:09:03.001Z\"},{\"generated_upload_schema\":\"2022-07-04T16:09:04.141Z\"},{\"creating_table_uploads\":\"2022-07-04T16:09:04.144Z\"},{\"created_table_uploads\":\"2022-07-04T16:09:04.164Z\"}]",
			loadFilesEpochStr: "0001-01-01T00:00:00.000Z",
		},
	}
	for _, input := range inputs {
		loadFilesEpochTime, err := time.Parse(misc.RFC3339Milli, input.loadFilesEpochStr)
		require.NoError(t, err)

		var timing model.Timings
		require.NoError(t, json.Unmarshal([]byte(input.timingsRaw), &timing))

		loadFileGenTime := model.GetLoadFileGenTime(timing)
		require.Equal(t, loadFilesEpochTime, loadFileGenTime)
	}
}

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
