package slave

import (
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-server/warehouse/constraints"
	"github.com/rudderlabs/rudder-server/warehouse/encoding"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/rudderlabs/rudder-server/warehouse/utils/types"
)

func TestMaxComputeSlaveOSSConfig(t *testing.T) {
	stop := errors.New("factory reached")
	for _, useRudderStorage := range []bool{false, true} {
		for _, mode := range []string{"role", "accessKey", "mixed", "missing", "partial"} {
			t.Run(fmt.Sprintf("%s/useRudderStorage=%t", mode, useRudderStorage), func(t *testing.T) {
				conf := map[string]any{"bucketName": "customer-bucket", "region": "cn-hangzhou", "ossRoleArn": "read-role"}
				switch mode {
				case "role":
					conf["iamRoleARN"] = "role"
				case "accessKey":
					conf["accessKeyId"], conf["accessKeySecret"] = "id", "secret"
				case "mixed":
					conf["iamRoleARN"], conf["accessKeyId"], conf["accessKeySecret"] = "role", "id", "secret"
				case "partial":
					conf["accessKeyId"] = "id"
				}
				called := false
				payload := &basePayload{DestinationType: whutils.MAXCOMPUTE, WorkspaceID: "workspace-id"}
				_, err := payload.fileManagerWithFactory(conf, useRudderStorage, func(s *filemanager.Settings) (filemanager.FileManager, error) {
					called = true
					require.Equal(t, whutils.AlibabaOSS, s.Provider)
					require.Equal(t, "customer-bucket", s.Config["bucketName"])
					require.Equal(t, mode == "role", s.Config["roleBasedAuth"])
					if mode == "role" {
						require.Equal(t, "workspace-id", s.Config["externalID"])
					} else {
						require.NotContains(t, s.Config, "externalID")
					}
					return nil, stop
				})
				if mode == "role" || mode == "accessKey" {
					require.ErrorIs(t, err, stop)
					require.True(t, called)
				} else {
					require.Error(t, err)
					require.False(t, called)
				}
			})
		}
	}
}

func TestNormalizeMaxComputeDateTime(t *testing.T) {
	for input, want := range map[string]string{
		"2025-03-04T05:06:07.123456Z":          "2025-03-04 05:06:07.123456",
		"2025-03-04 06:06:07.123456 +0100 CET": "2025-03-04 05:06:07.123456",
		"2025-03-04 06:06:07.123456 +0100":     "2025-03-04 05:06:07.123456",
		"2025-03-04 05:06:07.123456":           "2025-03-04 05:06:07.123456",
		"2025-03-04 05:06:07":                  "2025-03-04 05:06:07.000000",
	} {
		got, ok := normalizeMaxComputeDateTime(input)
		require.True(t, ok, input)
		require.Equal(t, want, got)
		again, ok := normalizeMaxComputeDateTime(got)
		require.True(t, ok)
		require.Equal(t, got, again)
	}
	for _, invalid := range []any{"not a date", 42, nil} {
		_, ok := normalizeMaxComputeDateTime(invalid)
		require.False(t, ok)
	}
}

func maxComputeLoadRow(t *testing.T, path string) map[string]any {
	t.Helper()
	file, err := os.Open(path)
	require.NoError(t, err)
	defer file.Close()
	reader, err := gzip.NewReader(file)
	require.NoError(t, err)
	defer reader.Close()
	var row map[string]any
	require.NoError(t, jsonrs.NewDecoder(reader).Decode(&row))
	return row
}

func TestMaxComputeFixedPartitionDiscard(t *testing.T) {
	for _, tc := range []struct {
		name, granularity, partitionTime string
	}{
		{"default day", "", "2025-03-04 00:00:00.000000"},
		{"day", "day", "2025-03-04 00:00:00.000000"},
		{"hour", "hour", "2025-03-04 22:00:00.000000"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conf := config.New()
			jr := newJobRun(basePayload{
				DestinationType:        whutils.MAXCOMPUTE,
				LoadFileType:           whutils.LoadFileTypeJson,
				PartitionModes:         map[string]string{whutils.DiscardsTable: encoding.FixedPartitionMode},
				PartitionGranularities: map[string]string{whutils.DiscardsTable: tc.granularity, "events": "hour"},
			}, 0, conf, logger.NOP, stats.NOP, encoding.NewFactory(conf))
			jr.uuidTS = time.Date(2025, 3, 5, 0, 6, 7, 0, time.FixedZone("UTC+2", 2*60*60))
			writer, err := jr.encodingFactory.NewLoadFileWriter(whutils.LoadFileTypeJson, filepath.Join(t.TempDir(), "discard.json.gz"), nil, whutils.MAXCOMPUTE)
			require.NoError(t, err)
			require.NoError(t, jr.handleDiscardTypes("events", "count", 1, types.Data{"id": "1", "received_at": "2025-03-04T22:06:07.123Z"}, &constraints.Violation{}, writer, "invalid"))
			require.NoError(t, writer.Close())
			row := maxComputeLoadRow(t, writer.GetLoadFile().Name())
			require.Equal(t, "2025-03-04 22:06:07.123000", row["received_at"])
			require.Equal(t, tc.partitionTime, row[encoding.PartitionTimeColumn])
			require.Equal(t, "2025-03-04 22:06:07.000000", row["loaded_at"])
			require.Equal(t, "2025-03-04 22:06:07.000000", row["uuid_ts"])
		})
	}
}

func TestMaxComputeFixedPartitionNormalWriter(t *testing.T) {
	conf := config.New()
	factory := encoding.NewFactory(conf)
	job := basePayload{
		DestinationType: whutils.MAXCOMPUTE,
		LoadFileType:    whutils.LoadFileTypeJson,
		UploadSchema: model.Schema{
			"daily":   {encoding.PartitionTimeColumn: model.DateTimeDataType, encoding.LoadedAtColumn: model.DateTimeDataType, encoding.UUIDTsColumn: model.DateTimeDataType},
			"hourly":  {encoding.PartitionTimeColumn: model.DateTimeDataType, encoding.LoadedAtColumn: model.DateTimeDataType, encoding.UUIDTsColumn: model.DateTimeDataType},
			"default": {encoding.PartitionTimeColumn: model.DateTimeDataType, encoding.LoadedAtColumn: model.DateTimeDataType, encoding.UUIDTsColumn: model.DateTimeDataType},
		},
		PartitionModes:         map[string]string{"daily": encoding.FixedPartitionMode, "hourly": encoding.FixedPartitionMode, "default": encoding.FixedPartitionMode},
		PartitionGranularities: map[string]string{"daily": "day", "hourly": "hour"},
	}
	jr := newJobRun(job, 0, conf, logger.NOP, stats.NOP, factory)
	defer jr.cleanup()
	jr.uuidTS = time.Date(2025, 3, 5, 0, 6, 7, 0, time.FixedZone("UTC+2", 2*60*60))
	stagingFile := stagingFileInfo{ID: 1, Location: "staging.json.gz"}
	stagingPath := filepath.Join(t.TempDir(), stagingFile.Location)
	file, err := os.Create(stagingPath)
	require.NoError(t, err)
	gz := gzip.NewWriter(file)
	for _, table := range []string{"daily", "hourly", "default"} {
		event, err := jsonrs.Marshal(types.BatchRouterEvent{Metadata: types.Metadata{Table: table}})
		require.NoError(t, err)
		_, err = gz.Write(append(event, '\n'))
		require.NoError(t, err)
	}
	require.NoError(t, gz.Close())
	require.NoError(t, file.Close())
	jr.stagingFilePaths[stagingFile.ID] = stagingPath
	jr.downloadStagingFile = func(context.Context, stagingFileInfo) error { return nil }
	w := newWorker(conf, logger.NOP, stats.NOP, nil, nil, constraints.New(conf), factory, 0)
	require.NoError(t, w.processSingleStagingFile(context.Background(), jr, &job, stagingFile, ""))
	jr.closeLoadFiles()
	for table, want := range map[string]string{
		"daily":   "2025-03-04 00:00:00.000000",
		"hourly":  "2025-03-04 22:00:00.000000",
		"default": "2025-03-04 00:00:00.000000",
	} {
		row := maxComputeLoadRow(t, jr.outputFileWritersMap[table].GetLoadFile().Name())
		require.Equal(t, want, row[encoding.PartitionTimeColumn], table)
		require.Equal(t, "2025-03-04 22:06:07.000000", row[encoding.LoadedAtColumn], table)
		require.Equal(t, "2025-03-04 22:06:07.000000", row[encoding.UUIDTsColumn], table)
	}
}
