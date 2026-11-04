package slave

import (
	"compress/gzip"
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
		"2025-03-04 05:06:07":                  "2025-03-04 05:06:07",
	} {
		got, ok := encoding.NormalizeMaxComputeDateTime(input)
		require.True(t, ok, input)
		require.Equal(t, want, got)
		again, ok := encoding.NormalizeMaxComputeDateTime(got)
		require.True(t, ok)
		require.Equal(t, got, again)
	}
	for _, invalid := range []any{"not a date", 42, nil} {
		_, ok := encoding.NormalizeMaxComputeDateTime(invalid)
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

func TestMaxComputeDiscardUsesLoadTimeAndNoPartitionTime(t *testing.T) {
	conf := config.New()
	jr := newJobRun(basePayload{DestinationType: whutils.MAXCOMPUTE, LoadFileType: whutils.LoadFileTypeJson}, 0, conf, logger.NOP, stats.NOP, encoding.NewFactory(conf))
	jr.uuidTS = time.Date(2025, 3, 5, 0, 6, 7, 0, time.FixedZone("UTC+2", 2*60*60))
	writer, err := jr.encodingFactory.NewLoadFileWriter(whutils.LoadFileTypeJson, filepath.Join(t.TempDir(), "discard.json.gz"), nil, whutils.MAXCOMPUTE)
	require.NoError(t, err)
	require.NoError(t, jr.handleDiscardTypes("events", "count", 1, types.Data{"id": "1", "received_at": "2025-03-04T22:06:07.123Z"}, &constraints.Violation{}, writer, "invalid"))
	require.NoError(t, writer.Close())
	row := maxComputeLoadRow(t, writer.GetLoadFile().Name())
	require.Equal(t, "2025-03-04 22:06:07.123", row["received_at"])
	require.Equal(t, "2025-03-04 22:06:07", row["loaded_at"])
	require.Equal(t, "2025-03-04 22:06:07", row["uuid_ts"])
	require.NotContains(t, row, "_partitiontime")
}

func TestMaxComputeInvalidDateTimeDiscardPreservesValue(t *testing.T) {
	conf := config.New()
	jr := newJobRun(basePayload{DestinationType: whutils.MAXCOMPUTE, LoadFileType: whutils.LoadFileTypeJson}, 0, conf, logger.NOP, stats.NOP, encoding.NewFactory(conf))
	jr.now = func() time.Time { return time.Date(2025, 3, 4, 5, 6, 7, 123456000, time.UTC) }
	writer, err := jr.encodingFactory.NewLoadFileWriter(whutils.LoadFileTypeJson, filepath.Join(t.TempDir(), "discard.json.gz"), nil, whutils.MAXCOMPUTE)
	require.NoError(t, err)
	require.NoError(t, jr.handleDiscardTypes("events", "received_at", "not a datetime", types.Data{"id": "1", "received_at": "not a datetime"}, &constraints.Violation{}, writer, "invalid datetime"))
	require.NoError(t, writer.Close())
	row := maxComputeLoadRow(t, writer.GetLoadFile().Name())
	require.Equal(t, "not a datetime", row["column_value"])
	require.Equal(t, "2025-03-04 05:06:07.123456", row["received_at"])
}
