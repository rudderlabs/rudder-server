package encoding_test

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"

	"github.com/rudderlabs/rudder-server/warehouse/encoding"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestMaxComputeJSONLoadTimeAndReader(t *testing.T) {
	factory := encoding.NewFactory(config.New())
	fixed := factory.NewEventLoader(nil, whutils.LoadFileTypeJson, whutils.MAXCOMPUTE, encoding.FixedPartitionMode)
	dynamic := factory.NewEventLoader(nil, whutils.LoadFileTypeJson, whutils.MAXCOMPUTE, "dynamic")
	bq := factory.NewEventLoader(nil, whutils.LoadFileTypeJson, whutils.BQ)
	for _, column := range []string{encoding.UUIDTsColumn, encoding.LoadedAtColumn, encoding.PartitionTimeColumn} {
		require.True(t, fixed.IsLoadTimeColumn(column), column)
		require.Equal(t, "2025-03-04 05:06:07.000000", time.Date(2025, 3, 4, 5, 6, 7, 0, time.UTC).Format(fixed.GetLoadTimeFormat(column)))
	}
	require.False(t, dynamic.IsLoadTimeColumn(encoding.PartitionTimeColumn))
	require.False(t, bq.IsLoadTimeColumn(encoding.PartitionTimeColumn))
	require.Equal(t, encoding.BQLoadedAtFormat, bq.GetLoadTimeFormat(encoding.LoadedAtColumn))
	fixed.AddColumn(encoding.PartitionTimeColumn, "datetime", "2025-03-04 05:06:07.000000")
	fixed.AddColumn("id", "string", "1")
	row, err := fixed.WriteToString()
	require.NoError(t, err)
	record, err := factory.NewEventReader(strings.NewReader(row), whutils.MAXCOMPUTE).Read([]string{"id", encoding.PartitionTimeColumn})
	require.NoError(t, err)
	require.Equal(t, []string{"1", "2025-03-04 05:06:07.000000"}, record)
}
