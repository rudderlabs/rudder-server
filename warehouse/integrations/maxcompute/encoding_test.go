package maxcompute

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"

	"github.com/rudderlabs/rudder-server/warehouse/encoding"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestJSONEncodingUsesMaxComputeTimestampAndReader(t *testing.T) {
	factory := encoding.NewFactory(config.New())
	loader := factory.NewEventLoader(nil, warehouseutils.LoadFileTypeJson, warehouseutils.MAXCOMPUTE)
	require.True(t, loader.IsLoadTimeColumn(encoding.UUIDTsColumn))
	require.True(t, loader.IsLoadTimeColumn(encoding.LoadedAtColumn))
	require.Equal(t, encoding.MaxComputeTimeFormat, loader.GetLoadTimeFormat(encoding.UUIDTsColumn))
	loader.AddColumn("id", "string", "1")
	loader.AddColumn("uuid_ts", "datetime", "2025-03-04 05:06:07.000000")
	row, err := loader.WriteToString()
	require.NoError(t, err)
	record, err := factory.NewEventReader(strings.NewReader(row), warehouseutils.MAXCOMPUTE).Read([]string{"id", "uuid_ts"})
	require.NoError(t, err)
	require.Equal(t, []string{"1", "2025-03-04 05:06:07.000000"}, record)
}
