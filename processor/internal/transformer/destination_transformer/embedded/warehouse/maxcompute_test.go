package warehouse

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestMaxComputeColumnsIncludeLoadTime(t *testing.T) {
	transformer := New(config.New(), logger.NOP, stats.Default)
	columns, err := transformer.getColumns(whutils.MAXCOMPUTE, map[string]any{"id": "one"}, nil)
	require.NoError(t, err)
	require.Equal(t, "datetime", columns["loaded_at"])
	require.Equal(t, "datetime", columns["_partitiontime"])
}
