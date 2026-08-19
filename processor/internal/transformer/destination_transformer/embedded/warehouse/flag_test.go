package warehouse

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestIsJSONPathSupported(t *testing.T) {
	const (
		wsID   = "ws1"
		destID = "dest1"
	)

	t.Run("clickhouse gated by flag (dest / workspace / global, default off)", func(t *testing.T) {
		testCases := []struct {
			name     string
			key      string
			value    bool
			expected bool
		}{
			{"default off", "", false, false},
			{"global on", "Warehouse.clickhouse.enableJSONColumns", true, true},
			{"workspace on", "Warehouse.clickhouse.ws1.enableJSONColumns", true, true},
			{"destination on", "Warehouse.clickhouse.dest1.enableJSONColumns", true, true},
		}
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				c := config.New()
				if tc.key != "" {
					c.Set(tc.key, tc.value)
				}
				tr := New(c, logger.NOP, stats.NOP)
				require.Equal(t, tc.expected, tr.isJSONPathSupported(whutils.CLICKHOUSE, wsID, destID))
			})
		}
	})

	t.Run("clickhouse destination flag takes precedence over global", func(t *testing.T) {
		c := config.New()
		c.Set("Warehouse.clickhouse.enableJSONColumns", true)
		c.Set("Warehouse.clickhouse.dest1.enableJSONColumns", false)
		tr := New(c, logger.NOP, stats.NOP)
		require.False(t, tr.isJSONPathSupported(whutils.CLICKHOUSE, wsID, destID))
	})

	t.Run("non-clickhouse destinations keep static capability, ignore clickhouse flag", func(t *testing.T) {
		c := config.New()
		c.Set("Warehouse.clickhouse.enableJSONColumns", true)
		tr := New(c, logger.NOP, stats.NOP)
		// Postgres statically supports jsonPaths.
		require.True(t, tr.isJSONPathSupported(whutils.POSTGRES, wsID, destID))
		// A destination that does not support jsonPaths stays unsupported.
		require.False(t, tr.isJSONPathSupported(whutils.DELTALAKE, wsID, destID))
	})
}
