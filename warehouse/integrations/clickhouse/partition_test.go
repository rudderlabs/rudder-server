package clickhouse

import (
	"testing"

	"github.com/stretchr/testify/require"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
)

func TestPartitionExpr(t *testing.T) {
	testCases := []struct {
		name          string
		partitionType string
		want          string
	}{
		{
			name: "default day for missing value",
			want: "toDate(received_at)",
		},
		{
			name:          "explicit day",
			partitionType: "day",
			want:          "toDate(received_at)",
		},
		{
			name:          "week",
			partitionType: "week",
			want:          "toStartOfWeek(received_at)",
		},
		{
			name:          "month",
			partitionType: "month",
			want:          "toStartOfMonth(received_at)",
		},
		{
			name:          "case-insensitive and trimmed value",
			partitionType: "  MoNtH ",
			want:          "toStartOfMonth(received_at)",
		},
		{
			name:          "invalid value defaults to day",
			partitionType: "year",
			want:          "toDate(received_at)",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			conf := config.New()
			ch := New(conf, logger.NOP, stats.NOP)
			cfg := map[string]any{}
			if tc.partitionType != "" {
				cfg[model.PartitionTypeSetting.String()] = tc.partitionType
			}
			ch.Warehouse = model.Warehouse{
				Destination: backendconfig.DestinationT{
					Config: cfg,
				},
			}

			require.Equal(t, tc.want, ch.partitionExpr())
		})
	}
}
