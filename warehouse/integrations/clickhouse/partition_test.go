package clickhouse

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
)

func TestPartitionExpr(t *testing.T) {
	testCases := []struct {
		name                  string
		partitionType         string
		expectedPartitionExpr string
		wantError             bool
	}{
		{
			name:                  "default day for empty value",
			partitionType:         "",
			expectedPartitionExpr: "toDate(received_at)",
		},
		{
			name:                  "explicit day",
			partitionType:         "day",
			expectedPartitionExpr: "toDate(received_at)",
		},
		{
			name:                  "explicit week",
			partitionType:         "week",
			expectedPartitionExpr: "toStartOfWeek(received_at)",
		},
		{
			name:                  "explicit month",
			partitionType:         "month",
			expectedPartitionExpr: "toStartOfMonth(received_at)",
		},
		{
			name:                  "invalid value",
			partitionType:         "invalid",
			expectedPartitionExpr: "",
			wantError:             true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			conf := config.New()
			ch := New(conf, logger.NOP, stats.NOP)
			cfg := map[string]any{
				"partitionType": tc.partitionType,
			}
			ch.Warehouse = model.Warehouse{
				Destination: backendconfig.DestinationT{
					Config: cfg,
				},
			}

			partitionExpr, err := ch.partitionExpr()
			if tc.wantError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expectedPartitionExpr, partitionExpr)
		})
	}
}
