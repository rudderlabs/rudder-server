package warehouse

import (
	"fmt"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestDestinationSpecificConfigKey(t *testing.T) {
	warehouseTypes := []string{
		"AZURE_DATALAKE",
		"GCS_DATALAKE",
		"BQ",
		"S3_DATALAKE",
		"RS",
		"DELTALAKE",
		"SNOWFLAKE",
		"MSSQL",
		"AZURE_SYNAPSE",
		"CLICKHOUSE",
		"POSTGRES",
	}
	for _, whType := range warehouseTypes {
		t.Run(whType, func(t *testing.T) {
			configKey := fmt.Sprintf("Warehouse.%s.feature", whType)
			got := config.TransformKey(configKey)
			expected := fmt.Sprintf("RSERVER_WAREHOUSE_%s_FEATURE", whType)
			require.Equal(t, got, expected)
		})
	}
}
