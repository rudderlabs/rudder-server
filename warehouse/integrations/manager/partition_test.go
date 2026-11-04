package manager

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestMaxComputeStatsManagerForwardsPartitionPlanner(t *testing.T) {
	warehouseManager, err := New(warehouseutils.MAXCOMPUTE, config.New(), logger.NOP, stats.Default)
	require.NoError(t, err)
	planner, ok := warehouseManager.(PartitionPlanner)
	require.True(t, ok)
	planner.SetPlannedPartitions(map[string]model.PartitionPlan{
		"events": {Source: "_partitiontime", Granularity: "day"},
	})
	_, err = planner.CatalogPartition(context.Background(), "events")
	require.ErrorContains(t, err, "not initialized")
}
