package model

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPartitionPlanMode(t *testing.T) {
	require.Equal(t, "fixed", (PartitionPlan{Source: "_partitiontime", Granularity: "hour"}).Mode())
	require.Equal(t, "dynamic", (PartitionPlan{Source: "received_at", Granularity: "day"}).Mode())
}
