package multitenant

import (
	"time"
)

type Legacy struct {
	*StatsT
}

func WithLegacyPickupJobs(stats *StatsT) *Legacy {
	return &Legacy{stats}
}

func (*Legacy) GetRouterPickupJobs(_ string, _ int, _ time.Duration, jobQueryBatchSize int, _ float64) (map[string]int, map[string]float64) {
	return map[string]int{
		"0": jobQueryBatchSize,
	}, map[string]float64{}
}
