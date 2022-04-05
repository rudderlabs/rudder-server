package multitenant

import (
	"time"
)

type Legacy struct {
	*MultitenantStatsT
}

func WithLegacyPickupJobs(stats *MultitenantStatsT) *Legacy {
	return &Legacy{stats}
}

func (*Legacy) GetRouterPickupJobs(destType string, noOfWorkers int, routerTimeOut time.Duration, jobQueryBatchSize int, timeGained float64) (map[string]int, map[string]float64) {
	return map[string]int{
		"0": jobQueryBatchSize,
	}, map[string]float64{}
}
