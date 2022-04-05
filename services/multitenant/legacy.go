package multitenant

import (
	"time"
)

type legacy struct {
	*MultitenantStatsT
}

func WithLegacyPickupJobs(stats *MultitenantStatsT) *legacy {
	return &legacy{stats}
}

func (*legacy) GetRouterPickupJobs(destType string, noOfWorkers int, routerTimeOut time.Duration, jobQueryBatchSize int, timeGained float64) (map[string]int, map[string]float64) {
	return map[string]int{
		"0": jobQueryBatchSize,
	}, map[string]float64{}
}
