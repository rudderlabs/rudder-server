package multitenant

import (
	"time"
)

type Legacy struct {
	*Stats
}

func WithLegacyPickupJobs(stats *Stats) *Legacy {
	return &Legacy{stats}
}

func (*Legacy) GetRouterPickupJobs(_ string, _ int, _ time.Duration, jobQueryBatchSize int) map[string]int {
	return map[string]int{
		"0": jobQueryBatchSize,
	}
}
