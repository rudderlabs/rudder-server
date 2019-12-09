package ratelimiter

import (
	"time"

	"github.com/EagleChen/restrictor"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

var (
	eventLimitInHostedService int
	rateLimitWindowInMins     time.Duration
	noOfBucketsInWindow       int
)

//HandleT is a Handle for event limiter
type HandleT struct {
	restrictor restrictor.Restrictor
}

func init() {
	config.Initialize()
	loadConfig()
}

func loadConfig() {
	// Event limit in hosted service. 1000 by default
	eventLimitInHostedService = config.GetInt("HostedService.eventLimit", 1000)
	// Rolling time window for event limit in hosted service. 60 mins by default
	rateLimitWindowInMins = config.GetDuration("HostedService.rateLimitWindowInMins", time.Duration(60)) * time.Minute
	// Number of buckets in time window. 12 by default
	noOfBucketsInWindow = config.GetInt("HostedService.noOfBucketsInWindow", 12)
}

//SetUp eventLimiter
func (eventLimiterHandle *HandleT) SetUp() {
	store, err := restrictor.NewMemoryStore()
	if err != nil {
		logger.Error("memory store failed")
	}

	eventLimiterHandle.restrictor = restrictor.NewRestrictor(rateLimitWindowInMins, uint32(eventLimitInHostedService), uint32(noOfBucketsInWindow), store)
}

//AllowEventBatch returns true if number of events in the rolling window is less than the max events allowed, else false
func (eventLimiterHandle *HandleT) LimitReached(key string) bool {
	return eventLimiterHandle.restrictor.LimitReached(key)
}
