package ratelimiter

//go:generate mockgen -destination=../mocks/rate-limiter/mock_ratelimiter.go -package=mocks_ratelimiter github.com/rudderlabs/rudder-server/rate-limiter RateLimiter

import (
	"time"

	"github.com/EagleChen/restrictor"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

var (
	eventLimit            int
	rateLimitWindowInMins time.Duration
	noOfBucketsInWindow   int
)

//RateLimiter is an interface for rate limiting functions
type RateLimiter interface {
	LimitReached(key string) bool
}

//HandleT is a Handle for event limiter
type HandleT struct {
	restrictor restrictor.Restrictor
}

func init() {
	config.Initialize()
	loadConfig()
}

func loadConfig() {
	// Event limit when rate limit is enabled. 1000 by default
	eventLimit = config.GetInt("RateLimit.eventLimit", 1000)
	// Rolling time window for event limit. 60 mins by default
	rateLimitWindowInMins = config.GetDuration("RateLimit.rateLimitWindowInMins", time.Duration(60)) * time.Minute
	// Number of buckets in time window. 12 by default
	noOfBucketsInWindow = config.GetInt("RateLimit.noOfBucketsInWindow", 12)
}

//SetUp eventLimiter
func (rateLimiter *HandleT) SetUp() {
	store, err := restrictor.NewMemoryStore()
	if err != nil {
		logger.Error("memory store failed")
	}

	rateLimiter.restrictor = restrictor.NewRestrictor(rateLimitWindowInMins, uint32(eventLimit), uint32(noOfBucketsInWindow), store)
}

//LimitReached returns true if number of events in the rolling window is less than the max events allowed, else false
func (rateLimiter *HandleT) LimitReached(key string) bool {
	return rateLimiter.restrictor.LimitReached(key)
}
