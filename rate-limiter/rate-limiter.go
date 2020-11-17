package ratelimiter

//go:generate mockgen -destination=../mocks/rate-limiter/mock_ratelimiter.go -package=mocks_ratelimiter github.com/rudderlabs/rudder-server/rate-limiter RateLimiter

import (
	"fmt"
	"net/http"
	"time"

	"github.com/EagleChen/restrictor"
	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

var (
	eventLimit            int
	rateLimitWindowInMins time.Duration
	noOfBucketsInWindow   int
	pkgLogger             logger.LoggerI
	configBackendURL      string
)

//RateLimiter is an interface for rate limiting functions
type RateLimiter interface {
	LimitReached(key string) bool
	HasLimitReachedNotified() bool
	HasLimitRelaxedNotified() bool
	Notfiy(val bool) bool
	SetLimitRelaxedNotified(val bool) bool
	SetLimitReachedNotified(val bool) bool
}

//HandleT is a Handle for event limiter
type HandleT struct {
	restrictor           restrictor.Restrictor
	LimitRelaxedNotified bool
	LimitReachedNotified bool
	backendConfig        backendconfig.BackendConfig
}

func init() {
	loadConfig()
	pkgLogger = logger.NewLogger().Child("rate-limiter")
}

func loadConfig() {
	// Event limit when rate limit is enabled. 1000 by default
	eventLimit = config.GetInt("RateLimit.eventLimit", 1000)
	// Rolling time window for event limit. 60 mins by default
	rateLimitWindowInMins = config.GetDuration("RateLimit.rateLimitWindowInMins", time.Duration(60)) * time.Minute
	// Number of buckets in time window. 12 by default
	noOfBucketsInWindow = config.GetInt("RateLimit.noOfBucketsInWindow", 12)

	configBackendURL = config.GetEnv("CONFIG_BACKEND_URL", "https://api.rudderlabs.com")
}

//SetUp eventLimiter
func (rateLimiter *HandleT) SetUp() {
	store, err := restrictor.NewMemoryStore()
	if err != nil {
		pkgLogger.Error("memory store failed")
	}

	rateLimiter.restrictor = restrictor.NewRestrictor(rateLimitWindowInMins, uint32(eventLimit), uint32(noOfBucketsInWindow), store)
	rateLimiter.LimitRelaxedNotified = true
	rateLimiter.LimitReachedNotified = false
}

//LimitReached returns true if number of events in the rolling window is less than the max events allowed, else false
func (rateLimiter *HandleT) LimitReached(key string) bool {
	return rateLimiter.restrictor.LimitReached(key)
}

// HasLimitReachedNotified returns the variable LimitReachedNotified
func (rateLimiter *HandleT) HasLimitReachedNotified() bool {
	return rateLimiter.LimitReachedNotified
}

// HasLimitRelaxedNotified returns the variable LimitRelaxedNotified
func (rateLimiter *HandleT) HasLimitRelaxedNotified() bool {
	return rateLimiter.LimitRelaxedNotified
}

// SetLimitReachedNotified sets the variable LimitReachedNotified
func (rateLimiter *HandleT) SetLimitReachedNotified(val bool) bool {
	rateLimiter.LimitReachedNotified = val
	return rateLimiter.LimitReachedNotified
}

// SetLimitRelaxedNotified sets the variable LimitRelaxedNotified
func (rateLimiter *HandleT) SetLimitRelaxedNotified(val bool) bool {
	rateLimiter.LimitRelaxedNotified = val
	return rateLimiter.LimitRelaxedNotified
}

// Notify makes a request to config-backend to toggle the rate-limit for the given workspace
func Notify(val bool) {

	backendConfig := backendconfig.GetConfig()
	client := &http.Client{}
	url := fmt.Sprintf("%s/workspaces/toggleRateLimit?isLimitReached=%t", configBackendURL, val)
	req, err := http.NewRequest("GET", url, nil)
	req.SetBasicAuth(backendConfig.WorkspaceID, "")
	_, err = client.Do(req)
	if err != nil {
		pkgLogger.Error("ConfigBackend error at %s", configBackendURL)
	}
}
