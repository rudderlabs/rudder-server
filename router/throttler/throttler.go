package throttler

import (
	"fmt"
	"time"

	"github.com/coinpaprika/ratelimiter"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

var (
	parameters map[string]map[string]interface{}
)

//Throttler is an interface for throttling functions
type Throttler interface {
	LimitReached(key string) bool
	IsEnabled() bool
}

//HandleT is a Handle for event limiter
type HandleT struct {
	UserEventOrderingRequired bool
	enabled                   bool
	destination               string
	eventLimit                int
	timeWindow                time.Duration
	bucketsInWindow           int
	userLevelThrottling       bool
	userLevelThrottler        *UserHandleT
	ratelimiter               *ratelimiter.RateLimiter
}

//UserHandleT is a handle for user level event limiter
type UserHandleT struct {
	enabled         bool
	destination     string
	eventLimit      int
	timeWindow      time.Duration
	bucketsInWindow int
	ratelimiter     *ratelimiter.RateLimiter
}

func init() {
	loadConfig()
}

func loadConfig() {
	parameters = map[string]map[string]interface{}{
		// https://customer.io/docs/api/#api-documentationlimits
		"CUSTOMERIO": {
			"limit":                     30,
			"timeWindowInS":             1,
			"userEventOrderingRequired": true,
			"userLevelThrottling":       false,
		},
		// https://help.amplitude.com/hc/en-us/articles/360032842391-HTTP-API-V2#upload-limit
		"AM": {
			"limit":                     1000,
			"timeWindowInS":             1,
			"userEventOrderingRequired": true,
			"userLevelThrottling":       true,
			"userLevelLimit":            10,
			"userLevelTimeWindowInS":    1,
		},
	}
}

func (throttler *HandleT) setLimits() {
	// set eventLimit
	defaultLimit, _ := parameters[throttler.destination]["limit"].(int)
	throttler.eventLimit = config.GetInt(fmt.Sprintf(`Router.throttler.%s.limit`, throttler.destination), defaultLimit)

	// set timeWindow
	defaultTimeWindow, _ := parameters[throttler.destination]["timeWindowInS"].(int)
	throttler.timeWindow = config.GetDuration(fmt.Sprintf(`Router.throttler.%s.timeWindowInS`, throttler.destination), time.Duration(defaultTimeWindow)) * time.Second

	// set userEventOrderingRequired
	defautOrderingRequired, ok := parameters[throttler.destination]["userEventOrderingRequired"].(bool)
	if !ok {
		defautOrderingRequired = true
	}
	throttler.UserEventOrderingRequired = config.GetBool(fmt.Sprintf(`Router.throttler.%s.userEventOrderingRequired`, throttler.destination), defautOrderingRequired)

	// enable throttler
	if throttler.eventLimit != 0 && throttler.timeWindow != 0 {
		throttler.enabled = true
	}

	defautUserLevelThrottling, _ := parameters[throttler.destination]["userLevelThrottling"].(bool)
	throttler.userLevelThrottling = config.GetBool(fmt.Sprintf(`Router.throttler.%s.userLevelThrottling`, throttler.destination), defautUserLevelThrottling)

	if throttler.userLevelThrottling {
		// set eventLimit for userLevelThrottler
		userLevelDefaultLimit, _ := parameters[throttler.destination]["userLevelLimit"].(int)
		throttler.userLevelThrottler.eventLimit = config.GetInt(fmt.Sprintf(`Router.throttler.%s.userLevelLimit`, throttler.destination), userLevelDefaultLimit)

		// set timeWindow for userLevelThrottler
		userLevelDefaultTimeWindow, _ := parameters[throttler.destination]["userLevelTimeWindowInS"].(int)
		throttler.userLevelThrottler.timeWindow = config.GetDuration(fmt.Sprintf(`Router.throttler.%s.userLevelTimeWindowInS`, throttler.destination), time.Duration(userLevelDefaultTimeWindow)) * time.Second

		// enable userLevelthrottler
		if throttler.userLevelThrottler.eventLimit != 0 && throttler.userLevelThrottler.timeWindow != 0 {
			throttler.userLevelThrottler.enabled = true
		}
	}
}

//SetUp eventLimiter
func (throttler *HandleT) SetUp(destination string) {
	throttler.destination = destination
	var userLevelThrottler UserHandleT
	throttler.userLevelThrottler = &userLevelThrottler

	// check if it has throttling config for destination
	throttler.setLimits()

	if throttler.enabled {
		dataStore := ratelimiter.NewMapLimitStore(2*throttler.timeWindow, 10*time.Second)
		throttler.ratelimiter = ratelimiter.New(dataStore, int64(throttler.eventLimit), throttler.timeWindow)
		throttler.enabled = true
	}

	if throttler.userLevelThrottler.enabled {
		dataStore := ratelimiter.NewMapLimitStore(2*throttler.userLevelThrottler.timeWindow, 10*time.Second)
		throttler.userLevelThrottler.ratelimiter = ratelimiter.New(dataStore, int64(throttler.userLevelThrottler.eventLimit), throttler.userLevelThrottler.timeWindow)
		throttler.enabled = true
	}
}

//LimitReached returns true if number of events in the rolling window is less than the max events allowed, else false
func (throttler *HandleT) LimitReached(destID string, userID string) bool {
	// do not call LimitReached in single if statement even though both throttlers are enabled
	// as restrictor.LimitReached has side-effect of incrementing the count for the key

	var destLevelLimitReached bool
	destKey := fmt.Sprintf(`%s_%s`, throttler.destination, destID)
	userKey := fmt.Sprintf(`%s_%s_%s`, throttler.destination, destID, userID)

	if throttler.enabled {
		limitStatus, err := throttler.ratelimiter.Check(destKey)
		if err != nil {
			// TODO: handle this
			logger.Errorf(`[[ %s-router-throttler: Error checking limitStatus: %v]]`, throttler.destination, err)
		} else {
			destLevelLimitReached = limitStatus.IsLimited
		}
	}

	var userLevelLimitReached bool
	if !destLevelLimitReached && throttler.userLevelThrottler.enabled {
		limitStatus, err := throttler.userLevelThrottler.ratelimiter.Check(userKey)
		if err != nil {
			// TODO: handle this
			logger.Errorf(`[[ %s-router-throttler: Error checking limitStatus: %v]]`, throttler.destination, err)
		} else {
			userLevelLimitReached = limitStatus.IsLimited
		}
	}

	limitReached := destLevelLimitReached || userLevelLimitReached

	if !limitReached {
		if throttler.enabled {
			throttler.ratelimiter.Inc(destKey)
		}
		if throttler.userLevelThrottler.enabled {
			throttler.userLevelThrottler.ratelimiter.Inc(userKey)
		}
	}

	return limitReached
}

func (throttler *HandleT) IsEnabled() bool {
	return throttler.enabled || throttler.userLevelThrottler.enabled
}
