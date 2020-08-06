package throttler

import (
	"fmt"
	"time"

	"github.com/coinpaprika/ratelimiter"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

var (
	settings map[string]Settings
)

//Throttler is an interface for throttling functions
type Throttler interface {
	LimitReached(key string) bool
	IsEnabled() bool
}

type Limiter struct {
	enabled     bool
	eventLimit  int
	timeWindow  time.Duration
	ratelimiter *ratelimiter.RateLimiter
}

type Settings struct {
	eventLimit             int
	timeWindowInS          int
	userLevelThrottling    bool
	userLevelLimit         int
	userLevelTimeWindowInS int
}

//HandleT is a Handle for event limiter
type HandleT struct {
	destinationName string
	destLimiter     *Limiter
	userLimiter     *Limiter
}

func init() {
	loadConfig()
}

func loadConfig() {
	settings = map[string]Settings{
		// https://customer.io/docs/api/#api-documentationlimits
		"CUSTOMERIO": {
			eventLimit:          30,
			timeWindowInS:       1,
			userLevelThrottling: false,
		},
		// https://help.amplitude.com/hc/en-us/articles/360032842391-HTTP-API-V2#upload-limit
		"AM": {
			eventLimit:             1000,
			timeWindowInS:          1,
			userLevelThrottling:    true,
			userLevelLimit:         10,
			userLevelTimeWindowInS: 1,
		},
	}
}

func (throttler *HandleT) setLimits() {
	destName := throttler.destinationName

	// set eventLimit
	throttler.destLimiter.eventLimit = config.GetInt(fmt.Sprintf(`Router.throttler.%s.limit`, destName), settings[destName].eventLimit)

	// set timeWindow
	throttler.destLimiter.timeWindow = config.GetDuration(fmt.Sprintf(`Router.throttler.%s.timeWindowInS`, destName), time.Duration(settings[destName].timeWindowInS)) * time.Second

	// enable dest throttler
	if throttler.destLimiter.eventLimit != 0 && throttler.destLimiter.timeWindow != 0 {
		throttler.destLimiter.enabled = true
	}

	// set eventLimit
	throttler.userLimiter.eventLimit = config.GetInt(fmt.Sprintf(`Router.throttler.%s.userLevelLimit`, destName), settings[destName].userLevelLimit)

	// set timeWindow
	throttler.userLimiter.timeWindow = config.GetDuration(fmt.Sprintf(`Router.throttler.%s.userLevelTimeWindowInS`, destName), time.Duration(settings[destName].userLevelTimeWindowInS)) * time.Second

	// enable dest throttler
	if throttler.userLimiter.eventLimit != 0 && throttler.userLimiter.timeWindow != 0 {
		throttler.userLimiter.enabled = true
	}
}

//SetUp eventLimiter
func (throttler *HandleT) SetUp(destName string) {
	throttler.destinationName = destName
	// var userLevelThrottler UserHandleT
	// throttler.userLevelThrottler = &userLevelThrottler
	throttler.destLimiter = &Limiter{}
	throttler.userLimiter = &Limiter{}

	// check if it has throttling config for destination
	throttler.setLimits()

	if throttler.destLimiter.enabled {
		dataStore := ratelimiter.NewMapLimitStore(2*throttler.destLimiter.timeWindow, 10*time.Second)
		throttler.destLimiter.ratelimiter = ratelimiter.New(dataStore, int64(throttler.destLimiter.eventLimit), throttler.destLimiter.timeWindow)
	}

	if throttler.userLimiter.enabled {
		dataStore := ratelimiter.NewMapLimitStore(2*throttler.userLimiter.timeWindow, 10*time.Second)
		throttler.userLimiter.ratelimiter = ratelimiter.New(dataStore, int64(throttler.userLimiter.eventLimit), throttler.userLimiter.timeWindow)
	}
}

//LimitReached returns true if number of events in the rolling window is less than the max events allowed, else false
func (throttler *HandleT) LimitReached(destID string, userID string) bool {
	// do not call LimitReached in single if statement even though both throttlers are enabled
	// as restrictor.LimitReached has side-effect of incrementing the count for the key

	destKey := fmt.Sprintf(`%s_%s`, throttler.destinationName, destID)
	userKey := fmt.Sprintf(`%s_%s_%s`, throttler.destinationName, destID, userID)

	var destLevelLimitReached bool
	if throttler.destLimiter.enabled {
		limitStatus, err := throttler.destLimiter.ratelimiter.Check(destKey)
		if err != nil {
			// TODO: handle this
			logger.Errorf(`[[ %s-router-throttler: Error checking limitStatus: %v]]`, throttler.destinationName, err)
		} else {
			destLevelLimitReached = limitStatus.IsLimited
		}
	}

	var userLevelLimitReached bool
	if !destLevelLimitReached && throttler.userLimiter.enabled {
		limitStatus, err := throttler.userLimiter.ratelimiter.Check(userKey)
		if err != nil {
			// TODO: handle this
			logger.Errorf(`[[ %s-router-throttler: Error checking limitStatus: %v]]`, throttler.destinationName, err)
		} else {
			userLevelLimitReached = limitStatus.IsLimited
		}
	}

	limitReached := destLevelLimitReached || userLevelLimitReached

	if !limitReached {
		if throttler.destLimiter.enabled {
			throttler.destLimiter.ratelimiter.Inc(destKey)
		}
		if throttler.userLimiter.enabled {
			throttler.userLimiter.ratelimiter.Inc(userKey)
		}
	}

	return limitReached
}

func (throttler *HandleT) IsEnabled() bool {
	return throttler.destLimiter.enabled || throttler.userLimiter.enabled
}
