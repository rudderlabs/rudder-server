package throttler

import (
	"fmt"
	"time"

	"github.com/coinpaprika/ratelimiter"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/utils/logger"
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
	limit                  int
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

func (throttler *HandleT) setLimits() {
	destName := throttler.destinationName

	// set eventLimit
	throttler.destLimiter.eventLimit = config.GetInt(fmt.Sprintf(`Router.throttler.%s.limit`, destName), destSettingsMap[destName].limit)

	// set timeWindow
	throttler.destLimiter.timeWindow = config.GetDuration(fmt.Sprintf(`Router.throttler.%s.timeWindowInS`, destName), time.Duration(destSettingsMap[destName].timeWindowInS)) * time.Second

	// enable dest throttler
	if throttler.destLimiter.eventLimit != 0 && throttler.destLimiter.timeWindow != 0 {
		logger.Infof(`[[ %s-router-throttler: Enabled throttler with eventLimit:%d, timeWindowInS: %v]]`, throttler.destinationName, throttler.destLimiter.eventLimit, throttler.destLimiter.timeWindow)
		throttler.destLimiter.enabled = true
	}

	// set eventLimit
	throttler.userLimiter.eventLimit = config.GetInt(fmt.Sprintf(`Router.throttler.%s.userLevelLimit`, destName), destSettingsMap[destName].userLevelLimit)

	// set timeWindow
	throttler.userLimiter.timeWindow = config.GetDuration(fmt.Sprintf(`Router.throttler.%s.userLevelTimeWindowInS`, destName), time.Duration(destSettingsMap[destName].userLevelTimeWindowInS)) * time.Second

	// enable dest throttler
	if throttler.userLimiter.eventLimit != 0 && throttler.userLimiter.timeWindow != 0 {
		logger.Infof(`[[ %s-router-throttler: Enabled user level throttler with eventLimit:%d, timeWindowInS: %v]]`, throttler.destinationName, throttler.userLimiter.eventLimit, throttler.userLimiter.timeWindow)
		throttler.userLimiter.enabled = true
	}
}

//SetUp eventLimiter
func (throttler *HandleT) SetUp(destName string) {
	throttler.destinationName = destName
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
