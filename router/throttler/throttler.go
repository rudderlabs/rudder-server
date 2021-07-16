package throttler

import (
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/router/throttler/ratelimiter"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

const (
	DESTINATION_LEVEL = "destination"
	USER_LEVEL        = "user"
	ALL_LEVELS        = "all"
)

//Throttler is an interface for throttling functions
type Throttler interface {
	CheckLimitReached(destID string, userID string, currentTime time.Time) bool
	Inc(destID string, userID string, currentTime time.Time)
	Dec(destID string, userID string, count int64, currentTime time.Time, atLevel string)
	IsEnabled() bool
	IsUserLevelEnabled() bool
	IsDestLevelEnabled() bool
}

type Limiter struct {
	enabled     bool
	eventLimit  int
	timeWindow  time.Duration
	ratelimiter *ratelimiter.RateLimiter
}

type Settings struct {
	limit               int
	timeWindow          int
	userLevelLimit      int
	userLevelTimeWindow int
}

//HandleT is a Handle for event limiter
type HandleT struct {
	destinationName string
	destLimiter     *Limiter
	userLimiter     *Limiter
}

var pkgLogger logger.LoggerI

func (throttler *HandleT) setLimits() {
	destName := throttler.destinationName

	// set eventLimit
	config.RegisterIntConfigVariable(destSettingsMap[destName].limit, &throttler.destLimiter.eventLimit, false, 1, fmt.Sprintf(`Router.throttler.%s.limit`, destName))

	// set timeWindow
	config.RegisterDurationConfigVariable(time.Duration(destSettingsMap[destName].timeWindow), &throttler.destLimiter.timeWindow, false, time.Second, []string{fmt.Sprintf(`Router.throttler.%s.timeWindow`, destName), fmt.Sprintf(`Router.throttler.%s.timeWindowInS`, destName)}...)

	// enable dest throttler
	if throttler.destLimiter.eventLimit != 0 && throttler.destLimiter.timeWindow != 0 {
		pkgLogger.Infof(`[[ %s-router-throttler: Enabled throttler with eventLimit:%d, timeWindowInS: %v]]`, throttler.destinationName, throttler.destLimiter.eventLimit, throttler.destLimiter.timeWindow)
		throttler.destLimiter.enabled = true
	}

	// set eventLimit
	config.RegisterIntConfigVariable(destSettingsMap[destName].userLevelLimit, &throttler.userLimiter.eventLimit, false, 1, fmt.Sprintf(`Router.throttler.%s.userLevelLimit`, destName))

	// set timeWindow
	config.RegisterDurationConfigVariable(time.Duration(destSettingsMap[destName].userLevelTimeWindow), &throttler.userLimiter.timeWindow, false, time.Second, []string{fmt.Sprintf(`Router.throttler.%s.userLevelTimeWindow`, destName), fmt.Sprintf(`Router.throttler.%s.userLevelTimeWindowInS`, destName)}...)

	// enable dest throttler
	if throttler.userLimiter.eventLimit != 0 && throttler.userLimiter.timeWindow != 0 {
		pkgLogger.Infof(`[[ %s-router-throttler: Enabled user level throttler with eventLimit:%d, timeWindowInS: %v]]`, throttler.destinationName, throttler.userLimiter.eventLimit, throttler.userLimiter.timeWindow)
		throttler.userLimiter.enabled = true
	}
}

//SetUp eventLimiter
func (throttler *HandleT) SetUp(destName string) {
	pkgLogger = logger.NewLogger().Child("router").Child("throttler")
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
func (throttler *HandleT) CheckLimitReached(destID string, userID string, currentTime time.Time) bool {
	var destLevelLimitReached bool
	if throttler.destLimiter.enabled {
		destKey := throttler.getDestKey(destID)
		limitStatus, err := throttler.destLimiter.ratelimiter.Check(destKey, currentTime)
		if err != nil {
			// TODO: handle this
			pkgLogger.Errorf(`[[ %s-router-throttler: Error checking limitStatus: %v]]`, throttler.destinationName, err)
		} else {
			destLevelLimitReached = limitStatus.IsLimited
		}
	}

	var userLevelLimitReached bool
	if !destLevelLimitReached && throttler.userLimiter.enabled {
		userKey := throttler.getUserKey(destID, userID)
		limitStatus, err := throttler.userLimiter.ratelimiter.Check(userKey, currentTime)
		if err != nil {
			// TODO: handle this
			pkgLogger.Errorf(`[[ %s-router-throttler: Error checking limitStatus: %v]]`, throttler.destinationName, err)
		} else {
			userLevelLimitReached = limitStatus.IsLimited
		}
	}

	return destLevelLimitReached || userLevelLimitReached
}

//Inc increases the destLimiter and userLimiter counters.
//If destID or userID passed is empty, we don't increment the counters.
func (throttler *HandleT) Inc(destID string, userID string, currentTime time.Time) {
	if throttler.destLimiter.enabled && destID != "" {
		destKey := throttler.getDestKey(destID)
		throttler.destLimiter.ratelimiter.Inc(destKey, currentTime)
	}
	if throttler.userLimiter.enabled && userID != "" {
		userKey := throttler.getUserKey(destID, userID)
		throttler.userLimiter.ratelimiter.Inc(userKey, currentTime)
	}
}

//Dec decrements the destLimiter and userLimiter counters by count passed
//If destID or userID passed is empty, we don't decrement the counters.
func (throttler *HandleT) Dec(destID string, userID string, count int64, currentTime time.Time, atLevel string) {
	if throttler.destLimiter.enabled && destID != "" && (atLevel == ALL_LEVELS || atLevel == DESTINATION_LEVEL) {
		destKey := throttler.getDestKey(destID)
		throttler.destLimiter.ratelimiter.Dec(destKey, count, currentTime)
	}
	if throttler.userLimiter.enabled && userID != "" && (atLevel == ALL_LEVELS || atLevel == USER_LEVEL) {
		userKey := throttler.getUserKey(destID, userID)
		throttler.userLimiter.ratelimiter.Dec(userKey, count, currentTime)
	}
}

func (throttler *HandleT) IsEnabled() bool {
	return throttler.destLimiter.enabled || throttler.userLimiter.enabled
}

func (throttler *HandleT) IsUserLevelEnabled() bool {
	return throttler.userLimiter.enabled
}

func (throttler *HandleT) IsDestLevelEnabled() bool {
	return throttler.destLimiter.enabled
}

func (throttler *HandleT) getDestKey(destID string) string {
	return fmt.Sprintf(`%s_%s`, throttler.destinationName, destID)
}

func (throttler *HandleT) getUserKey(destID, userID string) string {
	return fmt.Sprintf(`%s_%s_%s`, throttler.destinationName, destID, userID)
}
