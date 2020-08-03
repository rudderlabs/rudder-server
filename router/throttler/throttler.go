package throttler

import (
	"fmt"
	"time"

	"github.com/EagleChen/restrictor"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

var (
	parameters map[string]map[string]interface{}
)

//Throttler is an interface for throttling functions
type Throttler interface {
	LimitReached(key string) bool
	Enabled() bool
}

//HandleT is a Handle for event limiter
type HandleT struct {
	UserEventOrderingRequired bool
	permit                    bool
	restrictor                restrictor.Restrictor
	destination               string
	eventLimit                int
	timeWindow                time.Duration
	bucketsInWindow           int
	userLevelThrottling       bool
	userLevelThrottler        *UserHandleT
}

//UserHandleT is a handle for user level event limiter
type UserHandleT struct {
	permit          bool
	restrictor      restrictor.Restrictor
	destination     string
	eventLimit      int
	timeWindow      time.Duration
	bucketsInWindow int
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
		throttler.permit = true
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
			throttler.userLevelThrottler.permit = true
		}
	}

	// TOOD: should buckets be configurable
	throttler.bucketsInWindow = config.GetInt(fmt.Sprintf(`Router.throttle.%s.bucketsInWindow`, throttler.destination), 12)
	throttler.userLevelThrottler.bucketsInWindow = config.GetInt(fmt.Sprintf(`Router.throttle.%s.bucketsInWindow`, throttler.destination), 12)
}

//SetUp eventLimiter
func (throttler *HandleT) SetUp(destination string) {
	var userLevelThrottler UserHandleT
	throttler.userLevelThrottler = &userLevelThrottler

	throttler.destination = destination

	// check if it has throttling config for destination
	throttler.setLimits()

	if throttler.permit {
		store, err := restrictor.NewMemoryStore()
		if err != nil {
			logger.Errorf("[ROUTER]: Creating throttler memory store failed: %v", err)
		}

		throttler.restrictor = restrictor.NewRestrictor(throttler.timeWindow, uint32(throttler.eventLimit), uint32(throttler.bucketsInWindow), store)
		throttler.permit = true
	}

	if throttler.userLevelThrottler.permit {
		userStore, err := restrictor.NewMemoryStore()
		if err != nil {
			logger.Errorf("[ROUTER]: Creating user level throttler memory store failed: %v", err)
		}

		throttler.userLevelThrottler.restrictor = restrictor.NewRestrictor(throttler.userLevelThrottler.timeWindow, uint32(throttler.userLevelThrottler.eventLimit), uint32(throttler.userLevelThrottler.bucketsInWindow), userStore)
	}
}

//LimitReached returns true if number of events in the rolling window is less than the max events allowed, else false
func (throttler *HandleT) LimitReached(destID string, userID string) bool {
	// do not call LimitReached in single if statement even though both throttlers are enabled
	// as restrictor.LimitReached has side-effect of incrementing the count for the key

	var destLevelLimitReached bool
	if throttler.permit {
		key := fmt.Sprintf(`%s_%s`, throttler.destination, destID)
		destLevelLimitReached = throttler.restrictor.LimitReached(key)
	}

	var userLevelLimitReached bool
	if throttler.userLevelThrottler.permit {
		key := fmt.Sprintf(`%s_%s_%s`, throttler.destination, destID, userID)
		userLevelLimitReached = throttler.userLevelThrottler.restrictor.LimitReached(key)
	}

	return destLevelLimitReached || userLevelLimitReached
}

func (throttler *HandleT) Enabled() bool {
	return throttler.permit || throttler.userLevelThrottler.permit
}
