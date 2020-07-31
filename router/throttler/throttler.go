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
}

//HandleT is a Handle for event limiter
type HandleT struct {
	Enabled                   bool
	UserEventOrderingRequired bool
	restrictor                restrictor.Restrictor
	destination               string
	eventLimit                int
	timeWindow                time.Duration
	bucketsInWindow           int
	userLevelThrottling       bool
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
	}
}

func (throttler *HandleT) setLimits() {
	defultLimit, _ := parameters[throttler.destination]["limit"].(int)
	throttler.eventLimit = config.GetInt(fmt.Sprintf(`Router.throttle.%s.limit`, throttler.destination), defultLimit)

	defaultTimeWindow, _ := parameters[throttler.destination]["timeWindowInS"].(int)
	throttler.timeWindow = config.GetDuration(fmt.Sprintf(`Router.throttle.%s.timeWindowInS`, throttler.destination), time.Duration(defaultTimeWindow)) * time.Second

	defautOrderingRequired, ok := parameters[throttler.destination]["userEventOrderingRequired"].(bool)
	if !ok {
		defautOrderingRequired = true
	}
	throttler.UserEventOrderingRequired = config.GetBool(fmt.Sprintf(`Router.throttle.%s.userEventOrderingRequired`, throttler.destination), defautOrderingRequired)

	defautUserLevelThrottling, ok := parameters[throttler.destination]["userEventOrderingRequired"].(bool)
	throttler.userLevelThrottling = config.GetBool(fmt.Sprintf(`Router.throttle.%s.userLevelThrottling`, throttler.destination), defautUserLevelThrottling)

	// TOOD: should buckets be configurable
	throttler.bucketsInWindow = config.GetInt(fmt.Sprintf(`Router.throttle.%s.bucketsInWindow`, throttler.destination), 12)
}

//SetUp eventLimiter
func (throttler *HandleT) SetUp(destination string) {
	throttler.destination = destination

	// check if it has throttling config for destination
	throttler.setLimits()

	if throttler.eventLimit == 0 || throttler.timeWindow == 0 {
		return
	}

	store, err := restrictor.NewMemoryStore()
	if err != nil {
		logger.Errorf("[ROUTER]: Creating throttler memory store failed: %v", err)
	}

	throttler.restrictor = restrictor.NewRestrictor(throttler.timeWindow, uint32(throttler.eventLimit), uint32(throttler.bucketsInWindow), store)
	throttler.Enabled = true
}

//LimitReached returns true if number of events in the rolling window is less than the max events allowed, else false
func (throttler *HandleT) LimitReached(key string) bool {
	return throttler.restrictor.LimitReached(key)
}

func (throttler *HandleT) Key(destID string, userID string) string {
	key := fmt.Sprintf(`%s_%s`, throttler.destination, destID)
	// rate limit at user level for destinations that do at user level eg. amplitude
	// https://help.amplitude.com/hc/en-us/articles/360032842391-HTTP-API-V2#post__batch-responses
	if throttler.userLevelThrottling {
		key += fmt.Sprintf(`_%s`, userID)
	}
	return key
}
