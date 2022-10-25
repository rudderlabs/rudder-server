package throttler

import (
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/router/throttler/ratelimiter"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

const (
	DestinationLevel = "destination"
	UserLevel        = "user"
	AllLevels        = "all"
)

// Throttler is an interface for throttling functions
type Throttler interface {
	CheckLimitReached(destID, userID string, currentTime time.Time) bool
	Inc(destID, userID string, currentTime time.Time)
	Dec(destID, userID string, count int64, currentTime time.Time, atLevel string)
	IsEnabled() bool
	IsUserLevelEnabled() bool
	IsDestLevelEnabled() bool
}

type limiterSettings struct {
	enabled     bool
	eventLimit  int
	timeWindow  time.Duration
	rateLimiter *ratelimiter.RateLimiter
}

type destinationSettings struct {
	limit               int
	timeWindow          int64
	userLevelLimit      int
	userLevelTimeWindow int64
}

// Client is a Handle for event limiterSettings
type Client struct {
	destinationName string
	destLimiter     *limiterSettings
	userLimiter     *limiterSettings
	logger          logger.Logger
}

// New sets up a new eventLimiter
func New(destName string, opts ...Option) *Client {
	var c Client
	for _, opt := range opts {
		opt.apply(&c)
	}
	if c.logger == nil {
		c.logger = logger.NewLogger().Child("router").Child("throttler")
	}

	c.destinationName = destName
	c.destLimiter = &limiterSettings{}
	c.userLimiter = &limiterSettings{}

	// check if it has throttling config for destination
	c.setLimits()

	if c.destLimiter.enabled {
		dataStore := ratelimiter.NewMapLimitStore(2*c.destLimiter.timeWindow, 10*time.Second)
		c.destLimiter.rateLimiter = ratelimiter.New(dataStore, int64(c.destLimiter.eventLimit), c.destLimiter.timeWindow)
	}

	if c.userLimiter.enabled {
		dataStore := ratelimiter.NewMapLimitStore(2*c.userLimiter.timeWindow, 10*time.Second)
		c.userLimiter.rateLimiter = ratelimiter.New(dataStore, int64(c.userLimiter.eventLimit), c.userLimiter.timeWindow)
	}

	return &c
}

func (c *Client) setLimits() {
	destName := c.destinationName

	// set eventLimit
	config.RegisterIntConfigVariable(
		defaultDestinationSettings[destName].limit, &c.destLimiter.eventLimit, false, 1,
		fmt.Sprintf(`Router.throttler.%s.limit`, destName),
	)

	// set timeWindow
	config.RegisterDurationConfigVariable(
		defaultDestinationSettings[destName].timeWindow, &c.destLimiter.timeWindow, false, time.Second,
		[]string{
			fmt.Sprintf(`Router.throttler.%s.timeWindow`, destName),
			fmt.Sprintf(`Router.throttler.%s.timeWindowInS`, destName),
		}...,
	)

	// enable dest throttler
	if c.destLimiter.eventLimit != 0 && c.destLimiter.timeWindow != 0 {
		c.logger.Infof(
			`[[ %s-router-throttler: Enabled throttler with eventLimit:%d, timeWindowInS: %v]]`,
			c.destinationName, c.destLimiter.eventLimit, c.destLimiter.timeWindow,
		)
		c.destLimiter.enabled = true
	}

	// set eventLimit
	config.RegisterIntConfigVariable(
		defaultDestinationSettings[destName].userLevelLimit, &c.userLimiter.eventLimit, false, 1,
		fmt.Sprintf(`Router.throttler.%s.userLevelLimit`, destName),
	)

	// set timeWindow
	config.RegisterDurationConfigVariable(
		defaultDestinationSettings[destName].userLevelTimeWindow, &c.userLimiter.timeWindow, false, time.Second,
		[]string{
			fmt.Sprintf(`Router.throttler.%s.userLevelTimeWindow`, destName),
			fmt.Sprintf(`Router.throttler.%s.userLevelTimeWindowInS`, destName),
		}...,
	)

	// enable dest throttler
	if c.userLimiter.eventLimit != 0 && c.userLimiter.timeWindow != 0 {
		c.logger.Infof(
			`[[ %s-router-throttler: Enabled user level throttler with eventLimit:%d, timeWindowInS: %v]]`,
			c.destinationName, c.userLimiter.eventLimit, c.userLimiter.timeWindow,
		)
		c.userLimiter.enabled = true
	}
}

// CheckLimitReached returns true if number of events in the rolling window is less than the max events allowed, else false
func (c *Client) CheckLimitReached(destID, userID string, currentTime time.Time) bool {
	var destLevelLimitReached bool
	if c.destLimiter.enabled {
		destKey := c.getDestKey(destID)
		limitStatus, err := c.destLimiter.rateLimiter.Check(destKey, currentTime)
		if err != nil {
			// TODO: handle this
			c.logger.Errorf(`[[ %s-router-throttler: Error checking limitStatus: %v]]`, c.destinationName, err)
		} else {
			destLevelLimitReached = limitStatus.IsLimited
		}
	}

	var userLevelLimitReached bool
	if !destLevelLimitReached && c.userLimiter.enabled {
		userKey := c.getUserKey(destID, userID)
		limitStatus, err := c.userLimiter.rateLimiter.Check(userKey, currentTime)
		if err != nil {
			// TODO: handle this
			c.logger.Errorf(`[[ %s-router-throttler: Error checking limitStatus: %v]]`, c.destinationName, err)
		} else {
			userLevelLimitReached = limitStatus.IsLimited
		}
	}

	return destLevelLimitReached || userLevelLimitReached
}

// Inc increases the destLimiter and userLimiter counters.
// If destID or userID passed is empty, we don't increment the counters.
func (c *Client) Inc(destID, userID string, currentTime time.Time) {
	if c.destLimiter.enabled && destID != "" {
		destKey := c.getDestKey(destID)
		_ = c.destLimiter.rateLimiter.Inc(destKey, currentTime)
	}
	if c.userLimiter.enabled && userID != "" {
		userKey := c.getUserKey(destID, userID)
		_ = c.userLimiter.rateLimiter.Inc(userKey, currentTime)
	}
}

// Dec decrements the destLimiter and userLimiter counters by count passed
// If destID or userID passed is empty, we don't decrement the counters.
func (c *Client) Dec(destID, userID string, count int64, currentTime time.Time, atLevel string) {
	if c.destLimiter.enabled && destID != "" && (atLevel == AllLevels || atLevel == DestinationLevel) {
		destKey := c.getDestKey(destID)
		_ = c.destLimiter.rateLimiter.Dec(destKey, count, currentTime)
	}
	if c.userLimiter.enabled && userID != "" && (atLevel == AllLevels || atLevel == UserLevel) {
		userKey := c.getUserKey(destID, userID)
		_ = c.userLimiter.rateLimiter.Dec(userKey, count, currentTime)
	}
}

func (c *Client) IsEnabled() bool {
	return c.destLimiter.enabled || c.userLimiter.enabled
}

func (c *Client) IsUserLevelEnabled() bool {
	return c.userLimiter.enabled
}

func (c *Client) IsDestLevelEnabled() bool {
	return c.destLimiter.enabled
}

func (c *Client) getDestKey(destID string) string {
	return fmt.Sprintf(`%s_%s`, c.destinationName, destID)
}

func (c *Client) getUserKey(destID, userID string) string {
	return fmt.Sprintf(`%s_%s_%s`, c.destinationName, destID, userID)
}
