package throttler

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v9"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/internal/throttling"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

const (
	DestinationLevel = "destination"
	UserLevel        = "user"
	AllLevels        = "all" // TODO

	algoTypeGoRate         = "gorate"
	algoTypeGCRA           = "gcra"
	algoTypeRedisGCRA      = "redis-gcra"
	algoTypeRedisSortedSet = "redis-sorted-set"
)

type limiter interface {
	// Limit should return true if the request can be done or false if the limit is reached.
	Limit(ctx context.Context, cost, rate, window int64, key string) (
		allowed bool, ret throttling.TokenReturner, err error,
	)
}

type limiterSettings struct {
	enabled     bool
	eventLimit  int64
	timeWindow  time.Duration
	rateLimiter limiter
}

type destinationSettings struct {
	limit               int64
	timeWindow          int64
	userLevelLimit      int64
	userLevelTimeWindow int64
}

// Client is a Handle for event limiterSettings
type Client struct {
	algoType        string
	redisClient     *redis.Client
	destinationName string
	destLimiter     limiterSettings
	userLimiter     limiterSettings
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
	c.readConfiguration()

	if c.destLimiter.enabled {
		c.destLimiter.rateLimiter = c.newLimiter() // TODO use c.destLimiter.eventLimit & c.destLimiter.timeWindow
	}
	if c.userLimiter.enabled {
		c.userLimiter.rateLimiter = c.newLimiter() // TODO use c.userLimiter.eventLimit & c.userLimiter.timeWindow
	}

	return &c
}

func (c *Client) readConfiguration() {
	destName := c.destinationName

	// set algo type
	config.RegisterStringConfigVariable(
		algoTypeGoRate, &c.algoType, false, fmt.Sprintf(`Router.throttler.%s.algoType`, destName),
	)

	// set redis configuration
	var redisAddr, redisUser, redisPassword string
	config.RegisterStringConfigVariable("", &redisAddr, false, "Router.throttler.redisAddr")
	config.RegisterStringConfigVariable("", &redisUser, false, "Router.throttler.redisUser")
	config.RegisterStringConfigVariable("", &redisPassword, false, "Router.throttler.redisPassword")
	if redisAddr != "" {
		switch c.algoType {
		case algoTypeRedisGCRA, algoTypeRedisSortedSet:
		default:
			panic(fmt.Errorf("throttling algorithm type %q is not compatible with redis", c.algoType))
		}

		opts := redis.Options{Addr: redisAddr}
		if redisUser != "" {
			opts.Username = redisUser
		}
		if redisPassword != "" {
			opts.Password = redisPassword
		}
		c.redisClient = redis.NewClient(&opts)

		if err := c.redisClient.Ping(context.TODO()).Err(); err != nil {
			panic(fmt.Errorf("%s-router-throttler: failed to connect to Redis: %w", destName, err))
		}
	}

	// set eventLimit
	config.RegisterInt64ConfigVariable(
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
	config.RegisterInt64ConfigVariable(
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

func (c *Client) newLimiter() *throttling.Limiter {
	var (
		err     error
		limiter *throttling.Limiter
	)
	switch c.algoType {
	case algoTypeGoRate:
		limiter, err = throttling.New(throttling.WithGoRate())
	case algoTypeGCRA:
		limiter, err = throttling.New(throttling.WithGCRA())
	case algoTypeRedisGCRA:
		limiter, err = throttling.New(throttling.WithGCRA(), throttling.WithRedisClient(c.redisClient))
	case algoTypeRedisSortedSet:
		limiter, err = throttling.New(throttling.WithRedisClient(c.redisClient))
	default:
		panic(fmt.Errorf("unknown throttling algorithm type: %s", c.algoType))
	}
	if err != nil {
		panic(fmt.Errorf("failed to create throttling limiter: %v", err))
	}
	return limiter
}

// CheckLimitReached returns true if number of events in the rolling window is less than the max events allowed, else false
func (c *Client) CheckLimitReached(destID, userID string, cost int64) (
	limited bool, tr throttling.TokenReturner, retErr error,
) {
	var (
		ctx = context.TODO()
		mtr multiTokenReturner
	)

	if c.destLimiter.enabled {
		destKey := c.getDestKey(destID)
		allowed, tr, err := c.destLimiter.rateLimiter.Limit(
			ctx, cost, c.destLimiter.eventLimit, getWindowInSecs(c.destLimiter), destKey,
		)
		if err != nil {
			err = fmt.Errorf(`[[ %s-router-throttler: Error checking limitStatus: %w]]`, c.destinationName, err)
			c.logger.Error(err)
			return false, nil, err
		}
		if !allowed {
			return true, nil, nil // no token to return, so we don't return trs here
		}
		mtr.add(tr)
	}

	if c.userLimiter.enabled {
		userKey := c.getUserKey(destID, userID)
		allowed, tr, err := c.userLimiter.rateLimiter.Limit(
			ctx, cost, c.userLimiter.eventLimit, getWindowInSecs(c.userLimiter), userKey,
		)
		if err != nil {
			err = fmt.Errorf(`[[ %s-router-throttler: Error checking limitStatus: %w]]`, c.destinationName, err)
			c.logger.Error(err)
			return false, nil, err
		}
		if !allowed {
			// limit reached, in case we had requested a token for dest level throttling, we need to return it
			_ = mtr.Return(ctx)
			return true, nil, nil
		}
		mtr.add(tr)
	}

	return false, &mtr, nil
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

func getWindowInSecs(l limiterSettings) int64 {
	return int64(l.timeWindow.Seconds())
}

type multiTokenReturner struct {
	trs []throttling.TokenReturner
}

func (m *multiTokenReturner) add(tr throttling.TokenReturner) {
	m.trs = append(m.trs, tr)
}

func (m *multiTokenReturner) Return(ctx context.Context) (retErr error) {
	for _, tr := range m.trs {
		if err := tr.Return(ctx); retErr == nil && err != nil {
			retErr = err
		}
	}
	return
}
