package throttler

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/internal/throttling"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

const (
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
	limit      int64
	timeWindow int64
}

// Client is a Handle for event limiterSettings
type Client struct {
	algoType      string
	redisClient   *redis.Client
	destinationID string
	settings      limiterSettings
	logger        logger.Logger
}

// New sets up a new eventLimiter
func New(destinationID string, opts ...Option) *Client {
	var c Client
	for _, opt := range opts {
		opt(&c)
	}
	if c.logger == nil {
		c.logger = logger.NewLogger().Child("router").Child("throttler")
	}

	c.destinationID = destinationID
	c.readConfiguration()

	if c.settings.enabled {
		c.settings.rateLimiter = c.newLimiter()
	}

	return &c
}

// CheckLimitReached returns true if we're not allowed to process the number of events we asked for with cost
func (c *Client) CheckLimitReached(cost int64) (limited bool, tr throttling.TokenReturner, retErr error) {
	if !c.isEnabled() {
		return false, nil, nil
	}

	ctx := context.TODO()
	rateLimitingKey := c.destinationID
	allowed, tr, err := c.settings.rateLimiter.Limit(
		ctx, cost, c.settings.eventLimit, getWindowInSecs(c.settings), rateLimitingKey,
	)
	if err != nil {
		err = fmt.Errorf(`[[ %s-router-throttler: Error checking limitStatus: %w]]`, c.destinationID, err)
		c.logger.Error(err)
		return false, nil, err
	}
	if !allowed {
		return true, nil, nil // no token to return when limited
	}
	return false, tr, nil
}

func (c *Client) readConfiguration() {
	// set algo type
	config.RegisterStringConfigVariable(
		algoTypeGoRate, &c.algoType, false, fmt.Sprintf(`Router.throttler.%s.algoType`, c.destinationID),
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
			panic(fmt.Errorf("%s-router-throttler: failed to connect to Redis: %w", c.destinationID, err))
		}
	}

	// set eventLimit
	config.RegisterInt64ConfigVariable(
		defaultDestinationSettings[c.destinationID].limit, &c.settings.eventLimit, false, 1,
		fmt.Sprintf(`Router.throttler.%s.limit`, c.destinationID),
	)

	// set timeWindow
	config.RegisterDurationConfigVariable(
		defaultDestinationSettings[c.destinationID].timeWindow, &c.settings.timeWindow, false, time.Second,
		[]string{
			fmt.Sprintf(`Router.throttler.%s.timeWindow`, c.destinationID),
			fmt.Sprintf(`Router.throttler.%s.timeWindowInS`, c.destinationID),
		}...,
	)

	// enable dest throttler
	if c.settings.eventLimit != 0 && c.settings.timeWindow != 0 {
		c.logger.Infof(
			`[[ %s-router-throttler: Enabled throttler with eventLimit:%d, timeWindowInS: %v]]`,
			c.destinationID, c.settings.eventLimit, c.settings.timeWindow,
		)
		c.settings.enabled = true
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

func (c *Client) isEnabled() bool {
	return c.settings.enabled
}

func getWindowInSecs(l limiterSettings) int64 {
	return int64(l.timeWindow.Seconds())
}
