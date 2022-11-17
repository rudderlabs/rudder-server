package throttler

import (
	"context"
	"fmt"
	"sync"
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

var clientsPool redisClientsPool

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
func New(destinationID string, opts ...Option) (*Client, error) {
	var c Client
	for _, opt := range opts {
		opt(&c)
	}
	if c.logger == nil {
		c.logger = logger.NewLogger().Child("router").Child("throttler")
	}
	c.destinationID = destinationID

	err := c.readConfiguration()
	if err != nil {
		return nil, err
	}

	if c.settings.enabled {
		if c.settings.rateLimiter, err = c.newLimiter(); err != nil {
			return nil, err
		}
	}

	return &c, nil
}

// CheckLimitReached returns true if we're not allowed to process the number of events we asked for with cost.
// Along with the boolean, it also returns a TokenReturner and an error. The TokenReturner should be called to return
// the tokens to the limiter (bucket) in the eventuality that we did not move forward with the request.
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

func (c *Client) readConfiguration() error {
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
			return fmt.Errorf("throttling algorithm type %q is not compatible with redis", c.algoType)
		}

		client, err := clientsPool.get(context.TODO(), redisAddr, redisUser, redisPassword)
		if err != nil {
			return fmt.Errorf("%s-router-throttler: failed to connect to Redis: %w", c.destinationID, err)
		}
		c.redisClient = client
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

	return nil
}

func (c *Client) newLimiter() (l *throttling.Limiter, err error) {
	switch c.algoType {
	case algoTypeGoRate:
		l, err = throttling.New(throttling.WithGoRate())
	case algoTypeGCRA:
		l, err = throttling.New(throttling.WithGCRA())
	case algoTypeRedisGCRA:
		l, err = throttling.New(throttling.WithGCRA(), throttling.WithRedisClient(c.redisClient))
	case algoTypeRedisSortedSet:
		l, err = throttling.New(throttling.WithRedisClient(c.redisClient))
	default:
		err = fmt.Errorf("unknown throttling algorithm type: %s", c.algoType)
		return
	}
	if err != nil {
		err = fmt.Errorf("failed to create throttling limiter: %v", err)
		return
	}
	return
}

func (c *Client) isEnabled() bool {
	return c.settings.enabled
}

func getWindowInSecs(l limiterSettings) int64 {
	return int64(l.timeWindow.Seconds())
}

type redisClientsPool struct {
	clients map[string]*redis.Client
	mutex   sync.Mutex
}

func (p *redisClientsPool) get(ctx context.Context, addr, user, password string) (*redis.Client, error) {
	var (
		client *redis.Client
		ok     bool
	)

	p.mutex.Lock()
	if p.clients == nil {
		p.clients = make(map[string]*redis.Client)
	} else {
		client, ok = p.clients[addr]
	}
	p.mutex.Unlock() // unlock, no need to keep the lock while connecting to redis
	if ok {
		return client, nil
	}

	c := redis.NewClient(&redis.Options{
		Addr:     addr,
		Username: user,
		Password: password,
	})
	if err := c.Ping(ctx).Err(); err != nil {
		return nil, err
	}

	p.mutex.Lock()
	defer p.mutex.Unlock()
	client, ok = p.clients[addr]
	if ok { // somebody else was faster, return the other client and discard the one we just created
		return client, nil
	}

	p.clients[addr] = c

	return c, nil
}
