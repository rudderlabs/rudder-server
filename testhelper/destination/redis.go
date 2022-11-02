package destination

import (
	"context"
	_ "encoding/json"
	"fmt"

	"github.com/go-redis/redis/v9"
	_ "github.com/lib/pq"
	"github.com/ory/dockertest/v3"
)

// WithRedisCmdArg is used to specify the save argument when running the container.
func WithRedisCmdArg(key, value string) RedisOption {
	return withRedisOption{setup: func(c *redisConfig) {
		c.cmdArgs = append(c.cmdArgs, key, value)
	}}
}

// WithRedisEnv is used to pass environment variables to the container.
func WithRedisEnv(envs ...string) RedisOption {
	return withRedisOption{setup: func(c *redisConfig) {
		c.envs = envs
	}}
}

type RedisResource struct {
	Addr string
}

type RedisOption interface {
	apply(*redisConfig)
}

type withRedisOption struct{ setup func(*redisConfig) }

func (w withRedisOption) apply(c *redisConfig) { w.setup(c) }

type redisConfig struct {
	envs    []string
	cmdArgs []string
}

func SetupRedis(ctx context.Context, pool *dockertest.Pool, d cleaner, opts ...RedisOption) (*RedisResource, error) {
	conf := redisConfig{}
	for _, opt := range opts {
		opt.apply(&conf)
	}
	runOptions := &dockertest.RunOptions{
		Repository: "redis", Tag: "7.0.5-alpine3.16",
		Env: conf.envs,
		Cmd: []string{"redis-server"},
	}
	if len(conf.cmdArgs) > 0 {
		runOptions.Cmd = append(runOptions.Cmd, conf.cmdArgs...)
	}

	// pulls a redis image, creates a container based on it and runs it
	redisContainer, err := pool.RunWithOptions(runOptions)
	if err != nil {
		return nil, err
	}
	d.Cleanup(func() {
		if err := pool.Purge(redisContainer); err != nil {
			d.Log("Could not purge resource:", err)
		}
	})
	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	redisAddress := fmt.Sprintf("localhost:%s", redisContainer.GetPort("6379/tcp"))
	err = pool.Retry(func() error {
		redisClient := redis.NewClient(&redis.Options{
			Addr: redisAddress,
		})
		_, err := redisClient.Ping(ctx).Result()
		return err
	})
	if err != nil {
		return nil, err
	}
	return &RedisResource{Addr: redisAddress}, nil
}
