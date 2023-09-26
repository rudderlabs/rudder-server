package resource

import (
	"context"
	_ "encoding/json"
	"fmt"

	"github.com/go-redis/redis/v8"
	_ "github.com/lib/pq"
	"github.com/ory/dockertest/v3"
)

// WithRedisTag is used to specify a custom tag that is used when pulling the Redis image from the container registry
func WithRedisTag(tag string) RedisOption {
	return func(c *redisConfig) {
		c.tag = tag
	}
}

// WithRedisCmdArg is used to specify the save argument when running the container.
func WithRedisCmdArg(key, value string) RedisOption {
	return func(c *redisConfig) {
		c.cmdArgs = append(c.cmdArgs, key, value)
	}
}

// WithRedisEnv is used to pass environment variables to the container.
func WithRedisEnv(envs ...string) RedisOption {
	return func(c *redisConfig) {
		c.envs = envs
	}
}

type RedisResource struct {
	Addr string
}

type RedisOption func(*redisConfig)

type redisConfig struct {
	tag     string
	envs    []string
	cmdArgs []string
}

func SetupRedis(ctx context.Context, pool *dockertest.Pool, d cleaner, opts ...RedisOption) (*RedisResource, error) {
	conf := redisConfig{
		tag: "6.2-alpine",
	}
	for _, opt := range opts {
		opt(&conf)
	}
	runOptions := &dockertest.RunOptions{
		Repository: "redis",
		Tag:        conf.tag,
		Env:        conf.envs,
		Cmd:        []string{"redis-server"},
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
