package destination

import (
	_ "encoding/json"
	"fmt"
	"log"

	"github.com/go-redis/redis"
	_ "github.com/lib/pq"
	"github.com/ory/dockertest/v3"
)

type RedisResource struct {
	RedisAddress string
}

func SetupRedis(pool *dockertest.Pool, d deferer) (*RedisResource, error) {
	// pulls an redis image, creates a container based on it and runs it
	redisContainer, err := pool.Run("redis", "alpine3.14", []string{"requirepass=secret"})
	if err != nil {
		return nil, err
	}
	d.Defer(func() error {
		if err := pool.Purge(redisContainer); err != nil {
			log.Printf("Could not purge resource: %s \n", err)
		}
		return nil
	})
	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	redisAddress := fmt.Sprintf("localhost:%s", redisContainer.GetPort("6379/tcp"))
	if err := pool.Retry(func() error {
		redisClient := redis.NewClient(&redis.Options{
			Addr:     redisAddress,
			Password: "",
			DB:       0,
		})
		_, err := redisClient.Ping().Result()
		return err
	}); err != nil {
		return nil, err
	}
	return &RedisResource{
		RedisAddress: redisAddress,
	}, nil
}
