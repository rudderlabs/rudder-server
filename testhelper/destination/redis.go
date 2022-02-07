package main_test

import (
	_ "encoding/json"
	"fmt"
	_ "github.com/Shopify/sarama"
	"github.com/go-redis/redis"
	_ "github.com/lib/pq"
	"github.com/ory/dockertest"
	"log"
)

type RedisDesTest struct {
	redisAddress string
	redisClient  *redis.Client
}

func SetRedis(pool *dockertest.Pool) (string, *dockertest.Resource) {
	RedisTest := &RedisDesTest{}
	// pulls an redis image, creates a container based on it and runs it
	resourceRedis, err := pool.Run("redis", "alpine3.14", []string{"requirepass=secret"})
	if err != nil {
		log.Printf("Could not start resource: %s", err)
	}
	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	RedisTest.redisAddress = fmt.Sprintf("localhost:%s", resourceRedis.GetPort("6379/tcp"))
	if err := pool.Retry(func() error {
		RedisTest.redisClient = redis.NewClient(&redis.Options{
			Addr:     RedisTest.redisAddress,
			Password: "",
			DB:       0,
		})
		_, err := RedisTest.redisClient.Ping().Result()
		return err
	}); err != nil {
		log.Printf("Could not connect to docker: %s", err)
	}
	return RedisTest.redisAddress, resourceRedis
}
