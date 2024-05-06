package testutils

import (
	"context"
	"net"
	"testing"

	"github.com/ory/dockertest/v3"
	"github.com/redis/go-redis/v9"
)

func StartRedis(t *testing.T, redisImage, imageTag string) (string, func()) {
	t.Helper()

	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Fatalf("Failed to start Dockertest: %+v", err)
	}

	resource, err := pool.Run(redisImage, imageTag, nil)
	if err != nil {
		t.Fatalf("Failed to start redis: %+v", err)
	}

	// determine the port the container is listening on
	addr := net.JoinHostPort("localhost", resource.GetPort("6379/tcp"))

	// wait for the container to be ready
	err = pool.Retry(func() error {
		var e error
		client := redis.NewClient(&redis.Options{Addr: addr})
		defer client.Close()

		st, e := client.Ping(context.TODO()).Result()
		t.Logf("Resylt: %+v\n", st)
		return e
	})
	if err != nil {
		t.Fatalf("Failed to ping Redis: %+v", err)
	}

	destroyFunc := func() {
		err := pool.Purge(resource)
		if err != nil {
			t.Fatalf("Failed to purge redis: %s", err.Error())
		}
	}

	return addr, destroyFunc
}
