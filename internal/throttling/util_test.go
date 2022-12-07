package throttling

import (
	"context"

	"github.com/go-redis/redis/v8"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/testhelper/destination"
)

type tester interface {
	Helper()
	Log(...interface{})
	Errorf(format string, args ...interface{})
	Fatalf(format string, args ...any)
	FailNow()
	Cleanup(f func())
}

type testCase struct {
	rate,
	window int64
}

func newLimiter(t tester, opts ...Option) *Limiter {
	t.Helper()
	l, err := New(opts...)
	require.NoError(t, err)
	return l
}

func bootstrapRedis(ctx context.Context, t tester, pool *dockertest.Pool) *redis.Client {
	t.Helper()
	redisContainer, err := destination.SetupRedis(ctx, pool, t)
	require.NoError(t, err)

	rc := redis.NewClient(&redis.Options{
		Network: "tcp",
		Addr:    redisContainer.Addr,
	})
	t.Cleanup(func() { _ = rc.Close() })

	pong, err := rc.Ping(ctx).Result()
	if err != nil {
		t.Fatalf("Could not ping Redis cluster: %v", err)
	}

	require.Equal(t, "PONG", pong)

	return rc
}
