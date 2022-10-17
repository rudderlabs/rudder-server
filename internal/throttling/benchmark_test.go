package throttling

import (
	"context"
	"strconv"
	"testing"

	"github.com/go-redis/redis/v9"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/testhelper/destination"
	"github.com/rudderlabs/rudder-server/testhelper/rand"
)

/*
BenchmarkInMemoryLimiters/go_rate-24         	 6978067	       164.0 ns/op
BenchmarkInMemoryLimiters/gcra-24            	10721114	       110.8 ns/op
BenchmarkInMemoryLimiters/sorted_set-24      	 4254432	       281.0 ns/op
*/
func BenchmarkInMemoryLimiters(b *testing.B) {
	var (
		ctx          = context.Background()
		rate   int64 = 100
		window int64 = 10
	)

	rateLimiter := InMemoryLimiter{
		gcra:      &gcra{},
		sortedSet: &sortedSet{},
		goRate:    &goRate{},
	}

	b.Run("go rate", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = rateLimiter.goRateLimit(ctx, 1, rate, window, "some-key")
		}
	})

	b.Run("gcra", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = rateLimiter.gcraLimit(ctx, 1, rate, window, "some-key")
		}
	})

	b.Run("sorted set", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = rateLimiter.sortedSetLimit(ctx, 1, rate, window, "some-key")
		}
	})
}

func BenchmarkRedisSortedSetRemover(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool, err := dockertest.NewPool("")
	require.NoError(b, err)

	prepare := func(b *testing.B) (*redis.Client, string, []redis.Z) {
		rc := bootstrapRedis(ctx, b, pool)

		key := rand.UniqueString(10)
		members := make([]redis.Z, b.N*3)
		for i := range members {
			members[i] = redis.Z{
				Score:  float64(i),
				Member: strconv.Itoa(i),
			}
		}
		_, err := rc.ZAdd(ctx, key, members...).Result()
		require.NoError(b, err)

		count, err := rc.ZCard(ctx, key).Result()
		require.NoError(b, err)
		require.EqualValues(b, b.N*3, count)

		return rc, key, members
	}

	b.Run("sortedSetZRemReturn", func(b *testing.B) {
		rc, key, members := prepare(b)
		rem := func(members ...string) *sortedSetZRemReturn {
			return &sortedSetZRemReturn{
				key:     key,
				remover: rc,
				members: members,
			}
		}

		b.ResetTimer()
		for i, j := 0, 0; i < b.N; i, j = i+1, j+3 {
			err = rem( // check error only once at the end to avoid altering benchmark results
				members[j].Member.(string),
				members[j+1].Member.(string),
				members[j+2].Member.(string),
			).Return(ctx)
		}

		require.NoError(b, err)

		b.StopTimer()
		count, err := rc.ZCard(ctx, key).Result()
		require.NoError(b, err)
		require.EqualValues(b, 0, count)
	})
}

type tester interface {
	Helper()
	Log(...interface{})
	Errorf(format string, args ...interface{})
	Fatalf(format string, args ...any)
	FailNow()
	Cleanup(f func())
}

func bootstrapRedis(
	ctx context.Context, t tester, pool *dockertest.Pool, opts ...destination.RedisOption,
) *redis.Client {
	t.Helper()
	redisContainer, err := destination.SetupRedis(ctx, pool, t, opts...)
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
