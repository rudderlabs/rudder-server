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
* BenchmarkInMemoryLimiters/go_rate-24         	 7424371	       162.7 ns/op
* BenchmarkInMemoryLimiters/gcra-24            	 9857386	       121.5 ns/op
* BenchmarkInMemoryLimiters/sorted_set-24      	 4144581	       287.3 ns/op
 */
func BenchmarkInMemoryLimiters(b *testing.B) {
	var (
		ctx          = context.Background()
		rate   int64 = 100
		window int64 = 10
	)

	rateLimiter := InMemoryLimiter{
		gcra:      &gcra{getterSetter: &inMemoryGetterSetter{}},
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
		rc := bootstrapBenchmark(ctx, b, pool)

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

func bootstrapBenchmark(
	ctx context.Context, b *testing.B, pool *dockertest.Pool, opts ...destination.RedisOption,
) *redis.Client {
	b.Helper()
	redisContainer, err := destination.SetupRedis(ctx, pool, b, opts...)
	require.NoError(b, err)

	rc := redis.NewClient(&redis.Options{
		Network: "tcp",
		Addr:    redisContainer.Addr,
	})
	b.Cleanup(func() { _ = rc.Close() })

	pong, err := rc.Ping(ctx).Result()
	if err != nil {
		b.Fatalf("Could not ping Redis cluster: %v", err)
	}

	require.Equal(b, "PONG", pong)

	return rc
}
