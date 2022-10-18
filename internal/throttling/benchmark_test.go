package throttling

import (
	"context"
	"strconv"
	"testing"

	"github.com/go-redis/redis/v9"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/testhelper/rand"
)

/*
goos: linux, goarch: amd64
cpu: 12th Gen Intel(R) Core(TM) i9-12900K
BenchmarkLimiters/go_rate-24				9535816			121.0 ns/op
BenchmarkLimiters/gcra-24					16373722		74.00 ns/op
BenchmarkLimiters/gcra_redis-24				50640			20674 ns/op
BenchmarkLimiters/sorted_sets_redis-24		59044			20475 ns/op
*/
func BenchmarkLimiters(b *testing.B) {
	pool, err := dockertest.NewPool("")
	require.NoError(b, err)

	var (
		rate     int64 = 10
		window   int64 = 1
		ctx            = context.Background()
		rc             = bootstrapRedis(ctx, b, pool)
		limiters       = map[string]limiter{
			"go rate":           newLimiter(b, WithGoRate()),
			"gcra":              newLimiter(b, WithGCRA()),
			"gcra redis":        newLimiter(b, WithGCRA(), WithRedisClient(rc)),
			"sorted sets redis": newLimiter(b, WithRedisClient(rc)),
		}
	)

	for name, l := range limiters {
		l := l
		b.Run(name, func(b *testing.B) {
			key := rand.UniqueString(10)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = l.Limit(ctx, 1, rate, window, key)
			}
		})
	}
}

/*
goos: linux, goarch: amd64
cpu: 12th Gen Intel(R) Core(TM) i9-12900K
BenchmarkRedisSortedSetRemover/sortedSetRedisReturn-24		74870		14740 ns/op
*/
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

	b.Run("sortedSetRedisReturn", func(b *testing.B) {
		rc, key, members := prepare(b)
		rem := func(members ...string) *sortedSetRedisReturn {
			return &sortedSetRedisReturn{
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
