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

func BenchmarkSortedSetRemovers(b *testing.B) {
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
			return &sortedSetZRemReturn{sortedSetReturn: sortedSetReturn{
				key:     key,
				remover: rc,
				members: members,
			}}
		}

		b.ResetTimer()
		for i, j := 0, 0; i < b.N; i, j = i+1, j+3 {
			err = rem(
				members[j].Member.(string),
				members[j+1].Member.(string),
				members[j+2].Member.(string),
			).Return(ctx)
			require.NoError(b, err)
		}

		b.StopTimer()
		count, err := rc.ZCard(ctx, key).Result()
		require.NoError(b, err)
		require.EqualValues(b, 0, count)
	})

	//b.Run("sortedSetZRemReturn", func(b *testing.B) {
	//	rc, key, members := prepare(b)
	//	rem := func(members ...string) *sortedSetZRemRangeByLexReturn {
	//		return &sortedSetZRemRangeByLexReturn{sortedSetReturn: sortedSetReturn{
	//			key:     key,
	//			remover: rc,
	//			members: members,
	//		}}
	//	}
	//
	//	b.ResetTimer()
	//	for i, j := 0, 0; i < b.N; i, j = i+1, j+3 {
	//		err = rem(
	//			members[j].Member.(string),
	//			members[j+1].Member.(string),
	//			members[j+2].Member.(string),
	//		).Return(ctx)
	//		require.NoError(b, err)
	//	}
	//
	//	b.StopTimer()
	//	count, err := rc.ZCard(ctx, key).Result()
	//	require.NoError(b, err)
	//	require.EqualValues(b, 0, count)
	//})

	b.Run("sortedSetZRemReturn", func(b *testing.B) {
		rc := bootstrapBenchmark(ctx, b, pool)

		n := 4
		key := rand.UniqueString(10)
		members := make([]redis.Z, n*3)
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
		require.EqualValues(b, n*3, count)

		rem := func(members ...string) *sortedSetZRemRangeByLexReturn {
			b.Log("TO REMOVE:", members)
			return &sortedSetZRemRangeByLexReturn{sortedSetReturn: sortedSetReturn{
				key:     key,
				remover: rc,
				members: members,
			}}
		}

		b.ResetTimer()
		for i, j := 0, 0; i < n; i, j = i+1, j+3 {
			toRemove := []string{
				members[j].Member.(string),
				members[j+1].Member.(string),
				members[j+2].Member.(string),
			}
			err = rem(toRemove...).Return(ctx)
			require.NoError(b, err)

			all, err := rc.ZRange(ctx, key, 0, -1).Result()
			require.NoError(b, err)
			b.Log("ALL:", all)
		}

		b.StopTimer()
		count, err = rc.ZCard(ctx, key).Result()
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
