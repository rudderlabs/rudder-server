package throttling

import (
	"context"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
)

func TestRedisGCRA(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	var (
		delta = 50 * time.Millisecond
		rc    = bootstrapRedis(ctx, t, pool)
		rl    = &RedisLimiter{
			scripter:         rc,
			sortedSetRemover: rc,
		}
	)

	for _, tc := range []testCase{
		{name: "1 token each 1s", rate: 1, window: 1, expected: 1},
		{name: "2 tokens each 2s", rate: 2, window: 2, expected: 2},
		{name: "100 tokens each 1s", rate: 100, window: 1, expected: 100, errorMargin: 2},
		{name: "100 tokens each 3s", rate: 100, window: 3, expected: 100, errorMargin: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var (
				passed  int64
				endTest = time.NewTimer(time.Duration(tc.window)*time.Second + delta)
			)
		loop:
			for {
				select {
				case <-endTest.C:
					break loop
				default:
					cost := int64(1)
					returner, err := rl.Limit(ctx, cost, tc.rate, tc.window, tc.name)
					require.NoError(t, err)
					if returner != nil {
						passed += cost
					}
					time.Sleep(time.Millisecond)
				}
			}
			// expected +1 because of burst which is the initial number of tokens in the bucket
			require.InDeltaf(
				t, tc.expected+1, passed, float64(tc.errorMargin),
				"Expected %d, got %d", tc.expected, passed,
			)
		})
	}
}
