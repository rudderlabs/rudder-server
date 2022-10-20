package throttling

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/testhelper/rand"
)

func TestThrottling(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	var (
		ctx      = context.Background()
		rc       = bootstrapRedis(ctx, t, pool)
		limiters = map[string]limiter{
			"go rate":           newLimiter(t, WithGoRate()),
			"gcra":              newLimiter(t, WithGCRA()),
			"gcra redis":        newLimiter(t, WithGCRA(), WithRedisClient(rc)),
			"sorted sets redis": newLimiter(t, WithRedisClient(rc)),
		}
	)

	for _, tc := range []testCase{
		{rate: 1, window: 1, errorMargin: 2},
		{rate: 100, window: 1, errorMargin: 10},
		{rate: 200, window: 3, errorMargin: 10},
	} {
		for name, l := range limiters {
			l := l
			t.Run(testName(name, tc.rate, tc.window), func(t *testing.T) {
				expected := tc.rate
				testLimiter(ctx, t, l, tc.rate, tc.window, expected, tc.errorMargin)
			})
		}
	}
}

func testLimiter(ctx context.Context, t *testing.T, l limiter, rate, window, expected, errorMargin int64) {
	t.Helper()
	var (
		passed int64
		cost   int64 = 1
		key          = rand.UniqueString(10)
		runFor       = time.NewTimer(time.Duration(window) * time.Second)
	)
loop:
	for {
		select {
		case <-runFor.C:
			break loop
		default:
			returner, err := l.Limit(ctx, cost, rate, window, key)
			require.NoError(t, err)
			if returner != nil {
				passed += cost
			}
		}
	}

	diff := expected - passed
	if passed < 1 || diff < (errorMargin*-1) || diff > errorMargin {
		t.Errorf("Expected %d, got %d (diff: %d)", expected, passed, diff)
	}
}

func TestReturn(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	var (
		rate      int64 = 10
		window    int64 = 1
		windowDur       = time.Duration(window) * time.Second
		ctx             = context.Background()
		rc              = bootstrapRedis(ctx, t, pool)
		limiters        = map[string]limiter{
			"go rate":           newLimiter(t, WithGoRate()),
			"sorted sets redis": newLimiter(t, WithRedisClient(rc)),
		}
	)

	for name, l := range limiters {
		l := l
		t.Run(testName(name, rate, window), func(t *testing.T) {
			var (
				passed int
				key    = rand.UniqueString(10)
				tokens []TokenReturner
			)
			for i := int64(0); i < rate*10; i++ {
				returner, err := l.Limit(ctx, 1, rate, window, key)
				require.NoError(t, err)
				if returner != nil {
					passed++
					tokens = append(tokens, returner)
				}
			}

			require.EqualValues(t, rate, passed)

			returner, err := l.Limit(ctx, 1, rate, window, key)
			require.NoError(t, err)
			require.Nil(t, returner, "this request should not have been allowed")

			// return a couple tokens, sometimes returning a single token does not have an effect with go rate
			require.NoError(t, tokens[0].Return(ctx))
			require.NoError(t, tokens[1].Return(ctx))

			require.Eventually(t, func() bool {
				returner, err := l.Limit(ctx, 1, rate, window, key)
				return err == nil && returner != nil
			}, windowDur/2, time.Millisecond)
		})
		break // TODO fix fragile test
	}
}

func TestBadData(t *testing.T) {
	t.Skip("TODO")
}

func TestMultipleRedisClients(t *testing.T) {
	t.Skip("TODO")
}

func testName(name string, rate, window int64) string {
	return fmt.Sprintf("%s/%d tokens per %ds", name, rate, window)
}
