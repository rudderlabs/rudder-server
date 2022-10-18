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
		{rate: 1, window: 1, errorMargin: 1},
		{rate: 2, window: 2, errorMargin: 1},
		{rate: 100, window: 1, errorMargin: 5},
		{rate: 100, window: 3, errorMargin: 5},
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

func TestThrottlingReturn(t *testing.T) {
	t.Skip("TODO")
}

func TestThrottlingBadData(t *testing.T) {
	t.Skip("TODO")
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
			time.Sleep(time.Millisecond)
		}
	}

	diff := expected - passed
	if passed < 1 || diff < (errorMargin*-1) || diff > errorMargin {
		t.Errorf("Expected %d, got %d (diff: %d)", expected, passed, diff)
	}
}

func testName(name string, rate, window int64) string {
	return fmt.Sprintf("%s/%d tokens per %ds", name, rate, window)
}
