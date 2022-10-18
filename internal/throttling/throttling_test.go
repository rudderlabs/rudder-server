package throttling

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/testhelper/rand"
)

func TestThrottling(t *testing.T) {
	var (
		ctx      = context.Background()
		limiters = map[string]limiter{
			"gcra":        newLimiter(t, WithGCRA()),
			"sorted sets": newLimiter(t, WithInMemorySortedSets()),
		}
	)

	for _, tc := range []testCase{
		{rate: 1, window: 1, expected: 1},
		{rate: 2, window: 2, expected: 2},
		{rate: 100, window: 1, expected: 100},
		{rate: 100, window: 3, expected: 100},
	} {
		for name, l := range limiters {
			l := l
			t.Run(testName(name, tc.rate, tc.window), func(t *testing.T) {
				testLimiter(ctx, t, l, tc.rate, tc.window, tc.expected, tc.errorMargin)
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
			time.Sleep(time.Millisecond)
		}
	}
	// expected +1 because of burst which is the initial number of tokens in the bucket
	diff := expected + 1 - passed
	if diff < (errorMargin*-1) || diff > errorMargin {
		t.Errorf("Expected %d, got %d (diff: %d)", expected, passed, diff)
	}
}

func newLimiter(t *testing.T, opts ...Option) limiter {
	t.Helper()
	l, err := New(opts...)
	require.NoError(t, err)
	return l
}

func testName(name string, rate, window int64) string {
	return fmt.Sprintf("%s/%d tokens per %ds", name, rate, window)
}

type limiter interface {
	Limit(ctx context.Context, cost, rate, window int64, key string) (TokenReturner, error)
}

type testCase struct {
	name string
	rate,
	window,
	expected,
	errorMargin int64
}
