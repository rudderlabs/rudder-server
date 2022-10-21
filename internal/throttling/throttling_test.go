package throttling

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
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
		{rate: 10, window: 1, errorMargin: 2},
		{rate: 100, window: 2, errorMargin: 5},
		{rate: 200, window: 3, errorMargin: 5},
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
		wg          sync.WaitGroup
		passed      int64
		cost        int64 = 1
		key               = rand.UniqueString(10)
		maxRoutines       = make(chan struct{}, 5000)
		// Time tracking variables
		startTime   int64
		currentTime int64
		timeMutex   sync.Mutex
		stop        = make(chan struct{}, 1)
	)
loop:
	for {
		select {
		case <-stop:
			// To decrease the error margin (mostly introduced for the Redis algorithms) I'm measuring time in two
			// different ways depending on whether I'm using an in-memory algorithm or a Redis one.
			// For the Redis algorithms I measure the elapsed time by keeping track of the timestamps returned by
			// the Redis Lua scripts.
			// This is because there is a bit of a drift between the time as we measure it here and the time as it
			// is measured in the Lua scripts.
			break loop
		case maxRoutines <- struct{}{}:
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() { <-maxRoutines }()

				returner, err := l.Limit(ctx, cost, rate, window, key)
				require.NoError(t, err)

				timer, ok := returner.(interface{ getTime() time.Duration })
				timeMutex.Lock()
				defer timeMutex.Unlock()
				if ok { // Redis limiters have a getTime() function that returns the current time from Redis perspective
					if timer.getTime() > time.Duration(currentTime) {
						currentTime = int64(timer.getTime())
					}
				} else if now := time.Now().UnixNano(); now > currentTime { // in-memory algorithm, let's use time.Now()
					currentTime = now
				}
				if startTime == 0 {
					startTime = currentTime
				}
				if currentTime-startTime >= window*int64(time.Second) {
					select {
					case stop <- struct{}{}:
					default: // one signal to stop is enough, don't block
					}
					return
				}
				if returner != nil {
					atomic.AddInt64(&passed, cost)
				}
			}()
		}
	}

	wg.Wait()

	diff := expected - passed
	if passed < 1 || diff < -errorMargin || diff > errorMargin {
		t.Errorf("Expected %d, got %d (diff: %d)", expected, passed, diff)
	}
}

func TestReturn(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	type testCase struct {
		name         string
		limiter      limiter
		minDeletions int
	}

	var (
		rate      int64 = 10
		window    int64 = 1
		windowDur       = time.Duration(window) * time.Second
		ctx             = context.Background()
		rc              = bootstrapRedis(ctx, t, pool)
		testCases       = []testCase{
			{
				name:         "go rate",
				limiter:      newLimiter(t, WithGoRate()),
				minDeletions: 2,
			},
			{
				name:         "sorted sets redis",
				limiter:      newLimiter(t, WithRedisClient(rc)),
				minDeletions: 1,
			},
		}
	)

	for _, tc := range testCases {
		t.Run(testName(tc.name, rate, window), func(t *testing.T) {
			var (
				passed int
				key    = rand.UniqueString(10)
				tokens []TokenReturner
			)
			for i := int64(0); i < rate*10; i++ {
				returner, err := tc.limiter.Limit(ctx, 1, rate, window, key)
				require.NoError(t, err)
				if returner != nil {
					passed++
					tokens = append(tokens, returner)
				}
			}

			require.EqualValues(t, rate, passed)

			returner, err := tc.limiter.Limit(ctx, 1, rate, window, key)
			require.NoError(t, err)
			require.Nil(t, returner, "this request should not have been allowed")

			// Return as many tokens as minDeletions.
			// This is because returning a single token very quickly does not always have an effect with go rate.
			// The reason why is that the go rate algorithm calculates the number of tokens to be restored
			// based on a few criteria:
			// 1. https://cs.opensource.google/go/x/time/+/refs/tags/v0.1.0:rate/rate.go;l=175
			// 2. https://cs.opensource.google/go/x/time/+/refs/tags/v0.1.0:rate/rate.go;l=183
			for i := 0; i < tc.minDeletions; i++ {
				require.NoError(t, tokens[i].Return(ctx))
			}

			require.Eventually(t, func() bool {
				returner, err := tc.limiter.Limit(ctx, 1, rate, window, key)
				return err == nil && returner != nil
			}, windowDur/2, time.Millisecond)
		})
	}
}

func TestBadData(t *testing.T) {
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

	for name, l := range limiters {
		t.Run(name+" cost", func(t *testing.T) {
			ret, err := l.Limit(ctx, 0, 10, 1, "foo")
			require.Nil(t, ret)
			require.Error(t, err, "cost must be greater than 0")
		})
		t.Run(name+" rate", func(t *testing.T) {
			ret, err := l.Limit(ctx, 1, 0, 1, "foo")
			require.Nil(t, ret)
			require.Error(t, err, "rate must be greater than 0")
		})
		t.Run(name+" window", func(t *testing.T) {
			ret, err := l.Limit(ctx, 1, 10, 0, "foo")
			require.Nil(t, ret)
			require.Error(t, err, "window must be greater than 0")
		})
		t.Run(name+" key", func(t *testing.T) {
			ret, err := l.Limit(ctx, 1, 10, 1, "")
			require.Nil(t, ret)
			require.Error(t, err, "key must not be empty")
		})
	}
}

func TestMultipleRedisClients(t *testing.T) {
	t.Skip("TODO")
}

func testName(name string, rate, window int64) string {
	return fmt.Sprintf("%s/%d tokens per %ds", name, rate, window)
}
