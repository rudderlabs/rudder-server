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

	type limiterSettings struct {
		name    string
		limiter *Limiter
		// concurrency has been introduced because the GCRA algorithms (both in-memory and Redis) tend to lose precision
		// when the concurrency is too high. Until we fix it or come up with a guard to limit the amount of concurrent
		// requests, we're limiting the concurrency to X in the tests (to avoid test flakiness).
		concurrency int
	}

	var (
		ctx      = context.Background()
		rc       = bootstrapRedis(ctx, t, pool)
		limiters = []limiterSettings{
			{
				name:        "gcra",
				limiter:     newLimiter(t, WithInMemoryGCRA(0)),
				concurrency: 100,
			},
			{
				name:        "gcra redis",
				limiter:     newLimiter(t, WithRedisGCRA(rc, 100)), // TODO: this should work properly with burst = 0 as well (i.e. burst = rate)
				concurrency: 100,
			},
			{
				name:        "sorted sets redis",
				limiter:     newLimiter(t, WithRedisSortedSet(rc)),
				concurrency: 5000,
			},
		}
	)

	flakinessRate := 1 // increase to run the tests multiple times in a row to debug flaky tests
	for i := 0; i < flakinessRate; i++ {
		for _, tc := range []testCase{
			// avoid rates that are too small (e.g. 10), that's where there is the most flakiness
			{rate: 500, window: 1},
			{rate: 1000, window: 2},
			{rate: 2000, window: 3},
		} {
			for _, l := range limiters {
				l := l
				t.Run(testName(l.name, tc.rate, tc.window), func(t *testing.T) {
					expected := tc.rate
					testLimiter(ctx, t, l.limiter, tc.rate, tc.window, expected, l.concurrency)
				})
			}
		}
	}
}

func testLimiter(
	ctx context.Context, t *testing.T, l *Limiter, rate, window, expected int64, concurrency int,
) {
	t.Helper()
	var (
		wg          sync.WaitGroup
		passed      int64
		cost        int64 = 1
		key               = rand.UniqueString(10)
		maxRoutines       = make(chan struct{}, concurrency)
		// Time tracking variables
		startTime   int64
		currentTime int64
		timeMutex   sync.Mutex
		stop        = make(chan struct{}, 1)
	)

	run := func() (err error, allowed bool, redisTime time.Duration) {
		switch {
		case l.redisSpeaker != nil && l.useGCRA:
			redisTime, allowed, _, err = l.redisGCRA(ctx, cost, rate, window, key)
		case l.redisSpeaker != nil && !l.useGCRA:
			redisTime, allowed, _, err = l.redisSortedSet(ctx, cost, rate, window, key)
		default:
			allowed, _, err = l.Allow(ctx, cost, rate, window, key)
		}
		return
	}
	if l.useGCRA { // warm up the GCRA algorithms
		for i := 0; i < int(rate); i++ {
			_, _, _ = run()
		}
	}
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

				err, allowed, redisTime := run()
				require.NoError(t, err)
				now := time.Now().UnixNano()

				timeMutex.Lock()
				defer timeMutex.Unlock()
				if redisTime > 0 { // Redis limiters have a getTime() function that returns the current time from Redis perspective
					if redisTime > time.Duration(currentTime) {
						currentTime = int64(redisTime)
					}
				} else if now > currentTime { // in-memory algorithm, let's use time.Now()
					currentTime = now
				}
				if startTime == 0 {
					startTime = currentTime
				}
				if currentTime-startTime > window*int64(time.Second) {
					select {
					case stop <- struct{}{}:
					default: // one signal to stop is enough, don't block
					}
					return // do not increment "passed" because we're over the window
				}
				if allowed {
					atomic.AddInt64(&passed, cost)
				}
			}()
		}
	}

	wg.Wait()

	diff := expected - passed
	errorMargin := int64(0.05*float64(rate)) + 1 // ~5% error margin
	if passed < 1 || diff < -errorMargin || diff > errorMargin {
		t.Errorf("Expected %d, got %d (diff: %d, error margin: %d)", expected, passed, diff, errorMargin)
	}
}

func TestGCRABurstAsRate(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	var (
		ctx = context.Background()
		rc  = bootstrapRedis(ctx, t, pool)
		l   = newLimiter(t, WithRedisGCRA(rc, 0))
		// Configuration variables
		key          = "foo"
		cost   int64 = 1
		rate   int64 = 500
		window int64 = 1
		// Expectations
		passed int64
		start  = time.Now()
	)

	for i := int64(0); i < rate*2; i++ {
		allowed, _, err := l.Allow(ctx, cost, rate, window, key)
		require.NoError(t, err)
		if allowed {
			passed++
		}
	}

	require.GreaterOrEqual(t, passed, rate)
	require.Less(
		t, time.Since(start), time.Duration(window*int64(time.Second)),
		"we should've been able to make the required request in less than the window duration due to the burst setting",
	)
}

func TestReturn(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	type testCase struct {
		name    string
		limiter *Limiter
	}

	var (
		rate      int64 = 10
		window    int64 = 1
		windowDur       = time.Duration(window) * time.Second
		ctx             = context.Background()
		rc              = bootstrapRedis(ctx, t, pool)
		testCases       = []testCase{
			{
				name:    "sorted sets redis",
				limiter: newLimiter(t, WithRedisSortedSet(rc)),
			},
		}
	)

	for _, tc := range testCases {
		t.Run(testName(tc.name, rate, window), func(t *testing.T) {
			var (
				passed int
				key    = rand.UniqueString(10)
				tokens []func(context.Context) error
			)
			for i := int64(0); i < rate*10; i++ {
				allowed, returner, err := tc.limiter.Allow(ctx, 1, rate, window, key)
				require.NoError(t, err)
				if allowed {
					passed++
					tokens = append(tokens, returner)
				}
			}

			require.EqualValues(t, rate, passed)

			allowed, _, err := tc.limiter.Allow(ctx, 1, rate, window, key)
			require.NoError(t, err)
			require.False(t, allowed)

			// return one token and try again
			require.NoError(t, tokens[0](ctx))
			require.Eventually(t, func() bool {
				allowed, returner, err := tc.limiter.Allow(ctx, 1, rate, window, key)
				return allowed && err == nil && returner != nil
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
		limiters = map[string]*Limiter{
			"gcra":              newLimiter(t, WithInMemoryGCRA(0)),
			"gcra redis":        newLimiter(t, WithRedisGCRA(rc, 0)),
			"sorted sets redis": newLimiter(t, WithRedisSortedSet(rc)),
		}
	)

	for name, l := range limiters {
		t.Run(name+" cost", func(t *testing.T) {
			allowed, ret, err := l.Allow(ctx, 0, 10, 1, "foo")
			require.False(t, allowed)
			require.Nil(t, ret)
			require.Error(t, err, "cost must be greater than 0")
		})
		t.Run(name+" rate", func(t *testing.T) {
			allowed, ret, err := l.Allow(ctx, 1, 0, 1, "foo")
			require.False(t, allowed)
			require.Nil(t, ret)
			require.Error(t, err, "rate must be greater than 0")
		})
		t.Run(name+" window", func(t *testing.T) {
			allowed, ret, err := l.Allow(ctx, 1, 10, 0, "foo")
			require.False(t, allowed)
			require.Nil(t, ret)
			require.Error(t, err, "window must be greater than 0")
		})
		t.Run(name+" key", func(t *testing.T) {
			allowed, ret, err := l.Allow(ctx, 1, 10, 1, "")
			require.False(t, allowed)
			require.Nil(t, ret)
			require.Error(t, err, "key must not be empty")
		})
	}
}

func testName(name string, rate, window int64) string {
	return fmt.Sprintf("%s/%d tokens per %ds", name, rate, window)
}
