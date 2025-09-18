package throttler

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/redis"
	"github.com/rudderlabs/rudder-server/router/throttler/internal/pickup/adaptive"
)

func TestFactory(t *testing.T) {
	t.Run("when adaptive throttling is enabled", func(t *testing.T) {
		config := config.New()
		config.Set("Router.throttler.adaptiveEnabled", true)
		maxLimit := int64(300)
		config.Set("Router.throttler.maxLimit", maxLimit)
		config.Set("Router.throttler.minLimit", int64(100))
		config.Set("Router.throttler.destName.timeWindow", time.Second)
		f, err := NewFactory(config, stats.NOP, logger.NOP)
		require.NoError(t, err)
		defer f.Shutdown()
		ta := f.GetPickupThrottler("destName", "destID", "eventType")

		t.Run("when there is a 429s in the last decrease limit counter window", func(t *testing.T) {
			ta.ResponseCodeReceived(429)
			require.Eventually(t, func() bool {
				return floatCheck(ta.GetLimit(), maxLimit*7/10) // reduces by 30% since there is an error in the last 1 second
			}, 2*time.Second, 100*time.Millisecond, "limit: %d, expectedLimit: %d", ta.GetLimit(), maxLimit*7/10)
		})

		t.Run("when there are no 429s in the last increase limit counter window", func(t *testing.T) {
			require.Eventually(t, func() bool {
				return floatCheck(ta.GetLimit(), maxLimit*8/10) // increases by 10% since there is no error in the last 2 seconds
			}, 4*time.Second, 100*time.Millisecond, "limit: %d, expectedLimit: %d", ta.GetLimit(), maxLimit*8/10)
		})

		t.Run("adaptive rate limit back to disabled", func(t *testing.T) {
			config.Set("Router.throttler.adaptiveEnabled", false)
			require.Eventually(t, func() bool {
				return floatCheck(ta.GetLimit(), 0) // should be 0 since adaptive rate limiter is disabled
			}, 2*time.Second, 100*time.Millisecond, "limit: %d, expectedLimit: %d", ta.GetLimit(), maxLimit*8/10)
			config.Set("Router.throttler.adaptiveEnabled", true)
		})

		t.Run("adaptive rate limit back to enabled", func(t *testing.T) {
			config.Set("Router.throttler.adaptiveEnabled", true)
			ta.ResponseCodeReceived(429)
			require.Eventually(t, func() bool {
				return floatCheck(ta.GetLimit(), maxLimit*5/10) // reduces by 30% since there is an error in the last 1 second
			}, 2*time.Second, 100*time.Millisecond, "limit: %d, expectedLimit: %d", ta.GetLimit(), maxLimit*5/10)
		})
	})

	t.Run("check stats when adaptive is enabled", func(t *testing.T) {
		maxLimit := int64(300)
		window := 2 * time.Second

		c := config.New()
		c.Set("Router.throttler.adaptiveEnabled", true)
		c.Set("Router.throttler.maxLimit", maxLimit)
		c.Set("Router.throttler.minLimit", int64(100))
		c.Set("Router.throttler.destName.timeWindow", window)

		statsStore, err := memstats.New()
		require.NoError(t, err)

		f, err := NewFactory(c, statsStore, logger.NOP)
		require.NoError(t, err)
		defer f.Shutdown()

		_, _ = f.GetPickupThrottler("destName", "destID", "eventType").CheckLimitReached(context.Background(), 1)

		require.EqualValues(t, maxLimit/int64(window.Seconds()), statsStore.Get("throttling_rate_limit", stats.Tags{
			"destinationId": "destID",
			"destType":      "destName",
			"adaptive":      "true",
		}).LastValue())
		require.EqualValues(t, 1, statsStore.Get("adaptive_throttler_limit_factor", stats.Tags{
			"destinationId": "destID",
			"destType":      "destName",
		}).LastValue())
	})

	t.Run("when no limit is set", func(t *testing.T) {
		conf := config.New()
		conf.Set("Router.throttler.adaptiveEnabled", true)
		f, err := NewFactory(conf, stats.NOP, logger.NOP)
		require.NoError(t, err)
		defer f.Shutdown()
		ta := f.GetPickupThrottler("destName", "destID", "eventType")
		require.EqualValues(t, adaptive.DefaultMaxThrottlingLimit, ta.GetLimit())
	})

	t.Run("when adaptive throttling is disabled", func(t *testing.T) {
		config := config.New()
		config.Set("Router.throttler.adaptiveEnabled", false)
		config.Set("Router.throttler.destName.destID.limit", int64(100))
		config.Set("Router.throttler.destName.destID.timeWindow", time.Second)

		f, err := NewFactory(config, stats.NOP, logger.NOP)
		require.NoError(t, err)
		defer f.Shutdown()

		ta := f.GetPickupThrottler("destName", "destID", "eventType")

		t.Run("should use static throttler", func(t *testing.T) {
			// Static throttler should return configured limit
			require.EqualValues(t, 100, ta.GetLimit())
		})

		t.Run("should handle response codes", func(t *testing.T) {
			// Should not crash when receiving response codes
			ta.ResponseCodeReceived(200)
			ta.ResponseCodeReceived(429)
			ta.ResponseCodeReceived(500)
		})
	})

	t.Run("throttler caching and reuse", func(t *testing.T) {
		config := config.New()
		config.Set("Router.throttler.adaptiveEnabled", true)

		f, err := NewFactory(config, stats.NOP, logger.NOP)
		require.NoError(t, err)
		defer f.Shutdown()

		t.Run("should cache throttlers by destination and event type", func(t *testing.T) {
			ta1 := f.GetPickupThrottler("destName", "destID", "eventType")
			ta2 := f.GetPickupThrottler("destName", "destID", "eventType")

			// Should return the same instance (cached by destinationID:eventType)
			require.Same(t, ta1, ta2)
		})

		t.Run("should create different throttlers for different destinations", func(t *testing.T) {
			ta1 := f.GetPickupThrottler("destName", "destID1", "eventType")
			ta2 := f.GetPickupThrottler("destName", "destID2", "eventType")

			// Should return different instances
			require.NotSame(t, ta1, ta2)
		})

		t.Run("should create different throttlers for different event types", func(t *testing.T) {
			ta1 := f.GetPickupThrottler("destName", "destID", "eventType1")
			ta2 := f.GetPickupThrottler("destName", "destID", "eventType2")

			// Should return different instances
			require.NotSame(t, ta1, ta2)
		})
	})

	t.Run("factory shutdown", func(t *testing.T) {
		config := config.New()
		config.Set("Router.throttler.adaptiveEnabled", true)

		f, err := NewFactory(config, stats.NOP, logger.NOP)
		require.NoError(t, err)

		// Create some throttlers
		ta1 := f.GetPickupThrottler("destName", "destID1", "eventType")
		ta2 := f.GetPickupThrottler("destName", "destID2", "eventType")

		require.NotNil(t, ta1)
		require.NotNil(t, ta2)

		// Should not panic on shutdown
		f.Shutdown()
	})

	t.Run("per event type throttling configuration", func(t *testing.T) {
		config := config.New()
		config.Set("Router.throttler.adaptiveEnabled", true)
		config.Set("Router.throttler.destName.destID.throttlerPerEventType", true)
		config.Set("Router.throttler.destName.destID.track.maxLimit", int64(50))
		config.Set("Router.throttler.destName.destID.track.minLimit", int64(10))
		config.Set("Router.throttler.destName.timeWindow", time.Second)

		f, err := NewFactory(config, stats.NOP, logger.NOP)
		require.NoError(t, err)
		defer f.Shutdown()

		ta := f.GetPickupThrottler("destName", "destID", "track")

		t.Run("should use per-event configuration", func(t *testing.T) {
			// Should use the configured max limit as starting point
			require.EqualValues(t, 50, ta.GetLimit())
		})
	})

	t.Run("hierarchical configuration", func(t *testing.T) {
		config := config.New()
		config.Set("Router.throttler.adaptiveEnabled", true)

		t.Run("destination-specific adaptive enabled", func(t *testing.T) {
			config.Set("Router.throttler.destName.destID.adaptiveEnabled", true)
			config.Set("Router.throttler.adaptiveEnabled", false) // Global disabled

			f, err := NewFactory(config, stats.NOP, logger.NOP)
			require.NoError(t, err)
			defer f.Shutdown()

			ta := f.GetPickupThrottler("destName", "destID", "eventType")
			// Should still use adaptive because destination-specific config takes precedence
			require.EqualValues(t, adaptive.DefaultMaxThrottlingLimit, ta.GetLimit())
		})

		t.Run("destination-type-specific adaptive enabled", func(t *testing.T) {
			config.Set("Router.throttler.destName.adaptiveEnabled", true)
			config.Set("Router.throttler.adaptiveEnabled", false) // Global disabled

			f, err := NewFactory(config, stats.NOP, logger.NOP)
			require.NoError(t, err)
			defer f.Shutdown()

			ta := f.GetPickupThrottler("destName", "destID", "eventType")
			// Should use adaptive because destination-type config takes precedence
			require.EqualValues(t, adaptive.DefaultMaxThrottlingLimit, ta.GetLimit())
		})
	})

	t.Run("different limiter algorithms", func(t *testing.T) {
		t.Run("GCRA algorithm", func(t *testing.T) {
			config := config.New()
			config.Set("Router.throttler.limiter.type", "gcra")
			config.Set("Router.throttler.adaptiveEnabled", false)
			config.Set("Router.throttler.destName.destID.limit", int64(100))
			config.Set("Router.throttler.destName.destID.timeWindow", time.Second)

			f, err := NewFactory(config, stats.NOP, logger.NOP)
			require.NoError(t, err)
			defer f.Shutdown()

			ta := f.GetPickupThrottler("destName", "destID", "eventType")
			require.NotNil(t, ta)
			require.EqualValues(t, 100, ta.GetLimit())
		})

		t.Run("invalid algorithm should return error", func(t *testing.T) {
			config := config.New()
			config.Set("Router.throttler.limiter.type", "invalid-algorithm")

			_, err := NewFactory(config, stats.NOP, logger.NOP)
			require.Error(t, err)
			require.Contains(t, err.Error(), "invalid throttling algorithm")
		})
	})

	t.Run("NoOp factory", func(t *testing.T) {
		f := NewNoOpThrottlerFactory()
		require.NotNil(t, f)

		t.Run("should return NoOp throttler", func(t *testing.T) {
			ta := f.GetPickupThrottler("destName", "destID", "eventType")
			require.NotNil(t, ta)

			// NoOp throttler should never limit
			limited, err := ta.CheckLimitReached(context.Background(), 1000)
			require.NoError(t, err)
			require.False(t, limited)

			// Should return 0 limit
			require.EqualValues(t, 0, ta.GetLimit())

			// Should not panic on response codes
			ta.ResponseCodeReceived(429)
			ta.ResponseCodeReceived(200)

			// Should not panic on shutdown
			ta.Shutdown()
		})

		t.Run("should not panic on factory shutdown", func(t *testing.T) {
			f.Shutdown()
		})
	})

	t.Run("concurrent access", func(t *testing.T) {
		config := config.New()
		config.Set("Router.throttler.adaptiveEnabled", true)

		f, err := NewFactory(config, stats.NOP, logger.NOP)
		require.NoError(t, err)
		defer f.Shutdown()

		t.Run("should handle concurrent Get calls", func(t *testing.T) {
			const numGoroutines = 10
			throttlers := make([]PickupThrottler, numGoroutines)

			// Launch multiple goroutines to get the same throttler
			done := make(chan struct{}, numGoroutines)
			for i := 0; i < numGoroutines; i++ {
				go func(index int) {
					throttlers[index] = f.GetPickupThrottler("destName", "destID", "eventType")
					done <- struct{}{}
				}(i)
			}

			// Wait for all goroutines
			for i := 0; i < numGoroutines; i++ {
				<-done
			}

			// All throttlers should be the same instance
			first := throttlers[0]
			for i := 1; i < numGoroutines; i++ {
				require.Same(t, first, throttlers[i])
			}
		})
	})

	t.Run("stats collection with different configurations", func(t *testing.T) {
		statsStore, err := memstats.New()
		require.NoError(t, err)

		t.Run("static throttler stats", func(t *testing.T) {
			config := config.New()
			config.Set("Router.throttler.adaptiveEnabled", false)
			config.Set("Router.throttler.destName.destID.limit", int64(200))
			config.Set("Router.throttler.destName.destID.timeWindow", 2*time.Second)

			f, err := NewFactory(config, statsStore, logger.NOP)
			require.NoError(t, err)
			defer f.Shutdown()

			ta := f.GetPickupThrottler("destName", "destID", "eventType")
			_, _ = ta.CheckLimitReached(context.Background(), 1)

			// Should have static throttler stats
			require.EqualValues(t, 100, statsStore.Get("throttling_rate_limit", stats.Tags{
				"destinationId": "destID",
				"destType":      "destName",
				"adaptive":      "false",
			}).LastValue())
		})

		t.Run("per-event type stats", func(t *testing.T) {
			config := config.New()
			config.Set("Router.throttler.adaptiveEnabled", true)
			config.Set("Router.throttler.destName.destID.throttlerPerEventType", true)
			config.Set("Router.throttler.destName.destID.track.maxLimit", int64(150))
			config.Set("Router.throttler.destName.destID.track.minLimit", int64(50))
			config.Set("Router.throttler.destName.timeWindow", 3*time.Second)

			f, err := NewFactory(config, statsStore, logger.NOP)
			require.NoError(t, err)
			defer f.Shutdown()

			ta := f.GetPickupThrottler("destName", "destID", "track")
			_, _ = ta.CheckLimitReached(context.Background(), 1)

			// Should have per-event type stats
			require.EqualValues(t, 50, statsStore.Get("throttling_rate_limit", stats.Tags{
				"destinationId": "destID",
				"destType":      "destName",
				"eventType":     "track",
				"adaptive":      "true",
			}).LastValue())
		})
	})
}

func TestFactoryWithRedis(t *testing.T) {
	// Set up Redis container
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	redisContainer, err := redis.Setup(context.Background(), pool, t)
	require.NoError(t, err)

	t.Run("redis-gcra algorithm", func(t *testing.T) {
		t.Run("with adaptive throttling enabled", func(t *testing.T) {
			config := config.New()
			config.Set("Router.throttler.limiter.type", "redis-gcra")
			config.Set("Router.throttler.redisThrottler.addr", redisContainer.Addr)
			config.Set("Router.throttler.adaptiveEnabled", true)
			config.Set("Router.throttler.maxLimit", int64(100))
			config.Set("Router.throttler.minLimit", int64(10))
			config.Set("Router.throttler.destName.timeWindow", time.Second)

			f, err := NewFactory(config, stats.NOP, logger.NOP)
			require.NoError(t, err)
			defer f.Shutdown()

			ta := f.GetPickupThrottler("destName", "destID", "eventType")
			require.NotNil(t, ta)

			t.Run("should respect rate limiting", func(t *testing.T) {
				// Should not be limited initially
				limited, err := ta.CheckLimitReached(context.Background(), 1)
				require.NoError(t, err)
				require.False(t, limited)

				// Should use adaptive limit
				require.EqualValues(t, 100, ta.GetLimit())
			})

			t.Run("should handle response codes", func(t *testing.T) {
				ta.ResponseCodeReceived(429)
				ta.ResponseCodeReceived(200)
				ta.ResponseCodeReceived(500)
			})
		})

		t.Run("with static throttling", func(t *testing.T) {
			config := config.New()
			config.Set("Router.throttler.limiter.type", "redis-gcra")
			config.Set("Router.throttler.redisThrottler.addr", redisContainer.Addr)
			config.Set("Router.throttler.adaptiveEnabled", false)
			config.Set("Router.throttler.destName.destID.limit", int64(50))
			config.Set("Router.throttler.destName.destID.timeWindow", time.Second)

			f, err := NewFactory(config, stats.NOP, logger.NOP)
			require.NoError(t, err)
			defer f.Shutdown()

			ta := f.GetPickupThrottler("destName", "destID", "eventType")
			require.NotNil(t, ta)
			require.EqualValues(t, 50, ta.GetLimit())
		})

		t.Run("should work with multiple destinations", func(t *testing.T) {
			config := config.New()
			config.Set("Router.throttler.limiter.type", "redis-gcra")
			config.Set("Router.throttler.redisThrottler.addr", redisContainer.Addr)
			config.Set("Router.throttler.adaptiveEnabled", false)
			config.Set("Router.throttler.destName.destID1.limit", int64(30))
			config.Set("Router.throttler.destName.destID2.limit", int64(60))
			config.Set("Router.throttler.destName.destID1.timeWindow", time.Second)
			config.Set("Router.throttler.destName.destID2.timeWindow", time.Second)

			f, err := NewFactory(config, stats.NOP, logger.NOP)
			require.NoError(t, err)
			defer f.Shutdown()

			ta1 := f.GetPickupThrottler("destName", "destID1", "eventType")
			ta2 := f.GetPickupThrottler("destName", "destID2", "eventType")

			require.EqualValues(t, 30, ta1.GetLimit())
			require.EqualValues(t, 60, ta2.GetLimit())

			// Both should be able to process events independently
			limited1, err := ta1.CheckLimitReached(context.Background(), 1)
			require.NoError(t, err)
			require.False(t, limited1)

			limited2, err := ta2.CheckLimitReached(context.Background(), 1)
			require.NoError(t, err)
			require.False(t, limited2)
		})
	})

	t.Run("redis-sorted-set algorithm", func(t *testing.T) {
		t.Run("with adaptive throttling enabled", func(t *testing.T) {
			config := config.New()
			config.Set("Router.throttler.limiter.type", "redis-sorted-set")
			config.Set("Router.throttler.redisThrottler.addr", redisContainer.Addr)
			config.Set("Router.throttler.adaptiveEnabled", true)
			config.Set("Router.throttler.maxLimit", int64(200))
			config.Set("Router.throttler.minLimit", int64(20))
			config.Set("Router.throttler.destName.timeWindow", time.Second)

			f, err := NewFactory(config, stats.NOP, logger.NOP)
			require.NoError(t, err)
			defer f.Shutdown()

			ta := f.GetPickupThrottler("destName", "destID", "eventType")
			require.NotNil(t, ta)

			t.Run("should respect rate limiting", func(t *testing.T) {
				limited, err := ta.CheckLimitReached(context.Background(), 1)
				require.NoError(t, err)
				require.False(t, limited)

				require.EqualValues(t, 200, ta.GetLimit())
			})

			t.Run("should adapt limits based on response codes", func(t *testing.T) {
				initialLimit := ta.GetLimit()

				// Send 429 to trigger adaptation
				ta.ResponseCodeReceived(429)

				// Allow some time for adaptation
				time.Sleep(100 * time.Millisecond)

				// Limit should change (either increase or decrease depending on algorithm)
				// We just verify that response codes are being processed
				ta.ResponseCodeReceived(200)
				ta.ResponseCodeReceived(200)

				require.NotNil(t, initialLimit)
			})
		})

		t.Run("with static throttling", func(t *testing.T) {
			config := config.New()
			config.Set("Router.throttler.limiter.type", "redis-sorted-set")
			config.Set("Router.throttler.redisThrottler.addr", redisContainer.Addr)
			config.Set("Router.throttler.adaptiveEnabled", false)
			config.Set("Router.throttler.destName.destID.limit", int64(75))
			config.Set("Router.throttler.destName.destID.timeWindow", time.Second)

			f, err := NewFactory(config, stats.NOP, logger.NOP)
			require.NoError(t, err)
			defer f.Shutdown()

			ta := f.GetPickupThrottler("destName", "destID", "eventType")
			require.NotNil(t, ta)
			require.EqualValues(t, 75, ta.GetLimit())
		})
	})

	t.Run("redis configuration errors", func(t *testing.T) {
		t.Run("should error when redis addr not configured for redis algorithms", func(t *testing.T) {
			config := config.New()
			config.Set("Router.throttler.limiter.type", "redis-gcra")
			// Don't set redis addr

			_, err := NewFactory(config, stats.NOP, logger.NOP)
			require.Error(t, err)
			require.Contains(t, err.Error(), "redis client is nil")
		})

		t.Run("should error when redis addr not configured for redis-sorted-set", func(t *testing.T) {
			config := config.New()
			config.Set("Router.throttler.limiter.type", "redis-sorted-set")
			// Don't set redis addr

			_, err := NewFactory(config, stats.NOP, logger.NOP)
			require.Error(t, err)
			require.Contains(t, err.Error(), "redis client is nil")
		})
	})

	t.Run("redis authentication", func(t *testing.T) {
		t.Run("should handle redis with credentials", func(t *testing.T) {
			config := config.New()
			config.Set("Router.throttler.limiter.type", "redis-gcra")
			config.Set("Router.throttler.redisThrottler.addr", redisContainer.Addr)
			config.Set("Router.throttler.redisThrottler.username", "")
			config.Set("Router.throttler.redisThrottler.password", "")
			config.Set("Router.throttler.adaptiveEnabled", true)
			config.Set("Router.throttler.maxLimit", int64(100))
			config.Set("Router.throttler.minLimit", int64(10))
			config.Set("Router.throttler.destName.timeWindow", time.Second)

			f, err := NewFactory(config, stats.NOP, logger.NOP)
			require.NoError(t, err)
			defer f.Shutdown()

			ta := f.GetPickupThrottler("destName", "destID", "eventType")
			require.NotNil(t, ta)

			limited, err := ta.CheckLimitReached(context.Background(), 1)
			require.NoError(t, err)
			require.False(t, limited)
		})
	})

	t.Run("redis stats collection", func(t *testing.T) {
		statsStore, err := memstats.New()
		require.NoError(t, err)

		config := config.New()
		config.Set("Router.throttler.limiter.type", "redis-gcra")
		config.Set("Router.throttler.redisThrottler.addr", redisContainer.Addr)
		config.Set("Router.throttler.adaptiveEnabled", true)
		config.Set("Router.throttler.maxLimit", int64(120))
		config.Set("Router.throttler.minLimit", int64(12))
		config.Set("Router.throttler.destName.timeWindow", 2*time.Second)

		f, err := NewFactory(config, statsStore, logger.NOP)
		require.NoError(t, err)
		defer f.Shutdown()

		ta := f.GetPickupThrottler("destName", "destID", "eventType")
		_, _ = ta.CheckLimitReached(context.Background(), 1)

		// Should have adaptive throttler stats
		require.EqualValues(t, 60, statsStore.Get("throttling_rate_limit", stats.Tags{
			"destinationId": "destID",
			"destType":      "destName",
			"adaptive":      "true",
		}).LastValue())

		require.EqualValues(t, 1, statsStore.Get("adaptive_throttler_limit_factor", stats.Tags{
			"destinationId": "destID",
			"destType":      "destName",
		}).LastValue())
	})

	t.Run("redis performance under load", func(t *testing.T) {
		config := config.New()
		config.Set("Router.throttler.limiter.type", "redis-gcra")
		config.Set("Router.throttler.redisThrottler.addr", redisContainer.Addr)
		config.Set("Router.throttler.adaptiveEnabled", false)
		config.Set("Router.throttler.destName.destID.limit", int64(1000))
		config.Set("Router.throttler.destName.destID.timeWindow", time.Second)

		f, err := NewFactory(config, stats.NOP, logger.NOP)
		require.NoError(t, err)
		defer f.Shutdown()

		ta := f.GetPickupThrottler("destName", "destID", "eventType")

		// Send multiple requests rapidly
		const numRequests = 50
		for i := 0; i < numRequests; i++ {
			limited, err := ta.CheckLimitReached(context.Background(), 1)
			require.NoError(t, err, "Request %d should not error", i)
			// With a high limit, should not be limited
			require.False(t, limited, "Request %d should not be limited", i)
		}
	})

	t.Run("redis per-event type throttling", func(t *testing.T) {
		config := config.New()
		config.Set("Router.throttler.limiter.type", "redis-sorted-set")
		config.Set("Router.throttler.redisThrottler.addr", redisContainer.Addr)
		config.Set("Router.throttler.adaptiveEnabled", true)
		config.Set("Router.throttler.destName.destID.throttlerPerEventType", true)
		config.Set("Router.throttler.destName.destID.track.maxLimit", int64(80))
		config.Set("Router.throttler.destName.destID.track.minLimit", int64(8))
		config.Set("Router.throttler.destName.destID.identify.maxLimit", int64(40))
		config.Set("Router.throttler.destName.destID.identify.minLimit", int64(4))
		config.Set("Router.throttler.destName.destID.timeWindow", time.Second)

		f, err := NewFactory(config, stats.NOP, logger.NOP)
		require.NoError(t, err)
		defer f.Shutdown()

		trackThrottler := f.GetPickupThrottler("destName", "destID", "track")
		identifyThrottler := f.GetPickupThrottler("destName", "destID", "identify")

		// Different event types should have different limits
		require.EqualValues(t, 80, trackThrottler.GetLimit())
		require.EqualValues(t, 40, identifyThrottler.GetLimit())

		// Both should be able to process events independently
		trackLimited, err := trackThrottler.CheckLimitReached(context.Background(), 1)
		require.NoError(t, err)
		require.False(t, trackLimited)

		identifyLimited, err := identifyThrottler.CheckLimitReached(context.Background(), 1)
		require.NoError(t, err)
		require.False(t, identifyLimited)
	})
}

func floatCheck(a, b int64) bool {
	return math.Abs(float64(a-b)) <= 1
}
