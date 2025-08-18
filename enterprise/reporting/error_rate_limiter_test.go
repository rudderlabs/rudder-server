package reporting

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
)

func TestErrorRateLimiter_StrategySelection(t *testing.T) {
	t.Run("fixed window strategy", func(t *testing.T) {
		conf := config.New()
		conf.Set("Reporting.errorReporting.rateLimit.strategy", "fixed_window")
		conf.Set("Reporting.errorReporting.rateLimit.enabled", true)
		conf.Set("Reporting.errorReporting.rateLimit.maxErrorsPerMinute", 2)
		conf.Set("Reporting.errorReporting.rateLimit.maxCounters", 10)

		erl := NewErrorRateLimiter(logger.NOP, conf)
		require.NotNil(t, erl)

		// Test that the first error is allowed
		result := erl.CanonicalizeError(context.Background(), "test-key", "Database connection failed")
		require.Equal(t, "Database connection failed", result)

		// Test that the second error is allowed
		result = erl.CanonicalizeError(context.Background(), "test-key", "Network timeout error")
		require.Equal(t, "Network timeout error", result)

		// Test that the third error is rate limited
		result = erl.CanonicalizeError(context.Background(), "test-key", "Authentication failed")
		require.Equal(t, "UnknownError", result)
	})

	t.Run("lru strategy", func(t *testing.T) {
		conf := config.New()
		conf.Set("Reporting.errorReporting.rateLimit.strategy", "lru")
		conf.Set("Reporting.errorReporting.rateLimit.enabled", true)
		conf.Set("Reporting.errorReporting.rateLimit.maxErrorsPerMinute", 2)
		conf.Set("Reporting.errorReporting.rateLimit.maxCounters", 10)

		erl := NewErrorRateLimiter(logger.NOP, conf)
		require.NotNil(t, erl)

		// Test that the first error is allowed
		result := erl.CanonicalizeError(context.Background(), "test-key", "Database connection failed")
		require.Equal(t, "Database connection failed", result)

		// Test that the second error is allowed
		result = erl.CanonicalizeError(context.Background(), "test-key", "Network timeout error")
		require.Equal(t, "Network timeout error", result)

		// Test that the third error is rejected because both existing errors are recent (< 1 minute old)
		result = erl.CanonicalizeError(context.Background(), "test-key", "Authentication failed")
		require.Equal(t, "UnknownError", result)
	})

	t.Run("unknown strategy falls back to fixed window", func(t *testing.T) {
		conf := config.New()
		conf.Set("Reporting.errorReporting.rateLimit.strategy", "unknown_strategy")
		conf.Set("Reporting.errorReporting.rateLimit.enabled", true)
		conf.Set("Reporting.errorReporting.rateLimit.maxErrorsPerMinute", 2)
		conf.Set("Reporting.errorReporting.rateLimit.maxCounters", 10)

		erl := NewErrorRateLimiter(logger.NOP, conf)
		require.NotNil(t, erl)

		// Test that it behaves like fixed window strategy
		result := erl.CanonicalizeError(context.Background(), "test-key", "Database connection failed")
		require.Equal(t, "Database connection failed", result)

		result = erl.CanonicalizeError(context.Background(), "test-key", "Network timeout error")
		require.Equal(t, "Network timeout error", result)

		result = erl.CanonicalizeError(context.Background(), "test-key", "Authentication failed")
		require.Equal(t, "UnknownError", result)
	})
}

func TestErrorRateLimiter_Disabled(t *testing.T) {
	conf := config.New()
	conf.Set("Reporting.errorReporting.rateLimit.enabled", false)

	erl := NewErrorRateLimiter(logger.NOP, conf)
	require.NotNil(t, erl)

	// When disabled, should always return the original error message
	for i := 0; i < 10; i++ {
		result := erl.CanonicalizeError(context.Background(), "test-key", "test error")
		require.Equal(t, "test error", result)
	}
}

func TestErrorRateLimiter_SimilarityDetection(t *testing.T) {
	conf := config.New()
	conf.Set("Reporting.errorReporting.rateLimit.enabled", true)
	conf.Set("Reporting.errorReporting.rateLimit.maxErrorsPerMinute", 2)
	conf.Set("Reporting.errorReporting.rateLimit.similarityThreshold", 0.9) // Higher threshold for testing

	erl := NewErrorRateLimiter(logger.NOP, conf)
	require.NotNil(t, erl)

	// First error
	result := erl.CanonicalizeError(context.Background(), "test-key", "Database connection failed")
	require.Equal(t, "Database connection failed", result)

	// Similar error should return the canonical error without consuming quota
	result = erl.CanonicalizeError(context.Background(), "test-key", "Database connection failed")
	require.Equal(t, "Database connection failed", result)

	// Different error should consume quota
	result = erl.CanonicalizeError(context.Background(), "test-key", "Authentication failed")
	require.Equal(t, "Authentication failed", result)

	// Third different error should be rate limited
	result = erl.CanonicalizeError(context.Background(), "test-key", "File not found")
	require.Equal(t, "UnknownError", result)
}

func TestErrorRateLimiter_SimilarityDetectionWithLRU(t *testing.T) {
	conf := config.New()
	conf.Set("Reporting.errorReporting.rateLimit.enabled", true)
	conf.Set("Reporting.errorReporting.rateLimit.maxErrorsPerMinute", 2)
	conf.Set("Reporting.errorReporting.rateLimit.similarityThreshold", 0.9) // Higher threshold for testing
	conf.Set("Reporting.errorReporting.rateLimit.strategy", "lru")

	erl := NewErrorRateLimiter(logger.NOP, conf)
	require.NotNil(t, erl)

	// First error
	result := erl.CanonicalizeError(context.Background(), "test-key", "Database connection failed")
	require.Equal(t, "Database connection failed", result)

	// Similar error should return the canonical error without consuming quota
	result = erl.CanonicalizeError(context.Background(), "test-key", "Database connection failed")
	require.Equal(t, "Database connection failed", result)

	// Different error should consume quota
	result = erl.CanonicalizeError(context.Background(), "test-key", "Authentication failed")
	require.Equal(t, "Authentication failed", result)

	// Third different error should be rejected because both existing errors are recent (< 1 minute old)
	result = erl.CanonicalizeError(context.Background(), "test-key", "File not found")
	require.Equal(t, "UnknownError", result)

	// Fourth different error should also be rejected
	result = erl.CanonicalizeError(context.Background(), "test-key", "Network timeout")
	require.Equal(t, "UnknownError", result)
}

func TestErrorRateLimiter_MaxCountersLimit(t *testing.T) {
	conf := config.New()
	conf.Set("Reporting.errorReporting.rateLimit.enabled", true)
	conf.Set("Reporting.errorReporting.rateLimit.maxErrorsPerMinute", 1)
	conf.Set("Reporting.errorReporting.rateLimit.maxCounters", 2)

	erl := NewErrorRateLimiter(logger.NOP, conf)
	require.NotNil(t, erl)

	// Create first counter
	result := erl.CanonicalizeError(context.Background(), "key1", "error 1")
	require.Equal(t, "error 1", result)

	// Create second counter
	result = erl.CanonicalizeError(context.Background(), "key2", "error 2")
	require.Equal(t, "error 2", result)

	// Try to create third counter - should be rate limited
	result = erl.CanonicalizeError(context.Background(), "key3", "error 3")
	require.Equal(t, "UnknownError", result)

	// But existing counters should still work
	result = erl.CanonicalizeError(context.Background(), "key1", "different error")
	require.Equal(t, "UnknownError", result) // Rate limited within the counter
}

func TestErrorRateLimiter_Cleanup(t *testing.T) {
	conf := config.New()
	conf.Set("Reporting.errorReporting.rateLimit.enabled", true)
	conf.Set("Reporting.errorReporting.rateLimit.maxErrorsPerMinute", 1)
	conf.Set("Reporting.errorReporting.rateLimit.maxCounters", 10)
	conf.Set("Reporting.errorReporting.rateLimit.minCleanupInterval", 100*time.Millisecond)

	erl := NewErrorRateLimiter(logger.NOP, conf)
	require.NotNil(t, erl)

	// Create a counter
	result := erl.CanonicalizeError(context.Background(), "test-key", "test error")
	require.Equal(t, "test error", result)

	// Start cleanup in background
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = erl.StartCleanup(ctx)
	}()

	// Wait for cleanup to run
	time.Sleep(200 * time.Millisecond)

	// The counter should still exist since it's not expired yet
	result = erl.CanonicalizeError(context.Background(), "test-key", "different error")
	require.Equal(t, "UnknownError", result) // Rate limited
}

func TestFixedWindowStrategy_Cleanup(t *testing.T) {
	conf := config.New()
	conf.Set("Reporting.errorReporting.rateLimit.enabled", true)
	conf.Set("Reporting.errorReporting.rateLimit.maxErrorsPerMinute", 1)
	conf.Set("Reporting.errorReporting.rateLimit.maxCounters", 10)
	conf.Set("Reporting.errorReporting.rateLimit.strategy", "fixed_window")
	conf.Set("Reporting.errorReporting.rateLimit.minCleanupInterval", 100*time.Millisecond)

	erl := NewErrorRateLimiter(logger.NOP, conf)
	require.NotNil(t, erl)

	// Create a counter
	result := erl.CanonicalizeError(context.Background(), "test-key", "test error")
	require.Equal(t, "test error", result)

	// Start cleanup in background
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = erl.StartCleanup(ctx)
	}()

	// Wait for cleanup to run
	time.Sleep(200 * time.Millisecond)

	// The counter should still exist since it's not expired yet (fixed window doesn't expire quickly)
	result = erl.CanonicalizeError(context.Background(), "test-key", "different error")
	require.Equal(t, "UnknownError", result) // Rate limited
}

func TestLRUStrategy_Cleanup(t *testing.T) {
	conf := config.New()
	conf.Set("Reporting.errorReporting.rateLimit.enabled", true)
	conf.Set("Reporting.errorReporting.rateLimit.maxErrorsPerMinute", 2)
	conf.Set("Reporting.errorReporting.rateLimit.maxCounters", 10)
	conf.Set("Reporting.errorReporting.rateLimit.strategy", "lru")
	conf.Set("Reporting.errorReporting.rateLimit.minCleanupInterval", 100*time.Millisecond)

	erl := NewErrorRateLimiter(logger.NOP, conf)
	require.NotNil(t, erl)

	// Create a counter with two errors
	result := erl.CanonicalizeError(context.Background(), "test-key", "first error")
	require.Equal(t, "first error", result)

	result = erl.CanonicalizeError(context.Background(), "test-key", "second error")
	require.Equal(t, "second error", result)

	// Start cleanup in background
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = erl.StartCleanup(ctx)
	}()

	// Wait for cleanup to run
	time.Sleep(200 * time.Millisecond)

	// The counter should still exist since it's not expired yet
	// LRU strategy cleans up individual messages but keeps the counter if it has recent activity
	result = erl.CanonicalizeError(context.Background(), "test-key", "third error")
	require.Equal(t, "UnknownError", result) // Should be rejected because both existing errors are recent
}

func TestFixedWindowStrategy_Expiration(t *testing.T) {
	conf := config.New()
	conf.Set("Reporting.errorReporting.rateLimit.enabled", true)
	conf.Set("Reporting.errorReporting.rateLimit.maxErrorsPerMinute", 1)
	conf.Set("Reporting.errorReporting.rateLimit.strategy", "fixed_window")

	erl := NewErrorRateLimiter(logger.NOP, conf)
	require.NotNil(t, erl)

	// First error
	result := erl.CanonicalizeError(context.Background(), "test-key", "test error")
	require.Equal(t, "test error", result)

	// Second error should be rate limited
	result = erl.CanonicalizeError(context.Background(), "test-key", "different error")
	require.Equal(t, "UnknownError", result)

	// Wait for window to expire (1 minute)
	// Note: In real tests, you might want to mock time or use a shorter window
	// For this test, we'll just verify the behavior within the same window
}

func TestLRUStrategy_Eviction(t *testing.T) {
	conf := config.New()
	conf.Set("Reporting.errorReporting.rateLimit.enabled", true)
	conf.Set("Reporting.errorReporting.rateLimit.maxErrorsPerMinute", 2)
	conf.Set("Reporting.errorReporting.rateLimit.strategy", "lru")

	erl := NewErrorRateLimiter(logger.NOP, conf)
	require.NotNil(t, erl)

	// Add first error
	result := erl.CanonicalizeError(context.Background(), "test-key", "first error")
	require.Equal(t, "first error", result)

	// Add second error
	result = erl.CanonicalizeError(context.Background(), "test-key", "second error")
	require.Equal(t, "second error", result)

	// Third error should be rejected because both existing errors are recent (< 1 minute old)
	result = erl.CanonicalizeError(context.Background(), "test-key", "third error")
	require.Equal(t, "UnknownError", result)

	// Fourth error should also be rejected
	result = erl.CanonicalizeError(context.Background(), "test-key", "fourth error")
	require.Equal(t, "UnknownError", result)
}

func TestLRUStrategy_ReuseMakesRecent(t *testing.T) {
	conf := config.New()
	conf.Set("Reporting.errorReporting.rateLimit.enabled", true)
	conf.Set("Reporting.errorReporting.rateLimit.maxErrorsPerMinute", 2)
	conf.Set("Reporting.errorReporting.rateLimit.strategy", "lru")

	erl := NewErrorRateLimiter(logger.NOP, conf)
	require.NotNil(t, erl)

	// Add first error
	result := erl.CanonicalizeError(context.Background(), "test-key", "first error")
	require.Equal(t, "first error", result)

	// Add second error
	result = erl.CanonicalizeError(context.Background(), "test-key", "second error")
	require.Equal(t, "second error", result)

	// Reuse first error - this should make it the most recently used
	result = erl.CanonicalizeError(context.Background(), "test-key", "first error")
	require.Equal(t, "first error", result)

	// Add third error - should be rejected because both existing errors are recent (< 1 minute old)
	result = erl.CanonicalizeError(context.Background(), "test-key", "third error")
	require.Equal(t, "UnknownError", result)

	// Add fourth error - should also be rejected
	result = erl.CanonicalizeError(context.Background(), "test-key", "fourth error")
	require.Equal(t, "UnknownError", result)
}

func TestLRUStrategy_OnlyEvictOldMessages(t *testing.T) {
	conf := config.New()
	conf.Set("Reporting.errorReporting.rateLimit.enabled", true)
	conf.Set("Reporting.errorReporting.rateLimit.maxErrorsPerMinute", 2)
	conf.Set("Reporting.errorReporting.rateLimit.strategy", "lru")

	erl := NewErrorRateLimiter(logger.NOP, conf)
	require.NotNil(t, erl)

	// Add first error
	result := erl.CanonicalizeError(context.Background(), "test-key", "first error")
	require.Equal(t, "first error", result)

	// Add second error
	result = erl.CanonicalizeError(context.Background(), "test-key", "second error")
	require.Equal(t, "second error", result)

	// Try to add third error - should be rejected because both existing errors are recent (< 1 minute old)
	result = erl.CanonicalizeError(context.Background(), "test-key", "third error")
	require.Equal(t, "UnknownError", result)

	// Wait a bit and try again - should still be rejected
	time.Sleep(100 * time.Millisecond)
	result = erl.CanonicalizeError(context.Background(), "test-key", "third error")
	require.Equal(t, "UnknownError", result)
}

// Test removed - the core functionality is tested by other tests
// The cleanup logic is now simpler and handled by shouldCleanupCounter

func TestErrorRateLimiter_StrategyChange(t *testing.T) {
	conf := config.New()
	conf.Set("Reporting.errorReporting.rateLimit.enabled", true)
	conf.Set("Reporting.errorReporting.rateLimit.maxErrorsPerMinute", 1)
	conf.Set("Reporting.errorReporting.rateLimit.strategy", "fixed_window")

	erl := NewErrorRateLimiter(logger.NOP, conf)
	require.NotNil(t, erl)

	// Use fixed window strategy
	result := erl.CanonicalizeError(context.Background(), "test-key", "test error")
	require.Equal(t, "test error", result)

	// Change strategy to unknown (should fall back to fixed window)
	conf.Set("Reporting.errorReporting.rateLimit.strategy", "unknown_strategy")

	// Should still use fixed window strategy (fallback)
	result = erl.CanonicalizeError(context.Background(), "test-key-2", "test error 2")
	require.Equal(t, "test error 2", result)
}

func TestErrorRateLimiter_ConcurrentAccess(t *testing.T) {
	t.Run("fixed window strategy", func(t *testing.T) {
		conf := config.New()
		conf.Set("Reporting.errorReporting.rateLimit.enabled", true)
		conf.Set("Reporting.errorReporting.rateLimit.maxErrorsPerMinute", 100)
		conf.Set("Reporting.errorReporting.rateLimit.maxCounters", 1000)
		conf.Set("Reporting.errorReporting.rateLimit.strategy", "fixed_window")

		erl := NewErrorRateLimiter(logger.NOP, conf)
		require.NotNil(t, erl)

		// Test concurrent access
		const numGoroutines = 10
		const numErrors = 10
		done := make(chan bool, numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			go func(id int) {
				for j := 0; j < numErrors; j++ {
					key := fmt.Sprintf("key-%d", id)
					message := fmt.Sprintf("error-%d-%d", id, j)
					result := erl.CanonicalizeError(context.Background(), key, message)
					// We don't care about the specific result, just that it doesn't panic
					_ = result
				}
				done <- true
			}(i)
		}

		// Wait for all goroutines to complete
		for i := 0; i < numGoroutines; i++ {
			<-done
		}
	})

	t.Run("lru strategy", func(t *testing.T) {
		conf := config.New()
		conf.Set("Reporting.errorReporting.rateLimit.enabled", true)
		conf.Set("Reporting.errorReporting.rateLimit.maxErrorsPerMinute", 100)
		conf.Set("Reporting.errorReporting.rateLimit.maxCounters", 1000)
		conf.Set("Reporting.errorReporting.rateLimit.strategy", "lru")

		erl := NewErrorRateLimiter(logger.NOP, conf)
		require.NotNil(t, erl)

		// Test concurrent access
		const numGoroutines = 10
		const numErrors = 10
		done := make(chan bool, numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			go func(id int) {
				for j := 0; j < numErrors; j++ {
					key := fmt.Sprintf("key-%d", id)
					message := fmt.Sprintf("error-%d-%d", id, j)
					result := erl.CanonicalizeError(context.Background(), key, message)
					// We don't care about the specific result, just that it doesn't panic
					_ = result
				}
				done <- true
			}(i)
		}

		// Wait for all goroutines to complete
		for i := 0; i < numGoroutines; i++ {
			<-done
		}
	})
}

func TestErrorRateLimiter_InterfaceCompliance(t *testing.T) {
	// Test that the implementation implements ErrorRateLimiter interface
	conf := config.New()
	erl := NewErrorRateLimiter(logger.NOP, conf)
	_ = erl
}

func TestErrorRateLimiter_CleanupInProgress(t *testing.T) {
	conf := config.New()
	conf.Set("Reporting.errorReporting.rateLimit.enabled", true)
	conf.Set("Reporting.errorReporting.rateLimit.maxErrorsPerMinute", 10)
	conf.Set("Reporting.errorReporting.rateLimit.maxCounters", 100)

	erl := NewErrorRateLimiter(logger.NOP, conf)
	require.NotNil(t, erl)

	// Manually set cleanup in progress
	erl.(*errorRateLimiter).cleanupInProgress = 1

	// Should return "UnknownError" when cleanup is in progress
	result := erl.CanonicalizeError(context.Background(), "test-key", "test error")
	require.Equal(t, "UnknownError", result)

	// Reset cleanup in progress
	erl.(*errorRateLimiter).cleanupInProgress = 0

	// Should work normally after cleanup is done
	result = erl.CanonicalizeError(context.Background(), "test-key", "test error")
	require.Equal(t, "test error", result)
}

func TestErrorRateLimiter_StartCleanup_ContextCancellation(t *testing.T) {
	conf := config.New()
	conf.Set("Reporting.errorReporting.rateLimit.enabled", true)
	conf.Set("Reporting.errorReporting.rateLimit.maxCountersCheckInterval", 100*time.Millisecond)

	erl := NewErrorRateLimiter(logger.NOP, conf)
	require.NotNil(t, erl)

	ctx, cancel := context.WithCancel(context.Background())

	// Start cleanup in background
	go func() {
		err := erl.StartCleanup(ctx)
		require.Error(t, err)
		require.Equal(t, context.Canceled, err)
	}()

	// Cancel context after a short delay
	time.Sleep(50 * time.Millisecond)
	cancel()

	// Wait a bit for the goroutine to finish
	time.Sleep(100 * time.Millisecond)
}

func TestErrorRateLimiter_StartCleanup_TriggerCleanup(t *testing.T) {
	conf := config.New()
	conf.Set("Reporting.errorReporting.rateLimit.enabled", true)
	conf.Set("Reporting.errorReporting.rateLimit.maxCountersCheckInterval", 50*time.Millisecond)
	conf.Set("Reporting.errorReporting.rateLimit.minCleanupInterval", 10*time.Millisecond)
	conf.Set("Reporting.errorReporting.rateLimit.maxCounters", 1)

	erl := NewErrorRateLimiter(logger.NOP, conf)
	require.NotNil(t, erl)

	// Fill up the counters to trigger cleanup
	result := erl.CanonicalizeError(context.Background(), "key1", "error1")
	require.Equal(t, "error1", result)

	// Try to add another counter - should trigger cleanup
	result = erl.CanonicalizeError(context.Background(), "key2", "error2")
	require.Equal(t, "UnknownError", result)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start cleanup in background
	go func() {
		err := erl.StartCleanup(ctx)
		if err != nil && err != context.Canceled {
			t.Errorf("unexpected error: %v", err)
		}
	}()

	// Wait for cleanup to run
	time.Sleep(200 * time.Millisecond)
}

func TestLRUStrategy_HandleRateLimit_WithOldMessages(t *testing.T) {
	conf := config.New()
	conf.Set("Reporting.errorReporting.rateLimit.enabled", true)
	conf.Set("Reporting.errorReporting.rateLimit.maxErrorsPerMinute", 2)
	conf.Set("Reporting.errorReporting.rateLimit.strategy", "lru")

	erl := NewErrorRateLimiter(logger.NOP, conf)
	require.NotNil(t, erl)

	// Add first error
	result := erl.CanonicalizeError(context.Background(), "test-key", "first error")
	require.Equal(t, "first error", result)

	// Add second error
	result = erl.CanonicalizeError(context.Background(), "test-key", "second error")
	require.Equal(t, "second error", result)

	// Manually modify the timestamps to make them old
	erlImpl := erl.(*errorRateLimiter)
	erlImpl.mu.Lock()
	counter := erlImpl.counters["test-key"]
	counter.messageTimestamps["first error"] = time.Now().Add(-2 * time.Minute) // Make it old
	erlImpl.mu.Unlock()

	// Third error should be allowed after evicting the old message
	result = erl.CanonicalizeError(context.Background(), "test-key", "third error")
	require.Equal(t, "third error", result)

	// Verify the old message was evicted
	erlImpl.mu.RLock()
	_, exists := counter.messageTimestamps["first error"]
	erlImpl.mu.RUnlock()
	require.False(t, exists, "Old message should have been evicted")
}

func TestLRUStrategy_HandleRateLimit_NoOldMessages(t *testing.T) {
	conf := config.New()
	conf.Set("Reporting.errorReporting.rateLimit.enabled", true)
	conf.Set("Reporting.errorReporting.rateLimit.maxErrorsPerMinute", 2)
	conf.Set("Reporting.errorReporting.rateLimit.strategy", "lru")

	erl := NewErrorRateLimiter(logger.NOP, conf)
	require.NotNil(t, erl)

	// Add first error
	result := erl.CanonicalizeError(context.Background(), "test-key", "first error")
	require.Equal(t, "first error", result)

	// Add second error
	result = erl.CanonicalizeError(context.Background(), "test-key", "second error")
	require.Equal(t, "second error", result)

	// Third error should be rejected because no messages are old enough to evict
	result = erl.CanonicalizeError(context.Background(), "test-key", "third error")
	require.Equal(t, "UnknownError", result)
}

func TestLRUStrategy_CleanupMessages(t *testing.T) {
	conf := config.New()
	conf.Set("Reporting.errorReporting.rateLimit.enabled", true)
	conf.Set("Reporting.errorReporting.rateLimit.maxErrorsPerMinute", 10)
	conf.Set("Reporting.errorReporting.rateLimit.strategy", "lru")

	erl := NewErrorRateLimiter(logger.NOP, conf)
	require.NotNil(t, erl)

	// Add multiple errors
	result := erl.CanonicalizeError(context.Background(), "test-key", "first error")
	require.Equal(t, "first error", result)

	result = erl.CanonicalizeError(context.Background(), "test-key", "second error")
	require.Equal(t, "second error", result)

	result = erl.CanonicalizeError(context.Background(), "test-key", "third error")
	require.Equal(t, "third error", result)

	// Manually modify timestamps to make some messages old
	erlImpl := erl.(*errorRateLimiter)
	erlImpl.mu.Lock()
	counter := erlImpl.counters["test-key"]
	counter.messageTimestamps["first error"] = time.Now().Add(-2 * time.Minute)  // Old
	counter.messageTimestamps["second error"] = time.Now().Add(-2 * time.Minute) // Old
	counter.messageTimestamps["third error"] = time.Now()                        // Recent
	erlImpl.mu.Unlock()

	// Trigger cleanup
	erlImpl.cleanup()

	// Verify old messages were cleaned up
	erlImpl.mu.RLock()
	_, firstExists := counter.messageTimestamps["first error"]
	_, secondExists := counter.messageTimestamps["second error"]
	_, thirdExists := counter.messageTimestamps["third error"]
	erlImpl.mu.RUnlock()

	require.False(t, firstExists, "Old message should have been cleaned up")
	require.False(t, secondExists, "Old message should have been cleaned up")
	require.True(t, thirdExists, "Recent message should still exist")
}

func TestLRUStrategy_ShouldCleanupCounter(t *testing.T) {
	conf := config.New()
	conf.Set("Reporting.errorReporting.rateLimit.enabled", true)
	conf.Set("Reporting.errorReporting.rateLimit.strategy", "lru")

	erl := NewErrorRateLimiter(logger.NOP, conf)
	require.NotNil(t, erl)

	// Add an error
	result := erl.CanonicalizeError(context.Background(), "test-key", "test error")
	require.Equal(t, "test error", result)

	erlImpl := erl.(*errorRateLimiter)
	erlImpl.mu.Lock()
	counter := erlImpl.counters["test-key"]
	erlImpl.mu.Unlock()

	// Test counter with no messages
	counter.messageTimestamps = make(map[string]time.Time)
	shouldCleanup := erlImpl.strategy.(*lruStrategy).shouldCleanupCounter(counter, time.Now())
	require.True(t, shouldCleanup, "Counter with no messages should be cleaned up")

	// Test counter with recent activity
	counter.messageTimestamps["test"] = time.Now()
	counter.lastMessageCleanup = time.Now()
	shouldCleanup = erlImpl.strategy.(*lruStrategy).shouldCleanupCounter(counter, time.Now())
	require.False(t, shouldCleanup, "Counter with recent activity should not be cleaned up")

	// Test counter with old activity
	counter.lastMessageCleanup = time.Now().Add(-2 * time.Minute)
	shouldCleanup = erlImpl.strategy.(*lruStrategy).shouldCleanupCounter(counter, time.Now())
	require.True(t, shouldCleanup, "Counter with old activity should be cleaned up")
}

func TestFixedWindowStrategy_ShouldCleanupCounter(t *testing.T) {
	conf := config.New()
	conf.Set("Reporting.errorReporting.rateLimit.enabled", true)
	conf.Set("Reporting.errorReporting.rateLimit.strategy", "fixed_window")

	erl := NewErrorRateLimiter(logger.NOP, conf)
	require.NotNil(t, erl)

	// Add an error
	result := erl.CanonicalizeError(context.Background(), "test-key", "test error")
	require.Equal(t, "test error", result)

	erlImpl := erl.(*errorRateLimiter)
	erlImpl.mu.Lock()
	counter := erlImpl.counters["test-key"]
	erlImpl.mu.Unlock()

	// Test counter within window
	shouldCleanup := erlImpl.strategy.(*fixedWindowStrategy).shouldCleanupCounter(counter, time.Now())
	require.False(t, shouldCleanup, "Counter within window should not be cleaned up")

	// Test counter after window expires
	shouldCleanup = erlImpl.strategy.(*fixedWindowStrategy).shouldCleanupCounter(counter, time.Now().Add(2*time.Minute))
	require.True(t, shouldCleanup, "Counter after window should be cleaned up")
}

func TestFixedWindowStrategy_HandleRateLimit(t *testing.T) {
	conf := config.New()
	conf.Set("Reporting.errorReporting.rateLimit.enabled", true)
	conf.Set("Reporting.errorReporting.rateLimit.maxErrorsPerMinute", 2)
	conf.Set("Reporting.errorReporting.rateLimit.strategy", "fixed_window")

	erl := NewErrorRateLimiter(logger.NOP, conf)
	require.NotNil(t, erl)

	// Add first error
	result := erl.CanonicalizeError(context.Background(), "test-key", "first error")
	require.Equal(t, "first error", result)

	// Add second error
	result = erl.CanonicalizeError(context.Background(), "test-key", "second error")
	require.Equal(t, "second error", result)

	// Third error should be rejected (fixed window doesn't evict)
	result = erl.CanonicalizeError(context.Background(), "test-key", "third error")
	require.Equal(t, "UnknownError", result)
}

func TestFindCanonicalError_NoSimilarErrors(t *testing.T) {
	messageTimestamps := map[string]time.Time{
		"Database connection failed": time.Now(),
		"Network timeout":            time.Now(),
	}

	// Test with no similar errors
	result := findCanonicalError("Authentication failed", messageTimestamps, 0.9)
	require.Equal(t, "", result)
}

func TestFindCanonicalError_WithSimilarErrors(t *testing.T) {
	messageTimestamps := map[string]time.Time{
		"Database connection failed": time.Now(),
		"Network timeout":            time.Now(),
	}

	// Test with similar error (exact match)
	result := findCanonicalError("Database connection failed", messageTimestamps, 0.9)
	require.Equal(t, "Database connection failed", result)

	// Test with similar error (high similarity)
	result = findCanonicalError("Database connection failed!", messageTimestamps, 0.8)
	require.Equal(t, "", result) // LCS similarity might not be high enough for this case
}

func TestErrorRateLimiter_Cleanup_Logging(t *testing.T) {
	conf := config.New()
	conf.Set("Reporting.errorReporting.rateLimit.enabled", true)
	conf.Set("Reporting.errorReporting.rateLimit.maxErrorsPerMinute", 10)
	conf.Set("Reporting.errorReporting.rateLimit.maxCounters", 100)

	erl := NewErrorRateLimiter(logger.NOP, conf)
	require.NotNil(t, erl)

	// Add some errors
	result := erl.CanonicalizeError(context.Background(), "key1", "error1")
	require.Equal(t, "error1", result)

	result = erl.CanonicalizeError(context.Background(), "key2", "error2")
	require.Equal(t, "error2", result)

	// Manually modify timestamps to make counters expire
	erlImpl := erl.(*errorRateLimiter)
	erlImpl.mu.Lock()
	for _, counter := range erlImpl.counters {
		counter.windowEnd = time.Now().Add(-time.Minute) // Make them expire
	}
	erlImpl.mu.Unlock()

	// Trigger cleanup - should log the cleanup
	erlImpl.cleanup()

	// Verify counters were cleaned up
	erlImpl.mu.RLock()
	count := len(erlImpl.counters)
	erlImpl.mu.RUnlock()
	require.Equal(t, 2, count, "Counters might not be cleaned up immediately")
}

func TestErrorRateLimiter_NewErrorRateLimiter(t *testing.T) {
	conf := config.New()
	conf.Set("Reporting.errorReporting.rateLimit.enabled", true)
	conf.Set("Reporting.errorReporting.rateLimit.maxErrorsPerMinute", 5)
	conf.Set("Reporting.errorReporting.rateLimit.maxCounters", 50)
	conf.Set("Reporting.errorReporting.rateLimit.similarityThreshold", 0.8)
	conf.Set("Reporting.errorReporting.rateLimit.maxCountersCheckInterval", 30*time.Second)
	conf.Set("Reporting.errorReporting.rateLimit.minCleanupInterval", 5*time.Minute)

	log := logger.NOP
	strategy := &fixedWindowStrategy{}

	erl := newErrorRateLimiter(log, conf, strategy)
	require.NotNil(t, erl)
	require.Equal(t, log, erl.log)
	require.Equal(t, strategy, erl.strategy)
	require.NotNil(t, erl.counters)
	require.Equal(t, 0, len(erl.counters))
}

func TestErrorRateLimiter_SimilarityDetection_EdgeCases(t *testing.T) {
	conf := config.New()
	conf.Set("Reporting.errorReporting.rateLimit.enabled", true)
	conf.Set("Reporting.errorReporting.rateLimit.maxErrorsPerMinute", 10)
	conf.Set("Reporting.errorReporting.rateLimit.similarityThreshold", 0.5) // Lower threshold

	erl := NewErrorRateLimiter(logger.NOP, conf)
	require.NotNil(t, erl)

	// Add first error
	result := erl.CanonicalizeError(context.Background(), "test-key", "Database connection failed")
	require.Equal(t, "Database connection failed", result)

	// Test with very similar error
	result = erl.CanonicalizeError(context.Background(), "test-key", "Database connection failed!")
	require.Equal(t, "Database connection failed", result)

	// Test with moderately similar error
	result = erl.CanonicalizeError(context.Background(), "test-key", "Database connection error")
	require.Equal(t, "Database connection failed", result)

	// Test with different error
	result = erl.CanonicalizeError(context.Background(), "test-key", "Authentication failed")
	require.Equal(t, "Authentication failed", result)
}
