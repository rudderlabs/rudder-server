package reporting

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
)

func TestErrorRateLimiter_NewErrorRateLimiter(t *testing.T) {
	t.Parallel()

	conf := config.New()
	conf.Set("Reporting.errorReporting.rateLimit.enabled", true)
	conf.Set("Reporting.errorReporting.rateLimit.maxErrorsPerMinute", 10)
	conf.Set("Reporting.errorReporting.rateLimit.maxCounters", 2000)
	conf.Set("Reporting.errorReporting.rateLimit.similarityThreshold", 0.7)
	conf.Set("Reporting.errorReporting.rateLimit.cleanupInterval", "2m")

	log := logger.NewLogger()

	limiter := NewErrorRateLimiter(log, conf)

	require.NotNil(t, limiter)
	require.True(t, limiter.enabled.Load())
	require.Equal(t, 10, limiter.maxErrorsPerMinute.Load())
	require.Equal(t, 2000, limiter.maxCounters.Load())
	require.Equal(t, 0.7, limiter.similarityThreshold.Load())
	require.Equal(t, 2*time.Minute, limiter.cleanupInterval.Load())
}

func TestErrorRateLimiter_ShouldReport_WhenDisabled(t *testing.T) {
	t.Parallel()

	conf := config.New()
	conf.Set("Reporting.errorReporting.rateLimit.enabled", false)

	limiter := NewErrorRateLimiter(logger.NewLogger(), conf)

	ctx := context.Background()
	result := limiter.ShouldReport(ctx, "source1", "dest1", "event1", "reporter1", "error message")

	require.True(t, result, "Should always report when rate limiter is disabled")
}

func TestErrorRateLimiter_ShouldReport_NewWindow(t *testing.T) {
	t.Parallel()

	conf := config.New()
	conf.Set("Reporting.errorReporting.rateLimit.enabled", true)
	conf.Set("Reporting.errorReporting.rateLimit.maxErrorsPerMinute", 5)

	limiter := NewErrorRateLimiter(logger.NewLogger(), conf)

	ctx := context.Background()
	result := limiter.ShouldReport(ctx, "source1", "dest1", "event1", "reporter1", "error message")

	require.True(t, result, "Should report first error in new window")
}

func TestErrorRateLimiter_ShouldReport_RateLimitExceeded(t *testing.T) {
	t.Parallel()

	conf := config.New()
	conf.Set("Reporting.errorReporting.rateLimit.enabled", true)
	conf.Set("Reporting.errorReporting.rateLimit.maxErrorsPerMinute", 2)

	limiter := NewErrorRateLimiter(logger.NewLogger(), conf)

	ctx := context.Background()
	key := "source1:dest1:event1:reporter1"

	// Report first error - should succeed
	result1 := limiter.ShouldReport(ctx, "source1", "dest1", "event1", "reporter1", "error message 1")
	require.True(t, result1)

	// Report second error - should succeed
	result2 := limiter.ShouldReport(ctx, "source1", "dest1", "event1", "reporter1", "error message 2")
	require.True(t, result2)

	// Report third error - should be rate limited
	result3 := limiter.ShouldReport(ctx, "source1", "dest1", "event1", "reporter1", "error message 3")
	require.False(t, result3, "Should be rate limited after exceeding max errors per minute")

	// Verify counter exists and has correct count
	limiter.mu.RLock()
	counter, exists := limiter.counters[key]
	limiter.mu.RUnlock()

	require.True(t, exists)
	require.Equal(t, 2, counter.count)
}

func TestErrorRateLimiter_ShouldReport_SimilarErrorDetection(t *testing.T) {
	t.Parallel()

	conf := config.New()
	conf.Set("Reporting.errorReporting.rateLimit.enabled", true)
	conf.Set("Reporting.errorReporting.rateLimit.maxErrorsPerMinute", 10)
	conf.Set("Reporting.errorReporting.rateLimit.similarityThreshold", 0.8)

	limiter := NewErrorRateLimiter(logger.NewLogger(), conf)

	ctx := context.Background()

	// Report first error
	result1 := limiter.ShouldReport(ctx, "source1", "dest1", "event1", "reporter1", "Database connection failed: timeout")
	require.True(t, result1)

	// Report similar error - should be filtered
	result2 := limiter.ShouldReport(ctx, "source1", "dest1", "event1", "reporter1", "Database connection failed: timeout occurred")
	require.False(t, result2, "Should filter similar error messages")

	// Report different error - should succeed
	result3 := limiter.ShouldReport(ctx, "source1", "dest1", "event1", "reporter1", "Authentication failed: invalid credentials")
	require.True(t, result3, "Should report different error messages")
}

func TestErrorRateLimiter_ShouldReport_WindowExpiration(t *testing.T) {
	t.Parallel()

	conf := config.New()
	conf.Set("Reporting.errorReporting.rateLimit.enabled", true)
	conf.Set("Reporting.errorReporting.rateLimit.maxErrorsPerMinute", 1)

	limiter := NewErrorRateLimiter(logger.NewLogger(), conf)

	ctx := context.Background()

	// Report first error
	result1 := limiter.ShouldReport(ctx, "source1", "dest1", "event1", "reporter1", "error message 1")
	require.True(t, result1)

	// Try to report second error immediately - should be rate limited
	result2 := limiter.ShouldReport(ctx, "source1", "dest1", "event1", "reporter1", "error message 2")
	require.False(t, result2)

	// Manually expire the window by modifying the counter
	limiter.mu.Lock()
	key := "source1:dest1:event1:reporter1"
	if counter, exists := limiter.counters[key]; exists {
		counter.windowEnd = time.Now().Add(-time.Minute) // Expire the window
	}
	limiter.mu.Unlock()

	// Try to report error again - should succeed due to expired window
	result3 := limiter.ShouldReport(ctx, "source1", "dest1", "event1", "reporter1", "error message 3")
	require.True(t, result3, "Should report after window expiration")
}

func TestErrorRateLimiter_GenerateKey(t *testing.T) {
	t.Parallel()

	limiter := NewErrorRateLimiter(logger.NewLogger(), config.New())

	key := limiter.generateKey("source1", "dest1", "event1", "reporter1")
	expectedKey := "source1:dest1:event1:reporter1"

	require.Equal(t, expectedKey, key)
}

func TestErrorRateLimiter_Cleanup(t *testing.T) {
	t.Parallel()

	conf := config.New()
	conf.Set("Reporting.errorReporting.rateLimit.enabled", true)

	limiter := NewErrorRateLimiter(logger.NewLogger(), conf)

	// Add some counters with expired windows
	limiter.mu.Lock()
	limiter.counters["expired1"] = &errorCounter{
		count:     1,
		windowEnd: time.Now().Add(-time.Minute), // Expired
		messages:  []string{"error1"},
	}
	limiter.counters["expired2"] = &errorCounter{
		count:     1,
		windowEnd: time.Now().Add(-time.Minute), // Expired
		messages:  []string{"error2"},
	}
	limiter.counters["active"] = &errorCounter{
		count:     1,
		windowEnd: time.Now().Add(time.Minute), // Still active
		messages:  []string{"error3"},
	}
	limiter.mu.Unlock()

	// Run cleanup
	limiter.cleanup()

	// Verify expired counters are removed
	limiter.mu.RLock()
	_, expired1Exists := limiter.counters["expired1"]
	_, expired2Exists := limiter.counters["expired2"]
	_, activeExists := limiter.counters["active"]
	limiter.mu.RUnlock()

	require.False(t, expired1Exists, "Expired counter should be removed")
	require.False(t, expired2Exists, "Expired counter should be removed")
	require.True(t, activeExists, "Active counter should remain")
}

func TestErrorRateLimiter_StartCleanup(t *testing.T) {
	t.Parallel()

	conf := config.New()
	conf.Set("Reporting.errorReporting.rateLimit.maxCountersCheckInterval", "100ms") // Fast check for testing
	conf.Set("Reporting.errorReporting.rateLimit.minCleanupInterval", "50ms")        // Short interval for testing

	limiter := NewErrorRateLimiter(logger.NewLogger(), conf)

	// Add an expired counter
	limiter.mu.Lock()
	limiter.counters["expired"] = &errorCounter{
		count:     1,
		windowEnd: time.Now().Add(-time.Minute),
		messages:  []string{"error"},
	}
	limiter.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start cleanup routine
	limiter.StartCleanup(ctx)

	// Wait for cleanup to run
	time.Sleep(200 * time.Millisecond)

	// Verify expired counter is removed
	limiter.mu.RLock()
	_, exists := limiter.counters["expired"]
	limiter.mu.RUnlock()

	require.False(t, exists, "Expired counter should be cleaned up")
}

func TestErrorRateLimiter_ConcurrentAccess(t *testing.T) {
	t.Parallel()

	conf := config.New()
	conf.Set("Reporting.errorReporting.rateLimit.enabled", true)
	conf.Set("Reporting.errorReporting.rateLimit.maxErrorsPerMinute", 100)

	limiter := NewErrorRateLimiter(logger.NewLogger(), conf)

	ctx := context.Background()
	const numGoroutines = 10
	const reportsPerGoroutine = 10

	// Start multiple goroutines reporting errors concurrently
	done := make(chan bool, numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			for j := 0; j < reportsPerGoroutine; j++ {
				limiter.ShouldReport(ctx, "source1", "dest1", "event1", "reporter1",
					fmt.Sprintf("unique error %d-%d", id, j))
			}
			done <- true
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	// Verify the final state is consistent
	limiter.mu.RLock()
	counter, exists := limiter.counters["source1:dest1:event1:reporter1"]
	limiter.mu.RUnlock()

	require.True(t, exists)
	require.Equal(t, numGoroutines*reportsPerGoroutine, counter.count)
}

func TestErrorRateLimiter_IsBucketFull(t *testing.T) {
	t.Parallel()

	conf := config.New()
	conf.Set("Reporting.errorReporting.rateLimit.enabled", true)
	conf.Set("Reporting.errorReporting.rateLimit.maxErrorsPerMinute", 3)

	limiter := NewErrorRateLimiter(logger.NewLogger(), conf)
	ctx := context.Background()

	t.Run("bucket not full when no errors reported", func(t *testing.T) {
		result := limiter.IsBucketFull(ctx, "source1", "dest1", "event1", "reporter1")
		require.False(t, result, "Bucket should not be full when no errors have been reported")
	})

	t.Run("bucket not full when under limit", func(t *testing.T) {
		// Report 2 errors (under the limit of 3)
		limiter.ShouldReport(ctx, "source1", "dest1", "event1", "reporter1", "error 1")
		limiter.ShouldReport(ctx, "source1", "dest1", "event1", "reporter1", "error 2")

		result := limiter.IsBucketFull(ctx, "source1", "dest1", "event1", "reporter1")
		require.False(t, result, "Bucket should not be full when under the limit")
	})

	t.Run("bucket full when at limit", func(t *testing.T) {
		// Report 3rd error to reach the limit
		limiter.ShouldReport(ctx, "source1", "dest1", "event1", "reporter1", "error 3")

		result := limiter.IsBucketFull(ctx, "source1", "dest1", "event1", "reporter1")
		require.True(t, result, "Bucket should be full when at the limit")
	})

	t.Run("bucket not full when disabled", func(t *testing.T) {
		conf := config.New()
		conf.Set("Reporting.errorReporting.rateLimit.enabled", false)
		disabledLimiter := NewErrorRateLimiter(logger.NewLogger(), conf)

		result := disabledLimiter.IsBucketFull(ctx, "source1", "dest1", "event1", "reporter1")
		require.False(t, result, "Bucket should not be full when rate limiter is disabled")
	})

	t.Run("bucket not full after window expiration", func(t *testing.T) {
		// Create a counter with an expired window
		limiter.mu.Lock()
		limiter.counters["expired:dest1:event1:reporter1"] = &errorCounter{
			count:     5,                            // Over the limit
			windowEnd: time.Now().Add(-time.Minute), // Expired
			messages:  []string{"error1", "error2", "error3", "error4", "error5"},
		}
		limiter.mu.Unlock()

		result := limiter.IsBucketFull(ctx, "expired", "dest1", "event1", "reporter1")
		require.False(t, result, "Bucket should not be full after window expiration")
	})

	t.Run("different keys have independent buckets", func(t *testing.T) {
		// Fill up one bucket
		limiter.ShouldReport(ctx, "source3", "dest3", "event3", "reporter3", "error 1")
		limiter.ShouldReport(ctx, "source3", "dest3", "event3", "reporter3", "error 2")
		limiter.ShouldReport(ctx, "source3", "dest3", "event3", "reporter3", "error 3")

		// Check that the other bucket is not affected
		result := limiter.IsBucketFull(ctx, "source4", "dest4", "event4", "reporter4")
		require.False(t, result, "Different keys should have independent buckets")
	})
}

func TestErrorRateLimiter_IsBucketFull_MaxCountersLimit(t *testing.T) {
	t.Parallel()

	conf := config.New()
	conf.Set("Reporting.errorReporting.rateLimit.enabled", true)
	conf.Set("Reporting.errorReporting.rateLimit.maxErrorsPerMinute", 5)
	conf.Set("Reporting.errorReporting.rateLimit.maxCounters", 3) // Very low limit for testing

	limiter := NewErrorRateLimiter(logger.NewLogger(), conf)
	ctx := context.Background()

	// Fill up to the max counters limit
	limiter.ShouldReport(ctx, "source1", "dest1", "event1", "reporter1", "error 1")
	limiter.ShouldReport(ctx, "source2", "dest2", "event2", "reporter2", "error 2")
	limiter.ShouldReport(ctx, "source3", "dest3", "event3", "reporter3", "error 3")

	// Check that IsBucketFull returns true for new keys when at max counters
	result := limiter.IsBucketFull(ctx, "source4", "dest4", "event4", "reporter4")
	require.True(t, result, "IsBucketFull should return true for new keys when at max counters")

	// But existing keys should still work normally
	result2 := limiter.IsBucketFull(ctx, "source1", "dest1", "event1", "reporter1")
	require.False(t, result2, "IsBucketFull should return false for existing keys even at max counters")
}

func TestErrorRateLimiter_ShouldReport_MaxCountersLimit(t *testing.T) {
	t.Parallel()

	conf := config.New()
	conf.Set("Reporting.errorReporting.rateLimit.enabled", true)
	conf.Set("Reporting.errorReporting.rateLimit.maxErrorsPerMinute", 5)
	conf.Set("Reporting.errorReporting.rateLimit.maxCounters", 3) // Very low limit for testing

	limiter := NewErrorRateLimiter(logger.NewLogger(), conf)
	ctx := context.Background()

	// Fill up to the max counters limit
	result1 := limiter.ShouldReport(ctx, "source1", "dest1", "event1", "reporter1", "error 1")
	require.True(t, result1, "First counter should be created")

	result2 := limiter.ShouldReport(ctx, "source2", "dest2", "event2", "reporter2", "error 2")
	require.True(t, result2, "Second counter should be created")

	result3 := limiter.ShouldReport(ctx, "source3", "dest3", "event3", "reporter3", "error 3")
	require.True(t, result3, "Third counter should be created")

	// ShouldReport now checks maxCounters when creating new counters
	// So creating a fourth counter should fail
	result4 := limiter.ShouldReport(ctx, "source4", "dest4", "event4", "reporter4", "error 4")
	require.False(t, result4, "Fourth counter should be rejected since ShouldReport checks maxCounters")

	// Existing keys should still work
	result5 := limiter.ShouldReport(ctx, "source1", "dest1", "event1", "reporter1", "error 5")
	require.True(t, result5, "Existing key should still work")

	// Verify we have 3 counters now (maxCounters check is enforced in ShouldReport)
	limiter.mu.RLock()
	count := len(limiter.counters)
	limiter.mu.RUnlock()
	require.Equal(t, 3, count, "Should have 3 counters since ShouldReport enforces maxCounters")
}

func TestErrorRateLimiter_ShouldReport_EmptyMessage(t *testing.T) {
	t.Parallel()

	conf := config.New()
	conf.Set("Reporting.errorReporting.rateLimit.enabled", true)
	conf.Set("Reporting.errorReporting.rateLimit.maxErrorsPerMinute", 5)

	limiter := NewErrorRateLimiter(logger.NewLogger(), conf)
	ctx := context.Background()

	// Test various empty message scenarios
	testCases := []struct {
		name         string
		errorMessage string
		expected     bool
	}{
		{
			name:         "empty string",
			errorMessage: "",
			expected:     false,
		},
		{
			name:         "whitespace only",
			errorMessage: "   ",
			expected:     false,
		},
		{
			name:         "tabs and spaces",
			errorMessage: "\t\n\r ",
			expected:     false,
		},
		{
			name:         "non-empty message",
			errorMessage: "Database connection failed",
			expected:     true,
		},
		{
			name:         "message with leading/trailing spaces",
			errorMessage: "  Database connection failed  ",
			expected:     true,
		},
	}

	for i, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Use unique keys for each test case to avoid interference
			key := fmt.Sprintf("source%d", i+1)
			result := limiter.ShouldReport(ctx, key, "dest1", "event1", "reporter1", tc.errorMessage)
			require.Equal(t, tc.expected, result, "ShouldReport should return %v for '%s'", tc.expected, tc.errorMessage)
		})
	}

	// Verify that empty messages don't create counters
	// We should have 2 counters: one for "non-empty message" and one for "message with leading/trailing spaces"
	limiter.mu.RLock()
	count := len(limiter.counters)
	limiter.mu.RUnlock()
	require.Equal(t, 2, count, "Should have 2 counters for the non-empty messages")
}

func TestErrorRateLimiter_StartCleanup_MaxCountersTrigger(t *testing.T) {
	t.Parallel()

	conf := config.New()
	conf.Set("Reporting.errorReporting.rateLimit.enabled", true)
	conf.Set("Reporting.errorReporting.rateLimit.maxErrorsPerMinute", 5)
	conf.Set("Reporting.errorReporting.rateLimit.maxCounters", 3)                    // Very low limit for testing
	conf.Set("Reporting.errorReporting.rateLimit.maxCountersCheckInterval", "100ms") // Fast check for testing
	conf.Set("Reporting.errorReporting.rateLimit.minCleanupInterval", "50ms")        // Short interval for testing

	limiter := NewErrorRateLimiter(logger.NewLogger(), conf)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Fill up to the max counters limit
	limiter.ShouldReport(ctx, "source1", "dest1", "event1", "reporter1", "error 1")
	limiter.ShouldReport(ctx, "source2", "dest2", "event2", "reporter2", "error 2")
	limiter.ShouldReport(ctx, "source3", "dest3", "event3", "reporter3", "error 3")

	// Add an expired counter manually
	limiter.mu.Lock()
	limiter.counters["expired"] = &errorCounter{
		count:     1,
		windowEnd: time.Now().Add(-time.Minute), // Expired
		messages:  []string{"expired error"},
	}
	limiter.mu.Unlock()

	// Verify we have 4 counters now
	limiter.mu.RLock()
	count := len(limiter.counters)
	limiter.mu.RUnlock()
	require.Equal(t, 4, count, "Should have 4 counters including expired one")

	// Start the cleanup routine
	limiter.StartCleanup(ctx)

	// Wait for the cleanup to be triggered by max counters check
	time.Sleep(200 * time.Millisecond)

	// Verify the expired counter was cleaned up
	limiter.mu.RLock()
	_, expiredExists := limiter.counters["expired"]
	limiter.mu.RUnlock()
	require.False(t, expiredExists, "Expired counter should be cleaned up by StartCleanup")

	// Verify we still have the 3 active counters
	limiter.mu.RLock()
	count = len(limiter.counters)
	limiter.mu.RUnlock()
	require.Equal(t, 3, count, "Should have 3 active counters after cleanup")
}

func TestErrorRateLimiter_StartCleanup_MinCleanupInterval(t *testing.T) {
	t.Parallel()

	conf := config.New()
	conf.Set("Reporting.errorReporting.rateLimit.enabled", true)
	conf.Set("Reporting.errorReporting.rateLimit.maxErrorsPerMinute", 5)
	conf.Set("Reporting.errorReporting.rateLimit.maxCounters", 1000)                 // High limit to avoid max counters trigger
	conf.Set("Reporting.errorReporting.rateLimit.maxCountersCheckInterval", "100ms") // Fast check for testing
	conf.Set("Reporting.errorReporting.rateLimit.minCleanupInterval", "200ms")       // Short interval for testing

	limiter := NewErrorRateLimiter(logger.NewLogger(), conf)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Add an expired counter manually
	limiter.mu.Lock()
	limiter.counters["expired"] = &errorCounter{
		count:     1,
		windowEnd: time.Now().Add(-time.Minute), // Expired
		messages:  []string{"expired error"},
	}
	limiter.mu.Unlock()

	// Verify we have 1 counter
	limiter.mu.RLock()
	count := len(limiter.counters)
	limiter.mu.RUnlock()
	require.Equal(t, 1, count, "Should have 1 counter")

	// Start the cleanup routine
	limiter.StartCleanup(ctx)

	// Wait for the cleanup to be triggered by min cleanup interval
	time.Sleep(300 * time.Millisecond)

	// Verify the expired counter was cleaned up
	limiter.mu.RLock()
	_, expiredExists := limiter.counters["expired"]
	limiter.mu.RUnlock()
	require.False(t, expiredExists, "Expired counter should be cleaned up by StartCleanup after min interval")

	// Verify we have 0 counters
	limiter.mu.RLock()
	count = len(limiter.counters)
	limiter.mu.RUnlock()
	require.Equal(t, 0, count, "Should have 0 counters after cleanup")
}

func TestErrorRateLimiter_StartCleanup_NoCleanupTriggersFromMethods(t *testing.T) {
	t.Parallel()

	conf := config.New()
	conf.Set("Reporting.errorReporting.rateLimit.enabled", true)
	conf.Set("Reporting.errorReporting.rateLimit.maxErrorsPerMinute", 5)
	conf.Set("Reporting.errorReporting.rateLimit.maxCounters", 3) // Very low limit for testing

	limiter := NewErrorRateLimiter(logger.NewLogger(), conf)
	ctx := context.Background()

	// Fill up to the max counters limit
	limiter.ShouldReport(ctx, "source1", "dest1", "event1", "reporter1", "error 1")
	limiter.ShouldReport(ctx, "source2", "dest2", "event2", "reporter2", "error 2")
	limiter.ShouldReport(ctx, "source3", "dest3", "event3", "reporter3", "error 3")

	// Add an expired counter manually
	limiter.mu.Lock()
	limiter.counters["expired"] = &errorCounter{
		count:     1,
		windowEnd: time.Now().Add(-time.Minute), // Expired
		messages:  []string{"expired error"},
	}
	limiter.mu.Unlock()

	// Call methods that previously triggered cleanup - they should not trigger cleanup now
	limiter.IsBucketFull(ctx, "source4", "dest4", "event4", "reporter4")
	limiter.ShouldReport(ctx, "source4", "dest4", "event4", "reporter4", "error 4")

	// Wait a bit to ensure no cleanup was triggered
	time.Sleep(100 * time.Millisecond)

	// Verify the expired counter still exists (no cleanup was triggered)
	limiter.mu.RLock()
	_, expiredExists := limiter.counters["expired"]
	limiter.mu.RUnlock()
	require.True(t, expiredExists, "Expired counter should still exist since no cleanup was triggered")

	// Verify we still have 4 counters
	limiter.mu.RLock()
	count := len(limiter.counters)
	limiter.mu.RUnlock()
	require.Equal(t, 4, count, "Should have 4 counters since no cleanup was triggered")
}

func TestErrorRateLimiter_StartCleanup_TriggersOnMaxCounters(t *testing.T) {
	t.Parallel()

	conf := config.New()
	conf.Set("Reporting.errorReporting.rateLimit.enabled", true)
	conf.Set("Reporting.errorReporting.rateLimit.maxErrorsPerMinute", 5)
	conf.Set("Reporting.errorReporting.rateLimit.maxCounters", 3)                    // Very low limit for testing
	conf.Set("Reporting.errorReporting.rateLimit.maxCountersCheckInterval", "100ms") // Fast check for testing
	conf.Set("Reporting.errorReporting.rateLimit.minCleanupInterval", "50ms")        // Short interval for testing

	limiter := NewErrorRateLimiter(logger.NewLogger(), conf)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Fill up to the max counters limit
	limiter.ShouldReport(ctx, "source1", "dest1", "event1", "reporter1", "error 1")
	limiter.ShouldReport(ctx, "source2", "dest2", "event2", "reporter2", "error 2")
	limiter.ShouldReport(ctx, "source3", "dest3", "event3", "reporter3", "error 3")

	// Add an expired counter manually
	limiter.mu.Lock()
	limiter.counters["expired"] = &errorCounter{
		count:     1,
		windowEnd: time.Now().Add(-time.Minute), // Expired
		messages:  []string{"expired error"},
	}
	limiter.mu.Unlock()

	// Verify we have 4 counters including expired one
	limiter.mu.RLock()
	count := len(limiter.counters)
	limiter.mu.RUnlock()
	require.Equal(t, 4, count, "Should have 4 counters including expired one")

	// Start the cleanup routine
	limiter.StartCleanup(ctx)

	// Wait for cleanup to run (should trigger additional cleanup since we're at max counters)
	time.Sleep(200 * time.Millisecond)

	// Verify the expired counter was cleaned up
	limiter.mu.RLock()
	_, expiredExists := limiter.counters["expired"]
	limiter.mu.RUnlock()
	require.False(t, expiredExists, "Expired counter should be cleaned up by StartCleanup")

	// Verify we still have the 3 active counters
	limiter.mu.RLock()
	count = len(limiter.counters)
	limiter.mu.RUnlock()
	require.Equal(t, 3, count, "Should have 3 active counters after cleanup")
}

func TestErrorRateLimiter_CleanupInProgress_NonBlocking(t *testing.T) {
	t.Parallel()

	conf := config.New()
	conf.Set("Reporting.errorReporting.rateLimit.enabled", true)
	conf.Set("Reporting.errorReporting.rateLimit.maxErrorsPerMinute", 5)
	conf.Set("Reporting.errorReporting.rateLimit.maxCounters", 1000) // High limit to avoid max counters trigger

	limiter := NewErrorRateLimiter(logger.NewLogger(), conf)
	ctx := context.Background()

	// Add some active counters
	limiter.ShouldReport(ctx, "source1", "dest1", "event1", "reporter1", "error 1")
	limiter.ShouldReport(ctx, "source2", "dest2", "event2", "reporter2", "error 2")

	// Manually set cleanup in progress flag
	atomic.StoreInt32(&limiter.cleanupInProgress, 1)

	// Test IsBucketFull - should return true when cleanup is in progress
	result1 := limiter.IsBucketFull(ctx, "source3", "dest3", "event3", "reporter3")
	require.True(t, result1, "IsBucketFull should return true when cleanup is in progress")

	// Test ShouldReport - should return false when cleanup is in progress
	result2 := limiter.ShouldReport(ctx, "source3", "dest3", "event3", "reporter3", "error 3")
	require.False(t, result2, "ShouldReport should return false when cleanup is in progress")

	// Clear cleanup in progress flag
	atomic.StoreInt32(&limiter.cleanupInProgress, 0)

	// Test that normal behavior resumes
	result3 := limiter.IsBucketFull(ctx, "source3", "dest3", "event3", "reporter3")
	require.False(t, result3, "IsBucketFull should return false when cleanup is not in progress")

	result4 := limiter.ShouldReport(ctx, "source3", "dest3", "event3", "reporter3", "error 3")
	require.True(t, result4, "ShouldReport should return true when cleanup is not in progress")
}
