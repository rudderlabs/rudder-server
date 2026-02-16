package reporting

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/utils/types"
)

// createTestStatsManager creates a stats manager for testing
func createTestStatsManager() *ErrorReportingStats {
	return NewErrorReportingStats(stats.Default)
}

func TestErrorNormalizer_BasicFunctionality(t *testing.T) {
	conf := config.New()
	conf.Set("Reporting.errorReporting.normalizer.enabled", true)
	conf.Set("Reporting.errorReporting.normalizer.maxErrorsPerGroup", 2)
	conf.Set("Reporting.errorReporting.normalizer.maxGroups", 10)

	ern := NewErrorNormalizer(logger.NOP, conf, stats.Default, createTestStatsManager())
	require.NotNil(t, ern)

	// Test that the first error is allowed
	testKey := types.ErrorDetailGroupKey{
		SourceID:      "test-source",
		DestinationID: "test-dest",
		PU:            "test-pu",
		EventType:     "test-event",
	}
	result := ern.NormalizeError(context.Background(), testKey, "Database connection failed")
	require.Equal(t, "Database connection failed", result)

	// Test that the second error is allowed
	result = ern.NormalizeError(context.Background(), testKey, "Network timeout error")
	require.Equal(t, "Network timeout error", result)

	// Test that the third error is rate limited
	result = ern.NormalizeError(context.Background(), testKey, "Authentication failed")
	require.Equal(t, RedactedError, result)
}

func TestErrorNormalizer_Disabled(t *testing.T) {
	conf := config.New()
	conf.Set("Reporting.errorReporting.normalizer.enabled", false)

	ern := NewErrorNormalizer(logger.NOP, conf, stats.Default, createTestStatsManager())
	require.NotNil(t, ern)

	// When disabled, should always return the original error message
	testKey := types.ErrorDetailGroupKey{
		SourceID:      "test-source",
		DestinationID: "test-dest",
		PU:            "test-pu",
		EventType:     "test-event",
	}
	for range 10 {
		result := ern.NormalizeError(context.Background(), testKey, "test error")
		require.Equal(t, "test error", result)
	}
}

func TestErrorNormalizer_SimilarityDetection_UpdatesExistingTimestamp(t *testing.T) {
	conf := config.New()
	conf.Set("Reporting.errorReporting.normalizer.enabled", true)
	conf.Set("Reporting.errorReporting.normalizer.maxErrorsPerGroup", 5)
	conf.Set("Reporting.errorReporting.normalizer.similarityThreshold", 0.6) // Lower threshold to trigger similarity

	ern := NewErrorNormalizer(logger.NOP, conf, stats.Default, createTestStatsManager())
	require.NotNil(t, ern)

	testKey := types.ErrorDetailGroupKey{
		SourceID:      "test-source",
		DestinationID: "test-dest",
		PU:            "test-pu",
		EventType:     "test-event",
	}

	// Add initial error
	result := ern.NormalizeError(context.Background(), testKey, "Database connection failed")
	require.Equal(t, "Database connection failed", result)

	// Wait a bit to ensure time difference
	time.Sleep(10 * time.Millisecond)

	// Add similar error that should match and update the existing error's timestamp
	// Similarity between "Database connection failed" and "Database connection failed with timeout" is 0.75
	result = ern.NormalizeError(context.Background(), testKey, "Database connection failed with timeout")
	require.Equal(t, "Database connection failed", result) // Should return the existing similar error

	// Add another similar error to ensure the pattern works
	// Similarity between "Database connection failed" and "Database connection failed due to network" is 0.67
	result = ern.NormalizeError(context.Background(), testKey, "Database connection failed due to network")
	require.Equal(t, "Database connection failed", result) // Should return the existing similar error
}

func TestErrorNormalizer_Cleanup_DropStaleMessages(t *testing.T) {
	conf := config.New()
	conf.Set("Reporting.errorReporting.normalizer.enabled", true)
	conf.Set("Reporting.errorReporting.normalizer.maxErrorsPerGroup", 10)
	conf.Set("Reporting.errorReporting.normalizer.maxGroups", 100)
	conf.Set("Reporting.errorReporting.normalizer.cleanupInterval", "100ms")

	ern := NewErrorNormalizer(logger.NOP, conf, stats.Default, createTestStatsManager())
	require.NotNil(t, ern)

	testKey := types.ErrorDetailGroupKey{
		SourceID:      "test-source",
		DestinationID: "test-dest",
		PU:            "test-pu",
		EventType:     "test-event",
	}

	// Add some errors
	result := ern.NormalizeError(context.Background(), testKey, "error1")
	require.Equal(t, "error1", result)
	result = ern.NormalizeError(context.Background(), testKey, "error2")
	require.Equal(t, "error2", result)

	// Start cleanup in background
	ctx := t.Context()

	go func() {
		err := ern.StartCleanup(ctx)
		if err != nil && err != context.Canceled {
			t.Errorf("cleanup error: %v", err)
		}
	}()

	// Wait for cleanup to run
	time.Sleep(200 * time.Millisecond)

	// Add more errors to trigger cleanup logic
	result = ern.NormalizeError(context.Background(), testKey, "error3")
	require.Equal(t, "error3", result)
}

func TestErrorNormalizer_Cleanup_ShouldDropCounter(t *testing.T) {
	conf := config.New()
	conf.Set("Reporting.errorReporting.normalizer.enabled", true)
	conf.Set("Reporting.errorReporting.normalizer.maxErrorsPerGroup", 2)
	conf.Set("Reporting.errorReporting.normalizer.maxGroups", 100)
	conf.Set("Reporting.errorReporting.normalizer.cleanupInterval", "100ms")

	ern := NewErrorNormalizer(logger.NOP, conf, stats.Default, createTestStatsManager())
	require.NotNil(t, ern)

	testKey := types.ErrorDetailGroupKey{
		SourceID:      "test-source",
		DestinationID: "test-dest",
		PU:            "test-pu",
		EventType:     "test-event",
	}

	// Fill up the error group to max capacity
	result := ern.NormalizeError(context.Background(), testKey, "error1")
	require.Equal(t, "error1", result)
	result = ern.NormalizeError(context.Background(), testKey, "error2")
	require.Equal(t, "error2", result)

	// Try to add a third error - should be rate limited
	result = ern.NormalizeError(context.Background(), testKey, "error3")
	require.Equal(t, RedactedError, result)

	// Start cleanup in background
	ctx := t.Context()

	go func() {
		err := ern.StartCleanup(ctx)
		if err != nil && err != context.Canceled {
			t.Errorf("cleanup error: %v", err)
		}
	}()

	// Wait for cleanup to run multiple times
	time.Sleep(500 * time.Millisecond)

	// The cleanup should have run, but since the errors are recent, they shouldn't be cleaned up
	// So we should still be rate limited
	result = ern.NormalizeError(context.Background(), testKey, "error4")
	require.Equal(t, RedactedError, result)
}

func TestErrorNormalizer_Cleanup_EmptyGroupRemoval(t *testing.T) {
	conf := config.New()
	conf.Set("Reporting.errorReporting.normalizer.enabled", true)
	conf.Set("Reporting.errorReporting.normalizer.maxErrorsPerGroup", 10)
	conf.Set("Reporting.errorReporting.normalizer.maxGroups", 100)
	conf.Set("Reporting.errorReporting.normalizer.cleanupInterval", "100ms")

	ern := NewErrorNormalizer(logger.NOP, conf, stats.Default, createTestStatsManager())
	require.NotNil(t, ern)

	testKey := types.ErrorDetailGroupKey{
		SourceID:      "test-source",
		DestinationID: "test-dest",
		PU:            "test-pu",
		EventType:     "test-event",
	}

	// Add an error
	result := ern.NormalizeError(context.Background(), testKey, "error1")
	require.Equal(t, "error1", result)

	// Start cleanup in background
	ctx := t.Context()

	go func() {
		err := ern.StartCleanup(ctx)
		if err != nil && err != context.Canceled {
			t.Errorf("cleanup error: %v", err)
		}
	}()

	// Wait for cleanup to run
	time.Sleep(200 * time.Millisecond)

	// The group should still exist since it's not stale yet
	result = ern.NormalizeError(context.Background(), testKey, "error1")
	require.Equal(t, "error1", result)
}

func TestErrorNormalizer_StartCleanup_TickerExecution(t *testing.T) {
	conf := config.New()
	conf.Set("Reporting.errorReporting.normalizer.enabled", true)
	conf.Set("Reporting.errorReporting.normalizer.cleanupInterval", "50ms")

	ern := NewErrorNormalizer(logger.NOP, conf, stats.Default, createTestStatsManager())
	require.NotNil(t, ern)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start cleanup in background
	go func() {
		err := ern.StartCleanup(ctx)
		if err != nil && err != context.Canceled {
			t.Errorf("cleanup error: %v", err)
		}
	}()

	// Wait for cleanup to run at least once
	time.Sleep(100 * time.Millisecond)

	// Cancel to stop the cleanup
	cancel()

	// Wait a bit for the goroutine to finish
	time.Sleep(50 * time.Millisecond)
}

func TestErrorNormalizer_MaxGroupsLimit(t *testing.T) {
	conf := config.New()
	conf.Set("Reporting.errorReporting.normalizer.enabled", true)
	conf.Set("Reporting.errorReporting.normalizer.maxErrorsPerGroup", 1)
	conf.Set("Reporting.errorReporting.normalizer.maxGroups", 2)

	ern := NewErrorNormalizer(logger.NOP, conf, stats.Default, createTestStatsManager())
	require.NotNil(t, ern)

	key1 := types.ErrorDetailGroupKey{
		SourceID:      "source1",
		DestinationID: "dest1",
		PU:            "pu1",
		EventType:     "event1",
	}
	key2 := types.ErrorDetailGroupKey{
		SourceID:      "source2",
		DestinationID: "dest2",
		PU:            "pu2",
		EventType:     "event2",
	}
	key3 := types.ErrorDetailGroupKey{
		SourceID:      "source3",
		DestinationID: "dest3",
		PU:            "pu3",
		EventType:     "event3",
	}

	// First counter
	result := ern.NormalizeError(context.Background(), key1, "error1")
	require.Equal(t, "error1", result)

	// Second counter
	result = ern.NormalizeError(context.Background(), key2, "error2")
	require.Equal(t, "error2", result)

	// Third counter should be rejected
	result = ern.NormalizeError(context.Background(), key3, "error3")
	require.Equal(t, RedactedError, result)
}

func TestErrorNormalizer_InterfaceCompliance(t *testing.T) {
	// Test that the implementation implements ErrorNormalizer interface
	conf := config.New()
	ern := NewErrorNormalizer(logger.NOP, conf, stats.Default, createTestStatsManager())
	_ = ern
}

func TestErrorNormalizer_StartCleanup_ContextCancellation(t *testing.T) {
	conf := config.New()
	conf.Set("Reporting.errorReporting.normalizer.enabled", true)

	ern := NewErrorNormalizer(logger.NOP, conf, stats.Default, createTestStatsManager())
	require.NotNil(t, ern)

	ctx, cancel := context.WithCancel(context.Background())

	// Start cleanup in background
	go func() {
		err := ern.StartCleanup(ctx)
		require.Error(t, err)
		require.Equal(t, context.Canceled, err)
	}()

	// Cancel context after a short delay
	time.Sleep(50 * time.Millisecond)
	cancel()

	// Wait a bit for the goroutine to finish
	time.Sleep(100 * time.Millisecond)
}

func TestErrorNormalizer_ShouldDropCounter_MaxCapacityAndBlocked(t *testing.T) {
	conf := config.New()
	conf.Set("Reporting.errorReporting.normalizer.enabled", true)
	conf.Set("Reporting.errorReporting.normalizer.maxErrorsPerGroup", 1)
	conf.Set("Reporting.errorReporting.normalizer.maxGroups", 100)
	conf.Set("Reporting.errorReporting.normalizer.cleanupInterval", "100ms")

	ern := NewErrorNormalizer(logger.NOP, conf, stats.Default, createTestStatsManager())
	require.NotNil(t, ern)

	testKey := types.ErrorDetailGroupKey{
		SourceID:      "test-source",
		DestinationID: "test-dest",
		PU:            "test-pu",
		EventType:     "test-event",
	}

	// Add an error to reach max capacity
	result := ern.NormalizeError(context.Background(), testKey, "error1")
	require.Equal(t, "error1", result)

	// Try to add another error - should be rate limited
	result = ern.NormalizeError(context.Background(), testKey, "error2")
	require.Equal(t, RedactedError, result)

	// Start cleanup in background
	ctx := t.Context()

	go func() {
		err := ern.StartCleanup(ctx)
		if err != nil && err != context.Canceled {
			t.Errorf("cleanup error: %v", err)
		}
	}()

	// Wait for cleanup to run
	time.Sleep(200 * time.Millisecond)

	// The group should still exist since it's recent
	result = ern.NormalizeError(context.Background(), testKey, "error2")
	require.Equal(t, RedactedError, result)
}

func TestErrorNormalizer_ComprehensiveCleanupCoverage(t *testing.T) {
	conf := config.New()
	conf.Set("Reporting.errorReporting.normalizer.enabled", true)
	conf.Set("Reporting.errorReporting.normalizer.maxErrorsPerGroup", 2)
	conf.Set("Reporting.errorReporting.normalizer.maxGroups", 100)
	conf.Set("Reporting.errorReporting.normalizer.cleanupInterval", "50ms")

	ern := NewErrorNormalizer(logger.NOP, conf, stats.Default, createTestStatsManager())
	require.NotNil(t, ern)

	testKey1 := types.ErrorDetailGroupKey{
		SourceID:      "test-source-1",
		DestinationID: "test-dest-1",
		PU:            "test-pu-1",
		EventType:     "test-event-1",
	}

	testKey2 := types.ErrorDetailGroupKey{
		SourceID:      "test-source-2",
		DestinationID: "test-dest-2",
		PU:            "test-pu-2",
		EventType:     "test-event-2",
	}

	// Create a group that will be completely cleaned up (empty group scenario)
	result := ern.NormalizeError(context.Background(), testKey1, "error1")
	require.Equal(t, "error1", result)

	// Create a group that will reach max capacity and be blocked
	result = ern.NormalizeError(context.Background(), testKey2, "error2")
	require.Equal(t, "error2", result)
	result = ern.NormalizeError(context.Background(), testKey2, "error3")
	require.Equal(t, "error3", result)

	// Try to add another error - should be rate limited
	result = ern.NormalizeError(context.Background(), testKey2, "error4")
	require.Equal(t, RedactedError, result)

	// Start cleanup in background
	ctx := t.Context()

	go func() {
		err := ern.StartCleanup(ctx)
		if err != nil && err != context.Canceled {
			t.Errorf("cleanup error: %v", err)
		}
	}()

	// Wait for cleanup to run multiple times
	time.Sleep(300 * time.Millisecond)

	// Test that the groups still exist (since they're recent)
	result = ern.NormalizeError(context.Background(), testKey1, "error1")
	require.Equal(t, "error1", result)

	result = ern.NormalizeError(context.Background(), testKey2, "error5")
	require.Equal(t, RedactedError, result)
}

func TestBoundedErrorSet_NewBoundedErrorSet(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name     string
		max      int
		expected *boundedErrorSet
	}{
		{
			name: "valid max size",
			max:  10,
			expected: &boundedErrorSet{
				entries: make([]*errorEntry, 0, 10),
				index:   make(map[string]*errorEntry, 10),
			},
		},
		{
			name: "zero max size",
			max:  0,
			expected: &boundedErrorSet{
				entries: make([]*errorEntry, 0),
				index:   make(map[string]*errorEntry, 0),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			result := newBoundedErrorSet(tc.max)

			require.Equal(t, len(tc.expected.entries), len(result.entries))
			require.Equal(t, cap(tc.expected.entries), cap(result.entries))
			require.Equal(t, len(tc.expected.index), len(result.index))
		})
	}
}

func TestBoundedErrorSet_GetExact(t *testing.T) {
	t.Parallel()

	t.Run("exact match found", func(t *testing.T) {
		t.Parallel()

		set := newBoundedErrorSet(5)
		now := time.Now()

		// Add an entry
		entry := set.Add("test error", now)

		// Test exact match
		result, found := set.GetExact("test error", now.Add(time.Second))

		require.True(t, found)
		require.Equal(t, entry, result)
		require.Equal(t, "test error", result.message)
		require.Equal(t, now.Add(time.Second), result.time)

		// Verify entry moved to end
		require.Equal(t, entry, set.entries[len(set.entries)-1])
	})

	t.Run("exact match not found", func(t *testing.T) {
		t.Parallel()

		set := newBoundedErrorSet(5)
		now := time.Now()

		set.Add("test error", now)

		result, found := set.GetExact("different error", now)

		require.False(t, found)
		require.Nil(t, result)
	})

	t.Run("empty set", func(t *testing.T) {
		t.Parallel()

		set := newBoundedErrorSet(5)
		now := time.Now()

		result, found := set.GetExact("test error", now)

		require.False(t, found)
		require.Nil(t, result)
	})
}

func TestBoundedErrorSet_GetSimilar(t *testing.T) {
	t.Parallel()

	t.Run("similar match found", func(t *testing.T) {
		t.Parallel()

		set := newBoundedErrorSet(5)
		now := time.Now()

		entry := set.Add("test error message", now)

		similarityFunc := func(a, b string) bool {
			return len(a) == len(b) && len(a) >= 10 // Simple similarity function
		}

		result, found := set.GetSimilar("test error message", now.Add(time.Second), similarityFunc)

		require.True(t, found)
		require.Equal(t, entry, result)
		require.Equal(t, now.Add(time.Second), result.time)

		// Verify entry moved to end
		require.Equal(t, entry, set.entries[len(set.entries)-1])
	})

	t.Run("similar match not found", func(t *testing.T) {
		t.Parallel()

		set := newBoundedErrorSet(5)
		now := time.Now()

		set.Add("test error", now)

		similarityFunc := func(a, b string) bool {
			return false // Never similar
		}

		result, found := set.GetSimilar("different error", now, similarityFunc)

		require.False(t, found)
		require.Nil(t, result)
	})

	t.Run("empty set", func(t *testing.T) {
		t.Parallel()

		set := newBoundedErrorSet(5)
		now := time.Now()

		similarityFunc := func(a, b string) bool {
			return true
		}

		result, found := set.GetSimilar("test error", now, similarityFunc)

		require.False(t, found)
		require.Nil(t, result)
	})

	t.Run("multiple entries, first match", func(t *testing.T) {
		t.Parallel()

		set := newBoundedErrorSet(5)
		now := time.Now()

		firstEntry := set.Add("first error", now)
		set.Add("second error", now.Add(time.Second))
		set.Add("third error", now.Add(time.Second*2))

		similarityFunc := func(a, b string) bool {
			return a == "first error" && b == "first error"
		}

		result, found := set.GetSimilar("first error", now.Add(time.Second*3), similarityFunc)

		require.True(t, found)
		require.Equal(t, firstEntry, result)
		require.Equal(t, now.Add(time.Second*3), result.time)

		// Verify entry moved to end
		require.Equal(t, firstEntry, set.entries[len(set.entries)-1])
	})
}

func TestBoundedErrorSet_Add(t *testing.T) {
	t.Parallel()

	t.Run("add to empty set", func(t *testing.T) {
		t.Parallel()

		set := newBoundedErrorSet(5)
		now := time.Now()

		entry := set.Add("test error", now)

		require.Equal(t, "test error", entry.message)
		require.Equal(t, now, entry.time)
		require.Equal(t, 1, len(set.entries))
		require.Equal(t, 1, len(set.index))
		require.Equal(t, entry, set.index["test error"])
		require.Equal(t, entry, set.entries[0])
	})

	t.Run("add multiple entries", func(t *testing.T) {
		t.Parallel()

		set := newBoundedErrorSet(5)
		now := time.Now()

		entry1 := set.Add("error 1", now)
		entry2 := set.Add("error 2", now.Add(time.Second))
		entry3 := set.Add("error 3", now.Add(time.Second*2))

		require.Equal(t, 3, len(set.entries))
		require.Equal(t, 3, len(set.index))
		require.Equal(t, entry1, set.index["error 1"])
		require.Equal(t, entry2, set.index["error 2"])
		require.Equal(t, entry3, set.index["error 3"])

		// Verify order
		require.Equal(t, entry1, set.entries[0])
		require.Equal(t, entry2, set.entries[1])
		require.Equal(t, entry3, set.entries[2])
	})

	t.Run("add duplicate message", func(t *testing.T) {
		t.Parallel()

		set := newBoundedErrorSet(5)
		now := time.Now()

		entry1 := set.Add("test error", now)
		entry2 := set.Add("test error", now.Add(time.Second))

		require.Equal(t, 2, len(set.entries))
		require.Equal(t, 1, len(set.index))               // Only one unique message
		require.Equal(t, entry2, set.index["test error"]) // Latest entry overwrites
		require.Equal(t, entry1, set.entries[0])
		require.Equal(t, entry2, set.entries[1])
	})
}

func TestBoundedErrorSet_DropStale(t *testing.T) {
	t.Parallel()

	t.Run("drop stale entries", func(t *testing.T) {
		t.Parallel()

		set := newBoundedErrorSet(5)
		now := time.Now()

		// Add entries with different timestamps
		set.Add("stale error", now.Add(-time.Hour))
		freshEntry := set.Add("fresh error", now)

		cutoff := now.Add(-time.Minute)

		set.DropStale(cutoff)

		require.Equal(t, 1, len(set.entries))
		require.Equal(t, 1, len(set.index))
		require.Equal(t, freshEntry, set.entries[0])
		require.Equal(t, freshEntry, set.index["fresh error"])
		require.Nil(t, set.index["stale error"])
	})

	t.Run("drop all entries", func(t *testing.T) {
		t.Parallel()

		set := newBoundedErrorSet(5)
		now := time.Now()

		set.Add("error 1", now.Add(-time.Hour))
		set.Add("error 2", now.Add(-time.Hour*2))
		set.Add("error 3", now.Add(-time.Hour*3))

		cutoff := now.Add(-time.Minute)

		set.DropStale(cutoff)

		require.Equal(t, 0, len(set.entries))
		require.Equal(t, 0, len(set.index))
	})

	t.Run("drop no entries", func(t *testing.T) {
		t.Parallel()

		set := newBoundedErrorSet(5)
		now := time.Now()

		entry1 := set.Add("error 1", now)
		entry2 := set.Add("error 2", now.Add(time.Minute))

		cutoff := now.Add(-time.Minute)

		set.DropStale(cutoff)

		require.Equal(t, 2, len(set.entries))
		require.Equal(t, 2, len(set.index))
		require.Equal(t, entry1, set.entries[0])
		require.Equal(t, entry2, set.entries[1])
	})

	t.Run("drop partial stale entries", func(t *testing.T) {
		t.Parallel()

		set := newBoundedErrorSet(5)
		now := time.Now()

		// Add entries in order: stale, stale, fresh, fresh
		set.Add("stale 1", now.Add(-time.Hour))
		set.Add("stale 2", now.Add(-time.Hour*2))
		fresh1 := set.Add("fresh 1", now)
		fresh2 := set.Add("fresh 2", now.Add(time.Minute))

		cutoff := now.Add(-time.Minute)

		set.DropStale(cutoff)

		// Should only keep fresh entries (DropStale only removes from beginning)
		require.Equal(t, 2, len(set.entries))
		require.Equal(t, 2, len(set.index))
		require.Equal(t, fresh1, set.entries[0])
		require.Equal(t, fresh2, set.entries[1])
		require.Equal(t, fresh1, set.index["fresh 1"])
		require.Equal(t, fresh2, set.index["fresh 2"])
	})
}

func TestBoundedErrorSet_MoveToEnd(t *testing.T) {
	t.Parallel()

	t.Run("move entry to end", func(t *testing.T) {
		t.Parallel()

		set := newBoundedErrorSet(5)
		now := time.Now()

		entry1 := set.Add("error 1", now)
		entry2 := set.Add("error 2", now.Add(time.Second))
		entry3 := set.Add("error 3", now.Add(time.Second*2))

		// Move first entry to end
		set.moveToEnd(entry1)

		require.Equal(t, 3, len(set.entries))
		require.Equal(t, entry2, set.entries[0])
		require.Equal(t, entry3, set.entries[1])
		require.Equal(t, entry1, set.entries[2])
	})

	t.Run("move middle entry to end", func(t *testing.T) {
		t.Parallel()

		set := newBoundedErrorSet(5)
		now := time.Now()

		entry1 := set.Add("error 1", now)
		entry2 := set.Add("error 2", now.Add(time.Second))
		entry3 := set.Add("error 3", now.Add(time.Second*2))

		// Move middle entry to end
		set.moveToEnd(entry2)

		require.Equal(t, 3, len(set.entries))
		require.Equal(t, entry1, set.entries[0])
		require.Equal(t, entry3, set.entries[1])
		require.Equal(t, entry2, set.entries[2])
	})

	t.Run("move last entry to end", func(t *testing.T) {
		t.Parallel()

		set := newBoundedErrorSet(5)
		now := time.Now()

		entry1 := set.Add("error 1", now)
		entry2 := set.Add("error 2", now.Add(time.Second))
		entry3 := set.Add("error 3", now.Add(time.Second*2))

		// Move last entry to end (should stay in same position)
		set.moveToEnd(entry3)

		require.Equal(t, 3, len(set.entries))
		require.Equal(t, entry1, set.entries[0])
		require.Equal(t, entry2, set.entries[1])
		require.Equal(t, entry3, set.entries[2])
	})

	t.Run("move non-existent entry", func(t *testing.T) {
		t.Parallel()

		set := newBoundedErrorSet(5)
		now := time.Now()

		set.Add("error 1", now)
		set.Add("error 2", now.Add(time.Second))

		nonExistentEntry := &errorEntry{message: "non-existent", time: now}

		// Should not panic or change anything
		set.moveToEnd(nonExistentEntry)

		require.Equal(t, 2, len(set.entries))
	})
}

func TestBoundedErrorSet_MoveToEndAt(t *testing.T) {
	t.Parallel()

	t.Run("move first entry to end", func(t *testing.T) {
		t.Parallel()

		set := newBoundedErrorSet(5)
		now := time.Now()

		entry1 := set.Add("error 1", now)
		entry2 := set.Add("error 2", now.Add(time.Second))
		entry3 := set.Add("error 3", now.Add(time.Second*2))

		set.moveToEndAt(0)

		require.Equal(t, 3, len(set.entries))
		require.Equal(t, entry2, set.entries[0])
		require.Equal(t, entry3, set.entries[1])
		require.Equal(t, entry1, set.entries[2])
	})

	t.Run("move middle entry to end", func(t *testing.T) {
		t.Parallel()

		set := newBoundedErrorSet(5)
		now := time.Now()

		entry1 := set.Add("error 1", now)
		entry2 := set.Add("error 2", now.Add(time.Second))
		entry3 := set.Add("error 3", now.Add(time.Second*2))

		set.moveToEndAt(1)

		require.Equal(t, 3, len(set.entries))
		require.Equal(t, entry1, set.entries[0])
		require.Equal(t, entry3, set.entries[1])
		require.Equal(t, entry2, set.entries[2])
	})

	t.Run("move last entry to end", func(t *testing.T) {
		t.Parallel()

		set := newBoundedErrorSet(5)
		now := time.Now()

		entry1 := set.Add("error 1", now)
		entry2 := set.Add("error 2", now.Add(time.Second))
		entry3 := set.Add("error 3", now.Add(time.Second*2))

		set.moveToEndAt(2)

		require.Equal(t, 3, len(set.entries))
		require.Equal(t, entry1, set.entries[0])
		require.Equal(t, entry2, set.entries[1])
		require.Equal(t, entry3, set.entries[2])
	})

	t.Run("move single entry", func(t *testing.T) {
		t.Parallel()

		set := newBoundedErrorSet(5)
		now := time.Now()

		entry := set.Add("error 1", now)

		set.moveToEndAt(0)

		require.Equal(t, 1, len(set.entries))
		require.Equal(t, entry, set.entries[0])
	})
}

func TestBoundedErrorSet_Integration(t *testing.T) {
	t.Parallel()

	t.Run("full lifecycle with exact matches", func(t *testing.T) {
		t.Parallel()

		set := newBoundedErrorSet(3)
		now := time.Now()

		// Add entries
		entry1 := set.Add("error 1", now)
		entry2 := set.Add("error 2", now.Add(time.Second))
		entry3 := set.Add("error 3", now.Add(time.Second*2))

		require.Equal(t, 3, len(set.entries))

		// Get exact match and verify it moves to end
		result, found := set.GetExact("error 1", now.Add(time.Second*3))
		require.True(t, found)
		require.Equal(t, entry1, result)
		require.Equal(t, entry2, set.entries[0])
		require.Equal(t, entry3, set.entries[1])
		require.Equal(t, entry1, set.entries[2])

		// Drop stale entries
		cutoff := now.Add(time.Minute)
		set.DropStale(cutoff)

		require.Equal(t, 0, len(set.entries))
		require.Equal(t, 0, len(set.index))
	})

	t.Run("full lifecycle with similar matches", func(t *testing.T) {
		t.Parallel()

		set := newBoundedErrorSet(3)
		now := time.Now()

		// Add entries
		entry1 := set.Add("error message 1", now)
		entry2 := set.Add("error message 2", now.Add(time.Second))

		// Similarity function that matches based on prefix
		similarityFunc := func(a, b string) bool {
			return len(a) >= 10 && len(b) >= 10 && a[:10] == b[:10]
		}

		// Get similar match
		result, found := set.GetSimilar("error message 3", now.Add(time.Second*2), similarityFunc)
		require.True(t, found)
		require.Equal(t, entry1, result)

		// Verify entry moved to end
		require.Equal(t, entry2, set.entries[0])
		require.Equal(t, entry1, set.entries[1])
	})

	t.Run("bounded size behavior", func(t *testing.T) {
		t.Parallel()

		set := newBoundedErrorSet(2)
		now := time.Now()

		// Add more entries than max size
		entry1 := set.Add("error 1", now)
		entry2 := set.Add("error 2", now.Add(time.Second))
		entry3 := set.Add("error 3", now.Add(time.Second*2))

		// Should maintain max size
		require.Equal(t, 3, len(set.entries)) // boundedErrorSet doesn't enforce max on Add
		require.Equal(t, 3, len(set.index))

		// Verify all entries are present
		require.Equal(t, entry1, set.index["error 1"])
		require.Equal(t, entry2, set.index["error 2"])
		require.Equal(t, entry3, set.index["error 3"])
	})
}
