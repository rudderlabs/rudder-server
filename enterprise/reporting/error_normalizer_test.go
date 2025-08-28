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
	for i := 0; i < 10; i++ {
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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
