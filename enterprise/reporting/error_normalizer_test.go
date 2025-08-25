package reporting

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/utils/types"
)

func TestErrorNormalizer_BasicFunctionality(t *testing.T) {
	conf := config.New()
	conf.Set("Reporting.errorReporting.normalizer.enabled", true)
	conf.Set("Reporting.errorReporting.normalizer.maxErrorsPerMinute", 2)
	conf.Set("Reporting.errorReporting.normalizer.maxCounters", 10)

	ern := NewErrorNormalizer(logger.NOP, conf)
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
	require.Equal(t, "UnknownError", result)
}

func TestErrorNormalizer_Disabled(t *testing.T) {
	conf := config.New()
	conf.Set("Reporting.errorReporting.normalizer.enabled", false)

	ern := NewErrorNormalizer(logger.NOP, conf)
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

func TestErrorNormalizer_SimilarityDetection(t *testing.T) {
	conf := config.New()
	conf.Set("Reporting.errorReporting.normalizer.enabled", true)
	conf.Set("Reporting.errorReporting.normalizer.maxErrorsPerMinute", 2)
	conf.Set("Reporting.errorReporting.normalizer.similarityThreshold", 0.9) // Higher threshold for testing

	ern := NewErrorNormalizer(logger.NOP, conf)
	require.NotNil(t, ern)

	testKey := types.ErrorDetailGroupKey{
		SourceID:      "test-source",
		DestinationID: "test-dest",
		PU:            "test-pu",
		EventType:     "test-event",
	}

	// First error
	result := ern.NormalizeError(context.Background(), testKey, "Database connection failed")
	require.Equal(t, "Database connection failed", result)

	// Similar error should return the normalized error without consuming quota
	result = ern.NormalizeError(context.Background(), testKey, "Database connection failed")
	require.Equal(t, "Database connection failed", result)

	// Different error should consume quota
	result = ern.NormalizeError(context.Background(), testKey, "Authentication failed")
	require.Equal(t, "Authentication failed", result)

	// Third different error should be rate limited
	result = ern.NormalizeError(context.Background(), testKey, "Network timeout")
	require.Equal(t, "UnknownError", result)
}

func TestErrorNormalizer_MaxCountersLimit(t *testing.T) {
	conf := config.New()
	conf.Set("Reporting.errorReporting.normalizer.enabled", true)
	conf.Set("Reporting.errorReporting.normalizer.maxErrorsPerMinute", 1)
	conf.Set("Reporting.errorReporting.normalizer.maxCounters", 2)

	ern := NewErrorNormalizer(logger.NOP, conf)
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
	require.Equal(t, "UnknownError", result)
}

func TestErrorNormalizer_InterfaceCompliance(t *testing.T) {
	// Test that the implementation implements ErrorNormalizer interface
	conf := config.New()
	ern := NewErrorNormalizer(logger.NOP, conf)
	_ = ern
}

func TestErrorNormalizer_StartCleanup_ContextCancellation(t *testing.T) {
	conf := config.New()
	conf.Set("Reporting.errorReporting.normalizer.enabled", true)

	ern := NewErrorNormalizer(logger.NOP, conf)
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
