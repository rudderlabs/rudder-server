package reporting

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
)

func TestErrorLimiter_BasicFunctionality(t *testing.T) {
	conf := config.New()
	conf.Set("Reporting.errorReporting.rateLimit.enabled", true)
	conf.Set("Reporting.errorReporting.rateLimit.maxErrorsPerMinute", 2)
	conf.Set("Reporting.errorReporting.rateLimit.maxCounters", 10)

	erl := NewErrorLimiter(logger.NOP, conf)
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
}

func TestErrorLimiter_Disabled(t *testing.T) {
	conf := config.New()
	conf.Set("Reporting.errorReporting.rateLimit.enabled", false)

	erl := NewErrorLimiter(logger.NOP, conf)
	require.NotNil(t, erl)

	// When disabled, should always return the original error message
	for i := 0; i < 10; i++ {
		result := erl.CanonicalizeError(context.Background(), "test-key", "test error")
		require.Equal(t, "test error", result)
	}
}

func TestErrorLimiter_SimilarityDetection(t *testing.T) {
	conf := config.New()
	conf.Set("Reporting.errorReporting.rateLimit.enabled", true)
	conf.Set("Reporting.errorReporting.rateLimit.maxErrorsPerMinute", 2)
	conf.Set("Reporting.errorReporting.rateLimit.similarityThreshold", 0.9) // Higher threshold for testing

	erl := NewErrorLimiter(logger.NOP, conf)
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
	result = erl.CanonicalizeError(context.Background(), "test-key", "Network timeout")
	require.Equal(t, "UnknownError", result)
}

func TestErrorLimiter_MaxCountersLimit(t *testing.T) {
	conf := config.New()
	conf.Set("Reporting.errorReporting.rateLimit.enabled", true)
	conf.Set("Reporting.errorReporting.rateLimit.maxErrorsPerMinute", 1)
	conf.Set("Reporting.errorReporting.rateLimit.maxCounters", 2)

	erl := NewErrorLimiter(logger.NOP, conf)
	require.NotNil(t, erl)

	// First counter
	result := erl.CanonicalizeError(context.Background(), "key1", "error1")
	require.Equal(t, "error1", result)

	// Second counter
	result = erl.CanonicalizeError(context.Background(), "key2", "error2")
	require.Equal(t, "error2", result)

	// Third counter should be rejected
	result = erl.CanonicalizeError(context.Background(), "key3", "error3")
	require.Equal(t, "UnknownError", result)
}

func TestErrorLimiter_InterfaceCompliance(t *testing.T) {
	// Test that the implementation implements ErrorLimiter interface
	conf := config.New()
	erl := NewErrorLimiter(logger.NOP, conf)
	_ = erl
}

func TestErrorLimiter_StartCleanup_ContextCancellation(t *testing.T) {
	conf := config.New()
	conf.Set("Reporting.errorReporting.rateLimit.enabled", true)

	erl := NewErrorLimiter(logger.NOP, conf)
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
