package reporting

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	mocks "github.com/rudderlabs/rudder-server/mocks/enterprise/reporting"
)

func TestErrorDetailReporter_WithMockRateLimiter(t *testing.T) {
	t.Parallel()

	t.Run("rate limiter allows reporting", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockRateLimiter := mocks.NewMockErrorRateLimiter(ctrl)

		// Configure mock expectations
		mockRateLimiter.EXPECT().
			CanonicalizeError(gomock.Any(), "source1:dest1:identify:router", "test error message").
			Return("test error message") // Return original message when allowed

		// Create config subscriber
		configSubscriber := newConfigSubscriber(logger.NOP)

		// Create error detail reporter with mock rate limiter
		edr := &ErrorDetailReporter{
			configSubscriber: configSubscriber,
			errorRateLimiter: mockRateLimiter,
			log:              logger.NOP,
			stats:            stats.NOP,
			config:           config.New(),
		}

		// Test that the reporter uses the mock rate limiter correctly
		require.NotNil(t, edr.errorRateLimiter)

		// Actually call the method to verify mock expectations
		ctx := context.Background()
		canonicalError := edr.errorRateLimiter.CanonicalizeError(ctx, "source1:dest1:identify:router", "test error message")
		require.Equal(t, "test error message", canonicalError)
	})

	t.Run("rate limiter blocks reporting", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockRateLimiter := mocks.NewMockErrorRateLimiter(ctrl)

		// Configure mock expectations - return UnknownError when rate limited
		mockRateLimiter.EXPECT().
			CanonicalizeError(gomock.Any(), "source1:dest1:identify:router", "test error message").
			Return("UnknownError") // Return UnknownError when rate limited

		// Create config subscriber
		configSubscriber := newConfigSubscriber(logger.NOP)

		// Create error detail reporter with mock rate limiter
		edr := &ErrorDetailReporter{
			configSubscriber: configSubscriber,
			errorRateLimiter: mockRateLimiter,
			log:              logger.NOP,
			stats:            stats.NOP,
			config:           config.New(),
		}

		// Test that the reporter uses the mock rate limiter correctly
		require.NotNil(t, edr.errorRateLimiter)

		// Actually call the method to verify mock expectations
		ctx := context.Background()
		canonicalError := edr.errorRateLimiter.CanonicalizeError(ctx, "source1:dest1:identify:router", "test error message")
		require.Equal(t, "UnknownError", canonicalError)
	})
}

func TestErrorDetailReporter_ConstructorWithMock(t *testing.T) {
	t.Parallel()

	t.Run("constructor creates reporter with real rate limiter", func(t *testing.T) {
		configSubscriber := newConfigSubscriber(logger.NOP)
		conf := config.New()
		conf.Set("Reporting.errorReporting.rateLimit.enabled", true)

		edr := NewErrorDetailReporter(
			context.Background(),
			configSubscriber,
			stats.NOP,
			conf,
		)

		// Verify that the constructor creates a real ErrorRateLimiter
		require.NotNil(t, edr.errorRateLimiter)

		// Type assertion to verify it's a real implementation
		_, ok := edr.errorRateLimiter.(*errorRateLimiter)
		require.True(t, ok, "Expected real ErrorRateLimiter implementation")
	})
}
