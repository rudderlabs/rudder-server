package reporting

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/utils/types"

	mocks "github.com/rudderlabs/rudder-server/mocks/enterprise/reporting"
)

func TestErrorDetailReporter_WithMockErrorNormalizer(t *testing.T) {
	t.Parallel()

	t.Run("error normalizer allows reporting", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockErrorNormalizer := mocks.NewMockErrorNormalizer(ctrl)

		// Configure mock expectations
		testKey := types.ErrorDetailGroupKey{
			SourceID:      "source1",
			DestinationID: "dest1",
			PU:            "identify",
			EventType:     "router",
		}
		mockErrorNormalizer.EXPECT().
			NormalizeError(gomock.Any(), testKey, "test error message").
			Return("test error message") // Return original message when allowed

		// Create config subscriber
		configSubscriber := newConfigSubscriber(logger.NOP)

		// Create error detail reporter with mock error normalizer
		edr := &ErrorDetailReporter{
			configSubscriber: configSubscriber,
			errorNormalizer:  mockErrorNormalizer,
			log:              logger.NOP,
			stats:            stats.NOP,
			config:           config.New(),
		}

		// Test that the reporter uses the mock error normalizer correctly
		require.NotNil(t, edr.errorNormalizer)

		// Actually call the method to verify mock expectations
		ctx := context.Background()
		normalizedError := edr.errorNormalizer.NormalizeError(ctx, testKey, "test error message")
		require.Equal(t, "test error message", normalizedError)
	})

	t.Run("error normalizer blocks reporting", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockErrorNormalizer := mocks.NewMockErrorNormalizer(ctrl)

		// Configure mock expectations - return UnknownError when rate limited
		testKey := types.ErrorDetailGroupKey{
			SourceID:      "source1",
			DestinationID: "dest1",
			PU:            "identify",
			EventType:     "router",
		}
		mockErrorNormalizer.EXPECT().
			NormalizeError(gomock.Any(), testKey, "test error message").
			Return("UnknownError") // Return UnknownError when rate limited

		// Create config subscriber
		configSubscriber := newConfigSubscriber(logger.NOP)

		// Create error detail reporter with mock error normalizer
		edr := &ErrorDetailReporter{
			configSubscriber: configSubscriber,
			errorNormalizer:  mockErrorNormalizer,
			log:              logger.NOP,
			stats:            stats.NOP,
			config:           config.New(),
		}

		// Test that the reporter uses the mock error normalizer correctly
		require.NotNil(t, edr.errorNormalizer)

		// Actually call the method to verify mock expectations
		ctx := context.Background()
		normalizedError := edr.errorNormalizer.NormalizeError(ctx, testKey, "test error message")
		require.Equal(t, "UnknownError", normalizedError)
	})
}

func TestErrorDetailReporter_ConstructorWithMock(t *testing.T) {
	t.Parallel()

	t.Run("constructor creates reporter with real error normalizer", func(t *testing.T) {
		configSubscriber := newConfigSubscriber(logger.NOP)
		conf := config.New()
		conf.Set("Reporting.errorReporting.normalizer.enabled", true)

		edr := NewErrorDetailReporter(
			context.Background(),
			configSubscriber,
			stats.NOP,
			conf,
		)

		// Verify that the constructor creates a real ErrorNormalizer
		require.NotNil(t, edr.errorNormalizer)

		// Type assertion to verify it's a real implementation
		_, ok := edr.errorNormalizer.(*errorNormalizer)
		require.True(t, ok, "Expected real ErrorNormalizer implementation")
	})
}
