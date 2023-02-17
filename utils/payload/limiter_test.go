package payload

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPayloadLimiter(t *testing.T) {
	t.Run("free memory ticker", func(t *testing.T) {
		var freeMem float64
		freeMemFunc := func() (float64, error) {
			return freeMem, nil
		}

		limiter := NewAdaptiveLimiter(
			AdaptiveLimiterConfig{
				FreeMemory: freeMemFunc,
			},
		)

		freeMem = 0 // 0% free memory -> critical state
		limiter.tick()
		require.EqualValues(
			t,
			LimiterStateCritical,
			limiter.Stats().State,
			"limiter should be in a critical state",
		)
		require.EqualValues(
			t,
			1,
			limiter.Limit(100),
			"payload limit should be 1",
		)
		require.EqualValues(
			t,
			2,
			limiter.Stats().ThresholdFactor,
			"threshold factor should be 2",
		)

		freeMem = 11 // 11% free memory -> above threshold
		limiter.tick()
		require.EqualValues(
			t,
			LimiterStateThreshold,
			limiter.Stats().State,
			"limiter should be in a threshold state",
		)
		require.EqualValues(
			t,
			2,
			limiter.Stats().ThresholdFactor,
			"threshold factor should be 2",
		)
		require.EqualValues(
			t,
			int64(80),
			limiter.Limit(100),
			"payload limit should be 80",
		)

		freeMem = 45 // 45% free memory -> below threshold -> normal mode -> should reset threshold factor
		limiter.tick()
		require.EqualValues(
			t,
			LimiterStateNormal,
			limiter.Stats().State,
			"limiter should be in a normal state",
		)
		require.EqualValues(
			t,
			100,
			limiter.Limit(100),
			"payload limit should be 100",
		)
		require.EqualValues(
			t,
			1,
			limiter.Stats().ThresholdFactor,
			"threshold factor should be 1",
		)

		freeMem = 15 // 15% free memory -> above threshold -> shouldn't increase threshold factor
		limiter.tick()
		require.EqualValues(
			t,
			LimiterStateThreshold,
			limiter.Stats().State,
			"limiter should be in a threshold state",
		)
		require.EqualValues(
			t,
			1,
			limiter.Stats().ThresholdFactor,
			"threshold factor should be 1",
		)
		require.EqualValues(
			t,
			90,
			limiter.Limit(100),
			"payload limit should be 90",
		)

		freeMem = 9 // 9% free memory -> critical state
		limiter.tick()
		require.EqualValues(
			t,
			LimiterStateCritical,
			limiter.Stats().State,
			"limiter should be in a critical state",
		)
		require.EqualValues(
			t,
			1,
			limiter.Limit(100),
			"payload limit should be 1",
		)
		require.EqualValues(
			t,
			2,
			limiter.Stats().ThresholdFactor,
			"threshold factor should be 2",
		)

		freeMem = 11 // 11% free memory -> above critical
		limiter.tick()
		require.EqualValues(
			t,
			LimiterStateThreshold,
			limiter.Stats().State,
			"limiter should be in a threshold state",
		)
		require.EqualValues(
			t,
			2,
			limiter.Stats().ThresholdFactor,
			"threshold factor should be 2",
		)
		require.EqualValues(
			t,
			80,
			limiter.Limit(100),
			"payload limit should be 80",
		)

		freeMem = 8 // 8% free memory -> critical state
		limiter.tick()
		require.EqualValues(
			t,
			LimiterStateCritical,
			limiter.Stats().State,
			"limiter should be in a critical state",
		)
		require.EqualValues(
			t,
			1,
			limiter.Limit(100),
			"payload limit should be 1 if freeMemory is 8%",
		)
		require.EqualValues(
			t,
			3,
			limiter.Stats().ThresholdFactor,
			"threshold factor increases because freeMem is critical",
		)

		freeMem = 11 // 11% free memory -> above critical
		limiter.tick()
		require.EqualValues(
			t,
			LimiterStateThreshold,
			limiter.Stats().State,
			"limiter should be in a threshold state",
		)
		require.EqualValues(
			t,
			70,
			limiter.Limit(100),
			"payload limit depend on threshold factor if freeMemory is 11%",
		)
		require.EqualValues(
			t,
			3,
			limiter.Stats().ThresholdFactor,
			"threshold factor should not change if freeMem goes from crit to threshold",
		)

		freeMem = 8 // 8% free memory -> critical state
		limiter.tick()
		require.EqualValues(
			t,
			LimiterStateCritical,
			limiter.Stats().State,
			"limiter should be in a critical state",
		)
		require.EqualValues(
			t,
			1,
			limiter.Limit(100),
			"payload limit should be 1 if freeMemory is 8%",
		)
		require.Equal(
			t,
			4,
			limiter.Stats().ThresholdFactor,
			"threshold factor increases because freeMem is critical",
		)
	})

	t.Run("limiter reaches to maximum threshold factor", func(t *testing.T) {
		var freeMem float64
		freeMemFunc := func() (float64, error) {
			return freeMem, nil
		}
		maxThresholdFactor := 8
		limiter := NewAdaptiveLimiter(
			AdaptiveLimiterConfig{
				FreeMemory:         freeMemFunc,
				MaxThresholdFactor: maxThresholdFactor,
			},
		)

		for i := 2; i <= maxThresholdFactor; i++ {
			freeMem = 9
			limiter.tick()
			freeMem = 20
			limiter.tick()
			require.EqualValuesf(
				t,
				i,
				limiter.Stats().ThresholdFactor,
				"threshold factor should be %d", i,
			)
			maxLimit := int64(100)
			expectedLimit := int64(float64(maxLimit) * (1.0 - (0.1 * float64(i))))
			require.EqualValuesf(
				t,
				expectedLimit,
				limiter.Limit(maxLimit),
				"limiter's output should be %d", expectedLimit,
			)
		}

		freeMem = 9
		limiter.tick()
		freeMem = 20
		limiter.tick()
		require.EqualValuesf(
			t,
			maxThresholdFactor,
			limiter.Stats().ThresholdFactor,
			"threshold factor should remain %d", maxThresholdFactor,
		)
		maxLimit := int64(100)
		expectedLimit := int64(float64(maxLimit) * (1.0 - (0.1 * float64(maxThresholdFactor))))
		require.EqualValuesf(
			t,
			expectedLimit,
			limiter.Limit(maxLimit),
			"limiter's output should be %d", expectedLimit,
		)
	})
}
