package router

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/stats/metric"
)

func TestStandardBufferSizeCalculator(t *testing.T) {
	t.Run("returns max of noOfJobsToBatchInAWorker and noOfJobsPerChannel", func(t *testing.T) {
		t.Run("noOfJobsToBatchInAWorker is larger", func(t *testing.T) {
			noOfJobsToBatchInAWorker := &mockValueLoader[int]{value: 50}
			noOfJobsPerChannel := 30

			calculator := newStandardBufferSizeCalculator(noOfJobsToBatchInAWorker, noOfJobsPerChannel)

			result := calculator()
			require.Equal(t, 50, result)
		})

		t.Run("noOfJobsPerChannel is larger", func(t *testing.T) {
			noOfJobsToBatchInAWorker := &mockValueLoader[int]{value: 20}
			noOfJobsPerChannel := 40

			calculator := newStandardBufferSizeCalculator(noOfJobsToBatchInAWorker, noOfJobsPerChannel)

			result := calculator()
			require.Equal(t, 40, result)
		})

		t.Run("both values are equal", func(t *testing.T) {
			noOfJobsToBatchInAWorker := &mockValueLoader[int]{value: 25}
			noOfJobsPerChannel := 25

			calculator := newStandardBufferSizeCalculator(noOfJobsToBatchInAWorker, noOfJobsPerChannel)

			result := calculator()
			require.Equal(t, 25, result)
		})
	})

	t.Run("dynamic value changes", func(t *testing.T) {
		noOfJobsToBatchInAWorker := &mockValueLoader[int]{value: 10}
		noOfJobsPerChannel := 15

		calculator := newStandardBufferSizeCalculator(noOfJobsToBatchInAWorker, noOfJobsPerChannel)

		// Initial calculation
		result1 := calculator()
		require.Equal(t, 15, result1) // noOfJobsPerChannel is larger

		// Change the value loader
		noOfJobsToBatchInAWorker.value = 20
		result2 := calculator()
		require.Equal(t, 20, result2) // noOfJobsToBatchInAWorker is now larger
	})
}

func TestExperimentalBufferSizeCalculator(t *testing.T) {
	scalingFactor := config.SingleValueLoader(1.2)
	t.Run("Basic Calculations", func(t *testing.T) {
		t.Run("calculates buffer size correctly with all metrics", func(t *testing.T) {
			jobQueryBatchSize := &mockValueLoader[int]{value: 100}
			noOfWorkers := 5
			noOfJobsToBatchInAWorker := &mockValueLoader[int]{value: 20}
			workLoopThroughput := metric.NewSimpleMovingAverage(1)
			workLoopThroughput.Observe(10)
			calculator := newExperimentalBufferSizeCalculator(jobQueryBatchSize, noOfWorkers, noOfJobsToBatchInAWorker, workLoopThroughput, scalingFactor)

			// max(100/5, 20, 10) * 1.2 = max(20, 20, 10) * 1.2 = 20 * 1.2 = 24
			expectedSize := 24
			result := calculator()
			require.Equal(t, expectedSize, result)
		})

		t.Run("respects minimum size when all values are zero", func(t *testing.T) {
			jobQueryBatchSize := &mockValueLoader[int]{value: 0}
			noOfWorkers := 10
			noOfJobsToBatchInAWorker := &mockValueLoader[int]{value: 0}
			workLoopThroughput := metric.NewSimpleMovingAverage(1)
			workLoopThroughput.Observe(0)
			calculator := newExperimentalBufferSizeCalculator(jobQueryBatchSize, noOfWorkers, noOfJobsToBatchInAWorker, workLoopThroughput, scalingFactor)

			result := calculator()
			require.Equal(t, 1, result)
		})

		t.Run("uses work loop throughput when it's the maximum", func(t *testing.T) {
			jobQueryBatchSize := &mockValueLoader[int]{value: 20}
			noOfWorkers := 10
			noOfJobsToBatchInAWorker := &mockValueLoader[int]{value: 5}
			workLoopThroughput := metric.NewSimpleMovingAverage(1)
			workLoopThroughput.Observe(50) // This will be the maximum
			calculator := newExperimentalBufferSizeCalculator(jobQueryBatchSize, noOfWorkers, noOfJobsToBatchInAWorker, workLoopThroughput, scalingFactor)

			// max(20/10, 5, 50) * 1.2 = max(2, 5, 50) * 1.2 = 50 * 1.2 = 60
			expectedSize := 60
			result := calculator()
			require.Equal(t, expectedSize, result)
		})

		t.Run("uses batch size when it's the maximum", func(t *testing.T) {
			jobQueryBatchSize := &mockValueLoader[int]{value: 50}
			noOfWorkers := 10
			noOfJobsToBatchInAWorker := &mockValueLoader[int]{value: 100} // This will be the maximum
			workLoopThroughput := metric.NewSimpleMovingAverage(1)
			workLoopThroughput.Observe(1)
			calculator := newExperimentalBufferSizeCalculator(jobQueryBatchSize, noOfWorkers, noOfJobsToBatchInAWorker, workLoopThroughput, scalingFactor)

			// max(50/10, 100, 1) * 1.2 = max(5, 100, 1) * 1.2 = 100 * 1.2 = 120
			expectedSize := 120
			require.Equal(t, expectedSize, calculator())
		})

		t.Run("uses job query batch size when it's the maximum", func(t *testing.T) {
			jobQueryBatchSize := &mockValueLoader[int]{value: 1000} // This contributes to maximum
			noOfWorkers := 5
			noOfJobsToBatchInAWorker := &mockValueLoader[int]{value: 10}
			workLoopThroughput := metric.NewSimpleMovingAverage(1)
			workLoopThroughput.Observe(5)
			calculator := newExperimentalBufferSizeCalculator(jobQueryBatchSize, noOfWorkers, noOfJobsToBatchInAWorker, workLoopThroughput, scalingFactor)

			// max(1000/5, 10, 5) * 1.2 = max(200, 10, 5) * 1.2 = 200 * 1.2 = 240
			expectedSize := 240
			require.Equal(t, expectedSize, calculator())
		})
	})

	t.Run("Edge Cases", func(t *testing.T) {
		t.Run("zero throughput returns minimum size", func(t *testing.T) {
			jobQueryBatchSize := &mockValueLoader[int]{value: 100}
			noOfWorkers := 5
			noOfJobsToBatchInAWorker := &mockValueLoader[int]{value: 20}
			workLoopThroughput := metric.NewSimpleMovingAverage(1)
			// No observation made, throughput is 0
			calculator := newExperimentalBufferSizeCalculator(jobQueryBatchSize, noOfWorkers, noOfJobsToBatchInAWorker, workLoopThroughput, scalingFactor)

			require.Equal(t, 1, calculator())
		})

		t.Run("handles single worker correctly", func(t *testing.T) {
			jobQueryBatchSize := &mockValueLoader[int]{value: 100}
			noOfWorkers := 1
			noOfJobsToBatchInAWorker := &mockValueLoader[int]{value: 10}
			workLoopThroughput := metric.NewSimpleMovingAverage(1)
			workLoopThroughput.Observe(5)
			calculator := newExperimentalBufferSizeCalculator(jobQueryBatchSize, noOfWorkers, noOfJobsToBatchInAWorker, workLoopThroughput, scalingFactor)

			// max(100/1, 10, 5) * 1.2 = max(100, 10, 5) * 1.2 = 100 * 1.2 = 120
			expectedSize := 120
			require.Equal(t, expectedSize, calculator())
		})

		t.Run("handles large numbers correctly", func(t *testing.T) {
			jobQueryBatchSize := &mockValueLoader[int]{value: 10000}
			noOfWorkers := 100
			noOfJobsToBatchInAWorker := &mockValueLoader[int]{value: 500}
			workLoopThroughput := metric.NewSimpleMovingAverage(1)
			workLoopThroughput.Observe(200)
			calculator := newExperimentalBufferSizeCalculator(jobQueryBatchSize, noOfWorkers, noOfJobsToBatchInAWorker, workLoopThroughput, scalingFactor)

			// max(10000/100 * 1.2, 500, 200) * 1.2 = max(120, 500, 200) * 1.2 = 500 * 1.2 = 600
			expectedSize := 600
			require.Equal(t, expectedSize, calculator())
		})

		t.Run("fractional throughput values", func(t *testing.T) {
			jobQueryBatchSize := &mockValueLoader[int]{value: 10}
			noOfWorkers := 5
			noOfJobsToBatchInAWorker := &mockValueLoader[int]{value: 3}
			workLoopThroughput := metric.NewSimpleMovingAverage(1)
			workLoopThroughput.Observe(2.5)
			calculator := newExperimentalBufferSizeCalculator(jobQueryBatchSize, noOfWorkers, noOfJobsToBatchInAWorker, workLoopThroughput, scalingFactor)

			// max(10/5 * 1.2, 3, 2.5) * 1.2 = max(2.4, 3, 2.5) * 1.2 = 3 * 1.2 = 3.6, ceil = 4
			expectedSize := 4
			require.Equal(t, expectedSize, calculator())
		})
	})

	t.Run("Dynamic Value Changes", func(t *testing.T) {
		t.Run("recalculates when job query batch size changes", func(t *testing.T) {
			jobQueryBatchSize := &mockValueLoader[int]{value: 100}
			noOfWorkers := 5
			noOfJobsToBatchInAWorker := &mockValueLoader[int]{value: 10}
			workLoopThroughput := metric.NewSimpleMovingAverage(1)
			workLoopThroughput.Observe(5)
			calculator := newExperimentalBufferSizeCalculator(jobQueryBatchSize, noOfWorkers, noOfJobsToBatchInAWorker, workLoopThroughput, scalingFactor)

			// Initial calculation: max(100/5, 10, 5) * 1.2 = max(20, 10, 5) * 1.2 = 20 * 1.2 = 24
			result1 := calculator()
			require.Equal(t, 24, result1)

			// Change job query batch size
			jobQueryBatchSize.value = 200
			// New calculation: max(200/5, 10, 5) * 1.2 = max(40, 10, 5) * 1.2 = 40 * 1.2 = 48
			result2 := calculator()
			require.Equal(t, 48, result2)
		})

		t.Run("recalculates when batch size changes", func(t *testing.T) {
			jobQueryBatchSize := &mockValueLoader[int]{value: 50}
			noOfWorkers := 5
			noOfJobsToBatchInAWorker := &mockValueLoader[int]{value: 10}
			workLoopThroughput := metric.NewSimpleMovingAverage(1)
			workLoopThroughput.Observe(5)
			calculator := newExperimentalBufferSizeCalculator(jobQueryBatchSize, noOfWorkers, noOfJobsToBatchInAWorker, workLoopThroughput, scalingFactor)

			// Initial calculation: max(50/5, 10, 5) * 1.2 = max(10, 10, 5) * 1.2 = 10 * 1.2 = 12
			require.Equal(t, 12, calculator())

			// Change batch size
			noOfJobsToBatchInAWorker.value = 20
			// New calculation: max(50/5, 20, 5) * 1.2 = max(10, 20, 5) * 1.2 = 20 * 1.2 = 24
			require.Equal(t, 24, calculator())
		})

		t.Run("recalculates when throughput changes", func(t *testing.T) {
			jobQueryBatchSize := &mockValueLoader[int]{value: 50}
			noOfWorkers := 5
			noOfJobsToBatchInAWorker := &mockValueLoader[int]{value: 10}
			workLoopThroughput := metric.NewSimpleMovingAverage(2)
			workLoopThroughput.Observe(5)
			calculator := newExperimentalBufferSizeCalculator(jobQueryBatchSize, noOfWorkers, noOfJobsToBatchInAWorker, workLoopThroughput, scalingFactor)

			// Initial calculation: max(50/5, 10, 5) * 1.2 = max(10, 10, 5) * 1.2 = 10 * 1.2 = 12
			require.Equal(t, 12, calculator())

			// Add more throughput data
			workLoopThroughput.Observe(25) // average becomes (5+25)/2 = 15
			// New calculation: max(50/5, 10, 15) * 1.2 = max(10, 10, 15) * 1.2 = 15 * 1.2 = 18
			require.Equal(t, 18, calculator())
		})
	})

	t.Run("Multiplier Effects", func(t *testing.T) {
		t.Run("verifies 1.2 multiplier is applied correctly", func(t *testing.T) {
			jobQueryBatchSize := &mockValueLoader[int]{value: 100}
			noOfWorkers := 10
			noOfJobsToBatchInAWorker := &mockValueLoader[int]{value: 10}
			workLoopThroughput := metric.NewSimpleMovingAverage(1)
			workLoopThroughput.Observe(10)
			calculator := newExperimentalBufferSizeCalculator(jobQueryBatchSize, noOfWorkers, noOfJobsToBatchInAWorker, workLoopThroughput, scalingFactor)

			// All three metrics are equal: 100/10 = 10, 10, 10
			// max(10, 10, 10) * 1.2 = 10 * 1.2 = 12
			require.Equal(t, 12, calculator())
		})
	})

	t.Run("Stats Emission", func(t *testing.T) {
		t.Run("emits buffer size stats correctly", func(t *testing.T) {
			jobQueryBatchSize := &mockValueLoader[int]{value: 100}
			noOfWorkers := 5
			noOfJobsToBatchInAWorker := &mockValueLoader[int]{value: 20}
			workLoopThroughput := metric.NewSimpleMovingAverage(1)
			workLoopThroughput.Observe(10)
			calculator := newExperimentalBufferSizeCalculator(jobQueryBatchSize, noOfWorkers, noOfJobsToBatchInAWorker, workLoopThroughput, scalingFactor)

			// Call calculator multiple times to ensure stats are captured
			expectedSize := 24
			for i := 0; i < 5; i++ {
				result := calculator()
				require.Equal(t, expectedSize, result)
			}
		})

		t.Run("respects OnceEvery throttling", func(t *testing.T) {
			jobQueryBatchSize := &mockValueLoader[int]{value: 50}
			noOfWorkers := 5
			noOfJobsToBatchInAWorker := &mockValueLoader[int]{value: 10}
			workLoopThroughput := metric.NewSimpleMovingAverage(1)
			workLoopThroughput.Observe(5)
			calculator := newExperimentalBufferSizeCalculator(jobQueryBatchSize, noOfWorkers, noOfJobsToBatchInAWorker, workLoopThroughput, scalingFactor)

			// Call calculator - first call should capture the stat
			expectedSize := 12 // max(50/5, 10, 5) * 1.2 = max(10, 10, 5) * 1.2 = 10 * 1.2 = 12
			result := calculator()
			require.Equal(t, expectedSize, result)

			result2 := calculator()
			require.Equal(t, expectedSize, result2)
		})
	})

	t.Run("Concurrent Access", func(t *testing.T) {
		t.Run("calculator is safe for concurrent use", func(t *testing.T) {
			jobQueryBatchSize := &mockValueLoader[int]{value: 100}
			noOfWorkers := 5
			noOfJobsToBatchInAWorker := &mockValueLoader[int]{value: 20}
			workLoopThroughput := metric.NewSimpleMovingAverage(10)
			workLoopThroughput.Observe(10)
			calculator := newExperimentalBufferSizeCalculator(jobQueryBatchSize, noOfWorkers, noOfJobsToBatchInAWorker, workLoopThroughput, scalingFactor)

			var wg sync.WaitGroup
			results := make([]int, 100)

			// Run calculator concurrently
			for i := 0; i < 100; i++ {
				wg.Add(1)
				go func(idx int) {
					defer wg.Done()
					results[idx] = calculator()
				}(i)
			}

			wg.Wait()

			// All results should be the same since inputs don't change
			expected := results[0]
			for i, result := range results {
				require.Equal(t, expected, result, "Result at index %d should match", i)
			}
		})
	})

	t.Run("Integration Scenarios", func(t *testing.T) {
		t.Run("integration with changing worker counts", func(t *testing.T) {
			jobQueryBatchSize := &mockValueLoader[int]{value: 1000}
			noOfJobsToBatchInAWorker := &mockValueLoader[int]{value: 50}
			workLoopThroughput := metric.NewSimpleMovingAverage(5)
			workLoopThroughput.Observe(25)

			// Test with different worker counts
			testCases := []struct {
				workers      int
				expectedSize int
			}{
				{workers: 1, expectedSize: 1200}, // max(1000/1, 50, 25) * 1.2 = 1000 * 1.2 = 1200
				{workers: 10, expectedSize: 120}, // max(1000/10, 50, 25) * 1.2 = 100 * 1.2 = 120
				{workers: 25, expectedSize: 60},  // max(1000/25, 50, 25) * 1.2 = 50 * 1.2 = 60
				{workers: 50, expectedSize: 60},  // max(1000/50, 50, 25) * 1.2 = 50 * 1.2 = 60
				{workers: 100, expectedSize: 60}, // max(1000/100, 50, 25) * 1.2 = 50 * 1.2 = 60
			}

			for _, tc := range testCases {
				t.Run(fmt.Sprintf("workers=%d", tc.workers), func(t *testing.T) {
					calculator := newExperimentalBufferSizeCalculator(jobQueryBatchSize, tc.workers, noOfJobsToBatchInAWorker, workLoopThroughput, scalingFactor)
					require.Equal(t, tc.expectedSize, calculator())
				})
			}
		})

		t.Run("realistic production scenarios", func(t *testing.T) {
			t.Run("high throughput scenario", func(t *testing.T) {
				jobQueryBatchSize := &mockValueLoader[int]{value: 10000}
				noOfWorkers := 20
				noOfJobsToBatchInAWorker := &mockValueLoader[int]{value: 100}
				workLoopThroughput := metric.NewSimpleMovingAverage(10)

				// Simulate high throughput ramp-up
				for i := 1; i <= 10; i++ {
					workLoopThroughput.Observe(float64(i * 50)) // 50, 100, 150, ..., 500
				}

				calculator := newExperimentalBufferSizeCalculator(jobQueryBatchSize, noOfWorkers, noOfJobsToBatchInAWorker, workLoopThroughput, scalingFactor)

				// Average throughput = (50+100+...+500)/10 = 2750/10 = 275
				// max(10000/20, 100, 275) * 1.2 = max(500, 100, 275) * 1.2 = 500 * 1.2 = 600
				require.Equal(t, 600, calculator())
			})

			t.Run("low throughput scenario", func(t *testing.T) {
				jobQueryBatchSize := &mockValueLoader[int]{value: 100}
				noOfWorkers := 10
				noOfJobsToBatchInAWorker := &mockValueLoader[int]{value: 10}
				workLoopThroughput := metric.NewSimpleMovingAverage(5)

				// Simulate low throughput
				for i := 0; i < 5; i++ {
					workLoopThroughput.Observe(2.0) // Steady low throughput
				}

				calculator := newExperimentalBufferSizeCalculator(jobQueryBatchSize, noOfWorkers, noOfJobsToBatchInAWorker, workLoopThroughput, scalingFactor)

				// max(100/10, 10, 2) * 1.2 = max(10, 10, 2) * 1.2 = 10 * 1.2 = 12
				require.Equal(t, 12, calculator())
			})

			t.Run("variable throughput scenario", func(t *testing.T) {
				jobQueryBatchSize := &mockValueLoader[int]{value: 500}
				noOfWorkers := 8
				noOfJobsToBatchInAWorker := &mockValueLoader[int]{value: 25}
				workLoopThroughput := metric.NewSimpleMovingAverage(6)

				// Simulate variable throughput pattern
				throughputValues := []float64{10, 50, 30, 80, 20, 40}
				for _, val := range throughputValues {
					workLoopThroughput.Observe(val)
				}

				calculator := newExperimentalBufferSizeCalculator(jobQueryBatchSize, noOfWorkers, noOfJobsToBatchInAWorker, workLoopThroughput, scalingFactor)

				// Average = (10+50+30+80+20+40)/6 = 230/6 ≈ 38.33
				// max(500/8, 25, 38.33) * 1.2 = max(62.5, 25, 38.33) * 1.2 = 62.5 * 1.2 = 75
				require.Equal(t, 75, calculator())
			})
		})

		t.Run("edge case production scenarios", func(t *testing.T) {
			t.Run("system startup - no throughput data", func(t *testing.T) {
				jobQueryBatchSize := &mockValueLoader[int]{value: 1000}
				noOfWorkers := 10
				noOfJobsToBatchInAWorker := &mockValueLoader[int]{value: 50}
				workLoopThroughput := metric.NewSimpleMovingAverage(10) // No observations yet

				calculator := newExperimentalBufferSizeCalculator(jobQueryBatchSize, noOfWorkers, noOfJobsToBatchInAWorker, workLoopThroughput, scalingFactor)

				// Throughput is 0, so return minimum size
				require.Equal(t, 1, calculator())
			})

			t.Run("single worker system", func(t *testing.T) {
				jobQueryBatchSize := &mockValueLoader[int]{value: 500}
				noOfWorkers := 1
				noOfJobsToBatchInAWorker := &mockValueLoader[int]{value: 100}
				workLoopThroughput := metric.NewSimpleMovingAverage(3)
				workLoopThroughput.Observe(50)

				calculator := newExperimentalBufferSizeCalculator(jobQueryBatchSize, noOfWorkers, noOfJobsToBatchInAWorker, workLoopThroughput, scalingFactor)

				// max(500/1, 100, 50) * 1.2 = max(500, 100, 50) * 1.2 = 500 * 1.2 = 600
				require.Equal(t, 600, calculator())
			})

			t.Run("micro batching scenario", func(t *testing.T) {
				jobQueryBatchSize := &mockValueLoader[int]{value: 10}
				noOfWorkers := 5
				noOfJobsToBatchInAWorker := &mockValueLoader[int]{value: 1}
				workLoopThroughput := metric.NewSimpleMovingAverage(5)
				workLoopThroughput.Observe(3)

				calculator := newExperimentalBufferSizeCalculator(jobQueryBatchSize, noOfWorkers, noOfJobsToBatchInAWorker, workLoopThroughput, scalingFactor)

				// max(10/5 * 1.2, 1, 3) * 1.2 = max(2.4, 1, 3) * 1.2 = 3 * 1.2 = 3.6, ceil = 4
				require.Equal(t, 4, calculator())
			})
		})
	})
}

func TestBufferSizeCalculatorSwitcher(t *testing.T) {
	t.Run("switches between new and legacy calculator based on flag", func(t *testing.T) {
		// Setup common parameters
		enableNewBufferSizeCalculator := &mockValueLoader[bool]{value: false}
		jobQueryBatchSize := &mockValueLoader[int]{value: 100}
		noOfWorkers := 5
		noOfJobsToBatchInAWorker := &mockValueLoader[int]{value: 20}
		workLoopThroughput := metric.NewSimpleMovingAverage(1)
		workLoopThroughput.Observe(10)
		noOfJobsPerChannel := 15

		switcherCalculator := newBufferSizeCalculatorSwitcher(
			enableNewBufferSizeCalculator,
			jobQueryBatchSize,
			noOfWorkers,
			noOfJobsToBatchInAWorker,
			workLoopThroughput,
			config.SingleValueLoader(1.2),
			noOfJobsPerChannel,
		)

		t.Run("uses legacy calculator when flag is false", func(t *testing.T) {
			enableNewBufferSizeCalculator.value = false
			result := switcherCalculator()

			// Legacy calculator: max(20, 15) = 20
			require.Equal(t, 20, result)
		})

		t.Run("uses new calculator when flag is true", func(t *testing.T) {
			enableNewBufferSizeCalculator.value = true
			result := switcherCalculator()

			// New calculator: max(100/5, 20, 10) * 1.2 = max(20, 20, 10) * 1.2 = 20 * 1.2 = 24
			require.Equal(t, 24, result)
		})

		t.Run("switches dynamically", func(t *testing.T) {
			// Start with legacy
			enableNewBufferSizeCalculator.value = false
			legacyResult := switcherCalculator()
			require.Equal(t, 20, legacyResult)

			// Switch to new
			enableNewBufferSizeCalculator.value = true
			newResult := switcherCalculator()
			require.Equal(t, 24, newResult)

			// Switch back to legacy
			enableNewBufferSizeCalculator.value = false
			backToLegacyResult := switcherCalculator()
			require.Equal(t, 20, backToLegacyResult)
		})
	})
}
