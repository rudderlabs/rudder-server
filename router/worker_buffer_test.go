package router

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
	kitsync "github.com/rudderlabs/rudder-go-kit/sync"
	"github.com/rudderlabs/rudder-server/jobsdb"
	routerutils "github.com/rudderlabs/rudder-server/router/utils"
)

func TestWorkerBuffer(t *testing.T) {
	t.Run("Constructor and Basic Operations", func(t *testing.T) {
		t.Run("newWorkerBuffer creates buffer with correct initial state", func(t *testing.T) {
			maxSize := 10
			targetSize := func() int { return maxSize }
			wb := newWorkerBuffer(maxSize, targetSize, nil)

			require.Equal(t, maxSize, wb.maxCapacity)
			require.Equal(t, maxSize, wb.currentCapacity())
			require.Equal(t, 0, wb.reservations)
			require.NotNil(t, wb.jobs)
			require.Equal(t, maxSize, cap(wb.jobs))
			require.Equal(t, 0, len(wb.jobs))
		})

		t.Run("newWorkerBuffer with edge cases", func(t *testing.T) {
			t.Run("minimum size", func(t *testing.T) {
				wb := newWorkerBuffer(1, func() int { return 1 }, nil)
				require.Equal(t, 1, wb.maxCapacity)
				require.Equal(t, 1, wb.currentCapacity())
			})

			t.Run("zero size", func(t *testing.T) {
				wb := newWorkerBuffer(0, func() int { return 0 }, nil)
				require.Equal(t, 1, wb.maxCapacity)
				require.Equal(t, 1, wb.currentCapacity())
				require.Equal(t, 1, cap(wb.jobs))
			})

			t.Run("large size", func(t *testing.T) {
				wb := newWorkerBuffer(1000, func() int { return 1000 }, nil)
				require.Equal(t, 1000, wb.maxCapacity)
				require.Equal(t, 1000, wb.currentCapacity())
			})
		})

		t.Run("Jobs() returns read-only channel", func(t *testing.T) {
			wb := newWorkerBuffer(5, func() int { return 5 }, nil)
			jobsChan := wb.Jobs()

			// Verify it's a receive-only channel
			require.NotNil(t, jobsChan)
			// The channel should be the same as the internal one (same underlying channel)
			require.Equal(t, cap(wb.jobs), cap(jobsChan))
		})
	})

	t.Run("Dynamic Sizing Operations", func(t *testing.T) {
		t.Run("targetSize function controls current size", func(t *testing.T) {
			targetSize := 10
			wb := newWorkerBuffer(20, func() int { return targetSize }, nil)

			t.Run("initial size", func(t *testing.T) {
				require.Equal(t, 10, wb.currentCapacity())
			})

			t.Run("change target to smaller size", func(t *testing.T) {
				targetSize = 5
				require.Equal(t, 5, wb.currentCapacity())
			})

			t.Run("change target to larger size", func(t *testing.T) {
				targetSize = 15
				require.Equal(t, 15, wb.currentCapacity())
			})

			t.Run("target same size (no-op)", func(t *testing.T) {
				require.Equal(t, 15, wb.currentCapacity())
			})
		})

		t.Run("boundary condition sizing", func(t *testing.T) {
			targetSize := 10
			wb := newWorkerBuffer(10, func() int { return targetSize }, nil)

			t.Run("target equals maxSize", func(t *testing.T) {
				targetSize = 10
				require.Equal(t, 10, wb.currentCapacity())
			})

			t.Run("target above maxSize (should cap)", func(t *testing.T) {
				targetSize = 15
				require.Equal(t, 10, wb.currentCapacity()) // should be capped to maxSize
			})

			t.Run("target minimum (1)", func(t *testing.T) {
				targetSize = 1
				require.Equal(t, 1, wb.currentCapacity())
			})

			t.Run("target below 1 (should set to 1)", func(t *testing.T) {
				targetSize = 0
				require.Equal(t, 1, wb.currentCapacity())

				targetSize = -5
				require.Equal(t, 1, wb.currentCapacity())
			})
		})

		t.Run("concurrent access to dynamic sizing", func(t *testing.T) {
			targetSize := 10
			var targetSizeMu sync.RWMutex
			wb := newWorkerBuffer(20, func() int {
				targetSizeMu.RLock()
				defer targetSizeMu.RUnlock()
				return targetSize
			}, nil)
			var wg sync.WaitGroup

			// Launch multiple goroutines changing target size
			for i := 0; i < 10; i++ {
				wg.Add(1)
				go func(size int) {
					defer wg.Done()
					targetSizeMu.Lock()
					targetSize = size%15 + 1 // sizes from 1 to 15
					targetSizeMu.Unlock()

					// Check that currentSize returns a valid value
					current := wb.currentCapacity()
					require.True(t, current >= 1)
					require.True(t, current <= wb.maxCapacity)
				}(i)
			}

			wg.Wait()

			// Verify final state is valid
			require.True(t, wb.currentCapacity() >= 1)
			require.True(t, wb.currentCapacity() <= wb.maxCapacity)
		})
	})

	t.Run("Available Slots Calculation", func(t *testing.T) {
		t.Run("empty buffer scenarios", func(t *testing.T) {
			wb := newWorkerBuffer(5, func() int { return 5 }, nil)

			t.Run("empty buffer, no reservations", func(t *testing.T) {
				slots := wb.AvailableSlots()
				require.Equal(t, 5, slots)
			})

			t.Run("with reservations, no jobs", func(t *testing.T) {
				wb.reservations = 2
				slots := wb.AvailableSlots()
				require.Equal(t, 3, slots)
			})
		})

		t.Run("buffer with jobs", func(t *testing.T) {
			wb := newWorkerBuffer(5, func() int { return 5 }, nil)

			// Add some jobs to the buffer
			job1 := createTestWorkerJob()
			job2 := createTestWorkerJob()
			wb.jobs <- job1
			wb.jobs <- job2

			t.Run("jobs but no reservations", func(t *testing.T) {
				slots := wb.AvailableSlots()
				require.Equal(t, 3, slots) // 5 - 0 - 2 = 3
			})

			t.Run("jobs and reservations", func(t *testing.T) {
				wb.reservations = 1
				slots := wb.AvailableSlots()
				require.Equal(t, 2, slots) // 5 - 1 - 2 = 2
			})
		})

		t.Run("negative available slots (should return 0)", func(t *testing.T) {
			targetSize := 2
			wb := newWorkerBuffer(3, func() int { return targetSize }, nil) // target size smaller than max

			// Fill buffer beyond current size
			job1 := createTestWorkerJob()
			job2 := createTestWorkerJob()
			job3 := createTestWorkerJob()
			wb.jobs <- job1
			wb.jobs <- job2
			wb.jobs <- job3

			slots := wb.AvailableSlots()
			require.Equal(t, 0, slots) // Should not go negative
		})

		t.Run("after dynamic size change", func(t *testing.T) {
			targetSize := 10
			wb := newWorkerBuffer(10, func() int { return targetSize }, nil)

			targetSize = 5 // change target size
			slots := wb.AvailableSlots()
			require.Equal(t, 5, slots)
		})
	})

	t.Run("Slot Reservation", func(t *testing.T) {
		t.Run("successful reservations", func(t *testing.T) {
			wb := newWorkerBuffer(3, func() int { return 3 }, nil)

			t.Run("reserve when slots available", func(t *testing.T) {
				slot := wb.ReserveSlot()
				require.NotNil(t, slot)
				require.Equal(t, 1, wb.reservations)
				require.Equal(t, wb, slot.wb)
			})

			t.Run("multiple sequential reservations", func(t *testing.T) {
				slot2 := wb.ReserveSlot()
				slot3 := wb.ReserveSlot()

				require.NotNil(t, slot2)
				require.NotNil(t, slot3)
				require.Equal(t, 3, wb.reservations)
			})
		})

		t.Run("failed reservations", func(t *testing.T) {
			wb := newWorkerBuffer(2, func() int { return 2 }, nil)

			// Fill up all slots
			slot1 := wb.ReserveSlot()
			slot2 := wb.ReserveSlot()
			require.NotNil(t, slot1)
			require.NotNil(t, slot2)

			t.Run("no slots available", func(t *testing.T) {
				slot3 := wb.ReserveSlot()
				require.Nil(t, slot3)
				require.Equal(t, 2, wb.reservations) // Should not increment
			})
		})

		t.Run("concurrent reservations", func(t *testing.T) {
			wb := newWorkerBuffer(10, func() int { return 10 }, nil)
			var wg sync.WaitGroup
			var mu sync.Mutex
			var successfulReservations int

			// Try to reserve from multiple goroutines
			for i := 0; i < 15; i++ { // More attempts than slots
				wg.Add(1)
				go func() {
					defer wg.Done()
					if slot := wb.ReserveSlot(); slot != nil {
						mu.Lock()
						successfulReservations++
						mu.Unlock()
					}
				}()
			}

			wg.Wait()

			// Should only succeed for available slots
			require.Equal(t, 10, successfulReservations)
			require.Equal(t, 10, wb.reservations)
		})
	})

	t.Run("Reserved Slot Operations", func(t *testing.T) {
		t.Run("Use() method", func(t *testing.T) {
			wb := newWorkerBuffer(3, func() int { return 3 }, nil)
			slot := wb.ReserveSlot()
			require.NotNil(t, slot)

			job := createTestWorkerJob()

			t.Run("successful use", func(t *testing.T) {
				slot.Use(job)

				// Reservations should decrease
				require.Equal(t, 0, wb.reservations)

				// Job should be in channel
				require.Equal(t, 1, len(wb.jobs))
				receivedJob := <-wb.jobs
				require.Equal(t, job.job.JobID, receivedJob.job.JobID)
			})
		})

		t.Run("Release() method", func(t *testing.T) {
			wb := newWorkerBuffer(3, func() int { return 3 }, nil)

			t.Run("successful release", func(t *testing.T) {
				slot := wb.ReserveSlot()
				require.NotNil(t, slot)
				require.Equal(t, 1, wb.reservations)

				slot.Release()
				require.Equal(t, 0, wb.reservations)
			})

			t.Run("release when reservations is 0", func(t *testing.T) {
				slot := &reservedSlot{wb: wb}
				wb.reservations = 0

				// Should not panic and should not go negative
				slot.Release()
				require.Equal(t, 0, wb.reservations)
			})

			t.Run("multiple releases of same slot", func(t *testing.T) {
				slot := wb.ReserveSlot()
				require.NotNil(t, slot)

				slot.Release()
				slot.Release() // Second release

				require.Equal(t, 0, wb.reservations) // Should not go negative
			})
		})

		t.Run("concurrent use and release", func(t *testing.T) {
			wb := newWorkerBuffer(10, func() int { return 10 }, nil)
			var wg sync.WaitGroup

			// Reserve multiple slots concurrently and then use/release them
			for i := 0; i < 5; i++ {
				wg.Add(1)
				go func(id int) {
					defer wg.Done()
					slot := wb.ReserveSlot()
					if slot != nil {
						if id%2 == 0 {
							job := createTestWorkerJob()
							slot.Use(job)
						} else {
							slot.Release()
						}
					}
				}(i)
			}

			wg.Wait()

			// Final state should be consistent
			require.True(t, wb.reservations >= 0)
			require.True(t, len(wb.jobs) >= 0)
		})
	})

	t.Run("Integration Workflows", func(t *testing.T) {
		t.Run("complete reserve-use workflow", func(t *testing.T) {
			wb := newWorkerBuffer(5, func() int { return 5 }, nil)

			// Reserve a slot
			slot := wb.ReserveSlot()
			require.NotNil(t, slot)
			require.Equal(t, 4, wb.AvailableSlots())

			// Use the slot
			job := createTestWorkerJob()
			slot.Use(job)

			// Verify final state
			require.Equal(t, 0, wb.reservations)
			require.Equal(t, 1, len(wb.jobs))
			require.Equal(t, 4, wb.AvailableSlots()) // currentSize - reservations - jobs = 5 - 0 - 1 = 4
		})

		t.Run("complete reserve-release workflow", func(t *testing.T) {
			wb := newWorkerBuffer(5, func() int { return 5 }, nil)

			// Reserve a slot
			slot := wb.ReserveSlot()
			require.NotNil(t, slot)
			require.Equal(t, 4, wb.AvailableSlots())

			// Release the slot
			slot.Release()

			// Verify final state
			require.Equal(t, 0, wb.reservations)
			require.Equal(t, 0, len(wb.jobs))
			require.Equal(t, 5, wb.AvailableSlots())
		})

		t.Run("buffer dynamics under load", func(t *testing.T) {
			targetSize := 15
			var targetSizeMu sync.RWMutex
			wb := newWorkerBuffer(20, func() int {
				targetSizeMu.RLock()
				defer targetSizeMu.RUnlock()
				return targetSize
			}, nil)
			var wg sync.WaitGroup

			// Simulate realistic workload
			for i := 0; i < 50; i++ {
				wg.Add(1)
				go func(id int) {
					defer wg.Done()

					// Try to reserve
					slot := wb.ReserveSlot()
					if slot != nil {
						// Simulate some work
						time.Sleep(time.Millisecond)

						// Randomly use or release
						if id%3 == 0 {
							job := createTestWorkerJob()
							slot.Use(job)
						} else {
							slot.Release()
						}
					}

					// Also do some concurrent dynamic sizing
					if id%7 == 0 {
						targetSizeMu.Lock()
						targetSize = (id % 15) + 5 // sizes from 5 to 19
						targetSizeMu.Unlock()
					}
				}(i)
			}

			wg.Wait()

			// Verify invariants
			require.True(t, wb.reservations >= 0)
			require.True(t, wb.currentCapacity() >= 1)
			require.True(t, wb.currentCapacity() <= wb.maxCapacity)
			require.True(t, len(wb.jobs) <= cap(wb.jobs))
		})
	})

	t.Run("Close Operations", func(t *testing.T) {
		t.Run("close closes job channel", func(t *testing.T) {
			wb := newWorkerBuffer(5, func() int { return 5 }, nil)

			wb.Close()

			// Channel should be closed
			_, ok := <-wb.jobs
			require.False(t, ok)
		})

		t.Run("multiple close calls are safe", func(t *testing.T) {
			wb := newWorkerBuffer(5, func() int { return 5 }, nil)

			wb.Close()
			// Should not panic
			require.NotPanics(t, func() {
				wb.Close()
			})
		})
	})

	t.Run("Stats Operations", func(t *testing.T) {
		t.Run("buffer with stats tracks metrics", func(t *testing.T) {
			memStats, err := memstats.New()
			require.NoError(t, err)

			bufferStats := &workerBufferStats{
				onceEvery:       kitsync.NewOnceEvery(10 * time.Millisecond),
				currentCapacity: memStats.NewTaggedStat("worker_buffer_capacity", stats.HistogramType, stats.Tags{"test": "capacity"}),
				currentSize:     memStats.NewTaggedStat("worker_buffer_size", stats.HistogramType, stats.Tags{"test": "size"}),
			}

			targetSize := 5
			wb := newWorkerBuffer(10, func() int { return targetSize }, bufferStats)

			// Call currentCapacity to trigger stats recording
			capacity := wb.currentCapacity()
			require.Equal(t, 5, capacity)

			// Verify metrics were recorded
			capacityMetrics := memStats.GetByName("worker_buffer_capacity")
			require.Len(t, capacityMetrics, 1, "Expected exactly one capacity metric")
			require.Equal(t, stats.Tags{"test": "capacity"}, capacityMetrics[0].Tags)
			require.Len(t, capacityMetrics[0].Values, 1, "Expected exactly one capacity observation")
			require.Equal(t, float64(5), capacityMetrics[0].Values[0])

			sizeMetrics := memStats.GetByName("worker_buffer_size")
			require.Len(t, sizeMetrics, 1, "Expected exactly one size metric")
			require.Equal(t, stats.Tags{"test": "size"}, sizeMetrics[0].Tags)
			require.Len(t, sizeMetrics[0].Values, 1, "Expected exactly one size observation")
			require.Equal(t, float64(0), sizeMetrics[0].Values[0]) // Initially empty
		})

		t.Run("stats track size changes", func(t *testing.T) {
			memStats, err := memstats.New()
			require.NoError(t, err)

			bufferStats := &workerBufferStats{
				onceEvery:       kitsync.NewOnceEvery(10 * time.Millisecond),
				currentCapacity: memStats.NewTaggedStat("worker_buffer_capacity", stats.HistogramType, stats.Tags{"test": "dynamic"}),
				currentSize:     memStats.NewTaggedStat("worker_buffer_size", stats.HistogramType, stats.Tags{"test": "dynamic"}),
			}

			wb := newWorkerBuffer(10, func() int { return 5 }, bufferStats)

			// Add some jobs
			job1 := createTestWorkerJob()
			job2 := createTestWorkerJob()
			wb.jobs <- job1
			wb.jobs <- job2

			// Trigger stats recording
			wb.currentCapacity()

			// Verify size metric reflects the added jobs
			sizeMetrics := memStats.GetByName("worker_buffer_size")
			require.Len(t, sizeMetrics, 1, "Expected exactly one size metric")
			require.Equal(t, stats.Tags{"test": "dynamic"}, sizeMetrics[0].Tags)
			require.Len(t, sizeMetrics[0].Values, 1, "Expected exactly one size observation")
			require.Equal(t, float64(2), sizeMetrics[0].Values[0])
		})

		t.Run("stats track capacity changes", func(t *testing.T) {
			memStats, err := memstats.New()
			require.NoError(t, err)

			bufferStats := &workerBufferStats{
				onceEvery:       kitsync.NewOnceEvery(10 * time.Millisecond),
				currentCapacity: memStats.NewTaggedStat("worker_buffer_capacity", stats.HistogramType, stats.Tags{"test": "capacity_change"}),
				currentSize:     memStats.NewTaggedStat("worker_buffer_size", stats.HistogramType, stats.Tags{"test": "capacity_change"}),
			}

			targetSize := 5
			wb := newWorkerBuffer(20, func() int { return targetSize }, bufferStats)

			// Record initial capacity
			wb.currentCapacity()
			time.Sleep(20 * time.Millisecond) // sleep for >onceEvery

			// Change target size
			targetSize = 15
			wb.currentCapacity()

			// Verify both capacity values were recorded
			capacityMetrics := memStats.GetByName("worker_buffer_capacity")
			require.Len(t, capacityMetrics, 1, "Expected exactly one capacity metric")
			require.Equal(t, stats.Tags{"test": "capacity_change"}, capacityMetrics[0].Tags)
			capacityValues := capacityMetrics[0].Values
			require.Contains(t, capacityValues, float64(5), "Expected initial capacity of 5 to be recorded")
			require.Contains(t, capacityValues, float64(15), "Expected changed capacity of 15 to be recorded")
		})

		t.Run("onceEvery throttles stats recording", func(t *testing.T) {
			memStats, err := memstats.New()
			require.NoError(t, err)

			bufferStats := &workerBufferStats{
				onceEvery:       kitsync.NewOnceEvery(200 * time.Millisecond), // Longer throttle
				currentCapacity: memStats.NewTaggedStat("worker_buffer_capacity", stats.HistogramType, stats.Tags{"test": "throttle"}),
				currentSize:     memStats.NewTaggedStat("worker_buffer_size", stats.HistogramType, stats.Tags{"test": "throttle"}),
			}

			wb := newWorkerBuffer(10, func() int { return 5 }, bufferStats)

			// Call currentCapacity multiple times rapidly
			for range 5 {
				wb.currentCapacity()
				time.Sleep(10 * time.Millisecond)
			}

			// Should only record once due to throttling
			capacityMetrics := memStats.GetByName("worker_buffer_capacity")
			require.Len(t, capacityMetrics, 1, "Expected exactly one capacity metric")
			require.Equal(t, stats.Tags{"test": "throttle"}, capacityMetrics[0].Tags)
			capacityObservationCount := len(capacityMetrics[0].Values)
			require.Equal(t, 1, capacityObservationCount, "Expected only one observation due to throttling")
		})

		t.Run("nil stats doesn't cause panic", func(t *testing.T) {
			wb := newWorkerBuffer(5, func() int { return 5 }, nil)

			// Should not panic when stats is nil
			require.NotPanics(t, func() {
				wb.currentCapacity()
			})
		})

		t.Run("newSimpleWorkerBuffer doesn't track stats", func(t *testing.T) {
			wb := newSimpleWorkerBuffer(5)

			require.Nil(t, wb.stats)
			require.NotPanics(t, func() {
				wb.currentCapacity()
			})
		})

		t.Run("concurrent stats recording", func(t *testing.T) {
			memStats, err := memstats.New()
			require.NoError(t, err)

			bufferStats := &workerBufferStats{
				onceEvery:       kitsync.NewOnceEvery(10 * time.Millisecond),
				currentCapacity: memStats.NewTaggedStat("worker_buffer_capacity", stats.HistogramType, stats.Tags{"test": "concurrent"}),
				currentSize:     memStats.NewTaggedStat("worker_buffer_size", stats.HistogramType, stats.Tags{"test": "concurrent"}),
			}

			targetSize := 10
			var targetSizeMu sync.RWMutex
			wb := newWorkerBuffer(20, func() int {
				targetSizeMu.RLock()
				defer targetSizeMu.RUnlock()
				return targetSize
			}, bufferStats)

			var wg sync.WaitGroup

			// Launch multiple goroutines that access currentCapacity concurrently
			for i := 0; i < 10; i++ {
				wg.Add(1)
				go func(id int) {
					defer wg.Done()

					// Change target size occasionally
					if id%3 == 0 {
						targetSizeMu.Lock()
						targetSize = (id % 15) + 5 // sizes from 5 to 19
						targetSizeMu.Unlock()
					}

					// Access current capacity
					capacity := wb.currentCapacity()
					require.True(t, capacity >= 1)
					require.True(t, capacity <= wb.maxCapacity)

					time.Sleep(5 * time.Millisecond)
				}(i)
			}

			wg.Wait()

			// Verify some metrics were recorded (exact count depends on timing and throttling)
			time.Sleep(50 * time.Millisecond) // Allow final stats to be recorded
			capacityMetrics := memStats.GetByName("worker_buffer_capacity")
			require.NotEmpty(t, capacityMetrics, "Expected at least one capacity metric to be recorded during concurrent access")

			// Verify the metric has the correct tags
			foundCorrectMetric := false
			for _, metric := range capacityMetrics {
				if metric.Tags["test"] == "concurrent" {
					foundCorrectMetric = true
					break
				}
			}
			require.True(t, foundCorrectMetric, "Expected to find metric with correct tags")
		})

		t.Run("stats integration with workflow", func(t *testing.T) {
			memStats, err := memstats.New()
			require.NoError(t, err)

			bufferStats := &workerBufferStats{
				onceEvery:       kitsync.NewOnceEvery(10 * time.Millisecond),
				currentCapacity: memStats.NewTaggedStat("worker_buffer_capacity", stats.HistogramType, stats.Tags{"test": "workflow"}),
				currentSize:     memStats.NewTaggedStat("worker_buffer_size", stats.HistogramType, stats.Tags{"test": "workflow"}),
			}

			wb := newWorkerBuffer(5, func() int { return 5 }, bufferStats)

			// Complete workflow: reserve -> use -> check stats
			slot := wb.ReserveSlot()
			require.NotNil(t, slot)

			job := createTestWorkerJob()
			slot.Use(job)

			time.Sleep(20 * time.Millisecond)
			// Trigger stats recording after job usage
			wb.currentCapacity()

			// Verify metrics reflect the workflow
			sizeMetrics := memStats.GetByName("worker_buffer_size")
			require.Len(t, sizeMetrics, 1, "Expected exactly one size metric")
			require.Equal(t, stats.Tags{"test": "workflow"}, sizeMetrics[0].Tags)
			require.Len(t, sizeMetrics[0].Values, 2, "Expected exactly two size observations")
			require.Equal(t, float64(0), sizeMetrics[0].Values[0], "Expected size metric to show 0 jobs before Use()")
			require.Equal(t, float64(1), sizeMetrics[0].Values[1], "Expected size metric to show 1 job after Use()")
		})
	})
}

// mockValueLoader implements config.ValueLoader for testing
type mockValueLoader[T any] struct {
	value T
}

func (m *mockValueLoader[T]) Load() T {
	return m.value
}

// Helper function to create test workerJob
func createTestWorkerJob() workerJob {
	return workerJob{
		job: &jobsdb.JobT{
			JobID:      123,
			Parameters: []byte(`{"test": "data"}`),
		},
		parameters: &routerutils.JobParameters{
			DestinationID: "test-dest",
		},
		assignedAt: time.Now(),
	}
}
