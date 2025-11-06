package router

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/stats/metric"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/router/types"
	routerutils "github.com/rudderlabs/rudder-server/router/utils"
)

func TestWorkerBatchLoop(t *testing.T) {
	t.Run("no batching and no transform at router, acceptWorkerJob returning always nil", func(t *testing.T) {
		// Setup
		inputCh := make(chan workerJob, 10)
		batchSize := 5

		var processedJobs []types.DestinationJobT
		var transformCalls [][]types.RouterJobT
		var batchTransformCalls [][]types.RouterJobT
		var mu sync.Mutex

		wl := &workerBatchLoop{
			ctx:                      context.Background(),
			jobsBatchTimeout:         config.SingleValueLoader(50 * time.Millisecond),
			noOfJobsToBatchInAWorker: config.SingleValueLoader(batchSize),
			inputCh:                  inputCh,
			enableBatching:           false,
			batchTransform: func(routerJobs []types.RouterJobT) []types.DestinationJobT {
				mu.Lock()
				defer mu.Unlock()
				batchTransformCalls = append(batchTransformCalls, routerJobs)
				return []types.DestinationJobT{}
			},
			transform: func(routerJobs []types.RouterJobT) []types.DestinationJobT {
				mu.Lock()
				defer mu.Unlock()
				transformCalls = append(transformCalls, routerJobs)
				return []types.DestinationJobT{}
			},
			process: func(destinationJobs []types.DestinationJobT) {
				mu.Lock()
				defer mu.Unlock()
				processedJobs = append(processedJobs, destinationJobs...)
			},
			acceptWorkerJob: func(workerJob workerJob) *types.RouterJobT {
				// Always return nil to simulate accepting no jobs
				return nil
			},
			throughputStat: metric.NewSimpleMovingAverage(1),
		}

		// Start the worker loop in a goroutine
		done := make(chan struct{})
		go func() {
			defer close(done)
			wl.runLoop()
		}()

		// Send some jobs with transform_at != "router"
		for i := range 3 {
			job := &jobsdb.JobT{
				JobID:  int64(i + 1),
				UserID: "user1",
			}
			params := &routerutils.JobParameters{
				TransformAt: "processor", // Not "router"
			}
			inputCh <- workerJob{job: job, parameters: params}
		}

		// Wait a bit to ensure jobs are processed and some timeouts are triggered too
		time.Sleep(200 * time.Millisecond)

		// Close the input channel to stop the loop
		close(inputCh)

		// Wait for the loop to finish
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("Worker loop did not finish in time")
		}

		// Verify results
		mu.Lock()
		defer mu.Unlock()

		// Since acceptWorkerJob always returns nil, no router jobs should be created
		// Therefore no transform/batchTransform calls should happen
		assert.Empty(t, transformCalls, "No transform calls should happen when acceptWorkerJob returns nil")
		assert.Empty(t, batchTransformCalls, "No batch transform calls should happen when acceptWorkerJob returns nil")
		assert.Empty(t, processedJobs, "No jobs should be processed when acceptWorkerJob returns nil")
	})

	t.Run("no batching and transform at router, then switch to no router transform", func(t *testing.T) {
		// Setup
		inputCh := make(chan workerJob, 10)
		batchSize := 5

		var processedJobs []types.DestinationJobT
		var transformCalls [][]types.RouterJobT
		var batchTransformCalls [][]types.RouterJobT
		var mu sync.Mutex

		// Track how many jobs we've seen to control the behavior of acceptWorkerJob
		jobCount := 0

		wl := &workerBatchLoop{
			ctx:                      context.Background(),
			jobsBatchTimeout:         config.SingleValueLoader(50 * time.Millisecond),
			noOfJobsToBatchInAWorker: config.SingleValueLoader(batchSize),
			inputCh:                  inputCh,
			enableBatching:           false,
			batchTransform: func(routerJobs []types.RouterJobT) []types.DestinationJobT {
				mu.Lock()
				defer mu.Unlock()
				batchTransformCalls = append(batchTransformCalls, routerJobs)
				// Create destination jobs from router jobs
				var destJobs []types.DestinationJobT
				for _, rj := range routerJobs {
					destJobs = append(destJobs, types.DestinationJobT{
						Message:     rj.Message,
						Destination: rj.Destination,
					})
				}
				return destJobs
			},
			transform: func(routerJobs []types.RouterJobT) []types.DestinationJobT {
				mu.Lock()
				defer mu.Unlock()
				transformCalls = append(transformCalls, routerJobs)
				// Create destination jobs from router jobs
				var destJobs []types.DestinationJobT
				for _, rj := range routerJobs {
					destJobs = append(destJobs, types.DestinationJobT{
						Message:     rj.Message,
						Destination: rj.Destination,
					})
				}
				return destJobs
			},
			process: func(destinationJobs []types.DestinationJobT) {
				mu.Lock()
				defer mu.Unlock()
				processedJobs = append(processedJobs, destinationJobs...)
			},
			acceptWorkerJob: func(workerJob workerJob) *types.RouterJobT {
				if workerJob.parameters.TransformAt == "router" {
					return &types.RouterJobT{
						Message: json.RawMessage(`{"jobId": ` + string(rune(jobCount+'0')) + `}`),
						Destination: backendconfig.DestinationT{
							ID: "dest1",
						},
					}
				}
				return nil
			},
			throughputStat: metric.NewSimpleMovingAverage(1),
		}

		// Start the worker loop in a goroutine
		done := make(chan struct{})
		go func() {
			defer close(done)
			wl.runLoop()
		}()

		// Send jobs with transform_at = "router" first
		for i := range 2 {
			job := &jobsdb.JobT{
				JobID:  int64(i + 1),
				UserID: "user1",
			}
			params := &routerutils.JobParameters{
				TransformAt: "router",
			}
			inputCh <- workerJob{job: job, parameters: params}
		}

		// Send a job with transform_at != "router" - this should trigger batch processing
		job := &jobsdb.JobT{
			JobID:  3,
			UserID: "user1",
		}
		params := &routerutils.JobParameters{
			TransformAt: "processor", // Not "router"
		}
		inputCh <- workerJob{job: job, parameters: params}

		// Wait a bit to ensure processing
		time.Sleep(200 * time.Millisecond)

		// Close the input channel to stop the loop
		close(inputCh)

		// Wait for the loop to finish
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("Worker loop did not finish in time")
		}

		// Verify results
		mu.Lock()
		defer mu.Unlock()

		// We should have at least one transform call (when switching from router to processor)
		assert.NotEmpty(t, transformCalls, "Should have transform calls when processing router jobs")
		assert.Empty(t, batchTransformCalls, "Should not have batch transform calls when batching is disabled")

		// Verify that the first batch was processed
		assert.Len(t, transformCalls, 1, "Should have exactly one transform call")
		assert.Len(t, transformCalls[0], 2, "First batch should contain 2 router jobs")
		assert.Len(t, processedJobs, 2, "Should have processed 2 destination jobs")
	})

	t.Run("with batching enabled", func(t *testing.T) {
		// Setup
		inputCh := make(chan workerJob, 10)
		batchSize := 3

		var processedJobs []types.DestinationJobT
		var transformCalls [][]types.RouterJobT
		var batchTransformCalls [][]types.RouterJobT
		var mu sync.Mutex

		wl := &workerBatchLoop{
			ctx:                      context.Background(),
			jobsBatchTimeout:         config.SingleValueLoader(50 * time.Millisecond),
			noOfJobsToBatchInAWorker: config.SingleValueLoader(batchSize),
			inputCh:                  inputCh,
			enableBatching:           true, // Enable batching
			batchTransform: func(routerJobs []types.RouterJobT) []types.DestinationJobT {
				mu.Lock()
				defer mu.Unlock()
				batchTransformCalls = append(batchTransformCalls, routerJobs)
				// Create destination jobs from router jobs
				var destJobs []types.DestinationJobT
				for _, rj := range routerJobs {
					destJobs = append(destJobs, types.DestinationJobT{
						Message:     rj.Message,
						Destination: rj.Destination,
					})
				}
				return destJobs
			},
			transform: func(routerJobs []types.RouterJobT) []types.DestinationJobT {
				mu.Lock()
				defer mu.Unlock()
				transformCalls = append(transformCalls, routerJobs)
				return []types.DestinationJobT{}
			},
			process: func(destinationJobs []types.DestinationJobT) {
				mu.Lock()
				defer mu.Unlock()
				processedJobs = append(processedJobs, destinationJobs...)
			},
			acceptWorkerJob: func(workerJob workerJob) *types.RouterJobT {
				return &types.RouterJobT{
					Message: json.RawMessage(`{"jobId": ` + string(rune(workerJob.job.JobID+'0')) + `}`),
					Destination: backendconfig.DestinationT{
						ID: "dest1",
					},
				}
			},
			throughputStat: metric.NewSimpleMovingAverage(1),
		}

		// Start the worker loop in a goroutine
		done := make(chan struct{})
		go func() {
			defer close(done)
			wl.runLoop()
		}()

		// Send jobs to reach the batch size
		for i := 0; i < batchSize; i++ {
			job := &jobsdb.JobT{
				JobID:  int64(i + 1),
				UserID: "user1",
			}
			params := &routerutils.JobParameters{
				TransformAt: "router",
			}
			inputCh <- workerJob{job: job, parameters: params}
		}

		// Wait for batch processing
		time.Sleep(50 * time.Millisecond)

		// Send one more job to test that new batch starts
		job := &jobsdb.JobT{
			JobID:  int64(batchSize + 1),
			UserID: "user1",
		}
		params := &routerutils.JobParameters{
			TransformAt: "router",
		}
		inputCh <- workerJob{job: job, parameters: params}

		// Wait for timeout to trigger processing of remaining jobs
		time.Sleep(200*time.Millisecond + 50*time.Millisecond)

		// Close the input channel to stop the loop
		close(inputCh)

		// Wait for the loop to finish
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("Worker loop did not finish in time")
		}

		// Verify results
		mu.Lock()
		defer mu.Unlock()

		// When batching is enabled, should use batchTransform, not transform
		assert.Empty(t, transformCalls, "Should not have transform calls when batching is enabled")
		assert.NotEmpty(t, batchTransformCalls, "Should have batch transform calls when batching is enabled")

		// Should have processed jobs in batches
		assert.Len(t, batchTransformCalls, 2, "Should have 2 batch transform calls")
		assert.Len(t, batchTransformCalls[0], batchSize, "First batch should have batch size jobs")
		assert.Len(t, batchTransformCalls[1], 1, "Second batch should have 1 job")
		assert.Len(t, processedJobs, batchSize+1, "Should have processed all jobs")
	})

	t.Run("jobsBatchTimeout resets even when no jobs during timeout", func(t *testing.T) {
		// Setup
		inputCh := make(chan workerJob, 10)
		batchSize := 5

		var timeoutFireCount int
		var transformCallCount int
		var mu sync.Mutex

		wl := &workerBatchLoop{
			ctx:                      context.Background(),
			jobsBatchTimeout:         config.SingleValueLoader(50 * time.Millisecond), // Shorter timeout for testing
			noOfJobsToBatchInAWorker: config.SingleValueLoader(batchSize),
			inputCh:                  inputCh,
			enableBatching:           false,
			batchTransform: func(routerJobs []types.RouterJobT) []types.DestinationJobT {
				mu.Lock()
				defer mu.Unlock()
				transformCallCount++
				return []types.DestinationJobT{}
			},
			transform: func(routerJobs []types.RouterJobT) []types.DestinationJobT {
				mu.Lock()
				defer mu.Unlock()
				transformCallCount++
				return []types.DestinationJobT{}
			},
			process: func(destinationJobs []types.DestinationJobT) {
				// Process function - will be called when there are jobs to process
			},
			acceptWorkerJob: func(workerJob workerJob) *types.RouterJobT {
				// Return a router job for the first few calls, then nil
				mu.Lock()
				defer mu.Unlock()
				timeoutFireCount++
				if timeoutFireCount <= 3 { // Return jobs for first 3 calls to ensure we have jobs to process
					return &types.RouterJobT{
						Message: json.RawMessage(`{"test": true}`),
						Destination: backendconfig.DestinationT{
							ID: "test-dest",
						},
					}
				}
				return nil
			},
			throughputStat: metric.NewSimpleMovingAverage(1),
		}

		// Start the worker loop in a goroutine
		done := make(chan struct{})
		go func() {
			defer close(done)
			wl.runLoop()
		}()

		// Send a few jobs with some delay between them to trigger timeout behavior
		for i := range 3 {
			job := &jobsdb.JobT{
				JobID:  int64(i + 1),
				UserID: "user1",
			}
			params := &routerutils.JobParameters{
				TransformAt: "router",
			}
			inputCh <- workerJob{job: job, parameters: params}

			// Wait longer than timeout to ensure timeout fires
			time.Sleep(60 * time.Millisecond)
		}

		// Wait a bit more for final processing
		time.Sleep(100 * time.Millisecond)

		// Close the input channel to stop the loop
		close(inputCh)

		// Wait for the loop to finish
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("Worker loop did not finish in time")
		}

		// Verify that timeout was handled multiple times
		mu.Lock()
		defer mu.Unlock()

		// We should have multiple transform calls due to timeout processing
		// Each timeout should trigger processing of accumulated jobs
		assert.GreaterOrEqual(t, transformCallCount, 2, "Timeout should fire multiple times and process batches")

		// Verify we had multiple job acceptance calls
		assert.GreaterOrEqual(t, timeoutFireCount, 3, "Should have processed multiple jobs")
	})

	t.Run("batch processing when batch size is reached", func(t *testing.T) {
		// Setup
		inputCh := make(chan workerJob, 10)
		batchSize := 2 // Small batch size for testing

		var processedBatches [][]types.DestinationJobT
		var mu sync.Mutex

		wl := &workerBatchLoop{
			ctx:                      context.Background(),
			jobsBatchTimeout:         config.SingleValueLoader(1 * time.Second), // Long timeout so it doesn't interfere
			noOfJobsToBatchInAWorker: config.SingleValueLoader(batchSize),
			inputCh:                  inputCh,
			enableBatching:           false,
			batchTransform: func(routerJobs []types.RouterJobT) []types.DestinationJobT {
				return []types.DestinationJobT{}
			},
			transform: func(routerJobs []types.RouterJobT) []types.DestinationJobT {
				// Create destination jobs from router jobs
				var destJobs []types.DestinationJobT
				for _, rj := range routerJobs {
					destJobs = append(destJobs, types.DestinationJobT{
						Message:     rj.Message,
						Destination: rj.Destination,
					})
				}
				return destJobs
			},
			process: func(destinationJobs []types.DestinationJobT) {
				mu.Lock()
				defer mu.Unlock()
				if len(destinationJobs) > 0 {
					processedBatches = append(processedBatches, destinationJobs)
				}
			},
			acceptWorkerJob: func(workerJob workerJob) *types.RouterJobT {
				return &types.RouterJobT{
					Message: json.RawMessage(`{"jobId": ` + string(rune(workerJob.job.JobID+'0')) + `}`),
					Destination: backendconfig.DestinationT{
						ID: "dest1",
					},
				}
			},
			throughputStat: metric.NewSimpleMovingAverage(1),
		}

		// Start the worker loop in a goroutine
		done := make(chan struct{})
		go func() {
			defer close(done)
			wl.runLoop()
		}()

		// Send exactly batch size jobs
		for i := 0; i < batchSize; i++ {
			job := &jobsdb.JobT{
				JobID:  int64(i + 1),
				UserID: "user1",
			}
			params := &routerutils.JobParameters{
				TransformAt: "router",
			}
			inputCh <- workerJob{job: job, parameters: params}
		}

		// Wait for processing
		time.Sleep(50 * time.Millisecond)

		// Send one more batch
		for i := 0; i < batchSize; i++ {
			job := &jobsdb.JobT{
				JobID:  int64(batchSize + i + 1),
				UserID: "user1",
			}
			params := &routerutils.JobParameters{
				TransformAt: "router",
			}
			inputCh <- workerJob{job: job, parameters: params}
		}

		// Wait for processing
		time.Sleep(50 * time.Millisecond)

		// Close the input channel to stop the loop
		close(inputCh)

		// Wait for the loop to finish
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("Worker loop did not finish in time")
		}

		// Verify results
		mu.Lock()
		defer mu.Unlock()

		// Should have processed 2 batches, each with batchSize jobs
		require.Len(t, processedBatches, 2, "Should have processed exactly 2 batches")
		assert.Len(t, processedBatches[0], batchSize, "First batch should have correct size")
		assert.Len(t, processedBatches[1], batchSize, "Second batch should have correct size")
	})
}
