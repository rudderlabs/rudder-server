package batchrouter

import (
	"fmt"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/jobsdb"
	asynccommon "github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/workerpool"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

// JobBuffer provides buffering capabilities for jobs based on source-destination pairs
type JobBuffer struct {
	sourceDestMap map[string]chan *jobsdb.JobT
	uploadTimers  map[string]*time.Timer
	mu            sync.RWMutex
	brt           *Handle

	// Worker pool for batch processing
	workerPool     workerpool.WorkerPool
	batchWorkers   map[string]*batchWorker
	batchWorkersMu sync.RWMutex
}

// batchWorker implements the workerpool.Worker interface for processing batches
type batchWorker struct {
	partition string
	jobs      []*ConnectionJob
	jobBuffer *JobBuffer
}

type ConnectionJob struct {
	job      *jobsdb.JobT
	sourceID string
	destID   string
}

// Work processes the current batch of jobs
func (bw *batchWorker) Work() bool {
	if len(bw.jobs) == 0 {
		return false
	}
	bw.jobs = make([]*ConnectionJob, 0, bw.jobBuffer.brt.maxEventsInABatch)

	// Use the existing upload logic
	uploadSuccess := true
	var uploadError error

	// Define a function to update the failing destinations map based on the result
	updateFailingStatus := func(failed bool, err error) {
		bw.jobBuffer.brt.failingDestinationsMu.Lock()
		bw.jobBuffer.brt.failingDestinations[bw.partition] = failed
		bw.jobBuffer.brt.failingDestinationsMu.Unlock()

		if failed {
			bw.jobBuffer.brt.logger.Errorf("Upload failed for destination %s: %v", bw.partition, err)
		}
	}

	// Execute the upload inside a defer context to always update status
	defer func() {
		updateFailingStatus(!uploadSuccess, uploadError)
	}()

	defer bw.jobBuffer.brt.limiter.upload.Begin(bw.partition)()

	// Convert jobs to BatchedJobs
	jobs := make([]*jobsdb.JobT, 0, len(bw.jobs))
	var destinationId, sourceId string
	for _, job := range bw.jobs {
		jobs = append(jobs, job.job)
		destinationId = job.destID
		sourceId = job.sourceID
	}
	batchedJobs := BatchedJobs{
		Jobs: jobs,
		Connection: &Connection{
			Destination: bw.jobBuffer.brt.destinationsMap[destinationId].Destination,
			Source:      bw.jobBuffer.brt.destinationsMap[sourceId].Sources[0],
		},
	}

	// Process based on destination type
	switch {
	case IsObjectStorageDestination(bw.jobBuffer.brt.destType):
		destUploadStat := bw.jobBuffer.createStat("batch_router_dest_upload_time", stats.TimerType, sourceId, destinationId)
		destUploadStart := time.Now()
		output := bw.jobBuffer.brt.upload(bw.jobBuffer.brt.destType, &batchedJobs, false)
		bw.jobBuffer.brt.recordDeliveryStatus(destinationId, sourceId, output, false)
		bw.jobBuffer.brt.updateJobStatus(&batchedJobs, false, output.Error, false)
		misc.RemoveFilePaths(output.LocalFilePaths...)
		if output.JournalOpID > 0 {
			bw.jobBuffer.brt.jobsDB.JournalDeleteEntry(output.JournalOpID)
		}
		if output.Error == nil {
			bw.jobBuffer.brt.recordUploadStats(*batchedJobs.Connection, output)
		} else {
			uploadSuccess = false
			uploadError = output.Error
		}
		destUploadStat.Since(destUploadStart)

	case IsWarehouseDestination(bw.jobBuffer.brt.destType):
		useRudderStorage := misc.IsConfiguredToUseRudderObjectStorage(batchedJobs.Connection.Destination.Config)
		objectStorageType := warehouseutils.ObjectStorageType(bw.jobBuffer.brt.destType, batchedJobs.Connection.Destination.Config, useRudderStorage)
		destUploadStat := bw.jobBuffer.createStat("batch_router_dest_upload_time", stats.TimerType, sourceId, destinationId)
		destUploadStart := time.Now()
		splitBatchJobs := bw.jobBuffer.brt.splitBatchJobsOnTimeWindow(batchedJobs)
		allSuccess := true
		for _, splitJob := range splitBatchJobs {
			output := bw.jobBuffer.brt.upload(objectStorageType, splitJob, true)
			notifyWarehouseErr := false
			if output.Error == nil && output.Key != "" {
				output.Error = bw.jobBuffer.brt.pingWarehouse(splitJob, output)
				if output.Error != nil {
					notifyWarehouseErr = true
					allSuccess = false
					uploadError = output.Error
				}
				warehouseutils.DestStat(stats.CountType, "generate_staging_files", destinationId).Count(1)
				warehouseutils.DestStat(stats.CountType, "staging_file_batch_size", destinationId).Count(len(splitJob.Jobs))
			} else if output.Error != nil {
				allSuccess = false
				uploadError = output.Error
			}
			bw.jobBuffer.brt.recordDeliveryStatus(destinationId, sourceId, output, true)
			bw.jobBuffer.brt.updateJobStatus(splitJob, true, output.Error, notifyWarehouseErr)
			misc.RemoveFilePaths(output.LocalFilePaths...)
		}
		uploadSuccess = allSuccess
		destUploadStat.Since(destUploadStart)

	case asynccommon.IsAsyncDestination(bw.jobBuffer.brt.destType):
		destUploadStat := bw.jobBuffer.createStat("batch_router_dest_upload_time", stats.TimerType, sourceId, destinationId)
		destUploadStart := time.Now()
		bw.jobBuffer.brt.sendJobsToStorage(batchedJobs)
		destUploadStat.Since(destUploadStart)

	default:
		errMsg := fmt.Errorf("unsupported destination type %s for job buffer", bw.jobBuffer.brt.destType)
		bw.jobBuffer.brt.logger.Errorf("Unsupported destination type %s for job buffer: %v", bw.jobBuffer.brt.destType, errMsg)
		uploadSuccess = false
		uploadError = errMsg
	}

	return true
}

// SleepDurations returns the min and max sleep durations for the worker when idle
func (bw *batchWorker) SleepDurations() (min, max time.Duration) {
	return time.Millisecond * 100, time.Second * 5
}

// Stop is a no-op since this worker doesn't have internal goroutines
func (bw *batchWorker) Stop() {
	// No-op
}

// createStat creates a new stat with consistent source and destination tags
func (jb *JobBuffer) createStat(name, statType, sourceID, destID string) stats.Measurement {
	return stats.Default.NewTaggedStat(name, statType, stats.Tags{
		"destType": jb.brt.destType,
		"sourceId": sourceID,
		"destId":   destID,
	})
}

// Key format: "sourceID:destinationID"
func getSourceDestKey(sourceID, destID string) string {
	return fmt.Sprintf("%s:%s", sourceID, destID)
}

// Initialize or get a job channel for a source-destination pair
func (jb *JobBuffer) getJobChannel(sourceID, destID string) chan *jobsdb.JobT {
	jb.mu.RLock()
	ch, exists := jb.sourceDestMap[getSourceDestKey(sourceID, destID)]
	jb.mu.RUnlock()

	if !exists {
		jb.mu.Lock()
		// Double-check to avoid race conditions
		if ch, exists = jb.sourceDestMap[getSourceDestKey(sourceID, destID)]; !exists {
			// Match buffer size to batch size for efficiency
			bufferSize := jb.brt.maxEventsInABatch
			if customSize := jb.brt.conf.GetIntVar(0, 0, "BatchRouter."+jb.brt.destType+".channelBufferSize", "BatchRouter.channelBufferSize"); customSize > 0 {
				bufferSize = customSize
			}

			ch = make(chan *jobsdb.JobT, bufferSize)
			jb.sourceDestMap[getSourceDestKey(sourceID, destID)] = ch

			// Start consumer for this channel
			go jb.startJobConsumer(sourceID, destID, ch)
		}
		jb.mu.Unlock()
	}

	return ch
}

// AddJob adds a job to the appropriate buffer channel
func (jb *JobBuffer) AddJob(sourceID, destID string, job *jobsdb.JobT) {
	// Get or create channel and send job (non-blocking check)
	ch := jb.getJobChannel(sourceID, destID)

	// Try to send the job to the channel, but don't block if it's full
	select {
	case ch <- job:
		// Job added successfully
	default:
		// Channel is full, log warning
		jb.brt.logger.Warnf("Buffer channel full for %s:%s, job %d will be processed in next batch",
			sourceID, destID, job.JobID)
		// Force add to the channel (blocking)
		ch <- job
	}
}

// Stop gracefully stops all job consumers and cleans up resources
func (jb *JobBuffer) Stop() {
	jb.mu.Lock()
	defer jb.mu.Unlock()

	// Close all channels to signal consumers to stop
	for key, ch := range jb.sourceDestMap {
		close(ch)
		delete(jb.sourceDestMap, key)

		// Cancel timers
		if timer, ok := jb.uploadTimers[key]; ok {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			delete(jb.uploadTimers, key)
		}
	}

	// Shutdown the worker pool if it exists
	jb.workerPool.Shutdown()
}

// startJobConsumer starts a consumer for a source-destination pair that processes jobs using the worker pool
func (jb *JobBuffer) startJobConsumer(sourceID, destID string, ch chan *jobsdb.JobT) {
	key := getSourceDestKey(sourceID, destID)
	var lastJobTime time.Time

	// Simple consumer loop that forwards jobs to the worker
	for job := range ch {
		jb.batchWorkersMu.Lock()
		worker := jb.batchWorkers[key]
		worker.jobs = append(worker.jobs, &ConnectionJob{
			job:      job,
			sourceID: sourceID,
			destID:   destID,
		})
		currentBatchSize := len(worker.jobs)
		jb.batchWorkersMu.Unlock()

		if lastJobTime.IsZero() {
			lastJobTime = time.Now()
		}

		// Ping worker when either:
		// 1. We've reached max batch size
		// 2. Upload frequency time has elapsed since first job
		if currentBatchSize >= jb.brt.maxEventsInABatch || time.Since(lastJobTime) >= jb.brt.uploadFreq.Load() {
			jb.workerPool.PingWorker(key)
			lastJobTime = time.Time{} // Reset timer after ping
		}
	}

	// Process any remaining jobs when channel is closed
	jb.batchWorkersMu.Lock()
	if worker := jb.batchWorkers[key]; len(worker.jobs) > 0 {
		jb.workerPool.PingWorker(key)
	}
	jb.batchWorkersMu.Unlock()
}
