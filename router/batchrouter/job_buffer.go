package batchrouter

import (
	"fmt"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-go-kit/stats"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	asynccommon "github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	"github.com/rudderlabs/rudder-server/utils/misc"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

// JobBuffer provides buffering capabilities for jobs based on source-destination pairs
type JobBuffer struct {
	sourceDestMap map[string]chan *jobsdb.JobT
	uploadTimers  map[string]*time.Timer
	mu            sync.RWMutex
	brt           *Handle
}

// createStat creates a new stat with consistent destination, source, and destination type tags
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
	key := getSourceDestKey(sourceID, destID)

	jb.mu.RLock()
	ch, exists := jb.sourceDestMap[key]
	jb.mu.RUnlock()

	if !exists {
		jb.mu.Lock()
		// Double-check to avoid race conditions
		if ch, exists = jb.sourceDestMap[key]; !exists {
			bufferSize := jb.brt.conf.GetIntVar(100000, 1, "BatchRouter."+jb.brt.destType+".channelBufferSize", "BatchRouter.channelBufferSize")
			ch = make(chan *jobsdb.JobT, bufferSize)
			jb.sourceDestMap[key] = ch

			// Start consumer for this channel
			go jb.startJobConsumer(sourceID, destID, ch)
		}
		jb.mu.Unlock()
	}

	return ch
}

func (jb *JobBuffer) startJobConsumer(sourceID, destID string, jobCh chan *jobsdb.JobT) {
	key := getSourceDestKey(sourceID, destID)
	jobBatch := make([]*jobsdb.JobT, 0)

	// Configure upload frequency from settings
	jb.brt.configSubscriberMu.RLock()
	uploadFreq := jb.brt.uploadIntervalMap[destID]
	jb.brt.configSubscriberMu.RUnlock()

	if uploadFreq == 0 {
		uploadFreq = jb.brt.uploadFreq.Load()
	}

	// Create a semaphore to limit concurrent uploads
	maxConcurrentUploads := jb.brt.conf.GetIntVar(1, 1, "BatchRouter."+jb.brt.destType+".maxConcurrentUploads", "BatchRouter.maxConcurrentUploads")
	uploadSemaphore := make(chan struct{}, maxConcurrentUploads)

	jb.brt.logger.Debugf("Starting job consumer for %s:%s with upload frequency %s and max concurrent uploads %d",
		sourceID, destID, uploadFreq, maxConcurrentUploads)

	// Start timer for this source-destination pair
	timer := time.NewTimer(uploadFreq)
	jb.mu.Lock()
	jb.uploadTimers[key] = timer
	jb.mu.Unlock()

	// Helper function to safely stop and reset the timer
	resetTimer := func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(uploadFreq)
	}

	// Helper function to upload batch asynchronously
	processAsyncBatch := func(batch []*jobsdb.JobT) {
		// Try to acquire a slot from the semaphore, block if all slots are used
		select {
		case uploadSemaphore <- struct{}{}:
			// We got a slot, launch the upload in a goroutine
			go func(jobsToProcess []*jobsdb.JobT) {
				defer func() { <-uploadSemaphore }() // Release the semaphore when done

				if len(jobsToProcess) > 0 {
					batchSize := len(jobsToProcess)
					jb.brt.logger.Debugf("Processing batch of %d jobs for %s:%s asynchronously",
						batchSize, sourceID, destID)
					jb.processAndUploadBatch(sourceID, destID, jobsToProcess)
				}
			}(batch)
		case <-jb.brt.backgroundCtx.Done():
			// Context canceled, don't start new uploads
			jb.brt.logger.Debugf("Context canceled while waiting for upload slot for %s:%s", sourceID, destID)
			return
		}
	}

	for {
		select {
		case job, ok := <-jobCh:
			// Channel closed or context cancelled
			if !ok {
				jb.brt.logger.Debugf("Channel closed for %s:%s, processing remaining jobs and exiting", sourceID, destID)
				if len(jobBatch) > 0 {
					// Make a copy of the batch to avoid race conditions
					batchToProcess := make([]*jobsdb.JobT, len(jobBatch))
					copy(batchToProcess, jobBatch)
					processAsyncBatch(batchToProcess)
				}
				return
			}
			jobBatch = append(jobBatch, job)

			// Check if batch size threshold is reached
			if len(jobBatch) >= jb.brt.maxEventsInABatch {
				jb.brt.logger.Debugf("Batch size threshold reached for %s:%s: %d jobs", sourceID, destID, len(jobBatch))

				// Make a copy of the batch to avoid race conditions
				batchToProcess := make([]*jobsdb.JobT, len(jobBatch))
				copy(batchToProcess, jobBatch)
				processAsyncBatch(batchToProcess)

				// Clear the batch and reset timer
				jobBatch = make([]*jobsdb.JobT, 0)
				resetTimer()
			}

		case <-timer.C:
			// Upload on timer expiry if we have jobs
			jb.brt.logger.Debugf("Timer expired for %s:%s with %d jobs in batch", sourceID, destID, len(jobBatch))

			if len(jobBatch) > 0 {
				// Make a copy of the batch to avoid race conditions
				batchToProcess := make([]*jobsdb.JobT, len(jobBatch))
				copy(batchToProcess, jobBatch)
				processAsyncBatch(batchToProcess)

				// Clear the batch
				jobBatch = make([]*jobsdb.JobT, 0)
			}

			timer.Reset(uploadFreq)

		case <-jb.brt.backgroundCtx.Done():
			// Clean shutdown
			jb.brt.logger.Debugf("Shutting down job consumer for %s:%s with %d jobs in batch", sourceID, destID, len(jobBatch))

			if len(jobBatch) > 0 {
				// Make a copy of the batch to avoid race conditions
				batchToProcess := make([]*jobsdb.JobT, len(jobBatch))
				copy(batchToProcess, jobBatch)

				// Process synchronously during shutdown to ensure all jobs are handled
				jb.processAndUploadBatch(sourceID, destID, batchToProcess)
			}

			return
		}
	}
}

func (jb *JobBuffer) processAndUploadBatch(sourceID, destID string, jobs []*jobsdb.JobT) {
	if len(jobs) == 0 {
		return
	}

	// Track metrics for batch processing
	processedBatchSizeStat := jb.createStat("batch_router_processed_batch_size", stats.GaugeType, sourceID, destID)
	processedBatchSizeStat.Gauge(len(jobs))

	processingStartTime := time.Now()
	defer func() {
		jb.createStat("batch_router_buffered_batch_processing_time", stats.TimerType, sourceID, destID).Since(processingStartTime)
	}()

	// Get destination and source details
	jb.brt.configSubscriberMu.RLock()
	destWithSources, ok := jb.brt.destinationsMap[destID]
	jb.brt.configSubscriberMu.RUnlock()

	if !ok {
		// Handle destination not found
		jb.brt.logger.Errorf("Destination not found for ID: %s", destID)
		return
	}

	// Find the source
	var source backendconfig.SourceT
	sourceFound := false
	for _, s := range destWithSources.Sources {
		if s.ID == sourceID {
			source = s
			sourceFound = true
			break
		}
	}

	if !sourceFound {
		// Handle source not found
		jb.brt.logger.Errorf("Source not found for ID: %s", sourceID)
		return
	}

	batchedJobs := BatchedJobs{
		Jobs: jobs,
		Connection: &Connection{
			Destination: destWithSources.Destination,
			Source:      source,
		},
	}

	// Use the existing upload logic
	defer jb.brt.limiter.upload.Begin("")()

	// Helper function for standard object storage upload process
	processObjectStorageUpload := func(destType string, isWarehouse bool) {
		destUploadStat := jb.createStat("batch_router_dest_upload_time", stats.TimerType, sourceID, destID)
		destUploadStart := time.Now()
		output := jb.brt.upload(destType, &batchedJobs, isWarehouse)
		jb.brt.recordDeliveryStatus(*batchedJobs.Connection, output, isWarehouse)
		jb.brt.updateJobStatus(&batchedJobs, isWarehouse, output.Error, false)
		misc.RemoveFilePaths(output.LocalFilePaths...)
		if output.JournalOpID > 0 {
			jb.brt.jobsDB.JournalDeleteEntry(output.JournalOpID)
		}
		if output.Error == nil {
			jb.brt.recordUploadStats(*batchedJobs.Connection, output)
		}
		destUploadStat.Since(destUploadStart)
	}

	switch {
	case IsObjectStorageDestination(jb.brt.destType):
		processObjectStorageUpload(jb.brt.destType, false)
	case IsWarehouseDestination(jb.brt.destType):
		useRudderStorage := misc.IsConfiguredToUseRudderObjectStorage(batchedJobs.Connection.Destination.Config)
		objectStorageType := warehouseutils.ObjectStorageType(jb.brt.destType, batchedJobs.Connection.Destination.Config, useRudderStorage)
		destUploadStat := jb.createStat("batch_router_dest_upload_time", stats.TimerType, sourceID, destID)
		destUploadStart := time.Now()
		splitBatchJobs := jb.brt.splitBatchJobsOnTimeWindow(batchedJobs)
		for _, batchJob := range splitBatchJobs {
			output := jb.brt.upload(objectStorageType, batchJob, true)
			notifyWarehouseErr := false
			if output.Error == nil && output.Key != "" {
				output.Error = jb.brt.pingWarehouse(batchJob, output)
				if output.Error != nil {
					notifyWarehouseErr = true
				}
				warehouseutils.DestStat(stats.CountType, "generate_staging_files", batchJob.Connection.Destination.ID).Count(1)
				warehouseutils.DestStat(stats.CountType, "staging_file_batch_size", batchJob.Connection.Destination.ID).Count(len(batchJob.Jobs))
			}
			jb.brt.recordDeliveryStatus(*batchJob.Connection, output, true)
			jb.brt.updateJobStatus(batchJob, true, output.Error, notifyWarehouseErr)
			misc.RemoveFilePaths(output.LocalFilePaths...)
		}
		destUploadStat.Since(destUploadStart)
	case asynccommon.IsAsyncDestination(jb.brt.destType):
		destUploadStat := jb.createStat("batch_router_dest_upload_time", stats.TimerType, sourceID, destID)
		destUploadStart := time.Now()
		jb.brt.sendJobsToStorage(batchedJobs)
		destUploadStat.Since(destUploadStart)
	default:
		// Handle any other destination types
		jb.brt.logger.Warnf("Unsupported destination type %s for job buffer. Attempting generic processing.", jb.brt.destType)
		panic(fmt.Sprintf("Unsupported destination type %s for job buffer. Attempting generic processing.", jb.brt.destType))
	}
}
