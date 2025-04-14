package batchrouter

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	asynccommon "github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	"github.com/rudderlabs/rudder-server/utils/misc"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

// BatchWorker processes batches of jobs by grouping them by connection (source-destination pair).
// It implements the workerpool.Worker interface to integrate with the worker pool system.
type BatchWorker struct {
	partition string
	logger    logger.Logger
	brt       *Handle
	getJobs   func(string) []*ConnectionJob // Function to get jobs for this partition
}

// NewBatchWorker creates a new batch worker for the given partition
func NewBatchWorker(partition string, logger logger.Logger, brt *Handle, getJobs func(string) []*ConnectionJob) *BatchWorker {
	return &BatchWorker{
		partition: partition,
		logger:    logger.Child("batch-worker").With("partition", partition),
		brt:       brt,
		getJobs:   getJobs,
	}
}

// Work processes any available jobs in the shared job queue.
// Returns true if any jobs were processed.
func (bw *BatchWorker) Work() bool {
	// Get jobs from the shared queue
	jobs := bw.getJobs(bw.partition)
	if len(jobs) == 0 {
		bw.logger.Debugf("No jobs to process")
		return false
	}

	// Group jobs by connection
	jobsByConnection := make(map[string][]*ConnectionJob)
	for _, job := range jobs {
		connKey := fmt.Sprintf("%s:%s", job.sourceID, job.destID)
		jobsByConnection[connKey] = append(jobsByConnection[connKey], job)
	}

	// Process each connection's jobs
	var wg sync.WaitGroup
	for connKey, connJobs := range jobsByConnection {
		wg.Add(1)
		go func(key string, jobs []*ConnectionJob) {
			defer wg.Done()
			sourceID, destID := parseConnectionKey(key)
			bw.processConnectionJobs(sourceID, destID, jobs)
		}(connKey, connJobs)
	}
	wg.Wait()

	return true
}

// processConnectionJobs handles the processing of jobs for a specific source-destination connection
func (bw *BatchWorker) processConnectionJobs(sourceID, destID string, jobs []*ConnectionJob) {
	bw.logger.Infof("Processing batch of %d jobs for connection %s:%s", len(jobs), sourceID, destID)

	// Prepare batch for processing
	batchedJobs, err := bw.prepareBatch(sourceID, destID, jobs)
	if err != nil {
		bw.logger.Errorf("Failed to prepare batch: %v", err)
		return
	}

	// Process the batch based on destination type
	uploadSuccess, uploadError := bw.processBatch(sourceID, destID, batchedJobs)

	// Update failing destinations status
	bw.updateFailingStatus(destID, !uploadSuccess, uploadError)
}

// prepareBatch converts jobs into a BatchedJobs structure and validates the source and destination configuration
func (bw *BatchWorker) prepareBatch(sourceID, destID string, jobs []*ConnectionJob) (*BatchedJobs, error) {
	// Convert jobs to BatchedJobs format
	jobsToUpload := make([]*jobsdb.JobT, 0, len(jobs))
	for _, job := range jobs {
		jobsToUpload = append(jobsToUpload, job.job)
	}

	// Get destination and source configuration
	bw.brt.configSubscriberMu.RLock()
	destWithSources, ok := bw.brt.destinationsMap[destID]
	bw.brt.configSubscriberMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("destination %s not found in destinationsMap", destID)
	}

	// Find matching source
	var source *backendconfig.SourceT
	for _, s := range destWithSources.Sources {
		if s.ID == sourceID {
			source = &s
			break
		}
	}
	if source == nil {
		return nil, fmt.Errorf("source %s not found in destination %s", sourceID, destID)
	}

	return &BatchedJobs{
		Jobs: jobsToUpload,
		Connection: &Connection{
			Destination: destWithSources.Destination,
			Source:      *source,
		},
	}, nil
}

// processBatch handles the upload process based on the destination type
func (bw *BatchWorker) processBatch(sourceID, destID string, batchedJobs *BatchedJobs) (bool, error) {
	defer bw.brt.limiter.upload.Begin(destID)()

	destUploadStat := stats.Default.NewTaggedStat("batch_router_dest_upload_time", stats.TimerType, map[string]string{
		"sourceID": sourceID,
		"destID":   destID,
	})
	destUploadStart := time.Now()
	defer destUploadStat.Since(destUploadStart)

	switch {
	case IsObjectStorageDestination(bw.brt.destType):
		return bw.handleObjectStorageUpload(sourceID, destID, batchedJobs)
	case IsWarehouseDestination(bw.brt.destType):
		return bw.handleWarehouseUpload(sourceID, destID, batchedJobs)
	case asynccommon.IsAsyncDestination(bw.brt.destType):
		return bw.handleAsyncDestinationUpload(batchedJobs), nil
	default:
		err := fmt.Errorf("unsupported destination type %s for job buffer", bw.brt.destType)
		bw.logger.Error(err)
		return false, err
	}
}

// handleObjectStorageUpload processes uploads for object storage destinations
func (bw *BatchWorker) handleObjectStorageUpload(sourceID, destID string, batchedJobs *BatchedJobs) (bool, error) {
	output := bw.brt.upload(bw.brt.destType, batchedJobs, false)
	defer misc.RemoveFilePaths(output.LocalFilePaths...)

	bw.brt.recordDeliveryStatus(destID, sourceID, output, false)
	bw.brt.updateJobStatus(batchedJobs, false, output.Error, false)

	if output.JournalOpID > 0 {
		bw.brt.jobsDB.JournalDeleteEntry(output.JournalOpID)
	}

	if output.Error == nil {
		bw.brt.recordUploadStats(*batchedJobs.Connection, output)
		return true, nil
	}
	return false, output.Error
}

// handleWarehouseUpload processes uploads for warehouse destinations
func (bw *BatchWorker) handleWarehouseUpload(sourceID, destID string, batchedJobs *BatchedJobs) (bool, error) {
	useRudderStorage := misc.IsConfiguredToUseRudderObjectStorage(batchedJobs.Connection.Destination.Config)
	objectStorageType := warehouseutils.ObjectStorageType(bw.brt.destType, batchedJobs.Connection.Destination.Config, useRudderStorage)

	splitBatchJobs := bw.brt.splitBatchJobsOnTimeWindow(*batchedJobs)
	var lastError error
	allSuccess := true

	for _, splitJob := range splitBatchJobs {
		output := bw.brt.upload(objectStorageType, splitJob, true)
		defer misc.RemoveFilePaths(output.LocalFilePaths...)

		notifyWarehouseErr := false
		if output.Error == nil && output.Key != "" {
			output.Error = bw.brt.pingWarehouse(splitJob, output)
			if output.Error != nil {
				notifyWarehouseErr = true
				allSuccess = false
				lastError = output.Error
			}
			warehouseutils.DestStat(stats.CountType, "generate_staging_files", destID).Count(1)
			warehouseutils.DestStat(stats.CountType, "staging_file_batch_size", destID).Count(len(splitJob.Jobs))
		} else if output.Error != nil {
			allSuccess = false
			lastError = output.Error
		}

		bw.brt.recordDeliveryStatus(destID, sourceID, output, true)
		bw.brt.updateJobStatus(splitJob, true, output.Error, notifyWarehouseErr)
	}

	return allSuccess, lastError
}

// handleAsyncDestinationUpload processes uploads for async destinations
func (bw *BatchWorker) handleAsyncDestinationUpload(batchedJobs *BatchedJobs) bool {
	bw.brt.sendJobsToStorage(*batchedJobs)
	return true
}

// updateFailingStatus updates the failing destinations map with the current status
func (bw *BatchWorker) updateFailingStatus(destID string, failed bool, err error) {
	bw.brt.failingDestinationsMu.Lock()
	bw.brt.failingDestinations[destID] = failed
	bw.brt.failingDestinationsMu.Unlock()

	if failed {
		bw.logger.Errorf("Upload failed for destination %s: %v", destID, err)
	}
}

// SleepDurations returns the min and max sleep durations for the worker when idle
func (bw *BatchWorker) SleepDurations() (min, max time.Duration) {
	return time.Millisecond * 100, time.Second * 5
}

// Stop cleans up any resources used by the worker
func (bw *BatchWorker) Stop() {
	// No cleanup needed
}

// parseConnectionKey splits a connection key into sourceID and destID.
// Returns empty strings if the key format is invalid.
func parseConnectionKey(connKey string) (sourceID, destID string) {
	parts := strings.Split(connKey, ":")
	if len(parts) != 2 {
		return "", ""
	}
	return parts[0], parts[1]
}
