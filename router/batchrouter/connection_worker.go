package batchrouter

import (
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-go-kit/stats"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	asynccommon "github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	"github.com/rudderlabs/rudder-server/utils/misc"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

// ConnectionWorker processes batches of jobs for a specific source-destination connection.
// It handles the actual upload of data to various destination types (object storage,
// warehouse, async destinations) and manages the job status updates.
type ConnectionWorker struct {
	sourceID string           // Source identifier
	destID   string           // Destination identifier
	jobs     []*ConnectionJob // Jobs to process for this connection
	brt      *Handle          // Reference to the batch router handle
}

// Work processes the current batch of jobs for this connection.
// It handles different destination types and manages the upload process,
// including error handling and status updates.
func (cw *ConnectionWorker) Work() bool {
	if len(cw.jobs) == 0 {
		return false
	}

	cw.brt.logger.Infof("Processing batch of %d jobs for connection %s:%s", len(cw.jobs), cw.sourceID, cw.destID)
	// Prepare the batch for processing
	batchedJobs, err := cw.prepareBatch()
	if err != nil {
		cw.brt.logger.Errorf("Failed to prepare batch: %v", err)
		return false
	}

	// Process the batch based on destination type
	uploadSuccess, uploadError := cw.processBatch(batchedJobs)

	// Update failing destinations status
	cw.updateFailingStatus(!uploadSuccess, uploadError)

	return true
}

// prepareBatch converts the worker's jobs into a BatchedJobs structure and validates
// the source and destination configuration.
func (cw *ConnectionWorker) prepareBatch() (*BatchedJobs, error) {
	// Convert jobs to BatchedJobs format
	jobsToUpload := make([]*jobsdb.JobT, 0, len(cw.jobs))
	for _, job := range cw.jobs {
		jobsToUpload = append(jobsToUpload, job.job)
	}

	// Get destination and source configuration
	cw.brt.configSubscriberMu.RLock()
	destWithSources, ok := cw.brt.destinationsMap[cw.destID]
	cw.brt.configSubscriberMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("destination %s not found in destinationsMap", cw.destID)
	}

	// Find matching source
	var source *backendconfig.SourceT
	for _, s := range destWithSources.Sources {
		if s.ID == cw.sourceID {
			source = &s
			break
		}
	}
	if source == nil {
		return nil, fmt.Errorf("source %s not found in destination %s", cw.sourceID, cw.destID)
	}

	return &BatchedJobs{
		Jobs: jobsToUpload,
		Connection: &Connection{
			Destination: destWithSources.Destination,
			Source:      *source,
		},
	}, nil
}

// processBatch handles the upload process based on the destination type.
// Returns upload success status and any error that occurred.
func (cw *ConnectionWorker) processBatch(batchedJobs *BatchedJobs) (bool, error) {
	defer cw.brt.limiter.upload.Begin(cw.destID)()

	destUploadStat := stats.Default.NewTaggedStat("batch_router_dest_upload_time", stats.TimerType, map[string]string{
		"sourceID": cw.sourceID,
		"destID":   cw.destID,
	})
	destUploadStart := time.Now()
	defer destUploadStat.Since(destUploadStart)

	switch {
	case IsObjectStorageDestination(cw.brt.destType):
		return cw.handleObjectStorageUpload(batchedJobs)
	case IsWarehouseDestination(cw.brt.destType):
		return cw.handleWarehouseUpload(batchedJobs)
	case asynccommon.IsAsyncDestination(cw.brt.destType):
		return cw.handleAsyncDestinationUpload(batchedJobs), nil
	default:
		err := fmt.Errorf("unsupported destination type %s for job buffer", cw.brt.destType)
		cw.brt.logger.Error(err)
		return false, err
	}
}

// handleObjectStorageUpload processes uploads for object storage destinations
func (cw *ConnectionWorker) handleObjectStorageUpload(batchedJobs *BatchedJobs) (bool, error) {
	output := cw.brt.upload(cw.brt.destType, batchedJobs, false)
	defer misc.RemoveFilePaths(output.LocalFilePaths...)

	cw.brt.recordDeliveryStatus(cw.destID, cw.sourceID, output, false)
	cw.brt.updateJobStatus(batchedJobs, false, output.Error, false)

	if output.JournalOpID > 0 {
		cw.brt.jobsDB.JournalDeleteEntry(output.JournalOpID)
	}

	if output.Error == nil {
		cw.brt.recordUploadStats(*batchedJobs.Connection, output)
		return true, nil
	}
	return false, output.Error
}

// handleWarehouseUpload processes uploads for warehouse destinations
func (cw *ConnectionWorker) handleWarehouseUpload(batchedJobs *BatchedJobs) (bool, error) {
	useRudderStorage := misc.IsConfiguredToUseRudderObjectStorage(batchedJobs.Connection.Destination.Config)
	objectStorageType := warehouseutils.ObjectStorageType(cw.brt.destType, batchedJobs.Connection.Destination.Config, useRudderStorage)

	splitBatchJobs := cw.brt.splitBatchJobsOnTimeWindow(*batchedJobs)
	var lastError error
	allSuccess := true

	for _, splitJob := range splitBatchJobs {
		output := cw.brt.upload(objectStorageType, splitJob, true)
		defer misc.RemoveFilePaths(output.LocalFilePaths...)

		notifyWarehouseErr := false
		if output.Error == nil && output.Key != "" {
			output.Error = cw.brt.pingWarehouse(splitJob, output)
			if output.Error != nil {
				notifyWarehouseErr = true
				allSuccess = false
				lastError = output.Error
			}
			warehouseutils.DestStat(stats.CountType, "generate_staging_files", cw.destID).Count(1)
			warehouseutils.DestStat(stats.CountType, "staging_file_batch_size", cw.destID).Count(len(splitJob.Jobs))
		} else if output.Error != nil {
			allSuccess = false
			lastError = output.Error
		}

		cw.brt.recordDeliveryStatus(cw.destID, cw.sourceID, output, true)
		cw.brt.updateJobStatus(splitJob, true, output.Error, notifyWarehouseErr)
	}

	return allSuccess, lastError
}

// handleAsyncDestinationUpload processes uploads for async destinations
func (cw *ConnectionWorker) handleAsyncDestinationUpload(batchedJobs *BatchedJobs) bool {
	cw.brt.sendJobsToStorage(*batchedJobs)
	return true
}

// updateFailingStatus updates the failing destinations map with the current status
func (cw *ConnectionWorker) updateFailingStatus(failed bool, err error) {
	cw.brt.failingDestinationsMu.Lock()
	cw.brt.failingDestinations[cw.destID] = failed
	cw.brt.failingDestinationsMu.Unlock()

	if failed {
		cw.brt.logger.Errorf("Upload failed for destination %s: %v", cw.destID, err)
	}
}

// SleepDurations returns the min and max sleep durations for the worker when idle.
// These values determine how long the worker should wait before checking for new work
// when there are no jobs to process.
func (cw *ConnectionWorker) SleepDurations() (min, max time.Duration) {
	return time.Millisecond * 100, time.Second * 5
}

// Stop cleans up any resources used by the worker.
// Currently a no-op as ConnectionWorker doesn't maintain any persistent resources.
func (cw *ConnectionWorker) Stop() {
	// No cleanup needed
}
