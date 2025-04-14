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

// ConnectionWorker implements the workerpool.Worker interface for processing connection-level batches
type ConnectionWorker struct {
	sourceID  string
	destID    string
	jobs      []*ConnectionJob
	jobBuffer *JobBuffer
}

// Work processes jobs for a specific connection
func (cw *ConnectionWorker) Work() bool {
	if len(cw.jobs) == 0 {
		return false
	}

	// Use the existing upload logic
	uploadSuccess := true
	var uploadError error

	// Define a function to update the failing destinations map based on the result
	updateFailingStatus := func(failed bool, err error) {
		cw.jobBuffer.brt.failingDestinationsMu.Lock()
		cw.jobBuffer.brt.failingDestinations[cw.destID] = failed
		cw.jobBuffer.brt.failingDestinationsMu.Unlock()

		if failed {
			cw.jobBuffer.brt.logger.Errorf("Upload failed for destination %s: %v", cw.destID, err)
		}
	}

	// Execute the upload inside a defer context to always update status
	defer func() {
		updateFailingStatus(!uploadSuccess, uploadError)
	}()

	defer cw.jobBuffer.brt.limiter.upload.Begin(cw.destID)()

	// Convert jobs to BatchedJobs
	jobsToUpload := make([]*jobsdb.JobT, 0, len(cw.jobs))
	for _, job := range cw.jobs {
		jobsToUpload = append(jobsToUpload, job.job)
	}

	// Get destination and source from destinationsMap
	cw.jobBuffer.brt.configSubscriberMu.RLock()
	destWithSources, ok := cw.jobBuffer.brt.destinationsMap[cw.destID]
	cw.jobBuffer.brt.configSubscriberMu.RUnlock()
	if !ok {
		cw.jobBuffer.brt.logger.Errorf("Destination %s not found in destinationsMap", cw.destID)
		return false
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
		cw.jobBuffer.brt.logger.Errorf("Source %s not found in destination %s", cw.sourceID, cw.destID)
		return false
	}

	batchedJobs := BatchedJobs{
		Jobs: jobsToUpload,
		Connection: &Connection{
			Destination: destWithSources.Destination,
			Source:      *source,
		},
	}

	// Process based on destination type
	switch {
	case IsObjectStorageDestination(cw.jobBuffer.brt.destType):
		destUploadStat := cw.jobBuffer.createStat("batch_router_dest_upload_time", stats.TimerType, cw.sourceID, cw.destID)
		destUploadStart := time.Now()
		output := cw.jobBuffer.brt.upload(cw.jobBuffer.brt.destType, &batchedJobs, false)
		cw.jobBuffer.brt.recordDeliveryStatus(cw.destID, cw.sourceID, output, false)
		cw.jobBuffer.brt.updateJobStatus(&batchedJobs, false, output.Error, false)
		misc.RemoveFilePaths(output.LocalFilePaths...)
		if output.JournalOpID > 0 {
			cw.jobBuffer.brt.jobsDB.JournalDeleteEntry(output.JournalOpID)
		}
		if output.Error == nil {
			cw.jobBuffer.brt.recordUploadStats(*batchedJobs.Connection, output)
		} else {
			uploadSuccess = false
			uploadError = output.Error
		}
		destUploadStat.Since(destUploadStart)

	case IsWarehouseDestination(cw.jobBuffer.brt.destType):
		useRudderStorage := misc.IsConfiguredToUseRudderObjectStorage(batchedJobs.Connection.Destination.Config)
		objectStorageType := warehouseutils.ObjectStorageType(cw.jobBuffer.brt.destType, batchedJobs.Connection.Destination.Config, useRudderStorage)
		destUploadStat := cw.jobBuffer.createStat("batch_router_dest_upload_time", stats.TimerType, cw.sourceID, cw.destID)
		destUploadStart := time.Now()
		splitBatchJobs := cw.jobBuffer.brt.splitBatchJobsOnTimeWindow(batchedJobs)
		allSuccess := true
		for _, splitJob := range splitBatchJobs {
			output := cw.jobBuffer.brt.upload(objectStorageType, splitJob, true)
			notifyWarehouseErr := false
			if output.Error == nil && output.Key != "" {
				output.Error = cw.jobBuffer.brt.pingWarehouse(splitJob, output)
				if output.Error != nil {
					notifyWarehouseErr = true
					allSuccess = false
					uploadError = output.Error
				}
				warehouseutils.DestStat(stats.CountType, "generate_staging_files", cw.destID).Count(1)
				warehouseutils.DestStat(stats.CountType, "staging_file_batch_size", cw.destID).Count(len(splitJob.Jobs))
			} else if output.Error != nil {
				allSuccess = false
				uploadError = output.Error
			}
			cw.jobBuffer.brt.recordDeliveryStatus(cw.destID, cw.sourceID, output, true)
			cw.jobBuffer.brt.updateJobStatus(splitJob, true, output.Error, notifyWarehouseErr)
			misc.RemoveFilePaths(output.LocalFilePaths...)
		}
		uploadSuccess = allSuccess
		destUploadStat.Since(destUploadStart)

	case asynccommon.IsAsyncDestination(cw.jobBuffer.brt.destType):
		destUploadStat := cw.jobBuffer.createStat("batch_router_dest_upload_time", stats.TimerType, cw.sourceID, cw.destID)
		destUploadStart := time.Now()
		cw.jobBuffer.brt.sendJobsToStorage(batchedJobs)
		destUploadStat.Since(destUploadStart)

	default:
		errMsg := fmt.Errorf("unsupported destination type %s for job buffer", cw.jobBuffer.brt.destType)
		cw.jobBuffer.brt.logger.Errorf("Unsupported destination type %s for job buffer: %v", cw.jobBuffer.brt.destType, errMsg)
		uploadSuccess = false
		uploadError = errMsg
	}

	return true
}

// SleepDurations returns the min and max sleep durations for the worker when idle
func (cw *ConnectionWorker) SleepDurations() (min, max time.Duration) {
	return time.Millisecond * 100, time.Second * 5
}

// Stop is a no-op since this worker doesn't have internal goroutines
func (cw *ConnectionWorker) Stop() {
	// No-op
}
