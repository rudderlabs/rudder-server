package batchrouter

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	router_utils "github.com/rudderlabs/rudder-server/router/utils"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/rmetrics"
	"github.com/rudderlabs/rudder-server/utils/misc"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

// newWorker creates a new worker for the provided partition.
func newWorker(partition string, logger logger.Logger, brt *Handle) *worker {
	w := &worker{
		partition: partition,
		logger:    logger,
		brt:       brt,
	}
	return w
}

type worker struct {
	partition string
	logger    logger.Logger
	brt       *Handle
}

// Work retrieves jobs from batch router for the worker's partition and processes them,
// grouped by destination and in parallel.
// The function returns when processing completes and the return value is true if at least 1 job was processed,
// false otherwise.
func (w *worker) Work() bool {
	brt := w.brt
	workerJobs := brt.getWorkerJobs(w.partition)
	if len(workerJobs) == 0 {
		return false
	}
	brt.configSubscriberMu.RLock()
	destinationsMap := brt.destinationsMap
	brt.configSubscriberMu.RUnlock()
	var jobsWg sync.WaitGroup
	jobsWg.Add(len(workerJobs))
	for _, workerJob := range workerJobs {
		w.processJobAsync(&jobsWg, workerJob, destinationsMap)
	}
	jobsWg.Wait()
	return true
}

// processJobAsync spawns a goroutine and processes the destination's jobs. The provided wait group is notified when the goroutine completes.
func (w *worker) processJobAsync(jobsWg *sync.WaitGroup, destinationJobs *DestinationJobs, destinationsMap map[string]*router_utils.DestinationWithSources) {
	brt := w.brt
	rruntime.Go(func() {
		defer brt.limiter.process.Begin(w.partition)()
		defer jobsWg.Done()
		destWithSources := destinationJobs.destWithSources
		parameterFilters := []jobsdb.ParameterFilterT{{Name: "destination_id", Value: destWithSources.Destination.ID}}
		var statusList []*jobsdb.JobStatusT
		var drainList []*jobsdb.JobStatusT
		var drainJobList []*jobsdb.JobT
		drainStatsbyDest := make(map[string]*router_utils.DrainStats)

		jobsBySource := make(map[string][]*jobsdb.JobT)
		for _, job := range destinationJobs.jobs {
			var params router_utils.JobParameters
			_ = json.Unmarshal(job.Parameters, &params)
			if drain, reason := router_utils.ToBeDrained(
				job,
				params,
				router_utils.AbortConfigs{
					DestinationIDs: brt.toAbortDestinationIDs.Load(),
					JobRunIDs:      brt.toAbortJobRunIDs.Load(),
				},
				destinationsMap,
			); drain {
				status := jobsdb.JobStatusT{
					JobID:         job.JobID,
					AttemptNum:    job.LastJobStatus.AttemptNum + 1,
					JobState:      jobsdb.Aborted.State,
					ExecTime:      time.Now(),
					RetryTime:     time.Now(),
					ErrorCode:     router_utils.DRAIN_ERROR_CODE,
					ErrorResponse: router_utils.EnhanceJSON([]byte(`{}`), "reason", reason),
					Parameters:    []byte(`{}`), // check
					JobParameters: job.Parameters,
					WorkspaceId:   job.WorkspaceId,
				}
				// Enhancing job parameter with the drain reason.
				job.Parameters = router_utils.EnhanceJSON(job.Parameters, "stage", "batch_router")
				job.Parameters = router_utils.EnhanceJSON(job.Parameters, "reason", reason)
				drainList = append(drainList, &status)
				drainJobList = append(drainJobList, job)
				if _, ok := drainStatsbyDest[destWithSources.Destination.ID]; !ok {
					drainStatsbyDest[destWithSources.Destination.ID] = &router_utils.DrainStats{
						Count:     0,
						Reasons:   []string{},
						Workspace: job.WorkspaceId,
					}
				}
				drainStatsbyDest[destWithSources.Destination.ID].Count = drainStatsbyDest[destWithSources.Destination.ID].Count + 1
				if !slices.Contains(drainStatsbyDest[destWithSources.Destination.ID].Reasons, reason) {
					drainStatsbyDest[destWithSources.Destination.ID].Reasons = append(drainStatsbyDest[destWithSources.Destination.ID].Reasons, reason)
				}
			} else {
				sourceID := params.SourceID
				if _, ok := jobsBySource[sourceID]; !ok {
					jobsBySource[sourceID] = []*jobsdb.JobT{}
				}
				jobsBySource[sourceID] = append(jobsBySource[sourceID], job)

				status := jobsdb.JobStatusT{
					JobID:         job.JobID,
					AttemptNum:    job.LastJobStatus.AttemptNum + 1,
					JobState:      jobsdb.Executing.State,
					ExecTime:      time.Now(),
					RetryTime:     time.Now(),
					ErrorCode:     "",
					ErrorResponse: []byte(`{}`), // check
					Parameters:    []byte(`{}`), // check
					JobParameters: job.Parameters,
					WorkspaceId:   job.WorkspaceId,
				}
				statusList = append(statusList, &status)
			}
		}
		// Mark the drainList jobs as Aborted
		if len(drainList) > 0 {
			err := misc.RetryWithNotify(context.Background(), brt.jobsDBCommandTimeout.Load(), brt.jobdDBMaxRetries.Load(), func(ctx context.Context) error {
				return brt.errorDB.Store(ctx, drainJobList)
			}, brt.sendRetryStoreStats)
			if err != nil {
				panic(fmt.Errorf("storing %s jobs into ErrorDB: %w", brt.destType, err))
			}
			reportMetrics := brt.getReportMetrics(drainList, brt.getParamertsFromJobs(drainJobList))
			err = misc.RetryWithNotify(context.Background(), brt.jobsDBCommandTimeout.Load(), brt.jobdDBMaxRetries.Load(), func(ctx context.Context) error {
				return brt.jobsDB.WithUpdateSafeTx(ctx, func(tx jobsdb.UpdateSafeTx) error {
					err := brt.jobsDB.UpdateJobStatusInTx(ctx, tx, drainList, []string{brt.destType}, parameterFilters)
					if err != nil {
						return fmt.Errorf("marking %s job statuses as aborted: %w", brt.destType, err)
					}
					if brt.reporting != nil && brt.reportingEnabled {
						if err = brt.reporting.Report(reportMetrics, tx.Tx()); err != nil {
							return fmt.Errorf("reporting metrics: %w", err)
						}
					}
					// rsources stats
					return brt.updateRudderSourcesStats(ctx, tx, drainJobList, drainList)
				})
			}, brt.sendRetryUpdateStats)
			if err != nil {
				panic(err)
			}
			brt.updateProcessedEventsMetrics(statusList)
			for destID, destDrainStat := range drainStatsbyDest {
				stats.Default.NewTaggedStat("drained_events", stats.CountType, stats.Tags{
					"destType":    brt.destType,
					"destId":      destID,
					"module":      "batchrouter",
					"reasons":     strings.Join(destDrainStat.Reasons, ", "),
					"workspaceId": destDrainStat.Workspace,
				}).Count(destDrainStat.Count)
				rmetrics.DecreasePendingEvents(
					"batch_rt",
					destDrainStat.Workspace,
					brt.destType,
					float64(destDrainStat.Count),
				)
			}
		}
		// Mark the jobs as executing
		err := misc.RetryWithNotify(context.Background(), brt.jobsDBCommandTimeout.Load(), brt.jobdDBMaxRetries.Load(), func(ctx context.Context) error {
			return brt.jobsDB.UpdateJobStatus(ctx, statusList, []string{brt.destType}, parameterFilters)
		}, brt.sendRetryUpdateStats)
		if err != nil {
			panic(fmt.Errorf("storing %s jobs into ErrorDB: %w", brt.destType, err))
		}
		brt.logger.Debugf("BRT: %s: DB Status update complete for parameter Filters: %v", brt.destType, parameterFilters)

		var wg sync.WaitGroup
		wg.Add(len(jobsBySource))

		for sourceID, jobs := range jobsBySource {
			source, found := lo.Find(destWithSources.Sources, func(s backendconfig.SourceT) bool {
				return s.ID == sourceID
			})
			batchedJobs := BatchedJobs{
				Jobs: jobs,
				Connection: &Connection{
					Destination: destWithSources.Destination,
					Source:      source,
				},
			}
			if !found {
				// TODO: Should not happen. Handle this
				err := fmt.Errorf("BRT: Batch destination source not found in config for sourceID: %s", sourceID)
				brt.updateJobStatus(&batchedJobs, false, err, false)
				wg.Done()
				continue
			}
			rruntime.Go(func() {
				defer brt.limiter.upload.Begin(w.partition)()
				switch {
				case slices.Contains(objectStoreDestinations, brt.destType):
					destUploadStat := stats.Default.NewStat(fmt.Sprintf(`batch_router.%s_dest_upload_time`, brt.destType), stats.TimerType)
					destUploadStart := time.Now()
					output := brt.upload(brt.destType, &batchedJobs, false)
					brt.recordDeliveryStatus(*batchedJobs.Connection, output, false)
					brt.updateJobStatus(&batchedJobs, false, output.Error, false)
					misc.RemoveFilePaths(output.LocalFilePaths...)
					if output.JournalOpID > 0 {
						brt.jobsDB.JournalDeleteEntry(output.JournalOpID)
					}
					if output.Error == nil {
						brt.recordUploadStats(*batchedJobs.Connection, output)
					}

					destUploadStat.Since(destUploadStart)
				case slices.Contains(warehouseutils.WarehouseDestinations, brt.destType):
					useRudderStorage := misc.IsConfiguredToUseRudderObjectStorage(batchedJobs.Connection.Destination.Config)
					objectStorageType := warehouseutils.ObjectStorageType(brt.destType, batchedJobs.Connection.Destination.Config, useRudderStorage)
					destUploadStat := stats.Default.NewStat(fmt.Sprintf(`batch_router.%s_%s_dest_upload_time`, brt.destType, objectStorageType), stats.TimerType)
					destUploadStart := time.Now()
					splitBatchJobs := brt.splitBatchJobsOnTimeWindow(batchedJobs)
					for _, batchJob := range splitBatchJobs {
						output := brt.upload(objectStorageType, batchJob, true)
						notifyWarehouseErr := false
						if output.Error == nil && output.Key != "" {
							output.Error = brt.pingWarehouse(batchJob, output)
							if output.Error != nil {
								notifyWarehouseErr = true
							}
							warehouseutils.DestStat(stats.CountType, "generate_staging_files", batchJob.Connection.Destination.ID).Count(1)
							warehouseutils.DestStat(stats.CountType, "staging_file_batch_size", batchJob.Connection.Destination.ID).Count(len(batchJob.Jobs))
						}
						brt.recordDeliveryStatus(*batchJob.Connection, output, true)
						brt.updateJobStatus(batchJob, true, output.Error, notifyWarehouseErr)
						misc.RemoveFilePaths(output.LocalFilePaths...)
					}
					destUploadStat.Since(destUploadStart)
				case slices.Contains(asyncDestinations, brt.destType):
					destUploadStat := stats.Default.NewStat(fmt.Sprintf(`batch_router.%s_dest_upload_time`, brt.destType), stats.TimerType)
					destUploadStart := time.Now()
					brt.sendJobsToStorage(batchedJobs)
					destUploadStat.Since(destUploadStart)
				}
				wg.Done()
			})
		}
		wg.Wait()
	})
}

// SleepDurations returns the min and max sleep durations for the worker when idle, i.e when [Work] returns false.
func (w *worker) SleepDurations() (min, max time.Duration) {
	w.brt.lastExecTimesMu.Lock()
	defer w.brt.lastExecTimesMu.Unlock()
	if lastExecTime, ok := w.brt.lastExecTimes[w.partition]; ok {
		if nextAllowedTime := lastExecTime.Add(w.brt.uploadFreq.Load()); nextAllowedTime.After(time.Now()) {
			sleepTime := time.Until(nextAllowedTime)
			// sleep at least until the next upload frequency window opens
			return sleepTime, w.brt.uploadFreq.Load()
		}
	}
	return w.brt.minIdleSleep.Load(), w.brt.uploadFreq.Load() / 2
}

// Stop is no-op for this worker since the worker is not running any goroutine internally.
func (w *worker) Stop() {
	// no-op
}
