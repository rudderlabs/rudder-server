package batchrouter

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/samber/lo"
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	asynccommon "github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	routerutils "github.com/rudderlabs/rudder-server/router/utils"
	"github.com/rudderlabs/rudder-server/rruntime"
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
start:
	brt := w.brt
	workerJobs, limitsReached := brt.getWorkerJobs(w.partition)
	if len(workerJobs) == 0 {
		return false
	}
	var jobsWg sync.WaitGroup
	jobsWg.Add(len(workerJobs))
	for _, workerJob := range workerJobs {
		w.processJobAsync(&jobsWg, workerJob)
	}
	jobsWg.Wait()

	brt.logger.Infof("BRT: Worker %s processed %d jobs", w.partition, len(workerJobs[0].jobs))
	brt.logger.Infof("BRT: Worker %s limits reached: %v", w.partition, limitsReached)

	if limitsReached {
		goto start
	}

	return true
}

// processJobAsync spawns a goroutine and processes the destination's jobs. The provided wait group is notified when the goroutine completes.
func (w *worker) processJobAsync(jobsWg *sync.WaitGroup, destinationJobs *DestinationJobs) {
	brt := w.brt
	rruntime.Go(func() {
		defer brt.limiter.process.Begin("")()
		defer jobsWg.Done()
		destWithSources := destinationJobs.destWithSources
		parameterFilters := []jobsdb.ParameterFilterT{{Name: "destination_id", Value: destWithSources.Destination.ID}}
		start := time.Now()
		drainList, drainJobList, statusList, jobIDConnectionDetailsMap, jobsBySource := brt.getDrainList(destinationJobs, destWithSources)
		// Mark the drainList jobs as Aborted
		if len(drainList) > 0 {
			err := misc.RetryWithNotify(context.Background(), brt.jobsDBCommandTimeout.Load(), brt.jobdDBMaxRetries.Load(), func(ctx context.Context) error {
				return brt.errorDB.Store(ctx, drainJobList)
			}, brt.sendRetryStoreStats)
			if err != nil {
				panic(fmt.Errorf("storing %s jobs into ErrorDB: %w", brt.destType, err))
			}
			reportMetrics := brt.getReportMetrics(getReportMetricsParams{
				StatusList:    drainList,
				ParametersMap: brt.getParamertsFromJobs(drainJobList),
			})
			err = misc.RetryWithNotify(context.Background(), brt.jobsDBCommandTimeout.Load(), brt.jobdDBMaxRetries.Load(), func(ctx context.Context) error {
				return brt.jobsDB.WithUpdateSafeTx(ctx, func(tx jobsdb.UpdateSafeTx) error {
					err := brt.jobsDB.UpdateJobStatusInTx(ctx, tx, drainList, []string{brt.destType}, parameterFilters)
					if err != nil {
						return fmt.Errorf("marking %s job statuses as aborted: %w", brt.destType, err)
					}
					if brt.reporting != nil && brt.reportingEnabled {
						if err = brt.reporting.Report(ctx, reportMetrics, tx.Tx()); err != nil {
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
			routerutils.UpdateProcessedEventsMetrics(stats.Default, module, brt.destType, statusList, jobIDConnectionDetailsMap)
		}
		brt.stat.NewTaggedStat("batch_router.drain_time", stats.TimerType, stats.Tags{
			"destType": brt.destType,
		}).Since(start)
		// Mark the jobs as executing
		err := misc.RetryWithNotify(context.Background(), brt.jobsDBCommandTimeout.Load(), brt.jobdDBMaxRetries.Load(), func(ctx context.Context) error {
			return brt.jobsDB.UpdateJobStatus(ctx, statusList, []string{brt.destType}, parameterFilters)
		}, brt.sendRetryUpdateStats)
		if err != nil {
			panic(fmt.Errorf("storing %s jobs into ErrorDB: %w", brt.destType, err))
		}
		brt.logger.Debugf("BRT: %s: DB Status update complete for parameter Filters: %v", brt.destType, parameterFilters)

		brt.stat.NewTaggedStat("batch_router.mark_jobs_executing_time", stats.TimerType, stats.Tags{
			"destType": brt.destType,
		}).Since(start)

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
				defer brt.limiter.upload.Begin("")()
				switch {
				case IsObjectStorageDestination(brt.destType):
					destUploadStat := stats.Default.NewTaggedStat("batch_router.dest_upload_time", stats.TimerType, stats.Tags{
						"destType": brt.destType,
					})
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
				case IsWarehouseDestination(brt.destType):
					useRudderStorage := misc.IsConfiguredToUseRudderObjectStorage(batchedJobs.Connection.Destination.Config)
					brt.stat.NewTaggedStat("batch_router.use_rudder_storage", stats.TimerType, stats.Tags{
						"destType": brt.destType,
					}).Since(start)
					objectStorageType := warehouseutils.ObjectStorageType(brt.destType, batchedJobs.Connection.Destination.Config, useRudderStorage)
					brt.stat.NewTaggedStat("batch_router.get_object_storage_type", stats.TimerType, stats.Tags{
						"destType": brt.destType,
					}).Since(start)
					splitBatchJobs := brt.splitBatchJobsOnTimeWindow(batchedJobs)
					brt.stat.NewTaggedStat("batch_router.split_batch_jobs_on_time_window", stats.TimerType, stats.Tags{
						"destType": brt.destType,
					}).Since(start)
					for _, batchJob := range splitBatchJobs {
						output := brt.upload(objectStorageType, batchJob, true)
						brt.stat.NewTaggedStat("batch_router.upload_to_object_storage", stats.TimerType, stats.Tags{
							"destType": brt.destType,
						}).Since(start)
						notifyWarehouseErr := false
						if output.Error == nil && output.Key != "" {
							output.Error = brt.pingWarehouse(batchJob, output)
							if output.Error != nil {
								notifyWarehouseErr = true
							}
						}
						brt.stat.NewTaggedStat("batch_router.ping_warehouse", stats.TimerType, stats.Tags{
							"destType": brt.destType,
						}).Since(start)
						brt.recordDeliveryStatus(*batchJob.Connection, output, true)
						brt.stat.NewTaggedStat("batch_router.record_delivery_status", stats.TimerType, stats.Tags{
							"destType": brt.destType,
						}).Since(start)
						brt.updateJobStatus(batchJob, true, output.Error, notifyWarehouseErr)
						brt.stat.NewTaggedStat("batch_router.update_job_status", stats.TimerType, stats.Tags{
							"destType": brt.destType,
						}).Since(start)
						brt.stat.NewTaggedStat("batch_router.total_jobs_processed", stats.CountType, stats.Tags{
							"destType": brt.destType,
						}).Count(len(batchJob.Jobs))
						misc.RemoveFilePaths(output.LocalFilePaths...)
					}
				case asynccommon.IsAsyncDestination(brt.destType):
					destUploadStat := stats.Default.NewTaggedStat("batch_router.dest_upload_time", stats.TimerType, stats.Tags{
						"destType": brt.destType,
					})
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

type drainResult struct {
	drainList                 []*jobsdb.JobStatusT
	drainJobList              []*jobsdb.JobT
	statusList                []*jobsdb.JobStatusT
	jobIDConnectionDetailsMap map[int64]jobsdb.ConnectionDetails
	jobsBySource              map[string][]*jobsdb.JobT
}

func (brt *Handle) getDrainList(destinationJobs *DestinationJobs, destWithSources routerutils.DestinationWithSources) ([]*jobsdb.JobStatusT, []*jobsdb.JobT, []*jobsdb.JobStatusT, map[int64]jobsdb.ConnectionDetails, map[string][]*jobsdb.JobT) {
	jobs := destinationJobs.jobs
	totalJobs := len(jobs)
	numWorkers := 8

	// Calculate chunk size
	chunkSize := (totalJobs + numWorkers - 1) / numWorkers

	// Create result channels with exact sizes
	results := make([]drainResult, numWorkers)
	var wg sync.WaitGroup

	// Process chunks in parallel with no locks
	for i := 0; i < numWorkers; i++ {
		start := i * chunkSize
		if start >= totalJobs {
			continue
		}
		end := start + chunkSize
		if end > totalJobs {
			end = totalJobs
		}

		wg.Add(1)
		go func(chunk []*jobsdb.JobT, resultIdx int) {
			defer wg.Done()

			// Pre-allocate local containers
			chunkLen := len(chunk)
			result := drainResult{
				drainList:                 make([]*jobsdb.JobStatusT, 0, chunkLen),
				drainJobList:              make([]*jobsdb.JobT, 0, chunkLen),
				statusList:                make([]*jobsdb.JobStatusT, 0, chunkLen),
				jobIDConnectionDetailsMap: make(map[int64]jobsdb.ConnectionDetails, chunkLen),
				jobsBySource:              make(map[string][]*jobsdb.JobT),
			}

			for _, job := range chunk {
				sourceID := gjson.GetBytes(job.Parameters, "source_id").String()
				sourceJobRunID := gjson.GetBytes(job.Parameters, "source_job_run_id").String()
				destinationID := destWithSources.Destination.ID
				result.jobIDConnectionDetailsMap[job.JobID] = jobsdb.ConnectionDetails{
					SourceID:      sourceID,
					DestinationID: destinationID,
				}

				if drain, reason := brt.drainer.Drain(destinationID, sourceJobRunID, job.CreatedAt); drain {
					status := jobsdb.JobStatusT{
						JobID:         job.JobID,
						AttemptNum:    job.LastJobStatus.AttemptNum + 1,
						JobState:      jobsdb.Aborted.State,
						ExecTime:      time.Now(),
						RetryTime:     time.Now(),
						ErrorCode:     routerutils.DRAIN_ERROR_CODE,
						ErrorResponse: routerutils.EnhanceJSON([]byte(`{}`), "reason", reason),
						Parameters:    []byte(`{}`),
						JobParameters: job.Parameters,
						WorkspaceId:   job.WorkspaceId,
					}
					job.Parameters = routerutils.EnhanceJSON(job.Parameters, "stage", "batch_router")
					job.Parameters = routerutils.EnhanceJSON(job.Parameters, "reason", reason)
					result.drainList = append(result.drainList, &status)
					result.drainJobList = append(result.drainJobList, job)
				} else {
					if _, ok := result.jobsBySource[sourceID]; !ok {
						result.jobsBySource[sourceID] = make([]*jobsdb.JobT, 0, chunkLen)
					}
					result.jobsBySource[sourceID] = append(result.jobsBySource[sourceID], job)

					status := jobsdb.JobStatusT{
						JobID:         job.JobID,
						AttemptNum:    job.LastJobStatus.AttemptNum + 1,
						JobState:      jobsdb.Executing.State,
						ExecTime:      time.Now(),
						RetryTime:     time.Now(),
						ErrorCode:     "",
						ErrorResponse: []byte(`{}`),
						Parameters:    []byte(`{}`),
						JobParameters: job.Parameters,
						WorkspaceId:   job.WorkspaceId,
					}
					result.statusList = append(result.statusList, &status)
				}
			}

			results[resultIdx] = result
		}(jobs[start:end], i)
	}

	wg.Wait()

	// Calculate final sizes for pre-allocation
	totalDrainListSize := 0
	totalDrainJobListSize := 0
	totalStatusListSize := 0
	for i := 0; i < numWorkers; i++ {
		totalDrainListSize += len(results[i].drainList)
		totalDrainJobListSize += len(results[i].drainJobList)
		totalStatusListSize += len(results[i].statusList)
	}

	// Pre-allocate final slices with exact sizes
	drainList := make([]*jobsdb.JobStatusT, 0, totalDrainListSize)
	drainJobList := make([]*jobsdb.JobT, 0, totalDrainJobListSize)
	statusList := make([]*jobsdb.JobStatusT, 0, totalStatusListSize)
	jobIDConnectionDetails := make(map[int64]jobsdb.ConnectionDetails, totalJobs)
	jobsBySources := make(map[string][]*jobsdb.JobT)

	// Merge results without locks - we own all the data at this point
	for i := 0; i < numWorkers; i++ {
		result := results[i]
		drainList = append(drainList, result.drainList...)
		drainJobList = append(drainJobList, result.drainJobList...)
		statusList = append(statusList, result.statusList...)

		// Merge maps
		for k, v := range result.jobIDConnectionDetailsMap {
			jobIDConnectionDetails[k] = v
		}

		for sourceID, jobs := range result.jobsBySource {
			if existing, ok := jobsBySources[sourceID]; ok {
				jobsBySources[sourceID] = append(existing, jobs...)
			} else {
				jobsBySources[sourceID] = jobs
			}
		}
	}

	return drainList, drainJobList, statusList, jobIDConnectionDetails, jobsBySources
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
