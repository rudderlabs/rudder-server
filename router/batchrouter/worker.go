package batchrouter

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/sony/gobreaker"
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/jobsdb"
	routerutils "github.com/rudderlabs/rudder-server/router/utils"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

// newWorker creates a new worker for the provided partition.
func newWorker(partition string, logger logger.Logger, brt *Handle) *worker {
	w := &worker{
		partition: partition,
		logger:    logger,
		brt:       brt,
		pw:        NewPartitionWorker(logger, partition, brt),
		cb: gobreaker.NewCircuitBreaker(gobreaker.Settings{
			Name:    partition,
			Timeout: brt.conf.GetDuration("BatchRouter.timeout", 10, time.Second),
		}),
	}
	w.pw.Start()
	return w
}

type worker struct {
	partition string
	logger    logger.Logger
	brt       *Handle
	cb        *gobreaker.CircuitBreaker
	pw        *PartitionWorker
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

	for _, workerJob := range workerJobs {
		w.routeJobsToBuffer(workerJob)
	}
	return true
}

// routeJobsToBuffer sends jobs to appropriate channels in the job buffer
func (w *worker) routeJobsToBuffer(destinationJobs *DestinationJobs) {
	brt := w.brt
	destWithSources := destinationJobs.destWithSources
	parameterFilters := []jobsdb.ParameterFilterT{{Name: "destination_id", Value: destWithSources.Destination.ID}}
	var statusList []*jobsdb.JobStatusT
	var drainList []*jobsdb.JobStatusT
	var drainJobList []*jobsdb.JobT
	drainStatsbyDest := make(map[string]*routerutils.DrainStats)
	jobIDConnectionDetailsMap := make(map[int64]jobsdb.ConnectionDetails)

	// Organize jobs by destination and source
	type jobEntry struct {
		job      *jobsdb.JobT
		status   *jobsdb.JobStatusT
		sourceID string
		destID   string
	}

	var jobsToBuffer []jobEntry

	// Process jobs and check for drain conditions
	for _, job := range destinationJobs.jobs {
		sourceID := gjson.GetBytes(job.Parameters, "source_id").String()
		destinationID := destWithSources.Destination.ID
		jobIDConnectionDetailsMap[job.JobID] = jobsdb.ConnectionDetails{
			SourceID:      sourceID,
			DestinationID: destinationID,
		}
		sourceFound := false
		for _, s := range destWithSources.Sources {
			if s.ID == sourceID {
				sourceFound = true
				break
			}
		}
		if !sourceFound {
			status := jobsdb.JobStatusT{
				JobID:         job.JobID,
				AttemptNum:    job.LastJobStatus.AttemptNum + 1,
				JobState:      jobsdb.Aborted.State,
				ExecTime:      time.Now(),
				RetryTime:     time.Now(),
				ErrorCode:     routerutils.DRAIN_ERROR_CODE,
				ErrorResponse: routerutils.EnhanceJSON([]byte(`{}`), "reason", "source_not_found"),
				Parameters:    []byte(`{}`),
				JobParameters: job.Parameters,
				WorkspaceId:   job.WorkspaceId,
			}
			job.Parameters = routerutils.EnhanceJSON(job.Parameters, "stage", "batch_router")
			job.Parameters = routerutils.EnhanceJSON(job.Parameters, "reason", "source_not_found")
			drainList = append(drainList, &status)
			drainJobList = append(drainJobList, job)
			drainJobList = append(drainJobList, job)
			if _, ok := drainStatsbyDest[destinationID]; !ok {
				drainStatsbyDest[destinationID] = &routerutils.DrainStats{
					Count:     0,
					Reasons:   []string{},
					Workspace: job.WorkspaceId,
				}
			}
			drainStatsbyDest[destinationID].Count = drainStatsbyDest[destinationID].Count + 1
			if !slices.Contains(drainStatsbyDest[destinationID].Reasons, "source_not_found") {
				drainStatsbyDest[destinationID].Reasons = append(drainStatsbyDest[destinationID].Reasons, "source_not_found")
			}
			continue
		}

		if drain, reason := brt.drainer.Drain(
			job.CreatedAt,
			destWithSources.Destination.ID,
			gjson.GetBytes(job.Parameters, "source_job_run_id").String(),
		); drain {
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
			drainList = append(drainList, &status)
			drainJobList = append(drainJobList, job)
			if _, ok := drainStatsbyDest[destinationID]; !ok {
				drainStatsbyDest[destinationID] = &routerutils.DrainStats{
					Count:     0,
					Reasons:   []string{},
					Workspace: job.WorkspaceId,
				}
			}
			drainStatsbyDest[destinationID].Count = drainStatsbyDest[destinationID].Count + 1
			if !slices.Contains(drainStatsbyDest[destinationID].Reasons, reason) {
				drainStatsbyDest[destinationID].Reasons = append(drainStatsbyDest[destinationID].Reasons, reason)
			}
		} else {
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

			statusList = append(statusList, &status)
			jobsToBuffer = append(jobsToBuffer, jobEntry{
				job:      job,
				status:   &status,
				sourceID: sourceID,
				destID:   destinationID,
			})
		}
	}

	// Mark jobs as executing in a single batch operation
	if len(statusList) > 0 {
		w.brt.logger.Info("statusList", statusList)
		brt.logger.Debugf("BRT: %s: DB Status update complete for parameter Filters: %v", brt.destType, parameterFilters)
		err := misc.RetryWithNotify(context.Background(), brt.jobsDBCommandTimeout.Load(), brt.jobdDBMaxRetries.Load(), func(ctx context.Context) error {
			return brt.jobsDB.UpdateJobStatus(ctx, statusList, []string{brt.destType}, parameterFilters)
		}, brt.sendRetryUpdateStats)
		if err != nil {
			panic(fmt.Errorf("updating %s job statuses: %w", brt.destType, err))
		}
		brt.logger.Debugf("BRT: %s: DB Status update complete for parameter Filters: %v", brt.destType, parameterFilters)

		// Now that all statuses are updated, we can safely send jobs to channels
		for _, entry := range jobsToBuffer {
			w.pw.AddJob(entry.job, entry.sourceID, entry.destID)
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
				return brt.updateRudderSourcesStats(ctx, tx, drainJobList, drainList)
			})
		}, brt.sendRetryUpdateStats)
		if err != nil {
			panic(err)
		}
		routerutils.UpdateProcessedEventsMetrics(stats.Default, module, brt.destType, statusList, jobIDConnectionDetailsMap)
		for destID, destDrainStat := range drainStatsbyDest {
			stats.Default.NewTaggedStat("drained_events", stats.CountType, stats.Tags{
				"destType":    brt.destType,
				"destId":      destID,
				"module":      "batchrouter",
				"reasons":     strings.Join(destDrainStat.Reasons, ", "),
				"workspaceId": destDrainStat.Workspace,
			}).Count(destDrainStat.Count)
			w.brt.pendingEventsRegistry.DecreasePendingEvents("batch_rt", destDrainStat.Workspace, brt.destType, float64(destDrainStat.Count))
		}
	}
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
	w.pw.Stop()
}
