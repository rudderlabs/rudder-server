package batchrouter

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/router/batchrouter/circuitbreaker"
	routerutils "github.com/rudderlabs/rudder-server/router/utils"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

// newWorker creates a new worker for the provided partition.
func newWorker(partition string, logger logger.Logger, brt *Handle) *worker {
	w := &worker{
		partition: partition,
		logger:    logger,
		brt:       brt,
		cb: circuitbreaker.NewCircuitBreaker(
			partition,
			circuitbreaker.WithMaxRequests(1),
			circuitbreaker.WithTimeout(brt.conf.GetDurationVar(30, time.Second, "BatchRouter.timeout", "BatchRouter."+brt.destType+".uploadFreq", "BatchRouter.uploadFreq")),
			circuitbreaker.WithConsecutiveFailures(brt.conf.GetIntVar(3, 1, "BatchRouter.maxConsecutiveFailures", "BatchRouter."+brt.destType+".maxConsecutiveFailures")),
			circuitbreaker.WithLogger(logger),
		),
	}
	w.pw = NewPartitionWorker(logger, partition, brt, w.cb)
	w.pw.Start()
	return w
}

type worker struct {
	partition string
	logger    logger.Logger
	brt       *Handle
	cb        circuitbreaker.CircuitBreaker
	pw        *PartitionWorker
}

// Work retrieves jobs from batch router for the worker's partition and processes them,
// grouped by destination and in parallel.
// The function returns when processing completes and the return value is true if at least 1 job was processed,
// false otherwise.
func (w *worker) Work() bool {
	// Check if circuit breaker is open before doing any work
	if w.cb.IsOpen() {
		w.logger.Warnn("Circuit breaker is open, skipping job processing",
			logger.NewStringField("partition", w.partition))
		return false
	}
	scheduleTimeStat := stats.Default.NewTaggedStat("batchrouter_worker_schedule_jobs_time", stats.TimerType, stats.Tags{"partition": w.partition})
	brt := w.brt
	workerJobs := brt.getWorkerJobs(w.partition)
	if len(workerJobs) == 0 {
		return false
	}

	for _, workerJob := range workerJobs {
		startTime := time.Now()
		w.scheduleJobs(workerJob)
		scheduleTimeStat.SendTiming(time.Since(startTime))
	}
	return true
}

// routeJobsToBuffer sends jobs to appropriate channels in the job buffer
func (w *worker) scheduleJobs(destinationJobs *DestinationJobs) {
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

		// Check if source exists for the destination
		sourceFound := false
		for _, s := range destWithSources.Sources {
			if s.ID == sourceID {
				sourceFound = true
				break
			}
		}

		// Check standard drain conditions
		drain, reason := brt.drainer.Drain(
			job.CreatedAt,
			destWithSources.Destination.ID,
			gjson.GetBytes(job.Parameters, "source_job_run_id").String(),
		)

		// Consolidate drain checks: either source not found OR drainer says so
		if !sourceFound || drain {
			finalReason := reason
			if !sourceFound {
				finalReason = "source_not_found"
			}

			status := jobsdb.JobStatusT{
				JobID:         job.JobID,
				AttemptNum:    job.LastJobStatus.AttemptNum + 1,
				JobState:      jobsdb.Aborted.State,
				ExecTime:      time.Now(),
				RetryTime:     time.Now(),
				ErrorCode:     routerutils.DRAIN_ERROR_CODE,
				ErrorResponse: routerutils.EnhanceJSON([]byte(`{}`), "reason", finalReason),
				Parameters:    []byte(`{}`),
				JobParameters: job.Parameters,
				WorkspaceId:   job.WorkspaceId,
				PartitionID:   job.PartitionID,
			}
			job.Parameters = routerutils.EnhanceJSON(job.Parameters, "stage", "batch_router")
			job.Parameters = routerutils.EnhanceJSON(job.Parameters, "reason", finalReason)
			drainList = append(drainList, &status)
			drainJobList = append(drainJobList, job)

			if _, ok := drainStatsbyDest[destinationID]; !ok {
				drainStatsbyDest[destinationID] = &routerutils.DrainStats{
					Count:     0,
					Reasons:   []string{},
					Workspace: job.WorkspaceId,
				}
			}
			drainStatsbyDest[destinationID].Count++
			if !slices.Contains(drainStatsbyDest[destinationID].Reasons, finalReason) {
				drainStatsbyDest[destinationID].Reasons = append(drainStatsbyDest[destinationID].Reasons, finalReason)
			}
		} else {
			// Job is not drained, prepare it for buffering
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
				PartitionID:   job.PartitionID,
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

	// Mark the drainList jobs as Aborted
	if len(drainList) > 0 {
		reportMetrics := brt.getReportMetrics(getReportMetricsParams{
			StatusList:    drainList,
			ParametersMap: brt.getParamertsFromJobs(drainJobList),
		})
		err := misc.RetryWithNotify(context.Background(), brt.jobsDBCommandTimeout.Load(), brt.jobdDBMaxRetries.Load(), func(ctx context.Context) error {
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

	// Mark jobs as executing in a single batch operation
	if len(statusList) > 0 {
		brt.logger.Debugn("BRT: DB Status update complete for parameter Filters", obskit.DestinationType(brt.destType), logger.NewIntField("parameterFiltersCount", int64(len(parameterFilters))))
		err := misc.RetryWithNotify(context.Background(), brt.jobsDBCommandTimeout.Load(), brt.jobdDBMaxRetries.Load(), func(ctx context.Context) error {
			return brt.jobsDB.UpdateJobStatus(ctx, statusList, []string{brt.destType}, parameterFilters)
		}, brt.sendRetryUpdateStats)
		if err != nil {
			panic(fmt.Errorf("updating %s job statuses: %w", brt.destType, err))
		}
		brt.logger.Debugn("BRT: DB Status update complete for parameter Filters", obskit.DestinationType(brt.destType), logger.NewIntField("parameterFiltersCount", int64(len(parameterFilters))))

		// Now that all statuses are updated, we can safely send jobs to channels
		for _, entry := range jobsToBuffer {
			w.pw.AddJob(entry.job, entry.sourceID, entry.destID)
		}
	}
}

// SleepDurations returns the min and max sleep durations for the worker when idle, i.e when [Work] returns false.
func (w *worker) SleepDurations() (min, max time.Duration) {
	return w.brt.minIdleSleep.Load(), w.brt.uploadFreq.Load() / 2
}

// Stop is no-op for this worker since the worker is not running any goroutine internally.
func (w *worker) Stop() {
	w.pw.Stop()
}
