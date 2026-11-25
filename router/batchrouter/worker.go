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

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
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
		cb:        brt.newCircuitBreaker(partition, logger),
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

// newCircuitBreaker creates the circuit breaker that pauses a worker after repeated failures
func (brt *Handle) newCircuitBreaker(partition string, logger logger.Logger) circuitbreaker.CircuitBreaker {
	return circuitbreaker.NewCircuitBreaker(
		partition,
		circuitbreaker.WithMaxRequests(1),
		circuitbreaker.WithTimeout(brt.conf.GetDurationVar(30, time.Second, "BatchRouter.timeout", "BatchRouter."+brt.destType+".uploadFreq", "BatchRouter.uploadFreq")),
		circuitbreaker.WithConsecutiveFailures(brt.conf.GetIntVar(3, 1, "BatchRouter.maxConsecutiveFailures", "BatchRouter."+brt.destType+".maxConsecutiveFailures")),
		circuitbreaker.WithLogger(logger),
	)
}

// scheduleJobs aborts the jobs that must not be delivered, marks the rest as executing and sends them to the job buffer
func (w *worker) scheduleJobs(destinationJobs *DestinationJobs) {
	brt := w.brt
	destWithSources := destinationJobs.destWithSources
	jobs := brt.abortDrainedJobs(&destWithSources, destinationJobs.jobs)
	statusList := make([]*jobsdb.JobStatusT, 0, len(jobs))
	for _, job := range jobs {
		statusList = append(statusList, executingStatus(job))
	}
	if len(statusList) == 0 {
		return
	}
	err := misc.RetryWithNotify(context.Background(), brt.jobsDBCommandTimeout.Load(), brt.jobdDBMaxRetries.Load(), func(ctx context.Context) error {
		return brt.jobsDB.UpdateJobStatus(ctx, statusList)
	}, brt.sendRetryUpdateStats)
	if err != nil {
		panic(fmt.Errorf("updating %s job statuses: %w", brt.destType, err))
	}
	// Now that all statuses are updated, we can safely send jobs to channels
	for _, job := range jobs {
		w.pw.AddJob(job, gjson.GetBytes(job.Parameters, "source_id").String(), destWithSources.Destination.ID)
	}
}

// executingStatus returns the status that marks a job as taken by the batch router
func executingStatus(job *jobsdb.JobT) *jobsdb.JobStatusT {
	return &jobsdb.JobStatusT{
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
		CustomVal:     job.CustomVal,
	}
}

// abortDrainedJobs aborts the jobs of a destination that must not be delivered, either because their source is no longer
// connected to the destination or because the drainer says so, and returns the remaining jobs in their original order.
func (brt *Handle) abortDrainedJobs(destWithSources *routerutils.DestinationWithSources, jobs []*jobsdb.JobT) []*jobsdb.JobT {
	destinationID := destWithSources.Destination.ID
	var remaining []*jobsdb.JobT
	var drainList []*jobsdb.JobStatusT
	var drainJobList []*jobsdb.JobT
	var drainStats *routerutils.DrainStats
	jobIDConnectionDetailsMap := make(map[int64]jobsdb.ConnectionID)

	for _, job := range jobs {
		sourceID := gjson.GetBytes(job.Parameters, "source_id").String()
		sourceFound := slices.ContainsFunc(destWithSources.Sources, func(s backendconfig.SourceT) bool { return s.ID == sourceID })
		drain, reason := brt.drainer.Drain(
			job.CreatedAt,
			destinationID,
			gjson.GetBytes(job.Parameters, "source_job_run_id").String(),
		)
		if sourceFound && !drain {
			remaining = append(remaining, job)
			continue
		}
		if !sourceFound {
			reason = "source_not_found"
		}
		jobIDConnectionDetailsMap[job.JobID] = jobsdb.ConnectionID{
			SourceID:      sourceID,
			DestinationID: destinationID,
		}
		drainList = append(drainList, &jobsdb.JobStatusT{
			JobID:         job.JobID,
			AttemptNum:    job.LastJobStatus.AttemptNum + 1,
			JobState:      jobsdb.Aborted.State,
			ExecTime:      time.Now(),
			RetryTime:     time.Now(),
			ErrorCode:     routerutils.DRAIN_ERROR_CODE,
			ErrorResponse: routerutils.EnhanceJSON(job.LastJobStatus.ErrorResponse, "reason", reason),
			Parameters:    []byte(`{}`),
			JobParameters: job.Parameters,
			WorkspaceId:   job.WorkspaceId,
			PartitionID:   job.PartitionID,
			CustomVal:     job.CustomVal,
			DrainReason:   reason,
		})
		job.Parameters = routerutils.EnhanceJSON(job.Parameters, "stage", "batch_router")
		job.Parameters = routerutils.EnhanceJSON(job.Parameters, "reason", reason)
		drainJobList = append(drainJobList, job)
		if drainStats == nil {
			drainStats = &routerutils.DrainStats{Workspace: job.WorkspaceId}
		}
		drainStats.Count++
		if !slices.Contains(drainStats.Reasons, reason) {
			drainStats.Reasons = append(drainStats.Reasons, reason)
		}
	}
	if len(drainList) == 0 {
		return remaining
	}

	reportMetrics := brt.getReportMetrics(getReportMetricsParams{
		StatusList:    drainList,
		JobsList:      drainJobList,
		ParametersMap: brt.getParamertsFromJobs(drainJobList),
	})
	err := misc.RetryWithNotify(context.Background(), brt.jobsDBCommandTimeout.Load(), brt.jobdDBMaxRetries.Load(), func(ctx context.Context) error {
		return brt.jobsDB.WithUpdateSafeTx(ctx, func(tx jobsdb.UpdateSafeTx) error {
			err := brt.jobsDB.UpdateJobStatusInTx(ctx, tx, drainList)
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
	routerutils.UpdateProcessedEventsMetrics(stats.Default, module, brt.destType, drainList, jobIDConnectionDetailsMap)
	stats.Default.NewTaggedStat("drained_events", stats.CountType, stats.Tags{
		"destType":    brt.destType,
		"destId":      destinationID,
		"module":      "batchrouter",
		"reasons":     strings.Join(drainStats.Reasons, ", "),
		"workspaceId": drainStats.Workspace,
	}).Count(drainStats.Count)
	return remaining
}

// SleepDurations returns the min and max sleep durations for the worker when idle, i.e when [Work] returns false.
func (w *worker) SleepDurations() (min, max time.Duration) {
	return w.brt.minIdleSleep.Load(), w.brt.uploadFreq.Load() / 2
}

// Stop is no-op for this worker since the worker is not running any goroutine internally.
func (w *worker) Stop() {
	w.pw.Stop()
}
