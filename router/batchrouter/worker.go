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

// newWorker creates a new batch router worker
func newWorker(partition string, logger logger.Logger, brt *Handle) *worker {
	w := &worker{
		partition: partition,
		logger:    logger,
		brt:       brt,
	}
	w.cb = w.createCircuitBreaker()
	return w
}

// worker represents a batch router worker that processes jobs for a specific partition
type worker struct {
	partition string
	logger    logger.Logger
	brt       *Handle
	cb        *gobreaker.CircuitBreaker
}

// createCircuitBreaker creates a new circuit breaker for this worker
func (w *worker) createCircuitBreaker() *gobreaker.CircuitBreaker {
	maxFailures := w.brt.conf.GetIntVar(3, 1, "BatchRouter."+w.brt.destType+".circuitBreaker.maxFailures", "BatchRouter.circuitBreaker.maxFailures")
	interval := w.brt.conf.GetDurationVar(1, time.Minute, "BatchRouter."+w.brt.destType+".circuitBreaker.interval", "BatchRouter.circuitBreaker.interval")
	timeout := w.brt.conf.GetDurationVar(5, time.Minute, "BatchRouter."+w.brt.destType+".circuitBreaker.timeout", "BatchRouter.circuitBreaker.timeout")

	cbSettings := gobreaker.Settings{
		Name:        w.partition,
		MaxRequests: uint32(maxFailures),
		Interval:    interval,
		Timeout:     timeout,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			failureRatio := float64(counts.TotalFailures) / float64(counts.Requests)
			return counts.Requests >= uint32(maxFailures) && failureRatio >= 0.5
		},
		OnStateChange: func(name string, from, to gobreaker.State) {
			if from != to {
				w.logger.Infof("Circuit breaker %s state change from %s to %s", name, from, to)

				// Update failing destinations map based on circuit breaker state
				w.brt.failingDestinationsMu.Lock()
				w.brt.failingDestinations[w.partition] = to == gobreaker.StateOpen
				w.brt.failingDestinationsMu.Unlock()

				// Emit metrics for circuit breaker state changes
				statTags := stats.Tags{
					"destType": w.brt.destType,
					"key":      name,
					"state":    to.String(),
				}
				stats.Default.NewTaggedStat("batch_router_circuit_breaker_state", stats.GaugeType, statTags).Gauge(1)
			}
		},
	}

	return gobreaker.NewCircuitBreaker(cbSettings)
}

// Work retrieves jobs from batch router for the worker's partition and processes them,
// grouped by destination and in parallel.
// The function returns when processing completes and the return value is true if at least 1 job was processed,
// false otherwise.
func (w *worker) Work() bool {
	// Check if circuit breaker allows operations
	if w.cb.State() == gobreaker.StateOpen {
		// Circuit breaker is open, skip processing
		w.logger.Debugf("Circuit breaker is open for partition %s, skipping job processing", w.partition)
		return false
	}

	// Execute work through circuit breaker
	result, err := w.cb.Execute(func() (interface{}, error) {
		workerJobs := w.brt.getWorkerJobs(w.partition)
		if len(workerJobs) == 0 {
			return false, nil
		}

		// Record worker activity - number of destination jobs being processed
		stats.Default.NewTaggedStat("batch_router_worker_jobs", stats.GaugeType, stats.Tags{
			"partition": w.partition,
			"destType":  w.brt.destType,
		}).Gauge(len(workerJobs))

		for _, workerJob := range workerJobs {
			w.routeJobsToBuffer(workerJob)
		}
		return true, nil
	})
	if err != nil {
		w.logger.Errorf("Circuit breaker prevented processing for partition %s: %v", w.partition, err)
		return false
	}

	return result.(bool)
}

// routeJobsToBuffer sends jobs to appropriate channels in the job buffer.
// This is the entry point for jobs into the buffering system:
// 1. Jobs are checked for drain conditions (e.g., source not found, expired)
// 2. Jobs passing validation are marked as executing in JobsDB
// 3. Valid jobs are sent to JobBuffer.AddJob which:
//   - Ensures a consumer worker exists for the source-destination pair
//   - Adds the job to a channel specific to that source-destination pair
//     4. The ConsumerWorker then picks up jobs from the channel and processes them
//     in batches based on batch size and time thresholds
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

		// Normal job processing path - check custom drainer conditions first
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
		brt.logger.Debugf("BRT: %s: DB Status update complete for parameter Filters: %v", brt.destType, parameterFilters)
		err := misc.RetryWithNotify(context.Background(), brt.jobsDBCommandTimeout.Load(), brt.jobdDBMaxRetries.Load(), func(ctx context.Context) error {
			return brt.jobsDB.UpdateJobStatus(ctx, statusList, []string{brt.destType}, parameterFilters)
		}, brt.sendRetryUpdateStats)
		if err != nil {
			panic(fmt.Errorf("updating %s job statuses: %w", brt.destType, err))
		}
		brt.logger.Debugf("BRT: %s: DB Status update complete for parameter Filters: %v", brt.destType, parameterFilters)

		// Now that all statuses are updated, we can safely send jobs to buffer
		for _, entry := range jobsToBuffer {
			// Use the AddJob method which handles circuit breaker integration
			brt.jobBuffer.AddJob(entry.sourceID, entry.destID, entry.job)
		}
	}

	// Mark the drainList jobs as Aborted
	if len(drainList) > 0 {
		w.brt.logger.Info("drainList", drainList)
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
}
