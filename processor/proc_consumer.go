package processor

import (
	"context"
	"fmt"
	"time"

	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/processor/types"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/tracing"
	reportingtypes "github.com/rudderlabs/rudder-server/utils/types"
	"github.com/rudderlabs/rudder-server/utils/workerpool"
)

// procJobPayload is the contract of a single intermediate (proc) job's EventPayload.
// Following decision §3, only IDs are persisted (carried inside Metadata) and the full
// Destination/Connection/Libraries/Credentials are re-hydrated from live backendConfig
// at consume time. The gw-pool write side that produces this payload is out of scope here.
type procJobPayload struct {
	Message  types.SingularEventT `json:"message"`
	Metadata types.Metadata       `json:"metadata"`

	// per-source pipeline steps already applied in gw pipeline (affects metric attribution)
	SrcHydration           bool `json:"srcHydration,omitempty"`
	TrackingPlanValidation bool `json:"trackingPlanValidation,omitempty"`
}

// startProcConsumer runs the proc pool: a second worker pool that polls
// the intermediate (proc) jobsdb per partition and drains each pickup through
// rebuild → userTransform → destinationTransform → store. It mirrors the main pinger
// loop ([Handle.Start]) and is a no-op when procDB is not configured
// (Processor.DestinationIsolation.enabled=false).
func (proc *Handle) startProcConsumer(ctx context.Context) error {
	if proc.procDB == nil {
		return nil
	}
	proc.logger.Infon("Starting proc consumer loop")
	proc.backendConfig.WaitForConfig(ctx)

	select {
	case <-ctx.Done():
		return nil
	case <-proc.config.asyncInit.Wait():
	}
	select {
	case <-ctx.Done():
		return nil
	case <-proc.transformerFeaturesService.Wait():
	}

	pool := workerpool.New(ctx, func(partition string) workerpool.Worker {
		return newProcPartitionWorker(partition, proc, proc.statsFactory.NewTracer("procPartitionWorker"), proc.statsFactory)
	}, proc.logger.Child("proc-consumer"))
	defer pool.Shutdown()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(proc.config.pingerSleep.Load()):
		}
		for _, partition := range proc.activeProcPartitions(ctx) {
			pool.PingWorker(partition)
		}
	}
}

// activeProcPartitions returns the distinct destination IDs with pending jobs in the
// proc jobsdb. Each destination ID is a partition for the proc pool, giving it
// per-destination isolation.
func (proc *Handle) activeProcPartitions(ctx context.Context) []string {
	defer proc.statsFactory.NewStat("proc_dest_active_partitions_time", stats.TimerType).RecordDuration()()
	keys, err := proc.procDB.GetDistinctParameterValues(ctx, jobsdb.DestinationID, "")
	if err != nil && ctx.Err() == nil {
		panic(err)
	}
	proc.statsFactory.NewStat("proc_dest_active_partitions", stats.GaugeType).Gauge(len(keys))
	return keys
}

// getProcJobs reads a batch of unprocessed jobs from the proc jobsdb for the given
// partition (destination ID). The partition IS the destination_id filter: each proc
// pool worker owns exactly one destination.
func (proc *Handle) getProcJobs(ctx context.Context, partition string) jobsdb.JobsResult {
	s := time.Now()
	_, span := proc.tracer.Trace(ctx, "getProcJobs", tracing.WithTraceTags(stats.Tags{"partition": partition}))
	defer span.End()

	if proc.limiter.pread != nil {
		defer proc.limiter.pread.BeginWithPriority(partition, proc.getLimiterPriority(partition))()
	}

	var (
		jobs jobsdb.JobsResult
		err  error
	)
	for query := true; query; {
		queryParams := jobsdb.GetQueryParams{
			ParameterFilters: []jobsdb.ParameterFilterT{{Name: "destination_id", Value: partition}},
			JobsLimit:        proc.config.maxEventsToProcess.Load(),
			EventsLimit:      proc.config.maxEventsToProcess.Load(),
			PayloadSizeLimit: proc.adaptiveLimit(proc.payloadLimit.Load()),
		}
		jobs, err = misc.QueryWithRetriesAndNotify(context.Background(), proc.jobdDBQueryRequestTimeout.Load(), proc.jobdDBMaxRetries.Load(), func(ctx context.Context) (jobsdb.JobsResult, error) {
			return proc.procDB.GetUnprocessed(ctx, queryParams)
		}, proc.sendQueryRetryStats)
		if err != nil {
			proc.logger.Errorn("Failed to get unprocessed jobs from proc DB", obskit.Error(err))
			panic(err)
		}
		query = len(jobs.Jobs) == 0 && jobs.DSLimitsReached
	}

	proc.statsFactory.NewTaggedStat("proc_consumer_db_read", stats.TimerType, stats.Tags{"partition": partition}).Since(s)
	proc.statsFactory.NewTaggedStat("proc_consumer_db_read_jobs", stats.CountType, stats.Tags{"partition": partition}).Count(len(jobs.Jobs))
	return jobs
}

// procMarkExecuting marks the given proc jobs as executing. It mirrors
// [Handle.markExecuting] but targets procDB.
func (proc *Handle) procMarkExecuting(ctx context.Context, partition string, jobs []*jobsdb.JobT) error {
	_, span := proc.tracer.Trace(ctx, "procMarkExecuting", tracing.WithTraceTags(stats.Tags{"partition": partition}))
	defer span.End()

	statusList := make([]*jobsdb.JobStatusT, len(jobs))
	for i, job := range jobs {
		statusList[i] = &jobsdb.JobStatusT{
			JobID:         job.JobID,
			AttemptNum:    job.LastJobStatus.AttemptNum,
			JobState:      jobsdb.Executing.State,
			ExecTime:      time.Now(),
			RetryTime:     time.Now(),
			ErrorResponse: []byte(`{}`),
			Parameters:    []byte(`{}`),
			JobParameters: job.Parameters,
			WorkspaceId:   job.WorkspaceId,
			PartitionID:   job.PartitionID,
			CustomVal:     job.CustomVal,
		}
	}
	err := misc.RetryWithNotify(context.Background(), proc.jobsDBCommandTimeout.Load(), proc.jobdDBMaxRetries.Load(), func(ctx context.Context) error {
		return proc.procDB.UpdateJobStatus(ctx, statusList)
	}, proc.sendRetryUpdateStats)
	if err != nil {
		return fmt.Errorf("marking proc jobs as executing: %w", err)
	}
	return nil
}

// procRebuildStage is proc pool's entry stage. It reconstructs the post-fan-out
// [transformationMessage] from persisted proc jobs: it deserializes each job, re-hydrates
// Destination/Connection/Libraries/Credentials from live backendConfig and re-runs the
// dest-filter + consent checks against that live config. Jobs whose destination is
// gone/disabled or filtered by consent are
// dropped to a terminal status. Surviving events are grouped per
// (source,destination) so the reused transform stages operate on them unchanged.
func (proc *Handle) procRebuildStage(partition string, in subJob) (*transformationMessage, error) {
	s := time.Now()
	defer func() {
		proc.statsFactory.NewTaggedStat("proc_consumer_rebuild_stage", stats.TimerType, stats.Tags{"partition": partition}).Since(s)
	}()

	groupedEvents := make(map[string][]types.TransformerEvent)
	uniqueMessageIdsBySrcDestKey := make(map[string]map[string]struct{})
	eventsByMessageID := make(map[string]types.SingularEventWithReceivedAt)
	srcPipelineSteps := make(sourceIDPipelineSteps)
	statusList := make([]*jobsdb.JobStatusT, 0, len(in.subJobs))
	var reportMetrics []*reportingtypes.PUReportedMetric
	var totalEvents int

	for _, job := range in.subJobs {
		var payload procJobPayload
		if err := jsonrs.Unmarshal(job.EventPayload, &payload); err != nil {
			proc.logger.Errorn("Unmarshalling proc job payload", obskit.Error(err), logger.NewIntField("jobId", job.JobID))
			statusList = append(statusList, procJobStatus(job, jobsdb.Aborted.State, fmt.Sprintf(`{"error":%q}`, err.Error())))
			continue
		}
		totalEvents++
		sourceID := gjson.GetBytes(job.Parameters, "source_id").String()
		destID := gjson.GetBytes(job.Parameters, "destination_id").String()

		// Re-hydrate the destination from live config; drop gracefully if it is gone/disabled.
		dest, ok := proc.getEnabledDestinationByID(sourceID, destID)
		if !ok {
			statusList = append(statusList, procJobStatus(job, jobsdb.Filtered.State, `{"reason":"destination not found or disabled"}`))
			continue
		}
		// Re-run consent against live config; drop if the destination is now consent-filtered.
		if len(proc.getConsentFilteredDestinations(payload.Message, sourceID, []backendconfig.DestinationT{dest})) == 0 {
			statusList = append(statusList, procJobStatus(job, jobsdb.Filtered.State, `{"reason":"filtered by consent"}`))
			continue
		}

		event := types.TransformerEvent{
			Message:     payload.Message,
			Metadata:    payload.Metadata,
			Destination: dest,
			Connection:  proc.getConnectionConfig(connection{sourceID: sourceID, destinationID: destID}),
			Libraries:   proc.getWorkspaceLibraries(payload.Metadata.WorkspaceID),
			Credentials: proc.config.credentialsMap[payload.Metadata.WorkspaceID],
		}
		// Refresh destination metadata from the (possibly drifted) live config.
		event.Metadata.DestinationID = dest.ID
		event.Metadata.DestinationName = dest.Name
		event.Metadata.DestinationType = dest.DestinationDefinition.Name
		event.Metadata.DestinationDefinitionID = dest.DestinationDefinition.ID
		if len(dest.Transformations) > 0 {
			event.Metadata.TransformationID = dest.Transformations[0].ID
			event.Metadata.TransformationVersionID = dest.Transformations[0].VersionID
		}
		filterConfig(&event)

		srcAndDestKey := getKeyFromSourceAndDest(sourceID, destID)
		groupedEvents[srcAndDestKey] = append(groupedEvents[srcAndDestKey], event)
		if _, ok := uniqueMessageIdsBySrcDestKey[srcAndDestKey]; !ok {
			uniqueMessageIdsBySrcDestKey[srcAndDestKey] = make(map[string]struct{})
		}
		uniqueMessageIdsBySrcDestKey[srcAndDestKey][payload.Metadata.MessageID] = struct{}{}

		receivedAt, _ := misc.GetParsedTimestamp(payload.Metadata.ReceivedAt)
		eventsByMessageID[payload.Metadata.MessageID] = types.SingularEventWithReceivedAt{
			SingularEvent: payload.Message,
			ReceivedAt:    receivedAt,
		}
		srcPipelineSteps[SourceIDT(sourceID)] = SourcePipelineSteps{
			srcHydration:           payload.SrcHydration,
			trackingPlanValidation: payload.TrackingPlanValidation,
		}
		statusList = append(statusList, procJobStatus(job, jobsdb.Succeeded.State, `{}`))
	}

	return &transformationMessage{
		ctx:                          in.ctx,
		groupedEvents:                groupedEvents,
		srcPipelineSteps:             srcPipelineSteps,
		eventsByMessageID:            eventsByMessageID,
		uniqueMessageIdsBySrcDestKey: uniqueMessageIdsBySrcDestKey,
		reportMetrics:                reportMetrics,
		statusList:                   statusList,
		sourceDupStats:               make(map[dupStatKey]int),
		dedupKeys:                    make(map[string]struct{}),
		totalEvents:                  totalEvents,
		hasMore:                      in.hasMore,
		rsourcesStats:                in.rsourcesStats,
		trackedUsersReports:          nil, // tracked users stay entirely in gw pool (§5)
	}, nil
}

// getEnabledDestinationByID returns the live, enabled destination for the given
// (source, destination) connection, or false when it no longer exists / is disabled.
func (proc *Handle) getEnabledDestinationByID(sourceID, destinationID string) (backendconfig.DestinationT, bool) {
	proc.config.configSubscriberLock.RLock()
	defer proc.config.configSubscriberLock.RUnlock()
	for i := range proc.config.sourceIdDestinationMap[sourceID] {
		dest := &proc.config.sourceIdDestinationMap[sourceID][i]
		if dest.ID == destinationID && dest.Enabled {
			return *dest, true
		}
	}
	return backendconfig.DestinationT{}, false
}

// procJobStatus builds a terminal job status for a proc job.
func procJobStatus(job *jobsdb.JobT, state, errorResponse string) *jobsdb.JobStatusT {
	return &jobsdb.JobStatusT{
		JobID:         job.JobID,
		JobState:      state,
		AttemptNum:    job.LastJobStatus.AttemptNum + 1,
		ExecTime:      time.Now(),
		RetryTime:     time.Now(),
		ErrorResponse: []byte(errorResponse),
		Parameters:    []byte(`{}`),
		JobParameters: job.Parameters,
		WorkspaceId:   job.WorkspaceId,
		PartitionID:   job.PartitionID,
		CustomVal:     job.CustomVal,
	}
}
