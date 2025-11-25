package processor

import (
	"context"
	"net/http"
	"strconv"
	"sync"

	"github.com/rudderlabs/rudder-server/utils/misc"

	kitctx "github.com/rudderlabs/rudder-go-kit/context"

	"github.com/samber/lo"
	"golang.org/x/sync/errgroup"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/stats"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
	"github.com/rudderlabs/rudder-server/utils/tracing"

	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/processor/types"
	reportingtypes "github.com/rudderlabs/rudder-server/utils/types"
)

type srcHydrationMessage struct {
	partition                     string
	subJobs                       subJob
	archivalJobs                  []*jobsdb.JobT
	eventSchemaJobsBySourceId     map[SourceIDT][]*jobsdb.JobT
	connectionDetailsMap          map[string]*reportingtypes.ConnectionDetails
	statusDetailsMap              map[string]map[string]*reportingtypes.StatusDetail
	enricherStatusDetailsMap      map[string]map[string]*reportingtypes.StatusDetail
	botManagementStatusDetailsMap map[string]map[string]*reportingtypes.StatusDetail
	eventBlockingStatusDetailsMap map[string]map[string]*reportingtypes.StatusDetail
	destFilterStatusDetailMap     map[string]map[string]*reportingtypes.StatusDetail
	reportMetrics                 []*reportingtypes.PUReportedMetric
	totalEvents                   int
	groupedEventsBySourceId       map[SourceIDT][]types.TransformerEvent
	eventsByMessageID             map[string]types.SingularEventWithReceivedAt
	jobIDToSpecificDestMapOnly    map[int64]string
	statusList                    []*jobsdb.JobStatusT
	jobList                       []*jobsdb.JobT
	sourceDupStats                map[dupStatKey]int
	dedupKeys                     map[string]struct{}
}

func (proc *Handle) srcHydrationStage(partition string, message *srcHydrationMessage) (*preTransformationMessage, error) {
	spanTags := stats.Tags{"partition": partition}
	ctx, processJobsSpan := proc.tracer.Trace(message.subJobs.ctx, "srcHydrationStage", tracing.WithTraceTags(spanTags))
	defer processJobsSpan.End()

	ctx, cancel := kitctx.MergedContext(ctx, proc.backgroundCtx)
	defer cancel()

	if proc.limiter.srcHydration != nil {
		defer proc.limiter.srcHydration.BeginWithPriority(partition, proc.getLimiterPriority(partition))()
		defer proc.stats.statSrcHydrationStageCount(partition).Count(
			lo.Sum(lo.MapToSlice(message.groupedEventsBySourceId, func(key SourceIDT, jobs []types.TransformerEvent) int {
				return len(jobs)
			})))
	}

	// Process sources in parallel using errgroup
	g, ctx := errgroup.WithContext(ctx)

	// Mutex to protect shared maps
	var sharedMapsMutex sync.Mutex

	for sourceId, jobs := range message.groupedEventsBySourceId {
		g.Go(func() error {
			var hydrationFailedReports *reportingtypes.PUReportedMetric
			source, err := proc.getSourceBySourceID(string(sourceId))
			if err != nil {
				return err
			}
			if !source.IsSourceHydrationSupported() {
				return nil
			}

			hydratedJobs, err := proc.hydrate(ctx, source, jobs)
			if err != nil {
				proc.logger.Errorn("failed to hydrate source events, skipping jobs",
					obskit.SourceID(string(sourceId)),
					obskit.Error(err),
				)
				// report metrics for failed hydration
				hydrationFailedReports = proc.getHydrationFailedReports(source, jobs, err)
			}

			// Update shared maps with mutex protection
			sharedMapsMutex.Lock()
			defer sharedMapsMutex.Unlock()

			// Append hydration failed reports if any
			if hydrationFailedReports != nil {
				message.reportMetrics = append(message.reportMetrics, hydrationFailedReports)
			}

			// If no hydrated jobs, remove entry from groupedEventsBySourceId
			if len(hydratedJobs) == 0 {
				delete(message.groupedEventsBySourceId, sourceId)
				return nil
			}

			// Update eventsByMessageID map
			for _, job := range hydratedJobs {
				msgID := job.Metadata.MessageID
				originalJob := message.eventsByMessageID[msgID]
				message.eventsByMessageID[msgID] = types.SingularEventWithReceivedAt{
					SingularEvent: job.Message,
					ReceivedAt:    originalJob.ReceivedAt,
				}
			}
			// Update the groupedEventsBySourceId map
			message.groupedEventsBySourceId[sourceId] = hydratedJobs

			// Update eventSchemaJobsBySourceId map if needed
			if len(message.eventSchemaJobsBySourceId[sourceId]) > 0 {
				eventSchemaJobsByJobID := lo.SliceToMap(message.eventSchemaJobsBySourceId[sourceId], func(job *jobsdb.JobT) (int64, *jobsdb.JobT) {
					return job.JobID, job
				})
				for i := range hydratedJobs {
					jobId := hydratedJobs[i].Metadata.JobID
					job, exists := eventSchemaJobsByJobID[jobId]
					if !exists {
						continue
					}
					payload, err := jsonrs.Marshal(hydratedJobs[i].Message)
					if err != nil {
						proc.logger.Errorn("failed to marshal hydrated event for event schema",
							obskit.SourceID(string(sourceId)),
							obskit.Error(err),
						)
					} else {
						job.EventPayload = payload
					}
				}
			}
			return nil
		})
	}

	// Wait for all goroutines to complete and check for errors
	if err := g.Wait(); err != nil {
		if proc.backgroundCtx.Err() != nil {
			return nil, types.ErrProcessorStopping
		}
		return nil, err
	}

	return &preTransformationMessage{
		partition:                     message.partition,
		subJobs:                       message.subJobs,
		archivalJobs:                  message.archivalJobs,
		eventSchemaJobsBySourceId:     message.eventSchemaJobsBySourceId,
		connectionDetailsMap:          message.connectionDetailsMap,
		statusDetailsMap:              message.statusDetailsMap,
		enricherStatusDetailsMap:      message.enricherStatusDetailsMap,
		botManagementStatusDetailsMap: message.botManagementStatusDetailsMap,
		eventBlockingStatusDetailsMap: message.eventBlockingStatusDetailsMap,
		destFilterStatusDetailMap:     message.destFilterStatusDetailMap,
		reportMetrics:                 message.reportMetrics,
		totalEvents:                   message.totalEvents,
		groupedEventsBySourceId:       message.groupedEventsBySourceId,
		eventsByMessageID:             message.eventsByMessageID,
		jobIDToSpecificDestMapOnly:    message.jobIDToSpecificDestMapOnly,
		statusList:                    message.statusList,
		jobList:                       message.jobList,
		sourceDupStats:                message.sourceDupStats,
		dedupKeys:                     message.dedupKeys,
	}, nil
}

func (proc *Handle) hydrate(ctx context.Context, source *backendconfig.SourceT, events []types.TransformerEvent) ([]types.TransformerEvent, error) {
	req := types.SrcHydrationRequest{
		Source: types.SrcHydrationSource{
			ID:               source.ID,
			Config:           source.Config,
			WorkspaceID:      source.WorkspaceID,
			SourceDefinition: source.SourceDefinition,
			InternalSecret:   source.InternalSecret,
		},
	}

	req.Batch = make([]types.SrcHydrationEvent, 0, len(events))
	jobIDToEventMap := make(map[string]types.TransformerEvent)

	for _, event := range events {
		jobID := strconv.FormatInt(event.Metadata.JobID, 10)
		req.Batch = append(req.Batch, types.SrcHydrationEvent{
			ID:    jobID,
			Event: event.Message,
		})
		jobIDToEventMap[jobID] = event
	}

	resp, err := proc.transformerClients.SrcHydration().Hydrate(ctx, req)
	if err != nil {
		return nil, err
	}
	return lo.Map(resp.Batch, func(event types.SrcHydrationEvent, _ int) types.TransformerEvent {
		originalEvent := jobIDToEventMap[event.ID]
		originalEvent.Message = event.Event
		return originalEvent
	}), nil
}

func (proc *Handle) getHydrationFailedReports(source *backendconfig.SourceT, jobs []types.TransformerEvent, err error) *reportingtypes.PUReportedMetric {
	sampleEvent, _ := jsonrs.Marshal(jobs[0].Message)
	eventName, _ := misc.MapLookup(jobs[0].Message, "event").(string)
	eventType, _ := misc.MapLookup(jobs[0].Message, "type").(string)
	return &reportingtypes.PUReportedMetric{
		ConnectionDetails: reportingtypes.ConnectionDetails{
			SourceID:           source.ID,
			SourceDefinitionID: source.SourceDefinition.ID,
			SourceCategory:     source.SourceDefinition.Category,
		},
		PUDetails: *reportingtypes.CreatePUDetails("", reportingtypes.SOURCE_HYDRATION, false, false),
		StatusDetail: &reportingtypes.StatusDetail{
			Status:         jobsdb.Failed.State,
			Count:          int64(len(jobs)),
			StatusCode:     http.StatusInternalServerError,
			SampleResponse: err.Error(),
			SampleEvent:    sampleEvent,
			EventName:      eventName,
			EventType:      eventType,
		},
	}
}
