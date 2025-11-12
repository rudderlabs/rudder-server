package processor

import (
	"context"
	"strconv"

	"github.com/samber/lo"

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

	if proc.limiter.srcHydration != nil {
		defer proc.limiter.srcHydration.BeginWithPriority(partition, proc.getLimiterPriority(partition))()
		defer proc.stats.statSrcHydrationStageCount(partition).Count(
			lo.Sum(lo.MapToSlice(message.groupedEventsBySourceId, func(key SourceIDT, jobs []types.TransformerEvent) int {
				return len(jobs)
			})))
	}

	for sourceId, jobs := range message.groupedEventsBySourceId {
		source, err := proc.getSourceBySourceID(string(sourceId))
		if err != nil {
			return nil, err
		}
		if !source.IsSourceHydrationSupported() {
			continue
		}
		hydratedJobs, err := proc.hydrate(ctx, source, jobs)
		if err != nil {
			return nil, err
		}
		message.groupedEventsBySourceId[sourceId] = hydratedJobs
		if len(message.eventSchemaJobsBySourceId[sourceId]) > 0 {
			if len(hydratedJobs) > 0 {
				message.eventSchemaJobsBySourceId[sourceId] = make([]*jobsdb.JobT, 0, len(hydratedJobs))
			}
			eventSchemaJobsByJobID := lo.SliceToMap(message.eventSchemaJobsBySourceId[sourceId], func(job *jobsdb.JobT) (int64, *jobsdb.JobT) {
				return job.JobID, job
			})
			for i := 0; i < len(hydratedJobs); i++ {
				event := hydratedJobs[i]
				msgId := event.Metadata.MessageID
				jobId := hydratedJobs[i].Metadata.JobID
				marshalledPayload, marshallErr := jsonrs.Marshal(event.Message)
				if marshallErr != nil {
					proc.logger.Errorn("failed to marshal hydrated event for event schema",
						obskit.SourceID(string(sourceId)),
						obskit.Error(marshallErr),
					)
					panic(marshallErr)
				}
				if marshalledPayload != nil {
					originalJob := eventSchemaJobsByJobID[jobId]
					message.eventSchemaJobsBySourceId[sourceId] = append(message.eventSchemaJobsBySourceId[sourceId], &jobsdb.JobT{
						UUID:         originalJob.UUID,
						UserID:       originalJob.UserID,
						Parameters:   originalJob.Parameters,
						CustomVal:    originalJob.CustomVal,
						EventPayload: marshalledPayload,
						CreatedAt:    originalJob.CreatedAt,
						ExpireAt:     originalJob.ExpireAt,
						WorkspaceId:  originalJob.WorkspaceId,
					})
					message.eventsByMessageID[msgId] = types.SingularEventWithReceivedAt{
						SingularEvent: event.Message,
						ReceivedAt:    message.eventsByMessageID[msgId].ReceivedAt,
					}
				}
			}
		}
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
	req.Batch = lo.Map(events, func(event types.TransformerEvent, _ int) types.SrcHydrationEvent {
		return types.SrcHydrationEvent{
			ID:    strconv.FormatInt(event.Metadata.JobID, 10),
			Event: event.Message,
		}
	})
	resp, err := proc.transformerClients.SrcHydration().Hydrate(ctx, req)
	if err != nil {
		return nil, err
	}
	jobIDToEventMap := lo.SliceToMap(events, func(event types.TransformerEvent) (string, types.TransformerEvent) {
		return strconv.FormatInt(event.Metadata.JobID, 10), event
	})
	return lo.Map(resp.Batch, func(event types.SrcHydrationEvent, _ int) types.TransformerEvent {
		originalEvent := jobIDToEventMap[event.ID]
		originalEvent.Message = event.Event
		return originalEvent
	}), nil
}
