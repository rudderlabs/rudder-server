package processor

import (
	"context"
	"time"

	"github.com/samber/lo"

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
	eventSchemaJobsBySourceId     map[SourceIDT][]*jobsdb.JobT
	connectionDetailsMap          map[string]*reportingtypes.ConnectionDetails
	statusDetailsMap              map[string]map[string]*reportingtypes.StatusDetail
	enricherStatusDetailsMap      map[string]map[string]*reportingtypes.StatusDetail
	botManagementStatusDetailsMap map[string]map[string]*reportingtypes.StatusDetail
	eventBlockingStatusDetailsMap map[string]map[string]*reportingtypes.StatusDetail
	destFilterStatusDetailMap     map[string]map[string]*reportingtypes.StatusDetail
	reportMetrics                 []*reportingtypes.PUReportedMetric
	inCountMetadataMap            map[string]MetricMetadata
	inCountMap                    map[string]int64
	outCountMap                   map[string]int64
	totalEvents                   int
	groupedEventsBySourceId       map[SourceIDT][]types.TransformerEvent
	eventsByMessageID             map[string]types.SingularEventWithMetadata
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

	for source, jobs := range message.groupedEventsBySourceId {
		supported, err := proc.isSourceHydrationSupportedForSource(string(source))
		if err != nil {
			return nil, err
		}
		if !supported {
			continue
		}
		hydratedJobs := proc.hydrate(ctx, source, jobs)
		message.groupedEventsBySourceId[source] = hydratedJobs
		if len(message.eventSchemaJobsBySourceId[source]) > 0 {
			if len(hydratedJobs) > 0 {
				message.eventSchemaJobsBySourceId[source] = make([]*jobsdb.JobT, 0, len(hydratedJobs))
			}
			for i := 0; i < len(hydratedJobs); i++ {
				event := hydratedJobs[i]
				msgId := event.Metadata.MessageID
				originalEventWithMetadata := message.eventsByMessageID[msgId]
				marshalledPayload, marshallErr := jsonrs.Marshal(event.Message)
				if marshallErr != nil {
					proc.logger.Errorn("failed to marshal hydrated event for event schema",
						obskit.SourceID(string(source)),
						obskit.Error(marshallErr),
					)
				}
				if marshalledPayload != nil {
					message.eventSchemaJobsBySourceId[source] = append(message.eventSchemaJobsBySourceId[source], &jobsdb.JobT{
						UUID:         originalEventWithMetadata.UUID,
						UserID:       originalEventWithMetadata.UserID,
						Parameters:   originalEventWithMetadata.Parameters,
						CustomVal:    originalEventWithMetadata.CustomVal,
						EventPayload: marshalledPayload,
						CreatedAt:    time.Now(),
						ExpireAt:     time.Now(),
						WorkspaceId:  originalEventWithMetadata.WorkspaceId,
					})
				}
			}
		}
	}
	return &preTransformationMessage{
		partition:                     message.partition,
		subJobs:                       message.subJobs,
		eventSchemaJobsBySourceId:     message.eventSchemaJobsBySourceId,
		connectionDetailsMap:          message.connectionDetailsMap,
		statusDetailsMap:              message.statusDetailsMap,
		enricherStatusDetailsMap:      message.enricherStatusDetailsMap,
		botManagementStatusDetailsMap: message.botManagementStatusDetailsMap,
		eventBlockingStatusDetailsMap: message.eventBlockingStatusDetailsMap,
		destFilterStatusDetailMap:     message.destFilterStatusDetailMap,
		reportMetrics:                 message.reportMetrics,
		inCountMetadataMap:            message.inCountMetadataMap,
		inCountMap:                    message.inCountMap,
		outCountMap:                   message.outCountMap,
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

func (proc *Handle) hydrate(_ context.Context, _ SourceIDT, jobs []types.TransformerEvent) []types.TransformerEvent {
	return jobs
}
