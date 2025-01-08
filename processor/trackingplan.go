package processor

import (
	"context"
	"strconv"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/stats"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/processor/transformer"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/types"
)

type TrackingPlanStatT struct {
	numEvents                   stats.Measurement
	numValidationSuccessEvents  stats.Measurement
	numValidationFailedEvents   stats.Measurement
	numValidationFilteredEvents stats.Measurement
	tpValidationTime            stats.Measurement
}

// reportViolations It is going add violationErrors in context depending upon certain criteria:
// 1. sourceSchemaConfig in Metadata.MergedTpConfig should be true
func reportViolations(validateEvent *transformer.TransformerResponse, trackingPlanId string, trackingPlanVersion int) {
	if validateEvent.Metadata.MergedTpConfig["propagateValidationErrors"] == "false" {
		return
	}
	validationErrors := validateEvent.ValidationErrors
	output := validateEvent.Output

	eventContext, ok := output["context"]
	if !ok || eventContext == nil {
		context := make(map[string]interface{})
		context["trackingPlanId"] = trackingPlanId
		context["trackingPlanVersion"] = trackingPlanVersion
		context["violationErrors"] = validationErrors
		output["context"] = context
		return
	}
	context, castOk := eventContext.(map[string]interface{})
	if !castOk {
		return
	}
	context["trackingPlanId"] = trackingPlanId
	context["trackingPlanVersion"] = trackingPlanVersion
	context["violationErrors"] = validationErrors
}

// enhanceWithViolation It enhances extra information of ValidationErrors in context for:
// 1. response.Events
// 1. response.FailedEvents
func enhanceWithViolation(response transformer.Response, trackingPlanId string, trackingPlanVersion int) {
	for i := range response.Events {
		validatedEvent := &response.Events[i]
		reportViolations(validatedEvent, trackingPlanId, trackingPlanVersion)
	}

	for i := range response.FailedEvents {
		validatedEvent := &response.FailedEvents[i]
		reportViolations(validatedEvent, trackingPlanId, trackingPlanVersion)
	}
}

// validateEvents If the TrackingPlanId exist for a particular write key then we are going to Validate from the transformer.
// The Response will contain both the Events and FailedEvents
// 1. eventsToTransform gets added to validatedEventsBySourceId
// 2. failedJobs gets added to validatedErrorJobs
func (proc *Handle) validateEvents(groupedEventsBySourceId map[SourceIDT][]transformer.TransformerEvent, eventsByMessageID map[string]types.SingularEventWithReceivedAt) (map[SourceIDT][]transformer.TransformerEvent, []*types.PUReportedMetric, []*jobsdb.JobT, map[SourceIDT]bool) {
	validatedEventsBySourceId := make(map[SourceIDT][]transformer.TransformerEvent)
	validatedReportMetrics := make([]*types.PUReportedMetric, 0)
	validatedErrorJobs := make([]*jobsdb.JobT, 0)
	trackingPlanEnabledMap := make(map[SourceIDT]bool)

	for sourceId := range groupedEventsBySourceId {
		eventList := groupedEventsBySourceId[sourceId]
		validationStat := proc.newValidationStat(&eventList[0].Metadata)
		validationStat.numEvents.Count(len(eventList))
		proc.logger.Debug("Validation input size", len(eventList))

		// Checking if the tracking plan exists
		isTpExists := eventList[0].Metadata.TrackingPlanID != ""
		if !isTpExists {
			// pass on the jobs for transformation(User, Dest)
			validatedEventsBySourceId[sourceId] = make([]transformer.TransformerEvent, 0)
			validatedEventsBySourceId[sourceId] = append(validatedEventsBySourceId[sourceId], eventList...)
			continue
		}

		transformerEvent := eventList[0]
		destination := &transformerEvent.Destination
		commonMetaData := makeCommonMetadataFromTransformerEvent(&transformerEvent)

		validationStart := time.Now()
		response := proc.transformer.Validate(context.TODO(), eventList, proc.config.userTransformBatchSize.Load())
		validationStat.tpValidationTime.Since(validationStart)

		// If transformerInput does not match with transformerOutput then we do not consider transformerOutput
		// This is a safety check we are adding so that if something unexpected comes from transformer
		// We are ignoring it.
		if (len(response.Events) + len(response.FailedEvents)) != len(eventList) {
			validatedEventsBySourceId[sourceId] = make([]transformer.TransformerEvent, 0)
			validatedEventsBySourceId[sourceId] = append(validatedEventsBySourceId[sourceId], eventList...)
			continue
		}

		enhanceWithViolation(response, eventList[0].Metadata.TrackingPlanID, eventList[0].Metadata.TrackingPlanVersion)
		// Set trackingPlanEnabledMap for the sourceID to true.
		// This is being used to distinguish the flows in reporting service
		trackingPlanEnabledMap[sourceId] = true

		var successMetrics []*types.PUReportedMetric
		eventsToTransform, successMetrics, _, _ := proc.getTransformerEvents(response, commonMetaData, eventsByMessageID, destination, backendconfig.Connection{}, types.DESTINATION_FILTER, types.TRACKINGPLAN_VALIDATOR) // Note: Sending false for usertransformation enabled is safe because this stage is before user transformation.
		nonSuccessMetrics := proc.getNonSuccessfulMetrics(response, commonMetaData, eventsByMessageID, types.DESTINATION_FILTER, types.TRACKINGPLAN_VALIDATOR)

		validationStat.numValidationSuccessEvents.Count(len(eventsToTransform))
		validationStat.numValidationFailedEvents.Count(len(nonSuccessMetrics.failedJobs))
		validationStat.numValidationFilteredEvents.Count(len(nonSuccessMetrics.filteredJobs))
		proc.logger.Debug("Validation output size", len(eventsToTransform))

		validatedErrorJobs = append(validatedErrorJobs, nonSuccessMetrics.failedJobs...)

		// REPORTING - START
		if proc.isReportingEnabled() {
			// There will be no diff metrics for tracking plan validation
			validatedReportMetrics = append(validatedReportMetrics, successMetrics...)
			validatedReportMetrics = append(validatedReportMetrics, nonSuccessMetrics.failedMetrics...)
			validatedReportMetrics = append(validatedReportMetrics, nonSuccessMetrics.filteredMetrics...)
		}
		// REPORTING - END

		if len(eventsToTransform) == 0 {
			continue
		}
		validatedEventsBySourceId[sourceId] = make([]transformer.TransformerEvent, 0)
		validatedEventsBySourceId[sourceId] = append(validatedEventsBySourceId[sourceId], eventsToTransform...)
	}
	return validatedEventsBySourceId, validatedReportMetrics, validatedErrorJobs, trackingPlanEnabledMap
}

// makeCommonMetadataFromTransformerEvent Creates a new Metadata instance
func makeCommonMetadataFromTransformerEvent(transformerEvent *transformer.TransformerEvent) *transformer.Metadata {
	metadata := transformerEvent.Metadata
	commonMetaData := transformer.Metadata{
		SourceID:         metadata.SourceID,
		SourceName:       metadata.SourceName,
		SourceType:       metadata.SourceType,
		SourceCategory:   metadata.SourceCategory,
		WorkspaceID:      metadata.WorkspaceID,
		Namespace:        config.GetKubeNamespace(),
		InstanceID:       misc.GetInstanceID(),
		DestinationID:    metadata.DestinationID,
		DestinationType:  metadata.DestinationType,
		OriginalSourceID: metadata.OriginalSourceID,
	}
	return &commonMetaData
}

// newValidationStat Creates a new TrackingPlanStatT instance
func (proc *Handle) newValidationStat(metadata *transformer.Metadata) *TrackingPlanStatT {
	tags := map[string]string{
		"destination":         metadata.DestinationID,
		"destType":            metadata.DestinationType,
		"source":              metadata.SourceID,
		"workspaceId":         metadata.WorkspaceID,
		"trackingPlanId":      metadata.TrackingPlanID,
		"trackingPlanVersion": strconv.Itoa(metadata.TrackingPlanVersion),
	}

	numEvents := proc.statsFactory.NewTaggedStat("proc_num_tp_input_events", stats.CountType, tags)
	numValidationSuccessEvents := proc.statsFactory.NewTaggedStat("proc_num_tp_output_success_events", stats.CountType, tags)
	numValidationFailedEvents := proc.statsFactory.NewTaggedStat("proc_num_tp_output_failed_events", stats.CountType, tags)
	numValidationFilteredEvents := proc.statsFactory.NewTaggedStat("proc_num_tp_output_filtered_events", stats.CountType, tags)
	tpValidationTime := proc.statsFactory.NewTaggedStat("proc_tp_validation", stats.TimerType, tags)

	return &TrackingPlanStatT{
		numEvents:                   numEvents,
		numValidationSuccessEvents:  numValidationSuccessEvents,
		numValidationFailedEvents:   numValidationFailedEvents,
		numValidationFilteredEvents: numValidationFilteredEvents,
		tpValidationTime:            tpValidationTime,
	}
}
