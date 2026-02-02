package processor

import (
	"context"
	"strconv"
	"time"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/processor/types"
	reportingtypes "github.com/rudderlabs/rudder-server/utils/types"
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
func reportViolations(validateEvent *types.TransformerResponse, trackingPlanID string, trackingPlanVersion int) {
	if validateEvent.Metadata.MergedTpConfig["propagateValidationErrors"] == "false" {
		return
	}
	validationErrors := validateEvent.ValidationErrors
	output := validateEvent.Output
	if output == nil {
		output = make(map[string]any)
		validateEvent.Output = output
	}

	eventContext, ok := output["context"]
	if !ok || eventContext == nil {
		context := make(map[string]any)
		context["trackingPlanId"] = trackingPlanID
		context["trackingPlanVersion"] = trackingPlanVersion
		context["violationErrors"] = validationErrors
		output["context"] = context
		return
	}
	context, castOk := eventContext.(map[string]any)
	if !castOk {
		return
	}
	context["trackingPlanId"] = trackingPlanID
	context["trackingPlanVersion"] = trackingPlanVersion
	context["violationErrors"] = validationErrors
}

// enhanceWithViolation It enhances extra information of ValidationErrors in context for:
// 1. response.Events
// 1. response.FailedEvents
func enhanceWithViolation(response types.Response, trackingPlanID string, trackingPlanVersion int) {
	for i := range response.Events {
		validatedEvent := &response.Events[i]
		reportViolations(validatedEvent, trackingPlanID, trackingPlanVersion)
	}

	for i := range response.FailedEvents {
		validatedEvent := &response.FailedEvents[i]
		reportViolations(validatedEvent, trackingPlanID, trackingPlanVersion)
	}
}

// validateEvents If the TrackingPlanId exist for a particular write key then we are going to Validate from the transformer.
// The Response will contain both the Events and FailedEvents
// 1. eventsToTransform gets added to validatedEventsBySourceId
func (proc *Handle) validateEvents(groupedEventsBySourceId map[SourceIDT][]types.TransformerEvent, eventsByMessageID map[string]types.SingularEventWithReceivedAt, srcHydrationEnabledMap map[SourceIDT]bool) (map[SourceIDT][]types.TransformerEvent, []*reportingtypes.PUReportedMetric, sourceIDPipelineSteps) {
	validatedEventsBySourceId := make(map[SourceIDT][]types.TransformerEvent)
	validatedReportMetrics := make([]*reportingtypes.PUReportedMetric, 0)
	sourcePipelineSteps := make(sourceIDPipelineSteps)
	for enabledSourceId, enabled := range srcHydrationEnabledMap {
		sourcePipelineSteps[enabledSourceId] = SourcePipelineSteps{srcHydration: enabled}
	}

	for sourceId := range groupedEventsBySourceId {
		eventList := groupedEventsBySourceId[sourceId]

		trackingPlanID := eventList[0].Metadata.TrackingPlanID
		trackingPlanVersion := eventList[0].Metadata.TrackingPlanVersion

		if trackingPlanID == "" {
			// pass on the jobs for transformation(User, Dest)
			validatedEventsBySourceId[sourceId] = append(validatedEventsBySourceId[sourceId], eventList...)
			continue
		}
		validationStat := proc.newValidationStat(&eventList[0].Metadata)
		validationStat.numEvents.Count(len(eventList))
		transformerEvent := eventList[0]

		commonMetaData := transformerEvent.Metadata.CommonMetadata()

		validationStart := time.Now()
		response := proc.transformerClients.TrackingPlan().Validate(context.TODO(), eventList)
		validationStat.tpValidationTime.Since(validationStart)

		// If transformerInput does not match with transformerOutput then we do not consider transformerOutput
		// This is a safety check we are adding so that if something unexpected comes from transformer
		// We are ignoring it.
		if (len(response.Events) + len(response.FailedEvents)) != len(eventList) {
			validatedEventsBySourceId[sourceId] = append(validatedEventsBySourceId[sourceId], eventList...)
			continue
		}

		enhanceWithViolation(response, trackingPlanID, trackingPlanVersion)
		// Set sourcePipelineSteps.trackingPlanValidation for the sourceID to true.
		// This is being used to distinguish the flows in reporting service
		sourceSteps := sourcePipelineSteps[sourceId]
		sourceSteps.trackingPlanValidation = true
		sourcePipelineSteps[sourceId] = sourceSteps

		inPU := reportingtypes.DESTINATION_FILTER
		if sourcePipelineSteps[sourceId].srcHydration {
			inPU = reportingtypes.SOURCE_HYDRATION
		}

		var successMetrics []*reportingtypes.PUReportedMetric
		eventsToTransform, successMetrics, _, _ := proc.getTransformerEvents(response, commonMetaData, eventsByMessageID, &transformerEvent.Destination, backendconfig.Connection{}, inPU, reportingtypes.TRACKINGPLAN_VALIDATOR) // Note: Sending false for usertransformation enabled is safe because this stage is before user transformation.
		nonSuccessMetrics := proc.getNonSuccessfulMetrics(response, eventList, commonMetaData, eventsByMessageID, inPU, reportingtypes.TRACKINGPLAN_VALIDATOR)

		validationStat.numValidationSuccessEvents.Count(len(eventsToTransform))
		validationStat.numValidationFailedEvents.Count(len(nonSuccessMetrics.failedJobs))
		validationStat.numValidationFilteredEvents.Count(len(nonSuccessMetrics.filteredJobs))
		proc.logger.Debugn("Validation output size", logger.NewIntField("outputSize", int64(len(eventsToTransform))))

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
		validatedEventsBySourceId[sourceId] = append(validatedEventsBySourceId[sourceId], eventsToTransform...)
	}
	return validatedEventsBySourceId, validatedReportMetrics, sourcePipelineSteps
}

// newValidationStat Creates a new TrackingPlanStatT instance
func (proc *Handle) newValidationStat(metadata *types.Metadata) *TrackingPlanStatT {
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
