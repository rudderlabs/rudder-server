package processor

import (
	"strconv"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/processor/integrations"
	"github.com/rudderlabs/rudder-server/processor/transformer"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/types"
)

type TrackingPlanStatT struct {
	numEvents                  stats.RudderStats
	numValidationSuccessEvents stats.RudderStats
	numValidationFailedEvents  stats.RudderStats
	tpValidationTime           stats.RudderStats
}

// reportViolations It is going add violationErrors in context depending upon certain criteria:
// 1. sourceSchemaConfig in Metadata.MergedTpConfig should be true
func reportViolations(validateEvent *transformer.TransformerResponseT, trackingPlanId string, trackingPlanVersion int) {
	if validateEvent.Metadata.MergedTpConfig["propagateValidationErrors"] == "false" {
		return
	}
	validationErrors := validateEvent.ValidationErrors
	output := validateEvent.Output

	eventContext, castOk := output["context"].(map[string]interface{})
	if castOk {
		eventContext["trackingPlanId"] = trackingPlanId
		eventContext["trackingPlanVersion"] = trackingPlanVersion
		eventContext["violationErrors"] = validationErrors
	}
}

// enhanceWithViolation It enhances extra information of ValidationErrors in context for:
// 1. response.Events
// 1. response.FailedEvents
func enhanceWithViolation(response transformer.ResponseT, trackingPlanId string, trackingPlanVersion int) {
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
// The ResponseT will contain both the Events and FailedEvents
// 1. eventsToTransform gets added to validatedEventsByWriteKey
// 2. failedJobs gets added to validatedErrorJobs
func (proc *HandleT) validateEvents(groupedEventsByWriteKey map[WriteKeyT][]transformer.TransformerEventT, eventsByMessageID map[string]types.SingularEventWithReceivedAt) (map[WriteKeyT][]transformer.TransformerEventT, []*types.PUReportedMetric, []*jobsdb.JobT, map[SourceIDT]bool) {
	validatedEventsByWriteKey := make(map[WriteKeyT][]transformer.TransformerEventT)
	validatedReportMetrics := make([]*types.PUReportedMetric, 0)
	validatedErrorJobs := make([]*jobsdb.JobT, 0)
	trackingPlanEnabledMap := make(map[SourceIDT]bool)

	for writeKey := range groupedEventsByWriteKey {
		eventList := groupedEventsByWriteKey[writeKey]
		validationStat := proc.newValidationStat(&eventList[0].Metadata)
		validationStat.numEvents.Count(len(eventList))
		proc.logger.Debug("Validation input size", len(eventList))

		// Checking if the tracking plan exists
		isTpExists := eventList[0].Metadata.TrackingPlanId != ""
		if !isTpExists {
			// pass on the jobs for transformation(User, Dest)
			validatedEventsByWriteKey[writeKey] = make([]transformer.TransformerEventT, 0)
			validatedEventsByWriteKey[writeKey] = append(validatedEventsByWriteKey[writeKey], eventList...)
			continue
		}

		validationStat.tpValidationTime.Start()
		response := proc.transformer.Validate(eventList, integrations.GetTrackingPlanValidationURL(), userTransformBatchSize)
		validationStat.tpValidationTime.End()

		// If transformerInput does not match with transformerOutput then we do not consider transformerOutput
		// This is a safety check we are adding so that if something unexpected comes from transformer
		// We are ignoring it.
		if (len(response.Events) + len(response.FailedEvents)) != len(eventList) {
			validatedEventsByWriteKey[writeKey] = make([]transformer.TransformerEventT, 0)
			validatedEventsByWriteKey[writeKey] = append(validatedEventsByWriteKey[writeKey], eventList...)
			continue
		}

		enhanceWithViolation(response, eventList[0].Metadata.TrackingPlanId, eventList[0].Metadata.TrackingPlanVersion)

		transformerEvent := eventList[0]
		destination := &transformerEvent.Destination
		sourceID := transformerEvent.Metadata.SourceID
		commonMetaData := makeCommonMetadataFromTransformerEvent(&transformerEvent)

		// Set trackingPlanEnabledMap for the sourceID to true.
		// This is being used to distinguish the flows in reporting service
		trackingPlanEnabledMap[SourceIDT(sourceID)] = true

		var successMetrics []*types.PUReportedMetric
		eventsToTransform, successMetrics, _, _ := proc.getDestTransformerEvents(response, commonMetaData, destination, transformer.TrackingPlanValidationStage, true, false) // Note: Sending false for usertransformation enabled is safe because this stage is before user transformation.
		failedJobs, failedMetrics, _ := proc.getFailedEventJobs(response, commonMetaData, eventsByMessageID, transformer.TrackingPlanValidationStage, false, true)

		validationStat.numValidationSuccessEvents.Count(len(eventsToTransform))
		validationStat.numValidationFailedEvents.Count(len(failedJobs))
		proc.logger.Debug("Validation output size", len(eventsToTransform))

		validatedErrorJobs = append(validatedErrorJobs, failedJobs...)

		// REPORTING - START
		if proc.isReportingEnabled() {
			// There will be no diff metrics for tracking plan validation
			validatedReportMetrics = append(validatedReportMetrics, successMetrics...)
			validatedReportMetrics = append(validatedReportMetrics, failedMetrics...)
		}
		// REPORTING - END

		if len(eventsToTransform) == 0 {
			continue
		}
		validatedEventsByWriteKey[writeKey] = make([]transformer.TransformerEventT, 0)
		validatedEventsByWriteKey[writeKey] = append(validatedEventsByWriteKey[writeKey], eventsToTransform...)
	}
	return validatedEventsByWriteKey, validatedReportMetrics, validatedErrorJobs, trackingPlanEnabledMap
}

// makeCommonMetadataFromTransformerEvent Creates a new MetadataT instance
func makeCommonMetadataFromTransformerEvent(transformerEvent *transformer.TransformerEventT) *transformer.MetadataT {
	metadata := transformerEvent.Metadata
	commonMetaData := transformer.MetadataT{
		SourceID:        metadata.SourceID,
		SourceType:      metadata.SourceType,
		SourceCategory:  metadata.SourceCategory,
		WorkspaceID:     metadata.WorkspaceID,
		Namespace:       config.GetKubeNamespace(),
		InstanceID:      config.GetInstanceID(),
		DestinationID:   metadata.DestinationID,
		DestinationType: metadata.DestinationType,
	}
	return &commonMetaData
}

// newValidationStat Creates a new TrackingPlanStatT instance
func (proc *HandleT) newValidationStat(metadata *transformer.MetadataT) *TrackingPlanStatT {
	tags := map[string]string{
		"destination":         metadata.DestinationID,
		"destType":            metadata.DestinationType,
		"source":              metadata.SourceID,
		"workspaceId":         metadata.WorkspaceID,
		"trackingPlanId":      metadata.TrackingPlanId,
		"trackingPlanVersion": strconv.Itoa(metadata.TrackingPlanVersion),
	}

	numEvents := proc.statsFactory.NewTaggedStat("proc_num_tp_input_events", stats.CountType, tags)
	numValidationSuccessEvents := proc.statsFactory.NewTaggedStat("proc_num_tp_output_success_events", stats.CountType, tags)
	numValidationFailedEvents := proc.statsFactory.NewTaggedStat("proc_num_tp_output_failed_events", stats.CountType, tags)
	tpValidationTime := proc.statsFactory.NewTaggedStat("proc_tp_validation", stats.TimerType, tags)

	return &TrackingPlanStatT{
		numEvents:                  numEvents,
		numValidationSuccessEvents: numValidationSuccessEvents,
		numValidationFailedEvents:  numValidationFailedEvents,
		tpValidationTime:           tpValidationTime,
	}
}
