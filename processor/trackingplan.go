package processor

import (
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/processor/integrations"
	"github.com/rudderlabs/rudder-server/processor/transformer"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/types"
	"strconv"
)

type TrackingPlanT struct {
	numEvents                  stats.RudderStats
	numValidationSuccessEvents stats.RudderStats
	numValidationFailedEvents  stats.RudderStats
	tpValidationTime           stats.RudderStats
}

func (proc *HandleT) validateEvents(groupedEventsByWriteKey map[WriteKeyT][]transformer.TransformerEventT, eventsByMessageID map[string]types.SingularEventWithReceivedAt, procErrorJobsByDestID map[string][]*jobsdb.JobT, trackingPlanEnabledMap map[SourceIDT]bool, reportMetrics []*types.PUReportedMetric) map[WriteKeyT][]transformer.TransformerEventT {
	//validating with the tp here for every writeKey
	var validatedEventsByWriteKey = make(map[WriteKeyT][]transformer.TransformerEventT)

	//Placing the trackingPlan validation filters here.
	//Else further down events are duplicated by destId, so multiple validation takes places for same event
	for writeKey, eventList := range groupedEventsByWriteKey {
		validationStat := proc.newValidationStat(eventList[0].Metadata)
		validationStat.numEvents.Count(len(eventList))
		proc.logger.Debug("Validation input size", len(eventList))

		isTpExists := eventList[0].Metadata.TrackingPlanId != ""
		if !isTpExists {
			// pass on the jobs for transformation(User,Dest)
			validatedEventsByWriteKey[writeKey] = make([]transformer.TransformerEventT, 0)
			validatedEventsByWriteKey[writeKey] = append(validatedEventsByWriteKey[writeKey], eventList...)
			continue
		}

		validationStat.tpValidationTime.Start()
		response := proc.transformer.Validate(eventList, integrations.GetTrackingPlanValidationURL(), userTransformBatchSize)
		validationStat.tpValidationTime.End()

		// If transformerInput does not match with transformerOutput then we do not consider transformerOutput
		if (len(response.Events) + len(response.FailedEvents)) != len(eventList) {
			validatedEventsByWriteKey[writeKey] = append(validatedEventsByWriteKey[writeKey], eventList...)
			//Capture metrics
			continue
		}

		sourceID := eventList[0].Metadata.SourceID
		destID := eventList[0].Metadata.DestinationID
		destination := eventList[0].Destination
		workspaceID := eventList[0].Metadata.WorkspaceID
		commonMetaData := transformer.MetadataT{
			SourceID:        sourceID,
			SourceType:      eventList[0].Metadata.SourceType,
			SourceCategory:  eventList[0].Metadata.SourceCategory,
			WorkspaceID:     workspaceID,
			Namespace:       config.GetKubeNamespace(),
			InstanceID:      config.GetInstanceID(),
			DestinationID:   destID,
			DestinationType: destination.DestinationDefinition.Name,
		}

		trackingPlanEnabledMap[SourceIDT(sourceID)] = true

		var successMetrics []*types.PUReportedMetric
		eventsToTransform, successMetrics, _, _ := proc.getDestTransformerEvents(response, commonMetaData, destination, transformer.TrackingPlanValidationStage, true)
		failedJobs, failedMetrics, _ := proc.getFailedEventJobs(response, commonMetaData, eventsByMessageID, transformer.TrackingPlanValidationStage, false, true)

		validationStat.numValidationSuccessEvents.Count(len(eventsToTransform))
		validationStat.numValidationFailedEvents.Count(len(failedJobs))
		proc.logger.Debug("Validation output size", len(eventsToTransform))

		if _, ok := procErrorJobsByDestID[destID]; !ok {
			procErrorJobsByDestID[destID] = make([]*jobsdb.JobT, 0)
		}
		procErrorJobsByDestID[destID] = append(procErrorJobsByDestID[destID], failedJobs...)

		//REPORTING - START
		if proc.reporting != nil && proc.reportingEnabled {
			//There will be no diff metrics for tracking plan validation
			reportMetrics = append(reportMetrics, successMetrics...)
			reportMetrics = append(reportMetrics, failedMetrics...)
		}
		//REPORTING - END

		if len(eventsToTransform) == 0 {
			continue
		}
		validatedEventsByWriteKey[writeKey] = make([]transformer.TransformerEventT, 0)
		validatedEventsByWriteKey[writeKey] = append(validatedEventsByWriteKey[writeKey], eventsToTransform...)
	}
	return validatedEventsByWriteKey
}

func (proc *HandleT) newValidationStat(metadata transformer.MetadataT) *TrackingPlanT {
	tags := map[string]string{
		"destination":         metadata.DestinationID,
		"destType":            metadata.DestinationType,
		"source":              metadata.SourceID,
		"workspaceId":         metadata.WorkspaceID,
		"trackingPlanId":      metadata.TrackingPlanId,
		"trackingPlanVersion": strconv.Itoa(metadata.TrackingPlanVersion),
	}

	numEvents := proc.stats.NewTaggedStat("proc_num_tp_input_events", stats.CountType, tags)
	numValidationSuccessEvents := proc.stats.NewTaggedStat("proc_num_tp_output_success_events", stats.CountType, tags)
	numValidationFailedEvents := proc.stats.NewTaggedStat("proc_num_tp_output_failed_events", stats.CountType, tags)
	tpValidationTime := proc.stats.NewTaggedStat("proc_tp_validation", stats.TimerType, tags)

	return &TrackingPlanT{
		numEvents:                  numEvents,
		numValidationSuccessEvents: numValidationSuccessEvents,
		numValidationFailedEvents:  numValidationFailedEvents,
		tpValidationTime:           tpValidationTime,
	}
}
