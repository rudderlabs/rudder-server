package processor

import (
	"encoding/json"
	"fmt"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/processor/integrations"
	"github.com/rudderlabs/rudder-server/processor/transformer"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/types"
	uuid "github.com/satori/go.uuid"
	"time"
)

type TrackingPlanStatT struct {
	numEvents             stats.RudderStats
	validateSuccessEvents stats.RudderStats
	validateFailedEvents  stats.RudderStats
	validateEventTime     stats.RudderStats
	violationsByType      stats.RudderStats
}

// extracts the events from Validation response and make them transformer ready
func (proc *HandleT) GetTransformerEventsFromValidationResponse(response transformer.ResponseT, eventsByMessageID map[string]types.SingularEventWithReceivedAt) ([]transformer.TransformerEventT, []*jobsdb.JobT, map[string]int) {
	var eventsToTransform []transformer.TransformerEventT
	var eventsToDrop []transformer.TransformerResponseT
	var failedEventsToStore []*jobsdb.JobT
	var violationsByTypeStats = make(map[string]int)

	for _, validatedEvent := range response.Events {
		updatedEvent := transformer.TransformerEventT{
			Message:  validatedEvent.Output, // singular event
			Metadata: validatedEvent.Metadata,
		}
		// report all violations irrespective of SchemaConfig
		reportViolations(&validatedEvent)

		sourceTpConfig := validatedEvent.Metadata.SourceTpConfig
		mergedTpConfig := validatedEvent.Metadata.MergedTpConfig
		if len(validatedEvent.ValidationErrors) == 0 || len(sourceTpConfig) == 0 {
			eventsToTransform = append(eventsToTransform, updatedEvent)
			continue
		}

		//set of all violationTypes in event
		violationsByType := make(map[string]struct{})
		for _, violation := range validatedEvent.ValidationErrors {
			violationsByType[violation.Type] = struct{}{}
		}

		if len(mergedTpConfig) == 0 {
			//All events are forwarded
			eventsToTransform = append(eventsToTransform, updatedEvent)
			continue
		}

		dropEvent := false
		for k, v := range mergedTpConfig {
			value := fmt.Sprint(v)
			switch k {
			case "allowUnplannedEvents":
				_, exists := violationsByType[transformer.UnplannedEvent]
				if value == "false" && exists {
					misc.IncrementMapByKey(violationsByTypeStats, transformer.UnplannedEvent, 1)
					dropEvent = true
					break
				}
				if !(value == "true" || value == "false") {
					pkgLogger.Errorf("Unknown option %s in %s", value, k)
				}
			case "unplannedProperties":
				_, exists := violationsByType[transformer.AdditionalProperties]
				if value == "drop" && exists {
					misc.IncrementMapByKey(violationsByTypeStats, transformer.AdditionalProperties, 1)
					dropEvent = true
					break
				}
				if !(value == "forward" || value == "drop") {
					pkgLogger.Errorf("Unknown option %s in %s", value, k)
				}
			case "anyOtherViolation":
				_, exists := violationsByType[transformer.UnknownViolation]
				_, exists1 := violationsByType[transformer.DatatypeMismatch]
				_, exists2 := violationsByType[transformer.RequiredMissing]
				if value == "drop" && (exists || exists1 || exists2) {
					if exists {
						misc.IncrementMapByKey(violationsByTypeStats, transformer.UnknownViolation, 1)
					} else if exists1 {
						misc.IncrementMapByKey(violationsByTypeStats, transformer.DatatypeMismatch, 1)
					} else {
						misc.IncrementMapByKey(violationsByTypeStats, transformer.RequiredMissing, 1)
					}
					dropEvent = true
					break
				}
				if !(value == "forward" || value == "drop") {
					pkgLogger.Errorf("Unknown option %s in %s", value, k)
				}
			case "sendViolatedEventsTo":
				if value != "procErrors" {
					pkgLogger.Errorf("Unknown option %s in %s", value, k)
				}
			case "ajvOptions":
			default:
				pkgLogger.Errorf("Unknown option %s in %s in eventSchema config", value, k)
			}
		}
		if dropEvent {
			eventsToDrop = append(eventsToDrop, validatedEvent)
		} else {
			eventsToTransform = append(eventsToTransform, updatedEvent)
		}
	}
	//TODO: put length assertions; drop + transform = total
	for _, dropEvent := range eventsToDrop {
		var messages []types.SingularEventT
		if len(dropEvent.Metadata.MessageIDs) > 0 {
			messageIds := dropEvent.Metadata.MessageIDs
			for _, msgID := range messageIds {
				messages = append(messages, eventsByMessageID[msgID].SingularEvent)
			}
		} else {
			messages = append(messages, eventsByMessageID[dropEvent.Metadata.MessageID].SingularEvent)
		}
		payload, err := json.Marshal(messages)
		if err != nil {
			proc.logger.Errorf(`[Processor: getFailedEventJobs] Failed to unmarshal list of failed events: %v`, err)
			continue
		}
		id := uuid.NewV4()

		params := map[string]interface{}{
			"source_id":         dropEvent.Metadata.SourceID,
			"source_job_run_id": dropEvent.Metadata.JobRunID,
			"error":             dropEvent.Error,
			"status_code":       dropEvent.StatusCode,
			"stage":             transformer.TrackingPlanValidationStage,
			"validation_errors": dropEvent.ValidationErrors,
		}
		marshalledParams, err := json.Marshal(params)
		if err != nil {
			proc.logger.Errorf("[Processor] Failed to marshal parameters. Parameters: %v", params)
			marshalledParams = []byte(`{"error": "Processor failed to marshal params"}`)
		}
		newFailedJob := jobsdb.JobT{
			UUID:         id,
			EventPayload: payload,
			Parameters:   marshalledParams,
			CreatedAt:    time.Now(),
			ExpireAt:     time.Now(),
			UserID:       dropEvent.Metadata.RudderID,
		}
		failedEventsToStore = append(failedEventsToStore, &newFailedJob)

	}
	return eventsToTransform, failedEventsToStore, violationsByTypeStats
}

func reportViolations(validateEvent *transformer.TransformerResponseT) {
	if !sourceSchemaConfig {
		return
	}
	if len(validateEvent.ValidationErrors) == 0 {
		return
	}
	validationErrors := validateEvent.ValidationErrors
	output := validateEvent.Output

	pkgLogger.Errorf("%d errors reported", len(validationErrors))
	pkgLogger.Error(validationErrors)

	eventContext, castOk := output["context"].(map[string]interface{})
	if castOk {
		eventContext["violationErrors"] = validationErrors
	}
}

func (proc *HandleT) validateEvents(groupedEventsByWriteKey map[string][]transformer.TransformerEventT, eventsByMessageID map[string]types.SingularEventWithReceivedAt) map[string][]transformer.TransformerEventT {
	//validating with the tp here for every writeKey
	var validatedEventsByWriteKey = make(map[string][]transformer.TransformerEventT)
	for writeKey, eventList := range groupedEventsByWriteKey {
		isTpExists := eventList[0].Metadata.TrackingPlanId != ""
		if !isTpExists {
			// pass on the jobs for transformation(User,Dest)
			validatedEventsByWriteKey[writeKey] = make([]transformer.TransformerEventT, 0)
			validatedEventsByWriteKey[writeKey] = append(validatedEventsByWriteKey[writeKey], eventList...)
			continue
		}
		tpValidationStat := proc.newTrackingPlanStat(eventList[0])
		tpValidationStat.numEvents.Count(len(eventList))
		tpValidationStat.validateEventTime.Start()
		response := proc.transformer.Validate(eventList, integrations.GetTrackingPlanValidationURL(), userTransformBatchSize)
		tpValidationStat.validateEventTime.End()

		commonMetaData := transformer.MetadataT{
			SourceID:       eventList[0].Metadata.SourceID,
			SourceType:     eventList[0].Metadata.SourceType,
			SourceCategory: eventList[0].Metadata.SourceCategory,
		}

		//handling incompatible version types between rudder-server and transformer
		filteredFailedEvents := []transformer.TransformerResponseT{}
		for _, event := range response.FailedEvents {
			//handling case when rudder-transformer is missing validation end-point
			//passing on the events as successfull further forward to user/dest transformation
			//As tp is still in initial phase, forward all 400 events too for now,
			//Once when fully developed(like supporting multiple jsonschema versions etc), events can be dropped to proc_error
			if event.StatusCode == 404 || event.StatusCode == 400 {
				event.Output = eventsByMessageID[event.Metadata.MessageID].SingularEvent
				response.Events = append(response.Events, event)
				pkgLogger.Errorf("Missing validation endpoint(upgrade rudder-transformer), Error : %v", event.Error)
				continue
			}
			filteredFailedEvents = append(filteredFailedEvents, event)
		}
		response.FailedEvents = filteredFailedEvents
		eventsToTransform, validationFailedJobs, violationsByTypeStats := proc.GetTransformerEventsFromValidationResponse(response, eventsByMessageID)
		//dumps violated events as per sourceTpConfig to proc_error
		if len(validationFailedJobs) > 0 {
			proc.logger.Info("[Processor] Total validationFailedJobs written to proc_error: ", len(validationFailedJobs))
			err := proc.errorDB.Store(validationFailedJobs)
			if err != nil {
				proc.logger.Errorf("Store into proc error table failed with error: %v", err)
				proc.logger.Errorf("procErrorJobs: %v", validationFailedJobs)
				panic(err)
			}
		}
		// dumps all non-200 jobs from validation endpoint. (code:404 jobs are not dumped-handled above)
		failedJobs, failedMetrics, failedCountMap := proc.getFailedEventJobs(response, commonMetaData, eventsByMessageID, transformer.TrackingPlanValidationStage, false)
		if len(failedJobs) > 0 {
			proc.logger.Info("[Processor] Total jobs written to proc_error: ", len(failedJobs))
			err := proc.errorDB.Store(failedJobs)
			if err != nil {
				proc.logger.Errorf("Store into proc error table failed with error: %v", err)
				proc.logger.Errorf("procErrorJobs: %v", failedJobs)
				panic(err)
			}
			proc.logger.Errorf("Failed metrics : ", failedMetrics, failedCountMap)
		}

		tpValidationStat.validateSuccessEvents.Count(len(eventsToTransform))
		tpValidationStat.validateFailedEvents.Count(len(validationFailedJobs) + len(failedJobs))
		tpValidationStat.violationsByType.Gauge(violationsByTypeStats)

		if len(eventsToTransform) == 0 {
			continue
		} else {
			validatedEventsByWriteKey[writeKey] = make([]transformer.TransformerEventT, 0)
			validatedEventsByWriteKey[writeKey] = append(validatedEventsByWriteKey[writeKey], eventsToTransform...)
		}
	}
	return validatedEventsByWriteKey
}

func (proc *HandleT) newTrackingPlanStat(event transformer.TransformerEventT) *TrackingPlanStatT {
	tags := map[string]string{
		"source":              event.Metadata.SourceID,
		"workspaceId":         event.Metadata.WorkspaceID,
		"destination":         event.Destination.ID,
		"destType":            event.Destination.Name,
		"trackingPlanId":      event.Metadata.TrackingPlanId,
		"trackingPlanVersion": string(rune(event.Metadata.TrackingPlanVersion)),
	}

	numEvents := proc.stats.NewTaggedStat("processor.num_tp_events", stats.CountType, tags)
	validateSuccessEvents := proc.stats.NewTaggedStat("processor.num_tp_validate_success_events", stats.CountType, tags)
	validateFailedEvents := proc.stats.NewTaggedStat("processor.num_tp_validate_failed_events", stats.CountType, tags)
	violationsByType := proc.stats.NewTaggedStat("processor.tp_violation_by_type", stats.GaugeType, tags)
	validateTime := proc.stats.NewTaggedStat("processor.tp_validate_time", stats.TimerType, tags)

	return &TrackingPlanStatT{
		numEvents:             numEvents,
		validateSuccessEvents: validateSuccessEvents,
		validateFailedEvents:  validateFailedEvents,
		validateEventTime:     validateTime,
		violationsByType:      violationsByType,
	}
}
