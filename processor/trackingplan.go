package processor

import (
	"encoding/json"
	"fmt"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/processor/integrations"
	"github.com/rudderlabs/rudder-server/processor/transformer"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/types"
	uuid "github.com/satori/go.uuid"
	"strconv"
	"time"
)

// extracts the events from Validation response and make them transformer ready
func (proc *HandleT) GetTransformerEventsFromValidationResponse(response transformer.ResponseT, eventsByMessageID map[string]types.SingularEventWithReceivedAt) ([]transformer.TransformerEventT, []*jobsdb.JobT) {
	var eventsToTransform []transformer.TransformerEventT
	var eventsToDrop []transformer.TransformerResponseT
	var failedEventsToStore []*jobsdb.JobT

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
		dropEventViolationType := "None"
		for k, v := range mergedTpConfig {
			value := fmt.Sprint(v)
			switch k {
			case "allowUnplannedEvents":
				_, exists := violationsByType[transformer.UnplannedEvent]
				if value == "false" && exists {
					dropEvent = true
					dropEventViolationType = transformer.UnplannedEvent
					break
				}
				if !(value == "true" || value == "false") {
					pkgLogger.Errorf("Unknown option %s in %s", value, k)
				}
			case "unplannedProperties":
				_, exists := violationsByType[transformer.AdditionalProperties]
				if value == "drop" && exists {
					dropEvent = true
					dropEventViolationType = transformer.AdditionalProperties
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
						dropEventViolationType = transformer.UnknownViolation
					} else if exists1 {
						dropEventViolationType = transformer.DatatypeMismatch
					} else {
						dropEventViolationType = transformer.RequiredMissing
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

			tags := BuildTrackingPlanStatTags(&validatedEvent.Metadata)
			tags["statusCode"] = strconv.Itoa(400)
			tags["violationType"] = dropEventViolationType

			validateEventsOutputStat := proc.stats.NewTaggedStat("processor.validate_events_output", stats.CountType, tags)
			validateEventsOutputStat.Count(1)
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
	return eventsToTransform, failedEventsToStore
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

func (proc *HandleT) validateEvents(groupedEventsByWriteKey map[WriteKeyT][]transformer.TransformerEventT, eventsByMessageID map[string]types.SingularEventWithReceivedAt) map[WriteKeyT][]transformer.TransformerEventT {
	//validating with the tp here for every writeKey
	var validatedEventsByWriteKey = make(map[WriteKeyT][]transformer.TransformerEventT)
	for writeKey, eventList := range groupedEventsByWriteKey {
		isTpExists := eventList[0].Metadata.TrackingPlanId != ""
		if !isTpExists {
			// pass on the jobs for transformation(User,Dest)
			validatedEventsByWriteKey[writeKey] = make([]transformer.TransformerEventT, 0)
			validatedEventsByWriteKey[writeKey] = append(validatedEventsByWriteKey[writeKey], eventList...)
			continue
		}
		transformerValidateEventsTime := proc.stats.NewStat("transformer.validate_events_time", stats.TimerType)
		transformerValidateEventsTime.Start()
		response := proc.transformer.Validate(eventList, integrations.GetTrackingPlanValidationURL(), userTransformBatchSize)
		transformerValidateEventsTime.End()

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

			tags := BuildTrackingPlanStatTags(&event.Metadata)
			tags["statusCode"] = strconv.Itoa(400)
			tags["violationType"] = "None"

			validateEventsOutputStat := proc.stats.NewTaggedStat("processor.validate_events_output", stats.CountType, tags)
			validateEventsOutputStat.Count(1)
		}
		response.FailedEvents = filteredFailedEvents

		eventsToTransform, validationFailedJobs := proc.GetTransformerEventsFromValidationResponse(response, eventsByMessageID)
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

		if len(eventsToTransform) == 0 {
			continue
		} else {
			validatedEventsByWriteKey[writeKey] = make([]transformer.TransformerEventT, 0)
			validatedEventsByWriteKey[writeKey] = append(validatedEventsByWriteKey[writeKey], eventsToTransform...)
		}
	}
	return validatedEventsByWriteKey
}

func BuildTrackingPlanStatTags(metadata *transformer.MetadataT) map[string]string {
	return map[string]string{
		"destination":         metadata.DestinationID,
		"destType":            metadata.DestinationType,
		"source":              metadata.SourceID,
		"workspaceId":         metadata.WorkspaceID,
		"trackingPlanId":      metadata.TrackingPlanId,
		"trackingPlanVersion": strconv.Itoa(metadata.TrackingPlanVersion),
	}
}
