package transformationdebugger

import (
	"context"
	"fmt"
	"sync"
	"time"

	jsoniter "github.com/json-iterator/go"

	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/processor/transformer"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/debugger"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/types"
)

type TransformationStatusT struct {
	SourceID              string
	DestID                string
	Destination           *backendconfig.DestinationT
	UserTransformedEvents []transformer.TransformerEventT
	EventsByMessageID     map[string]types.SingularEventWithReceivedAt
	FailedEvents          []transformer.TransformerResponseT
	UniqueMessageIds      map[string]struct{}
}

// TransformStatusT is a structure to hold transformation status
type TransformStatusT struct {
	TransformationID string                `json:"transformationId"`
	SourceID         string                `json:"sourceId"`
	DestinationID    string                `json:"destinationId"`
	EventBefore      *EventBeforeTransform `json:"eventBefore"`
	EventsAfter      *EventsAfterTransform `json:"eventsAfter"`
	IsError          bool                  `json:"error"`
}

type EventBeforeTransform struct {
	EventName  string               `json:"eventName"`
	EventType  string               `json:"eventType"`
	ReceivedAt string               `json:"receivedAt"`
	Payload    types.SingularEventT `json:"payload"`
}

type EventPayloadAfterTransform struct {
	EventName string               `json:"eventName"`
	EventType string               `json:"eventType"`
	Payload   types.SingularEventT `json:"payload"`
}

type EventsAfterTransform struct {
	ReceivedAt    string                        `json:"receivedAt"`
	IsDropped     bool                          `json:"isDropped"`
	Error         string                        `json:"error"`
	StatusCode    int                           `json:"statusCode"`
	EventPayloads []*EventPayloadAfterTransform `json:"payload"`
}

type UploadT struct {
	Payload []interface{} `json:"payload"`
}

var (
	jsonfast = jsoniter.ConfigCompatibleWithStandardLibrary

	configBackendURL             string
	disableTransformationUploads bool
	uploader                     debugger.UploaderI
	pkgLogger                    logger.LoggerI
	transformationCacheMap       debugger.Cache
)

var (
	uploadEnabledTransformations map[string]bool
	configSubscriberLock         sync.RWMutex
)

func Init() {
	loadConfig()
	pkgLogger = logger.NewLogger().Child("debugger").Child("transformation")
}

func loadConfig() {
	configBackendURL = config.GetEnv("CONFIG_BACKEND_URL", "https://api.rudderlabs.com")
	config.RegisterBoolConfigVariable(false, &disableTransformationUploads, true, "TransformationDebugger.disableTransformationStatusUploads")
}

type TransformationStatusUploader struct{}

func IsUploadEnabled(id string) bool {
	configSubscriberLock.RLock()
	defer configSubscriberLock.RUnlock()
	_, ok := uploadEnabledTransformations[id]
	return ok
}

// Setup initializes this module
func Setup() {
	url := fmt.Sprintf("%s/dataplane/eventTransformStatus", configBackendURL)
	transformationStatusUploader := &TransformationStatusUploader{}
	uploader = debugger.New(url, transformationStatusUploader)
	uploader.Start()

	rruntime.Go(func() {
		backendConfigSubscriber()
	})
}

// RecordTransformationStatus is used to put the transform event in the eventBatchChannel,
// which will be processed by handleEvents.
func RecordTransformationStatus(transformStatus *TransformStatusT) bool {
	// if disableTransformationUploads is true, return;
	if disableTransformationUploads {
		return false
	}

	uploader.RecordEvent(transformStatus)
	return true
}

func (transformationStatusUploader *TransformationStatusUploader) Transform(data interface{}) ([]byte, error) {
	eventBuffer := data.([]interface{})
	uploadT := UploadT{Payload: eventBuffer}

	rawJSON, err := jsonfast.Marshal(uploadT)
	if err != nil {
		pkgLogger.Errorf("[Transformation status uploader] Failed to marshal payload. Err: %v", err)
		return nil, err
	}

	return rawJSON, nil
}

func updateConfig(sources backendconfig.ConfigT) {
	configSubscriberLock.Lock()
	uploadEnabledTransformations = make(map[string]bool)
	var uploadEnabledTransformationsIDs []string
	for _, source := range sources.Sources {
		for _, destination := range source.Destinations {
			for _, transformation := range destination.Transformations {
				eventTransform, ok := transformation.Config["eventTransform"].(bool)
				if ok && eventTransform {
					uploadEnabledTransformations[transformation.ID] = true
					uploadEnabledTransformationsIDs = append(uploadEnabledTransformationsIDs, transformation.ID)
				}
			}
		}
	}
	recordHistoricTransformations(uploadEnabledTransformationsIDs)
	configSubscriberLock.Unlock()
}

func backendConfigSubscriber() {
	ch := backendconfig.DefaultBackendConfig.Subscribe(context.TODO(), backendconfig.TopicProcessConfig)
	for config := range ch {
		updateConfig(config.Data.(backendconfig.ConfigT))
	}
}

func UploadTransformationStatus(tStatus *TransformationStatusT) {
	defer func() {
		if r := recover(); r != nil {
			pkgLogger.Error("Error occurred while uploading transformation statuses to config backend")
			pkgLogger.Error(r)
		}
	}()

	// if disableTransformationUploads is true, return;
	if disableTransformationUploads {
		return
	}

	for _, transformation := range tStatus.Destination.Transformations {
		if IsUploadEnabled(transformation.ID) {
			processRecordTransformationStatus(tStatus, transformation.ID)
		} else {
			tStatusUpdated := *tStatus
			tStatusUpdated.Destination.Transformations = []backendconfig.TransformationT{transformation}
			tStatusUpdatedData, _ := jsonfast.Marshal(tStatusUpdated)
			transformationCacheMap.Update(transformation.ID, tStatusUpdatedData)
		}
	}
}

func getEventBeforeTransform(singularEvent types.SingularEventT, receivedAt time.Time) *EventBeforeTransform {
	eventType, _ := singularEvent["type"].(string)
	eventName, _ := singularEvent["event"].(string)
	if eventName == "" {
		eventName = eventType
	}

	return &EventBeforeTransform{
		EventName:  eventName,
		EventType:  eventType,
		ReceivedAt: receivedAt.Format(misc.RFC3339Milli),
		Payload:    singularEvent,
	}
}

func getEventAfterTransform(singularEvent types.SingularEventT) *EventPayloadAfterTransform {
	eventType, _ := singularEvent["type"].(string)
	eventName, _ := singularEvent["event"].(string)
	if eventName == "" {
		eventName = eventType
	}

	return &EventPayloadAfterTransform{
		EventName: eventName,
		EventType: eventType,
		Payload:   singularEvent,
	}
}

func getEventsAfterTransform(singularEvent types.SingularEventT, receivedAt time.Time) *EventsAfterTransform {
	return &EventsAfterTransform{
		ReceivedAt:    receivedAt.Format(misc.RFC3339Milli),
		StatusCode:    200,
		EventPayloads: []*EventPayloadAfterTransform{getEventAfterTransform(singularEvent)},
	}
}

func recordHistoricTransformations(tIDs []string) {
	for _, tID := range tIDs {
		tStatuses := transformationCacheMap.ReadAndPopData(tID)
		for _, tStatus := range tStatuses {
			var tStatusData TransformationStatusT
			if err := jsonfast.Unmarshal(tStatus, &tStatusData); err != nil {
				panic(err)
			}
			processRecordTransformationStatus(&tStatusData, tID)
		}
	}
}

func processRecordTransformationStatus(tStatus *TransformationStatusT, tID string) {
	reportedMessageIDs := make(map[string]struct{})
	eventBeforeMap := make(map[string]*EventBeforeTransform)
	eventAfterMap := make(map[string]*EventsAfterTransform)
	for i := range tStatus.UserTransformedEvents {
		metadata := tStatus.UserTransformedEvents[i].Metadata
		if metadata.MessageID != "" {
			reportedMessageIDs[metadata.MessageID] = struct{}{}
			singularEventWithReceivedAt := tStatus.EventsByMessageID[metadata.MessageID]
			if _, ok := eventBeforeMap[metadata.MessageID]; !ok {
				eventBeforeMap[metadata.MessageID] = getEventBeforeTransform(singularEventWithReceivedAt.SingularEvent, singularEventWithReceivedAt.ReceivedAt)
			}

			if _, ok := eventAfterMap[metadata.MessageID]; !ok {
				eventAfterMap[metadata.MessageID] = getEventsAfterTransform(tStatus.UserTransformedEvents[i].Message, time.Now())
			} else {
				payloadArr := eventAfterMap[metadata.MessageID].EventPayloads
				payloadArr = append(payloadArr, getEventAfterTransform(tStatus.UserTransformedEvents[i].Message))
				eventAfterMap[metadata.MessageID].EventPayloads = payloadArr
			}
		}
	}

	for k := range eventBeforeMap {
		RecordTransformationStatus(&TransformStatusT{
			TransformationID: tID,
			SourceID:         tStatus.SourceID,
			DestinationID:    tStatus.DestID,
			EventBefore:      eventBeforeMap[k],
			EventsAfter:      eventAfterMap[k],
			IsError:          false,
		})
	}

	for _, failedEvent := range tStatus.FailedEvents {
		if len(failedEvent.Metadata.MessageIDs) > 0 {
			for _, msgID := range failedEvent.Metadata.MessageIDs {
				reportedMessageIDs[msgID] = struct{}{}
				singularEventWithReceivedAt := tStatus.EventsByMessageID[msgID]
				eventBefore := getEventBeforeTransform(singularEventWithReceivedAt.SingularEvent, singularEventWithReceivedAt.ReceivedAt)
				eventAfter := &EventsAfterTransform{
					Error:      failedEvent.Error,
					ReceivedAt: time.Now().Format(misc.RFC3339Milli),
					StatusCode: failedEvent.StatusCode,
				}

				RecordTransformationStatus(&TransformStatusT{
					TransformationID: tID,
					SourceID:         tStatus.SourceID,
					DestinationID:    tStatus.DestID,
					EventBefore:      eventBefore,
					EventsAfter:      eventAfter,
					IsError:          true,
				})
			}
		} else if failedEvent.Metadata.MessageID != "" {
			reportedMessageIDs[failedEvent.Metadata.MessageID] = struct{}{}
			singularEventWithReceivedAt := tStatus.EventsByMessageID[failedEvent.Metadata.MessageID]
			eventBefore := getEventBeforeTransform(singularEventWithReceivedAt.SingularEvent, singularEventWithReceivedAt.ReceivedAt)
			eventAfter := &EventsAfterTransform{
				Error:      failedEvent.Error,
				ReceivedAt: time.Now().Format(misc.RFC3339Milli),
				StatusCode: failedEvent.StatusCode,
			}

			RecordTransformationStatus(&TransformStatusT{
				TransformationID: tID,
				SourceID:         tStatus.SourceID,
				DestinationID:    tStatus.DestID,
				EventBefore:      eventBefore,
				EventsAfter:      eventAfter,
				IsError:          true,
			})
		}
	}

	for msgID := range tStatus.UniqueMessageIds {
		if _, ok := reportedMessageIDs[msgID]; !ok {
			singularEventWithReceivedAt := tStatus.EventsByMessageID[msgID]
			eventBefore := getEventBeforeTransform(singularEventWithReceivedAt.SingularEvent, singularEventWithReceivedAt.ReceivedAt)
			eventAfter := &EventsAfterTransform{
				ReceivedAt: time.Now().Format(misc.RFC3339Milli),
				IsDropped:  true,
			}

			RecordTransformationStatus(&TransformStatusT{
				TransformationID: tID,
				SourceID:         tStatus.SourceID,
				DestinationID:    tStatus.DestID,
				EventBefore:      eventBefore,
				EventsAfter:      eventAfter,
				IsError:          false,
			})
		}
	}
}
