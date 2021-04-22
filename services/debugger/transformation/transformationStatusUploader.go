package transformationdebugger

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/processor/transformer"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/debugger"
	"github.com/rudderlabs/rudder-server/utils"
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

//TransformStatusT is a structure to hold transformation status
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

var uploader *debugger.Uploader

var (
	configBackendURL             string
	disableTransformationUploads bool
	pkgLogger                    logger.LoggerI
)

var uploadEnabledTransformations map[string]bool
var configSubscriberLock sync.RWMutex

func init() {
	loadConfig()
	pkgLogger = logger.NewLogger().Child("debugger").Child("transformation")
}

func loadConfig() {
	configBackendURL = config.GetEnv("CONFIG_BACKEND_URL", "https://api.rudderlabs.com")
	disableTransformationUploads = config.GetBool("TransformationDebugger.disableTransformationStatusUploads", false)
}

type TransformationStatusUploader struct {
}

func IsUploadEnabled(id string) bool {
	configSubscriberLock.RLock()
	defer configSubscriberLock.RUnlock()
	_, ok := uploadEnabledTransformations[id]
	return ok
}

//Setup initializes this module
func Setup() {
	url := fmt.Sprintf("%s/dataplane/eventTransformStatus", configBackendURL)
	transformationStatusUploader := &TransformationStatusUploader{}
	uploader = debugger.New(url, transformationStatusUploader)
	uploader.Setup()

	rruntime.Go(func() {
		backendConfigSubscriber()
	})
}

//RecordTransformationStatus is used to put the transform event in the eventBatchChannel,
//which will be processed by handleEvents.
func RecordTransformationStatus(transformStatus *TransformStatusT) bool {
	//if disableTransformationUploads is true, return;
	if disableTransformationUploads {
		return false
	}

	uploader.RecordEvent(transformStatus)
	return true
}

func (transformationStatusUploader *TransformationStatusUploader) Transform(data interface{}) ([]byte, error) {
	eventBuffer := data.([]interface{})
	uploadT := UploadT{Payload: eventBuffer}

	rawJSON, err := json.Marshal(uploadT)
	if err != nil {
		pkgLogger.Debugf(string(rawJSON))
		misc.AssertErrorIfDev(err)
		return nil, err
	}

	return rawJSON, nil
}

func updateConfig(sources backendconfig.ConfigT) {
	configSubscriberLock.Lock()
	uploadEnabledTransformations = make(map[string]bool)
	for _, source := range sources.Sources {
		for _, destination := range source.Destinations {
			for _, transformation := range destination.Transformations {
				eventTransform, ok := transformation.Config["eventTransform"].(bool)
				if ok && eventTransform {
					uploadEnabledTransformations[transformation.ID] = true
				}
			}
		}
	}
	configSubscriberLock.Unlock()
}

func backendConfigSubscriber() {
	configChannel := make(chan utils.DataEvent)
	backendconfig.Subscribe(configChannel, backendconfig.TopicProcessConfig)
	for {
		config := <-configChannel
		updateConfig(config.Data.(backendconfig.ConfigT))
	}
}

func UploadTransformationStatus(tStatus *TransformationStatusT) {
	defer func() {
		if r := recover(); r != nil {
			pkgLogger.Error("Error occured while uploading transformation statuses to config backend")
			pkgLogger.Error(r)
		}
	}()

	//if disableTransformationUploads is true, return;
	if disableTransformationUploads {
		return
	}

	for _, transformation := range tStatus.Destination.Transformations {
		if IsUploadEnabled(transformation.ID) {
			reportedMessageIDs := make(map[string]struct{})
			eventBeforeMap := make(map[string]*EventBeforeTransform)
			eventAfterMap := make(map[string]*EventsAfterTransform)
			for _, userTransformerEvent := range tStatus.UserTransformedEvents {
				if userTransformerEvent.Metadata.MessageID != "" {
					reportedMessageIDs[userTransformerEvent.Metadata.MessageID] = struct{}{}
					singularEventWithReceivedAt := tStatus.EventsByMessageID[userTransformerEvent.Metadata.MessageID]
					if _, ok := eventBeforeMap[userTransformerEvent.Metadata.MessageID]; !ok {
						eventBeforeMap[userTransformerEvent.Metadata.MessageID] = getEventBeforeTransform(singularEventWithReceivedAt.SingularEvent, singularEventWithReceivedAt.ReceivedAt)
					}

					if _, ok := eventAfterMap[userTransformerEvent.Metadata.MessageID]; !ok {
						eventAfterMap[userTransformerEvent.Metadata.MessageID] = getEventsAfterTransform(userTransformerEvent.Message, time.Now())
					} else {
						payloadArr := eventAfterMap[userTransformerEvent.Metadata.MessageID].EventPayloads
						payloadArr = append(payloadArr, getEventAfterTransform(userTransformerEvent.Message))
						eventAfterMap[userTransformerEvent.Metadata.MessageID].EventPayloads = payloadArr
					}
				}
			}

			for k := range eventBeforeMap {
				RecordTransformationStatus(&TransformStatusT{TransformationID: transformation.ID,
					SourceID:      tStatus.SourceID,
					DestinationID: tStatus.DestID,
					EventBefore:   eventBeforeMap[k],
					EventsAfter:   eventAfterMap[k],
					IsError:       false})
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

						RecordTransformationStatus(&TransformStatusT{TransformationID: transformation.ID,
							SourceID:      tStatus.SourceID,
							DestinationID: tStatus.DestID,
							EventBefore:   eventBefore,
							EventsAfter:   eventAfter,
							IsError:       true})
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

					RecordTransformationStatus(&TransformStatusT{TransformationID: transformation.ID,
						SourceID:      tStatus.SourceID,
						DestinationID: tStatus.DestID,
						EventBefore:   eventBefore,
						EventsAfter:   eventAfter,
						IsError:       true})
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

					RecordTransformationStatus(&TransformStatusT{TransformationID: transformation.ID,
						SourceID:      tStatus.SourceID,
						DestinationID: tStatus.DestID,
						EventBefore:   eventBefore,
						EventsAfter:   eventAfter,
						IsError:       false})
				}
			}
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
