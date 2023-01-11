package transformationdebugger

import (
	"context"
	"fmt"
	"sync"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/processor/transformer"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/debugger"
	"github.com/rudderlabs/rudder-server/services/debugger/cache"
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
	Payload []*TransformStatusT `json:"payload"`
}

type Opt func(*Handle)

var jsonfast = jsoniter.ConfigCompatibleWithStandardLibrary

type Handle struct {
	configBackendURL               string
	started                        bool
	disableTransformationUploads   bool
	limitEventsInMemory            int
	uploader                       debugger.Uploader[*TransformStatusT]
	log                            logger.Logger
	transformationCacheMap         cache.Cache[TransformationStatusT]
	uploadEnabledTransformations   map[string]bool
	uploadEnabledTransformationsMu sync.RWMutex
	ctx                            context.Context
	cancel                         func()
	initialized                    chan struct{}
	done                           chan struct{}
}

var WithDisableTransformationStatusUploads = func(disableTransformationStatusUploads bool) func(h *Handle) {
	return func(h *Handle) {
		h.disableTransformationUploads = disableTransformationStatusUploads
	}
}

func NewHandle(opts ...Opt) *Handle {
	h := &Handle{
		configBackendURL: config.GetString("CONFIG_BACKEND_URL", "https://api.rudderstack.com"),
		log:              logger.NewLogger().Child("debugger").Child("transformation"),
	}

	config.RegisterBoolConfigVariable(false, &h.disableTransformationUploads, true, "TransformationDebugger.disableTransformationStatusUploads")
	config.RegisterIntConfigVariable(1, &h.limitEventsInMemory, true, 1, "TransformationDebugger.limitEventsInMemory")
	for _, opt := range opts {
		opt(h)
	}
	return h
}

var _instance = NewHandle()

func Start(backendConfig backendconfig.BackendConfig) {
	_instance.Start(backendConfig)
}

func Stop() {
	_instance.Stop()
}

func UploadTransformationStatus(tStatus *TransformationStatusT) {
	_instance.UploadTransformationStatus(tStatus)
}

type TransformationStatusUploader struct {
	log logger.Logger
}

func (h *Handle) IsUploadEnabled(id string) bool {
	h.uploadEnabledTransformationsMu.RLock()
	defer h.uploadEnabledTransformationsMu.RUnlock()
	_, ok := h.uploadEnabledTransformations[id]
	return ok
}

// Start initializes this module
func (h *Handle) Start(backendConfig backendconfig.BackendConfig) {
	url := fmt.Sprintf("%s/dataplane/eventTransformStatus", h.configBackendURL)
	transformationStatusUploader := &TransformationStatusUploader{}
	h.uploader = debugger.New[*TransformStatusT](url, transformationStatusUploader)
	h.uploader.Start()

	cacheType := cache.CacheType(config.GetInt("TransformationDebugger.cacheType", int(cache.BadgerCacheType)))
	h.transformationCacheMap = cache.New[TransformationStatusT](cacheType, "transformation", h.log)

	ctx, cancel := context.WithCancel(context.Background())
	h.ctx = ctx
	h.cancel = cancel
	h.initialized = make(chan struct{})
	h.done = make(chan struct{})

	rruntime.Go(func() {
		h.backendConfigSubscriber(backendConfig)
	})
	h.started = true
}

func (h *Handle) Stop() {
	if !h.started {
		return
	}
	h.cancel()
	<-h.done
	if h.transformationCacheMap != nil {
		_ = h.transformationCacheMap.Stop()
	}
	h.started = false
}

// RecordTransformationStatus is used to put the transform event in the eventBatchChannel,
// which will be processed by handleEvents.
func (h *Handle) RecordTransformationStatus(transformStatus *TransformStatusT) {
	// if disableTransformationUploads is true, return;
	if !h.started || h.disableTransformationUploads {
		return
	}

	h.uploader.RecordEvent(transformStatus)
}

func (t *TransformationStatusUploader) Transform(eventBuffer []*TransformStatusT) ([]byte, error) {
	uploadT := UploadT{Payload: eventBuffer}

	rawJSON, err := jsonfast.Marshal(uploadT)
	if err != nil {
		t.log.Errorf("[Transformation status uploader] Failed to marshal payload. Err: %v", err)
		return nil, err
	}

	return rawJSON, nil
}

func (h *Handle) updateConfig(config map[string]backendconfig.ConfigT) {
	uploadEnabledTransformations := make(map[string]bool)
	var uploadEnabledTransformationsIDs []string
	for _, wConfig := range config {
		for _, source := range wConfig.Sources {
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
	}
	h.uploadEnabledTransformationsMu.Lock()
	h.uploadEnabledTransformations = uploadEnabledTransformations
	h.uploadEnabledTransformationsMu.Unlock()

	h.recordHistoricTransformations(uploadEnabledTransformationsIDs)
}

func (h *Handle) backendConfigSubscriber(backendConfig backendconfig.BackendConfig) {
	ch := backendConfig.Subscribe(h.ctx, backendconfig.TopicProcessConfig)
	for c := range ch {
		h.updateConfig(c.Data.(map[string]backendconfig.ConfigT))
		select {
		case <-h.initialized:
		default:
			close(h.initialized)
		}
	}
	close(h.done)
}

func (h *Handle) UploadTransformationStatus(tStatus *TransformationStatusT) bool {
	defer func() {
		if r := recover(); r != nil {
			h.log.Error("Error occurred while uploading transformation statuses to config backend")
			h.log.Error(r)
		}
	}()

	// if disableTransformationUploads is true, return;
	if h.disableTransformationUploads {
		return false
	}

	for _, transformation := range tStatus.Destination.Transformations {
		if h.IsUploadEnabled(transformation.ID) {
			h.processRecordTransformationStatus(tStatus, transformation.ID)
		} else {
			tStatusUpdated := *tStatus
			lo.Slice(tStatusUpdated.UserTransformedEvents, 0, h.limitEventsInMemory+1)
			tStatusUpdated.Destination.Transformations = []backendconfig.TransformationT{transformation}
			tStatusUpdated.UserTransformedEvents = lo.Slice(tStatusUpdated.UserTransformedEvents, 0, h.limitEventsInMemory+1)
			tStatusUpdated.FailedEvents = lo.Slice(tStatusUpdated.FailedEvents, 0, h.limitEventsInMemory+1)
			h.transformationCacheMap.Update(transformation.ID, tStatusUpdated)
		}
	}
	return true
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

func (h *Handle) recordHistoricTransformations(tIDs []string) {
	h.uploadEnabledTransformationsMu.RLock()
	defer h.uploadEnabledTransformationsMu.RUnlock()
	for _, tID := range tIDs {
		tStatuses := h.transformationCacheMap.Read(tID)
		for _, tStatus := range tStatuses {
			h.processRecordTransformationStatus(&tStatus, tID)
		}
	}
}

func (h *Handle) processRecordTransformationStatus(tStatus *TransformationStatusT, tID string) {
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
		h.RecordTransformationStatus(&TransformStatusT{
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

				h.RecordTransformationStatus(&TransformStatusT{
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

			h.RecordTransformationStatus(&TransformStatusT{
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

			h.RecordTransformationStatus(&TransformStatusT{
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
