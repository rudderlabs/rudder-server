package transformationdebugger

import (
	"context"
	"fmt"
	"sync"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/processor/transformer"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/debugger"
	"github.com/rudderlabs/rudder-server/services/debugger/cache"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/types"
)

type TransformationStatusT struct {
	SourceID              string
	DestID                string
	Destination           *backendconfig.DestinationT
	UserTransformedEvents []transformer.TransformerEvent
	EventsByMessageID     map[string]types.SingularEventWithReceivedAt
	FailedEvents          []transformer.TransformerResponse
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

var jsonfast = jsoniter.ConfigCompatibleWithStandardLibrary

type Handle struct {
	configBackendURL               string
	started                        bool
	disableTransformationUploads   config.ValueLoader[bool]
	limitEventsInMemory            config.ValueLoader[int]
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

type TransformationDebugger interface {
	IsUploadEnabled(id string) bool
	RecordTransformationStatus(transformStatus *TransformStatusT)
	UploadTransformationStatus(tStatus *TransformationStatusT) bool
	Stop()
}

func NewHandle(backendConfig backendconfig.BackendConfig) (TransformationDebugger, error) {
	h := &Handle{
		configBackendURL: config.GetString("CONFIG_BACKEND_URL", "https://api.rudderstack.com"),
		log:              logger.NewLogger().Child("debugger").Child("transformation"),
		disableTransformationUploads: config.GetReloadableBoolVar(
			false, "TransformationDebugger.disableTransformationStatusUploads",
		),
		limitEventsInMemory: config.GetReloadableIntVar(1, 1, "TransformationDebugger.limitEventsInMemory"),
	}

	var (
		err                          error
		url                          = fmt.Sprintf("%s/dataplane/eventTransformStatus", h.configBackendURL)
		transformationStatusUploader = &TransformationStatusUploader{}
		cacheType                    = cache.CacheType(config.GetInt(
			"TransformationDebugger.cacheType", int(cache.MemoryCacheType),
		))
	)

	h.transformationCacheMap, err = cache.New[TransformationStatusT](cacheType, "transformation", h.log)
	if err != nil {
		return nil, err
	}

	h.uploader = debugger.New[*TransformStatusT](url, backendConfig.Identity(), transformationStatusUploader)
	h.uploader.Start()

	h.start(backendConfig)
	return h, nil
}

type TransformationStatusUploader struct {
	log logger.Logger
}

func (h *Handle) IsUploadEnabled(id string) bool {
	<-h.initialized
	h.uploadEnabledTransformationsMu.RLock()
	defer h.uploadEnabledTransformationsMu.RUnlock()
	_, ok := h.uploadEnabledTransformations[id]
	return ok
}

// Start initializes this module
func (h *Handle) start(backendConfig backendconfig.BackendConfig) {
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
	h.uploader.Stop()
	h.started = false
}

// RecordTransformationStatus is used to put the transform event in the eventBatchChannel,
// which will be processed by handleEvents.
func (h *Handle) RecordTransformationStatus(transformStatus *TransformStatusT) {
	// if disableTransformationUploads is true, return;
	if !h.started || h.disableTransformationUploads.Load() {
		return
	}
	<-h.initialized

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

// limit the number of stored events
func (ts *TransformationStatusT) Limit(
	limit int,
	transformation backendconfig.TransformationT,
) *TransformationStatusT {
	ts.Destination.Transformations = []backendconfig.TransformationT{transformation}
	ts.UserTransformedEvents = lo.Slice(ts.UserTransformedEvents, 0, limit)
	ts.FailedEvents = lo.Slice(ts.FailedEvents, 0, limit)
	messageIDs := lo.SliceToMap(
		append(
			lo.Map(
				ts.UserTransformedEvents,
				func(event transformer.TransformerEvent, _ int) string {
					return event.Metadata.MessageID
				},
			),
			lo.Map(
				ts.FailedEvents,
				func(event transformer.TransformerResponse, _ int) string {
					return event.Metadata.MessageID
				},
			)...,
		),
		func(messageID string) (string, struct{}) {
			return messageID, struct{}{}
		},
	)
	ts.UniqueMessageIds = messageIDs
	ts.EventsByMessageID = lo.PickByKeys(
		ts.EventsByMessageID,
		lo.Keys(messageIDs),
	)
	return ts
}

func (h *Handle) UploadTransformationStatus(tStatus *TransformationStatusT) bool {
	defer func() {
		if r := recover(); r != nil {
			h.log.Error("Error occurred while uploading transformation statuses to config backend")
			h.log.Error(r)
		}
	}()

	// if disableTransformationUploads is true, return;
	if h.disableTransformationUploads.Load() {
		return false
	}
	<-h.initialized

	for _, transformation := range tStatus.Destination.Transformations {
		if h.IsUploadEnabled(transformation.ID) {
			h.processRecordTransformationStatus(tStatus, transformation.ID)
		} else {
			err := h.transformationCacheMap.Update(
				transformation.ID,
				*(tStatus.Limit(h.limitEventsInMemory.Load()+1, transformation)),
			)
			if err != nil {
				h.log.Errorf("Error while updating transformation cache: %v", err)
				return false
			}
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
	for _, tID := range tIDs {
		tStatuses, err := h.transformationCacheMap.Read(tID)
		if err != nil {
			continue
		}
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
		messageIDs := failedEvent.Metadata.GetMessagesIDs()
		for _, msgID := range messageIDs {
			if msgID != "" {
				reportedMessageIDs[msgID] = struct{}{}
				singularEventWithReceivedAt := tStatus.EventsByMessageID[msgID]
				eventBefore := getEventBeforeTransform(singularEventWithReceivedAt.SingularEvent, singularEventWithReceivedAt.ReceivedAt)
				eventAfter := &EventsAfterTransform{
					ReceivedAt: time.Now().Format(misc.RFC3339Milli),
					StatusCode: failedEvent.StatusCode,
				}
				var isError bool
				switch failedEvent.StatusCode {
				case types.FilterEventCode:
					eventAfter.IsDropped = true
					isError = false
				default:
					eventAfter.Error = failedEvent.Error
					isError = true
				}

				h.RecordTransformationStatus(&TransformStatusT{
					TransformationID: tID,
					SourceID:         tStatus.SourceID,
					DestinationID:    tStatus.DestID,
					EventBefore:      eventBefore,
					EventsAfter:      eventAfter,
					IsError:          isError,
				})
			}
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

func NewNoOpService() TransformationDebugger {
	return &noopService{}
}

type noopService struct{}

func (*noopService) Start(_ backendconfig.BackendConfig) {
}

func (*noopService) IsUploadEnabled(_ string) bool {
	return false
}

func (*noopService) RecordTransformationStatus(_ *TransformStatusT) {
}

func (*noopService) UploadTransformationStatus(_ *TransformationStatusT) bool {
	return false
}

func (*noopService) Stop() {
}
