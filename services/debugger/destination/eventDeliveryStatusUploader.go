package destinationdebugger

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/debugger"
	"github.com/rudderlabs/rudder-server/services/debugger/cache"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

// DeliveryStatusT is a structure to hold everything related to event delivery
type DeliveryStatusT struct {
	DestinationID string          `json:"destinationId"`
	SourceID      string          `json:"sourceId"`
	Payload       json.RawMessage `json:"payload"`
	AttemptNum    int             `json:"attemptNum"`
	JobState      string          `json:"jobState"`
	ErrorCode     string          `json:"errorCode"`
	ErrorResponse json.RawMessage `json:"errorResponse"`
	SentAt        string          `json:"sentAt"`
	EventName     string          `json:"eventName"`
	EventType     string          `json:"eventType"`
}

type Opt func(*Handle)

type Handle struct {
	configBackendURL                  string
	log                               logger.Logger
	disableEventDeliveryStatusUploads bool
	eventsDeliveryCache               cache.Cache[*DeliveryStatusT]
	uploader                          debugger.Uploader[*DeliveryStatusT]
	uploadEnabledDestinationIDs       map[string]bool
	configSubscriberLock              sync.RWMutex
	ctx                               context.Context
	cancel                            func()
	initialized                       chan struct{}
	done                              chan struct{}
}

var WithDisableEventUploads = func(disableEventDeliveryStatusUploads bool) func(h *Handle) {
	return func(h *Handle) {
		h.disableEventDeliveryStatusUploads = disableEventDeliveryStatusUploads
	}
}

func NewHandle(opts ...Opt) *Handle {
	h := &Handle{
		configBackendURL: config.GetString("CONFIG_BACKEND_URL", "https://api.rudderstack.com"),
		log:              logger.NewLogger().Child("debugger").Child("destination"),
	}
	ctx, cancel := context.WithCancel(context.Background())
	h.ctx = ctx
	h.cancel = cancel
	h.done = make(chan struct{})

	cacheType := cache.CacheType(config.GetInt("DestinationDebugger.cacheType", int(cache.BadgerCacheType)))
	h.eventsDeliveryCache = cache.New[*DeliveryStatusT](cacheType, "destination", h.log)
	config.RegisterBoolConfigVariable(false, &h.disableEventDeliveryStatusUploads, true, "DestinationDebugger.disableEventDeliveryStatusUploads")
	h.initialized = make(chan struct{})
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

func RecordEventDeliveryStatus(destinationID string, deliveryStatus *DeliveryStatusT) bool {
	return _instance.RecordEventDeliveryStatus(destinationID, deliveryStatus)
}

func HasUploadEnabled(destID string) bool {
	return _instance.HasUploadEnabled(destID)
}

func (h *Handle) Start(backendConfig backendconfig.BackendConfig) {
	url := fmt.Sprintf("%s/dataplane/v2/eventUploads", h.configBackendURL)
	eventUploader := NewEventDeliveryStatusUploader(h.log)
	h.uploader = debugger.New[*DeliveryStatusT](url, eventUploader)
	h.uploader.Start()

	rruntime.Go(func() {
		h.backendConfigSubscriber(backendConfig)
	})
}

func (h *Handle) Stop() {
	h.cancel()
	<-h.done
	if h.eventsDeliveryCache != nil {
		_ = h.eventsDeliveryCache.Stop()
	}
}

func NewEventDeliveryStatusUploader(log logger.Logger) *EventDeliveryStatusUploader {
	return &EventDeliveryStatusUploader{log: log}
}

type EventDeliveryStatusUploader struct {
	log logger.Logger
}

// RecordEventDeliveryStatus is used to put the delivery status in the deliveryStatusesBatchChannel,
// which will be processed by handleJobs.
func (h *Handle) RecordEventDeliveryStatus(destinationID string, deliveryStatus *DeliveryStatusT) bool {
	// if disableEventDeliveryStatusUploads is true, return;
	if h.disableEventDeliveryStatusUploads {
		return false
	}
	<-h.initialized
	// Check if destinationID part of enabled destinations, if not then push the job in cache to keep track
	h.configSubscriberLock.RLock()
	defer h.configSubscriberLock.RUnlock()
	if !h.HasUploadEnabled(destinationID) {
		h.eventsDeliveryCache.Update(destinationID, deliveryStatus)
		return false
	}

	h.uploader.RecordEvent(deliveryStatus)
	return true
}

func (h *Handle) HasUploadEnabled(destID string) bool {
	h.configSubscriberLock.RLock()
	defer h.configSubscriberLock.RUnlock()
	_, ok := h.uploadEnabledDestinationIDs[destID]
	return ok
}

func (e *EventDeliveryStatusUploader) Transform(deliveryStatusesBuffer []*DeliveryStatusT) ([]byte, error) {
	res := make(map[string]interface{})
	res["version"] = "v2"
	for _, job := range deliveryStatusesBuffer {
		var arr []*DeliveryStatusT
		if value, ok := res[job.DestinationID]; ok {
			arr, _ = value.([]*DeliveryStatusT)
		}
		arr = append(arr, job)
		res[job.DestinationID] = arr
	}

	rawJSON, err := json.Marshal(res)
	if err != nil {
		e.log.Errorf("[Destination live events] Failed to marshal payload. Err: %v", err)
		return nil, err
	}

	return rawJSON, nil
}

func (h *Handle) updateConfig(config map[string]backendconfig.ConfigT) {
	uploadEnabledDestinationIDs := make(map[string]bool)
	var uploadEnabledDestinationIdsList []string
	for _, wConfig := range config {
		for _, source := range wConfig.Sources {
			for _, destination := range source.Destinations {
				if destination.Config != nil {
					if destination.Enabled && destination.Config["eventDelivery"] == true {
						uploadEnabledDestinationIdsList = append(uploadEnabledDestinationIdsList, destination.ID)
						uploadEnabledDestinationIDs[destination.ID] = true
					}
				}
			}
		}
	}
	h.configSubscriberLock.Lock()
	h.uploadEnabledDestinationIDs = uploadEnabledDestinationIDs
	h.configSubscriberLock.Unlock()

	h.recordHistoricEventsDelivery(uploadEnabledDestinationIdsList)
}

func (h *Handle) backendConfigSubscriber(backendConfig backendconfig.BackendConfig) {
	configChannel := backendConfig.Subscribe(h.ctx, "backendConfig")
	for c := range configChannel {
		h.updateConfig(c.Data.(map[string]backendconfig.ConfigT))
		select {
		case <-h.initialized:
		default:
			close(h.initialized)
		}
	}
	close(h.done)
}

func (h *Handle) recordHistoricEventsDelivery(destinationIDs []string) {
	h.configSubscriberLock.RLock()
	defer h.configSubscriberLock.RUnlock()
	for _, destinationID := range destinationIDs {
		historicEventsDelivery := h.eventsDeliveryCache.Read(destinationID)
		for _, event := range historicEventsDelivery {
			h.uploader.RecordEvent(event)
		}
	}
}
