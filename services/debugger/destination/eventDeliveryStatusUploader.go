//go:generate mockgen -destination=../../../mocks/services/debugger/destination/eventDeliveryStatusUploader.go -package mock_destinationdebugger github.com/rudderlabs/rudder-server/services/debugger/destination/ DestinationDebugger

package destinationdebugger

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/debugger"
	"github.com/rudderlabs/rudder-server/services/debugger/cache"
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

type DestinationDebugger interface {
	RecordEventDeliveryStatus(destinationID string, deliveryStatus *DeliveryStatusT) bool
	HasUploadEnabled(destID string) bool
	Stop()
}

type Handle struct {
	configBackendURL                  string
	log                               logger.Logger
	started                           bool
	disableEventDeliveryStatusUploads config.ValueLoader[bool]
	eventsDeliveryCache               cache.Cache[*DeliveryStatusT]
	uploader                          debugger.Uploader[*DeliveryStatusT]
	uploadEnabledDestinationIDs       map[string]bool
	uploadEnabledDestinationIDsMu     sync.RWMutex
	ctx                               context.Context
	cancel                            func()
	initialized                       chan struct{}
	done                              chan struct{}
}

func NewHandle(backendConfig backendconfig.BackendConfig) (DestinationDebugger, error) {
	h := &Handle{
		log:              logger.NewLogger().Child("debugger").Child("destination"),
		configBackendURL: config.GetString("CONFIG_BACKEND_URL", "https://api.rudderstack.com"),
		disableEventDeliveryStatusUploads: config.GetReloadableBoolVar(
			false, "DestinationDebugger.disableEventDeliveryStatusUploads",
		),
	}
	var err error
	url := fmt.Sprintf("%s/dataplane/v2/eventDeliveryStatus", h.configBackendURL)
	eventUploader := NewEventDeliveryStatusUploader(h.log)
	h.uploader = debugger.New[*DeliveryStatusT](url, backendConfig.Identity(), eventUploader)
	h.uploader.Start()

	cacheType := cache.CacheType(config.GetInt("DestinationDebugger.cacheType", int(cache.MemoryCacheType)))
	h.eventsDeliveryCache, err = cache.New[*DeliveryStatusT](cacheType, "destination", h.log)
	if err != nil {
		return nil, err
	}

	h.start(backendConfig)
	return h, nil
}

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
	if h.eventsDeliveryCache != nil {
		_ = h.eventsDeliveryCache.Stop()
	}
	h.uploader.Stop()
	h.started = false
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
	if !h.started || h.disableEventDeliveryStatusUploads.Load() {
		return false
	}
	<-h.initialized
	// Check if destinationID part of enabled destinations, if not then push the job in cache to keep track
	if !h.HasUploadEnabled(destinationID) {
		err := h.eventsDeliveryCache.Update(destinationID, deliveryStatus)
		if err != nil {
			h.log.Errorf("DestinationDebugger: Error while updating cache: %v", err)
		}
		return false
	}

	h.uploader.RecordEvent(deliveryStatus)
	return true
}

func (h *Handle) HasUploadEnabled(destID string) bool {
	<-h.initialized
	h.uploadEnabledDestinationIDsMu.RLock()
	defer h.uploadEnabledDestinationIDsMu.RUnlock()
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
	h.uploadEnabledDestinationIDsMu.Lock()
	h.uploadEnabledDestinationIDs = uploadEnabledDestinationIDs
	h.uploadEnabledDestinationIDsMu.Unlock()

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
	for _, destinationID := range destinationIDs {
		historicEventsDelivery, err := h.eventsDeliveryCache.Read(destinationID)
		if err != nil {
			continue
		}
		for _, event := range historicEventsDelivery {
			h.uploader.RecordEvent(event)
		}
	}
}

func NewNoOpService() DestinationDebugger {
	return &noopService{}
}

type noopService struct{}

func (*noopService) RecordEventDeliveryStatus(_ string, _ *DeliveryStatusT) bool {
	return false
}

func (*noopService) HasUploadEnabled(_ string) bool {
	return false
}

func (*noopService) Start(_ backendconfig.BackendConfig) {
}

func (*noopService) Stop() {
}
