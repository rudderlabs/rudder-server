package sourcedebugger

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/debugger"
	"github.com/rudderlabs/rudder-server/services/debugger/cache"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

// GatewayEventBatchT is a structure to hold batch of events
type GatewayEventBatchT struct {
	WriteKey   string
	EventBatch []byte
}

// EventUploadT is a structure to hold actual event data
type EventUploadT map[string]interface{}

// EventUploadBatchT is a structure to hold batch of events
type EventUploadBatchT struct {
	WriteKey   string
	ReceivedAt string
	Batch      []EventUploadT
}

type Opt func(*Handle)

var WithDisableEventUploads = func(disableEventUploads bool) func(h *Handle) {
	return func(h *Handle) {
		h.disableEventUploads = disableEventUploads
	}
}

type Handle struct {
	uploader            debugger.Uploader[*GatewayEventBatchT]
	configBackendURL    string
	disableEventUploads bool
	log                 logger.Logger
	eventsCache         cache.Cache[[]byte]

	uploadEnabledWriteKeysMu sync.RWMutex
	uploadEnabledWriteKeys   []string

	ctx         context.Context
	cancel      func()
	initialized chan struct{}
	done        chan struct{}
}

func NewHandle(opts ...Opt) *Handle {
	h := &Handle{
		configBackendURL: config.GetString("CONFIG_BACKEND_URL", "https://api.rudderstack.com"),
		log:              logger.NewLogger().Child("debugger").Child("source"),
	}
	ctx, cancel := context.WithCancel(context.Background())
	h.ctx = ctx
	h.cancel = cancel
	h.done = make(chan struct{})

	cacheType := cache.CacheType(config.GetInt("SourceDebugger.cacheType", int(cache.BadgerCacheType)))
	h.eventsCache = cache.New[[]byte](cacheType, "source", h.log)
	config.RegisterBoolConfigVariable(false, &h.disableEventUploads, true, "SourceDebugger.disableEventUploads")
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

func RecordEvent(writeKey string, eventBatch []byte) bool {
	return _instance.RecordEvent(writeKey, eventBatch)
}

// Start initializes this module
func (h *Handle) Start(backendConfig backendconfig.BackendConfig) {
	url := fmt.Sprintf("%s/dataplane/v2/eventUploads", h.configBackendURL)
	eventUploader := NewEventUploader(h.log)
	h.uploader = debugger.New[*GatewayEventBatchT](url, eventUploader)
	h.uploader.Start()

	rruntime.Go(func() {
		h.backendConfigSubscriber(backendConfig)
	})
}

func (h *Handle) Stop() {
	h.cancel()
	<-h.done
	if h.eventsCache != nil {
		_ = h.eventsCache.Stop()
	}
}

// RecordEvent is used to put the event batch in the eventBatchChannel,
// which will be processed by handleEvents.
func (h *Handle) RecordEvent(writeKey string, eventBatch []byte) bool {
	// if disableEventUploads is true, return;
	if h.disableEventUploads {
		return false
	}
	<-h.initialized
	// Check if writeKey part of enabled sources
	h.uploadEnabledWriteKeysMu.RLock()
	defer h.uploadEnabledWriteKeysMu.RUnlock()
	if !misc.Contains(h.uploadEnabledWriteKeys, writeKey) {
		h.eventsCache.Update(writeKey, eventBatch)
		return false
	}
	h.uploader.RecordEvent(&GatewayEventBatchT{writeKey, eventBatch})
	return true
}

func (h *Handle) updateConfig(config map[string]backendconfig.ConfigT) {
	uploadEnabledWriteKeys := []string{}
	for _, wConfig := range config {
		for _, source := range wConfig.Sources {
			if source.Config != nil {
				if source.Enabled && source.Config["eventUpload"] == true {
					uploadEnabledWriteKeys = append(uploadEnabledWriteKeys, source.WriteKey)
				}
			}
		}
	}
	h.uploadEnabledWriteKeysMu.Lock()
	h.uploadEnabledWriteKeys = uploadEnabledWriteKeys
	h.uploadEnabledWriteKeysMu.Unlock()
	h.recordHistoricEvents()
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

// recordHistoricEvents sends the events collected in cache as live events.
// This is called on config update.
// IMP: The function must be called before releasing configSubscriberLock lock to ensure the order of RecordEvent call
func (h *Handle) recordHistoricEvents() {
	h.uploadEnabledWriteKeysMu.RLock()
	defer h.uploadEnabledWriteKeysMu.RUnlock()
	for _, writeKey := range h.uploadEnabledWriteKeys {
		historicEvents := h.eventsCache.Read(writeKey)
		for _, eventBatchData := range historicEvents {
			h.uploader.RecordEvent(&GatewayEventBatchT{writeKey, eventBatchData})
		}
	}
}

type EventUploader struct {
	log logger.Logger
}

func NewEventUploader(log logger.Logger) *EventUploader {
	return &EventUploader{log: log}
}

func (e *EventUploader) Transform(eventBuffer []*GatewayEventBatchT) ([]byte, error) {
	res := make(map[string]interface{})
	res["version"] = "v2"
	for _, event := range eventBuffer {
		var batchedEvent EventUploadBatchT
		err := json.Unmarshal(event.EventBatch, &batchedEvent)
		if err != nil {
			e.log.Errorf("[Source live events] Failed to unmarshal. Err: %v", err)
			continue
		}

		receivedAtTS, err := time.Parse(time.RFC3339, batchedEvent.ReceivedAt)
		if err != nil {
			receivedAtTS = time.Now()
		}
		receivedAtStr := receivedAtTS.Format(misc.RFC3339Milli)

		var arr []EventUploadT
		if value, ok := res[batchedEvent.WriteKey]; ok {
			arr, _ = value.([]EventUploadT)
		}

		for _, ev := range batchedEvent.Batch {
			// add the receivedAt time to each event
			event := map[string]interface{}{
				"payload":       ev,
				"receivedAt":    receivedAtStr,
				"eventName":     misc.GetStringifiedData(ev["event"]),
				"eventType":     misc.GetStringifiedData(ev["type"]),
				"errorResponse": make(map[string]interface{}),
				"errorCode":     200,
			}
			arr = append(arr, event)
		}

		res[batchedEvent.WriteKey] = arr
	}

	rawJSON, err := json.Marshal(res)
	if err != nil {
		e.log.Errorf("[Source live events] Failed to marshal payload. Err: %v", err)
		return nil, err
	}

	return rawJSON, nil
}
