package sourcedebugger

//go:generate mockgen -destination=./mocks/mock.go -package=mocks github.com/rudderlabs/rudder-server/services/debugger/source SourceDebugger
import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stringify"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/debugger"
	"github.com/rudderlabs/rudder-server/services/debugger/cache"
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

type SourceDebugger interface {
	RecordEvent(writeKey string, eventBatch []byte) bool
	Stop()
}

type Handle struct {
	started             bool
	uploader            debugger.Uploader[*GatewayEventBatchT]
	configBackendURL    string
	disableEventUploads config.ValueLoader[bool]
	log                 logger.Logger
	eventsCache         cache.Cache[[]byte]

	uploadEnabledWriteKeysMu sync.RWMutex
	uploadEnabledWriteKeys   []string

	ctx         context.Context
	cancel      func()
	initialized chan struct{}
	done        chan struct{}
}

func NewHandle(backendConfig backendconfig.BackendConfig) (SourceDebugger, error) {
	h := &Handle{
		configBackendURL: config.GetString("CONFIG_BACKEND_URL", "https://api.rudderstack.com"),
		log:              logger.NewLogger().Child("debugger").Child("source"),
	}
	var err error
	h.disableEventUploads = config.GetReloadableBoolVar(false, "SourceDebugger.disableEventUploads")
	url := fmt.Sprintf("%s/dataplane/v2/eventUploads", h.configBackendURL)
	eventUploader := NewEventUploader(h.log)
	h.uploader = debugger.New[*GatewayEventBatchT](url, backendConfig.Identity(), eventUploader)
	h.uploader.Start()

	cacheType := cache.CacheType(config.GetInt("SourceDebugger.cacheType", int(cache.MemoryCacheType)))
	h.eventsCache, err = cache.New[[]byte](cacheType, "source", h.log)
	if err != nil {
		return nil, err
	}

	h.start(backendConfig)
	return h, nil
}

// Start initializes this module
func (h *Handle) start(backendConfig backendconfig.BackendConfig) {
	ctx, cancel := context.WithCancel(context.Background())
	h.ctx = ctx
	h.cancel = cancel
	h.done = make(chan struct{})
	h.initialized = make(chan struct{})
	h.started = true
	rruntime.Go(func() {
		h.backendConfigSubscriber(backendConfig)
	})
}

func (h *Handle) Stop() {
	if !h.started {
		return
	}
	h.cancel()
	<-h.done
	if h.eventsCache != nil {
		_ = h.eventsCache.Stop()
	}
	h.uploader.Stop()
	h.started = false
}

// RecordEvent is used to put the event batch in the eventBatchChannel,
// which will be processed by handleEvents.
func (h *Handle) RecordEvent(writeKey string, eventBatch []byte) bool {
	if !h.started || h.disableEventUploads.Load() {
		return false
	}
	<-h.initialized
	// Check if writeKey part of enabled sources
	h.uploadEnabledWriteKeysMu.RLock()
	defer h.uploadEnabledWriteKeysMu.RUnlock()
	if !slices.Contains(h.uploadEnabledWriteKeys, writeKey) {
		err := h.eventsCache.Update(writeKey, eventBatch)
		if err != nil {
			h.log.Errorf("Error while updating cache: %v", err)
		}
		return false
	}
	h.uploader.RecordEvent(&GatewayEventBatchT{writeKey, eventBatch})
	return true
}

func (h *Handle) updateConfig(config map[string]backendconfig.ConfigT) {
	var uploadEnabledWriteKeys []string
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
	h.recordHistoricEvents(uploadEnabledWriteKeys)
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
func (h *Handle) recordHistoricEvents(uploadEnabledWriteKeys []string) {
	for _, writeKey := range uploadEnabledWriteKeys {
		historicEvents, err := h.eventsCache.Read(writeKey)
		if err != nil {
			continue
		}
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
				"eventName":     stringify.Any(ev["event"]),
				"eventType":     stringify.Any(ev["type"]),
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

func NewNoOpService() SourceDebugger {
	return &noopService{}
}

type noopService struct{}

func (*noopService) Start(_ backendconfig.BackendConfig) {
}

func (*noopService) RecordEvent(_ string, _ []byte) bool {
	return false
}

func (*noopService) Stop() {
}
