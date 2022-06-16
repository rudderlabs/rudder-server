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
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

// GatewayEventBatchT is a structure to hold batch of events
type GatewayEventBatchT struct {
	writeKey   string
	eventBatch string
}

// EventUploadT is a structure to hold actual event data
type EventUploadT map[string]interface{}

// EventUploadBatchT is a structure to hold batch of events
type EventUploadBatchT struct {
	WriteKey   string
	ReceivedAt string
	Batch      []EventUploadT
}

var (
	uploadEnabledWriteKeys []string
	configSubscriberLock   sync.RWMutex
)

var uploader debugger.UploaderI

var (
	configBackendURL    string
	disableEventUploads bool
	pkgLogger           logger.LoggerI
	eventsCacheMap      debugger.Cache
)

func Init() {
	loadConfig()
	pkgLogger = logger.NewLogger().Child("debugger").Child("source")
}

func loadConfig() {
	configBackendURL = config.GetEnv("CONFIG_BACKEND_URL", "https://api.rudderlabs.com")
	config.RegisterBoolConfigVariable(false, &disableEventUploads, true, "SourceDebugger.disableEventUploads")
}

type EventUploader struct{}

// Setup initializes this module
func Setup(backendConfig backendconfig.BackendConfig) {
	url := fmt.Sprintf("%s/dataplane/v2/eventUploads", configBackendURL)
	eventUploader := &EventUploader{}
	uploader = debugger.New(url, eventUploader)
	uploader.Start()

	rruntime.Go(func() {
		backendConfigSubscriber(backendConfig)
	})
}

// recordHistoricEvents sends the events collected in cache as live events.
// This is called on config update.
// IMP: The function must be called before releasing configSubscriberLock lock to ensure the order of RecordEvent call
func recordHistoricEvents(writeKeys []string) {
	for _, writeKey := range writeKeys {
		historicEvents := eventsCacheMap.ReadAndPopData(writeKey)
		for _, eventBatchData := range historicEvents {
			var eventBatch string
			if err := json.Unmarshal(eventBatchData, &eventBatch); err != nil {
				panic(err)
			}
			uploader.RecordEvent(&GatewayEventBatchT{writeKey, eventBatch})
		}
	}
}

// RecordEvent is used to put the event batch in the eventBatchChannel,
// which will be processed by handleEvents.
func RecordEvent(writeKey, eventBatch string) bool {
	// if disableEventUploads is true, return;
	if disableEventUploads {
		return false
	}

	// Check if writeKey part of enabled sources
	configSubscriberLock.RLock()
	defer configSubscriberLock.RUnlock()
	if !misc.ContainsString(uploadEnabledWriteKeys, writeKey) {
		eventBatchData, _ := json.Marshal(eventBatch)
		eventsCacheMap.Update(writeKey, eventBatchData)
		return false
	}

	uploader.RecordEvent(&GatewayEventBatchT{writeKey, eventBatch})
	return true
}

func (eventUploader *EventUploader) Transform(data interface{}) ([]byte, error) {
	eventBuffer := data.([]interface{})
	res := make(map[string]interface{})
	res["version"] = "v2"
	for _, e := range eventBuffer {
		event := e.(*GatewayEventBatchT)
		batchedEvent := EventUploadBatchT{}
		err := json.Unmarshal([]byte(event.eventBatch), &batchedEvent)
		if err != nil {
			pkgLogger.Errorf("[Source live events] Failed to unmarshal. Err: %v", err)
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
		} else {
			arr = make([]EventUploadT, 0)
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
		pkgLogger.Errorf("[Source live events] Failed to marshal payload. Err: %v", err)
		return nil, err
	}

	return rawJSON, nil
}

func updateConfig(sources backendconfig.ConfigT) {
	configSubscriberLock.Lock()
	uploadEnabledWriteKeys = []string{}
	for _, source := range sources.Sources {
		if source.Config != nil {
			if source.Enabled && source.Config["eventUpload"] == true {
				uploadEnabledWriteKeys = append(uploadEnabledWriteKeys, source.WriteKey)
			}
		}
	}

	recordHistoricEvents(uploadEnabledWriteKeys)
	configSubscriberLock.Unlock()
}

func backendConfigSubscriber(backendConfig backendconfig.BackendConfig) {
	ch := backendConfig.Subscribe(context.TODO(), backendconfig.TopicProcessConfig)
	for config := range ch {
		updateConfig(config.Data.(backendconfig.ConfigT))
	}
}
