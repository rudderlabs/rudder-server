package sourcedebugger

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/debugger"
	"github.com/rudderlabs/rudder-server/utils"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

//GatewayEventBatchT is a structure to hold batch of events
type GatewayEventBatchT struct {
	writeKey   string
	eventBatch string
}

//EventUploadT is a structure to hold actual event data
type EventUploadT map[string]interface{}

//EventUploadBatchT is a structure to hold batch of events
type EventUploadBatchT struct {
	WriteKey   string
	ReceivedAt string
	Batch      []EventUploadT
}

var uploadEnabledWriteKeys []string
var configSubscriberLock sync.RWMutex

var uploader debugger.UploaderI

var (
	configBackendURL    string
	disableEventUploads bool
	pkgLogger           logger.LoggerI
)

func init() {
	loadConfig()
	pkgLogger = logger.NewLogger().Child("debugger").Child("source")

}

func loadConfig() {
	configBackendURL = config.GetEnv("CONFIG_BACKEND_URL", "https://api.rudderlabs.com")
	config.RegisterBoolConfigVariable(false, &disableEventUploads, true, "SourceDebugger.disableEventUploads")
}

type EventUploader struct {
}

//Setup initializes this module
func Setup() {
	url := fmt.Sprintf("%s/dataplane/eventUploads", configBackendURL)
	eventUploader := &EventUploader{}
	uploader = debugger.New(url, eventUploader)
	uploader.Start()

	rruntime.Go(func() {
		backendConfigSubscriber()
	})
}

//RecordEvent is used to put the event batch in the eventBatchChannel,
//which will be processed by handleEvents.
func RecordEvent(writeKey string, eventBatch string) bool {
	//if disableEventUploads is true, return;
	if disableEventUploads {
		return false
	}

	// Check if writeKey part of enabled sources
	configSubscriberLock.RLock()
	defer configSubscriberLock.RUnlock()
	if !misc.ContainsString(uploadEnabledWriteKeys, writeKey) {
		return false
	}

	uploader.RecordEvent(&GatewayEventBatchT{writeKey, eventBatch})
	return true
}

func (eventUploader *EventUploader) Transform(data interface{}) ([]byte, error) {
	eventBuffer := data.([]interface{})
	res := make(map[string]interface{})
	res["version"] = "v1"
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
			arr = value.([]EventUploadT)
		} else {
			arr = make([]EventUploadT, 0)
		}

		for _, ev := range batchedEvent.Batch {
			// add the receivedAt time to each event
			event := map[string]interface{}{
				"payload":       ev,
				"receivedAt":    receivedAtStr,
				"eventName":     ensureString(ev["event"]),
				"eventType":     ensureString(ev["type"]),
				"errorResponse": nil,
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

func ensureString(eventDetail interface{}) string {
	switch detail := eventDetail.(type) {
	case string:
		return detail
	case map[string]interface{}:
		detailBytes, err := json.Marshal(detail)
		if err != nil {
			return fmt.Sprint(detail)
		}
		return string(detailBytes)
	default:
		return fmt.Sprint(eventDetail)
	}
}
