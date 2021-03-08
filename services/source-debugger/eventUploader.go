package sourcedebugger

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/rruntime"
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
var eventBatchChannel chan *GatewayEventBatchT
var eventBufferLock sync.RWMutex
var eventBuffer []*GatewayEventBatchT

var (
	configBackendURL                       string
	maxBatchSize, maxRetry, maxESQueueSize int
	batchTimeout, retrySleep               time.Duration
	disableEventUploads                    bool
	pkgLogger                              logger.LoggerI
)

func init() {
	loadConfig()
	pkgLogger = logger.NewLogger().Child("source-debugger").Child("eventUploader")

}

func loadConfig() {
	configBackendURL = config.GetEnv("CONFIG_BACKEND_URL", "https://api.rudderlabs.com")
	//Number of events that are batched before sending schema to control plane
	maxBatchSize = config.GetInt("SourceDebugger.maxBatchSize", 32)
	maxESQueueSize = config.GetInt("SourceDebugger.maxESQueueSize", 1024)
	maxRetry = config.GetInt("SourceDebugger.maxRetry", 3)
	batchTimeout = config.GetDuration("SourceDebugger.batchTimeoutInS", time.Duration(2)) * time.Second
	retrySleep = config.GetDuration("SourceDebugger.retrySleepInMS", time.Duration(100)) * time.Millisecond
	disableEventUploads = config.GetBool("SourceDebugger.disableEventUploads", false)
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

	eventBatchChannel <- &GatewayEventBatchT{writeKey, eventBatch}
	return true
}

//Setup initializes this module
func Setup() {
	// TODO: Fix the buffer size
	eventBatchChannel = make(chan *GatewayEventBatchT)
	eventBuffer = make([]*GatewayEventBatchT, 0)

	rruntime.Go(func() {
		backendConfigSubscriber()
	})
	rruntime.Go(func() {
		handleEvents()
	})
	rruntime.Go(func() {
		flushEvents()
	})
}

func uploadEvents(eventBuffer []*GatewayEventBatchT) {
	// Upload to a Config Backend
	res := make(map[string][]EventUploadT)
	for _, event := range eventBuffer {
		batchedEvent := EventUploadBatchT{}
		err := json.Unmarshal([]byte(event.eventBatch), &batchedEvent)
		if err != nil {
			pkgLogger.Errorf(string(event.eventBatch))
			misc.AssertErrorIfDev(err)
			continue
		}

		receivedAtTS, err := time.Parse(time.RFC3339, batchedEvent.ReceivedAt)
		if err != nil {
			receivedAtTS = time.Now()
		}
		receivedAtStr := receivedAtTS.Format(misc.RFC3339Milli)

		var arr []EventUploadT
		if value, ok := res[batchedEvent.WriteKey]; ok {
			arr = value
		} else {
			arr = make([]EventUploadT, 0)
		}

		for _, ev := range batchedEvent.Batch {
			// add the receivedAt time to each event
			ev["receivedAt"] = receivedAtStr

			arr = append(arr, ev)
		}

		res[batchedEvent.WriteKey] = arr
	}

	rawJSON, err := json.Marshal(res)
	if err != nil {
		pkgLogger.Debugf(string(rawJSON))
		misc.AssertErrorIfDev(err)
		return
	}

	client := &http.Client{}
	url := fmt.Sprintf("%s/dataplane/eventUploads", configBackendURL)

	retryCount := 0
	var resp *http.Response
	//Sending event schema to Config Backend
	for {

		req, err := http.NewRequest("POST", url, bytes.NewBuffer([]byte(rawJSON)))
		if err != nil {
			misc.AssertErrorIfDev(err)
			return
		}
		req.Header.Set("Content-Type", "application/json;charset=UTF-8")
		req.SetBasicAuth(config.GetWorkspaceToken(), "")

		resp, err = client.Do(req)
		if err != nil {
			pkgLogger.Error("Config Backend connection error", err)
			if retryCount > maxRetry {
				pkgLogger.Errorf("Max retries exceeded trying to connect to config backend")
				return
			}
			retryCount++
			time.Sleep(retrySleep)
			//Refresh the connection
			continue
		}
		break
	}

	if !(resp.StatusCode == http.StatusOK ||
		resp.StatusCode == http.StatusBadRequest) {
		pkgLogger.Errorf("Response Error from Config Backend: Status: %v, Body: %v ", resp.StatusCode, resp.Body)
	}
}

func handleEvents() {
	for {
		select {
		case eventSchema := <-eventBatchChannel:
			eventBufferLock.Lock()

			//If eventBuffer size is more than maxESQueueSize, Delete oldest.
			if len(eventBuffer) > maxESQueueSize {
				eventBuffer[0] = nil
				eventBuffer = eventBuffer[1:]
			}

			//Append to request buffer
			eventBuffer = append(eventBuffer, eventSchema)

			eventBufferLock.Unlock()
		}
	}
}

func flushEvents() {
	for {
		select {
		case <-time.After(batchTimeout):
			eventBufferLock.Lock()

			flushSize := len(eventBuffer)
			var flushEvents []*GatewayEventBatchT

			if flushSize > maxBatchSize {
				flushSize = maxBatchSize
			}

			if flushSize > 0 {
				flushEvents = eventBuffer[:flushSize]
				eventBuffer = eventBuffer[flushSize:]
			}

			eventBufferLock.Unlock()

			if flushSize > 0 {
				uploadEvents(flushEvents)
			}

			flushEvents = nil
		}
	}
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
