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
	"github.com/rudderlabs/rudder-server/utils"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

//EventSchemaT is a structure to hold batch of events
type EventSchemaT struct {
	writeKey   string
	eventBatch string
}

//MessageT is a structure to hold actual event data
type MessageT struct {
	Event             string      `json:"event"`
	Integrations      interface{} `json:"integrations"`
	Properties        interface{} `json:"properties"`
	OriginalTimestamp string      `json:"originalTimestamp"`
	SentAt            string      `json:"sentAt"`
	Type              string      `json:"type"`
}

//EventT is a structure to hold batch of events
type EventT struct {
	WriteKey   string
	ReceivedAt string
	Batch      []MessageT
}

var uploadEnabledWriteKeys []string
var configSubscriberLock sync.RWMutex
var eventSchemaChannel chan *EventSchemaT
var eventBufferLock sync.RWMutex
var eventBuffer []*EventSchemaT

var (
	configBackendURL                       string
	maxBatchSize, maxRetry, maxESQueueSize int
	batchTimeout, retrySleep               time.Duration
	disableEventUploads                    bool
)

func init() {
	config.Initialize()
	loadConfig()
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

//RecordEvent is used to put the event in the eventSchemaChannel,
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

	eventSchemaChannel <- &EventSchemaT{writeKey, eventBatch}
	return true
}

//Setup initializes this module
func Setup() {
	// TODO: Fix the buffer size
	eventSchemaChannel = make(chan *EventSchemaT)
	go backendConfigSubscriber()
	go handleEvents()
	go flushEvents()
}

func uploadEvents(eventBuffer []*EventSchemaT) {
	// Upload to a Config Backend
	var res map[string][]MessageT
	res = make(map[string][]MessageT)
	for _, event := range eventBuffer {
		batchedEvent := EventT{}
		err := json.Unmarshal([]byte(event.eventBatch), &batchedEvent)
		misc.AssertError(err)

		receivedAtTS, err := time.Parse(time.RFC3339, batchedEvent.ReceivedAt)
		if err != nil {
			receivedAtTS = time.Now()
		}

		var arr []MessageT
		if value, ok := res[batchedEvent.WriteKey]; ok {
			arr = value
		} else {
			arr = make([]MessageT, 0)
		}

		for _, ev := range batchedEvent.Batch {
			filterValues(&ev)

			//updating originalTimestamp in the event using the formula
			//receivedAt - (sentAt - originalTimeStamp)
			orgTS, err := time.Parse(time.RFC3339, ev.OriginalTimestamp)
			if err != nil {
				orgTS = time.Now()
			}

			sentAtTS, err := time.Parse(time.RFC3339, ev.SentAt)
			if err != nil {
				sentAtTS = time.Now()
			}

			ev.OriginalTimestamp = misc.GetChronologicalTimeStamp(receivedAtTS, sentAtTS, orgTS).Format(time.RFC3339)

			arr = append(arr, ev)
		}

		res[batchedEvent.WriteKey] = arr
	}

	rawJSON, err := json.Marshal(res)
	misc.AssertError(err)

	tr := &http.Transport{}
	client := &http.Client{Transport: tr}
	url := fmt.Sprintf("%s/eventUploads", configBackendURL)

	retryCount := 0
	var resp *http.Response
	//Sending event schema to Config Backend
	for {
		resp, err = client.Post(url, "application/json; charset=utf-8",
			bytes.NewBuffer(rawJSON))
		if err != nil {
			logger.Error("Config Backend connection error", err)
			if retryCount > maxRetry {
				logger.Errorf("Max retries exceeded trying to connect to config backend")
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
		logger.Errorf("Response Error from Config Backend: Status: %v, Body: %v ", resp.StatusCode, resp.Body)
	}
}

func filterValues(message *MessageT) {
	message.Properties = getKeys(message.Properties.(map[string]interface{}))
}

func getKeys(dataMap map[string]interface{}) []string {
	keys := make([]string, 0, len(dataMap))
	for k := range dataMap {
		keys = append(keys, k)
	}

	return keys
}

func handleEvents() {
	eventBuffer = make([]*EventSchemaT, 0)
	for {
		select {
		case eventSchema := <-eventSchemaChannel:
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
			var flushEvents []*EventSchemaT

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

func updateConfig(sources backendconfig.SourcesT) {
	configSubscriberLock.Lock()
	uploadEnabledWriteKeys = []string{}
	for _, source := range sources.Sources {
		if source.Config != nil {
			if source.Enabled && source.Config.(map[string]interface{})["eventUpload"] == true {
				uploadEnabledWriteKeys = append(uploadEnabledWriteKeys, source.WriteKey)
			}
		}
	}
	configSubscriberLock.Unlock()
}

func backendConfigSubscriber() {
	configChannel := make(chan utils.DataEvent)
	backendconfig.Subscribe(configChannel)
	for {
		config := <-configChannel
		updateConfig(config.Data.(backendconfig.SourcesT))
	}
}
