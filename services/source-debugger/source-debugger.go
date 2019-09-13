package sourcedebugger

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/misc"
	"github.com/rudderlabs/rudder-server/misc/logger"
	"github.com/rudderlabs/rudder-server/utils"
)

type EventSchemaT struct {
	writeKey   string
	eventBatch string
}

type MessageT struct {
	Rl_event        string      `json:"rl_event"`
	Rl_integrations interface{} `json:"rl_integrations"`
	Rl_properties   interface{} `json:"rl_properties"`
	Rl_timestamp    string      `json:"rl_timestamp"`
	Rl_type         string      `json:"rl_type"`
}

type MessageBatchT struct {
	Rl_message MessageT
}

type EventT struct {
	WriteKey string
	Batch    []MessageBatchT
}

var uploadEnabledWriteKeys []string
var configSubscriberLock sync.RWMutex
var eventSchemaChannel chan *EventSchemaT

var (
	configBackendURL string
	maxRetry         int
	retrySleep       time.Duration
)

const (
	maxBatchSize = 10
	batchTimeout = 2 * time.Second
)

func init() {
	config.Initialize()
	loadConfig()
}

func loadConfig() {
	configBackendURL = config.GetEnv("CONFIG_BACKEND_URL", "https://api.rudderlabs.com")
}

func RecordEvent(writeKey string, eventBatch string) bool {
	// Check if writeKey part of enabled sources
	configSubscriberLock.RLock()
	defer configSubscriberLock.RUnlock()
	if !misc.ContainsString(uploadEnabledWriteKeys, writeKey) {
		return false
	}

	eventSchemaChannel <- &EventSchemaT{writeKey, eventBatch}
	return true
}

func Setup() {
	// TODO: Fix the buffer size
	eventSchemaChannel = make(chan *EventSchemaT)
	go backendConfigSubscriber()
	go handleEvents()
}

func uploadEvents(eventBuffer []*EventSchemaT) {
	// Upload to a Config Backend
	var res map[string][]MessageT
	res = make(map[string][]MessageT)
	for _, event := range eventBuffer {
		batchedEvent := EventT{}
		err := json.Unmarshal([]byte(event.eventBatch), &batchedEvent)
		misc.AssertError(err)

		var arr []MessageT
		if value, ok := res[batchedEvent.WriteKey]; ok {
			arr = value
		} else {
			arr = make([]MessageT, 0)
		}

		for _, ev := range batchedEvent.Batch {
			filterValues(&ev.Rl_message)
			arr = append(arr, ev.Rl_message)
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
				misc.Assert(false)
			}
			retryCount++
			time.Sleep(retrySleep)
			//Refresh the connection
			continue
		}
		break
	}

	misc.Assert(resp.StatusCode == http.StatusOK ||
		resp.StatusCode == http.StatusBadRequest)
}

func filterValues(message *MessageT) {
	message.Rl_properties = getKeys(message.Rl_properties.(map[string]interface{}))
}

func getKeys(dataMap map[string]interface{}) []string {
	keys := make([]string, 0, len(dataMap))
	for k := range dataMap {
		keys = append(keys, k)
	}

	return keys
}

func handleEvents() {
	eventBuffer := make([]*EventSchemaT, 0)
	for {
		select {
		case eventSchema := <-eventSchemaChannel:
			//Append to request buffer
			eventBuffer = append(eventBuffer, eventSchema)
			if len(eventBuffer) == maxBatchSize {
				uploadEvents(eventBuffer)
				eventBuffer = nil
				eventBuffer = make([]*EventSchemaT, 0)
			}
		case <-time.After(batchTimeout):
			if len(eventBuffer) > 0 {
				uploadEvents(eventBuffer)
				eventBuffer = nil
				eventBuffer = make([]*EventSchemaT, 0)
			}
		}
	}
}

func backendConfigSubscriber() {
	configChannel := make(chan utils.DataEvent)
	backendconfig.Eb.Subscribe("backendconfig", configChannel)
	for {
		config := <-configChannel
		configSubscriberLock.Lock()
		uploadEnabledWriteKeys = []string{}
		sources := config.Data.(backendconfig.SourcesT)
		for _, source := range sources.Sources {
			if source.Enabled && source.UploadEvents {
				uploadEnabledWriteKeys = append(uploadEnabledWriteKeys, source.WriteKey)
			}
		}
		configSubscriberLock.Unlock()
	}
}
