package sourcedebugger

import (
	"fmt"
	"sync"
	"time"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/misc"
	"github.com/rudderlabs/rudder-server/utils"
)

type EventSchemaT struct {
	writeKey   string
	eventBatch string
}

var uploadEnabledWriteKeys []string
var configSubscriberLock sync.RWMutex
var eventSchemaChannel chan *EventSchemaT

const (
	maxBatchSize = 10
	batchTimeout = 2 * time.Second
)

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

func uploadEvents() {
	// Upload to a specific endpoint
}

func Setup() {
	fmt.Println("Inside Setup")
	// TODO: Fix the buffer size
	eventSchemaChannel = make(chan *EventSchemaT)
	go backendConfigSubscriber()
	go handleEvents()
}

func handleEvents() {
	eventBuffer := make([]*EventSchemaT, 0)
	for {
		select {
		case eventSchema := <-eventSchemaChannel:
			//Append to request buffer
			eventBuffer = append(eventBuffer, eventSchema)
			if len(eventBuffer) == maxBatchSize {
				uploadEvents()
				eventBuffer = nil
				eventBuffer = make([]*EventSchemaT, 0)
			}
		case <-time.After(batchTimeout):
			if len(eventBuffer) > 0 {
				uploadEvents()
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
