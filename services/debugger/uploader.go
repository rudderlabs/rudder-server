package debugger

import (
	"bytes"
	"net/http"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

var (
	configBackendURL                       string
	maxBatchSize, maxRetry, maxESQueueSize int
	batchTimeout, retrySleep               time.Duration
	pkgLogger                              logger.LoggerI
)

type Transformer interface {
	Transform(data interface{}) ([]byte, error)
}

type Uploader struct {
	url               string
	transformer       Transformer
	eventBatchChannel chan interface{}
	eventBufferLock   sync.RWMutex
	eventBuffer       []interface{}
}

func init() {
	loadConfig()
	pkgLogger = logger.NewLogger().Child("debugger")
}

func loadConfig() {
	//Number of events that are batched before sending events to control plane
	config.RegisterIntConfigVariable(32, &maxBatchSize, true, 1, "Debugger.maxBatchSize")
	config.RegisterIntConfigVariable(1024, &maxESQueueSize, true, 1, "Debugger.maxESQueueSize")
	config.RegisterIntConfigVariable(3, &maxRetry, true, 1, "Debugger.maxRetry")
	config.RegisterDurationConfigVariable(time.Duration(2), &batchTimeout, true, time.Second, []string{"Debugger.batchTimeout","Debugger.batchTimeoutInS"}...)
	config.RegisterDurationConfigVariable(time.Duration(100), &retrySleep, true, time.Millisecond, []string{"Debugger.retrySleep","Debugger.retrySleepInMS"}...)
}

func New(url string, transformer Transformer) *Uploader {
	eventBatchChannel := make(chan interface{})
	eventBuffer := make([]interface{}, 0)

	return &Uploader{url: url, transformer: transformer, eventBatchChannel: eventBatchChannel, eventBuffer: eventBuffer}
}

func (uploader *Uploader) Setup() {
	rruntime.Go(func() {
		uploader.handleEvents()
	})
	rruntime.Go(func() {
		uploader.flushEvents()
	})
}

//RecordEvent is used to put the event batch in the eventBatchChannel,
//which will be processed by handleEvents.
func (uploader *Uploader) RecordEvent(data interface{}) bool {
	uploader.eventBatchChannel <- data
	return true
}

func (uploader *Uploader) uploadEvents(eventBuffer []interface{}) {
	// Upload to a Config Backend
	rawJSON, err := uploader.transformer.Transform(eventBuffer)
	if err != nil {
		return
	}

	client := &http.Client{}
	url := uploader.url

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

func (uploader *Uploader) handleEvents() {
	for {
		select {
		case eventSchema := <-uploader.eventBatchChannel:
			uploader.eventBufferLock.Lock()

			//If eventBuffer size is more than maxESQueueSize, Delete oldest.
			if len(uploader.eventBuffer) > maxESQueueSize {
				uploader.eventBuffer[0] = nil
				uploader.eventBuffer = uploader.eventBuffer[1:]
			}

			//Append to request buffer
			uploader.eventBuffer = append(uploader.eventBuffer, eventSchema)

			uploader.eventBufferLock.Unlock()
		}
	}
}

func (uploader *Uploader) flushEvents() {
	for {
		select {
		case <-time.After(batchTimeout):
			uploader.eventBufferLock.Lock()

			flushSize := len(uploader.eventBuffer)
			var flushEvents []interface{}

			if flushSize > maxBatchSize {
				flushSize = maxBatchSize
			}

			if flushSize > 0 {
				flushEvents = uploader.eventBuffer[:flushSize]
				uploader.eventBuffer = uploader.eventBuffer[flushSize:]
			}

			uploader.eventBufferLock.Unlock()

			if flushSize > 0 {
				uploader.uploadEvents(flushEvents)
			}

			flushEvents = nil
		}
	}
}
