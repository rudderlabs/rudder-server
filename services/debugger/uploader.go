//go:generate mockgen -destination=../../mocks/services/debugger/uploader.go -package mock_debugger github.com/rudderlabs/rudder-server/services/debugger Transformer,UploaderI

package debugger

import (
	"bytes"
	"net/http"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/sysUtils"
)

var (
	pkgLogger logger.LoggerI
	Http      sysUtils.HttpI = sysUtils.NewHttp()
)

type UploaderI interface {
	Start()
	RecordEvent(data interface{}) bool
}

type Transformer interface {
	Transform(data interface{}) ([]byte, error)
}

type Uploader struct {
	url                                    string
	transformer                            Transformer
	eventBatchChannel                      chan interface{}
	eventBufferLock                        sync.RWMutex
	eventBuffer                            []interface{}
	Client                                 sysUtils.HTTPClientI
	maxBatchSize, maxRetry, maxESQueueSize int
	batchTimeout, retrySleep               time.Duration
}

func init() {
	pkgLogger = logger.NewLogger().Child("debugger")
}

func (uploader *Uploader) Setup() {
	//Number of events that are batched before sending events to control plane
	config.RegisterIntConfigVariable(32, &uploader.maxBatchSize, true, 1, "Debugger.maxBatchSize")
	config.RegisterIntConfigVariable(1024, &uploader.maxESQueueSize, true, 1, "Debugger.maxESQueueSize")
	config.RegisterIntConfigVariable(3, &uploader.maxRetry, true, 1, "Debugger.maxRetry")
	config.RegisterDurationConfigVariable(time.Duration(2), &uploader.batchTimeout, true, time.Second, "Debugger.batchTimeoutInS")
	config.RegisterDurationConfigVariable(time.Duration(100), &uploader.retrySleep, true, time.Millisecond, "Debugger.retrySleepInMS")
}

func New(url string, transformer Transformer) UploaderI {
	eventBatchChannel := make(chan interface{})
	eventBuffer := make([]interface{}, 0)
	client := &http.Client{}

	uploader := &Uploader{url: url, transformer: transformer, eventBatchChannel: eventBatchChannel, eventBuffer: eventBuffer, Client: client}
	uploader.Setup()
	return uploader
}

func (uploader *Uploader) Start() {
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

	url := uploader.url

	retryCount := 0
	var resp *http.Response
	//Sending event schema to Config Backend
	for {
		req, err := Http.NewRequest("POST", url, bytes.NewBuffer([]byte(rawJSON)))
		if err != nil {
			pkgLogger.Errorf("[Uploader] Failed to create new http request. Err: %v", err)
			return
		}
		req.Header.Set("Content-Type", "application/json;charset=UTF-8")
		req.SetBasicAuth(config.GetWorkspaceToken(), "")

		resp, err = uploader.Client.Do(req)
		if err != nil {
			pkgLogger.Error("Config Backend connection error", err)
			if retryCount > uploader.maxRetry {
				pkgLogger.Errorf("Max retries exceeded trying to connect to config backend")
				return
			}
			retryCount++
			time.Sleep(uploader.retrySleep)
			//Refresh the connection
			continue
		}
		defer resp.Body.Close()
		break
	}

	if resp.StatusCode != http.StatusOK {
		pkgLogger.Errorf("[Uploader] Response Error from Config Backend: Status: %v, Body: %v ", resp.StatusCode, resp.Body)
	}
}

func (uploader *Uploader) handleEvents() {
	for eventSchema := range uploader.eventBatchChannel {
		uploader.eventBufferLock.Lock()

		//If eventBuffer size is more than maxESQueueSize, Delete oldest.
		if len(uploader.eventBuffer) >= uploader.maxESQueueSize {
			uploader.eventBuffer[0] = nil
			uploader.eventBuffer = uploader.eventBuffer[1:]
		}

		//Append to request buffer
		uploader.eventBuffer = append(uploader.eventBuffer, eventSchema)

		uploader.eventBufferLock.Unlock()
	}
}

func (uploader *Uploader) flushEvents() {
	for {
		time.Sleep(uploader.batchTimeout)
		uploader.eventBufferLock.Lock()

		flushSize := len(uploader.eventBuffer)
		var flushEvents []interface{}

		if flushSize > uploader.maxBatchSize {
			flushSize = uploader.maxBatchSize
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
