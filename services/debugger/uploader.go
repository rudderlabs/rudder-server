//go:generate mockgen -destination=../../mocks/services/debugger/uploader.go -package mock_debugger github.com/rudderlabs/rudder-server/services/debugger TransformerAny

package debugger

import (
	"bytes"
	"context"
	"net/http"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/httputil"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/sysUtils"
)

var (
	pkgLogger logger.Logger
	Http      sysUtils.HttpI = sysUtils.NewHttp()
)

type Uploader[E any] interface {
	Start()
	Stop()
	RecordEvent(data E)
}

type TransformerAny interface {
	Transformer[any]
}
type Transformer[E any] interface {
	Transform(data []E) ([]byte, error)
}

type uploaderImpl[E any] struct {
	url                                    string
	transformer                            Transformer[E]
	eventBatchChannel                      chan E
	eventBufferLock                        sync.RWMutex
	eventBuffer                            []E
	Client                                 sysUtils.HTTPClientI
	maxBatchSize, maxRetry, maxESQueueSize int
	batchTimeout, retrySleep               time.Duration
	region                                 string

	bgWaitGroup sync.WaitGroup
}

func init() {
	pkgLogger = logger.NewLogger().Child("debugger")
}

func (uploader *uploaderImpl[E]) Setup() {
	// Number of events that are batched before sending events to control plane
	config.RegisterIntConfigVariable(32, &uploader.maxBatchSize, true, 1, "Debugger.maxBatchSize")
	config.RegisterIntConfigVariable(256, &uploader.maxESQueueSize, true, 1, "Debugger.maxESQueueSize")
	config.RegisterIntConfigVariable(3, &uploader.maxRetry, true, 1, "Debugger.maxRetry")
	config.RegisterDurationConfigVariable(2, &uploader.batchTimeout, true, time.Second, "Debugger.batchTimeoutInS")
	config.RegisterDurationConfigVariable(100, &uploader.retrySleep, true, time.Millisecond, "Debugger.retrySleepInMS")
	uploader.region = config.GetString("region", "")
}

func New[E any](url string, transformer Transformer[E]) Uploader[E] {
	eventBatchChannel := make(chan E)
	eventBuffer := make([]E, 0)
	client := &http.Client{Timeout: config.GetDuration("HttpClient.debugger.timeout", 30, time.Second)}

	uploader := &uploaderImpl[E]{url: url, transformer: transformer, eventBatchChannel: eventBatchChannel, eventBuffer: eventBuffer, Client: client, bgWaitGroup: sync.WaitGroup{}}
	uploader.Setup()
	return uploader
}

func (uploader *uploaderImpl[E]) Start() {
	ctx, cancel := context.WithCancel(context.Background())

	rruntime.Go(func() {
		uploader.handleEvents()
		cancel()
	})

	uploader.bgWaitGroup.Add(1)
	rruntime.Go(func() {
		uploader.flushEvents(ctx)
		uploader.bgWaitGroup.Done()
	})
}

func (uploader *uploaderImpl[E]) Stop() {
	close(uploader.eventBatchChannel)
	uploader.bgWaitGroup.Wait()
}

// RecordEvent is used to put the event batch in the eventBatchChannel,
// which will be processed by handleEvents.
func (uploader *uploaderImpl[E]) RecordEvent(data E) {
	uploader.eventBatchChannel <- data
}

func (uploader *uploaderImpl[E]) uploadEvents(eventBuffer []E) {
	// Upload to a Config Backend
	rawJSON, err := uploader.transformer.Transform(eventBuffer)
	if err != nil {
		return
	}

	url := uploader.url

	retryCount := 1
	// Sending live events to Config Backend
	for {
		var resp *http.Response
		startTime := time.Now()
		resource := path.Base(url)
		req, err := Http.NewRequest("POST", url, bytes.NewBuffer(rawJSON))
		if err != nil {
			pkgLogger.Errorf("[Uploader] Failed to create new http request. Err: %v", err)
			return
		}
		if uploader.region != "" {
			q := req.URL.Query()
			q.Add("region", uploader.region)
			req.URL.RawQuery = q.Encode()
		}
		req.Header.Set("Content-Type", "application/json;charset=UTF-8")
		req.SetBasicAuth(config.GetWorkspaceToken(), "")

		resp, err = uploader.Client.Do(req)
		if err != nil {
			pkgLogger.Error("Config Backend connection error", err)
			stats.Default.NewTaggedStat("debugger_http_errors", stats.CountType, map[string]string{
				"resource": resource,
			}).Increment()
			if retryCount >= uploader.maxRetry {
				pkgLogger.Errorf("Max retries exceeded trying to connect to config backend")
				return
			}
			retryCount++
			time.Sleep(uploader.retrySleep)
			// Refresh the connection
			continue
		}

		stats.Default.NewTaggedStat("debugger_http_requests", stats.TimerType, stats.Tags{
			"responseCode": strconv.Itoa(resp.StatusCode),
			"resource":     resource,
		}).Since(startTime)

		func() { httputil.CloseResponse(resp) }()
		if resp.StatusCode != http.StatusOK {
			pkgLogger.Errorf("[Uploader] Response Error from Config Backend: Status: %v, Body: %v ", resp.StatusCode, resp.Body)
		}
		break
	}
}

func (uploader *uploaderImpl[E]) handleEvents() {
	for eventSchema := range uploader.eventBatchChannel {
		uploader.eventBufferLock.Lock()

		// If eventBuffer size is more than maxESQueueSize, Delete oldest.
		if len(uploader.eventBuffer) >= uploader.maxESQueueSize {
			var z E
			uploader.eventBuffer[0] = z
			uploader.eventBuffer = uploader.eventBuffer[1:]
		}

		// Append to request buffer
		uploader.eventBuffer = append(uploader.eventBuffer, eventSchema)

		uploader.eventBufferLock.Unlock()
	}
}

func (uploader *uploaderImpl[E]) flushEvents(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
		case <-time.After(uploader.batchTimeout):
		}
		uploader.eventBufferLock.Lock()

		flushSize := len(uploader.eventBuffer)
		var flushEvents []E

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

		if ctx.Err() != nil {
			return
		}
	}
}
