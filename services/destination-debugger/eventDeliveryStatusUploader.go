package destinationdebugger

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

//DeliveryStatusT is a structure to hold everything related to event delivery
type DeliveryStatusT struct {
	DestinationID string          `json:"destinationId"`
	SourceID      string          `json:"sourceId"`
	Payload       json.RawMessage `json:"payload"`
	AttemptNum    int             `json:"attemptNum"`
	JobState      string          `json:"jobState"`
	ErrorCode     string          `json:"errorCode"`
	ErrorResponse json.RawMessage `json:"errorResponse"`
}

var uploadEnabledDestinationIDs map[string]bool
var configSubscriberLock sync.RWMutex
var deliveryStatusesBatchChannel chan *DeliveryStatusT
var deliveryStatusesBufferLock sync.RWMutex
var deliveryStatusesBuffer []*DeliveryStatusT

var (
	configBackendURL                       string
	maxBatchSize, maxRetry, maxESQueueSize int
	batchTimeout, retrySleep               time.Duration
	disableEventDeliveryStatusUploads      bool
)

var pkgLogger logger.LoggerI

func init() {
	loadConfig()
	pkgLogger = logger.NewLogger().Child("destination-debugger").Child("eventDeliveryStatusUploader")
}

func loadConfig() {
	configBackendURL = config.GetEnv("CONFIG_BACKEND_URL", "https://api.rudderlabs.com")
	//Number of events that are batched before sending schema to control plane
	maxBatchSize = config.GetInt("DestinationDebugger.maxBatchSize", 32)
	maxESQueueSize = config.GetInt("DestinationDebugger.maxESQueueSize", 1024)
	maxRetry = config.GetInt("DestinationDebugger.maxRetry", 3)
	batchTimeout = config.GetDuration("DestinationDebugger.batchTimeoutInS", time.Duration(2)) * time.Second
	retrySleep = config.GetDuration("DestinationDebugger.retrySleepInMS", time.Duration(100)) * time.Millisecond
	disableEventDeliveryStatusUploads = config.GetBool("DestinationDebugger.disableEventDeliveryStatusUploads", false)
}

//RecordEventDeliveryStatus is used to put the delivery status in the deliveryStatusesBatchChannel,
//which will be processed by handleJobs.
func RecordEventDeliveryStatus(destinationID string, deliveryStatus *DeliveryStatusT) bool {
	//if disableEventUploads is true, return;
	if disableEventDeliveryStatusUploads {
		return false
	}

	// Check if destinationID part of enabled destinations
	configSubscriberLock.RLock()
	defer configSubscriberLock.RUnlock()
	if _, ok := uploadEnabledDestinationIDs[destinationID]; !ok {
		return false
	}

	deliveryStatusesBatchChannel <- deliveryStatus
	return true
}

func HasUploadEnabled(destID string) bool {
	configSubscriberLock.RLock()
	defer configSubscriberLock.RUnlock()
	_, ok := uploadEnabledDestinationIDs[destID]
	return ok
}

//Setup initializes this module
func Setup() {
	deliveryStatusesBatchChannel = make(chan *DeliveryStatusT)
	deliveryStatusesBuffer = make([]*DeliveryStatusT, 0)

	rruntime.Go(func() {
		backendConfigSubscriber()
	})
	rruntime.Go(func() {
		handleJobs()
	})
	rruntime.Go(func() {
		flushJobs()
	})
}

func uploadJobs(deliveryStatusesBuffer []*DeliveryStatusT) {
	// Upload to a Config Backend
	res := make(map[string][]*DeliveryStatusT)
	for _, job := range deliveryStatusesBuffer {
		var arr []*DeliveryStatusT
		if value, ok := res[job.DestinationID]; ok {
			arr = value
		} else {
			arr = make([]*DeliveryStatusT, 0)
		}
		arr = append(arr, job)
		res[job.DestinationID] = arr
	}

	rawJSON, err := json.Marshal(res)
	if err != nil {
		pkgLogger.Debugf(string(rawJSON))
		misc.AssertErrorIfDev(err)
		return
	}

	client := &http.Client{}
	url := fmt.Sprintf("%s/dataplane/eventDeliveryStatus", configBackendURL)

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

func handleJobs() {
	for {
		select {
		case deliveryStatus := <-deliveryStatusesBatchChannel:
			deliveryStatusesBufferLock.Lock()

			//If deliveryStatusesBuffer size is more than maxESQueueSize, Delete oldest.
			if len(deliveryStatusesBuffer) > maxESQueueSize {
				deliveryStatusesBuffer[0] = nil
				deliveryStatusesBuffer = deliveryStatusesBuffer[1:]
			}

			//Append to request buffer
			deliveryStatusesBuffer = append(deliveryStatusesBuffer, deliveryStatus)

			deliveryStatusesBufferLock.Unlock()
		}
	}
}

func flushJobs() {
	for {
		select {
		case <-time.After(batchTimeout):
			deliveryStatusesBufferLock.Lock()

			flushSize := len(deliveryStatusesBuffer)
			var flushJobs []*DeliveryStatusT

			if flushSize > maxBatchSize {
				flushSize = maxBatchSize
			}

			if flushSize > 0 {
				flushJobs = deliveryStatusesBuffer[:flushSize]
				deliveryStatusesBuffer = deliveryStatusesBuffer[flushSize:]
			}

			deliveryStatusesBufferLock.Unlock()

			if flushSize > 0 {
				uploadJobs(flushJobs)
			}

			flushJobs = nil
		}
	}
}

func updateConfig(sources backendconfig.ConfigT) {
	configSubscriberLock.Lock()
	uploadEnabledDestinationIDs = make(map[string]bool)
	for _, source := range sources.Sources {
		for _, destination := range source.Destinations {
			if destination.Config != nil {
				if destination.Enabled && destination.Config["eventDelivery"] == true {
					uploadEnabledDestinationIDs[destination.ID] = true
				}
			}
		}
	}
	configSubscriberLock.Unlock()
}

func backendConfigSubscriber() {
	configChannel := make(chan utils.DataEvent)
	backendconfig.Subscribe(configChannel, "backendConfig")
	for {
		config := <-configChannel
		updateConfig(config.Data.(backendconfig.ConfigT))
	}
}
