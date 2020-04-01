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

//RouterJobT is a structure to hold everything about router job like ids, request payload, response codes etc.
type RouterJobT struct {
	DestinationID string          `json:"destinationId"`
	SourceID      string          `json:"sourceId"`
	Payload       json.RawMessage `json:"payload"`
	AttemptNum    int             `json:"attemptNum"`
	JobState      string          `json:"jobState"`
	ErrorCode     string          `json:"errorCode"`
	ErrorResponse json.RawMessage `json:"errorResponse"`
}

var uploadEnabledDestinationIDs []string
var configSubscriberLock sync.RWMutex
var routerJobsBatchChannel chan *RouterJobT
var jobsBufferLock sync.RWMutex
var routerJobsBuffer []*RouterJobT

var (
	configBackendURL                       string
	maxBatchSize, maxRetry, maxESQueueSize int
	batchTimeout, retrySleep               time.Duration
	disableRouterJobUploads                bool
)

func init() {
	config.Initialize()
	loadConfig()
}

func loadConfig() {
	configBackendURL = config.GetEnv("CONFIG_BACKEND_URL", "https://api.rudderlabs.com")
	//Number of events that are batched before sending schema to control plane
	maxBatchSize = config.GetInt("DestinationDebugger.maxBatchSize", 32)
	maxESQueueSize = config.GetInt("DestinationDebugger.maxESQueueSize", 1024)
	maxRetry = config.GetInt("DestinationDebugger.maxRetry", 3)
	batchTimeout = config.GetDuration("DestinationDebugger.batchTimeoutInS", time.Duration(2)) * time.Second
	retrySleep = config.GetDuration("DestinationDebugger.retrySleepInMS", time.Duration(100)) * time.Millisecond
	disableRouterJobUploads = config.GetBool("DestinationDebugger.disableRouterJobUploads", false)
}

//RecordRouterJob is used to put the route job in the routerJobsBatchChannel,
//which will be processed by handleJobs.
func RecordRouterJob(destinationID string, routerJob *RouterJobT) bool {
	//if disableEventUploads is true, return;
	if disableRouterJobUploads {
		return false
	}

	// Check if destinationID part of enabled destinations
	configSubscriberLock.RLock()
	defer configSubscriberLock.RUnlock()
	if !misc.ContainsString(uploadEnabledDestinationIDs, destinationID) {
		return false
	}

	routerJobsBatchChannel <- routerJob
	return true
}

//Setup initializes this module
func Setup() {
	routerJobsBatchChannel = make(chan *RouterJobT)
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

func uploadJobs(jobsBuffer []*RouterJobT) {
	// Upload to a Config Backend
	var res map[string][]*RouterJobT
	res = make(map[string][]*RouterJobT)
	for _, job := range jobsBuffer {
		var arr []*RouterJobT
		if value, ok := res[job.DestinationID]; ok {
			arr = value
		} else {
			arr = make([]*RouterJobT, 0)
		}
		arr = append(arr, job)
		res[job.DestinationID] = arr
	}

	rawJSON, err := json.Marshal(res)
	if err != nil {
		logger.Debugf(string(rawJSON))
		misc.AssertErrorIfDev(err)
		return
	}

	client := &http.Client{}
	url := fmt.Sprintf("%s/dataplane/destinationDeliveryStatus", configBackendURL)

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

func handleJobs() {
	routerJobsBuffer = make([]*RouterJobT, 0)

	for {
		select {
		case routerJob := <-routerJobsBatchChannel:
			jobsBufferLock.Lock()

			//If routerJobsBuffer size is more than maxESQueueSize, Delete oldest.
			if len(routerJobsBuffer) > maxESQueueSize {
				routerJobsBuffer[0] = nil
				routerJobsBuffer = routerJobsBuffer[1:]
			}

			//Append to request buffer
			routerJobsBuffer = append(routerJobsBuffer, routerJob)

			jobsBufferLock.Unlock()
		}
	}
}

func flushJobs() {
	for {
		select {
		case <-time.After(batchTimeout):
			jobsBufferLock.Lock()

			flushSize := len(routerJobsBuffer)
			var flushJobs []*RouterJobT

			if flushSize > maxBatchSize {
				flushSize = maxBatchSize
			}

			if flushSize > 0 {
				flushJobs = routerJobsBuffer[:flushSize]
				routerJobsBuffer = routerJobsBuffer[flushSize:]
			}

			jobsBufferLock.Unlock()

			if flushSize > 0 {
				uploadJobs(flushJobs)
			}

			flushJobs = nil
		}
	}
}

func updateConfig(sources backendconfig.SourcesT) {
	configSubscriberLock.Lock()
	uploadEnabledDestinationIDs = []string{}
	for _, source := range sources.Sources {
		for _, destination := range source.Destinations {
			if destination.Config != nil {
				if destination.Enabled && destination.Config.(map[string]interface{})["eventDelivery"] == true {
					uploadEnabledDestinationIDs = append(uploadEnabledDestinationIDs, destination.ID)
				}
			}
		}
	}
	configSubscriberLock.Unlock()
}

func backendConfigSubscriber() {
	configChannel := make(chan utils.DataEvent)
	backendconfig.Subscribe(configChannel, "processConfig")
	for {
		config := <-configChannel
		updateConfig(config.Data.(backendconfig.SourcesT))
	}
}
