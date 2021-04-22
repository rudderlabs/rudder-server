package destinationdebugger

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/debugger"
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

var uploader *debugger.Uploader

var (
	configBackendURL                  string
	disableEventDeliveryStatusUploads bool
)

var pkgLogger logger.LoggerI

func init() {
	loadConfig()
	pkgLogger = logger.NewLogger().Child("debugger").Child("destination")
}

func loadConfig() {
	configBackendURL = config.GetEnv("CONFIG_BACKEND_URL", "https://api.rudderlabs.com")
	config.RegisterBoolConfigVariable(false, &disableEventDeliveryStatusUploads, true, "DestinationDebugger.disableEventDeliveryStatusUploads")
}

type EventDeliveryStatusUploader struct {
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

	uploader.RecordEvent(deliveryStatus)
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
	url := fmt.Sprintf("%s/dataplane/eventDeliveryStatus", configBackendURL)
	eventDeliveryStatusUploader := &EventDeliveryStatusUploader{}
	uploader = debugger.New(url, eventDeliveryStatusUploader)
	uploader.Setup()

	rruntime.Go(func() {
		backendConfigSubscriber()
	})
}

func (eventDeliveryStatusUploader *EventDeliveryStatusUploader) Transform(data interface{}) ([]byte, error) {
	deliveryStatusesBuffer := data.([]interface{})
	res := make(map[string][]*DeliveryStatusT)
	for _, j := range deliveryStatusesBuffer {
		job := j.(*DeliveryStatusT)
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
		misc.AssertErrorIfDev(err)
		return nil, err
	}

	return rawJSON, nil
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
