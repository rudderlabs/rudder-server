package destinationdebugger

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/debugger"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

// DeliveryStatusT is a structure to hold everything related to event delivery
type DeliveryStatusT struct {
	DestinationID string          `json:"destinationId"`
	SourceID      string          `json:"sourceId"`
	Payload       json.RawMessage `json:"payload"`
	AttemptNum    int             `json:"attemptNum"`
	JobState      string          `json:"jobState"`
	ErrorCode     string          `json:"errorCode"`
	ErrorResponse json.RawMessage `json:"errorResponse"`
	SentAt        string          `json:"sentAt"`
	EventName     string          `json:"eventName"`
	EventType     string          `json:"eventType"`
}

var (
	uploadEnabledDestinationIDs map[string]bool
	configSubscriberLock        sync.RWMutex
)

var uploader debugger.UploaderI

var (
	configBackendURL                  string
	disableEventDeliveryStatusUploads bool
	eventsDeliveryCache               debugger.Cache
)

var pkgLogger logger.LoggerI

func Init() {
	loadConfig()
	pkgLogger = logger.NewLogger().Child("debugger").Child("destination")
}

func loadConfig() {
	configBackendURL = config.GetEnv("CONFIG_BACKEND_URL", "https://api.rudderlabs.com")
	config.RegisterBoolConfigVariable(false, &disableEventDeliveryStatusUploads, true, "DestinationDebugger.disableEventDeliveryStatusUploads")
}

type EventDeliveryStatusUploader struct{}

// RecordEventDeliveryStatus is used to put the delivery status in the deliveryStatusesBatchChannel,
// which will be processed by handleJobs.
func RecordEventDeliveryStatus(destinationID string, deliveryStatus *DeliveryStatusT) bool {
	// if disableEventDeliveryStatusUploads is true, return;
	if disableEventDeliveryStatusUploads {
		return false
	}

	// Check if destinationID part of enabled destinations, if not then push the job in cache to keep track
	configSubscriberLock.RLock()
	defer configSubscriberLock.RUnlock()
	if !HasUploadEnabled(destinationID) {
		deliveryStatusData, _ := json.Marshal(deliveryStatus)
		eventsDeliveryCache.Update(destinationID, deliveryStatusData)
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

// Setup initializes this module
func Setup(backendConfig backendconfig.BackendConfig) {
	url := fmt.Sprintf("%s/dataplane/v2/eventDeliveryStatus", configBackendURL)
	eventDeliveryStatusUploader := &EventDeliveryStatusUploader{}
	uploader = debugger.New(url, eventDeliveryStatusUploader)
	uploader.Start()

	rruntime.Go(func() {
		backendConfigSubscriber(backendConfig)
	})
}

func (eventDeliveryStatusUploader *EventDeliveryStatusUploader) Transform(data interface{}) ([]byte, error) {
	deliveryStatusesBuffer := data.([]interface{})
	res := make(map[string]interface{})
	res["version"] = "v2"
	for _, j := range deliveryStatusesBuffer {
		job := j.(*DeliveryStatusT)
		var arr []*DeliveryStatusT
		if value, ok := res[job.DestinationID]; ok {
			arr, _ = value.([]*DeliveryStatusT)
		} else {
			arr = make([]*DeliveryStatusT, 0)
		}
		arr = append(arr, job)
		res[job.DestinationID] = arr
	}

	rawJSON, err := json.Marshal(res)
	if err != nil {
		pkgLogger.Errorf("[Destination live events] Failed to marshal payload. Err: %v", err)
		return nil, err
	}

	return rawJSON, nil
}

func updateConfig(sources backendconfig.ConfigT) {
	configSubscriberLock.Lock()
	uploadEnabledDestinationIDs = make(map[string]bool)
	var uploadEnabledDestinationIdsList []string
	for _, source := range sources.Sources {
		for _, destination := range source.Destinations {
			if destination.Config != nil {
				if destination.Enabled && destination.Config["eventDelivery"] == true {
					uploadEnabledDestinationIdsList = append(uploadEnabledDestinationIdsList, destination.ID)
					uploadEnabledDestinationIDs[destination.ID] = true
				}
			}
		}
	}
	recordHistoricEventsDelivery(uploadEnabledDestinationIdsList)
	configSubscriberLock.Unlock()
}

func backendConfigSubscriber(backendConfig backendconfig.BackendConfig) {
	configChannel := backendConfig.Subscribe(context.TODO(), "backendConfig")
	for config := range configChannel {
		updateConfig(config.Data.(backendconfig.ConfigT))
	}
}

func recordHistoricEventsDelivery(destinationIDs []string) {
	for _, destinationID := range destinationIDs {
		historicEventsDelivery := eventsDeliveryCache.ReadAndPopData(destinationID)
		for _, event := range historicEventsDelivery {
			var historicEventsDeliveryData DeliveryStatusT
			if err := json.Unmarshal(event, &historicEventsDeliveryData); err != nil {
				panic(err)
			}
			uploader.RecordEvent(&historicEventsDeliveryData)
		}
	}
}
