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
	"github.com/rudderlabs/rudder-server/services/debugger/cache"
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

var uploader debugger.Uploader[*DeliveryStatusT]

var (
	configBackendURL                  string
	disableEventDeliveryStatusUploads bool
	eventsDeliveryCache               cache.Cache[*DeliveryStatusT]
	cachetype                         int
	origin                            string
)

var pkgLogger logger.Logger

func Init() {
	loadConfig()
	pkgLogger = logger.NewLogger().Child("debugger").Child("destination")
	eventsDeliveryCache = cache.New[*DeliveryStatusT](cache.CacheType(cachetype), origin, pkgLogger)
}

func loadConfig() {
	configBackendURL = config.GetString("CONFIG_BACKEND_URL", "https://api.rudderstack.com")
	config.RegisterBoolConfigVariable(false, &disableEventDeliveryStatusUploads, true, "DestinationDebugger.disableEventDeliveryStatusUploads")
	config.RegisterIntConfigVariable(1, &cachetype, true, 1, "DestinationDebugger.cacheType")
	config.RegisterStringConfigVariable("destination", &origin, true, "DestinationDebugger.origin")
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
		eventsDeliveryCache.Update(destinationID, deliveryStatus)
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
	uploader = debugger.New[*DeliveryStatusT](url, eventDeliveryStatusUploader)
	uploader.Start()

	rruntime.Go(func() {
		backendConfigSubscriber(backendConfig)
	})
}

func (*EventDeliveryStatusUploader) Transform(deliveryStatusesBuffer []*DeliveryStatusT) ([]byte, error) {
	res := make(map[string]interface{})
	res["version"] = "v2"
	for _, job := range deliveryStatusesBuffer {
		var arr []*DeliveryStatusT
		if value, ok := res[job.DestinationID]; ok {
			arr, _ = value.([]*DeliveryStatusT)
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

func updateConfig(config map[string]backendconfig.ConfigT) {
	configSubscriberLock.Lock()
	uploadEnabledDestinationIDs = make(map[string]bool)
	var uploadEnabledDestinationIdsList []string
	for _, wConfig := range config {
		for _, source := range wConfig.Sources {
			for _, destination := range source.Destinations {
				if destination.Config != nil {
					if destination.Enabled && destination.Config["eventDelivery"] == true {
						uploadEnabledDestinationIdsList = append(uploadEnabledDestinationIdsList, destination.ID)
						uploadEnabledDestinationIDs[destination.ID] = true
					}
				}
			}
		}
	}
	recordHistoricEventsDelivery(uploadEnabledDestinationIdsList)
	configSubscriberLock.Unlock()
}

func backendConfigSubscriber(backendConfig backendconfig.BackendConfig) {
	configChannel := backendConfig.Subscribe(context.TODO(), "backendConfig")
	for c := range configChannel {
		updateConfig(c.Data.(map[string]backendconfig.ConfigT))
	}
}

func recordHistoricEventsDelivery(destinationIDs []string) {
	for _, destinationID := range destinationIDs {
		historicEventsDelivery := eventsDeliveryCache.Read(destinationID)
		for _, event := range historicEventsDelivery {
			uploader.RecordEvent(event)
		}
	}
}
