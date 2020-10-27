package customdestinationmanager

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sync"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/kvstoremanager"
	"github.com/rudderlabs/rudder-server/services/streammanager"
	"github.com/rudderlabs/rudder-server/utils"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/tidwall/gjson"
)

const (
	STREAM = "stream"
	KV     = "kv"
)

var (
	ObjectStreamDestinations []string
	KVStoreDestinations      []string
	Destinations             []string
	customManagerMap         map[string]*CustomManagerT
)

// DestinationManager implements the method to send the events to custom destinations
type DestinationManager interface {
	SendData(jsonData json.RawMessage, sourceID string, destID string) (int, string, string)
}

// CustomManagerT handles this module
type CustomManagerT struct {
	destType           string
	managerType        string
	destinationsMap    map[string]*CustomDestination
	destinationLockMap map[string]*sync.RWMutex
	latestConfig       map[string]backendconfig.DestinationT
}

//CustomDestination keeps the config of a destination and corresponding producer for a stream destination
type CustomDestination struct {
	Config interface{}
	Client interface{}
}

func init() {
	loadConfig()
}

func loadConfig() {
	ObjectStreamDestinations = []string{"KINESIS", "KAFKA", "AZURE_EVENT_HUB", "FIREHOSE", "EVENTBRIDGE", "GOOGLEPUBSUB"}
	KVStoreDestinations = []string{"REDIS"}
	Destinations = append(ObjectStreamDestinations, KVStoreDestinations...)
	customManagerMap = make(map[string]*CustomManagerT)
}

// newClient delegates the call to the appropriate manager
func (customManager *CustomManagerT) newClient(destID string) (*CustomDestination, error) {
	destLock := customManager.destinationLockMap[destID]
	destLock.Lock()
	defer destLock.Unlock()

	destConfig := customManager.latestConfig[destID].Config
	switch customManager.managerType {
	case STREAM:
		producer, err := streammanager.NewProducer(destConfig, customManager.destType)
		var customDestination *CustomDestination
		if err == nil {
			customDestination = &CustomDestination{
				Config: destConfig,
				Client: producer,
			}
			customManager.destinationsMap[destID] = customDestination
		}
		return customDestination, err

	case KV:
		kvManager := kvstoremanager.New(kvstoremanager.SettingsT{
			Provider: customManager.destType,
			Config:   destConfig,
		})
		customDestination := &CustomDestination{
			Config: destConfig,
			Client: kvManager,
		}
		customManager.destinationsMap[destID] = customDestination
		return customDestination, nil
	default:
		return nil, fmt.Errorf("No provider configured for Custom Destination Manager")
	}
}

func (customManager *CustomManagerT) send(jsonData json.RawMessage, destType string, client interface{}, config interface{}) (int, string, string) {
	switch customManager.managerType {
	case STREAM:
		return streammanager.Produce(jsonData, destType, client, config)
	case KV:
		key := gjson.GetBytes(jsonData, "message.key").String()
		result := gjson.GetBytes(jsonData, "message.fields").Map()
		fields := make(map[string]interface{})
		for k, v := range result {
			fields[k] = v.Str
		}
		kvManager, _ := client.(kvstoremanager.KVStoreManager)

		err := kvManager.HMSet(key, fields)
		statusCode := kvManager.StatusCode(err)
		var respBody string
		if err != nil {
			respBody = err.Error()
		}
		return statusCode, respBody, ""
	default:
		return 404, "No provider configured for StreamManager", ""
	}
}

// SendData gets the producer from streamDestinationsMap and sends data
func (customManager *CustomManagerT) SendData(jsonData json.RawMessage, sourceID string, destID string) (int, string, string) {

	destLock, ok := customManager.destinationLockMap[destID]
	if !ok {
		return 400, "Invalid dest ID", "Producer lock could not be found"
	}

	destLock.RLock()
	customDestination, ok := customManager.destinationsMap[destID]
	if !ok {
		destLock.RUnlock()
		_, err := customManager.newClient(destID)
		if err != nil {
			return 400, "Producer not found in router", "Producer could not be created"
		}
		destLock.RLock()
		customDestination = customManager.destinationsMap[destID]
	}
	destLock.RUnlock()

	respStatusCode, respStatus, respBody := customManager.send(jsonData, customManager.destType, customDestination.Client, customDestination.Config)
	return respStatusCode, respStatus, respBody
}

func (customManager *CustomManagerT) close(destination backendconfig.DestinationT) {
	destID := destination.ID
	customDestination, _ := customManager.destinationsMap[destID]
	switch customManager.managerType {
	case STREAM:
		streammanager.CloseProducer(customDestination.Client, customManager.destType)
	case KV:
		kvManager, _ := customDestination.Client.(kvstoremanager.KVStoreManager)
		kvManager.Close()
	}
	delete(customManager.destinationsMap, destID)
}

func (customManager *CustomManagerT) onConfigChange(destination backendconfig.DestinationT) error {
	newDestConfig := destination.Config
	customDestination, ok := customManager.destinationsMap[destination.ID]

	if ok {
		hasDestConfigChanged := !reflect.DeepEqual(customDestination.Config, newDestConfig)

		if !hasDestConfigChanged {
			return nil
		}

		logger.Infof("[%s] Config changed. Closing Existing client for destination: %s", customManager.destType, destination.Name)
		customManager.close(destination)
	}

	customDestination, err := customManager.newClient(destination.ID)
	if err != nil {
		return err
	}
	logger.Infof("[%s Destination manager] Created new client: %v for destination: %s", customManager.destType, destination.Name)
	return nil
}

// New returns CustomdestinationManager
func New(destType string) DestinationManager {
	if misc.ContainsString(Destinations, destType) {

		managerType := STREAM
		if misc.ContainsString(KVStoreDestinations, destType) {
			managerType = KV
		}

		customManager, ok := customManagerMap[destType]
		if ok {
			return customManager
		}

		customManager = &CustomManagerT{
			destType:           destType,
			managerType:        managerType,
			destinationsMap:    make(map[string]*CustomDestination),
			destinationLockMap: make(map[string]*sync.RWMutex),
		}
		rruntime.Go(func() {
			customManager.backendConfigSubscriber()
		})
		return customManager
	}

	return nil
}

func (customManager *CustomManagerT) backendConfigSubscriber() {
	ch := make(chan utils.DataEvent)
	backendconfig.Subscribe(ch, "backendConfig")
	for {
		config := <-ch
		allSources := config.Data.(backendconfig.SourcesT)
		for _, source := range allSources.Sources {
			for _, destination := range source.Destinations {
				if destination.DestinationDefinition.Name == customManager.destType {
					destLock, ok := customManager.destinationLockMap[destination.ID]
					if !ok {
						destLock = &sync.RWMutex{}
						customManager.destinationLockMap[destination.ID] = destLock
					}
					destLock.Lock()
					customManager.latestConfig[destination.ID] = destination
					_ = customManager.onConfigChange(destination)
					destLock.Unlock()
				}
			}
		}
	}
}
