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
	pkgLogger                logger.LoggerI
)

// DestinationManager implements the method to send the events to custom destinations
type DestinationManager interface {
	SendData(jsonData json.RawMessage, sourceID string, destID string) (int, string)
}

// CustomManagerT handles this module
type CustomManagerT struct {
	destType             string
	managerType          string
	destinationsMap      map[string]*CustomDestination
	destinationLockMap   map[string]*sync.RWMutex
	latestConfig         map[string]backendconfig.DestinationT
	configSubscriberLock sync.RWMutex
}

//CustomDestination keeps the config of a destination and corresponding producer for a stream destination
type CustomDestination struct {
	Config interface{}
	Client interface{}
}

func init() {
	loadConfig()
	pkgLogger = logger.NewLogger().Child("router").Child("customdestinationmanager")
}

func loadConfig() {
	ObjectStreamDestinations = []string{"KINESIS", "KAFKA", "AZURE_EVENT_HUB", "FIREHOSE", "EVENTBRIDGE", "GOOGLEPUBSUB", "CONFLUENT_CLOUD", "PERSONALIZE"}
	KVStoreDestinations = []string{"REDIS"}
	Destinations = append(ObjectStreamDestinations, KVStoreDestinations...)
	customManagerMap = make(map[string]*CustomManagerT)
}

// newClient delegates the call to the appropriate manager
func (customManager *CustomManagerT) newClient(destID string) error {

	destConfig := customManager.latestConfig[destID].Config
	var customDestination *CustomDestination
	var err error

	switch customManager.managerType {
	case STREAM:
		var producer interface{}
		producer, err = streammanager.NewProducer(destConfig, customManager.destType)
		if err == nil {
			customDestination = &CustomDestination{
				Config: destConfig,
				Client: producer,
			}
			customManager.destinationsMap[destID] = customDestination
		}
	case KV:
		kvManager := kvstoremanager.New(customManager.destType, destConfig)
		customDestination = &CustomDestination{
			Config: destConfig,
			Client: kvManager,
		}
		customManager.destinationsMap[destID] = customDestination
	default:
		return fmt.Errorf("No provider configured for Custom Destination Manager")
	}
	return err
}

func (customManager *CustomManagerT) send(jsonData json.RawMessage, destType string, client interface{}, config interface{}) (int, string) {
	var statusCode int
	var respBody string
	switch customManager.managerType {
	case STREAM:
		statusCode, _, respBody = streammanager.Produce(jsonData, destType, client, config)
	case KV:
		kvManager, _ := client.(kvstoremanager.KVStoreManager)

		key, fields := kvstoremanager.EventToKeyValue(jsonData)
		err := kvManager.HMSet(key, fields)
		statusCode = kvManager.StatusCode(err)
		if err != nil {
			respBody = err.Error()
		}
	default:
		return 404, "No provider configured for Custom Destination Manager"
	}

	return statusCode, respBody
}

// SendData gets the producer from streamDestinationsMap and sends data
func (customManager *CustomManagerT) SendData(jsonData json.RawMessage, sourceID string, destID string) (int, string) {

	customManager.configSubscriberLock.RLock()
	destLock, ok := customManager.destinationLockMap[destID]
	customManager.configSubscriberLock.RUnlock()
	if !ok {
		panic(fmt.Sprintf("[CDM %s] Unexpected state: Lock missing for %s", customManager.destType, destID))
	}

	destLock.RLock()
	customDestination, ok := customManager.destinationsMap[destID]

	if !ok {
		destLock.RUnlock()
		destLock.Lock()
		err := customManager.newClient(destID)
		destLock.Unlock()
		if err != nil {
			return 400, fmt.Sprintf("[CDM %s] Unable to create client for %s", customManager.destType, destID)
		}
		destLock.RLock()
		customDestination = customManager.destinationsMap[destID]
	}
	destLock.RUnlock()

	respStatusCode, respBody := customManager.send(jsonData, customManager.destType, customDestination.Client, customDestination.Config)
	return respStatusCode, respBody
}

func (customManager *CustomManagerT) close(destination backendconfig.DestinationT) {
	destID := destination.ID
	customDestination := customManager.destinationsMap[destID]
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

		pkgLogger.Infof("[CDM %s] Config changed. Closing Existing client for destination: %s", customManager.destType, destination.Name)
		customManager.close(destination)
	}

	if err := customManager.newClient(destination.ID); err != nil {
		pkgLogger.Errorf("[CDM %s] DestID: %s, Error while creating new customer client: %v", customManager.destType, destination.ID, err)
		return err
	}
	pkgLogger.Infof("[CDM %s] DestID: %s, Created new client", customManager.destType, destination.ID)
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
			latestConfig:       make(map[string]backendconfig.DestinationT),
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
		customManager.configSubscriberLock.Lock()
		allSources := config.Data.(backendconfig.ConfigT)
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
		customManager.configSubscriberLock.Unlock()
	}
}
