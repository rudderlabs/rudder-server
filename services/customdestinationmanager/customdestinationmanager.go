package customdestinationmanager

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sync"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/utils"

	"github.com/rudderlabs/rudder-server/services/streammanager"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

var (
	objectStreamDestinations     []string
	destinationConfigProducerMap map[string]ProducerConfig
	producerCreationLock         sync.RWMutex
)

// DestinationManager implements the method to send the events to custom destinations
type DestinationManager interface {
	SendData(jsonData json.RawMessage, sourceID string, destID string) (int, string, string)
}

// CustomManagerT handles this module
type CustomManagerT struct {
	destination string
}

//ProducerConfig keeps the config of a destination and corresponding producer for a stream destination
type ProducerConfig struct {
	Config   interface{}
	Producer interface{}
}

// DataConfigT a structure to hold data required for sending events to custom destinations
type DataConfigT struct {
	SourceID                     string
	DestinationID                string
	DestinationConfigProducerMap map[string]ProducerConfig
	ProducerCreationLock         *sync.RWMutex
}

func init() {
	loadConfig()
}

func loadConfig() {
	objectStreamDestinations = []string{"KINESIS", "KAFKA"}
	destinationConfigProducerMap = make(map[string]ProducerConfig)
}

// newProducer delegates the call to the appropriate manager based on parameter destination for creating producer
func newProducer(destinationConfig interface{}, destination string) (interface{}, error) {

	switch {
	case misc.ContainsString(objectStreamDestinations, destination):
		return streammanager.NewProducer(destinationConfig, destination)
	default:
		return nil, fmt.Errorf("No provider configured for StreamManager")
	}

}

// closeProducer delegates the call to the appropriate manager based on parameter destination to close a given producer
func closeProducer(producer interface{}, destination string) error {
	switch {
	case misc.ContainsString(objectStreamDestinations, destination):
		streammanager.CloseProducer(producer, destination)
		return nil
	default:
		return fmt.Errorf("No provider configured for StreamManager with destination %s", destination)
	}

}

func send(jsonData json.RawMessage, destination string, producer interface{}, config interface{}) (int, string, string) {

	switch {
	case misc.ContainsString(objectStreamDestinations, destination):
		return streammanager.Produce(jsonData, destination, producer, config)
	default:
		return 404, "No provider configured for StreamManager", ""
	}

}

// SendData gets the producer from destinationConfigProducerMap and sends data
func (customManager *CustomManagerT) SendData(jsonData json.RawMessage, sourceID string, destID string) (int, string, string) {

	var respStatusCode int
	var respStatus, respBody string

	destination := customManager.destination
	key := sourceID + "-" + destID

	producerConfig := ProducerConfig{}

	producerCreationLock.RLock()
	if destinationConfigProducerMap[key] != (ProducerConfig{}) {

		producerConfigFromMap := destinationConfigProducerMap[key]
		producer := producerConfigFromMap.Producer
		config := producerConfigFromMap.Config
		producerCreationLock.RUnlock()
		if producer != nil {
			producerConfig = producerConfigFromMap
		} else {
			/* As the producers are created when the server gets up or workspace config changes, it may happen that initially
			the destinatin, such as Kafka server was not up, so, producer could not be created. But, after sometime it becomes reachable,
			so, while sending the event if the producer is not creatd alrady, then it tries to create it. */
			producerCreationLock.Lock()
			producerConfigFromMap := destinationConfigProducerMap[key]
			producer = producerConfigFromMap.Producer
			config = producerConfigFromMap.Config
			if producer != nil {
				producerConfig = producerConfigFromMap
			} else {
				producer, err := newProducer(config, destination)
				producerConfig = ProducerConfig{Config: config, Producer: producer}
				destinationConfigProducerMap[key] = producerConfig
				if err != nil {
					logger.Errorf("[%s Destination manager] error while creating producer for destination: %s with error %v ", customManager.destination, destID, err)
				} else {
					logger.Infof("[%s Destination manager] created new producer: %v for destination: %s and source %s", customManager.destination, producer, destID, sourceID)
				}
			}
			producerCreationLock.Unlock()
		}
	} else {
		producerCreationLock.RUnlock()
	}

	if producerConfig != (ProducerConfig{}) || producerConfig.Producer != nil {
		respStatusCode, respStatus, respBody = send(jsonData, destination, producerConfig.Producer, producerConfig.Config)
	} else {
		respStatusCode, respStatus, respBody = 400, "Producer not found in router", "Producer could not be created"
	}

	return respStatusCode, respStatus, respBody
}

// createOrUpdateProducer creates or updates producer based on destination config and updates destinationConfigProducerMap
func createOrUpdateProducer(sourceID string, destID string, destType string, sourceName string, destination backendconfig.DestinationT) {
	key := sourceID + "-" + destID
	destConfig := destination.Config

	if destinationConfigProducerMap[key] != (ProducerConfig{}) {
		producerConfigFromMap := (destinationConfigProducerMap[key])
		producer := producerConfigFromMap.Producer
		config := producerConfigFromMap.Config
		if reflect.DeepEqual(config, destConfig) {
			if !destination.Enabled {
				logger.Infof("[%s Destination manager] closing existing producer as destination disabled for destination: %s and source: %s", destType, destination.Name, sourceName)
				closeProducer(producer, destType)
				delete(destinationConfigProducerMap, key)
			}
			return
		}
		logger.Infof("[%s Destination manager] config changed closing existing producer for destination: %s and source: %s", destType, destination.Name, sourceName)
		closeProducer(producer, destType)
		delete(destinationConfigProducerMap, key)
	}
	if destination.Enabled {
		producer, err := newProducer(destConfig, destType)
		producerConfig := ProducerConfig{Config: destConfig, Producer: producer}
		destinationConfigProducerMap[key] = producerConfig
		if err == nil {
			logger.Infof("[%s Destination manager] created new producer: %v for destination: %s and source %s", destType, producer, destination.Name, sourceName)
		} else {
			logger.Errorf("[%s Destination manager] error while creating producer for destination: %s and source %s with error %v ", destType, destination.Name, sourceName, err)
		}
	}

}

// New returns CustomdestinationManager
func New(destType string) DestinationManager {
	if misc.ContainsString(objectStreamDestinations, destType) {
		customManager := &CustomManagerT{destination: destType}
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
			if len(source.Destinations) > 0 {
				for _, destination := range source.Destinations {
					if destination.DestinationDefinition.Name == customManager.destination && misc.ContainsString(objectStreamDestinations, customManager.destination) {
						producerCreationLock.Lock()
						// Producers of stream destinations are created or closed while server starts up or workspace config gets changed
						createOrUpdateProducer(source.ID, destination.ID, customManager.destination, source.Name, destination)
						producerCreationLock.Unlock()
					}
				}
			}
		}
	}
}
