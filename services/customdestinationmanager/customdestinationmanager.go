package customdestinationmanager

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sync"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"

	"github.com/rudderlabs/rudder-server/services/streammanager"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

var (
	objectStreamDestinations []string
)

//ProducerConfig keeps the config of a destination and corresponding producer for a stream destination
type ProducerConfig struct {
	Config   interface{}
	Producer interface{}
}

// DataConfigT a structure to hold data required for sending events to custom destinations
type DataConfigT struct {
	SourceID                     string
	DestinationID                string
	DestinationConfigProducerMap map[string]interface{}
	ProducerCreationLock         *sync.RWMutex
}

func init() {
	loadConfig()
}

func loadConfig() {
	objectStreamDestinations = []string{"KINESIS", "KAFKA"}
}

// GetProducer delegates the call to the appropriate manager based on parameter destination for creating producer
func GetProducer(destinationConfig interface{}, destination string) (interface{}, error) {

	switch {
	case misc.ContainsString(objectStreamDestinations, destination):
		return streammanager.GetProducer(destinationConfig, destination)
	default:
		return nil, fmt.Errorf("No provider configured for StreamManager")
	}

}

// CloseProducer delegates the call to the appropriate manager based on parameter destination to close a given producer
func CloseProducer(producer interface{}, destination string) error {
	switch {
	case misc.ContainsString(objectStreamDestinations, destination):
		streammanager.CloseProducer(producer, destination)
		return nil
	default:
		return fmt.Errorf("No provider configured for StreamManager with destination %v", destination)
	}

}

// Send delegates call to appropriate manager based on parameter destination
func Send(jsonData json.RawMessage, destination string, producer interface{}, config interface{}) (int, string, string) {

	switch {
	case misc.ContainsString(objectStreamDestinations, destination):
		return streammanager.Produce(jsonData, destination, producer, config)
	default:
		return 404, "No provider configured for StreamManager", ""
	}

}

// SendData gets the producer from destinationConfigProducerMap and sends data
func SendData(jsonData json.RawMessage, destination string, dataConfig *DataConfigT) (int, string, string) {

	var respStatusCode int
	var respStatus, respBody string

	sourceID := dataConfig.SourceID
	destID := dataConfig.DestinationID
	destinationConfigProducerMap := dataConfig.DestinationConfigProducerMap
	producerCreationLock := &dataConfig.ProducerCreationLock
	key := sourceID + "-" + destID

	if destinationConfigProducerMap[key] != nil {
		producerConfigFromMap := (destinationConfigProducerMap[key]).(ProducerConfig)
		producer := producerConfigFromMap.Producer
		config := producerConfigFromMap.Config
		if producer != nil {
			respStatusCode, respStatus, respBody = Send(jsonData, destination, producer, config)
		} else {
			/* As the producers are created when the server gets up or workspace config changes, it may happen that initially
			the destinatin, such as Kafka server was not up, so, producer could not be created. But, after sometime it becomes reachable,
			so, while sending the event if the producer is not creatd alrady, then it tries to create it. */
			(*producerCreationLock).Lock()
			producer, err := GetProducer(config, destination)
			producerConfig := ProducerConfig{Config: config, Producer: producer}
			destinationConfigProducerMap[key] = producerConfig
			(*producerCreationLock).Unlock()
			logger.Infof("========== created new producer: %v for destination: %v and source %v", producer, destID, sourceID)
			if err == nil {
				respStatusCode, respStatus, respBody = Send(jsonData, destination, producer, config)
			} else {
				respStatusCode, respStatus, respBody = 400, "Producer not found in router", "Producer could not be created"
			}

		}

	} else {
		respStatusCode, respStatus, respBody = 400, "Producer not found in router", "Producer could not be created"
	}
	return respStatusCode, respStatus, respBody
}

// CreateUpdateProducer creates or updates producer based on destination config
func CreateUpdateProducer(dataConfig *DataConfigT, destType string, sourceName string, destination backendconfig.DestinationT) {
	sourceID := dataConfig.SourceID
	destID := dataConfig.DestinationID
	destinationConfigProducerMap := dataConfig.DestinationConfigProducerMap
	// producerCreationLock := &dataConfig.ProducerCreationLock
	key := sourceID + "-" + destID
	destConfig := destination.Config

	//if producerConfigFromMap != (ProducerConfig{}) {
	if destinationConfigProducerMap[key] != nil {
		producerConfigFromMap := (destinationConfigProducerMap[key]).(ProducerConfig)
		producer := producerConfigFromMap.Producer
		config := producerConfigFromMap.Config
		if reflect.DeepEqual(config, destConfig) {
			if !destination.Enabled {
				logger.Infof("======== closing existing producer as destination disabled for destination: %v and source: %v", destination.Name, sourceName)
				fmt.Printf("======= existing producer ======== %v \n", producer)
				CloseProducer(producer, destType)
				delete(destinationConfigProducerMap, key)
			}
			return
		}
		logger.Infof("========= config changed ======== closing existing producer ======= for destination: %v and source: %v", destination.Name, sourceName)
		CloseProducer(producer, destType)
		delete(destinationConfigProducerMap, key)
	}
	if destination.Enabled {
		producer, err := GetProducer(destConfig, destType)
		producerConfig := ProducerConfig{Config: destConfig, Producer: producer}
		destinationConfigProducerMap[key] = producerConfig
		if err == nil {
			logger.Infof("========== created new producer: %v for destination: %v and source %v", producer, destination.Name, sourceName)
		} else {
			logger.Errorf("========== error while creating producer for destination: %v with error %v ", destination.Name, err)
		}
	}

}
