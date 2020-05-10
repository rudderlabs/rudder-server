package customdestinationmanager

import (
	"encoding/json"
	"fmt"

	"github.com/rudderlabs/rudder-server/services/streammanager"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

var (
	objectStreamDestinations []string
)

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
