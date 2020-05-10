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

func GetProducer(destinationConfigProducerMap map[string]interface{}, jsonData json.RawMessage, destination string, destinationID string) (interface{}, error) {

	switch {
	case misc.ContainsString(objectStreamDestinations, destination):
		return streammanager.GetProducer(jsonData, destination)
	default:
		return nil, fmt.Errorf("No provider configured for StreamManager")
	}

}

// Send delegates call to appropriate manager based on parameter destination
func Send(jsonData json.RawMessage, destination string, sourceID string, destinationID string) (int, string, string) {

	switch {
	case misc.ContainsString(objectStreamDestinations, destination):
		return streammanager.Produce(jsonData, destination, sourceID, destinationID)
	default:
		return 404, "No provider configured for StreamManager", ""
	}

}
