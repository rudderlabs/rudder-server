package customdestinationmanager

import (
	"encoding/json"

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

// Send delegates call to appropriate manager based on parameter destination
func Send(jsonData json.RawMessage, destination string) (int, string, string) {

	switch {
	case misc.ContainsString(objectStreamDestinations, destination):
		return streammanager.Produce(jsonData, destination)
	default:
		return 404, "No provider configured for StreamManager", ""
	}

}
