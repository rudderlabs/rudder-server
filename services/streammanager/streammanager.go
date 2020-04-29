package streammanager

import (
	"encoding/json"

	"github.com/rudderlabs/rudder-server/services/streammanager/kafka"
	"github.com/rudderlabs/rudder-server/services/streammanager/kinesis"
)

// Produce delegates call to appropriate manager based on parameter destination
func Produce(jsonData json.RawMessage, destination string) (int, string, string) {

	switch destination {
	case "KINESIS":
		return kinesis.Produce(jsonData)
	case "KAFKA":
		return kafka.Produce(jsonData)
	default:
		return 404, "No provider configured for StreamManager", ""
	}

}
