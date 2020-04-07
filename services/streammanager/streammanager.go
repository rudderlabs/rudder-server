package streammanager

import (
	"encoding/json"

	"github.com/rudderlabs/rudder-server/services/streammanager/kinesis"
)

// StreamManager inplements produce method.
type StreamManager interface {
	ProduceIndividual(EventPayload json.RawMessage) (int, string, string)
}

// Produce delegates call to appropriate manager based on parameter destination
func Produce(jsonData json.RawMessage, destination string) (int, string, string) {

	switch destination {
	case "KINESIS":
		return kinesis.Produce(jsonData)
	default:
		return 404, "No provider configured for StreamManager", ""
	}

}
