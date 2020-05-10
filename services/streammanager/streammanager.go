package streammanager

import (
	"encoding/json"
	"fmt"

	"github.com/rudderlabs/rudder-server/services/streammanager/kafka"
	"github.com/rudderlabs/rudder-server/services/streammanager/kinesis"
)

type StreamProducer struct {
	//Produce(jsonData json.RawMessage, destination string, sourceID string, destinationID string) (int, string, string)
	Producer interface{}
}

func GetProducer(jsonData json.RawMessage, destination string) (interface{}, error) {

	switch destination {
	case "KINESIS":
		return nil, nil //kinesis.Produce(jsonData)
	case "KAFKA":
		producer, err := kafka.NewProducer(jsonData)
		return StreamProducer{Producer: producer}, err
	default:
		return nil, fmt.Errorf("No provider configured for StreamManager") //404, "No provider configured for StreamManager", ""
	}

}

// Produce delegates call to appropriate manager based on parameter destination
func Produce(jsonData json.RawMessage, destination string, sourceID string, destinationID string) (int, string, string) {

	switch destination {
	case "KINESIS":
		return kinesis.Produce(jsonData)
	case "KAFKA":
		return kafka.Produce(jsonData, sourceID, destinationID)
	default:
		return 404, "No provider configured for StreamManager", ""
	}

}
