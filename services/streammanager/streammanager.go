package streammanager

import (
	"encoding/json"
	"fmt"

	"github.com/rudderlabs/rudder-server/services/streammanager/kafka"
	"github.com/rudderlabs/rudder-server/services/streammanager/kinesis"
)

// GetProducer delegates the call to the appropriate based on parameter destination for creating producer
func GetProducer(destinationConfig interface{}, destination string) (interface{}, error) {

	switch destination {
	case "KINESIS":
		producer, err := kinesis.NewProducer(destinationConfig)
		return producer, err
	case "KAFKA":
		producer, err := kafka.NewProducer(destinationConfig)
		return producer, err
	default:
		return nil, fmt.Errorf("No provider configured for StreamManager") //404, "No provider configured for StreamManager", ""
	}

}

// CloseProducer delegates the call to the appropriate manager based on parameter destination to close a given producer
func CloseProducer(producer interface{}, destination string) error {

	switch destination {
	case "KINESIS":
		return nil
	case "KAFKA":
		err := kafka.CloseProducer(producer)
		return err
	default:
		return fmt.Errorf("No provider configured for StreamManager") //404, "No provider configured for StreamManager", ""
	}

}

// Produce delegates call to appropriate manager based on parameter destination
func Produce(jsonData json.RawMessage, destination string, producer interface{}, config interface{}) (int, string, string) {

	switch destination {
	case "KINESIS":
		return kinesis.Produce(jsonData, producer, config)
	case "KAFKA":
		return kafka.Produce(jsonData, producer, config)
	default:
		return 404, "No provider configured for StreamManager", ""
	}

}
