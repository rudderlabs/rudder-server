package streammanager

import (
	"encoding/json"
	"fmt"

	"github.com/rudderlabs/rudder-server/services/streammanager/eventbridge"
	"github.com/rudderlabs/rudder-server/services/streammanager/firehose"
	"github.com/rudderlabs/rudder-server/services/streammanager/kafka"
	"github.com/rudderlabs/rudder-server/services/streammanager/kinesis"
)

// NewProducer delegates the call to the appropriate based on parameter destination for creating producer
func NewProducer(destinationConfig interface{}, destination string) (interface{}, error) {

	switch destination {
	case "AZURE_EVENT_HUB":
		producer, err := kafka.NewProducerForAzureEventHub(destinationConfig)
		return producer, err
	case "EVENTBRIDGE":
		producer, err := eventbridge.NewProducer(destinationConfig)
		return producer, err
	case "FIREHOSE":
		producer, err := firehose.NewProducer(destinationConfig)
		return producer, err
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
	case "KINESIS", "FIREHOSE", "EVENTBRIDGE":
		return nil
	case "KAFKA", "AZURE_EVENT_HUB":
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
	case "KAFKA", "AZURE_EVENT_HUB":
		return kafka.Produce(jsonData, producer, config)
	case "EVENTBRIDGE":
		return eventbridge.Produce(jsonData, producer, config)
	case "FIREHOSE":
		return firehose.Produce(jsonData, producer, config)
	default:
		return 404, "No provider configured for StreamManager", ""
	}

}
