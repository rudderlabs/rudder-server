package streammanager

import (
	"encoding/json"
	"fmt"

	"github.com/rudderlabs/rudder-server/services/streammanager/eventbridge"
	"github.com/rudderlabs/rudder-server/services/streammanager/firehose"
	"github.com/rudderlabs/rudder-server/services/streammanager/googlepubsub"
	"github.com/rudderlabs/rudder-server/services/streammanager/kafka"
	"github.com/rudderlabs/rudder-server/services/streammanager/kinesis"
	"github.com/rudderlabs/rudder-server/services/streammanager/personalize"
)

// NewProducer delegates the call to the appropriate based on parameter destination for creating producer
func NewProducer(destinationConfig interface{}, destType string) (interface{}, error) {

	switch destType {
	case "AZURE_EVENT_HUB":
		producer, err := kafka.NewProducerForAzureEventHub(destinationConfig)
		return producer, err
	case "CONFLUENT_CLOUD":
		producer, err := kafka.NewProducerForConfluentCloud(destinationConfig)
		return producer, err
	case "EVENTBRIDGE":
		producer, err := eventbridge.NewProducer(destinationConfig)
		return producer, err
	case "FIREHOSE":
		producer, err := firehose.NewProducer(destinationConfig)
		return producer, err
	case "KAFKA":
		producer, err := kafka.NewProducer(destinationConfig)
		return producer, err
	case "KINESIS":
		producer, err := kinesis.NewProducer(destinationConfig)
		return producer, err
	case "GOOGLEPUBSUB":
		producer, err := googlepubsub.NewProducer(destinationConfig)
		return producer, err
	case "PERSONALIZE":
		producer, err := personalize.NewProducer(destinationConfig)
		return producer, err
	default:
		return nil, fmt.Errorf("No provider configured for StreamManager") //404, "No provider configured for StreamManager", ""
	}

}

// CloseProducer delegates the call to the appropriate manager based on parameter destination to close a given producer
func CloseProducer(producer interface{}, destType string) error {

	switch destType {
	case "KINESIS", "FIREHOSE", "EVENTBRIDGE", "PERSONALIZE":
		return nil
	case "KAFKA", "AZURE_EVENT_HUB", "CONFLUENT_CLOUD":
		err := kafka.CloseProducer(producer)
		return err
	case "GOOGLEPUBSUB":
		err := googlepubsub.CloseProducer(producer)
		return err
	default:
		return fmt.Errorf("No provider configured for StreamManager") //404, "No provider configured for StreamManager", ""
	}

}

type StreamProducer interface {
	Produce(jsonData json.RawMessage, producer interface{}, destConfig interface{}) (int, string, string)
}

// Produce delegates call to appropriate manager based on parameter destination
func Produce(jsonData json.RawMessage, destType string, producer interface{}, config interface{}) (int, string, string) {
	switch destType {
	case "KINESIS":
		return kinesis.Produce(jsonData, producer, config)
	case "KAFKA", "AZURE_EVENT_HUB", "CONFLUENT_CLOUD":
		return kafka.Produce(jsonData, producer, config)
	case "FIREHOSE":
		return firehose.Produce(jsonData, producer, config)
	case "EVENTBRIDGE":
		return eventbridge.Produce(jsonData, producer, config)
	case "GOOGLEPUBSUB":
		return googlepubsub.Produce(jsonData, producer, config)
	case "PERSONALIZE":
		return personalize.Produce(jsonData, producer, config)
	default:
		return 404, "No provider configured for StreamManager", "No provider configured for StreamManager"
	}

}
