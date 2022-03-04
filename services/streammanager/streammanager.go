package streammanager

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-server/services/streammanager/bqstream"
	"github.com/rudderlabs/rudder-server/services/streammanager/eventbridge"
	"github.com/rudderlabs/rudder-server/services/streammanager/firehose"
	"github.com/rudderlabs/rudder-server/services/streammanager/googlepubsub"
	"github.com/rudderlabs/rudder-server/services/streammanager/googlesheets"
	"github.com/rudderlabs/rudder-server/services/streammanager/kafka"
	"github.com/rudderlabs/rudder-server/services/streammanager/kinesis"
	"github.com/rudderlabs/rudder-server/services/streammanager/personalize"
)

type Opts struct {
	Timeout time.Duration
}

// NewProducer delegates the call to the appropriate based on parameter destination for creating producer
func NewProducer(destinationConfig interface{}, destType string, o Opts) (interface{}, error) {

	switch destType {
	case "AZURE_EVENT_HUB":
		producer, err := kafka.NewProducerForAzureEventHub(destinationConfig, kafka.Opts{
			Timeout: o.Timeout,
		})
		return producer, err
	case "CONFLUENT_CLOUD":
		producer, err := kafka.NewProducerForConfluentCloud(destinationConfig, kafka.Opts{
			Timeout: o.Timeout,
		})
		return producer, err
	case "EVENTBRIDGE":
		producer, err := eventbridge.NewProducer(destinationConfig, eventbridge.Opts{
			Timeout: o.Timeout,
		})
		return producer, err
	case "FIREHOSE":
		producer, err := firehose.NewProducer(destinationConfig, firehose.Opts{
			Timeout: o.Timeout,
		})
		return producer, err
	case "KAFKA":
		producer, err := kafka.NewProducer(destinationConfig, kafka.Opts{
			Timeout: o.Timeout,
		})
		return producer, err
	case "KINESIS":
		producer, err := kinesis.NewProducer(destinationConfig, kinesis.Opts{
			Timeout: o.Timeout,
		})
		return producer, err
	case "GOOGLEPUBSUB":
		producer, err := googlepubsub.NewProducer(destinationConfig, googlepubsub.Opts{
			Timeout: o.Timeout,
		})
		return producer, err
	case "GOOGLESHEETS":
		producer, err := googlesheets.NewProducer(destinationConfig, googlesheets.Opts{
			Timeout: o.Timeout,
		})
		return producer, err
	case "PERSONALIZE":
		producer, err := personalize.NewProducer(destinationConfig, personalize.Opts{
			Timeout: o.Timeout,
		})
		return producer, err
	case "BQSTREAM":
		producer, err := bqstream.NewProducer(destinationConfig)
		return producer, err
	default:
		return nil, fmt.Errorf("No provider configured for StreamManager") //404, "No provider configured for StreamManager", ""
	}

}

// CloseProducer delegates the call to the appropriate manager based on parameter destination to close a given producer
func CloseProducer(producer interface{}, destType string) error {

	switch destType {
	case "KINESIS", "FIREHOSE", "EVENTBRIDGE", "PERSONALIZE", "GOOGLESHEETS":
		return nil
	case "BQSTREAM":
		return bqstream.CloseProducer(producer)
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
func Produce(jsonData json.RawMessage, destType string, producer interface{}, config interface{}, o Opts) (int, string, string) {
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
	case "GOOGLESHEETS":
		return googlesheets.Produce(jsonData, producer, config)
	case "PERSONALIZE":
		return personalize.Produce(jsonData, producer, config)
	case "BQSTREAM":
		return bqstream.Produce(jsonData, producer, config, bqstream.Opts{
			Timeout: o.Timeout,
		})
	default:
		return 404, "No provider configured for StreamManager", "No provider configured for StreamManager"
	}

}
