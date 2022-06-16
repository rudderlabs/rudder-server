package streammanager

import (
	"context"
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
		return kafka.NewProducerForAzureEventHubs(destinationConfig, kafka.Opts{
			Timeout: o.Timeout,
		})
	case "CONFLUENT_CLOUD":
		return kafka.NewProducerForConfluentCloud(destinationConfig, kafka.Opts{
			Timeout: o.Timeout,
		})
	case "EVENTBRIDGE":
		return eventbridge.NewProducer(destinationConfig, eventbridge.Opts{
			Timeout: o.Timeout,
		})
	case "FIREHOSE":
		return firehose.NewProducer(destinationConfig, firehose.Opts{
			Timeout: o.Timeout,
		})
	case "KAFKA":
		return kafka.NewProducer(destinationConfig, kafka.Opts{
			Timeout: o.Timeout,
		})
	case "KINESIS":
		return kinesis.NewProducer(destinationConfig, kinesis.Opts{
			Timeout: o.Timeout,
		})
	case "GOOGLEPUBSUB":
		return googlepubsub.NewProducer(destinationConfig, googlepubsub.Opts{
			Timeout: o.Timeout,
		})
	case "GOOGLESHEETS":
		return googlesheets.NewProducer(destinationConfig, googlesheets.Opts{
			Timeout: o.Timeout,
		})
	case "PERSONALIZE":
		return personalize.NewProducer(destinationConfig, personalize.Opts{
			Timeout: o.Timeout,
		})
	case "BQSTREAM":
		return bqstream.NewProducer(destinationConfig, bqstream.Opts{
			Timeout: o.Timeout,
		})
	default:
		return nil, fmt.Errorf("no provider configured for StreamManager") //404, "No provider configured for StreamManager", ""
	}
}

// CloseProducer delegates the call to the appropriate manager based on parameter destination to close a given producer
func CloseProducer(producer interface{}, destType string) error { // TODO check if it's possible to pass a context
	switch destType {
	case "KINESIS", "FIREHOSE", "EVENTBRIDGE", "PERSONALIZE", "GOOGLESHEETS":
		return nil
	case "BQSTREAM":
		return bqstream.CloseProducer(producer)
	case "KAFKA", "AZURE_EVENT_HUB", "CONFLUENT_CLOUD":
		return kafka.CloseProducer(context.TODO(), producer)
	case "GOOGLEPUBSUB":
		return googlepubsub.CloseProducer(producer)
	default:
		return fmt.Errorf("no provider configured for StreamManager") //404, "no provider configured for StreamManager", ""
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
	case "GOOGLESHEETS":
		return googlesheets.Produce(jsonData, producer, config)
	case "PERSONALIZE":
		return personalize.Produce(jsonData, producer, config)
	case "BQSTREAM":
		return bqstream.Produce(jsonData, producer, config)
	default:
		return 404, "No provider configured for StreamManager", "No provider configured for StreamManager"
	}
}
