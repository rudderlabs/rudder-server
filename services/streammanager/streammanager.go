package streammanager

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/rudderlabs/rudder-server/services/streammanager/bqstream"
	"github.com/rudderlabs/rudder-server/services/streammanager/common"
	"github.com/rudderlabs/rudder-server/services/streammanager/eventbridge"
	"github.com/rudderlabs/rudder-server/services/streammanager/firehose"
	"github.com/rudderlabs/rudder-server/services/streammanager/googlepubsub"
	"github.com/rudderlabs/rudder-server/services/streammanager/googlesheets"
	"github.com/rudderlabs/rudder-server/services/streammanager/kafka"
	"github.com/rudderlabs/rudder-server/services/streammanager/kinesis"
	"github.com/rudderlabs/rudder-server/services/streammanager/personalize"
)

// NewProducer delegates the call to the appropriate based on parameter destination for creating producer
func NewProducer(destinationConfig interface{}, destType string, opts common.Opts) (common.StreamProducer, error) {
	switch destType {
	case "AZURE_EVENT_HUB":
		return kafka.NewProducerForAzureEventHubs(destinationConfig, opts)
	case "CONFLUENT_CLOUD":
		return kafka.NewProducerForConfluentCloud(destinationConfig, opts)
	case "EVENTBRIDGE":
		return eventbridge.NewProducer(destinationConfig, opts)
	case "FIREHOSE":
		return firehose.NewProducer(destinationConfig, opts)
	case "KAFKA":
		return kafka.NewProducer(destinationConfig, opts)
	case "KINESIS":
		return kinesis.NewProducer(destinationConfig, opts)
	case "GOOGLEPUBSUB":
		return googlepubsub.NewProducer(destinationConfig, opts)
	case "GOOGLESHEETS":
		return googlesheets.NewProducer(destinationConfig, opts)
	case "PERSONALIZE":
		return personalize.NewProducer(destinationConfig, opts)
	case "BQSTREAM":
		return bqstream.NewProducer(destinationConfig, opts)
	default:
		return nil, fmt.Errorf("no provider configured for StreamManager") // 404, "No provider configured for StreamManager", ""
	}
}

// Close delegates the call to the appropriate manager based on parameter destination to close a given producer
// TODO check if it's possible to pass a context
func Close(producer interface{}, destType string) error {
	switch destType {
	case "KINESIS", "FIREHOSE", "EVENTBRIDGE", "PERSONALIZE", "GOOGLESHEETS":
		return nil
	case "BQSTREAM", "KAFKA", "AZURE_EVENT_HUB", "CONFLUENT_CLOUD", "GOOGLEPUBSUB":
		streamProducer, ok := producer.(common.ClosableStreamProducer)
		if !ok {
			return errors.New("producer is not closable")
		}
		return streamProducer.Close()
	default:
		return fmt.Errorf("no provider configured for StreamManager") // 404, "no provider configured for StreamManager", ""
	}
}

// Produce delegates call to appropriate manager based on parameter destination
func Produce(jsonData json.RawMessage, destType string, producer, config interface{}) (int, string, string) {
	switch destType {
	case "KINESIS", "KAFKA", "AZURE_EVENT_HUB", "CONFLUENT_CLOUD", "PERSONALIZE",
		"FIREHOSE", "EVENTBRIDGE", "GOOGLEPUBSUB", "GOOGLESHEETS", "BQSTREAM":
		streamProducer, ok := producer.(common.StreamProducer)
		if !ok {
			return 400, "Could not create stream producer", "Could not create stream producer"
		}
		return streamProducer.Produce(jsonData, config)
	default:
		return 404, "No provider configured for StreamManager", "No provider configured for StreamManager"
	}
}
