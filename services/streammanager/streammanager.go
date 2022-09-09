package streammanager

import (
	"encoding/json"
	"errors"
	"fmt"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/services/streammanager/bqstream"
	"github.com/rudderlabs/rudder-server/services/streammanager/common"
	"github.com/rudderlabs/rudder-server/services/streammanager/eventbridge"
	"github.com/rudderlabs/rudder-server/services/streammanager/firehose"
	"github.com/rudderlabs/rudder-server/services/streammanager/googlepubsub"
	"github.com/rudderlabs/rudder-server/services/streammanager/googlesheets"
	"github.com/rudderlabs/rudder-server/services/streammanager/kafka"
	"github.com/rudderlabs/rudder-server/services/streammanager/kinesis"
	"github.com/rudderlabs/rudder-server/services/streammanager/lambda"
	"github.com/rudderlabs/rudder-server/services/streammanager/personalize"
)

// NewProducer delegates the call to the appropriate based on parameter destination for creating producer
func NewProducer(destination *backendconfig.DestinationT, opts common.Opts) (common.StreamProducer, error) {
	if destination == nil {
		return nil, errors.New("destination should not be nil")
	}
	switch destination.DestinationDefinition.Name {
	case "AZURE_EVENT_HUB":
		return kafka.NewProducerForAzureEventHubs(destination, opts)
	case "CONFLUENT_CLOUD":
		return kafka.NewProducerForConfluentCloud(destination, opts)
	case "EVENTBRIDGE":
		return eventbridge.NewProducer(destination, opts)
	case "FIREHOSE":
		return firehose.NewProducer(destination, opts)
	case "KAFKA":
		return kafka.NewProducer(destination, opts)
	case "KINESIS":
		return kinesis.NewProducer(destination, opts)
	case "GOOGLEPUBSUB":
		return googlepubsub.NewProducer(destination, opts)
	case "GOOGLESHEETS":
		return googlesheets.NewProducer(destination, opts)
	case "PERSONALIZE":
		return personalize.NewProducer(destination, opts)
	case "BQSTREAM":
		return bqstream.NewProducer(destination, opts)
	case "LAMBDA":
		return lambda.NewProducer(destination, opts)
	default:
		return nil, fmt.Errorf("no provider configured for StreamManager") // 404, "No provider configured for StreamManager", ""
	}
}

// Close delegates the call to the appropriate manager based on parameter destination to close a given producer
// TODO check if it's possible to pass a context
func Close(producer interface{}, destType string) error {
	switch destType {
	case "KINESIS", "FIREHOSE", "EVENTBRIDGE", "PERSONALIZE", "GOOGLESHEETS", "LAMBDA":
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
func Produce(jsonData json.RawMessage, destType string, producer common.StreamProducer, config map[string]interface{}) (int, string, string) {
	switch destType {
	case "KINESIS", "KAFKA", "AZURE_EVENT_HUB", "CONFLUENT_CLOUD", "PERSONALIZE",
		"FIREHOSE", "EVENTBRIDGE", "GOOGLEPUBSUB", "GOOGLESHEETS", "BQSTREAM", "LAMBDA":
		if producer == nil {
			return 500, "producer is nil", "producer is nil"
		}
		return producer.Produce(jsonData, config)
	default:
		return 404, "No provider configured for StreamManager", "No provider configured for StreamManager"
	}
}
