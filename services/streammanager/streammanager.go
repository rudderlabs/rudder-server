package streammanager

import (
	"errors"
	"fmt"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/services/streammanager/bqstream"
	"github.com/rudderlabs/rudder-server/services/streammanager/common"
	"github.com/rudderlabs/rudder-server/services/streammanager/eventbridge"
	"github.com/rudderlabs/rudder-server/services/streammanager/firehose"
	"github.com/rudderlabs/rudder-server/services/streammanager/googlecloudfunction"
	"github.com/rudderlabs/rudder-server/services/streammanager/googlepubsub"
	"github.com/rudderlabs/rudder-server/services/streammanager/googlesheets"
	"github.com/rudderlabs/rudder-server/services/streammanager/kafka"
	"github.com/rudderlabs/rudder-server/services/streammanager/kinesis"
	"github.com/rudderlabs/rudder-server/services/streammanager/lambda"
	"github.com/rudderlabs/rudder-server/services/streammanager/personalize"
	"github.com/rudderlabs/rudder-server/services/streammanager/wunderkind"
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
	case "GOOGLE_CLOUD_FUNCTION":
		return googlecloudfunction.NewProducer(destination, opts)
	case "WUNDERKIND":
		return wunderkind.NewProducer(config.Default, logger.NewLogger().Child("streammanager"))
	default:
		return nil, fmt.Errorf("no provider configured for StreamManager") // 404, "No provider configured for StreamManager", ""
	}
}
