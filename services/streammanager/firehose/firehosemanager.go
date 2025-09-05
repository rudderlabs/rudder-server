//go:generate mockgen -destination=../../../mocks/services/streammanager/firehose/mock_firehose.go -package mock_firehose github.com/rudderlabs/rudder-server/services/streammanager/firehose FirehoseClient

package firehose

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/firehose"
	"github.com/aws/aws-sdk-go-v2/service/firehose/types"
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/awsutil"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/services/streammanager/common"
	"github.com/rudderlabs/rudder-server/utils/awsutils"
)

type FirehoseProducer struct {
	client FirehoseClient
}

type FirehoseClient interface {
	PutRecord(ctx context.Context, input *firehose.PutRecordInput, opts ...func(*firehose.Options)) (*firehose.PutRecordOutput, error)
}

// NewProducer creates a producer based on destination config
func NewProducer(destination *backendconfig.DestinationT, o common.Opts) (common.Producer, error) {
	sessionConfig, err := awsutils.NewSessionConfigForDestination(destination, o.Timeout, "firehose")
	if err != nil {
		return nil, err
	}
	sessionConfig.MaxIdleConnsPerHost = config.GetIntVar(64, 1, "Router.FIREHOSE.httpMaxIdleConnsPerHost", "Router.FIREHOSE.noOfWorkers", "Router.noOfWorkers")
	awsConfig, err := awsutil.CreateAWSConfig(context.Background(), sessionConfig)
	if err != nil {
		return nil, err
	}
	return &FirehoseProducer{client: firehose.NewFromConfig(awsConfig)}, nil
}

// Produce creates a producer and send data to Firehose.
func (producer *FirehoseProducer) Produce(jsonData json.RawMessage, _ interface{}) (int, string, string) {
	parsedJSON := gjson.ParseBytes(jsonData)
	client := producer.client
	if client == nil {
		return 400, "Failure", "[FireHose] error :: Could not create producer"
	}
	data := parsedJSON.Get("message").Value()
	if data == nil {
		return 400, "Failure", "[FireHose] error :: message from payload not found"
	}
	value, err := jsonrs.Marshal(data)
	if err != nil {
		pkgLogger.Errorn("[FireHose] error", obskit.Error(err))
		return 400, "Failure", "[FireHose] error  :: " + err.Error()
	}

	deliveryStreamMapTo := parsedJSON.Get("deliveryStreamMapTo").Value()
	if deliveryStreamMapTo == nil {
		return 400, "Failure", "[FireHose] error  :: Delivery Stream not found"
	}

	deliveryStreamMapToInputString, ok := deliveryStreamMapTo.(string)
	if !ok {
		return 400, "Failure", "[FireHose] error :: Could not parse delivery stream to string"
	}
	if deliveryStreamMapToInputString == "" {
		return 400, "Failure", "[FireHose] error :: empty delivery stream"
	}

	putInput := firehose.PutRecordInput{
		DeliveryStreamName: aws.String(deliveryStreamMapToInputString),
		Record:             &types.Record{Data: value},
	}

	putOutput, err := client.PutRecord(context.Background(), &putInput)
	if err != nil {
		statusCode, respStatus, responseMessage := common.ParseAWSError(err)
		pkgLogger.Errorn("[FireHose] error  :: %d : %s : %s",
			logger.NewIntField("statusCode", int64(statusCode)),
			logger.NewStringField("respStatus", respStatus),
			logger.NewStringField("responseMessage", responseMessage))
		return statusCode, respStatus, responseMessage
	}

	return 200, "Success", fmt.Sprintf("Message delivered with Record information %v", putOutput)
}

func (*FirehoseProducer) Close() error {
	// no-op
	return nil
}
