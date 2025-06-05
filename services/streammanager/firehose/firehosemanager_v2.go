//go:generate mockgen -destination=../../../mocks/services/streammanager/firehose/mock_eventbride.go -package mock_eventbride github.com/rudderlabs/rudder-server/services/streammanager/firehose FireHoseClient

package firehose

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/firehose"
	"github.com/aws/aws-sdk-go-v2/service/firehose/types"
	"github.com/tidwall/gjson"

	awsutil "github.com/rudderlabs/rudder-go-kit/awsutil_v2"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/services/streammanager/common"
	"github.com/rudderlabs/rudder-server/utils/awsutils"
)

type FireHoseProducerV2 struct {
	client FireHoseClientV2
}

type FireHoseClientV2 interface {
	PutRecord(ctx context.Context, input *firehose.PutRecordInput, opts ...func(*firehose.Options)) (*firehose.PutRecordOutput, error)
}

// NewProducer creates a producer based on destination config
func NewProducerV2(destination *backendconfig.DestinationT, o common.Opts) (*FireHoseProducerV2, error) {
	sessionConfig, err := awsutils.NewSessionConfigForDestinationV2(destination, o.Timeout, "firehose")
	if err != nil {
		return nil, err
	}
	sessionConfig.MaxIdleConnsPerHost = config.GetIntVar(64, 1, "Router.FIREHOSE.httpMaxIdleConnsPerHost", "Router.FIREHOSE.noOfWorkers", "Router.noOfWorkers")
	awsConfig, err := awsutil.CreateAWSConfig(context.Background(), sessionConfig)
	if err != nil {
		return nil, err
	}
	return &FireHoseProducerV2{client: firehose.NewFromConfig(awsConfig)}, nil
}

// Produce creates a producer and send data to Firehose.
func (producer *FireHoseProducerV2) Produce(jsonData json.RawMessage, _ interface{}) (int, string, string) {
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
		pkgLogger.Errorf("[FireHose] error  :: %v", err)
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

	putOutput, errorRec := client.PutRecord(context.Background(), &putInput)

	if errorRec != nil {
		statusCode, respStatus, responseMessage := common.ParseAWSError(errorRec)
		pkgLogger.Errorf("[FireHose] error  :: %d : %s : %s", statusCode, respStatus, responseMessage)
		return statusCode, respStatus, responseMessage
	}

	return 200, "Success", fmt.Sprintf("Message delivered with Record information %v", putOutput)
}

func (*FireHoseProducerV2) Close() error {
	// no-op
	return nil
}
