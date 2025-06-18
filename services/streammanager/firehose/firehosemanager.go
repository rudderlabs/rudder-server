//go:generate mockgen -destination=../../../mocks/services/streammanager/firehose_v1/mock_firehose_v1.go -package mock_firehose_v1 github.com/rudderlabs/rudder-server/services/streammanager/firehose FireHoseClientV1

package firehose

import (
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/firehose"
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/awsutil"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/services/streammanager/common"
	"github.com/rudderlabs/rudder-server/utils/awsutils"
)

type FireHoseProducerV1 struct {
	client FireHoseClientV1
}

type FireHoseClientV1 interface {
	PutRecord(input *firehose.PutRecordInput) (*firehose.PutRecordOutput, error)
}

// NewProducer creates a producer based on destination config
func NewProducerV1(destination *backendconfig.DestinationT, o common.Opts) (common.Producer, error) {
	sessionConfig, err := awsutils.NewSessionConfigForDestination(destination, o.Timeout, firehose.ServiceName)
	if err != nil {
		return nil, err
	}
	sessionConfig.MaxIdleConnsPerHost = config.GetIntVar(64, 1, "Router.FIREHOSE.httpMaxIdleConnsPerHost", "Router.FIREHOSE.noOfWorkers", "Router.noOfWorkers")
	awsSession, err := awsutil.CreateSession(sessionConfig)
	if err != nil {
		return nil, err
	}
	return &FireHoseProducerV1{client: firehose.New(awsSession)}, nil
}

// Produce creates a producer and send data to Firehose.
func (producer *FireHoseProducerV1) Produce(jsonData json.RawMessage, _ interface{}) (int, string, string) {
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
		Record:             &firehose.Record{Data: value},
	}
	if err = putInput.Validate(); err != nil {
		return 400, "InvalidInput", err.Error()
	}
	putOutput, errorRec := client.PutRecord(&putInput)

	if errorRec != nil {
		statusCode, respStatus, responseMessage := common.ParseAWSError(errorRec)
		pkgLogger.Errorf("[FireHose] error  :: %d : %s : %s", statusCode, respStatus, responseMessage)
		return statusCode, respStatus, responseMessage
	}

	return 200, "Success", fmt.Sprintf("Message delivered with Record information %v", putOutput)
}

func (*FireHoseProducerV1) Close() error {
	// no-op
	return nil
}
