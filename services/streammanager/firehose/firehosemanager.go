//go:generate mockgen -destination=../../../mocks/services/streammanager/firehose/mock_eventbride.go -package mock_eventbride github.com/rudderlabs/rudder-server/services/streammanager/firehose FireHoseClient

package firehose

import (
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/firehose"
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/awsutil"
	"github.com/rudderlabs/rudder-go-kit/logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/services/streammanager/common"
	"github.com/rudderlabs/rudder-server/utils/awsutils"
)

var pkgLogger logger.Logger

func init() {
	pkgLogger = logger.NewLogger().Child("streammanager").Child(firehose.ServiceName)
}

type FireHoseProducer struct {
	client FireHoseClient
}

type FireHoseClient interface {
	PutRecord(input *firehose.PutRecordInput) (*firehose.PutRecordOutput, error)
}

// NewProducer creates a producer based on destination config
func NewProducer(destination *backendconfig.DestinationT, o common.Opts) (*FireHoseProducer, error) {
	sessionConfig, err := awsutils.NewSessionConfigForDestination(destination, o.Timeout, firehose.ServiceName)
	if err != nil {
		return nil, err
	}
	awsSession, err := awsutil.CreateSession(sessionConfig)
	if err != nil {
		return nil, err
	}
	return &FireHoseProducer{client: firehose.New(awsSession)}, nil
}

// Produce creates a producer and send data to Firehose.
func (producer *FireHoseProducer) Produce(jsonData json.RawMessage, _ interface{}) (int, string, string) {
	parsedJSON := gjson.ParseBytes(jsonData)
	client := producer.client
	if client == nil {
		return 400, "Failure", "[FireHose] error :: Could not create producer"
	}
	data := parsedJSON.Get("message").Value()
	if data == nil {
		return 400, "Failure", "[FireHose] error :: message from payload not found"
	}
	value, err := json.Marshal(data)
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

func (*FireHoseProducer) Close() error {
	// no-op
	return nil
}
