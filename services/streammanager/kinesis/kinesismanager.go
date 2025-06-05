//go:generate mockgen -destination=../../../mocks/services/streammanager/kinesis_v1/mock_kinesis_v1.go -package mock_kinesis_v1 github.com/rudderlabs/rudder-server/services/streammanager/kinesis KinesisClientV1

package kinesis

import (
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/awsutil"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/services/streammanager/common"
	"github.com/rudderlabs/rudder-server/utils/awsutils"
)

type KinesisProducerV1 struct {
	client KinesisClientV1
}

type KinesisClientV1 interface {
	PutRecord(input *kinesis.PutRecordInput) (*kinesis.PutRecordOutput, error)
}

// NewProducer creates a producer based on destination config
func NewProducerV1(destination *backendconfig.DestinationT, o common.Opts) (*KinesisProducerV1, error) {
	sessionConfig, err := awsutils.NewSessionConfigForDestination(destination, o.Timeout, kinesis.ServiceName)
	if err != nil {
		return nil, err
	}

	sessionConfig.MaxIdleConnsPerHost = config.GetIntVar(64, 1, "Router.KINESIS.httpMaxIdleConnsPerHost", "Router.KINESIS.noOfWorkers", "Router.noOfWorkers")

	awsSession, err := awsutil.CreateSession(sessionConfig)
	if err != nil {
		return nil, err
	}
	return &KinesisProducerV1{client: kinesis.New(awsSession)}, err
}

// Produce creates a producer and send data to Kinesis.
func (producer *KinesisProducerV1) Produce(jsonData json.RawMessage, destConfig interface{}) (int, string, string) {
	client := producer.client
	if client == nil {
		return 400, "Could not create producer for Kinesis", "Could not create producer for Kinesis"
	}

	config := Config{}

	jsonConfig, err := jsonrs.Marshal(destConfig)
	if err != nil {
		outErr := fmt.Errorf("[KinesisManager] Error while Marshalling destination config %+v Error: %w", destConfig, err)
		return 400, outErr.Error(), outErr.Error()
	}
	err = jsonrs.Unmarshal(jsonConfig, &config)
	if err != nil {
		outErr := fmt.Errorf("[KinesisManager] Error while Unmarshalling destination config: %w", err)
		return 400, outErr.Error(), outErr.Error()
	}

	streamName := aws.String(config.Stream)
	parsedJSON := gjson.ParseBytes(jsonData)
	data := parsedJSON.Get("message").Value()
	if data == nil {
		return 400, "InvalidPayload", "Empty Payload"
	}
	value, err := jsonrs.Marshal(data)
	if err != nil {
		return 400, err.Error(), err.Error()
	}

	var partitionKey string

	if config.UseMessageID {
		partitionKey = parsedJSON.Get("message.messageId").String()
	}

	if partitionKey == "" {
		partitionKey = parsedJSON.Get("userId").String()
	}
	putInput := kinesis.PutRecordInput{
		Data:         value,
		StreamName:   streamName,
		PartitionKey: aws.String(partitionKey),
	}
	if err = putInput.Validate(); err != nil {
		return 400, "InvalidInput", err.Error()
	}
	putOutput, err := client.PutRecord(&putInput)
	if err != nil {
		statusCode, respStatus, responseMessage := common.ParseAWSError(err)
		pkgLogger.Errorf("[Kinesis] error  :: %d : %s : %s", statusCode, respStatus, responseMessage)
		return statusCode, respStatus, responseMessage
	}
	message := fmt.Sprintf("Message delivered at SequenceNumber: %v , shard Id: %v", putOutput.SequenceNumber, putOutput.ShardId)
	return 200, "Success", message
}

func (*KinesisProducerV1) Close() error {
	// no-op
	return nil
}
