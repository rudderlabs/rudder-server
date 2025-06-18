//go:generate mockgen -destination=../../../mocks/services/streammanager/kinesis_v2/mock_kinesis_v2.go -package mock_kinesis_v2 github.com/rudderlabs/rudder-server/services/streammanager/kinesis KinesisClientV2

package kinesis

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/tidwall/gjson"

	awsutil "github.com/rudderlabs/rudder-go-kit/awsutil_v2"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/services/streammanager/common"
	"github.com/rudderlabs/rudder-server/utils/awsutils"
)

type KinesisProducerV2 struct {
	client KinesisClientV2
}

type KinesisClientV2 interface {
	PutRecord(ctx context.Context, input *kinesis.PutRecordInput, opts ...func(*kinesis.Options)) (*kinesis.PutRecordOutput, error)
}

// NewProducer creates a producer based on destination config
func NewProducerV2(destination *backendconfig.DestinationT, o common.Opts) (common.Producer, error) {
	sessionConfig, err := awsutils.NewSessionConfigForDestinationV2(destination, o.Timeout, "kinesis")
	if err != nil {
		return nil, err
	}

	sessionConfig.MaxIdleConnsPerHost = config.GetIntVar(64, 1, "Router.KINESIS.httpMaxIdleConnsPerHost", "Router.KINESIS.noOfWorkers", "Router.noOfWorkers")

	awsConfig, err := awsutil.CreateAWSConfig(context.Background(), sessionConfig)
	if err != nil {
		return nil, err
	}
	return &KinesisProducerV2{client: kinesis.NewFromConfig(awsConfig)}, err
}

// Produce creates a producer and send data to Kinesis.
func (producer *KinesisProducerV2) Produce(jsonData json.RawMessage, destConfig interface{}) (int, string, string) {
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
	putOutput, err := client.PutRecord(context.Background(), &putInput)
	if err != nil {
		statusCode, respStatus, responseMessage := common.ParseAWSErrorV2(err)
		pkgLogger.Errorf("[Kinesis] error  :: %d : %s : %s", statusCode, respStatus, responseMessage)
		return statusCode, respStatus, responseMessage
	}
	message := fmt.Sprintf("Message delivered at SequenceNumber: %v , shard Id: %v", putOutput.SequenceNumber, putOutput.ShardId)
	return 200, "Success", message
}

func (*KinesisProducerV2) Close() error {
	// no-op
	return nil
}
