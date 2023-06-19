//go:generate mockgen -destination=../../../mocks/services/streammanager/kinesis/mock_kinesis.go -package mock_kinesis github.com/rudderlabs/rudder-server/services/streammanager/kinesis KinesisClient

package kinesis

import (
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/awsutil"
	"github.com/rudderlabs/rudder-go-kit/logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/services/streammanager/common"
	"github.com/rudderlabs/rudder-server/utils/awsutils"
)

var pkgLogger logger.Logger

// Config is the config that is required to send data to Kinesis
type Config struct {
	Stream       string
	UseMessageID bool
}

func init() {
	pkgLogger = logger.NewLogger().Child("streammanager").Child(kinesis.ServiceName)
}

type KinesisProducer struct {
	client KinesisClient
}

type KinesisClient interface {
	PutRecord(input *kinesis.PutRecordInput) (*kinesis.PutRecordOutput, error)
}

// NewProducer creates a producer based on destination config
func NewProducer(destination *backendconfig.DestinationT, o common.Opts) (*KinesisProducer, error) {
	sessionConfig, err := awsutils.NewSessionConfigForDestination(destination, o.Timeout, kinesis.ServiceName)
	if err != nil {
		return nil, err
	}
	awsSession, err := awsutil.CreateSession(sessionConfig)
	if err != nil {
		return nil, err
	}
	return &KinesisProducer{client: kinesis.New(awsSession)}, err
}

// Produce creates a producer and send data to Kinesis.
func (producer *KinesisProducer) Produce(jsonData json.RawMessage, destConfig interface{}) (int, string, string) {
	client := producer.client
	if client == nil {
		return 400, "Could not create producer for Kinesis", "Could not create producer for Kinesis"
	}

	config := Config{}

	jsonConfig, err := json.Marshal(destConfig)
	if err != nil {
		outErr := fmt.Errorf("[KinesisManager] Error while Marshalling destination config %+v Error: %w", destConfig, err)
		return 400, outErr.Error(), outErr.Error()
	}
	err = json.Unmarshal(jsonConfig, &config)
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
	value, err := json.Marshal(data)
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

func (*KinesisProducer) Close() error {
	// no-op
	return nil
}
